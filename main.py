#!/usr/bin/env python3
"""BingXBot v1.0 — IA + Scanner + Telegram + Webhook + Balance Service + Kill Switch"""

import asyncio
import logging
import os
from dotenv import load_dotenv
from bot.risk import RiskManager
from bot.trader import FuturesTrader
from bot.global_risk import GlobalRisk
from bot.pair_scanner import PairScanner
from bot.ai_filter import ai_rank_pairs
from bot.logger import setup_logger
from bot.telegram_bot import notify_startup, notify_scanner_update, setup_telegram_commands
from bot.ws_feed import ws_feed
from bot.balance_service import balance_svc
from bot.kill_switch import kill_switch
from bot.state import bot_state, clear_position
from ai_rate_limiter import start_traders_staggered, telegram_ai_status
from webhook import start_webhook_server, register_traders

load_dotenv()
logger = setup_logger()

active_traders: dict = {}
global_risk: GlobalRisk = None
_trader_instances: dict = {}

MAX_ACTIVE_TRADERS     = int(os.getenv("MAX_ACTIVE_TRADERS", "10"))
# Lee LEVERAGE primero; si no existe, prueba BINGX_DEFAULT_LEVERAGE; default=5
_LEVERAGE_BASE         = int(
    os.getenv("LEVERAGE")
    or os.getenv("BINGX_DEFAULT_LEVERAGE")
    or "5"
)
_max_leverage_map: dict[str, int] = {}
_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_USDC_PER_TRADE        = float(os.getenv("USDC_PER_TRADE", "20"))

# ── Credenciales BingX ───────────────────────────────────────────────
_BINGX_API_KEY    = os.getenv("BINGX_API_KEY",    "")
_BINGX_API_SECRET = os.getenv("BINGX_API_SECRET", "")
_USE_TESTNET      = os.getenv("BINGX_TESTNET", "false").lower() in ("true", "1", "yes")

_IDLE_ROTATE_CYCLES = int(os.getenv("IDLE_ROTATE_CYCLES", "30"))
_idle_cycles: dict[str, int] = {}

# Fix #16: tiempo máximo (segundos) que _stop_pair_with_cleanup espera a que
# baje el flag _pending_order antes de forzar la cancelación de la tarea.
_PENDING_ORDER_WAIT_S = float(os.getenv("PENDING_ORDER_WAIT_S", "30"))


# ── Notifier ligero que delega en telegram_bot._send ───────────────────────
class _TelegramNotifier:
    """
    Adapter que expone la interfaz `notifier.send(text)` esperada por
    BacktestScheduler y run_backtest_now(), delegando en las funciones
    existentes de telegram_bot.py (HTML parse_mode).
    """
    async def send(self, text: str) -> None:
        from bot import telegram_bot
        await telegram_bot._send(text)


def make_risk():
    return RiskManager(
        usdc_per_trade=_USDC_PER_TRADE,
        tp_pct=float(os.getenv("TP_PCT", "4.0")),
        sl_pct=float(os.getenv("SL_PCT", "2.0")),
        trailing_sl=os.getenv("TRAILING_SL", "true").lower() == "true",
        trailing_activation_pct=float(os.getenv("TRAILING_ACTIVATION_PCT", "1.5")),
        trailing_callback_pct=float(os.getenv("TRAILING_CALLBACK_PCT", "0.8")),
        max_daily_loss_pct=float(os.getenv("MAX_DAILY_LOSS_PCT", "5.0")),
        max_open_trades=int(os.getenv("MAX_OPEN_TRADES_PER_SYMBOL", "1")),
    )


def _effective_leverage(symbol: str) -> int:
    snapshot_max = _max_leverage_map.get(symbol.upper())
    if snapshot_max and snapshot_max > 0:
        effective = min(_LEVERAGE_BASE, snapshot_max)
        if effective < _LEVERAGE_BASE:
            logger.info(
                "[%s] \u2699\ufe0f  Leverage capado por snapshot: %dx \u2192 %dx (max=%dx)",
                symbol, _LEVERAGE_BASE, effective, snapshot_max,
            )
        return effective
    return _LEVERAGE_BASE


async def _start_single_pair(symbol: str):
    if symbol in active_traders:
        return
    if len(active_traders) >= MAX_ACTIVE_TRADERS:
        logger.info(
            "[%s] Límite de traders activos alcanzado (%d/%d) — omitiendo",
            symbol, len(active_traders), MAX_ACTIVE_TRADERS,
        )
        return
    logger.info(
        "\U0001f680 Iniciando trader: %s (leverage=%dx | usdc_per_trade=%.2f)",
        symbol, _effective_leverage(symbol), _USDC_PER_TRADE,
    )
    trader = FuturesTrader(
        api_key=_BINGX_API_KEY    or None,
        api_secret=_BINGX_API_SECRET,
        passphrase=None,                          # BingX no usa passphrase
        symbol=symbol,
        leverage=_effective_leverage(symbol),
        margin_mode=os.getenv("MARGIN_MODE", "isolated"),
        dry_run=os.getenv("DRY_RUN", "true").lower() == "true",
    )
    _trader_instances[symbol] = trader
    register_traders(_trader_instances)
    task = asyncio.create_task(trader.run(make_risk(), global_risk=global_risk))
    active_traders[symbol] = task
    _idle_cycles[symbol] = 0


async def start_pair(symbol: str):
    await _start_single_pair(symbol)


async def stop_pair(symbol: str):
    if symbol not in active_traders:
        return
    task = active_traders.pop(symbol)
    _trader_instances.pop(symbol, None)
    _idle_cycles.pop(symbol, None)
    register_traders(_trader_instances)
    task.cancel()
    logger.info("\u23f9 Trader detenido: %s", symbol)


def _update_leverage_map(scored_data: list[dict]) -> None:
    updated = 0
    for entry in scored_data:
        sym = entry.get("symbol", "").upper()
        ml  = entry.get("max_leverage")
        if sym and ml and isinstance(ml, int) and ml > 0:
            _max_leverage_map[sym] = ml
            updated += 1
    if updated:
        logger.debug("\u2699\ufe0f  Mapa de leverage actualizado: %d pares", updated)


async def _stop_pair_with_cleanup(symbol: str) -> None:
    """
    Fix #16: antes de cancelar la tarea, espera hasta _PENDING_ORDER_WAIT_S
    segundos a que el flag _pending_order del trader baje a False.
    Evita cancelar una tarea en medio de open_order() y dejar posiciones
    huérfanas sin SL en el exchange.
    """
    task   = active_traders.pop(symbol, None)
    trader = _trader_instances.pop(symbol, None)
    _idle_cycles.pop(symbol, None)
    register_traders(_trader_instances)

    # Fix #16: esperar a que termine la orden en vuelo (si la hay)
    if trader is not None and getattr(trader, "_pending_order", False):
        logger.info(
            "[%s] ⏳ _pending_order=True — esperando hasta %.0fs antes de cancelar tarea.",
            symbol, _PENDING_ORDER_WAIT_S,
        )
        deadline = asyncio.get_event_loop().time() + _PENDING_ORDER_WAIT_S
        while getattr(trader, "_pending_order", False):
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                logger.warning(
                    "[%s] \u26a0\ufe0f  _pending_order no bajó en %.0fs — forzando cancelación "
                    "(puede haber posición huérfana sin SL en el exchange).",
                    symbol, _PENDING_ORDER_WAIT_S,
                )
                break
            await asyncio.sleep(min(0.5, remaining))
        else:
            logger.info("[%s] _pending_order bajó — cancelando tarea de forma segura.", symbol)

    if task and not task.done():
        task.cancel()
    if trader is not None:
        try:
            await asyncio.wait_for(trader._stopped_event.wait(), timeout=_TRADER_STOP_TIMEOUT_S)
            logger.info("[%s] Trader parado limpiamente.", symbol)
        except asyncio.TimeoutError:
            logger.warning("[%s] Trader no paró en %.0fs — continuando.", symbol, _TRADER_STOP_TIMEOUT_S)
        try:
            await trader.cleanup()
        except Exception as e:
            logger.debug("[%s] cleanup() secundario: %s", symbol, e)
    else:
        logger.info("\u23f9 Trader detenido: %s", symbol)


async def _purge_stale_state() -> set:
    """
    Compara el state local contra las posiciones reales en BingX.
    Elimina del state los símbolos que ya no tienen posición abierta.
    """
    saved_symbols = set(bot_state._positions.keys())
    if not saved_symbols:
        return set()

    if not _BINGX_API_KEY:
        logger.warning("\u26a0\ufe0f  No hay credenciales BingX — no se puede verificar state.")
        return saved_symbols

    try:
        import hashlib
        import hmac
        import time
        import urllib.parse
        import requests

        ts     = str(int(time.time() * 1000))
        params = {"timestamp": ts}
        qs     = urllib.parse.urlencode(sorted(params.items()))
        sign   = hmac.new(
            _BINGX_API_SECRET.encode(), qs.encode(), hashlib.sha256
        ).hexdigest()
        params["sign"] = sign

        base = (
            "https://open-api-vst.bingx.com"
            if _USE_TESTNET
            else "https://open-api.bingx.com"
        )
        resp = await asyncio.to_thread(
            lambda: requests.get(
                f"{base}/openApi/swap/v2/user/positions",
                params=params,
                headers={"X-BX-APIKEY": _BINGX_API_KEY},
                timeout=10,
            ).json()
        )
        positions = resp.get("data", []) or []
    except Exception as e:
        logger.warning(
            "\u26a0\ufe0f  No se pudo consultar BingX para purge stale state: %s — conservando state.", e
        )
        return saved_symbols

    real_positions: set = set()
    for p in positions:
        sym     = p.get("symbol", "")           # e.g. "BTC-USDT"
        pos_amt = float(p.get("positionAmt") or 0)
        if abs(pos_amt) > 0:
            coin = sym.replace("-USDT", "").upper()
            real_positions.add(coin)

    stale = saved_symbols - real_positions
    if stale:
        for sym in stale:
            clear_position(sym)
            logger.warning(
                "\U0001f9f9 STALE STATE: '%s' estaba en state local pero NO en BingX — eliminado.", sym
            )
        logger.info(
            "\U0001f9f9 State purgado: %d fantasmas eliminados (%s). Posiciones reales: %s",
            len(stale), ", ".join(stale),
            ", ".join(real_positions) if real_positions else "ninguna",
        )
    else:
        logger.info(
            "\u2705 State OK: todas las posiciones guardadas existen en BingX (%s).",
            ", ".join(saved_symbols),
        )
    return real_positions


async def _idle_rotation_loop(scanner: "PairScanner") -> None:
    while True:
        await asyncio.sleep(60)
        try:
            to_rotate = []
            for sym, cycles in list(_idle_cycles.items()):
                trader = _trader_instances.get(sym)
                has_position = (
                    (trader is not None and getattr(trader, "position", None) is not None)
                    or sym in bot_state._positions
                )
                if has_position:
                    _idle_cycles[sym] = 0
                    continue

                # Fix #15: forzar rotación inmediata si el trader marcó _force_idle_rotate
                if trader is not None and getattr(trader, "_force_idle_rotate", False):
                    logger.info(
                        "[%s] \U0001f504 _force_idle_rotate=True (OHLCV fail streak) — rotando inmediatamente.",
                        sym,
                    )
                    to_rotate.append(sym)
                    continue

                _idle_cycles[sym] = cycles + 1
                if _idle_cycles[sym] >= _IDLE_ROTATE_CYCLES:
                    to_rotate.append(sym)

            if not to_rotate:
                continue

            last_scored = getattr(scanner, "_last_scored", None) or []
            scanner_pairs = [
                scanner.normalize(p["symbol"] if isinstance(p, dict) else p)
                for p in last_scored
            ]
            available = [p for p in scanner_pairs if p not in active_traders]

            for sym in to_rotate:
                if not available:
                    break
                new_sym = available.pop(0)
                logger.info(
                    "\U0001f504 Rotando trader idle: %s (%d ciclos sin posición) \u2192 %s",
                    sym, _idle_cycles.get(sym, 0), new_sym,
                )
                await _stop_pair_with_cleanup(sym)
                await asyncio.sleep(1)
                await _start_single_pair(new_sym)
                ws_feed.update_symbols(list(active_traders.keys()))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("[IdleRotation] Error: %s", e, exc_info=True)


async def on_pairs_updated(new_pairs: list, added: set = None, removed: set = None):
    open_symbols = set(bot_state._positions.keys())
    for sym, t in _trader_instances.items():
        if getattr(t, "position", None) is not None:
            open_symbols.add(sym)

    protected = open_symbols - set(new_pairs)
    if protected:
        logger.info("\U0001f512 Pares con posición abierta reinyectados: %s", ", ".join(protected))
        new_pairs = list(protected) + list(new_pairs)
        new_pairs = new_pairs[:MAX_ACTIVE_TRADERS + len(protected)]

    current = set(active_traders.keys())
    capped  = set(new_pairs[:MAX_ACTIVE_TRADERS + len(protected)])

    if added is None:
        added = capped - current
    if removed is None:
        removed = current - capped

    removed = removed - open_symbols

    if removed:
        logger.info("\u2796 Deteniendo traders salientes: %s", ", ".join(removed))
        await asyncio.gather(*[_stop_pair_with_cleanup(s) for s in removed], return_exceptions=True)

    if added:
        ws_feed.update_symbols(list(added))
        await start_traders_staggered(list(added), _start_single_pair, delay=3.0)

    await notify_scanner_update(added, removed, len(active_traders))
    logger.info("\U0001f4ca Traders activos: %d/%d", len(active_traders), MAX_ACTIVE_TRADERS)


async def main():
    global global_risk

    logger.info("=" * 60)
    logger.info("  BingXBot v1.0 — IA + Scanner + Telegram + KS")
    logger.info("=" * 60)
    logger.info("\U0001f4b0 Sizing: USDC_PER_TRADE=%.2f USDT | LEVERAGE=%dx | TESTNET=%s",
                _USDC_PER_TRADE, _LEVERAGE_BASE, _USE_TESTNET)

    if not _BINGX_API_KEY:
        logger.warning("\u26a0\ufe0f  BINGX_API_KEY no configurado — operando en DRY-RUN.")

    # ── Balance service ──────────────────────────────────────────────
    try:
        balance_svc.init_bingx(_BINGX_API_KEY, _BINGX_API_SECRET, testnet=_USE_TESTNET)
        logger.info("\u2705 Balance service inicializado (BingX, testnet=%s)", _USE_TESTNET)
    except AttributeError:
        logger.warning("\u26a0\ufe0f  balance_svc.init_bingx() no disponible — balance service desactivado.")
    except Exception as e:
        logger.warning("\u26a0\ufe0f  Balance service no pudo inicializarse: %s", e)

    # ── Purge stale state ───────────────────────────────────────────────
    logger.info("\U0001f50d Verificando state local contra BingX...")
    real_open_symbols = await _purge_stale_state()

    global_risk = GlobalRisk(
        max_concurrent_trades=int(os.getenv("MAX_CONCURRENT_TRADES", str(MAX_ACTIVE_TRADERS))),
        max_global_daily_loss_pct=float(os.getenv("MAX_GLOBAL_DAILY_LOSS_PCT", "10.0")),
    )

    await global_risk.sync_open_count(len(real_open_symbols))

    webhook_runner = await start_webhook_server()

    scanner = PairScanner(on_pairs_updated=on_pairs_updated)
    asyncio.create_task(scanner.run())

    asyncio.create_task(kill_switch.run())
    asyncio.create_task(_idle_rotation_loop(scanner))

    # ── Telegram: comandos + backtest scheduler ───────────────────────────
    setup_telegram_commands()

    # Crear notifier ligero y registrarlo para el comando /backtest
    _notifier = _TelegramNotifier()
    from bot import telegram_bot as _tgbot
    from bot.backtest_scheduler import get_scheduler
    _tgbot.set_notifier(_notifier)
    get_scheduler(_notifier).start()
    logger.info("\u23f0 Backtest scheduler activo (cada %s días a las %s:00 UTC)",
                os.getenv("BACKTEST_SCHED_DAYS", "7"),
                os.getenv("BACKTEST_SCHED_HOUR", "3"))

    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    top_n   = MAX_ACTIVE_TRADERS

    await notify_startup(
        pairs=list(active_traders.keys()),
        dry_run=dry_run,
        top_n=top_n,
    )

    logger.info("\U0001f7e2 Bot arrancado — esperando se\u00f1ales del scanner...")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
