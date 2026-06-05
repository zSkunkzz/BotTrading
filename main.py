#!/usr/bin/env python3
"""OKXBot v2.0 — IA + Scanner + Telegram + Webhook + Balance Service + Kill Switch"""

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
_LEVERAGE_BASE         = int(os.getenv("LEVERAGE", "5"))
_max_leverage_map: dict[str, int] = {}
_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_USDC_PER_TRADE        = float(os.getenv("USDC_PER_TRADE", "20"))

# ── Credenciales OKX ─────────────────────────────────────────────
_OKX_API_KEY    = os.getenv("OKX_API_KEY",    "")
_OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
_OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")
_USE_DEMO       = os.getenv("OKX_DEMO", "false").lower() in ("true", "1", "yes")
_FLAG           = "1" if _USE_DEMO else "0"  # 1=demo, 0=live

_IDLE_ROTATE_CYCLES = int(os.getenv("IDLE_ROTATE_CYCLES", "30"))
_idle_cycles: dict[str, int] = {}


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
        api_key=_OKX_API_KEY   or None,
        api_secret=_OKX_API_SECRET,
        passphrase=_OKX_PASSPHRASE or None,
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
    task   = active_traders.pop(symbol, None)
    trader = _trader_instances.pop(symbol, None)
    _idle_cycles.pop(symbol, None)
    register_traders(_trader_instances)
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
    Compara el state local contra las posiciones reales en OKX.
    Elimina del state los símbolos que ya no tienen posición abierta.
    """
    saved_symbols = set(bot_state._positions.keys())
    if not saved_symbols:
        return set()

    if not _OKX_API_KEY:
        logger.warning("\u26a0\ufe0f  No hay credenciales OKX — no se puede verificar state.")
        return saved_symbols

    try:
        import okx.Account as Account
        account_api = Account.AccountAPI(
            _OKX_API_KEY, _OKX_API_SECRET, _OKX_PASSPHRASE, False, _FLAG
        )
        data = await asyncio.to_thread(account_api.get_positions)
        positions = data.get("data", [])
    except Exception as e:
        logger.warning(
            "\u26a0\ufe0f  No se pudo consultar OKX para purge stale state: %s — conservando state.", e
        )
        return saved_symbols

    real_positions: set = set()
    for p in positions:
        inst_id = p.get("instId", "")
        pos_qty = float(p.get("pos") or 0)
        if abs(pos_qty) > 0:
            # inst_id es 'BTC-USDT-SWAP' → extraer 'BTC'
            coin = inst_id.split("-")[0].upper()
            real_positions.add(coin)

    stale = saved_symbols - real_positions
    if stale:
        for sym in stale:
            clear_position(sym)
            logger.warning(
                "\U0001f9f9 STALE STATE: '%s' estaba en state local pero NO en OKX — eliminado.", sym
            )
        logger.info(
            "\U0001f9f9 State purgado: %d fantasmas eliminados (%s). Posiciones reales: %s",
            len(stale), ", ".join(stale),
            ", ".join(real_positions) if real_positions else "ninguna",
        )
    else:
        logger.info(
            "\u2705 State OK: todas las posiciones guardadas existen en OKX (%s).",
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
    logger.info("  OKXBot v2.0 — IA + Scanner + Telegram + KS")
    logger.info("=" * 60)
    logger.info("\U0001f4b0 Sizing: USDC_PER_TRADE=%.2f USDT | LEVERAGE=%dx | DEMO=%s",
                _USDC_PER_TRADE, _LEVERAGE_BASE, _USE_DEMO)

    if not _OKX_API_KEY:
        logger.warning("\u26a0\ufe0f  OKX_API_KEY no configurado — operando en DRY-RUN.")

    # ── Balance service ──────────────────────────────────────────
    try:
        import okx.Account as Account
        _account_api = Account.AccountAPI(
            _OKX_API_KEY, _OKX_API_SECRET, _OKX_PASSPHRASE, False, _FLAG
        )
        balance_svc.init_okx(_account_api)
        logger.info("\u2705 Balance service inicializado (OKX, demo=%s)", _USE_DEMO)
    except Exception as e:
        logger.warning("\u26a0\ufe0f  Balance service no pudo inicializarse: %s", e)

    # ── Purge stale state ────────────────────────────────────────
    logger.info("\U0001f50d Verificando state local contra OKX...")
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

    await setup_telegram_commands(
        start_pair_fn=start_pair,
        stop_pair_fn=stop_pair,
        active_traders=active_traders,
        trader_instances=_trader_instances,
    )

    await notify_startup(
        pairs=list(active_traders.keys()),
        leverage=_LEVERAGE_BASE,
        usdc_per_trade=_USDC_PER_TRADE,
    )

    logger.info("\U0001f7e2 Bot arrancado — esperando señales del scanner...")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
