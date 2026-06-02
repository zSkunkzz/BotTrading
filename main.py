#!/usr/bin/env python3
"""HyperliquidBot v1.0 — IA + Scanner + Telegram + Webhook + Balance Service + Kill Switch"""

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

MAX_ACTIVE_TRADERS = int(os.getenv("MAX_ACTIVE_TRADERS", "5"))
_LEVERAGE_BASE     = int(os.getenv("LEVERAGE", "5"))
_max_leverage_map: dict[str, int] = {}
_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))
_USDC_PER_TRADE = float(os.getenv("USDC_PER_TRADE", "20"))

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

# Cuántos ciclos de HOLD consecutivos (sin posición) antes de rotar un trader
# Un ciclo = un tick del loop de decisión del trader (~60s por defecto).
# 30 ciclos x 60s = ~30 minutos sin entrar → se rota.
_IDLE_ROTATE_CYCLES = int(os.getenv("IDLE_ROTATE_CYCLES", "30"))
_idle_cycles: dict[str, int] = {}  # symbol -> ciclos consecutivos sin posición


def _resolve_hl_address() -> str:
    addr = os.getenv("HL_API_WALLET_ADDRESS", "").strip()
    if addr:
        return addr
    addr = os.getenv("HL_ACCOUNT_ADDR", "").strip()
    if addr:
        return addr
    pk = os.getenv("HL_PRIVATE_KEY", "").strip()
    if pk:
        import eth_account
        addr = eth_account.Account.from_key(pk).address
        logger.info("\U0001f511 Dirección derivada de HL_PRIVATE_KEY: %s", addr[:12] + "...")
        return addr
    return ""


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
    private_key = (
        os.getenv("HL_API_PRIVATE_KEY", "").strip()
        or os.getenv("HL_PRIVATE_KEY", "").strip()
    )
    trader = FuturesTrader(
        api_key=os.getenv("HL_API_WALLET_ADDRESS", "").strip() or None,
        api_secret=private_key,
        passphrase=None,
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
        logger.info(
            "\u2699\ufe0f  Mapa de leverage actualizado: %d pares | ejemplo: %s",
            updated,
            ", ".join(f"{k}={v}x" for k, v in list(_max_leverage_map.items())[:5]),
        )


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


async def _purge_stale_state(hl_addr: str) -> set:
    """
    Verifica qué símbolos del state local realmente tienen posición abierta
    en el exchange. Los que NO existen se borran del state.
    Retorna el conjunto de símbolos con posición REAL en el exchange.
    """
    import aiohttp
    import json as _json

    saved_symbols = set(bot_state._positions.keys())
    if not saved_symbols:
        return set()

    if not hl_addr:
        logger.warning("\u26a0\ufe0f  No hay dirección HL — no se puede verificar state contra exchange.")
        return saved_symbols

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "clearinghouseState", "user": hl_addr},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = _json.loads(await resp.text())
    except Exception as e:
        logger.warning("\u26a0\ufe0f  No se pudo consultar exchange para purge stale state: %s — conservando state.", e)
        return saved_symbols

    real_positions: set = set()
    for p in data.get("assetPositions", []):
        pos = p.get("position", {})
        try:
            szi = float(pos.get("szi", 0) or 0)
        except (TypeError, ValueError):
            continue
        if abs(szi) > 0:
            real_positions.add(pos.get("coin", "").upper())

    stale = saved_symbols - real_positions
    if stale:
        for sym in stale:
            clear_position(sym)
            logger.warning("\U0001f9f9 STALE STATE: '%s' estaba en state local pero NO en exchange — eliminado.", sym)
        logger.info(
            "\U0001f9f9 State purgado: %d fantasmas eliminados (%s). Posiciones reales: %s",
            len(stale), ", ".join(stale),
            ", ".join(real_positions) if real_positions else "ninguna",
        )
    else:
        logger.info(
            "\u2705 State OK: todas las posiciones guardadas existen en el exchange (%s).",
            ", ".join(saved_symbols),
        )
    return real_positions


async def _idle_rotation_loop(scanner: "PairScanner") -> None:
    """
    Cada 60 segundos revisa qué traders llevan demasiados ciclos sin posición
    abierta (idle). Los rota por el siguiente par del scanner que no esté activo.
    Los traders con posición abierta nunca se rotan.
    """
    while True:
        await asyncio.sleep(60)
        try:
            open_symbols = set(bot_state._positions.keys())
            to_rotate = []
            for sym, cycles in list(_idle_cycles.items()):
                if sym in open_symbols:
                    _idle_cycles[sym] = 0  # resetear si tiene posición
                    continue
                trader = _trader_instances.get(sym)
                if trader and getattr(trader, "position", None):
                    _idle_cycles[sym] = 0
                    continue
                _idle_cycles[sym] = cycles + 1
                if _idle_cycles[sym] >= _IDLE_ROTATE_CYCLES:
                    to_rotate.append(sym)

            if not to_rotate:
                continue

            # Pares disponibles en el scanner que no están activos
            scanner_pairs = [scanner.normalize(s) for s in (getattr(scanner, "_last_scored", None) or [])]
            scanner_pairs = [p["symbol"] if isinstance(p, dict) else p for p in (getattr(scanner, "_last_scored", []) or [])]
            available = [p for p in scanner_pairs if p not in active_traders]

            for sym in to_rotate:
                if not available:
                    break
                new_sym = available.pop(0)
                logger.info(
                    "\U0001f504 Rotando trader idle: %s (%d ciclos sin posición) → %s",
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
    logger.info("  HyperliquidBot v1.0 — IA + Scanner + Telegram + KS")
    logger.info("=" * 60)
    logger.info("\U0001f4b0 Sizing: USDC_PER_TRADE=%.2f USDC | LEVERAGE=%dx", _USDC_PER_TRADE, _LEVERAGE_BASE)

    hl_addr = _resolve_hl_address()
    if not hl_addr:
        logger.warning("\u26a0\ufe0f No se pudo resolver dirección HL.")

    import httpx
    _HL_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
    _HL_INFO_URL = "https://api.hyperliquid-testnet.xyz/info" if _HL_TESTNET else "https://api.hyperliquid.xyz/info"

    async def _info_post(payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(_HL_INFO_URL, json=payload)
            resp.raise_for_status()
            return resp.json()

    balance_svc.init_hl(address=hl_addr, info_post_fn=_info_post)
    logger.info("\u2705 Balance service inicializado (addr=%s)", hl_addr[:12] + "..." if hl_addr else "N/A")

    logger.info("\U0001f50d Verificando state local contra exchange...")
    real_open_symbols = await _purge_stale_state(hl_addr)

    global_risk = GlobalRisk(
        max_concurrent_trades=int(os.getenv("MAX_CONCURRENT_TRADES", str(MAX_ACTIVE_TRADERS))),
        max_global_daily_loss_pct=float(os.getenv("MAX_GLOBAL_DAILY_LOSS_PCT", "10.0")),
    )

    webhook_runner = await start_webhook_server()

    top_n = int(os.getenv("TOP_PAIRS", str(MAX_ACTIVE_TRADERS * 2)))
    logger.info("\u2699\ufe0f  MAX_ACTIVE_TRADERS=%d | TOP_PAIRS=%d | LEVERAGE_BASE=%dx | IDLE_ROTATE_CYCLES=%d",
                MAX_ACTIVE_TRADERS, top_n, _LEVERAGE_BASE, _IDLE_ROTATE_CYCLES)

    scanner = PairScanner(
        min_volume_usdt=float(os.getenv("MIN_VOLUME_USDT", "1000000")),
        min_price_change_pct=float(os.getenv("MIN_CHANGE_PCT", "0.5")),
        top_n=top_n,
        refresh_interval_min=int(os.getenv("SCANNER_REFRESH_MIN", "30")),
    )

    logger.info("\U0001f50d Escaneando mercado Hyperliquid inicial...")
    initial_pairs = await scanner.scan()

    scored_data = list(getattr(scanner, "_last_scored", []))
    if not scored_data and initial_pairs:
        scored_data = [{"symbol": sym, "volume_usdt": 0, "change_pct": 0, "score": 0} for sym in initial_pairs]

    _update_leverage_map(scored_data)

    if scored_data:
        logger.info("\U0001f916 Filtrando con IA (%d pares)...", len(scored_data))
        ai_ranked   = await ai_rank_pairs(scored_data)
        final_pairs = [scanner.normalize(s) for s in ai_ranked[:top_n]]
        seen = set()
        final_pairs = [p for p in final_pairs if not (p in seen or seen.add(p))]
    else:
        final_pairs = []

    if not final_pairs:
        logger.warning("\u26a0\ufe0f ai_rank_pairs vacío → usando scanner directamente")
        final_pairs = [scanner.normalize(s) for s in initial_pairs[:top_n]]
        seen = set()
        final_pairs = [p for p in final_pairs if not (p in seen or seen.add(p))]

    if not final_pairs:
        logger.error("\u274c Scanner devolvio 0 pares. Revisa MIN_VOLUME_USDT y MIN_CHANGE_PCT.")

    logger.info("\u2705 Pares finales (%d): %s", len(final_pairs), ", ".join(final_pairs))

    open_symbols   = real_open_symbols
    missing_from_scan = open_symbols - set(final_pairs)
    if missing_from_scan:
        logger.warning("\u26a0\ufe0f  Pares con posición REAL no están en scanner — forzando: %s", ", ".join(missing_from_scan))
        final_pairs = list(missing_from_scan) + final_pairs

    ws_feed.start(final_pairs)
    logger.info("\U0001f50c WS feed arrancado para %d símbolos", len(final_pairs))

    await asyncio.sleep(3)

    guaranteed   = [p for p in final_pairs if p in open_symbols]
    scanner_fill = [p for p in final_pairs if p not in open_symbols][:max(0, MAX_ACTIVE_TRADERS - len(guaranteed))]
    pairs_to_trade = guaranteed + scanner_fill

    logger.info("\U0001f680 Arrancando %d traders (de %d disponibles): %s",
                len(pairs_to_trade), len(final_pairs), ", ".join(pairs_to_trade))
    await start_traders_staggered(pairs_to_trade, _start_single_pair, delay=3.0)

    watchdog_task  = asyncio.create_task(kill_switch.run_watchdog(_trader_instances))
    rotation_task  = asyncio.create_task(_idle_rotation_loop(scanner))
    logger.info("\U0001f415 Kill Switch Watchdog arrancado")
    logger.info("\U0001f504 Idle Rotation Loop arrancado (rota después de %d ciclos idle)", _IDLE_ROTATE_CYCLES)

    tg_task = setup_telegram_commands()
    if tg_task:
        logger.info("\U0001f4f2 Telegram comandos activos (/resetks, /ksstatus)")
    else:
        logger.info("\U0001f4f2 Telegram comandos desactivados (sin TELEGRAM_TOKEN)")

    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    await notify_startup(pairs_to_trade, dry_run, top_n)

    try:
        await scanner.run_scanner_loop(on_pairs_updated)
    finally:
        watchdog_task.cancel()
        rotation_task.cancel()
        if tg_task:
            tg_task.cancel()
        ws_feed.stop()
        await webhook_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot detenido.")
