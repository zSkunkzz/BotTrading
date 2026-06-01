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
from ai_rate_limiter import start_traders_staggered, telegram_ai_status
from webhook import start_webhook_server, register_traders

load_dotenv()
logger = setup_logger()

active_traders: dict = {}
global_risk: GlobalRisk = None
_trader_instances: dict = {}

# ── Límite de traders simultáneos activos ──────────────────────────────
# Con 15 traders HL devuelve 429 continuos. Máximo recomendado: 5.
MAX_ACTIVE_TRADERS = int(os.getenv("MAX_ACTIVE_TRADERS", "5"))

# Leverage base del usuario (techo por defecto — se capará con max_leverage del par)
_LEVERAGE_BASE = int(os.getenv("LEVERAGE", "5"))

# Índice de max_leverage por símbolo: {"ZEC": 10, "LIT": 5, ...}
# Se rellena desde el snapshot y se consulta al arrancar cada trader.
_max_leverage_map: dict[str, int] = {}

# BUG #4 FIX: timeout máximo (segundos) esperando que un trader saliente
# setee _stopped_event en su cleanup(). Configurable via env var.
_TRADER_STOP_TIMEOUT_S = float(os.getenv("TRADER_STOP_TIMEOUT_S", "15"))

# ── USDC por operación — fuente ÚNICA: variable de entorno USDC_PER_TRADE ──
# NOTA: el antiguo fallback "USBC_PER_TRADE" era un typo (B en vez de D).
#       Se elimina para evitar confusión. El default es 20 USDC.
_USDC_PER_TRADE = float(os.getenv("USDC_PER_TRADE", "20"))


def _resolve_hl_address() -> str:
    """
    Resuelve la dirección del wallet principal en orden de prioridad:
      1. HL_API_WALLET_ADDRESS  (Opción A: API key agente)
      2. HL_ACCOUNT_ADDR        (Opción B: dirección explícita)
      3. Derivada de HL_PRIVATE_KEY (Opción B: derivación automática)
    """
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
        logger.info("🔑 Dirección derivada de HL_PRIVATE_KEY: %s", addr[:12] + "...")
        return addr
    return ""


def make_risk():
    """
    Crea el objeto RiskManager para cada trader.
    usdc_per_trade = _USDC_PER_TRADE (leído una vez al arranque desde USDC_PER_TRADE).
    Por defecto: 20 USDC por operación.
    """
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
    """
    Devuelve min(LEVERAGE_BASE, max_leverage_del_par).

    Si el par no está en el mapa (scanner API, sin snapshot),
    se usa LEVERAGE_BASE directamente — trader.py luego lo capará
    con _exchange_max_lev() al arrancar.
    """
    snapshot_max = _max_leverage_map.get(symbol.upper())
    if snapshot_max and snapshot_max > 0:
        effective = min(_LEVERAGE_BASE, snapshot_max)
        if effective < _LEVERAGE_BASE:
            logger.info(
                "[%s] ⚙️  Leverage capado por snapshot: %dx → %dx (max=%dx)",
                symbol, _LEVERAGE_BASE, effective, snapshot_max,
            )
        return effective
    # Sin dato de snapshot — usamos la base; trader.py consultará la API
    return _LEVERAGE_BASE


async def _start_single_pair(symbol: str):
    """Arranca un trader para el símbolo dado, respetando MAX_ACTIVE_TRADERS."""
    if symbol in active_traders:
        return
    if len(active_traders) >= MAX_ACTIVE_TRADERS:
        logger.info(
            "[%s] Límite de traders activos alcanzado (%d/%d) — omitiendo",
            symbol, len(active_traders), MAX_ACTIVE_TRADERS,
        )
        return
    logger.info(
        "🚀 Iniciando trader: %s (leverage=%dx | usdc_per_trade=%.2f)",
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
        leverage=_effective_leverage(symbol),   # ← capado por max_leverage del par
        margin_mode=os.getenv("MARGIN_MODE", "isolated"),
        dry_run=os.getenv("DRY_RUN", "true").lower() == "true",
    )
    _trader_instances[symbol] = trader
    register_traders(_trader_instances)
    task = asyncio.create_task(trader.run(make_risk(), global_risk=global_risk))
    active_traders[symbol] = task


async def start_pair(symbol: str):
    await _start_single_pair(symbol)


async def stop_pair(symbol: str):
    if symbol not in active_traders:
        return
    task = active_traders.pop(symbol)
    _trader_instances.pop(symbol, None)
    register_traders(_trader_instances)
    task.cancel()
    logger.info("⏹ Trader detenido: %s", symbol)


def _update_leverage_map(scored_data: list[dict]) -> None:
    """
    Rellena _max_leverage_map con los datos del último snapshot/scanner.
    Acepta tanto el formato del snapshot (max_leverage) como el de la API
    (no tiene ese campo — se ignora y se deja la entrada anterior si existe).
    """
    updated = 0
    for entry in scored_data:
        sym = entry.get("symbol", "").upper()
        ml  = entry.get("max_leverage")
        if sym and ml and isinstance(ml, int) and ml > 0:
            _max_leverage_map[sym] = ml
            updated += 1
    if updated:
        logger.info(
            "⚙️  Mapa de leverage actualizado: %d pares | ejemplo: %s",
            updated,
            ", ".join(f"{k}={v}x" for k, v in list(_max_leverage_map.items())[:5]),
        )


async def _stop_pair_with_cleanup(symbol: str) -> None:
    """
    BUG #4 FIX: cancela la tarea del trader y espera a que cleanup()
    setee _stopped_event antes de continuar, con timeout de seguridad.

    Esto evita la race condition donde el nuevo trader arranca mientras
    el anterior aún tiene posición abierta o locks adquiridos.
    """
    task = active_traders.pop(symbol, None)
    trader = _trader_instances.pop(symbol, None)
    register_traders(_trader_instances)

    if task and not task.done():
        task.cancel()

    if trader is not None:
        try:
            await asyncio.wait_for(
                trader._stopped_event.wait(),
                timeout=_TRADER_STOP_TIMEOUT_S,
            )
            logger.info("[%s] Trader parado limpiamente (BUG#4).", symbol)
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] Trader no paró en %.0fs — continuando de todas formas.",
                symbol, _TRADER_STOP_TIMEOUT_S,
            )
        # cleanup() ya fue llamado desde dentro del trader (run() finally),
        # pero lo llamamos también aquí por si la tarea fue cancelada antes
        # de llegar al finally.
        try:
            await trader.cleanup()
        except Exception as e:
            logger.debug("[%s] cleanup() secundario: %s", symbol, e)
    else:
        logger.info("⏹ Trader detenido: %s", symbol)


async def on_pairs_updated(new_pairs: list, added: set = None, removed: set = None):
    """
    BUG #4 FIX: callback con firma nueva (new_pairs, added, removed).

    pair_scanner.py detecta automáticamente esta firma con inspect.signature
    y la llama con los 3 argumentos. Si se llama con firma antigua (solo
    new_pairs), added/removed son None y se recalculan aquí.

    El orden importa:
      1. Primero hacer cleanup de traders SALIENTES (con espera de _stopped_event).
      2. Después arrancar traders ENTRANTES.

    Esto garantiza que no hay dos traders operando el mismo símbolo
    simultáneamente ni tampoco traders zombi bloqueando locks/márgenes.
    """
    current = set(active_traders.keys())
    capped  = set(new_pairs[:MAX_ACTIVE_TRADERS])

    # Recalcular si se llama con firma antigua
    if added is None:
        added = capped - current
    if removed is None:
        removed = current - capped

    # ── 1. Cleanup traders salientes PRIMERO ──────────────────────────────
    if removed:
        logger.info("➖ Deteniendo traders salientes: %s", ", ".join(removed))
        stop_tasks = [_stop_pair_with_cleanup(sym) for sym in removed]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

    # ── 2. Arrancar traders entrantes ─────────────────────────────────────
    if added:
        ws_feed.update_symbols(list(added))
        await start_traders_staggered(list(added), _start_single_pair, delay=3.0)

    await notify_scanner_update(added, removed, len(active_traders))
    logger.info("📊 Traders activos: %d/%d", len(active_traders), MAX_ACTIVE_TRADERS)


async def main():
    global global_risk

    logger.info("=" * 60)
    logger.info("  HyperliquidBot v1.0 — IA + Scanner + Telegram + KS")
    logger.info("=" * 60)

    # ── Confirmar sizing al arranque ──────────────────────────────────────
    logger.info(
        "💰 Sizing: USDC_PER_TRADE=%.2f USDC | LEVERAGE=%dx",
        _USDC_PER_TRADE, _LEVERAGE_BASE,
    )

    # ── Balance service ──────────────────────────────────
    # FIX: init_hl(address, info_post_fn) — firma real de BalanceService.
    # La llamada anterior usaba addr= y testnet= que no existen en la clase.
    # info_post_fn se inyecta desde trader.py via _hl_info_post cuando está
    # disponible; aquí se inicializa con None y se sobreescribe al arrancar
    # el primer trader (balance_svc.init_hl se puede llamar de nuevo).
    hl_addr = _resolve_hl_address()
    if not hl_addr:
        logger.warning(
            "⚠️ No se pudo resolver dirección HL. "
            "Configura HL_API_WALLET_ADDRESS o HL_ACCOUNT_ADDR."
        )

    # info_post_fn: función async que hace POST al endpoint de info de HL.
    # Se construye aquí con httpx para no depender de que un trader esté activo.
    import httpx

    _HL_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
    _HL_INFO_URL = (
        "https://api.hyperliquid-testnet.xyz/info"
        if _HL_TESTNET
        else "https://api.hyperliquid.xyz/info"
    )

    async def _info_post(payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(_HL_INFO_URL, json=payload)
            resp.raise_for_status()
            return resp.json()

    balance_svc.init_hl(
        address=hl_addr,
        info_post_fn=_info_post,
    )
    logger.info(
        "✅ Balance service inicializado (addr=%s)",
        hl_addr[:12] + "..." if hl_addr else "N/A",
    )

    global_risk = GlobalRisk(
        max_concurrent_trades=int(os.getenv("MAX_CONCURRENT_TRADES", str(MAX_ACTIVE_TRADERS))),
        max_global_daily_loss_pct=float(os.getenv("MAX_GLOBAL_DAILY_LOSS_PCT", "10.0")),
    )

    webhook_runner = await start_webhook_server()

    top_n = int(os.getenv("TOP_PAIRS", str(MAX_ACTIVE_TRADERS * 2)))
    logger.info("⚙️  MAX_ACTIVE_TRADERS=%d | TOP_PAIRS=%d | LEVERAGE_BASE=%dx",
                MAX_ACTIVE_TRADERS, top_n, _LEVERAGE_BASE)

    scanner = PairScanner(
        min_volume_usdt=float(os.getenv("MIN_VOLUME_USDT", "1000000")),
        min_price_change_pct=float(os.getenv("MIN_CHANGE_PCT", "0.5")),
        top_n=top_n,
        refresh_interval_min=int(os.getenv("SCANNER_REFRESH_MIN", "30")),
    )

    logger.info("🔍 Escaneando mercado Hyperliquid inicial...")
    initial_pairs = await scanner.scan()

    scored_data = list(getattr(scanner, "_last_scored", []))
    if not scored_data and initial_pairs:
        scored_data = [
            {"symbol": sym, "volume_usdt": 0, "change_pct": 0, "score": 0}
            for sym in initial_pairs
        ]

    # ── Poblar mapa de leverage desde el scan inicial ─────────────────
    _update_leverage_map(scored_data)

    if scored_data:
        logger.info("🤖 Filtrando con IA (%d pares)...", len(scored_data))
        ai_ranked   = await ai_rank_pairs(scored_data)
        final_pairs = [scanner.normalize(s) for s in ai_ranked[:top_n]]
        seen = set()
        final_pairs = [p for p in final_pairs if not (p in seen or seen.add(p))]
    else:
        final_pairs = []

    if not final_pairs:
        logger.warning("⚠️ ai_rank_pairs vacío → usando scanner directamente")
        final_pairs = [scanner.normalize(s) for s in initial_pairs[:top_n]]
        seen = set()
        final_pairs = [p for p in final_pairs if not (p in seen or seen.add(p))]

    if not final_pairs:
        logger.error("❌ Scanner devolvio 0 pares. Revisa MIN_VOLUME_USDT y MIN_CHANGE_PCT.")

    logger.info("✅ Pares finales (%d): %s", len(final_pairs), ", ".join(final_pairs))

    ws_feed.start(final_pairs)
    logger.info("🔌 WS feed arrancado para %d símbolos", len(final_pairs))

    await asyncio.sleep(3)

    pairs_to_trade = final_pairs[:MAX_ACTIVE_TRADERS]
    logger.info(
        "🚀 Arrancando %d traders (de %d disponibles): %s",
        len(pairs_to_trade), len(final_pairs), ", ".join(pairs_to_trade),
    )
    await start_traders_staggered(pairs_to_trade, _start_single_pair, delay=3.0)

    watchdog_task = asyncio.create_task(
        kill_switch.run_watchdog(_trader_instances)
    )
    logger.info("🐕 Kill Switch Watchdog arrancado")

    # ── Telegram command polling (long-poll ligero, task independiente) ──
    tg_task = setup_telegram_commands()
    if tg_task:
        logger.info("📲 Telegram comandos activos (/resetks, /ksstatus)")
    else:
        logger.info("📲 Telegram comandos desactivados (sin TELEGRAM_TOKEN)")

    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    await notify_startup(pairs_to_trade, dry_run, top_n)

    try:
        await scanner.run_scanner_loop(on_pairs_updated)
    finally:
        watchdog_task.cancel()
        if tg_task:
            tg_task.cancel()
        ws_feed.stop()
        await webhook_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot detenido.")
