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
from bot.telegram_bot import notify_startup, notify_scanner_update
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

# ── Límite de traders simultáneos activos ──────────────────────────────────
# Con 15 traders HL devuelve 429 continuos. Máximo recomendado: 5.
# Sobreescribir con la variable de entorno MAX_ACTIVE_TRADERS.
MAX_ACTIVE_TRADERS = int(os.getenv("MAX_ACTIVE_TRADERS", "5"))


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
    return RiskManager(
        # USDC_PER_TRADE (era USBC_PER_TRADE — typo corregido)
        usdc_per_trade=float(os.getenv("USDC_PER_TRADE", os.getenv("USBC_PER_TRADE", "10"))),
        tp_pct=float(os.getenv("TP_PCT", "4.0")),
        sl_pct=float(os.getenv("SL_PCT", "2.0")),
        trailing_sl=os.getenv("TRAILING_SL", "true").lower() == "true",
        trailing_activation_pct=float(os.getenv("TRAILING_ACTIVATION_PCT", "1.5")),
        trailing_callback_pct=float(os.getenv("TRAILING_CALLBACK_PCT", "0.8")),
        max_daily_loss_pct=float(os.getenv("MAX_DAILY_LOSS_PCT", "5.0")),
        max_open_trades=int(os.getenv("MAX_OPEN_TRADES_PER_SYMBOL", "1")),
    )


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
    logger.info("🚀 Iniciando trader: %s", symbol)

    private_key = (
        os.getenv("HL_API_PRIVATE_KEY", "").strip()
        or os.getenv("HL_PRIVATE_KEY", "").strip()
    )

    trader = FuturesTrader(
        api_key=os.getenv("HL_API_WALLET_ADDRESS", "").strip() or None,
        api_secret=private_key,
        passphrase=None,
        symbol=symbol,
        leverage=int(os.getenv("LEVERAGE", "5")),
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


async def on_pairs_updated(new_pairs: list):
    current = set(active_traders.keys())
    # Sólo consideramos los primeros MAX_ACTIVE_TRADERS pares del nuevo scan
    updated = set(new_pairs[:MAX_ACTIVE_TRADERS])
    added   = updated - current
    removed = current - updated

    if added:
        ws_feed.update_symbols(list(added))
        await start_traders_staggered(list(added), _start_single_pair, delay=3.0)

    for sym in removed:
        await stop_pair(sym)

    await notify_scanner_update(added, removed, len(active_traders))
    logger.info("📊 Traders activos: %d/%d", len(active_traders), MAX_ACTIVE_TRADERS)


async def main():
    global global_risk

    logger.info("=" * 60)
    logger.info("  HyperliquidBot v1.0 — IA + Scanner + Telegram + KS")
    logger.info("=" * 60)

    # ── Balance service ──────────────────────────────────────────────
    hl_addr = _resolve_hl_address()
    if not hl_addr:
        logger.warning("⚠️ No se pudo resolver dirección HL. Configura HL_API_WALLET_ADDRESS o HL_ACCOUNT_ADDR.")
    balance_svc.init_hl(
        addr=hl_addr,
        testnet=os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes"),
    )
    logger.info("✅ Balance service inicializado (addr=%s)",
                hl_addr[:12] + "..." if hl_addr else "N/A")

    global_risk = GlobalRisk(
        max_concurrent_trades=int(os.getenv("MAX_CONCURRENT_TRADES", str(MAX_ACTIVE_TRADERS))),
        max_global_daily_loss_pct=float(os.getenv("MAX_GLOBAL_DAILY_LOSS_PCT", "10.0")),
    )

    webhook_runner = await start_webhook_server()

    # TOP_PAIRS: cuántos pares escanea el scanner. Escaneamos un poco más
    # de lo que vamos a tradear para tener opciones de calidad.
    # MAX_ACTIVE_TRADERS controla cuántos traders se arrancan realmente.
    top_n = int(os.getenv("TOP_PAIRS", str(MAX_ACTIVE_TRADERS * 2)))  # por defecto 10
    logger.info("⚙️  MAX_ACTIVE_TRADERS=%d | TOP_PAIRS=%d", MAX_ACTIVE_TRADERS, top_n)

    scanner = PairScanner(
        min_volume_usdt=float(os.getenv("MIN_VOLUME_USDT", "1000000")),
        min_price_change_pct=float(os.getenv("MIN_CHANGE_PCT", "0.5")),
        top_n=top_n,
        refresh_interval_min=int(os.getenv("SCANNER_REFRESH_MIN", "30")),
    )

    logger.info("🔍 Escaneando mercado Hyperliquid inicial...")
    initial_pairs = await scanner.scan()

    scored_data = []
    for entry in getattr(scanner, "_last_scored", []):
        scored_data.append(entry)

    if not scored_data and initial_pairs:
        scored_data = [
            {"symbol": sym, "volume_usdt": 0, "change_pct": 0, "score": 0}
            for sym in initial_pairs
        ]

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
        logger.error("❌ Scanner devolvió 0 pares. Revisa MIN_VOLUME_USDT y MIN_CHANGE_PCT.")

    logger.info("✅ Pares finales (%d): %s", len(final_pairs), ", ".join(final_pairs))

    # El WS feed sigue escuchando todos los pares escaneados (para el scanner),
    # pero sólo arrancamos traders para los primeros MAX_ACTIVE_TRADERS.
    ws_feed.start(final_pairs)
    logger.info("🔌 WS feed arrancado para %d símbolos", len(final_pairs))

    await asyncio.sleep(3)

    # Sólo arrancamos los primeros MAX_ACTIVE_TRADERS pares
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

    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    await notify_startup(pairs_to_trade, dry_run, top_n)

    try:
        await scanner.run_scanner_loop(on_pairs_updated)
    finally:
        watchdog_task.cancel()
        ws_feed.stop()
        await webhook_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot detenido.")
