#!/usr/bin/env python3
"""BitgetProBot v5.3 — IA + Scanner + Telegram + Webhook + Balance Service"""

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
from ai_rate_limiter import start_traders_staggered, telegram_ai_status
from webhook import start_webhook_server, register_traders

load_dotenv()
logger = setup_logger()

active_traders: dict = {}
global_risk: GlobalRisk = None

# Mapa símbolo → instancia FuturesTrader (para el webhook)
_trader_instances: dict = {}


def _sym_clean(symbol: str) -> str:
    """Convierte BTC/USDT:USDT o BTCUSDTUSDT → BTCUSDT (formato WS feed)."""
    return symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")


def make_risk():
    return RiskManager(
        usdt_per_trade=float(os.getenv("USDT_PER_TRADE", "10")),
        tp_pct=float(os.getenv("TP_PCT", "4.0")),
        sl_pct=float(os.getenv("SL_PCT", "2.0")),
        trailing_sl=os.getenv("TRAILING_SL", "true").lower() == "true",
        trailing_activation_pct=float(os.getenv("TRAILING_ACTIVATION_PCT", "1.5")),
        trailing_callback_pct=float(os.getenv("TRAILING_CALLBACK_PCT", "0.8")),
        max_daily_loss_pct=float(os.getenv("MAX_DAILY_LOSS_PCT", "5.0")),
        max_open_trades=1,
    )


async def _start_single_pair(symbol: str):
    if symbol in active_traders:
        return
    logger.info(f"🚀 Iniciando trader: {symbol}")
    trader = FuturesTrader(
        api_key=os.getenv("BITGET_API_KEY"),
        api_secret=os.getenv("BITGET_API_SECRET"),
        passphrase=os.getenv("BITGET_PASSPHRASE"),
        symbol=symbol,
        leverage=int(os.getenv("LEVERAGE", "5")),
        margin_mode=os.getenv("MARGIN_MODE", "isolated"),
        dry_run=os.getenv("DRY_RUN", "true").lower() == "true",
    )
    # Registrar instancia para el webhook
    _trader_instances[symbol] = trader
    register_traders(_trader_instances)

    task = asyncio.create_task(
        trader.run(make_risk(), global_risk=global_risk)
    )
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
    logger.info(f"⏹ Trader detenido: {symbol}")


async def on_pairs_updated(new_pairs: list):
    current = set(active_traders.keys())
    updated = set(new_pairs)
    added = updated - current
    removed = current - updated

    if added:
        # Añadir nuevos símbolos al WS feed antes de arrancar los traders
        ws_feed.update_symbols([_sym_clean(s) for s in added])
        await start_traders_staggered(list(added), _start_single_pair, delay=2.0)

    for sym in removed:
        await stop_pair(sym)

    await notify_scanner_update(added, removed, len(active_traders))
    logger.info(f"📊 Traders activos: {len(active_traders)}")


async def main():
    global global_risk

    logger.info("=" * 60)
    logger.info("  BitgetProBot v5.3 — IA + Scanner + Telegram + Webhook")
    logger.info("=" * 60)

    # ── Inicializar servicio de balance (singleton, una sola vez) ──────────
    balance_svc.init(
        os.getenv("BITGET_API_KEY"),
        os.getenv("BITGET_API_SECRET"),
        os.getenv("BITGET_PASSPHRASE")
    )
    logger.info("✅ Balance service inicializado")

    global_risk = GlobalRisk(
        max_concurrent_trades=int(os.getenv("MAX_CONCURRENT_TRADES", "5")),
        max_global_daily_loss_pct=float(os.getenv("MAX_GLOBAL_DAILY_LOSS_PCT", "10.0")),
    )

    # Arrancar webhook server en paralelo
    webhook_runner = await start_webhook_server()

    scanner = PairScanner(
        api_key=os.getenv("BITGET_API_KEY"),
        api_secret=os.getenv("BITGET_API_SECRET"),
        passphrase=os.getenv("BITGET_PASSPHRASE"),
        min_volume_usdt=float(os.getenv("MIN_VOLUME_USDT", "5000000")),
        min_price_change_pct=float(os.getenv("MIN_CHANGE_PCT", "1.5")),
        top_n=int(os.getenv("TOP_PAIRS", "15")),
        refresh_interval_min=int(os.getenv("SCANNER_REFRESH_MIN", "30")),
    )

    logger.info("🔍 Escaneando mercado inicial...")
    initial_pairs = await scanner.scan()

    scored_data = []
    for sym in initial_pairs:
        try:
            ticker = await scanner.exchange.fetch_ticker(sym)
            scored_data.append({
                "symbol": sym,
                "volume_usdt": round(float(ticker.get("quoteVolume") or 0) / 1e6, 2),
                "change_pct": round(abs(float(ticker.get("percentage") or 0)), 2),
                "score": 0,
            })
        except Exception:
            pass

    logger.info("🤖 Filtrando con IA...")
    ai_ranked = await ai_rank_pairs(scored_data)
    top_n = int(os.getenv("TOP_PAIRS", "15"))

    # Normalizar al formato estándar ccxt (BASE/USDT:USDT)
    final_pairs = [scanner.normalize(sym) for sym in ai_ranked[:top_n]]

    # Deduplicar preservando orden
    seen = set()
    final_pairs = [p for p in final_pairs if not (p in seen or seen.add(p))]

    # Fallback: si IA devolvió lista vacía usar el scan inicial
    if not final_pairs:
        logger.warning("⚠️ ai_rank_pairs vacío → usando scanner directamente")
        final_pairs = initial_pairs[:top_n]

    logger.info(f"✅ Pares finales ({len(final_pairs)}): {', '.join(final_pairs)}")

    # ── Arrancar WS feed antes que los traders ─────────────────────────
    ws_symbols = [_sym_clean(s) for s in final_pairs]
    ws_feed.start(ws_symbols)
    logger.info(f"🔌 WS feed arrancado para {len(ws_symbols)} símbolos")

    # Esperar mínimo 3s para que el WS haga snapshot inicial de candles
    await asyncio.sleep(3)

    await start_traders_staggered(final_pairs, _start_single_pair, delay=2.0)

    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    await notify_startup(final_pairs, dry_run, top_n)

    try:
        await scanner.run_scanner_loop(on_pairs_updated)
    finally:
        ws_feed.stop()
        await webhook_runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot detenido.")