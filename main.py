#!/usr/bin/env python3
"""
BitgetProBot v5.0 — IA decide BUY/SELL/HOLD/CLOSE + scanner dinámico multi-par
"""

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

load_dotenv()
logger = setup_logger()

active_traders: dict = {}
global_risk: GlobalRisk = None


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


async def start_pair(symbol: str):
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
    task = asyncio.create_task(
        trader.run(None, make_risk(), global_risk=global_risk)
    )
    active_traders[symbol] = task


async def stop_pair(symbol: str):
    if symbol not in active_traders:
        return
    task = active_traders.pop(symbol)
    task.cancel()
    logger.info(f"⏹ Trader detenido: {symbol}")


async def on_pairs_updated(new_pairs: list):
    current = set(active_traders.keys())
    updated = set(new_pairs)
    for sym in updated - current:
        await asyncio.sleep(1)
        await start_pair(sym)
    for sym in current - updated:
        await stop_pair(sym)
    logger.info(f"📊 Traders activos: {len(active_traders)}")


async def main():
    global global_risk

    logger.info("=" * 60)
    logger.info("  BitgetProBot v5.0 — IA decisions + Scanner dinámico")
    logger.info("=" * 60)

    global_risk = GlobalRisk(
        max_concurrent_trades=int(os.getenv("MAX_CONCURRENT_TRADES", "3")),
        max_global_daily_loss_pct=float(os.getenv("MAX_GLOBAL_DAILY_LOSS_PCT", "10.0")),
    )

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
    final_pairs = ai_ranked[:top_n]

    logger.info(f"✅ Pares finales ({len(final_pairs)}): {', '.join(final_pairs)}")
    await on_pairs_updated(final_pairs)
    await scanner.run_scanner_loop(on_pairs_updated)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot detenido.")
