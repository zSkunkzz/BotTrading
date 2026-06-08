#!/usr/bin/env python3
"""
backtest_proposed.py — Backtesting con parámetros propuestos sobre datos reales de BingX.

Uso:
    python backtest_proposed.py
    python backtest_proposed.py --days 90 --symbols BTC-USDT ETH-USDT SOL-USDT
    python backtest_proposed.py --days 30 --csv

Requiere en .env (o variables de entorno en Railway):
    BINGX_API_KEY=...
    BINGX_API_SECRET=...

Los parámetros propuestos se inyectan como variables de entorno ANTES de importar
el signal_engine, por lo que no modifican nada en producción.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import logging

# ── Inyectar config propuesta ANTES de importar signal_engine ─────────────────
# Esto sobreescribe los valores solo en este proceso, nunca en Railway.
PROPOSED = {
    "MIN_SIGNAL_SCORE":      "5",
    "MIN_SCORE_RATIO":       "0.45",
    "MIN_RR":                "1.3",
    "EMA_SPREAD_RANGE_MAX":  "0.0008",
    "VOL_MIN_GLOBAL":        "0.4",
    "VOL_SIGNAL_MIN":        "0.7",
    "ADX_MIN":               "15.0",
}
for k, v in PROPOSED.items():
    os.environ[k] = v

# ── Ahora sí importamos el backtester real ────────────────────────────────────
from bot.backtester import _fetch_full_history, _run_backtest, save_csv

log = logging.getLogger("backtest_proposed")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Pares por defecto a testear (los más líquidos en BingX perpetuos)
DEFAULT_SYMBOLS = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "BNB-USDT",
    "XRP-USDT",
]


def _print_config():
    print("\n" + "─" * 54)
    print("  CONFIG PROPUESTA (sobreescribe defaults del bot)")
    print("─" * 54)
    for k, v in PROPOSED.items():
        print(f"  {k:<28} = {v}")
    print("─" * 54 + "\n")


async def run_all(symbols: list[str], days: int, tf: str, leverage: int, export_csv: bool):
    api_key    = os.getenv("BINGX_API_KEY", "")
    api_secret = os.getenv("BINGX_API_SECRET", "")

    if not api_key or not api_secret:
        log.error(
            "Faltan BINGX_API_KEY / BINGX_API_SECRET.\n"
            "  Añádelas en .env o en las variables de entorno de Railway."
        )
        sys.exit(1)

    _print_config()

    all_trades_total = 0
    all_wins_total   = 0
    all_pnl_total    = 0.0
    results = []

    for sym in symbols:
        print(f"\n{'═'*54}")
        print(f"  Descargando {sym}  ({days}d  tf={tf})")
        print(f"{'═'*54}")

        try:
            df = await _fetch_full_history(
                symbol=sym, tf=tf, days=days,
                api_key=api_key, api_secret=api_secret,
            )
        except Exception as e:
            log.warning(f"No se pudo descargar {sym}: {e}")
            continue

        if df is None or df.empty:
            log.warning(f"Sin datos para {sym}, saltando.")
            continue

        result = _run_backtest(df, sym, tf, days, leverage)
        print(result.summary())

        if export_csv:
            fname = save_csv(result)
            if fname:
                print(f"  CSV exportado → {fname}")

        all_trades_total += result.n_trades
        all_wins_total   += result.wins
        all_pnl_total    += result.total_pnl
        results.append(result)

    # ── Resumen global ─────────────────────────────────────────────────────────
    if results:
        wr_global = all_wins_total / all_trades_total * 100 if all_trades_total else 0
        print("\n" + "═" * 54)
        print("  RESUMEN GLOBAL — CONFIG PROPUESTA")
        print("═" * 54)
        print(f"  Símbolos testeados : {len(results)}/{len(symbols)}")
        print(f"  Total trades       : {all_trades_total}")
        print(f"  Winrate global     : {wr_global:.1f}%  "
              f"({all_wins_total}W / {all_trades_total - all_wins_total}L)")
        print(f"  PnL total acum.    : {all_pnl_total:+.2f}%")
        print(f"  PnL medio/trade    : {all_pnl_total/all_trades_total:+.2f}%" if all_trades_total else "")

        best = max(results, key=lambda r: r.total_pnl)
        worst = min(results, key=lambda r: r.total_pnl)
        print(f"  Mejor par          : {best.symbol}  ({best.total_pnl:+.2f}%,  WR {best.win_rate:.0f}%)")
        print(f"  Peor par           : {worst.symbol}  ({worst.total_pnl:+.2f}%,  WR {worst.win_rate:.0f}%)")
        print("═" * 54 + "\n")
    else:
        print("\n⚠️  No se completó ningún backtest. Revisa credenciales y conectividad.")


def main():
    parser = argparse.ArgumentParser(
        description="Backtest con config propuesta sobre datos reales de BingX"
    )
    parser.add_argument(
        "--symbols", nargs="+", default=DEFAULT_SYMBOLS,
        help="Lista de símbolos BingX perpetuos (ej: BTC-USDT ETH-USDT)"
    )
    parser.add_argument("--days",     type=int, default=90,  help="Días de histórico (default: 90)")
    parser.add_argument("--tf",       default="15m",          help="Timeframe (default: 15m)")
    parser.add_argument("--leverage", type=int, default=5,   help="Apalancamiento simulado (default: 5)")
    parser.add_argument("--csv",      action="store_true",   help="Exportar trades a CSV")
    args = parser.parse_args()

    asyncio.run(run_all(
        symbols=args.symbols,
        days=args.days,
        tf=args.tf,
        leverage=args.leverage,
        export_csv=args.csv,
    ))


if __name__ == "__main__":
    main()
