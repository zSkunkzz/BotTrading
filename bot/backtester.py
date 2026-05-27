# ============================================================
# bot/backtester.py  —  Backtesting vectorizado sobre OHLCV
# Uso:
#   python -m bot.backtester --symbol BTC/USDT:USDT --days 90
#   python -m bot.backtester --symbol ETH/USDT:USDT --days 30 --tf 1h
# ============================================================

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

import ccxt.async_support as ccxt
import numpy as np
import pandas as pd

try:
    import ta as ta_lib
except ImportError:
    ta_lib = None

from bot.signal_engine import _analyze_tf, _compute_score, MIN_SCORE, MIN_RR
from bot.signal_engine import ATR_MULT_SL, TP1_MULT, TP2_MULT, TP3_MULT

log = logging.getLogger("Backtester")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


# ─────────────────────────────────────────────────────────────
# ESTRUCTURAS
# ─────────────────────────────────────────────────────────────

@dataclass
class Trade:
    symbol:      str
    direction:   str        # LONG / SHORT
    entry_idx:   int
    entry_price: float
    sl:          float
    tp1:         float
    tp2:         float
    tp3:         float
    score:       int
    exit_price:  Optional[float] = None
    exit_idx:    Optional[int]   = None
    reason:      str = ""
    pnl_pct:     float = 0.0
    tp2_hit:     bool  = False


@dataclass
class BacktestResult:
    symbol:       str
    timeframe:    str
    n_days:       int
    trades:       list[Trade] = field(default_factory=list)
    leverage:     int = 5

    @property
    def n_trades(self): return len(self.trades)

    @property
    def wins(self): return sum(1 for t in self.trades if t.pnl_pct > 0)

    @property
    def losses(self): return sum(1 for t in self.trades if t.pnl_pct <= 0)

    @property
    def win_rate(self):
        return self.wins / self.n_trades * 100 if self.n_trades else 0

    @property
    def total_pnl(self): return sum(t.pnl_pct for t in self.trades)

    @property
    def avg_pnl(self):
        return self.total_pnl / self.n_trades if self.n_trades else 0

    @property
    def max_dd(self):
        """Máximo drawdown acumulado."""
        if not self.trades:
            return 0.0
        equity = np.cumsum([t.pnl_pct for t in self.trades])
        peak   = np.maximum.accumulate(equity)
        dd     = equity - peak
        return float(dd.min())

    @property
    def profit_factor(self):
        gross_win  = sum(t.pnl_pct for t in self.trades if t.pnl_pct > 0)
        gross_loss = abs(sum(t.pnl_pct for t in self.trades if t.pnl_pct <= 0))
        return gross_win / gross_loss if gross_loss > 0 else float("inf")

    @property
    def expectancy(self):
        """Ganancia esperada por trade (en % con leverage)."""
        if not self.n_trades:
            return 0.0
        avg_win  = np.mean([t.pnl_pct for t in self.trades if t.pnl_pct > 0]) if self.wins  else 0
        avg_loss = np.mean([t.pnl_pct for t in self.trades if t.pnl_pct <= 0]) if self.losses else 0
        wr = self.win_rate / 100
        return wr * avg_win + (1 - wr) * avg_loss

    def summary(self) -> str:
        sep = "═" * 52
        lines = [
            sep,
            f"  BACKTEST  {self.symbol}  {self.timeframe}  ({self.n_days}d)",
            sep,
            f"  Trades       : {self.n_trades}",
            f"  Win Rate     : {self.win_rate:.1f}%  ({self.wins}W / {self.losses}L)",
            f"  PnL total    : {self.total_pnl:+.2f}%",
            f"  PnL medio    : {self.avg_pnl:+.2f}% / trade",
            f"  Max Drawdown : {self.max_dd:.2f}%",
            f"  Profit Factor: {self.profit_factor:.2f}",
            f"  Expectancy   : {self.expectancy:+.2f}% / trade",
            sep,
        ]
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# FETCH HISTÓRICO
# ─────────────────────────────────────────────────────────────

async def _fetch_full_history(
    symbol: str, tf: str, days: int, api_key="", api_secret="", passphrase=""
) -> pd.DataFrame:
    exchange = ccxt.bitget({
        "apiKey":   api_key,
        "secret":   api_secret,
        "password": passphrase,
        "options":  {"defaultType": "swap"},
    })
    try:
        await exchange.load_markets()
        tf_ms = {
            "1m": 60_000, "5m": 300_000, "15m": 900_000,
            "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
        }.get(tf, 900_000)
        since = exchange.milliseconds() - days * 86_400_000
        all_bars = []
        while True:
            bars = await exchange.fetch_ohlcv(symbol, tf, since=since, limit=1000)
            if not bars:
                break
            all_bars.extend(bars)
            if len(bars) < 1000:
                break
            since = bars[-1][0] + tf_ms
            await asyncio.sleep(0.5)
        df = pd.DataFrame(all_bars, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.drop_duplicates("ts").set_index("ts").astype(float)
        log.info(f"Descargadas {len(df)} velas {tf} para {symbol}")
        return df
    finally:
        await exchange.close()


# ─────────────────────────────────────────────────────────────
# MOTOR DE BACKTEST
# ─────────────────────────────────────────────────────────────

def _run_backtest(
    df: pd.DataFrame,
    symbol: str,
    tf: str,
    days: int,
    leverage: int = 5,
    warmup: int = 200,
    tp2_partial_ratio: float = 0.5,
) -> BacktestResult:
    result  = BacktestResult(symbol=symbol, timeframe=tf, n_days=days, leverage=leverage)
    n       = len(df)
    in_trade: Optional[Trade] = None

    close_arr = df["close"].values
    high_arr  = df["high"].values
    low_arr   = df["low"].values

    for i in range(warmup, n):
        price = close_arr[i]
        hi    = high_arr[i]
        lo    = low_arr[i]

        # ── Gestionar trade abierto ───────────────────────────────
        if in_trade is not None:
            t = in_trade
            is_long = t.direction == "LONG"

            # SL
            sl_hit = (lo <= t.sl) if is_long else (hi >= t.sl)
            if sl_hit:
                exit_p = t.sl
                pnl = (exit_p - t.entry_price) / t.entry_price * 100 * leverage
                if not is_long:
                    pnl = -pnl
                t.exit_price = exit_p
                t.exit_idx   = i
                t.reason     = "SL"
                t.pnl_pct    = round(pnl, 3)
                result.trades.append(t)
                in_trade = None
                continue

            # TP2 parcial
            if t.tp2 and not t.tp2_hit:
                tp2_hit = (hi >= t.tp2) if is_long else (lo <= t.tp2)
                if tp2_hit:
                    t.tp2_hit = True
                    t.sl      = t.entry_price   # mover SL a BE
                    # El PnL parcial se realizará en la salida final

            # TP3
            if t.tp3:
                tp3_hit = (hi >= t.tp3) if is_long else (lo <= t.tp3)
                if tp3_hit:
                    exit_p = t.tp3
                    pnl_full = (exit_p - t.entry_price) / t.entry_price * 100 * leverage
                    if not is_long:
                        pnl_full = -pnl_full
                    # Si TP2 fue hit, parte del PnL ya se realizó a TP2
                    if t.tp2_hit and t.tp2:
                        pnl_tp2 = abs(t.tp2 - t.entry_price) / t.entry_price * 100 * leverage
                        pnl = pnl_tp2 * tp2_partial_ratio + pnl_full * (1 - tp2_partial_ratio)
                    else:
                        pnl = pnl_full
                    t.exit_price = exit_p
                    t.exit_idx   = i
                    t.reason     = "TP3"
                    t.pnl_pct    = round(pnl, 3)
                    result.trades.append(t)
                    in_trade = None
                    continue

            # TP1 (si no hay TP2/TP3)
            if t.tp1 and not t.tp2:
                tp1_hit = (hi >= t.tp1) if is_long else (lo <= t.tp1)
                if tp1_hit:
                    exit_p = t.tp1
                    pnl = abs(exit_p - t.entry_price) / t.entry_price * 100 * leverage
                    t.exit_price = exit_p
                    t.exit_idx   = i
                    t.reason     = "TP1"
                    t.pnl_pct    = round(pnl, 3)
                    result.trades.append(t)
                    in_trade = None
                    continue

            continue  # Posición abierta, esperando trigger

        # ── Buscar señal ──────────────────────────────────────────
        window = df.iloc[max(0, i - 199): i + 1]
        if len(window) < 55:
            continue

        # Para el backtest usamos solo el timeframe del df cargado
        # (equivalente a 15m). Señal simplificada 1-TF.
        s = _analyze_tf(window)
        if not s:
            continue

        score_l = sum(max(0,  s.get(k, 0)) for k in
                      ("ema_trend", "macd", "rsi", "supertrend", "stoch", "volume", "bb"))
        score_s = sum(max(0, -s.get(k, 0)) for k in
                      ("ema_trend", "macd", "rsi", "supertrend", "stoch", "volume", "bb"))
        score = max(score_l, score_s)
        direction = "LONG" if score_l >= score_s else "SHORT"

        if score < MIN_SCORE:
            continue

        # ATR
        try:
            atr_s = ta_lib.volatility.AverageTrueRange(
                window["high"], window["low"], window["close"], window=14
            ).average_true_range()
            atr = float(atr_s.iloc[-1])
        except Exception:
            atr = price * 0.005

        risk = atr * ATR_MULT_SL
        if direction == "LONG":
            sl  = price - risk
            tp1 = price + risk * TP1_MULT
            tp2 = price + risk * TP2_MULT
            tp3 = price + risk * TP3_MULT
        else:
            sl  = price + risk
            tp1 = price - risk * TP1_MULT
            tp2 = price - risk * TP2_MULT
            tp3 = price - risk * TP3_MULT

        rr = abs(tp1 - price) / abs(price - sl) if abs(price - sl) > 0 else 0
        if rr < MIN_RR:
            continue

        in_trade = Trade(
            symbol=symbol, direction=direction,
            entry_idx=i, entry_price=price,
            sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, score=score,
        )

    # Trade aún abierto al final del histórico → cerrar al último precio
    if in_trade is not None:
        t = in_trade
        exit_p = close_arr[-1]
        is_long = t.direction == "LONG"
        pnl = (exit_p - t.entry_price) / t.entry_price * 100 * leverage
        if not is_long:
            pnl = -pnl
        t.exit_price = exit_p
        t.exit_idx   = n - 1
        t.reason     = "END"
        t.pnl_pct    = round(pnl, 3)
        result.trades.append(t)

    return result


# ─────────────────────────────────────────────────────────────
# REPORT CSV
# ─────────────────────────────────────────────────────────────

def save_csv(result: BacktestResult, path: str = ""):
    if not result.trades:
        log.warning("Sin trades para exportar.")
        return
    rows = []
    for t in result.trades:
        rows.append({
            "symbol":      t.symbol,
            "direction":   t.direction,
            "entry_price": t.entry_price,
            "exit_price":  t.exit_price,
            "sl":          t.sl,
            "tp1":         t.tp1,
            "tp2":         t.tp2,
            "tp3":         t.tp3,
            "score":       t.score,
            "tp2_hit":     t.tp2_hit,
            "reason":      t.reason,
            "pnl_pct":     t.pnl_pct,
        })
    df = pd.DataFrame(rows)
    sym_clean = result.symbol.replace("/", "_").replace(":", "_")
    fname = path or f"backtest_{sym_clean}_{result.timeframe}_{result.n_days}d.csv"
    df.to_csv(fname, index=False)
    log.info(f"CSV guardado: {fname}")
    return fname


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

async def _main():
    parser = argparse.ArgumentParser(description="Backtester BitgetProBot")
    parser.add_argument("--symbol",   default="BTC/USDT:USDT")
    parser.add_argument("--tf",       default="15m")
    parser.add_argument("--days",     type=int, default=90)
    parser.add_argument("--leverage", type=int, default=5)
    parser.add_argument("--csv",      action="store_true", help="Exportar CSV")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    df = await _fetch_full_history(
        symbol     = args.symbol,
        tf         = args.tf,
        days       = args.days,
        api_key    = os.getenv("BITGET_API_KEY",    ""),
        api_secret = os.getenv("BITGET_API_SECRET", ""),
        passphrase = os.getenv("BITGET_PASSPHRASE", ""),
    )

    if df.empty:
        log.error("No se descargaron datos. Verifica credenciales y símbolo.")
        return

    result = _run_backtest(df, args.symbol, args.tf, args.days, args.leverage)
    print(result.summary())

    if args.csv:
        save_csv(result)


if __name__ == "__main__":
    asyncio.run(_main())
