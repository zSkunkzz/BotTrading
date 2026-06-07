# ============================================================
# bot/backtester.py  —  Backtesting vectorizado + Optimizador Walk-Forward
# Uso básico:
#   python -m bot.backtester --symbol BTC/USDT:USDT --days 90
#   python -m bot.backtester --symbol BTC/USDT:USDT --days 90 --tf 1h --csv
#
# Optimizador Walk-Forward:
#   python -m bot.backtester --symbol BTC/USDT:USDT --days 180 --optimize
#   python -m bot.backtester --symbol ETH/USDT:USDT --days 360 --optimize --folds 5
#
# El optimizador hace grid search sobre (MIN_SCORE, MIN_RR, MIN_SCORE_RATIO)
# usando walk-forward para evitar overfitting. Exporta ranking CSV con todos
# los resultados y muestra los top-5 parámetros óptimos con Sharpe ratio.
# ============================================================

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from dataclasses import dataclass, field
from itertools import product
from typing import Dict, List, Optional, Tuple

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
    # Scaling out: fracción cerrada en TP1
    scaled_out:  bool  = False


@dataclass
class BacktestResult:
    symbol:       str
    timeframe:    str
    n_days:       int
    trades:       list[Trade] = field(default_factory=list)
    leverage:     int = 5
    params:       Dict = field(default_factory=dict)

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

    @property
    def sharpe(self) -> float:
        """Sharpe ratio anualizado (asume retornos por trade independientes)."""
        if self.n_trades < 3:
            return 0.0
        returns = np.array([t.pnl_pct for t in self.trades])
        std = returns.std()
        if std == 0:
            return 0.0
        # Anualizado asumiendo ~252 días de trading
        trades_per_year = self.n_trades * (252 / max(self.n_days, 1))
        return float((returns.mean() / std) * np.sqrt(trades_per_year))

    @property
    def calmar(self) -> float:
        """Calmar ratio: total_pnl / |max_dd|. Mayor = mejor."""
        dd = abs(self.max_dd)
        return self.total_pnl / dd if dd > 0 else float("inf")

    def summary(self) -> str:
        sep = "═" * 52
        lines = [
            sep,
            f"  BACKTEST  {self.symbol}  {self.timeframe}  ({self.n_days}d)",
        ]
        if self.params:
            p = self.params
            lines.append(
                f"  Params: score≥{p.get('min_score','?')}  "
                f"rr≥{p.get('min_rr','?')}  "
                f"ratio≥{p.get('min_ratio','?')}"
            )
        lines += [
            sep,
            f"  Trades       : {self.n_trades}",
            f"  Win Rate     : {self.win_rate:.1f}%  ({self.wins}W / {self.losses}L)",
            f"  PnL total    : {self.total_pnl:+.2f}%",
            f"  PnL medio    : {self.avg_pnl:+.2f}% / trade",
            f"  Max Drawdown : {self.max_dd:.2f}%",
            f"  Profit Factor: {self.profit_factor:.2f}",
            f"  Expectancy   : {self.expectancy:+.2f}% / trade",
            f"  Sharpe       : {self.sharpe:.2f}",
            f"  Calmar       : {self.calmar:.2f}",
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
# MOTOR DE BACKTEST (con scaling out simulado)
# ─────────────────────────────────────────────────────────────

def _run_backtest(
    df: pd.DataFrame,
    symbol: str,
    tf: str,
    days: int,
    leverage: int = 5,
    warmup: int = 200,
    tp2_partial_ratio: float = 0.5,
    # Parámetros de señal — permiten override en el optimizador
    min_score: int = MIN_SCORE,
    min_rr: float = MIN_RR,
    min_score_ratio: float = 0.62,
    # Scaling out: simula cerrar tp2_partial_ratio en TP1 y mover SL a BE
    scale_out_enabled: bool = True,
    scale_out_ratio: float = 0.5,
) -> BacktestResult:
    result = BacktestResult(
        symbol=symbol, timeframe=tf, n_days=days, leverage=leverage,
        params={"min_score": min_score, "min_rr": min_rr, "min_ratio": min_score_ratio},
    )
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
                pnl_raw = (exit_p - t.entry_price) / t.entry_price * 100 * leverage
                if not is_long:
                    pnl_raw = -pnl_raw
                # Si ya hicimos scale out en TP1, el SL de la mitad restante es BE → PnL=0
                if t.scaled_out:
                    pnl = pnl_raw * (1 - scale_out_ratio) + abs(t.tp1 - t.entry_price) / t.entry_price * 100 * leverage * scale_out_ratio
                else:
                    pnl = pnl_raw
                t.exit_price = exit_p
                t.exit_idx   = i
                t.reason     = "SL" if not t.scaled_out else "SL_AFTER_TP1"
                t.pnl_pct    = round(pnl, 3)
                result.trades.append(t)
                in_trade = None
                continue

            # ── SCALING OUT: TP1 hit → cerrar scale_out_ratio, SL→BE ──────────
            if scale_out_enabled and not t.scaled_out and t.tp1:
                tp1_hit = (hi >= t.tp1) if is_long else (lo <= t.tp1)
                if tp1_hit:
                    t.scaled_out = True
                    # SL se mueve a entry (breakeven)
                    t.sl = t.entry_price
                    # Continuar el trade con el tp2/tp3 restante
                    continue

            # TP2 parcial (sin scaling out, modo antiguo)
            if not scale_out_enabled and t.tp2 and not t.tp2_hit:
                tp2_hit = (hi >= t.tp2) if is_long else (lo <= t.tp2)
                if tp2_hit:
                    t.tp2_hit = True
                    t.sl      = t.entry_price

            # TP3 (objetivo final si hay scaling out o tp3 directo)
            target_tp = t.tp2 if scale_out_enabled else t.tp3
            if target_tp:
                tgt_hit = (hi >= target_tp) if is_long else (lo <= target_tp)
                if tgt_hit:
                    exit_p = target_tp
                    pnl_full = abs(exit_p - t.entry_price) / t.entry_price * 100 * leverage
                    if scale_out_enabled and t.scaled_out:
                        # PnL = tp1 profit en scale_out_ratio + tp2 profit en (1-scale_out_ratio)
                        pnl_tp1 = abs(t.tp1 - t.entry_price) / t.entry_price * 100 * leverage
                        pnl = pnl_tp1 * scale_out_ratio + pnl_full * (1 - scale_out_ratio)
                    elif not scale_out_enabled and t.tp2_hit and t.tp2:
                        pnl_tp2 = abs(t.tp2 - t.entry_price) / t.entry_price * 100 * leverage
                        pnl = pnl_tp2 * tp2_partial_ratio + pnl_full * (1 - tp2_partial_ratio)
                    else:
                        pnl = pnl_full
                    t.exit_price = exit_p
                    t.exit_idx   = i
                    t.reason     = "TP2" if scale_out_enabled else "TP3"
                    t.pnl_pct    = round(pnl, 3)
                    result.trades.append(t)
                    in_trade = None
                    continue

            # TP1 sin scaling (fallback si tp2 no existe)
            if not scale_out_enabled and t.tp1 and not t.tp2:
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

            continue

        # ── Buscar señal ──────────────────────────────────────────
        window = df.iloc[max(0, i - 199): i + 1]
        if len(window) < 55:
            continue

        s = _analyze_tf(window)
        if not s:
            continue

        score_l = sum(max(0,  s.get(k, 0)) for k in
                      ("ema_trend", "macd", "rsi", "supertrend", "stoch", "volume", "bb"))
        score_s = sum(max(0, -s.get(k, 0)) for k in
                      ("ema_trend", "macd", "rsi", "supertrend", "stoch", "volume", "bb"))
        score = max(score_l, score_s)
        direction = "LONG" if score_l >= score_s else "SHORT"

        # Filtro parametrizable
        if score < min_score:
            continue

        max_sc = len(["ema_trend", "macd", "rsi", "supertrend", "stoch", "volume", "bb"])
        if max_sc > 0 and score / max_sc < min_score_ratio:
            continue

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
        if rr < min_rr:
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
        pnl_raw = (exit_p - t.entry_price) / t.entry_price * 100 * leverage
        if not is_long:
            pnl_raw = -pnl_raw
        if t.scaled_out:
            pnl_tp1 = abs(t.tp1 - t.entry_price) / t.entry_price * 100 * leverage
            pnl = pnl_tp1 * scale_out_ratio + pnl_raw * (1 - scale_out_ratio)
        else:
            pnl = pnl_raw
        t.exit_price = exit_p
        t.exit_idx   = len(df) - 1
        t.reason     = "END"
        t.pnl_pct    = round(pnl, 3)
        result.trades.append(t)

    return result


# ─────────────────────────────────────────────────────────────
# OPTIMIZADOR WALK-FORWARD
# ─────────────────────────────────────────────────────────────

# Grid de parámetros a explorar
_OPT_MIN_SCORES  = [6, 7, 8, 9, 10]
_OPT_MIN_RRS     = [1.2, 1.5, 1.8, 2.0, 2.2]
_OPT_MIN_RATIOS  = [0.50, 0.55, 0.62, 0.68, 0.72]


@dataclass
class OptResult:
    min_score:     int
    min_rr:        float
    min_ratio:     float
    sharpe:        float
    total_pnl:     float
    win_rate:      float
    n_trades:      int
    max_dd:        float
    profit_factor: float
    fold_sharpes:  List[float] = field(default_factory=list)

    @property
    def sharpe_consistency(self) -> float:
        """Desviación estándar de Sharpe entre folds — menor = más robusto."""
        return float(np.std(self.fold_sharpes)) if len(self.fold_sharpes) > 1 else 0.0


def _walk_forward_optimize(
    df: pd.DataFrame,
    symbol: str,
    tf: str,
    total_days: int,
    leverage: int = 5,
    folds: int = 3,
) -> List[OptResult]:
    """
    Walk-Forward optimization:
      - Divide el histórico en `folds` ventanas temporales NO solapadas.
      - Para cada combinación de parámetros, corre el backtest en cada fold.
      - Métrica principal: media de Sharpe ratio entre folds.
      - Métrica de robustez: std de Sharpe entre folds (menor = mejor).
      - Descarta parámetros con n_trades < 5 en algún fold (sin estadística).

    Retorna lista de OptResult ordenada de mejor a peor Sharpe.
    """
    n = len(df)
    fold_size = n // folds
    log.info(
        "[Optimizer] Walk-forward: %d folds × %d velas (~%d días c/u)",
        folds, fold_size, total_days // folds,
    )

    total_combinations = len(_OPT_MIN_SCORES) * len(_OPT_MIN_RRS) * len(_OPT_MIN_RATIOS)
    log.info("[Optimizer] Grid: %d combinaciones × %d folds = %d backtests",
             total_combinations, folds, total_combinations * folds)

    results: List[OptResult] = []
    done = 0

    for min_score, min_rr, min_ratio in product(
        _OPT_MIN_SCORES, _OPT_MIN_RRS, _OPT_MIN_RATIOS
    ):
        fold_results: List[BacktestResult] = []
        skip = False

        for fold_idx in range(folds):
            start = fold_idx * fold_size
            end   = start + fold_size if fold_idx < folds - 1 else n
            fold_df = df.iloc[start:end]
            fold_days = max(1, (end - start) * (total_days // n))

            if len(fold_df) < 300:  # muy pocos datos
                skip = True
                break

            r = _run_backtest(
                df=fold_df,
                symbol=symbol,
                tf=tf,
                days=fold_days,
                leverage=leverage,
                warmup=min(200, len(fold_df) // 3),
                min_score=min_score,
                min_rr=min_rr,
                min_score_ratio=min_ratio,
                scale_out_enabled=True,
            )

            if r.n_trades < 5:
                skip = True
                break

            fold_results.append(r)

        done += 1
        if done % 25 == 0:
            log.info("[Optimizer] Progreso: %d/%d combinaciones", done, total_combinations)

        if skip or not fold_results:
            continue

        fold_sharpes   = [r.sharpe for r in fold_results]
        mean_sharpe    = float(np.mean(fold_sharpes))
        mean_pnl       = float(np.mean([r.total_pnl for r in fold_results]))
        mean_wr        = float(np.mean([r.win_rate   for r in fold_results]))
        mean_trades    = int(np.mean([r.n_trades    for r in fold_results]))
        mean_dd        = float(np.mean([r.max_dd     for r in fold_results]))
        mean_pf        = float(np.mean([
            r.profit_factor for r in fold_results
            if r.profit_factor != float("inf")
        ]) if any(r.profit_factor != float("inf") for r in fold_results) else 0.0)

        results.append(OptResult(
            min_score=min_score,
            min_rr=min_rr,
            min_ratio=min_ratio,
            sharpe=round(mean_sharpe, 3),
            total_pnl=round(mean_pnl, 2),
            win_rate=round(mean_wr, 1),
            n_trades=mean_trades,
            max_dd=round(mean_dd, 2),
            profit_factor=round(mean_pf, 2),
            fold_sharpes=fold_sharpes,
        ))

    results.sort(key=lambda x: x.sharpe, reverse=True)
    log.info("[Optimizer] Completado: %d combinaciones válidas de %d", len(results), total_combinations)
    return results


def _print_optimizer_results(results: List[OptResult], top_n: int = 5) -> None:
    sep = "═" * 72
    print(f"\n{sep}")
    print(f"  OPTIMIZADOR WALK-FORWARD — TOP {top_n} PARÁMETROS")
    print(sep)
    print(f"  {'#':<3} {'Score':>5} {'MinRR':>6} {'Ratio':>6} "
          f"{'Sharpe':>7} {'PnL%':>7} {'WR%':>6} "
          f"{'Trades':>7} {'MaxDD%':>7} {'PF':>5} {'Consistencia':>12}")
    print("  " + "─" * 70)
    for rank, r in enumerate(results[:top_n], 1):
        print(
            f"  {rank:<3} {r.min_score:>5} {r.min_rr:>6.2f} {r.min_ratio:>6.2f} "
            f"{r.sharpe:>7.2f} {r.total_pnl:>+7.1f} {r.win_rate:>6.1f} "
            f"{r.n_trades:>7} {r.max_dd:>7.1f} {r.profit_factor:>5.2f} "
            f"{r.sharpe_consistency:>12.3f}"
        )
    print(sep)

    if results:
        best = results[0]
        print("\n  📌 CONFIGURACIÓN RECOMENDADA (mejor Sharpe)")
        print(f"     MIN_SIGNAL_SCORE={best.min_score}")
        print(f"     MIN_RR_REQUIRED={best.min_rr}")
        print(f"     MIN_SCORE_RATIO={best.min_ratio}")
        print("")
        print("  Añade estas variables a tu .env y reinicia el bot.")
    print(f"{sep}\n")


def save_optimizer_csv(results: List[OptResult], symbol: str, tf: str, days: int) -> str:
    if not results:
        log.warning("[Optimizer] Sin resultados para exportar.")
        return ""
    rows = []
    for r in results:
        rows.append({
            "min_score":        r.min_score,
            "min_rr":           r.min_rr,
            "min_ratio":        r.min_ratio,
            "sharpe":           r.sharpe,
            "total_pnl_pct":    r.total_pnl,
            "win_rate_pct":     r.win_rate,
            "n_trades":         r.n_trades,
            "max_dd_pct":       r.max_dd,
            "profit_factor":    r.profit_factor,
            "sharpe_consistency": r.sharpe_consistency,
            "fold_sharpes":     str(r.fold_sharpes),
        })
    out_df = pd.DataFrame(rows)
    sym_clean = symbol.replace("/", "_").replace(":", "_")
    fname = f"optimizer_{sym_clean}_{tf}_{days}d.csv"
    out_df.to_csv(fname, index=False)
    log.info(f"[Optimizer] CSV guardado: {fname}")
    return fname


# ─────────────────────────────────────────────────────────────
# REPORT CSV (backtest normal)
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
            "scaled_out":  t.scaled_out,
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
    parser = argparse.ArgumentParser(description="Backtester + Optimizador BotTrading")
    parser.add_argument("--symbol",   default="BTC/USDT:USDT")
    parser.add_argument("--tf",       default="15m")
    parser.add_argument("--days",     type=int, default=90)
    parser.add_argument("--leverage", type=int, default=5)
    parser.add_argument("--csv",      action="store_true", help="Exportar CSV")
    # Optimizador
    parser.add_argument(
        "--optimize", action="store_true",
        help="Activar optimizador walk-forward de thresholds",
    )
    parser.add_argument(
        "--folds", type=int, default=3,
        help="Número de folds para walk-forward (default 3, recomendado 3-5)",
    )
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

    if args.optimize:
        log.info("[Optimizer] Iniciando walk-forward optimization...")
        opt_results = _walk_forward_optimize(
            df=df,
            symbol=args.symbol,
            tf=args.tf,
            total_days=args.days,
            leverage=args.leverage,
            folds=args.folds,
        )
        _print_optimizer_results(opt_results, top_n=5)
        if args.csv:
            save_optimizer_csv(opt_results, args.symbol, args.tf, args.days)
    else:
        result = _run_backtest(df, args.symbol, args.tf, args.days, args.leverage)
        print(result.summary())
        if args.csv:
            save_csv(result)


if __name__ == "__main__":
    asyncio.run(_main())
