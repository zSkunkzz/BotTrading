#!/usr/bin/env python3
"""
auto_backtest.py — Backtest automático de parámetros actuales

Valida si los parámetros actuales (ATR_MULT_SL, MIN_SCORE, TP_MULT, etc.)
habrían sido rentables en las últimas N velas históricas.

Se ejecuta al arranque y cada AUTO_BACKTEST_HOURS horas.
Los resultados se loguean y se pueden enviar por Telegram.

Config Railway:
  AUTO_BACKTEST_ENABLED   → default false (activar con cuidado, consume CPU)
  AUTO_BACKTEST_HOURS     → default 24 (cada cuántas horas recalcular)
  AUTO_BACKTEST_CANDLES   → default 500 (velas históricas a evaluar)
  AUTO_BACKTEST_MIN_WR    → default 0.45 (win rate mínimo esperado)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger(__name__)

BT_ENABLED    = os.getenv("AUTO_BACKTEST_ENABLED", "false").lower() == "true"
BT_HOURS      = float(os.getenv("AUTO_BACKTEST_HOURS",   "24"))
BT_CANDLES    = int(os.getenv("AUTO_BACKTEST_CANDLES",   "500"))
BT_MIN_WR     = float(os.getenv("AUTO_BACKTEST_MIN_WR",  "0.45"))


@dataclass
class BacktestResult:
    symbol: str
    trades:     int   = 0
    wins:       int   = 0
    losses:     int   = 0
    gross_pnl:  float = 0.0
    max_dd:     float = 0.0
    win_rate:   float = 0.0
    profit_factor: float = 0.0


@dataclass
class BacktestSummary:
    timestamp: float = field(default_factory=time.time)
    results: List[BacktestResult] = field(default_factory=list)
    avg_win_rate: float = 0.0
    total_trades: int   = 0
    passed: bool        = False

    def format(self) -> str:
        icon = "✅" if self.passed else "⚠️"
        lines = [
            f"{icon} *Auto-Backtest* · {self.total_trades} trades · "
            f"WR avg {self.avg_win_rate*100:.1f}%",
        ]
        for r in sorted(self.results, key=lambda x: -x.trades)[:5]:
            wr = f"{r.win_rate*100:.0f}%"
            pf = f"{r.profit_factor:.2f}"
            lines.append(
                f"  {r.symbol}: {r.trades}t · WR {wr} · PF {pf} · "
                f"PnL ${r.gross_pnl:+.1f}"
            )
        return "\n".join(lines)


class AutoBacktester:
    def __init__(self) -> None:
        self._last_run: float = 0.0
        self._last_summary: Optional[BacktestSummary] = None
        self._lock = asyncio.Lock()

    def last_summary(self) -> Optional[BacktestSummary]:
        return self._last_summary

    async def maybe_run(self, exch=None, symbols: Optional[List[str]] = None) -> None:
        """Ejecuta el backtest si ha pasado suficiente tiempo."""
        if not BT_ENABLED:
            return
        now = time.monotonic()
        if now - self._last_run < BT_HOURS * 3600:
            return
        async with self._lock:
            if now - self._last_run < BT_HOURS * 3600:
                return
            try:
                summary = await self._run(exch, symbols or ["BTC", "ETH", "SOL"])
                self._last_summary = summary
                self._last_run = time.monotonic()
                log.info("[backtest] %s", summary.format())

                if not summary.passed:
                    log.warning(
                        "[backtest] ⚠️ Win rate %.1f%% por debajo del mínimo %.0f%% — "
                        "considera ajustar MIN_SCORE o ATR_MULT_SL",
                        summary.avg_win_rate * 100, BT_MIN_WR * 100,
                    )

                try:
                    from bot.notifier import send_telegram
                    await send_telegram(summary.format())
                except Exception:
                    pass

            except Exception as e:
                log.error("[backtest] Error: %s", e)

    async def _run(
        self,
        exch,
        symbols: List[str],
    ) -> BacktestSummary:
        import numpy as np
        from bot.signal_engine import _fetch_ohlcv, _analyze_tf, _compute_score
        from bot.signal_engine import ATR_MULT_SL, TP1_MULT, TP2_MULT, MIN_SCORE, MIN_RR

        results = []
        tasks = [self._backtest_symbol(exch, sym, ATR_MULT_SL, TP1_MULT, TP2_MULT, MIN_SCORE, MIN_RR) for sym in symbols]
        done = await asyncio.gather(*tasks, return_exceptions=True)

        for r in done:
            if isinstance(r, BacktestResult):
                results.append(r)

        total_trades = sum(r.trades for r in results)
        avg_wr = (
            sum(r.win_rate * r.trades for r in results) / total_trades
            if total_trades > 0 else 0.0
        )

        return BacktestSummary(
            results=results,
            avg_win_rate=avg_wr,
            total_trades=total_trades,
            passed=avg_wr >= BT_MIN_WR,
        )

    async def _backtest_symbol(
        self, exch, symbol: str,
        atr_mult: float, tp1_m: float, tp2_m: float,
        min_score: int, min_rr: float,
    ) -> BacktestResult:
        import numpy as np
        import ta
        from bot.signal_engine import _fetch_ohlcv_hl, _analyze_tf, _compute_score

        res = BacktestResult(symbol=symbol)

        try:
            df15 = await _fetch_ohlcv_hl(symbol, "15m", BT_CANDLES)
            df1h = await _fetch_ohlcv_hl(symbol, "1h",  BT_CANDLES)
            df4h = await _fetch_ohlcv_hl(symbol, "4h",  BT_CANDLES)

            if df15.empty or len(df15) < 100:
                return res

            # Walk-forward simple: ventana deslizante de 55 velas
            wins_pnl  = []
            loses_pnl = []

            for i in range(55, len(df15) - 20):
                w15 = df15.iloc[i-55:i]
                w1h = df1h.iloc[max(0, i-55):i] if not df1h.empty else w15
                w4h = df4h.iloc[max(0, i-55):i] if not df4h.empty else w15

                s15 = _analyze_tf(w15)
                s1h = _analyze_tf(w1h) if len(w1h) >= 55 else {}
                s4h = _analyze_tf(w4h) if len(w4h) >= 55 else {}

                score, direction = _compute_score(s4h, s1h, s15)
                if score < min_score:
                    continue

                # ATR & niveles
                try:
                    atr = float(
                        ta.volatility.AverageTrueRange(
                            w15["high"], w15["low"], w15["close"], window=14
                        ).average_true_range().iloc[-1]
                    )
                except Exception:
                    atr = float(w15["close"].iloc[-1]) * 0.005

                entry = float(w15["close"].iloc[-1])
                risk  = atr * atr_mult

                if direction == "LONG":
                    sl  = entry - risk
                    tp1 = entry + risk * tp1_m
                else:
                    sl  = entry + risk
                    tp1 = entry - risk * tp1_m

                rr = abs(tp1 - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
                if rr < min_rr:
                    continue

                # Simular resultado en las siguientes 20 velas
                future = df15.iloc[i:i+20]
                hit_tp1 = False
                hit_sl  = False
                exit_pnl = 0.0

                for _, row in future.iterrows():
                    if direction == "LONG":
                        if row["low"] <= sl:
                            exit_pnl = -(risk)  # pérdida
                            hit_sl = True
                            break
                        if row["high"] >= tp1:
                            exit_pnl = risk * tp1_m
                            hit_tp1 = True
                            break
                    else:
                        if row["high"] >= sl:
                            exit_pnl = -(risk)
                            hit_sl = True
                            break
                        if row["low"] <= tp1:
                            exit_pnl = risk * tp1_m
                            hit_tp1 = True
                            break

                res.trades += 1
                if hit_tp1:
                    res.wins += 1
                    wins_pnl.append(exit_pnl)
                elif hit_sl:
                    res.losses += 1
                    loses_pnl.append(abs(exit_pnl))

                res.gross_pnl += exit_pnl

            if res.trades > 0:
                res.win_rate = res.wins / res.trades
                total_wins  = sum(wins_pnl) if wins_pnl else 0.0
                total_loss  = sum(loses_pnl) if loses_pnl else 0.01
                res.profit_factor = total_wins / max(total_loss, 0.01)

                # Max drawdown
                equity = np.cumsum(
                    [p for p in (wins_pnl + [-x for x in loses_pnl])]
                )
                if len(equity) > 0:
                    rolling_max = np.maximum.accumulate(equity)
                    dd = (equity - rolling_max)
                    res.max_dd = float(dd.min()) if len(dd) > 0 else 0.0

        except Exception as e:
            log.debug("[backtest] %s error: %s", symbol, e)

        return res


# Singleton
auto_backtest = AutoBacktester()
