#!/usr/bin/env python3
"""
daily_drawdown.py — Límite de drawdown diario

Si las pérdidas del día superan MAX_DAILY_DRAWDOWN_PCT del balance,
el bot deja de abrir nuevas posiciones hasta el reset diario (00:00 UTC).

Config Railway:
  MAX_DAILY_DRAWDOWN_PCT  → default 5.0  (5% del balance)
  DRAWDOWN_RESET_HOUR_UTC → default 0    (medianoche UTC)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

MAX_DD_PCT        = float(os.getenv("MAX_DAILY_DRAWDOWN_PCT", "5.0"))
RESET_HOUR_UTC    = int(os.getenv("DRAWDOWN_RESET_HOUR_UTC", "0"))


class DailyDrawdown:
    """
    Registra P&L realizado del día y bloquea nuevas entradas si se supera el límite.
    """

    def __init__(self) -> None:
        self._day_pnl: float  = 0.0      # USD neto del día
        self._balance_ref: float = 0.0   # Balance al inicio del día
        self._reset_day: int  = -1       # día UTC del último reset
        self._blocked: bool   = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_balance_ref(self, balance: float) -> None:
        """Llamar al inicio del día con el balance actual."""
        self._balance_ref = balance
        log.info("[drawdown] Balance ref = $%.2f", balance)

    def record_trade(self, pnl_usd: float) -> None:
        """Registrar resultado de un trade cerrado."""
        self._check_reset()
        self._day_pnl += pnl_usd
        pct = self._drawdown_pct()
        log.info(
            "[drawdown] P&L hoy: $%.2f (%.2f%%) · Límite: %.1f%%",
            self._day_pnl, pct, MAX_DD_PCT,
        )
        if pct <= -MAX_DD_PCT and not self._blocked:
            self._blocked = True
            log.warning(
                "[drawdown] 🛑 Drawdown diario %.2f%% supera límite %.1f%% — "
                "bloqueando nuevas entradas hasta 00:00 UTC",
                pct, MAX_DD_PCT,
            )

    def is_blocked(self) -> bool:
        self._check_reset()
        return self._blocked

    def day_pnl(self) -> float:
        return self._day_pnl

    def drawdown_pct(self) -> float:
        return self._drawdown_pct()

    def summary(self) -> str:
        icon = "🛑" if self._blocked else ("🟡" if self._drawdown_pct() < -MAX_DD_PCT * 0.7 else "✅")
        return (
            f"{icon} Drawdown hoy: ${self._day_pnl:+.2f} "
            f"({self._drawdown_pct():+.2f}%) · Límite: {MAX_DD_PCT:.1f}%"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _drawdown_pct(self) -> float:
        if self._balance_ref <= 0:
            return 0.0
        return (self._day_pnl / self._balance_ref) * 100.0

    def _check_reset(self) -> None:
        today = datetime.now(timezone.utc).day
        if today != self._reset_day:
            prev_pnl = self._day_pnl
            self._day_pnl   = 0.0
            self._blocked   = False
            self._reset_day = today
            if prev_pnl != 0:
                log.info(
                    "[drawdown] Reset diario — P&L ayer: $%.2f", prev_pnl
                )


# Singleton
daily_drawdown = DailyDrawdown()
