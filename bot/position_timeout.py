#!/usr/bin/env python3
"""
position_timeout.py — Cierre automático de posiciones estancadas

Si una posición lleva más de TIMEOUT_HOURS sin alcanzar TP1
y el precio no ha movido MIN_MOVE_PCT desde la entrada,
se cierra automáticamente para liberar capital.

Config Railway:
  POSITION_TIMEOUT_ENABLED → default true
  POSITION_TIMEOUT_HOURS   → default 24 (horas)
  POSITION_MIN_MOVE_PCT    → default 0.3 (% mínimo de movimiento hacia TP1)
"""
from __future__ import annotations

import logging
import os
import time

log = logging.getLogger(__name__)

TIMEOUT_ENABLED  = os.getenv("POSITION_TIMEOUT_ENABLED", "true").lower() != "false"
TIMEOUT_HOURS    = float(os.getenv("POSITION_TIMEOUT_HOURS",   "24"))
MIN_MOVE_PCT     = float(os.getenv("POSITION_MIN_MOVE_PCT",    "0.3"))

TIMEOUT_SECS     = TIMEOUT_HOURS * 3600


def should_timeout(
    symbol: str,
    entry_price: float,
    current_price: float,
    tp1: float,
    side: str,                  # "long" / "short"
    opened_at: float,           # timestamp unix
    tp1_hit: bool = False,
) -> tuple[bool, str]:
    """
    Determina si una posición debe cerrarse por timeout.

    Returns:
        (True, reason) si se debe cerrar
        (False, "")    si no
    """
    if not TIMEOUT_ENABLED:
        return False, ""

    if tp1_hit:
        return False, ""  # ya llegó a TP1, dejar correr

    age_h = (time.time() - opened_at) / 3600.0
    if age_h < TIMEOUT_HOURS:
        return False, ""

    # Calcular progreso hacia TP1
    if entry_price <= 0 or tp1 <= 0:
        return False, ""

    tp1_dist = abs(tp1 - entry_price)
    if tp1_dist == 0:
        return False, ""

    if side == "long":
        move = current_price - entry_price
    else:
        move = entry_price - current_price

    progress_pct = (move / tp1_dist) * 100.0

    if progress_pct < MIN_MOVE_PCT:
        reason = (
            f"Timeout {age_h:.1f}h — progreso hacia TP1: {progress_pct:.1f}% "
            f"(min {MIN_MOVE_PCT}%)"
        )
        log.warning("[timeout] %s → cerrar: %s", symbol, reason)
        return True, reason

    return False, ""
