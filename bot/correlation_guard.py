#!/usr/bin/env python3
"""
correlation_guard.py — Guarda de correlación entre posiciones abiertas

En cripto, la mayoría de altcoins están altamente correlacionadas con BTC.
Abrir 5 posiciones SHORT simultáneas = 1 trade grande con más comisiones.

Reglas:
  1. MAX_SAME_DIRECTION: máximo N posiciones en la misma dirección (default 3)
  2. MAX_OPEN_POSITIONS: máximo M posiciones abiertas en total (default 5)
  3. BTC_HEDGE_CHECK: si BTC va contra la dirección propuesta, reducir size

Config Railway:
  CORR_MAX_SAME_DIR     → default 3
  CORR_MAX_OPEN         → default 5
  CORR_ENABLED          → default true
"""
from __future__ import annotations

import logging
import os
from typing import Dict, List

log = logging.getLogger(__name__)

CORR_ENABLED      = os.getenv("CORR_ENABLED",       "true").lower() != "false"
MAX_SAME_DIR      = int(os.getenv("CORR_MAX_SAME_DIR", "3"))
MAX_OPEN          = int(os.getenv("CORR_MAX_OPEN",     "5"))


def check_correlation(
    proposed_direction: str,           # "LONG" / "SHORT"
    open_positions: Dict[str, dict],   # {symbol: {"side": "long"/"short", ...}}
) -> tuple[bool, str]:
    """
    Comprueba si abrir una nueva posición viola las reglas de correlación.

    Returns:
        (True, "")           → OK, se puede abrir
        (False, reason)      → Rechazado
    """
    if not CORR_ENABLED:
        return True, ""

    total_open = len(open_positions)
    if total_open >= MAX_OPEN:
        reason = f"Máximo de posiciones abiertas alcanzado ({total_open}/{MAX_OPEN})"
        log.info("[correlation] Bloqueado: %s", reason)
        return False, reason

    dir_lower = proposed_direction.lower()
    same_dir  = sum(
        1 for p in open_positions.values()
        if str(p.get("side", "")).lower() == dir_lower
    )

    if same_dir >= MAX_SAME_DIR:
        reason = (
            f"Demasiadas posiciones {proposed_direction} abiertas "
            f"({same_dir}/{MAX_SAME_DIR}) — riesgo de correlación"
        )
        log.info("[correlation] Bloqueado: %s", reason)
        return False, reason

    return True, ""


def size_penalty_btc(
    proposed_direction: str,
    btc_trend: int,           # +1 long, -1 short, 0 neutral (de market_regime)
) -> float:
    """
    Penaliza el size si BTC va contra la dirección propuesta.
    Returns: multiplicador (0.7 si BTC contrario, 1.0 si alineado o neutral)
    """
    if not CORR_ENABLED:
        return 1.0

    dir_sign = 1 if proposed_direction == "LONG" else -1

    if btc_trend != 0 and btc_trend * dir_sign < 0:
        log.debug(
            "[correlation] BTC trend %+d contra %s → penalizar size ×0.7",
            btc_trend, proposed_direction,
        )
        return 0.7

    return 1.0
