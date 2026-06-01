#!/usr/bin/env python3
"""
market_regime.py — Detección de régimen de mercado (TRENDING / RANGING / VOLATILE)

MEJORAS v2:
  - verify_regime_gate(): función de gate bloqueante para decision_engine.
    Si MARKET_REGIME_GATE=true (default) y el régimen es RANGING → bloquea entrada.
  - El régimen se calcula sobre 1h OHLCV (más estable que 15m).

Config Railway:
  MARKET_REGIME_GATE     → default true  (false = solo informativo, no bloquea)
  REGIME_ADX_TREND       → ADX mínimo para TRENDING (default 25)
  REGIME_ADX_RANGING     → ADX máximo para RANGING (default 20)
  REGIME_BB_WIDTH_FACTOR → factor ancho BB para VOLATILE (default 2.0)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import numpy as np
import pandas as pd

try:
    import ta as ta_lib
except ImportError:
    ta_lib = None

log = logging.getLogger(__name__)

MARKET_REGIME_GATE     = os.getenv("MARKET_REGIME_GATE",     "true").lower() != "false"
REGIME_ADX_TREND       = float(os.getenv("REGIME_ADX_TREND",       "25"))
REGIME_ADX_RANGING     = float(os.getenv("REGIME_ADX_RANGING",     "20"))
REGIME_BB_WIDTH_FACTOR = float(os.getenv("REGIME_BB_WIDTH_FACTOR", "2.0"))


def detect_regime(df: pd.DataFrame) -> str:
    """
    Detecta el régimen de mercado actual.

    Returns:
        'TRENDING' | 'RANGING' | 'VOLATILE' | 'UNKNOWN'
    """
    if ta_lib is None or df.empty or len(df) < 30:
        return "UNKNOWN"

    try:
        # ADX
        adx_ind = ta_lib.trend.ADXIndicator(
            df["high"], df["low"], df["close"], window=14
        )
        adx = float(adx_ind.adx().iloc[-1])

        # Bollinger Band Width (BBW) normalizado
        bb = ta_lib.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        bbw     = float((bb.bollinger_hband().iloc[-1] - bb.bollinger_lband().iloc[-1])
                        / bb.bollinger_mavg().iloc[-1])
        bbw_ma  = float((bb.bollinger_hband() - bb.bollinger_lband())
                        / bb.bollinger_mavg()).rolling(50).mean().iloc[-1]

        if np.isnan(adx):
            return "UNKNOWN"

        # VOLATILE: BB muy expandido respecto a su media histórica
        if not np.isnan(bbw_ma) and bbw > bbw_ma * REGIME_BB_WIDTH_FACTOR:
            return "VOLATILE"

        # TRENDING: ADX alto
        if adx >= REGIME_ADX_TREND:
            return "TRENDING"

        # RANGING: ADX bajo
        if adx < REGIME_ADX_RANGING:
            return "RANGING"

        return "TRENDING"  # zona intermedia: tratar como tendencia débil

    except Exception as e:
        log.debug("[regime] detect_regime error: %s", e)
        return "UNKNOWN"


def verify_regime_gate(df: pd.DataFrame, symbol: str = "") -> tuple[bool, str]:
    """
    Gate bloqueante para decision_engine.

    Returns:
        (allowed, reason)
        allowed=True  → se puede abrir posición
        allowed=False → régimen desfavorable, bloquear entrada
    """
    if not MARKET_REGIME_GATE:
        return True, ""

    regime = detect_regime(df)

    if regime == "RANGING":
        reason = f"market_regime=RANGING (ADX bajo) — entrada bloqueada"
        log.info("[regime] %s %s", symbol, reason)
        return False, reason

    log.debug("[regime] %s regime=%s → permitido", symbol, regime)
    return True, ""
