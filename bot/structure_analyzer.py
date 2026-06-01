#!/usr/bin/env python3
"""
structure_analyzer.py — Análisis de estructura de mercado (BOS / HH-HL / LH-LL)

Detecta:
  - Break of Structure (BOS): precio rompe último swing high/low con volumen
  - Higher Highs / Higher Lows (HH/HL) → tendencia alcista confirmada
  - Lower Highs / Lower Lows (LH/LL)  → tendencia bajista confirmada

Retorna un score de estructura: +1 (bull), -1 (bear), 0 (neutral)
Puede añadirse como bonus en signal_engine._compute_score()

Config Railway:
  STRUCTURE_ENABLED     → default true
  STRUCTURE_SWING_N     → nº de velas para detectar swing (default 5)
  STRUCTURE_VOL_CONFIRM → si true, BOS requiere volumen > 1.2x media (default true)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

STRUCTURE_ENABLED     = os.getenv("STRUCTURE_ENABLED",     "true").lower() != "false"
STRUCTURE_SWING_N     = int(os.getenv("STRUCTURE_SWING_N", "5"))
STRUCTURE_VOL_CONFIRM = os.getenv("STRUCTURE_VOL_CONFIRM", "true").lower() != "false"


def _find_swings(df: pd.DataFrame, n: int = 5):
    """Detecta swing highs y swing lows usando ventana de n velas."""
    h = df["high"].values
    l = df["low"].values
    swing_highs = []
    swing_lows  = []

    for i in range(n, len(h) - n):
        if h[i] == max(h[i-n:i+n+1]):
            swing_highs.append((i, h[i]))
        if l[i] == min(l[i-n:i+n+1]):
            swing_lows.append((i, l[i]))

    return swing_highs, swing_lows


def _check_hh_hl(swing_highs, swing_lows) -> int:
    """
    HH + HL → bull structure (+1)
    LH + LL → bear structure (-1)
    Else → 0
    """
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return 0

    # Últimos 2 swing highs y lows
    sh1, sh2 = swing_highs[-2][1], swing_highs[-1][1]
    sl1, sl2 = swing_lows[-2][1],  swing_lows[-1][1]

    hh = sh2 > sh1   # Higher High
    hl = sl2 > sl1   # Higher Low
    lh = sh2 < sh1   # Lower High
    ll = sl2 < sl1   # Lower Low

    if hh and hl:
        return 1
    if lh and ll:
        return -1
    return 0


def _check_bos(df: pd.DataFrame, swing_highs, swing_lows, direction: int) -> bool:
    """
    Break of Structure: precio actual rompe el último swing high (LONG)
    o swing low (SHORT) con volumen confirmado.
    """
    if not swing_highs or not swing_lows:
        return False

    close   = df["close"].iloc[-1]
    vol_now = df["volume"].iloc[-1]
    vol_ma  = df["volume"].rolling(20).mean().iloc[-1]
    vol_ok  = (vol_now >= vol_ma * 1.2) if STRUCTURE_VOL_CONFIRM else True

    if direction == 1:
        last_swing_high = swing_highs[-1][1]
        return close > last_swing_high and vol_ok
    elif direction == -1:
        last_swing_low = swing_lows[-1][1]
        return close < last_swing_low and vol_ok

    return False


def analyze_structure(df: pd.DataFrame, direction: int = 0) -> dict:
    """
    Analiza la estructura del mercado en el DataFrame dado.

    Args:
        df: DataFrame OHLCV (1h recomendado)
        direction: señal propuesta (+1 LONG, -1 SHORT, 0 cualquiera)

    Returns:
        dict con:
          'score'     → int (+1, 0, -1)
          'bos'       → bool (Break of Structure detectado)
          'hh_hl'     → int (+1, -1, 0)
          'last_sh'   → float (último swing high)
          'last_sl'   → float (último swing low)
    """
    result = {"score": 0, "bos": False, "hh_hl": 0, "last_sh": 0.0, "last_sl": 0.0}

    if not STRUCTURE_ENABLED:
        return result

    try:
        if df.empty or len(df) < STRUCTURE_SWING_N * 3:
            return result

        sh, sl = _find_swings(df, STRUCTURE_SWING_N)

        result["last_sh"] = sh[-1][1] if sh else 0.0
        result["last_sl"] = sl[-1][1] if sl else 0.0

        hh_hl = _check_hh_hl(sh, sl)
        result["hh_hl"] = hh_hl

        bos = _check_bos(df, sh, sl, direction)
        result["bos"] = bos

        # Score: HH/HL base + bonus BOS
        score = hh_hl
        if bos and direction != 0:
            score += direction   # +1 más si hay BOS en la dirección correcta

        result["score"] = max(-2, min(2, score))

    except Exception as e:
        log.debug("[structure] Error: %s", e)

    return result
