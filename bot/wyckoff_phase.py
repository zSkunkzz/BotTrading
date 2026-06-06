#!/usr/bin/env python3
"""
wyckoff_phase.py — Detección de fase de ciclo de mercado (Wyckoff simplificado).

PROBLEMA (gap #2):
  Los setups TENDENCIA/BREAKOUT/REVERSAL no distinguen la fase del par en el
  ciclo mayor. Un BREAKOUT en fase de distribución es una trampa (spring/UTAD),
  pero el bot lo puntúa igual que un breakout en markup. Mismo problema para
  REVERSAL en acumulación vs markdown.

SOLUCIÓN:
  WyckoffPhase detecta la fase probable usando velas 4h/1d y expone:
    - detect_phase(df_4h, df_1d)  → str con la fase detectada
    - setup_allowed(setup, direction, phase) → (bool, reason)
    - phase_score_adj(setup, direction, phase) → int ⎔score (-2 a +2)

FASES DETECTADAS:
  ACCUMULATION  — consolidación tras caída prolongada con volumen decreciente
  MARKUP        — tendencia alcista con estructura HH/HL
  DISTRIBUTION  — consolidación tras subida prolongada con volumen decreciente
  MARKDOWN      — tendencia bajista con estructura LL/LH
  UNCERTAIN     — no encaja en ninguna fase clara

REGLAS DE COMPATIBILIDAD setup ↔ fase:
  BREAKOUT  LONG  en DISTRIBUTION  → penalización -2 (trampa UTAD)
  BREAKOUT  SHORT en ACCUMULATION  → penalización -2 (trampa spring)
  TENDENCIA LONG  en MARKDOWN      → penalización -1 (contratendencia)
  TENDENCIA SHORT en MARKUP        → penalización -1 (contratendencia)
  REVERSAL  en la fase correcta    → bonus +2
  Setup alineado con fase          → bonus +1

Config Railway:
  WYCKOFF_ENABLED         → activar (default true)
  WYCKOFF_LOOKBACK_BARS   → barras 4h para calcular estructura (default 60)
  WYCKOFF_VOL_WINDOW      → ventana de volumen para detectar compresión (default 20)
"""
from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ENABLED       = os.getenv("WYCKOFF_ENABLED", "true").lower() != "false"
_LOOKBACK      = int(os.getenv("WYCKOFF_LOOKBACK_BARS", "60"))
_VOL_WINDOW    = int(os.getenv("WYCKOFF_VOL_WINDOW", "20"))


def detect_phase(df_4h: Optional[pd.DataFrame],
                 df_1d:  Optional[pd.DataFrame] = None) -> str:
    """
    Detecta la fase Wyckoff del par usando velas 4h (y opcionalmente 1d).

    Returns:
        'ACCUMULATION' | 'MARKUP' | 'DISTRIBUTION' | 'MARKDOWN' | 'UNCERTAIN'
    """
    if not _ENABLED:
        return "UNCERTAIN"

    try:
        df = _prepare(df_4h)
        if df is None or len(df) < _LOOKBACK // 2:
            return "UNCERTAIN"

        closes  = df["close"].values
        highs   = df["high"].values
        lows    = df["low"].values
        vols    = df["volume"].values

        # ── 1. Dirección del precio (EMA20 vs EMA50 en 4h) ──────────────────
        ema20 = _ema(closes, 20)
        ema50 = _ema(closes, 50) if len(closes) >= 50 else ema20
        trending_up   = bool(ema20[-1] > ema50[-1] * 1.005)
        trending_down = bool(ema20[-1] < ema50[-1] * 0.995)

        # ── 2. Estructura HH/HL o LL/LH en últimos N swings ────────────────
        n = min(len(closes), _LOOKBACK)
        pivot_highs = _swing_highs(highs[-n:], window=5)
        pivot_lows  = _swing_lows (lows[-n:],  window=5)

        hh = (len(pivot_highs) >= 2 and
              pivot_highs[-1] > pivot_highs[-2])
        hl = (len(pivot_lows)  >= 2 and
              pivot_lows[-1]  > pivot_lows[-2])
        ll = (len(pivot_lows)  >= 2 and
              pivot_lows[-1]  < pivot_lows[-2])
        lh = (len(pivot_highs) >= 2 and
              pivot_highs[-1] < pivot_highs[-2])

        # ── 3. Compresión de volumen (posible fase de consolidación) ───────
        vol_compressed = False
        if len(vols) >= _VOL_WINDOW * 2:
            recent_vol = float(np.mean(vols[-_VOL_WINDOW:]))
            prior_vol  = float(np.mean(vols[-_VOL_WINDOW * 2:-_VOL_WINDOW]))
            vol_compressed = recent_vol < prior_vol * 0.75

        # ── 4. Determinar fase ───────────────────────────────────────────
        if trending_up and hh and hl:
            return "MARKUP"

        if trending_down and ll and lh:
            return "MARKDOWN"

        # Consolidación con volumen comprimido
        if vol_compressed:
            # Si la estructura previa era alcista → posible DISTRIBUTION
            # Si era bajista → posible ACCUMULATION
            if trending_up or hh:
                return "DISTRIBUTION"
            if trending_down or ll:
                return "ACCUMULATION"

        # Fallback → MARKUP si EMA apunta arriba, MARKDOWN si apunta abajo
        if trending_up:
            return "MARKUP"
        if trending_down:
            return "MARKDOWN"

        return "UNCERTAIN"

    except Exception as e:
        log.debug("[wyckoff] detect_phase error: %s", e)
        return "UNCERTAIN"


def phase_score_adj(setup: str, direction: str, phase: str) -> int:
    """
    Devuelve el ajuste de score basado en compatibilidad setup ↔ fase.

    Returns:
        -2  trampa conocida (BREAKOUT en distribución, etc.)
        -1  contratendencia débil
         0  neutral / no relevante
        +1  setup alineado con fase
        +2  setup óptimo para la fase (REVERSAL en acumulación/distribución)
    """
    if not _ENABLED or phase == "UNCERTAIN":
        return 0

    s = setup.upper()
    d = direction.upper()  # LONG | SHORT
    p = phase.upper()

    # — Trampas conocidas (-2) ——————————————————————————————
    if s == "BREAKOUT" and d == "LONG"  and p == "DISTRIBUTION":
        log.debug("[wyckoff] BREAKOUT LONG en DISTRIBUTION → posible UTAD, -2")
        return -2
    if s == "BREAKOUT" and d == "SHORT" and p == "ACCUMULATION":
        log.debug("[wyckoff] BREAKOUT SHORT en ACCUMULATION → posible spring, -2")
        return -2

    # — Contratendencia débil (-1) ———————————————————————————
    if s == "TENDENCIA" and d == "LONG"  and p == "MARKDOWN":
        return -1
    if s == "TENDENCIA" and d == "SHORT" and p == "MARKUP":
        return -1

    # — Óptimo: REVERSAL alineado con fase (+2) —————————————————
    if s == "REVERSAL" and d == "LONG"  and p == "ACCUMULATION":
        return 2
    if s == "REVERSAL" and d == "SHORT" and p == "DISTRIBUTION":
        return 2

    # — Alineado (+1) ———————————————————————————————————
    if d == "LONG"  and p == "MARKUP":
        return 1
    if d == "SHORT" and p == "MARKDOWN":
        return 1

    return 0


def setup_allowed(setup: str, direction: str, phase: str) -> Tuple[bool, str]:
    """
    Bloquea la señal si el ajuste de score es <= -2 Y no supera el umbral
    de override configurado. Devuelve (allowed, reason).

    En la práctica, una señal con puntuación -2 dificultará alcanzar
    MIN_SCORE, pero setup_allowed() es la válvula explícita para bloqueo duro.
    """
    adj = phase_score_adj(setup, direction, phase)
    if adj <= -2:
        reason = (
            f"Wyckoff BLOCK: {setup} {direction} en fase {phase} "
            f"(área de trampa conocida, adj={adj})"
        )
        return False, reason
    return True, ""


# ── Helpers internos ─────────────────────────────────────────────────────

def _prepare(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    if isinstance(df, list):
        try:
            df = pd.DataFrame(df, columns=["ts", "open", "high", "low", "close", "volume"])
        except Exception:
            return None
    required = {"high", "low", "close", "volume"}
    if not required.issubset(df.columns):
        return None
    return df.tail(_LOOKBACK).copy()


def _ema(arr: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1)
    out   = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
    return out


def _swing_highs(arr: np.ndarray, window: int = 5) -> list:
    pivots = []
    w = window
    for i in range(w, len(arr) - w):
        if arr[i] == max(arr[i - w: i + w + 1]):
            pivots.append(float(arr[i]))
    return pivots


def _swing_lows(arr: np.ndarray, window: int = 5) -> list:
    pivots = []
    w = window
    for i in range(w, len(arr) - w):
        if arr[i] == min(arr[i - w: i + w + 1]):
            pivots.append(float(arr[i]))
    return pivots
