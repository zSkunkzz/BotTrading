"""
wyckoff_phase.py — Detección de fase de mercado por par (Wyckoff simplificado).

A diferencia del régimen global (market_regime.py que usa BTC 1h como proxy),
este módulo evalúa CADA PAR individualmente usando su propia estructura de
velas 1h y 4h. Esto resuelve el problema de que un BREAKOUT en fase de
distribución sea puntuado igual que uno en markup: son setups opuestos.

FASES DETECTADAS:
  ACCUMULATION  — precio en rango bajo, volumen creciente, sin dirección
  MARKUP        — tendencia alcista confirmada, estructura HH/HL
  DISTRIBUTION  — precio en rango alto, volumen decreciente, sin dirección
  MARKDOWN      — tendencia bajista confirmada, estructura LL/LH
  RANGING       — rango sin contexto claro (ni acumulación ni distribución)
  UNKNOWN       — datos insuficientes

BONUS/PENALTY POR SETUP:
  TENDENCIA  en MARKUP       → +1 (confluencia)
  TENDENCIA  en MARKDOWN     → +1 (SHORT confluente)
  BREAKOUT   en DISTRIBUTION → -3 (trampa de distribución)
  BREAKOUT   en ACCUMULATION → +1 (breakout de estructura sana)
  REVERSAL   en ACCUMULATION → +2 (setup institucional de acumulación)
  REVERSAL   en DISTRIBUTION → +2 (setup institucional de distribución)

Variables de entorno:
  WYCKOFF_ENABLED     true|false  Activar/desactivar (default true)
  WYCKOFF_ADX_MIN     float       ADX mínimo para confirmar markup/markdown (default 20)
  WYCKOFF_BB_HIGH_PCT float       Percentil BB width para considerar distribución (default 70)
  WYCKOFF_BB_LOW_PCT  float       Percentil BB width para considerar acumulación (default 30)

Uso:
  from bot.wyckoff_phase import detect_wyckoff_phase, wyckoff_score_adjust

  phase = detect_wyckoff_phase(df_1h, df_4h)  # "MARKUP" | "DISTRIBUTION" | ...
  delta = wyckoff_score_adjust(phase, setup_type="BREAKOUT", direction="LONG")
  score += delta
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

_ENABLED     = os.getenv("WYCKOFF_ENABLED",     "true").lower() != "false"
_ADX_MIN     = float(os.getenv("WYCKOFF_ADX_MIN",     "20.0"))
_BB_HIGH_PCT = float(os.getenv("WYCKOFF_BB_HIGH_PCT", "70.0"))
_BB_LOW_PCT  = float(os.getenv("WYCKOFF_BB_LOW_PCT",  "30.0"))

# Tabla de ajuste: (phase, setup_type) → delta de score
_PHASE_SETUP_DELTA: dict[tuple[str, str], int] = {
    ("MARKUP",       "TENDENCIA"):  +1,
    ("MARKDOWN",     "TENDENCIA"):  +1,   # SHORT en markdown
    ("MARKUP",       "BREAKOUT"):   +1,
    ("DISTRIBUTION", "BREAKOUT"):   -3,   # TRAMPA — penalización fuerte
    ("ACCUMULATION", "BREAKOUT"):   +1,
    ("ACCUMULATION", "REVERSAL"):   +2,   # Setup institucional
    ("DISTRIBUTION", "REVERSAL"):   +2,   # Setup institucional SHORT
    ("RANGING",      "BREAKOUT"):    0,
    ("RANGING",      "TENDENCIA"):  -1,   # Sin fase = tendencia poco confiable
}


def detect_wyckoff_phase(
    df_1h: Optional[pd.DataFrame],
    df_4h: Optional[pd.DataFrame] = None,
) -> str:
    """
    Detecta la fase Wyckoff del par a partir de sus propias velas 1h/4h.

    Returns:
        'ACCUMULATION' | 'MARKUP' | 'DISTRIBUTION' | 'MARKDOWN' | 'RANGING' | 'UNKNOWN'
    """
    if not _ENABLED:
        return "UNKNOWN"

    df = df_1h
    if df is None or df.empty or len(df) < 50:
        return "UNKNOWN"

    try:
        close  = df["close"].astype(float)
        high   = df["high"].astype(float)
        low    = df["low"].astype(float)
        volume = df["volume"].astype(float)

        # ── EMA200 y EMA50 para posición de precio ───────────────────────────
        ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1])
        ema50  = float(close.ewm(span=50,  adjust=False).mean().iloc[-1])
        price  = float(close.iloc[-1])

        # ── Pendiente EMA50 (últimas 10 barras, normalizada) ─────────────────
        ema50_series = close.ewm(span=50, adjust=False).mean()
        slope_window = min(10, len(ema50_series) - 1)
        slope_raw    = float(ema50_series.iloc[-1] - ema50_series.iloc[-slope_window])
        slope_pct    = slope_raw / float(ema50_series.iloc[-slope_window]) if float(ema50_series.iloc[-slope_window]) > 0 else 0.0

        # ── ADX simple (DX proxy con ATR) ────────────────────────────────────
        adx = _calc_adx(high, low, close, period=14)

        # ── BB width percentil (últimas 100 barras) ──────────────────────────
        rolling_mid = close.rolling(20).mean()
        rolling_std = close.rolling(20).std()
        bb_width    = (2 * 2 * rolling_std / rolling_mid).dropna()
        bb_pct      = float(np.percentile(bb_width.values[-100:], 50)) if len(bb_width) >= 20 else 0.0
        bb_current  = float(bb_width.iloc[-1]) if len(bb_width) > 0 else 0.0
        bb_high_thr = float(np.percentile(bb_width.values[-100:], _BB_HIGH_PCT)) if len(bb_width) >= 20 else bb_current + 1
        bb_low_thr  = float(np.percentile(bb_width.values[-100:], _BB_LOW_PCT))  if len(bb_width) >= 20 else 0.0

        # ── Volumen: media últimas 10 vs media últimas 40 ────────────────────
        vol_recent = float(volume.iloc[-10:].mean()) if len(volume) >= 10 else 0.0
        vol_hist   = float(volume.iloc[-40:].mean()) if len(volume) >= 40 else vol_recent
        vol_ratio  = vol_recent / vol_hist if vol_hist > 0 else 1.0

        # ── Lógica de clasificación ──────────────────────────────────────────
        above_ema200 = price > ema200
        above_ema50  = price > ema50
        trending_up  = slope_pct > 0.002   # +0.2% en 10 barras
        trending_dn  = slope_pct < -0.002
        strong_adx   = adx >= _ADX_MIN
        tight_bb     = bb_current <= bb_low_thr
        wide_bb      = bb_current >= bb_high_thr

        # MARKUP: precio sobre ambas EMAs, pendiente positiva, ADX fuerte
        if above_ema200 and above_ema50 and trending_up and strong_adx:
            phase = "MARKUP"

        # MARKDOWN: precio bajo ambas EMAs, pendiente negativa, ADX fuerte
        elif not above_ema200 and not above_ema50 and trending_dn and strong_adx:
            phase = "MARKDOWN"

        # DISTRIBUTION: precio sobre EMA200, BB ancho, volumen decreciente
        elif above_ema200 and wide_bb and vol_ratio < 0.85:
            phase = "DISTRIBUTION"

        # ACCUMULATION: precio bajo EMA200, BB estrecho, volumen creciente
        elif not above_ema200 and tight_bb and vol_ratio > 1.1:
            phase = "ACCUMULATION"

        # RANGING: ninguna condición clara
        else:
            phase = "RANGING"

        log.debug(
            "[wyckoff] phase=%s | price=%.4f ema50=%.4f ema200=%.4f "
            "slope=%.3f%% adx=%.1f bb_curr=%.4f vol_ratio=%.2f",
            phase, price, ema50, ema200,
            slope_pct * 100, adx, bb_current, vol_ratio,
        )
        return phase

    except Exception as exc:
        log.debug("[wyckoff] detect error: %s", exc)
        return "UNKNOWN"


def wyckoff_score_adjust(
    phase: str,
    setup_type: str,
    direction: str = "LONG",
) -> int:
    """
    Devuelve el delta de score que debe aplicarse al setup según la fase.

    Args:
        phase      : resultado de detect_wyckoff_phase()
        setup_type : 'TENDENCIA' | 'BREAKOUT' | 'REVERSAL'
        direction  : 'LONG' | 'SHORT' (para ajustes direccionales futuros)

    Returns:
        int: puntos a sumar (negativo = penalización)
    """
    if not _ENABLED or phase == "UNKNOWN":
        return 0

    key = (phase.upper(), setup_type.upper())
    delta = _PHASE_SETUP_DELTA.get(key, 0)

    if delta != 0:
        log.info(
            "[wyckoff] %s + %s → delta=%+d (phase=%s dir=%s)",
            setup_type, direction, delta, phase, direction,
        )
    return delta


# ── Helpers internos ─────────────────────────────────────────────────────────

def _calc_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    """ADX simplificado (Wilder smoothing) sin dependencia de ta-lib."""
    try:
        tr   = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low  - close.shift(1)).abs(),
        ], axis=1).max(axis=1)

        dm_p = (high - high.shift(1)).clip(lower=0)
        dm_n = (low.shift(1) - low).clip(lower=0)
        # Solo el que domine en cada barra
        dm_p = dm_p.where(dm_p > dm_n, 0.0)
        dm_n = dm_n.where(dm_n > dm_p, 0.0)

        atr  = tr.ewm(alpha=1/period, adjust=False).mean()
        di_p = 100 * dm_p.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, np.nan)
        di_n = 100 * dm_n.ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, np.nan)

        dx   = (100 * (di_p - di_n).abs() / (di_p + di_n).replace(0, np.nan)).fillna(0)
        adx  = float(dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1])
        return adx if not np.isnan(adx) else 0.0
    except Exception:
        return 0.0
