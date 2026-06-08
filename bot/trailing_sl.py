"""trailing_sl.py — Trailing Stop Loss post-TP1.

Modos:
  pct (default): SL se mueve a pct% del pico favorable.
  atr:           SL se mueve a N×ATR del pico favorable.
                 Más adaptativo — en mercados volátiles el trail se ensancha;
                 en mercados calmados se ajusta más al precio.

Config Railway:
  TRAILING_SL_MODE      → 'pct' | 'atr'   (default 'atr')
  TRAILING_SL_PCT       → 0.015            (solo en modo pct, 1.5%)
  TRAILING_SL_ATR_MULT  → 1.5             (multiplicador ATR en modo atr)

Reglas comunes:
  - Solo activo cuando trailing_sl_activated = True (post-TP1).
  - El SL solo avanza en dirección favorable, nunca retrocede.
  - Si el precio toca el trailing SL → trail_sl_hit=True.
"""
from __future__ import annotations
import os

TRAILING_SL_MODE     = os.getenv("TRAILING_SL_MODE",     "atr").lower()   # 'atr' | 'pct'
TRAILING_SL_PCT      = float(os.getenv("TRAILING_SL_PCT",      "0.015"))  # 1.5% (modo pct)
TRAILING_SL_ATR_MULT = float(os.getenv("TRAILING_SL_ATR_MULT", "1.5"))   # N×ATR (modo atr)


def compute_trailing_sl(
    *,
    is_long: bool,
    current_price: float,
    peak_price: float,
    current_sl: float,
    trailing_pct: float = TRAILING_SL_PCT,
    atr_val: float = 0.0,
    mode: str = TRAILING_SL_MODE,
) -> tuple[float, float]:
    """
    Calcula el nuevo SL trailing y el nuevo pico favorable.

    Args:
        is_long:       True para posiciones LONG.
        current_price: Precio actual del activo.
        peak_price:    Mejor precio alcanzado desde la activación del trailing.
        current_sl:    SL actual (el trailing nunca lo empeora).
        trailing_pct:  Distancia en % (solo en modo 'pct').
        atr_val:       ATR actual del timeframe de seguimiento (modo 'atr').
        mode:          'atr' o 'pct'.

    Returns:
        (new_sl, new_peak)
    """
    if is_long:
        new_peak = max(peak_price, current_price)
        if mode == "atr" and atr_val > 0:
            candidate_sl = new_peak - TRAILING_SL_ATR_MULT * atr_val
        else:
            candidate_sl = new_peak * (1.0 - trailing_pct)
        new_sl = max(current_sl, candidate_sl)
    else:
        new_peak = min(peak_price, current_price) if peak_price > 0 else current_price
        if mode == "atr" and atr_val > 0:
            candidate_sl = new_peak + TRAILING_SL_ATR_MULT * atr_val
        else:
            candidate_sl = new_peak * (1.0 + trailing_pct)
        new_sl = min(current_sl, candidate_sl)

    return new_sl, new_peak


def is_trailing_sl_hit(
    *,
    is_long: bool,
    current_price: float,
    trailing_sl: float,
) -> bool:
    """True si el precio ha tocado el trailing SL."""
    if is_long:
        return current_price <= trailing_sl
    return current_price >= trailing_sl
