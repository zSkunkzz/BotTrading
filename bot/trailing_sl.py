#!/usr/bin/env python3
"""
bot/trailing_sl.py — Lógica de trailing stop-loss.

Modos:
  'atr'  (default): trailing_sl = peak ± atr_val * TRAILING_SL_ATR_MULT
  'pct'           : trailing_sl = peak ± peak * TRAILING_SL_PCT

El trailing SL:
  - Solo avanza en la dirección favorable (nunca retrocede).
  - Activado en PositionManager cuando trader.trailing_sl_activated=True
    (normalmente tras el hit de TP1).
  - El pico favorable (peak_price) se actualiza cada ciclo.

Variables de entorno:
  TRAILING_SL_MODE      (default: 'atr')   — 'atr' o 'pct'
  TRAILING_SL_ATR_MULT  (default: 1.5)    — multiplicador de ATR
  TRAILING_SL_PCT       (default: 0.015)  — 1.5% para modo 'pct'
"""

import logging
import os
from typing import Tuple

log = logging.getLogger(__name__)

_MODE     = os.getenv("TRAILING_SL_MODE",     "atr").lower()
_ATR_MULT = float(os.getenv("TRAILING_SL_ATR_MULT", "1.5"))
_PCT      = float(os.getenv("TRAILING_SL_PCT",      "0.015"))


def compute_trailing_sl(
    is_long:       bool,
    current_price: float,
    peak_price:    float,
    current_sl:    float,
    atr_val:       float = 0.0,
) -> Tuple[float, float]:
    """
    Calcula el nuevo trailing SL y el nuevo pico favorable.

    Retorna:
      (new_sl, new_peak)

    El SL nuevo NUNCA retrocede respecto al actual:
      - LONG:  new_sl  = max(current_sl, computed_sl)
      - SHORT: new_sl  = min(current_sl, computed_sl)
    """
    # Actualizar pico favorable
    if is_long:
        new_peak = max(peak_price, current_price)
    else:
        new_peak = min(peak_price, current_price)

    # Calcular distancia de trailing
    if _MODE == "atr" and atr_val > 0:
        distance = atr_val * _ATR_MULT
    else:
        # Modo 'pct' o ATR no disponible
        distance = new_peak * _PCT

    if is_long:
        computed_sl = new_peak - distance
        new_sl      = max(current_sl, computed_sl)  # nunca retrocede
    else:
        computed_sl = new_peak + distance
        new_sl      = min(current_sl, computed_sl)  # nunca retrocede

    log.debug(
        "[trailing_sl] is_long=%s current=%.6f peak=%.6f→%.6f dist=%.6f "
        "sl=%.6f→%.6f (mode=%s)",
        is_long, current_price, peak_price, new_peak, distance,
        current_sl, new_sl, _MODE,
    )

    return new_sl, new_peak


def is_trailing_sl_hit(
    is_long:      bool,
    current_price: float,
    trailing_sl:   float,
) -> bool:
    """
    Retorna True si el precio ha tocado o superado el trailing SL.
      - LONG:  precio <= trailing_sl
      - SHORT: precio >= trailing_sl
    """
    if is_long:
        return current_price <= trailing_sl
    return current_price >= trailing_sl
