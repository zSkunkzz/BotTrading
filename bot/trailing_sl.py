"""trailing_sl.py — Lógica de Trailing Stop Loss post-TP1.

Activado después de que TP1 es alcanzado. En cada iteración del trader,
actualiza el SL local siguiendo el máximo favorable del precio.

Reglas:
  - Solo se activa cuando _trailing_sl_activated = True
  - El SL solo se mueve en dirección favorable (nunca retrocede)
  - El % de trailing es configurable via TRAILING_SL_PCT (default 1.5%)
  - Si el precio toca el trailing SL, se retorna trail_sl_hit=True
"""
from __future__ import annotations
import os

TRAILING_SL_PCT = float(os.getenv("TRAILING_SL_PCT", "0.015"))  # 1.5%


def compute_trailing_sl(
    *,
    is_long: bool,
    current_price: float,
    peak_price: float,
    current_sl: float,
    trailing_pct: float = TRAILING_SL_PCT,
) -> tuple[float, float]:
    """
    Dado el precio actual y el pico histórico favorable, calcula el nuevo SL
    trailing y el nuevo pico.

    Devuelve (new_sl, new_peak):
      - new_peak: max(peak_price, current_price) para longs,
                  min(peak_price, current_price) para shorts
      - new_sl:   new_peak * (1 - pct) para longs,
                  new_peak * (1 + pct) para shorts
        pero NUNCA inferior a current_sl para longs (ni superior para shorts),
        garantizando que el SL solo avanza en dirección favorable.
    """
    if is_long:
        new_peak = max(peak_price, current_price)
        candidate_sl = new_peak * (1.0 - trailing_pct)
        new_sl = max(current_sl, candidate_sl)
    else:
        # Para shorts: el "pico" es el mínimo favorable (el precio más bajo)
        new_peak = min(peak_price, current_price) if peak_price > 0 else current_price
        candidate_sl = new_peak * (1.0 + trailing_pct)
        new_sl = min(current_sl, candidate_sl)

    return new_sl, new_peak


def is_trailing_sl_hit(
    *,
    is_long: bool,
    current_price: float,
    trailing_sl: float,
) -> bool:
    """Devuelve True si el precio ha tocado el trailing SL."""
    if is_long:
        return current_price <= trailing_sl
    return current_price >= trailing_sl
