#!/usr/bin/env python3
"""
kelly_sizer.py — Sizing fraccionado basado en Kelly Criterion

Fórmula: f* = (p*b - q) / b
  p = win rate histórico (del shadow_mode por entry_mode)
  q = 1 - p
  b = R/R del trade

Se aplica Kelly_fraction (default 0.25 = quarter-Kelly) para reducir volatilidad.
El resultado es un multiplicador sobre el USDC_PER_TRADE base.

Config Railway:
  KELLY_ENABLED      → default true  (se activa cuando hay historial suficiente)
  KELLY_FRACTION     → default 0.25  (fraccción de Kelly full)
  KELLY_MIN_MULT     → default 0.5   (mínimo 50% del size base)
  KELLY_MAX_MULT     → default 2.0   (máximo 200% del size base)
  KELLY_MIN_TRADES   → default 30    (mínimo trades para activar; si < esto → mult=1.0)

Nota: KELLY_ENABLED=true no fuerza Kelly si hay < KELLY_MIN_TRADES en shadow_mode.
En ese caso silenciosamente usa mult=1.0 hasta acumular historial suficiente.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

KELLY_ENABLED    = os.getenv("KELLY_ENABLED",  "true").lower() != "false"   # default ON
KELLY_FRACTION   = float(os.getenv("KELLY_FRACTION",   "0.25"))
KELLY_MIN_MULT   = float(os.getenv("KELLY_MIN_MULT",   "0.5"))
KELLY_MAX_MULT   = float(os.getenv("KELLY_MAX_MULT",   "2.0"))
KELLY_MIN_TRADES = int(os.getenv("KELLY_MIN_TRADES",   "30"))


def kelly_multiplier(entry_mode: str, rr: float) -> float:
    """
    Calcula el multiplicador de sizing Kelly para un trade.

    Returns:
        float: multiplicador (ej. 1.0 = sin cambio, 1.5 = +50% size)
                Devuelve 1.0 si Kelly está desactivado o hay insuficiente historial.
    """
    if not KELLY_ENABLED:
        return 1.0

    if rr <= 0:
        return KELLY_MIN_MULT

    try:
        from bot.shadow_mode import shadow_mode
        stats = shadow_mode.win_rate_by_mode()
        mode_stats = stats.get(entry_mode)
        if mode_stats is None or mode_stats["trades"] < KELLY_MIN_TRADES:
            log.debug(
                "[kelly] %s: insuficientes trades (%d < %d) — mult=1.0 (sin historial)",
                entry_mode,
                mode_stats["trades"] if mode_stats else 0,
                KELLY_MIN_TRADES,
            )
            return 1.0

        p = mode_stats["win_rate"]   # 0.0 – 1.0
        q = 1.0 - p
        b = rr

        f_full = (p * b - q) / b
        f = f_full * KELLY_FRACTION

        # f negativo → edge negativo para este modo → usar mínimo
        mult = 1.0 + f
        mult = max(KELLY_MIN_MULT, min(KELLY_MAX_MULT, mult))

        log.info(
            "[kelly] %s: p=%.2f b=%.2f f_full=%.3f f=%.3f → mult=%.2f",
            entry_mode, p, b, f_full, f, mult,
        )
        return round(mult, 3)

    except Exception as e:
        log.warning("[kelly] Error calculando mult: %s", e)
        return 1.0
