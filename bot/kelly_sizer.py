#!/usr/bin/env python3
"""
kelly_sizer.py — Sizing fraccionado basado en Kelly Criterion + score_ratio.

Fórmula base: f* = (p*b - q) / b
  p = win rate histórico (shadow_mode por entry_mode)
  q = 1 - p
  b = R/R del trade

Ajuste por score_ratio:
  El multiplicador Kelly se pondera adicionalmente por la calidad de la señal
  (score_ratio = score / max_score). Una señal con ratio 0.95 merece más size
  que una con 0.62, aunque ambas pasen el umbral mínimo.

  score_factor = KELLY_SCORE_MIN_MULT + (KELLY_SCORE_MAX_MULT - KELLY_SCORE_MIN_MULT)
                 * clamp((score_ratio - min_ratio) / (1.0 - min_ratio), 0, 1)

  El multiplicador final es: kelly_mult × score_factor, acotado a [KELLY_MIN_MULT, KELLY_MAX_MULT].

Config Railway:
  KELLY_ENABLED           → default true
  KELLY_FRACTION          → default 0.25  (quarter-Kelly)
  KELLY_MIN_MULT          → default 0.5
  KELLY_MAX_MULT          → default 2.0
  KELLY_MIN_TRADES        → default 30
  KELLY_SCORE_WEIGHT      → default true  (activar ponderación por score_ratio)
  KELLY_SCORE_MIN_MULT    → default 0.7   (factor para score_ratio mínimo)
  KELLY_SCORE_MAX_MULT    → default 1.3   (factor para score_ratio = 1.0)
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except (ValueError, TypeError):
        log.warning("[kelly] %s='%s' no es entero válido — usando default=%d", name, raw, default)
        return default


KELLY_ENABLED        = os.getenv("KELLY_ENABLED",      "true").lower() not in ("false", "0", "no")
KELLY_FRACTION       = float(os.getenv("KELLY_FRACTION",      "0.25"))
KELLY_MIN_MULT       = float(os.getenv("KELLY_MIN_MULT",      "0.5"))
KELLY_MAX_MULT       = float(os.getenv("KELLY_MAX_MULT",      "2.0"))
KELLY_MIN_TRADES     = _parse_int_env("KELLY_MIN_TRADES", 30)
KELLY_SCORE_WEIGHT   = os.getenv("KELLY_SCORE_WEIGHT", "true").lower() not in ("false", "0", "no")
KELLY_SCORE_MIN_MULT = float(os.getenv("KELLY_SCORE_MIN_MULT", "0.7"))  # factor en score_ratio mínimo
KELLY_SCORE_MAX_MULT = float(os.getenv("KELLY_SCORE_MAX_MULT", "1.3"))  # factor en score_ratio = 1.0


def _score_factor(score_ratio: float, min_ratio: float) -> float:
    """Interpola linealmente entre KELLY_SCORE_MIN_MULT y KELLY_SCORE_MAX_MULT."""
    if not KELLY_SCORE_WEIGHT or min_ratio >= 1.0:
        return 1.0
    t = (score_ratio - min_ratio) / (1.0 - min_ratio)
    t = max(0.0, min(1.0, t))
    return KELLY_SCORE_MIN_MULT + (KELLY_SCORE_MAX_MULT - KELLY_SCORE_MIN_MULT) * t


def kelly_multiplier(
    entry_mode: str,
    rr: float,
    score_ratio: float = 0.0,
    min_ratio: float = 0.62,
) -> float:
    """
    Calcula el multiplicador de sizing Kelly ponderado por score_ratio.

    Args:
        entry_mode:  'STRONG', 'FAST', 'NORMAL', 'EARLY'
        rr:          Risk/Reward del trade.
        score_ratio: score / max_score (0–1). Default 0 → factor neutro.
        min_ratio:   Umbral mínimo de ratio de la señal (MIN_SCORE_RATIO).

    Returns:
        float: multiplicador final acotado en [KELLY_MIN_MULT, KELLY_MAX_MULT].
    """
    if not KELLY_ENABLED:
        return 1.0

    if rr <= 0:
        return KELLY_MIN_MULT

    base_mult = 1.0
    try:
        from bot.shadow_mode import shadow_mode
        stats = shadow_mode.win_rate_by_mode()
        mode_stats = stats.get(entry_mode)
        if mode_stats is None or mode_stats["trades"] < KELLY_MIN_TRADES:
            log.debug(
                "[kelly] %s: insuficientes trades (%d < %d) — mult=1.0",
                entry_mode,
                mode_stats["trades"] if mode_stats else 0,
                KELLY_MIN_TRADES,
            )
            base_mult = 1.0
        else:
            p = mode_stats["win_rate"]
            q = 1.0 - p
            b = rr
            f_full = (p * b - q) / b
            f = f_full * KELLY_FRACTION
            base_mult = 1.0 + f
            log.info(
                "[kelly] %s: p=%.2f b=%.2f f_full=%.3f f=%.3f → base_mult=%.2f",
                entry_mode, p, b, f_full, f, base_mult,
            )
    except Exception as e:
        log.warning("[kelly] Error calculando mult: %s", e)
        base_mult = 1.0

    sf = _score_factor(score_ratio, min_ratio)
    mult = base_mult * sf
    mult = max(KELLY_MIN_MULT, min(KELLY_MAX_MULT, mult))

    log.info(
        "[kelly] %s: base_mult=%.3f × score_factor=%.3f (ratio=%.2f) → mult_final=%.3f",
        entry_mode, base_mult, sf, score_ratio, mult,
    )
    return round(mult, 3)
