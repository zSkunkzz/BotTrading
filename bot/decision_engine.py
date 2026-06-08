#!/usr/bin/env python3
"""
bot/decision_engine.py — Cálculo de tamaño de posición y sizing.

v10 — Kelly sizing:
  - compute_kelly_fraction(win_rate, avg_win, avg_loss) calcula la fracción
    óptima de Kelly y la limita en [KELLY_MIN_FRACTION, KELLY_MAX_FRACTION].
  - calc_position_size() acepta kelly_stats opcional. Si se provee y
    KELLY_ENABLED=true, multiplica el tamaño base por la fracción de Kelly.
  - Config: KELLY_ENABLED (default false), KELLY_MIN_FRACTION (default 0.05),
            KELLY_MAX_FRACTION (default 0.25).

fix: clase DecisionEngine añadida para que bot/core/decision_engine.py
     pueda hacer `from bot.decision_engine import DecisionEngine` sin
     ImportError. Envuelve compute_kelly_fraction y calc_position_size.
"""

import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

# ─ Config general ───────────────────────────────────────────────────────────────
_CAPITAL               = float(os.getenv("CAPITAL",                "100.0"))
_MAX_RISK_PCT          = float(os.getenv("MAX_RISK_PCT",           "0.01"))
_MAX_LEVERAGE          = int(os.getenv("MAX_LEVERAGE",             "10"))
_EF_PENALTY_REDUCTION  = float(os.getenv("EF_PENALTY_REDUCTION",   "0.10"))  # 10% reducción por penalización

# ─ Kelly ───────────────────────────────────────────────────────────────────────────
_KELLY_ENABLED      = os.getenv("KELLY_ENABLED",     "false").lower() not in ("false", "0", "no")
_KELLY_MIN_FRACTION = float(os.getenv("KELLY_MIN_FRACTION", "0.05"))   # 5% mínimo de Kelly
_KELLY_MAX_FRACTION = float(os.getenv("KELLY_MAX_FRACTION", "0.25"))   # 25% máximo de Kelly


def compute_kelly_fraction(
    win_rate: float,
    avg_win:  float,
    avg_loss: float,
) -> float:
    """
    Fracción de Kelly = (p * b - q) / b
      donde p = win_rate,  q = 1 - p,
            b = avg_win / avg_loss  (odds)

    La fracción se limita al intervalo [KELLY_MIN_FRACTION, KELLY_MAX_FRACTION].
    Si los inputs son inválidos o la fracción es negativa (EV negativo)
    se devuelve KELLY_MIN_FRACTION para forzar tamaño mínimo seguro.
    """
    if avg_loss <= 0 or win_rate <= 0 or win_rate >= 1:
        log.debug("[kelly] inputs inválidos: win_rate=%.3f avg_win=%.4f avg_loss=%.4f",
                  win_rate, avg_win, avg_loss)
        return _KELLY_MIN_FRACTION

    b = avg_win / avg_loss
    q = 1.0 - win_rate
    kelly = (win_rate * b - q) / b

    if kelly <= 0:
        log.debug("[kelly] EV negativo (kelly=%.4f) — usando mínimo %.3f", kelly, _KELLY_MIN_FRACTION)
        return _KELLY_MIN_FRACTION

    clipped = max(_KELLY_MIN_FRACTION, min(_KELLY_MAX_FRACTION, kelly))
    log.debug("[kelly] raw=%.4f → clipped=%.4f (min=%.3f max=%.3f)",
              kelly, clipped, _KELLY_MIN_FRACTION, _KELLY_MAX_FRACTION)
    return clipped


def calc_position_size(
    entry:       float,
    sl:          float,
    leverage:    int,
    capital:     Optional[float] = None,
    ef_penalty:  int             = 0,
    kelly_stats: Optional[dict]  = None,
) -> float:
    """
    Calcula el tamaño de posición (en contratos/monedas).

    Flujo:
      1. Riesgo base = capital * MAX_RISK_PCT
      2. Si kelly_stats provisto y KELLY_ENABLED=true:
           kelly_fraction = compute_kelly_fraction(...)
           riesgo_efectivo = riesgo_base * kelly_fraction
         else:
           riesgo_efectivo = riesgo_base
      3. Penalización EF: riesgo *= (1 - ef_penalty * EF_PENALTY_REDUCTION)
      4. qty = riesgo_efectivo / (risk_per_unit * leverage)

    kelly_stats: dict con claves 'win_rate', 'avg_win', 'avg_loss' (floats).
    ef_penalty:  0–3 penalizaciones de enriched_filter.
    """
    _cap = capital if capital is not None else _CAPITAL
    if entry <= 0 or sl <= 0 or entry == sl:
        log.warning("[decision_engine] entry/sl inválidos: entry=%.6f sl=%.6f", entry, sl)
        return 0.0

    risk_per_unit = abs(entry - sl)
    base_risk     = _cap * _MAX_RISK_PCT

    # Kelly
    kelly_fraction = 1.0
    if _KELLY_ENABLED and kelly_stats:
        kelly_fraction = compute_kelly_fraction(
            win_rate = float(kelly_stats.get("win_rate",  0.5)),
            avg_win  = float(kelly_stats.get("avg_win",   1.0)),
            avg_loss = float(kelly_stats.get("avg_loss",  1.0)),
        )
        log.info(
            "[decision_engine] Kelly fraction=%.4f (win_rate=%.3f avg_win=%.4f avg_loss=%.4f)",
            kelly_fraction,
            kelly_stats.get("win_rate", 0.5),
            kelly_stats.get("avg_win",  1.0),
            kelly_stats.get("avg_loss", 1.0),
        )

    effective_risk = base_risk * kelly_fraction

    # Penalización por enriquecido (0–3 niveles)
    if ef_penalty > 0:
        factor = max(0.0, 1.0 - ef_penalty * _EF_PENALTY_REDUCTION)
        effective_risk *= factor
        log.info(
            "[decision_engine] ef_penalty=%d → riesgo reducido x%.2f (%.4f → %.4f)",
            ef_penalty, factor, base_risk, effective_risk,
        )

    lev = min(leverage, _MAX_LEVERAGE)
    qty = effective_risk / (risk_per_unit * lev)

    log.debug(
        "[decision_engine] entry=%.6f sl=%.6f lev=%d kelly=%.4f risk=%.4f qty=%.6f",
        entry, sl, lev, kelly_fraction, effective_risk, qty,
    )
    return qty


class DecisionEngine:
    """
    Fachada orientada a objetos sobre las funciones de sizing de este módulo.

    Permite que bot/core/decision_engine.py haga:
        from bot.decision_engine import DecisionEngine  # re-export
    sin ImportError.

    Uso:
        de = DecisionEngine()
        qty = de.calc_position_size(entry=..., sl=..., leverage=...)
        frac = de.compute_kelly_fraction(win_rate=..., avg_win=..., avg_loss=...)
    """

    def calc_position_size(
        self,
        entry:       float,
        sl:          float,
        leverage:    int,
        capital:     Optional[float] = None,
        ef_penalty:  int             = 0,
        kelly_stats: Optional[dict]  = None,
    ) -> float:
        return calc_position_size(
            entry=entry,
            sl=sl,
            leverage=leverage,
            capital=capital,
            ef_penalty=ef_penalty,
            kelly_stats=kelly_stats,
        )

    def compute_kelly_fraction(
        self,
        win_rate: float,
        avg_win:  float,
        avg_loss: float,
    ) -> float:
        return compute_kelly_fraction(
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
        )
