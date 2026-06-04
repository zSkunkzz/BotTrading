#!/usr/bin/env python3
"""
correlation.py — Guarda de correlación entre posiciones abiertas

En cripto, la mayoría de altcoins están altamente correlacionadas con BTC.
Abrir 5 posiciones SHORT simultáneas = 1 trade grande con más comisiones.

Reglas:
  1. MAX_SAME_DIRECTION: máximo N posiciones en la misma dirección (default 3)
  2. MAX_OPEN_POSITIONS: máximo M posiciones abiertas en total (default 5)
  3. BTC_HEDGE_CHECK: si BTC va contra la dirección propuesta, reducir size
  4. DYNAMIC_CORR: correlación rolling de los últimos N candles contra BTC.
     Si la correlación entre el símbolo propuesto y BTC supera CORR_DYNAMIC_THRESHOLD
     y ya hay una posición BTC abierta, penalizar el size como si BTC fuera contrario.

Config Railway:
  CORR_MAX_SAME_DIR        → default 3
  CORR_MAX_OPEN            → default 5
  CORR_ENABLED             → default true
  CORR_DYNAMIC_ENABLED     → default true  (false = solo reglas estáticas)
  CORR_DYNAMIC_THRESHOLD   → default 0.75  (correlación Pearson mínima para penalizar)
  CORR_DYNAMIC_WINDOW      → default 20    (número de candles para correlación rolling)
"""
from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

CORR_ENABLED            = os.getenv("CORR_ENABLED",          "true").lower() != "false"
MAX_SAME_DIR            = int(os.getenv("CORR_MAX_SAME_DIR", "3"))
MAX_OPEN                = int(os.getenv("CORR_MAX_OPEN",     "5"))
CORR_DYNAMIC_ENABLED    = os.getenv("CORR_DYNAMIC_ENABLED",  "true").lower() != "false"
CORR_DYNAMIC_THRESHOLD  = float(os.getenv("CORR_DYNAMIC_THRESHOLD", "0.75"))
CORR_DYNAMIC_WINDOW     = int(os.getenv("CORR_DYNAMIC_WINDOW",     "20"))


def check_correlation(
    proposed_direction: str,           # "LONG" / "SHORT"
    open_positions: Dict[str, dict],   # {symbol: {"side": "long"/"short", ...}}
) -> tuple[bool, str]:
    """
    Comprueba si abrir una nueva posición viola las reglas de correlación.

    Returns:
        (True, "")           → OK, se puede abrir
        (False, reason)      → Rechazado
    """
    if not CORR_ENABLED:
        return True, ""

    total_open = len(open_positions)
    if total_open >= MAX_OPEN:
        reason = f"Máximo de posiciones abiertas alcanzado ({total_open}/{MAX_OPEN})"
        log.info("[correlation] Bloqueado: %s", reason)
        return False, reason

    dir_lower = proposed_direction.lower()
    same_dir  = sum(
        1 for p in open_positions.values()
        if str(p.get("side", "")).lower() == dir_lower
    )

    if same_dir >= MAX_SAME_DIR:
        reason = (
            f"Demasiadas posiciones {proposed_direction} abiertas "
            f"({same_dir}/{MAX_SAME_DIR}) — riesgo de correlación"
        )
        log.info("[correlation] Bloqueado: %s", reason)
        return False, reason

    return True, ""


def compute_rolling_correlation(
    prices_symbol: List[float],
    prices_btc: List[float],
    window: int = CORR_DYNAMIC_WINDOW,
) -> Optional[float]:
    """
    Calcula la correlación de Pearson de los últimos `window` candles
    entre dos series de precios.

    Retorna None si no hay suficientes datos o hay error.
    Retorna un float en [-1, 1].
    """
    try:
        if len(prices_symbol) < window or len(prices_btc) < window:
            return None

        xs = prices_symbol[-window:]
        ys = prices_btc[-window:]

        n  = len(xs)
        mx = sum(xs) / n
        my = sum(ys) / n

        cov   = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        std_x = (sum((x - mx) ** 2 for x in xs) / n) ** 0.5
        std_y = (sum((y - my) ** 2 for y in ys) / n) ** 0.5

        if std_x == 0 or std_y == 0:
            return None

        return cov / (std_x * std_y)

    except Exception as e:
        log.debug("[correlation] compute_rolling_correlation error: %s", e)
        return None


def size_penalty_btc(
    proposed_direction: str,
    btc_trend: int,                       # +1 long, -1 short, 0 neutral (de market_regime)
    prices_symbol: Optional[List[float]] = None,  # cierres del símbolo propuesto
    prices_btc: Optional[List[float]] = None,     # cierres de BTC
) -> float:
    """
    Penaliza el size si BTC va contra la dirección propuesta.
    Con correlación dinámica: si el símbolo está muy correlacionado con BTC
    en los últimos N candles y el trend de BTC es contrario, amplía la penalización.

    Returns: multiplicador de size:
      1.0  → sin penalización
      0.7  → BTC contrario (correlación estática o dinámica moderada)
      0.5  → BTC contrario + alta correlación dinámica (≥ threshold)
    """
    if not CORR_ENABLED:
        return 1.0

    dir_sign = 1 if proposed_direction == "LONG" else -1
    btc_contrario = btc_trend != 0 and btc_trend * dir_sign < 0

    # Correlación dinámica rolling
    dynamic_corr: Optional[float] = None
    if CORR_DYNAMIC_ENABLED and prices_symbol and prices_btc:
        dynamic_corr = compute_rolling_correlation(prices_symbol, prices_btc)
        if dynamic_corr is not None:
            log.debug(
                "[correlation] %s correlación rolling(%d) con BTC = %.3f",
                proposed_direction, CORR_DYNAMIC_WINDOW, dynamic_corr,
            )

    # Penalización base: BTC contrario
    if btc_contrario:
        # Alta correlación dinámica + BTC contrario → penalización fuerte
        if dynamic_corr is not None and dynamic_corr >= CORR_DYNAMIC_THRESHOLD:
            log.info(
                "[correlation] %s BTC contrario + correlación dinámica alta (%.2f >= %.2f) → size ×0.5",
                proposed_direction, dynamic_corr, CORR_DYNAMIC_THRESHOLD,
            )
            return 0.5
        log.debug(
            "[correlation] BTC trend %+d contra %s → penalizar size ×0.7",
            btc_trend, proposed_direction,
        )
        return 0.7

    # BTC no contrario, pero correlación muy alta en periodo volátil → aviso
    if dynamic_corr is not None and dynamic_corr >= CORR_DYNAMIC_THRESHOLD:
        log.debug(
            "[correlation] %s correlación dinámica alta (%.2f) pero BTC alineado — sin penalización",
            proposed_direction, dynamic_corr,
        )

    return 1.0


class CorrelationGuard:
    """
    Facade orientada a objetos sobre las funciones de correlación.
    Usada por RiskManager como self.correlation.
    """

    def __init__(self, config: dict | None = None):
        # config reservado para futura configuración por instancia
        self._config = config or {}

    def is_blocked(
        self,
        proposed_direction: str,
        open_positions: Dict[str, dict],
    ) -> tuple[bool, str]:
        """
        Delega en check_correlation.
        Returns (blocked: bool, reason: str).
        """
        allowed, reason = check_correlation(proposed_direction, open_positions)
        return not allowed, reason

    def size_penalty(
        self,
        proposed_direction: str,
        btc_trend: int,
        prices_symbol: Optional[List[float]] = None,
        prices_btc: Optional[List[float]] = None,
    ) -> float:
        """Delega en size_penalty_btc."""
        return size_penalty_btc(proposed_direction, btc_trend, prices_symbol, prices_btc)

    def rolling_correlation(
        self,
        prices_symbol: List[float],
        prices_btc: List[float],
        window: int = CORR_DYNAMIC_WINDOW,
    ) -> Optional[float]:
        """Delega en compute_rolling_correlation."""
        return compute_rolling_correlation(prices_symbol, prices_btc, window)
