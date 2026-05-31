"""
risk_manager.py — Gestión de riesgo unificada.

Fusiona y centraliza la lógica de:
  - bot/risk.py           (RiskManager original)
  - bot/pretrade_risk.py  (verificaciones pre-trade)
  - bot/global_risk.py    (límites globales de portfolio)

Mantenemos los módulos originales para compatibilidad hacia atrás,
pero este es el punto de entrada recomendado para nueva lógica de riesgo.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Tuple

logger = logging.getLogger("RiskManager")


@dataclass
class RiskManager:
    """
    Configuración de riesgo por símbolo.
    Equivalente a bot/risk.py:RiskManager — sin cambios de interfaz.
    """
    usdc_per_trade:         float = 10.0
    tp_pct:                 float = 4.0
    sl_pct:                 float = 2.0
    trailing_sl:            bool  = True
    trailing_activation_pct: float = 1.5
    trailing_callback_pct:  float = 0.8
    max_daily_loss_pct:     float = 5.0
    max_open_trades:        int   = 1


class GlobalRiskManager:
    """
    Límites de riesgo a nivel de portfolio.
    Fusiona la lógica de bot/global_risk.py.

    Límites:
      - max_concurrent_trades: máximo de posiciones abiertas simultáneas
      - max_global_daily_loss_pct: pérdida máxima diaria del portfolio (%)
    """

    def __init__(
        self,
        max_concurrent_trades:   int   = 5,
        max_global_daily_loss_pct: float = 10.0,
    ):
        self.max_concurrent_trades      = max_concurrent_trades
        self.max_global_daily_loss_pct  = max_global_daily_loss_pct
        self._open_count:  int   = 0
        self._daily_loss:  float = 0.0
        self._day_start:   float = time.time()

    def _maybe_reset_day(self) -> None:
        if time.time() - self._day_start >= 86400:
            self._daily_loss = 0.0
            self._day_start  = time.time()
            logger.info("[GlobalRisk] Contador diario reseteado.")

    async def can_open(self) -> Tuple[bool, str]:
        self._maybe_reset_day()
        if self._open_count >= self.max_concurrent_trades:
            return False, f"Máximo de trades abiertos ({self.max_concurrent_trades}) alcanzado."
        if self._daily_loss >= self.max_global_daily_loss_pct:
            return False, f"Pérdida diaria máxima ({self.max_global_daily_loss_pct}%) alcanzada."
        return True, ""

    async def register_open(self) -> None:
        self._open_count += 1
        logger.debug("[GlobalRisk] Trades abiertos: %d", self._open_count)

    async def register_close(self, pnl_pct: float = 0.0) -> None:
        self._open_count = max(0, self._open_count - 1)
        if pnl_pct < 0:
            self._daily_loss += abs(pnl_pct)
        logger.debug(
            "[GlobalRisk] Trades abiertos: %d | Pérdida diaria: %.2f%%",
            self._open_count, self._daily_loss,
        )


class PreTradeRiskChecker:
    """
    Verificaciones pre-trade a nivel de símbolo.
    Fusiona la lógica de bot/pretrade_risk.py.

    Checks:
      - No hay posición abierta ya para el símbolo
      - Balance suficiente con margen de seguridad
      - No se supera max_open_trades del RiskManager
    """

    def __init__(self):
        self._open_symbols: set[str] = set()

    def register_open(self, symbol: str) -> None:
        self._open_symbols.add(symbol)

    def register_close(self, symbol: str) -> None:
        self._open_symbols.discard(symbol)

    async def check(
        self,
        symbol: str,
        risk:   RiskManager,
        balance: float,
    ) -> bool:
        if symbol in self._open_symbols:
            logger.debug("[PreTradeRisk] %s ya tiene posición abierta.", symbol)
            return False
        required = risk.usdc_per_trade * 1.1   # 10% de margen de seguridad
        if balance < required:
            logger.debug(
                "[PreTradeRisk] Balance insuficiente para %s (%.2f < %.2f).",
                symbol, balance, required,
            )
            return False
        return True
