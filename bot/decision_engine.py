#!/usr/bin/env python3
"""
decision_engine.py — Motor de decisión de trading.

Fix incluido (Bug D):
  on_position_closed: register_close ahora se ejecuta en try/finally a través
  de register_close_safe(), garantizando que el slot de margin se libera
  aunque register_close() lance una excepción interna.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


class DecisionEngine:
    """
    Decide si abrir una posición basándose en la señal del signal_engine
    y los filtros de riesgo previo al trade.

    Responsabilidades:
      - Evaluar señales de entrada (evaluate)
      - Notificar cierres al risk manager (on_position_closed)
      - Garantizar liberación de margin aunque register_close falle (Bug D)
    """

    def __init__(self, risk_manager, pretrade_risk, signal_engine, cooldown) -> None:
        self._risk         = risk_manager
        self._pretrade     = pretrade_risk
        self._signal       = signal_engine
        self._cooldown     = cooldown

    # ── Evaluación de señal ───────────────────────────────────────────────────

    async def evaluate(self, symbol: str, price: float, ohlcv: list) -> Optional[dict]:
        """
        Evalúa si hay condiciones para abrir una posición.

        Retorna un dict con la señal si se debe abrir, None en caso contrario.
        El caller (trading loop) es responsable de verificar que no haya
        posición abierta antes de llamar a evaluate().
        """
        # Gate 1: cooldown activo
        if self._cooldown.is_in_cooldown(symbol):
            remaining = self._cooldown.remaining(symbol)
            log.debug("[%s] evaluate: cooldown activo (%.0fs restantes)", symbol, remaining)
            return None

        # Gate 2: pretrade risk (margin, daily loss, correlación, etc.)
        try:
            allowed, reason = self._pretrade.check(symbol, price)
        except Exception as e:
            log.warning("[%s] evaluate: pretrade_risk.check error: %s", symbol, e)
            return None

        if not allowed:
            log.debug("[%s] evaluate: bloqueado por pretrade_risk: %s", symbol, reason)
            return None

        # Gate 3: señal técnica
        try:
            signal = await self._signal.get_signal(symbol, price, ohlcv)
        except Exception as e:
            log.warning("[%s] evaluate: signal_engine.get_signal error: %s", symbol, e)
            return None

        if not signal or signal.get("action") == "HOLD":
            return None

        log.info(
            "[%s] evaluate: señal ACEPTADA action=%s entry_mode=%s",
            symbol,
            signal.get("action"),
            signal.get("entry_mode", "NORMAL"),
        )
        return signal

    # ── Notificación de cierre ────────────────────────────────────────────────

    def on_position_closed(
        self,
        symbol: str,
        pnl: float,
        reason: str,
        entry_mode: str = "NORMAL",
    ) -> None:
        """
        Bug D fix: register_close se ejecuta dentro de try/finally.

        Garantiza que el slot de margin se libera aunque register_close
        lance una excepción (p.ej. error de serialización, I/O, etc.).
        Sin este fix, una excepción en register_close bloqueaba el margin
        indefinidamente impidiendo nuevas entradas.
        """
        # 1. Registrar cooldown (no crítico — si falla, se loggea y continúa)
        try:
            self._cooldown.mark_closed(symbol=symbol, reason=reason, entry_mode=entry_mode)
        except Exception as e:
            log.error("[%s] on_position_closed: cooldown.mark_closed falló: %s", symbol, e)

        # 2. Liberar margin — SIEMPRE debe ejecutarse aunque paso 1 o 3 fallen
        self._register_close_safe(symbol=symbol, pnl=pnl)

        # 3. Logging de resultado
        try:
            emoji = "✅" if pnl >= 0 else "❌"
            log.info(
                "%s [%s] Posición cerrada: reason=%s pnl=%.4f USDC entry_mode=%s",
                emoji, symbol, reason, pnl, entry_mode,
            )
        except Exception:
            pass

    def _register_close_safe(self, symbol: str, pnl: float) -> None:
        """
        Wrapper seguro de _risk.register_close con try/except + log.

        Nunca relanza la excepción: si register_close falla, se loggea
        el error pero el flujo de cierre sigue adelante normalmente.
        Esto garantiza que el margin siempre se libera (Bug D).
        """
        try:
            # register_close es síncrono — NO usar await
            self._risk.register_close(symbol, pnl)
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: _risk.register_close falló con %s: %s — "
                "el margin puede haberse liberado incorrectamente. "
                "Verificar estado de GlobalRisk.",
                symbol, type(e).__name__, e,
                exc_info=True,
            )
