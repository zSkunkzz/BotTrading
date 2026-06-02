#!/usr/bin/env python3
"""
decision_engine.py — Motor de decisión de trading.

Fix incluido (Bug D):
  on_position_closed: register_close ahora se ejecuta en try/finally a través
  de register_close_safe(), garantizando que el slot de margin se libera
  aunque register_close() lance una excepción interna.

Fix Bug K (2026-06-02):
  decision_engine llamaba a self._signal.get_signal() que NO existe en
  signal_engine.py — solo existe analyze_pair(). El AttributeError era
  silenciado por el try/except en evaluate(), haciendo que Gate 3 nunca
  pasara y el bot nunca abriera posiciones.
  Fix: llamar a analyze_pair() directamente y convertir SignalResult al
  dict que espera el caller ({action, entry_mode, sl, tp1, tp2, ...}).

Fix Bug P (2026-06-02):
  Gate 2 llamaba self._pretrade.check(symbol, price) SIN await y con firma
  incorrecta. check() es async y requiere (symbol, margin=..., ...) — no
  (symbol, price). Sin await devuelve una coroutine siempre truthy → nunca
  bloquea nada y se filtra silenciosamente. Ahora se llama correctamente
  con await y se pasa margin desde la configuración del trader.

  _register_close_safe llamaba self._risk.register_close() (GlobalRisk)
  que es async, sin await. Corregido: on_position_closed ahora es async
  y usa await para ambas llamadas.
"""
from __future__ import annotations

import asyncio
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
        self._signal       = signal_engine   # módulo signal_engine (no una instancia)
        self._cooldown     = cooldown

    # ── Evaluación de señal ───────────────────────────────────────────────────

    async def evaluate(
        self,
        symbol: str,
        price: float,
        ohlcv: list,
        margin: float = 0.0,
        leverage: float = 1.0,
    ) -> Optional[dict]:
        """
        Evalúa si hay condiciones para abrir una posición.

        Retorna un dict con la señal si se debe abrir, None en caso contrario.
        El caller (trading loop) es responsable de verificar que no haya
        posición abierta antes de llamar a evaluate().

        Bug K fix: usa analyze_pair() de signal_engine (no get_signal() que
        no existe) y convierte SignalResult → dict para compatibilidad.

        Bug P fix: await pretrade_risk.check() con firma correcta.
        """
        # Gate 1: cooldown activo
        if self._cooldown.is_in_cooldown(symbol):
            remaining = self._cooldown.remaining(symbol)
            log.debug("[%s] evaluate: cooldown activo (%.0fs restantes)", symbol, remaining)
            return None

        # Gate 2: pretrade risk (margin, daily loss, correlación, etc.)
        # Bug P fix: check() es ASYNC → await; firma correcta con margin/leverage/price
        try:
            allowed, reason = await self._pretrade.check(
                symbol=symbol,
                price=price,
                margin=margin if margin > 0 else 1.0,  # evitar margin=0 que siempre bloquea
                leverage=leverage,
            )
        except Exception as e:
            log.warning("[%s] evaluate: pretrade_risk.check error: %s", symbol, e)
            return None

        if not allowed:
            log.debug("[%s] evaluate: bloqueado por pretrade_risk: %s", symbol, reason)
            return None

        # Gate 3: señal técnica
        # Bug K fix: signal_engine expone analyze_pair(), NO get_signal().
        # Llamamos analyze_pair() y convertimos SignalResult → dict.
        try:
            from bot.signal_engine import analyze_pair
            result = await analyze_pair(exch=None, symbol=symbol, ohlcv_fn=_make_ohlcv_fn(ohlcv))
        except Exception as e:
            log.warning("[%s] evaluate: analyze_pair error: %s", symbol, e)
            return None

        if result is None or not result.is_valid:
            log.debug("[%s] evaluate: señal inválida — %s", symbol, getattr(result, 'reason', ''))
            return None

        if result.signal not in ("LONG", "SHORT"):
            log.debug("[%s] evaluate: señal NEUTRAL/HOLD — sin entrada", symbol)
            return None

        # Convertir SignalResult → dict compatible con el caller
        signal = {
            "action":      "BUY" if result.signal == "LONG" else "SELL",
            "side":        "long" if result.signal == "LONG" else "short",
            "entry_mode":  result.entry_mode,
            "entry":       result.entry,
            "sl":          result.sl,
            "tp1":         result.tp1,
            "tp2":         result.tp2,
            "atr":         result.atr,
            "rr":          result.rr,
            "score":       result.score,
            "max_score":   result.max_score,
            "leverage":    result.suggested_lev,
            "indicators":  result.indicators,
            "reason":      result.reason,
        }

        log.info(
            "[%s] evaluate: señal ACEPTADA action=%s entry_mode=%s score=%d/%d rr=%.2f",
            symbol,
            signal["action"],
            signal["entry_mode"],
            result.score,
            result.max_score,
            result.rr,
        )
        return signal

    # ── Notificación de cierre ────────────────────────────────────────────────

    async def on_position_closed(
        self,
        symbol: str,
        pnl: float,
        reason: str,
        entry_mode: str = "NORMAL",
    ) -> None:
        """
        Bug D fix: register_close se ejecuta dentro de try/finally.
        Bug P fix: on_position_closed ahora es async para poder awaitar
          GlobalRisk.register_close() que es async.

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
        await self._register_close_safe(symbol=symbol, pnl=pnl)

        # 3. Logging de resultado
        try:
            emoji = "✅" if pnl >= 0 else "❌"
            log.info(
                "%s [%s] Posición cerrada: reason=%s pnl=%.4f USDC entry_mode=%s",
                emoji, symbol, reason, pnl, entry_mode,
            )
        except Exception:
            pass

    async def _register_close_safe(self, symbol: str, pnl: float) -> None:
        """
        Bug P fix: GlobalRisk.register_close es ASYNC → requiere await.
        PreTradeRisk.register_close es SYNC → llamar directamente.

        Wrapper seguro: nunca relanza la excepción — si falla, se loggea
        y el flujo de cierre sigue adelante normalmente.
        """
        # GlobalRisk (async)
        try:
            await self._risk.register_close(pnl_pct=pnl, symbol=symbol)
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: GlobalRisk.register_close falló: %s — "
                "verificar estado de GlobalRisk.",
                symbol, e,
                exc_info=True,
            )

        # PreTradeRisk (sync) — libera el margen reservado para este symbol
        try:
            self._pretrade.register_close(symbol=symbol, notional_or_margin=0.0)
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: PreTradeRisk.register_close falló: %s",
                symbol, e,
                exc_info=True,
            )


# ── Helper: adaptar lista ohlcv plana a ohlcv_fn callable ────────────────────

def _make_ohlcv_fn(ohlcv_data: list):
    """
    Bug K fix: analyze_pair() acepta un callable async ohlcv_fn(timeframe) -> list.
    Cuando DecisionEngine recibe ohlcv como lista plana (un solo timeframe),
    este helper lo envuelve en un callable compatible.

    Si ohlcv_data es un dict {tf: bars}, lo sirve directamente por timeframe.
    Si es una lista plana, se asume que son velas 15m y se sirve para cualquier TF.
    """
    if isinstance(ohlcv_data, dict):
        async def _fn(tf: str):
            return ohlcv_data.get(tf, [])
    else:
        async def _fn(tf: str):
            return ohlcv_data if tf == "15m" else []
    return _fn
