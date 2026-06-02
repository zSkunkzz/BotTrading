#!/usr/bin/env python3
"""
decision_engine.py — Motor de decisión de trading.

Fix ohlcv_fn (2026-06-02):
  evaluate() ahora acepta ohlcv como Callable (ohlcv_fn) además de list.
  Si es callable, lo pasa directamente a analyze_pair(ohlcv_fn=...).
  Si es list (legado), usa _make_ohlcv_fn() como antes.

Fix incluido (Bug D):
  on_position_closed: register_close ahora se ejecuta en try/finally a través
  de register_close_safe(), garantizando que el slot de margin se libera
  aunque register_close() lance una excepción interna.

Fix Bug K (2026-06-02):
  decision_engine llamaba a self._signal.get_signal() que NO existe en
  signal_engine.py — solo existe analyze_pair(). El AttributeError era
  silenciado por el try/except en evaluate(), haciendo que Gate 3 nunca
  pasara y el bot nunca abriera posiciones.
  Fix: llamar a analyze_pair() directamente.

Fix Bug P (2026-06-02):
  Gate 2 llamaba self._pretrade.check(symbol, price) SIN await y con firma
  incorrecta. check() es async y requiere (symbol, margin=..., ...).
  Corregido: await con firma correcta.

  _register_close_safe llamaba self._risk.register_close() (GlobalRisk)
  que es async, sin await. Corregido.

Fix Bug Q (2026-06-03):
  Gate 2: margin=1.0 es un fallback que bloqueaba trades válidos cuando
  no se pasaba un margin real. Ahora si margin <= 1.0 (fallback), se omite
  el check de open_margin (solo se aplica rate limiting).
  Además, evaluate() recibe usdc_per_trade del risk para pasar margin real.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Callable, Optional, Union

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


class DecisionEngine:

    def __init__(self, risk_manager, pretrade_risk, signal_engine, cooldown) -> None:
        self._risk         = risk_manager
        self._pretrade     = pretrade_risk
        self._signal       = signal_engine
        self._cooldown     = cooldown

    # ── Evaluación de señal ───────────────────────────────────────────────────

    async def evaluate(
        self,
        symbol: str,
        price: float,
        ohlcv: Union[list, Callable],
        margin: float = 0.0,
        leverage: float = 1.0,
    ) -> Optional[dict]:
        """
        Evalúa si hay condiciones para abrir una posición.

        ohlcv puede ser:
          - un Callable async (ohlcv_fn) → se pasa directamente a analyze_pair
          - una list (legado) → se envuelve con _make_ohlcv_fn
        """
        # Gate 1: cooldown activo
        if self._cooldown.is_in_cooldown(symbol):
            remaining = self._cooldown.remaining(symbol)
            log.debug("[%s] evaluate: cooldown activo (%.0fs restantes)", symbol, remaining)
            return None

        # Gate 2: pretrade risk
        # FIX Bug Q: si margin es 0 o el fallback 1.0, intentar obtenerlo del risk_manager
        effective_margin = margin
        if effective_margin <= 1.0:
            try:
                usdc = float(getattr(self._risk, "usdc_per_trade", 0) or 0)
                lev  = float(getattr(self._risk, "leverage", 1) or 1)
                if usdc > 0:
                    effective_margin = usdc  # margen = capital por trade (no notional)
            except Exception:
                pass

        # Si seguimos sin margin válido, usar 10.0 como fallback conservador
        if effective_margin <= 0:
            effective_margin = 10.0

        try:
            allowed, reason = await self._pretrade.check(
                symbol=symbol,
                price=price,
                margin=effective_margin,
                leverage=leverage,
            )
        except Exception as e:
            log.warning("[%s] evaluate: pretrade_risk.check error: %s", symbol, e)
            return None

        if not allowed:
            log.debug("[%s] evaluate: bloqueado por pretrade_risk: %s", symbol, reason)
            return None

        # Gate 3: señal técnica
        # FIX ohlcv_fn: si ohlcv es callable, usarlo directamente.
        # Si es list (legado), envolver con _make_ohlcv_fn.
        if callable(ohlcv):
            ohlcv_fn = ohlcv
        else:
            ohlcv_fn = _make_ohlcv_fn(ohlcv)

        try:
            from bot.signal_engine import analyze_pair
            result = await analyze_pair(exch=None, symbol=symbol, ohlcv_fn=ohlcv_fn)
        except Exception as e:
            log.warning("[%s] evaluate: analyze_pair error: %s", symbol, e)
            return None

        if result is None or not result.is_valid:
            log.debug("[%s] evaluate: señal inválida — %s", symbol, getattr(result, 'reason', ''))
            return None

        if result.signal not in ("LONG", "SHORT"):
            log.debug("[%s] evaluate: señal NEUTRAL/HOLD — sin entrada", symbol)
            return None

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
        try:
            self._cooldown.mark_closed(symbol=symbol, reason=reason, entry_mode=entry_mode)
        except Exception as e:
            log.error("[%s] on_position_closed: cooldown.mark_closed falló: %s", symbol, e)

        await self._register_close_safe(symbol=symbol, pnl=pnl)

        try:
            emoji = "✅" if pnl >= 0 else "❌"
            log.info(
                "%s [%s] Posición cerrada: reason=%s pnl=%.4f USDC entry_mode=%s",
                emoji, symbol, reason, pnl, entry_mode,
            )
        except Exception:
            pass

    async def _register_close_safe(self, symbol: str, pnl: float) -> None:
        try:
            await self._risk.register_close(pnl_pct=pnl, symbol=symbol)
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: GlobalRisk.register_close falló: %s",
                symbol, e,
                exc_info=True,
            )

        try:
            self._pretrade.register_close(symbol=symbol, notional_or_margin=0.0)
        except Exception as e:
            log.error(
                "[%s] CRÍTICO: PreTradeRisk.register_close falló: %s",
                symbol, e,
            )


# ── Helper: adaptar lista ohlcv plana a ohlcv_fn callable ────────────────────

def _make_ohlcv_fn(ohlcv_data: list):
    """
    Compatibilidad legado: envuelve lista OHLCV en callable async.
    """
    if isinstance(ohlcv_data, dict):
        async def _fn(tf: str):
            return ohlcv_data.get(tf, [])
    else:
        async def _fn(tf: str):
            return ohlcv_data if tf == "15m" else []
    return _fn
