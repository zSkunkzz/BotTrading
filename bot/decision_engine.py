"""
bot/decision_engine.py  –  High-level trade decision con todos los gates.

Gates en orden:
  1. Signal válido (side presente)
  2. Cooldown dinámico por símbolo (signal_cooldown)
  3. Market regime gate (market_regime.verify_regime_gate)
  4. Pre-trade risk (pretrade_risk.check)

Despues de aprobar:
  5. Kelly sizing → ajusta el margin efectivo
  6. on_order_confirmed() → registra en pretrade_risk
  7. on_position_closed() → libera margin + actualiza cooldown

INTEGRATION_NOTE para trader.py:
  Al cerrar posición (SL/TP/timeout) llamar:
    await decision_engine.on_position_closed(
        symbol, margin=<margin_original>, reason='SL'|'TP1'|'TP2'|'TP3'|'TIMEOUT',
        entry_mode=<mode>
    )

FIX 2026-06-02:
  on_position_closed llamaba await self._risk.register_close() pero
  pretrade_risk.register_close() es síncrono (no corrutina). Llamar
  await sobre un método síncrono devuelve None y lanza:
    TypeError: object NoneType can't be used in 'await' expression
  Fix: llamada directa sin await (es thread-safe ya que no usa asyncio.Lock
  en el path de escritura, sólo en check()).
"""
from __future__ import annotations

import logging
import pandas as pd
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)


class DecisionEngine:
    """
    Wraps signal evaluation + pre-trade risk en una sola corrutina.

    Parameters
    ----------
    pretrade_risk : PreTradeRisk
    signal_engine : SignalEngine | None
    usdc_per_trade : float  — margin base por trade
    leverage : int
    """

    def __init__(
        self,
        pretrade_risk,
        signal_engine=None,
        usdc_per_trade: float = 50.0,
        leverage: int = 10,
    ) -> None:
        self._risk     = pretrade_risk
        self._signals  = signal_engine
        self._usdc     = usdc_per_trade
        self._leverage = leverage

    # ── main entry point ────────────────────────────────────────────────────

    async def evaluate(
        self,
        symbol: str,
        signal: Dict[str, Any],
        price: float,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Returns (approved, reason, enriched_signal).
        """
        side: str = signal.get("side", "")
        if not side:
            return False, "Signal missing 'side'", None

        # ── Gate 2: Cooldown dinámico ──────────────────────────────────────
        try:
            from bot.signal_cooldown import signal_cooldown
            if signal_cooldown.is_in_cooldown(symbol):
                rem = signal_cooldown.remaining(symbol)
                nsl = signal_cooldown.consecutive_sl(symbol)
                reason = (
                    f"Cooldown activo para {symbol}: {rem:.0f}s restantes"
                    f" (SL consecutivos: {nsl})"
                )
                log.info("[DecisionEngine] %s", reason)
                return False, reason, None
        except Exception as e:
            log.debug("[DecisionEngine] signal_cooldown no disponible: %s", e)

        # ── Gate 3: Market regime ───────────────────────────────────────
        try:
            from bot.market_regime import verify_regime_gate
            closes_1h = signal.get("indicators", {}).get("_closes_1h", [])
            if closes_1h and len(closes_1h) >= 30:
                df_regime = pd.DataFrame({"close": closes_1h})
                df_regime["high"]  = df_regime["close"] * 1.001
                df_regime["low"]   = df_regime["close"] * 0.999
                allowed, reg_reason = verify_regime_gate(df_regime, symbol)
                if not allowed:
                    return False, reg_reason, None
        except Exception as e:
            log.debug("[DecisionEngine] market_regime no disponible: %s", e)

        # ── Gate 4: Pre-trade risk ───────────────────────────────────────
        margin_base = signal.get("_margin", self._usdc)

        ok, reason = await self._risk.check(
            symbol=symbol,
            side=side,
            margin=margin_base,
            price=price,
        )
        if not ok:
            log.info("[DecisionEngine] %s rejected by pretrade_risk: %s", symbol, reason)
            return False, reason, None

        # ── Kelly sizing ─────────────────────────────────────────────
        entry_mode = signal.get("entry_mode", "NORMAL")
        rr         = float(signal.get("rr", 2.0))
        try:
            from bot.kelly_sizer import kelly_multiplier
            k_mult  = kelly_multiplier(entry_mode, rr)
            margin  = round(margin_base * k_mult, 4)
        except Exception as e:
            log.debug("[DecisionEngine] kelly_sizer no disponible: %s", e)
            margin = margin_base
            k_mult = 1.0

        enriched = {
            **signal,
            "_margin":            margin,
            "_margin_base":       margin_base,
            "_kelly_mult":        k_mult,
            "_leverage":          self._leverage,
            "_price_at_decision": price,
        }
        return True, "", enriched

    # ── lifecycle callbacks ─────────────────────────────────────────────

    async def on_order_confirmed(
        self, symbol: str, margin: Optional[float] = None
    ) -> None:
        """
        Llamar después de que el exchange confirme la orden.
        Registra el margin en el risk ledger + sella el rate-limiter.
        """
        m = margin if margin is not None else self._usdc
        # confirm_order es síncrono — llamada directa
        self._risk.confirm_order(symbol=symbol, notional_or_margin=m)
        log.debug("[DecisionEngine] order confirmed %s (margin %.2f)", symbol, m)

    async def on_position_closed(
        self,
        symbol: str,
        margin: Optional[float] = None,
        reason: str = "SL",
        entry_mode: str = "NORMAL",
    ) -> None:
        """
        Llamar cuando una posición se cierra completamente.
        Libera el margin + actualiza el cooldown dinámico.

        FIX 2026-06-02: register_close() es síncrono en PreTradeRisk.
        Llamar con await generaba:
          TypeError: object NoneType can't be used in 'await' expression
        Fix: llamada directa (síncrona).

        Parameters
        ----------
        reason : str
            'SL' | 'TP1' | 'TP2' | 'TP3' | 'TIMEOUT'
        entry_mode : str
            Modo de entrada original (para escalar el cooldown correctamente)
        """
        m = margin if margin is not None else self._usdc
        # FIX: register_close es síncrono — NO usar await
        self._risk.register_close(symbol=symbol, notional_or_margin=m)
        log.debug("[DecisionEngine] position closed %s (%.2f released, reason=%s)", symbol, m, reason)

        try:
            from bot.signal_cooldown import signal_cooldown
            signal_cooldown.mark_closed(symbol, reason=reason, entry_mode=entry_mode)
        except Exception as e:
            log.debug("[DecisionEngine] signal_cooldown.mark_closed error: %s", e)
