"""
decision_engine.py — Toma de decisiones de trading.

Responsabilidades:
  - Obtener precio y OHLCV
  - Verificar cooldown de reapertura (signal_cooldown)
  - Verificar balance y risk pre-trade (pretrade_risk con firma correcta)
  - Llamar a decide() + ai_decide()
  - Calcular qty y apalancamiento
  - Abrir posición y persistir estado
  - Registrar exposición en pretrade_risk tras fill confirmado
  - Notificar apertura via Telegram

Extraído de FuturesTrader._iteration en trader.py.
"""
from __future__ import annotations

import logging
import os

from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open
from bot.state import save_position
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.signal_cooldown import signal_cooldown

logger = logging.getLogger("DecisionEngine")


class DecisionEngine:
    """
    Encapsula la lógica de decisión de entrada al mercado.
    Recibe el trader como contexto para acceder a precio, OHLCV y órdenes.
    """

    def __init__(self, symbol: str, position_mgr=None):
        self.symbol = symbol
        # Referencia al PositionManager para comunicar entry_mode al cooldown
        self._position_mgr = position_mgr

    def bind_position_manager(self, pm) -> None:
        """Conecta el PositionManager para que reciba entry_mode al abrir."""
        self._position_mgr = pm

    async def evaluate(
        self,
        trader,
        risk,
        global_risk=None,
    ) -> None:
        """
        Evalúa si se debe abrir una posición. Si la decisión es BUY/SELL,
        coloca la orden y actualiza el estado del trader.
        """
        if kill_switch.is_halted(self.symbol):
            logger.debug("[%s] Kill switch activo — skip.", self.symbol)
            return

        # ── #4 Cooldown de reapertura ─────────────────────────────────────────
        if signal_cooldown.is_blocked(self.symbol):
            remaining = signal_cooldown.remaining(self.symbol)
            logger.debug(
                "[%s] Cooldown activo — %.0fs restantes.", self.symbol, remaining
            )
            return

        # ── Verificaciones previas ────────────────────────────────────────────
        if global_risk:
            allowed, reason = await global_risk.can_open()
            if not allowed:
                logger.debug("[%s] GlobalRisk: %s", self.symbol, reason)
                return

        balance = await trader.get_balance()
        if balance is not None and balance < risk.usdc_per_trade:
            logger.warning(
                "[%s] Balance insuficiente (%.2f < %.2f USDC).",
                self.symbol, balance, risk.usdc_per_trade,
            )
            return

        # ── Señal de estrategia (necesitamos precio y señal antes del check) ──
        try:
            exch     = await trader._get_ccxt()
            decision = await decide(
                exch=exch,
                symbol=self.symbol,
                ai_decide_fn=ai_decide,
                has_open_position=False,
                current_pnl=None,
            )
        except Exception as e:
            logger.error("[%s] decide() error: %s", self.symbol, e)
            return

        action = decision.get("action", "HOLD")
        signal = decision.get("signal")
        if action not in ("BUY", "SELL"):
            return

        # ── Calcular parámetros de la orden ───────────────────────────────────
        try:
            price = await trader.get_price()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
            return

        if signal:
            entry      = signal.entry or price
            sl         = signal.sl
            tp1        = signal.tp1
            tp2        = signal.tp2
            tp3        = getattr(signal, "tp3", None)
            lev        = signal.suggested_lev or trader.leverage
            entry_mode = getattr(signal, "entry_mode", "") or ""
        else:
            entry      = price
            sl = tp1 = tp2 = tp3 = None
            lev        = trader.leverage
            entry_mode = ""

        lev = min(int(lev), trader.leverage)
        if lev != trader.leverage:
            await trader._set_leverage(lev)

        notional = risk.usdc_per_trade * lev
        side     = "buy" if action == "BUY" else "sell"

        # ── pretrade_risk.check() con firma correcta ──────────────────────────
        # La firma es: check(symbol, side, notional, price, balance, sl, leverage)
        # Devuelve tuple (ok: bool, reason: str) — NO es un bool directo.
        try:
            ok, reason = await pretrade_risk.check(
                symbol=self.symbol,
                side=side,
                notional=notional,
                price=entry,
                balance=balance,
                sl=sl,
                leverage=lev,
            )
            if not ok:
                logger.debug("[%s] pretrade_risk bloqueó la entrada: %s", self.symbol, reason)
                return
        except Exception as e:
            logger.warning("[%s] pretrade_risk.check() error: %s — continuando", self.symbol, e)

        # FIX: respetar szDecimals del coin — usar _round_qty del trader
        qty = trader._round_qty(notional / entry)
        if qty <= 0:
            logger.warning("[%s] qty <= 0 tras redondeo szDecimals, skip.", self.symbol)
            return

        logger.info(
            "[%s] 📈 Abriendo %s · qty=%s · entry=~%s · sl=%s · tp1=%s | %s",
            self.symbol, action, qty, round(entry, 4),
            round(sl, 4) if sl else "N/A",
            round(tp1, 4) if tp1 else "N/A",
            decision.get("reason", ""),
        )

        # ── Ejecutar orden ────────────────────────────────────────────────────
        result = (
            {"status": "ok", "_fill_price": entry}
            if trader.dry_run
            else await trader._place_order(side, qty, sl=sl, tp=tp1)
        )

        if result.get("status") != "ok":
            return

        # ── pretrade_risk: registrar exposición tras fill confirmado ──────────
        pretrade_risk.confirm_order(self.symbol, notional)

        try:
            fill_price = float(
                result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                .get("filled", {}).get("avgPx") or entry
            )
        except Exception:
            fill_price = entry

        if fill_price and fill_price != entry:
            # FIX: también szDecimals al recalcular con fill real
            qty = trader._round_qty(notional / fill_price)
            logger.debug(
                "[%s] Fill real: %.4f (estimado: %.4f) — qty ajustada a %s",
                self.symbol, fill_price, entry, qty,
            )

        # ── Actualizar estado del trader ──────────────────────────────────────
        trader.position       = "long" if action == "BUY" else "short"
        trader.entry_price    = fill_price
        trader.sl             = sl
        trader.tp1            = tp1
        trader.tp2            = tp2
        trader.tp3            = tp3
        trader.tp2_hit        = False
        trader._tp1_be_done   = False
        trader._open_notional = notional
        trader._open_leverage = lev
        trader._protection_ok = False
        trader.trade_count   += 1

        # Comunicar entry_mode al PositionManager para cooldown correcto al cerrar
        if self._position_mgr is not None:
            self._position_mgr.set_entry_mode(entry_mode)

        save_position(self.symbol, {
            "side":        trader.position,
            "entry":       trader.entry_price,
            "sl":          trader.sl,
            "tp1":         trader.tp1,
            "tp2":         trader.tp2,
            "tp3":         trader.tp3,
            "tp2_hit":     trader.tp2_hit,
            "usdc_amount": notional,
            "leverage":    lev,
        })

        if global_risk:
            await global_risk.register_open()

        await notify_open(
            symbol=self.symbol,
            side=trader.position,
            entry=trader.entry_price,
            sl=trader.sl,
            tp1=trader.tp1,
            tp2=trader.tp2,
            size_usdc=notional,
            leverage=lev,
            signal_block=decision.get("signal_block", ""),
            ai_used=decision.get("ai_used", False),
            ai_confidence=decision.get("ai_confidence", 0),
        )
