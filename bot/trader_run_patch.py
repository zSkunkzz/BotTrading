"""
trader_run_patch.py — Añade el método run() a FuturesTrader en tiempo de importación.
Importar este módulo ANTES de usar FuturesTrader (ya está importado en main.py a través
de bot/trader.py — ver instrucciones).

USAGE: borra este archivo cuando muevas run() directamente a trader.py.
"""
import asyncio
import logging
import os
import time

from bot.trader import FuturesTrader
from bot.risk import RiskManager
from bot.global_risk import GlobalRisk
from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close, notify_tp_partial
from bot.state import save_position, clear_position, mark_tp2_hit

logger = logging.getLogger("Trader")

LOOP_SLEEP = float(os.getenv("LOOP_SLEEP", "10"))


async def _run(self: FuturesTrader, risk: RiskManager, *, global_risk: GlobalRisk = None):
    """
    Loop principal del trader para un símbolo.
    Inicializa la conexión y ejecuta el ciclo de trading indefinidamente.
    """
    await self._init(risk.usdc_per_trade)

    while True:
        try:
            await _iteration(self, risk, global_risk)
        except asyncio.CancelledError:
            logger.info("[%s] Trader cancelado.", self.symbol)
            raise
        except Exception as e:
            logger.error("[%s] Error en iteración: %s", self.symbol, e, exc_info=True)

        await asyncio.sleep(LOOP_SLEEP)


async def _iteration(self: FuturesTrader, risk: RiskManager, global_risk: GlobalRisk):
    """Una iteración del loop de trading."""
    from bot.kill_switch import kill_switch

    # Kill switch activo → no operar
    if kill_switch.is_halted(self.symbol):
        logger.debug("[%s] Kill switch activo — skip.", self.symbol)
        return

    # ── Obtener precio actual ────────────────────────────────────────
    try:
        price = await self.get_price()
    except Exception as e:
        logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
        return

    # ── Verificar posición abierta en el exchange ────────────────────
    now = time.monotonic()
    exchange_positions = []
    if now - self._last_pos_check_at >= _POS_CHECK_INTERVAL_S:
        exchange_positions = await self._get_positions()
        self._last_pos_check_at = now

    # Sincronizar estado local con el exchange
    if exchange_positions:
        ep = exchange_positions[0]
        if self.position is None:
            # Posición abierta en el exchange pero no registrada localmente
            self.position    = ep["side"]
            self.entry_price = ep["entryPx"]
            logger.info("[%s] Posición detectada en exchange: %s @ %s",
                        self.symbol, self.position, self.entry_price)
    elif not exchange_positions and self.position is not None:
        # Posición cerrada externamente
        logger.info("[%s] Posición cerrada externamente.", self.symbol)
        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

    has_position = self.position is not None

    # ── Gestión de posición abierta ──────────────────────────────────
    if has_position:
        await _manage_open_position(self, price, risk)
        return

    # ── Verificar límites de riesgo global ──────────────────────────
    if global_risk:
        allowed, reason = await global_risk.can_open()
        if not allowed:
            logger.debug("[%s] GlobalRisk: %s", self.symbol, reason)
            return

    # ── Check de balance mínimo ─────────────────────────────────────
    balance = await self.get_balance()
    if balance is not None and balance < risk.usdc_per_trade:
        logger.warning("[%s] Balance insuficiente (%.2f < %.2f USDC).",
                       self.symbol, balance, risk.usdc_per_trade)
        return

    # ── Pre-trade risk check ────────────────────────────────────────
    try:
        from bot.pretrade_risk import pretrade_risk
        if not await pretrade_risk.check(self.symbol, risk, balance or 0.0):
            logger.debug("[%s] pretrade_risk bloqueó la entrada.", self.symbol)
            return
    except Exception as e:
        logger.debug("[%s] pretrade_risk error (ignorando): %s", self.symbol, e)

    # ── Decisión de trading ─────────────────────────────────────────
    try:
        decision = await decide(
            exch=self.exchange,
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

    # ── Calcular niveles de entrada ─────────────────────────────────
    if signal:
        entry  = signal.entry  or price
        sl     = signal.sl
        tp1    = signal.tp1
        tp2    = signal.tp2
        tp3    = getattr(signal, "tp3", None)
        lev    = signal.suggested_lev or self.leverage
    else:
        entry = price
        sl = tp1 = tp2 = tp3 = None
        lev = self.leverage

    # Usar leverage sugerido por la señal (dentro del máximo configurado)
    lev = min(int(lev), self.leverage)
    if lev != self.leverage:
        await self._set_leverage(lev)

    # Calcular tamaño
    notional = risk.usdc_per_trade * lev
    qty = round(notional / entry, 6)
    if qty <= 0:
        logger.warning("[%s] Cantidad calculada <= 0, skip.", self.symbol)
        return

    side = "buy" if action == "BUY" else "sell"

    logger.info(
        "[%s] 📈 Abriendo %s · qty=%s · entry=~%s · sl=%s · tp1=%s | %s",
        self.symbol, action, qty, round(entry, 4),
        round(sl, 4) if sl else "N/A",
        round(tp1, 4) if tp1 else "N/A",
        decision.get("reason", ""),
    )

    if self.dry_run:
        result = {"status": "ok"}
    else:
        result = await self._place_order(side, qty, sl=sl, tp=tp1)

    if result.get("status") == "ok":
        self.position    = "long" if action == "BUY" else "short"
        self.entry_price = entry
        self.sl          = sl
        self.tp1         = tp1
        self.tp2         = tp2
        self.tp3         = tp3
        self.tp2_hit     = False
        self._open_notional = notional
        self._open_leverage = lev
        self._protection_ok = False
        self.trade_count += 1

        save_position(self.symbol, {
            "side":        self.position,
            "entry":       self.entry_price,
            "sl":          self.sl,
            "tp1":         self.tp1,
            "tp2":         self.tp2,
            "tp3":         self.tp3,
            "tp2_hit":     self.tp2_hit,
            "usdc_amount": notional,
            "leverage":    lev,
        })

        if global_risk:
            await global_risk.register_open()

        await notify_open(
            symbol=self.symbol,
            side=self.position,
            entry=self.entry_price,
            sl=self.sl,
            tp1=self.tp1,
            tp2=self.tp2,
            size_usdc=notional,
            leverage=lev,
            signal_block=decision.get("signal_block", ""),
            ai_used=decision.get("ai_used", False),
            ai_confidence=decision.get("ai_confidence", 0),
        )


async def _manage_open_position(self: FuturesTrader, price: float, risk: RiskManager):
    """Gestiona TP parciales, trailing stop y cierre de posición."""
    if self.position is None or self.entry_price is None:
        return

    is_long = self.position == "long"
    pnl_pct = ((price - self.entry_price) / self.entry_price) * (1 if is_long else -1) * 100

    # ── TP2 parcial ─────────────────────────────────────────────────
    if self.tp2 and not self.tp2_hit:
        tp2_hit = (is_long and price >= self.tp2) or (not is_long and price <= self.tp2)
        if tp2_hit:
            self.tp2_hit = True
            mark_tp2_hit(self.symbol)
            partial_qty = round(
                (self._open_notional / self.entry_price) * TP2_PARTIAL_RATIO, 6
            )
            if partial_qty > 0 and not self.dry_run:
                close_side = "sell" if is_long else "buy"
                r = await self._place_order(close_side, partial_qty, reduce_only=True)
                if r.get("status") == "ok":
                    logger.info("[%s] TP2 parcial ejecutado (%.1f%%)", self.symbol, TP2_PARTIAL_RATIO * 100)
                    await notify_tp_partial(self.symbol, self.position, price, self.tp2, partial_qty)

    # ── Trailing stop ────────────────────────────────────────────────
    if risk.trailing_sl and self.sl is not None:
        activation_px = self.entry_price * (
            1 + risk.trailing_activation_pct / 100 if is_long
            else 1 - risk.trailing_activation_pct / 100
        )
        activated = (is_long and price >= activation_px) or (not is_long and price <= activation_px)

        if activated:
            callback = risk.trailing_callback_pct / 100
            new_sl = price * (1 - callback if is_long else 1 + callback)
            if is_long and new_sl > self.sl:
                self.sl = new_sl
                logger.debug("[%s] Trailing SL actualizado: %.4f", self.symbol, self.sl)
            elif not is_long and new_sl < self.sl:
                self.sl = new_sl
                logger.debug("[%s] Trailing SL actualizado: %.4f", self.symbol, self.sl)

    # ── Comprobar SL y TP3 ───────────────────────────────────────────
    sl_hit  = self.sl  and ((is_long and price <= self.sl)  or (not is_long and price >= self.sl))
    tp3_hit = self.tp3 and ((is_long and price >= self.tp3) or (not is_long and price <= self.tp3))
    tp1_hit = self.tp1 and not self.tp2 and ((is_long and price >= self.tp1) or (not is_long and price <= self.tp1))

    close_reason = None
    if sl_hit:
        close_reason = "SL"
    elif tp3_hit:
        close_reason = "TP3"
    elif tp1_hit:
        close_reason = "TP1"

    if close_reason:
        positions = await self._get_positions()
        if positions:
            qty = positions[0]["size"]
            close_side = "sell" if is_long else "buy"
            if not self.dry_run:
                await self._place_order(close_side, qty, reduce_only=True)

        pnl_usd = (pnl_pct / 100) * self._open_notional
        if pnl_usd > 0:
            self.win_count += 1
        self.total_pnl += pnl_usd

        logger.info("[%s] 🔒 Cerrado por %s · PnL=%.2f USDC (%.2f%%)",
                    self.symbol, close_reason, pnl_usd, pnl_pct)

        pos_copy = self.position
        self.position = self.entry_price = self.sl = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

        await notify_close(
            symbol=self.symbol,
            side=pos_copy,
            entry=self.entry_price or 0,
            exit_price=price,
            pnl_usd=pnl_usd,
            reason=close_reason,
        )


# Constantes locales (importadas de trader.py scope global)
TP2_PARTIAL_RATIO    = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))
_POS_CHECK_INTERVAL_S = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))

# ── Monkey-patch ─────────────────────────────────────────────────────────────
FuturesTrader.run = _run
