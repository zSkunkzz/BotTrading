"""
position_manager.py — Gestión de posición abierta.

Responsabilidades:
  - Detectar TP2 parcial
  - Calcular y actualizar trailing stop
  - Evaluar SL / TP1 / TP3 y emitir señal de cierre
  - Persistir y limpiar estado de posición
  - Notificar cierres y TP parciales via Telegram

Extraído de FuturesTrader._manage_open_position en trader.py.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

from bot.state import save_position, clear_position, mark_tp2_hit
from bot.telegram_bot import notify_close, notify_tp_partial

logger = logging.getLogger("PositionManager")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))


class PositionManager:
    """
    Gestiona el ciclo de vida de una posición abierta para un símbolo.
    No hace llamadas HTTP directas — recibe el trader como contexto
    para poder colocar órdenes de cierre.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol

    async def manage(
        self,
        trader,       # FuturesTrader — contexto con _place_order y estado
        price: float,
        risk,
    ) -> None:
        """
        Evalúa la posición abierta y actúa según precio actual.
        Modifica directamente el estado del trader (position, sl, tp*, tp2_hit).
        """
        if trader.position is None or trader.entry_price is None:
            return

        is_long = trader.position == "long"
        pnl_pct = (
            (price - trader.entry_price) / trader.entry_price
        ) * (1 if is_long else -1) * 100

        # ── TP2 parcial ───────────────────────────────────────────────────────
        if trader.tp2 and not trader.tp2_hit:
            tp2_triggered = (
                (is_long and price >= trader.tp2)
                or (not is_long and price <= trader.tp2)
            )
            if tp2_triggered:
                trader.tp2_hit = True
                mark_tp2_hit(self.symbol)
                partial_qty = round(
                    (trader._open_notional / trader.entry_price) * TP2_PARTIAL_RATIO, 6
                )
                if partial_qty > 0 and not trader.dry_run:
                    close_side = "sell" if is_long else "buy"
                    r = await trader._place_order(close_side, partial_qty, reduce_only=True)
                    if r.get("status") == "ok":
                        logger.info(
                            "[%s] TP2 parcial ejecutado (%.0f%%)",
                            self.symbol, TP2_PARTIAL_RATIO * 100,
                        )
                        await notify_tp_partial(
                            self.symbol, trader.position, price, trader.tp2, partial_qty
                        )

        # ── Trailing stop ─────────────────────────────────────────────────────
        if risk.trailing_sl and trader.sl is not None:
            activation_px = trader.entry_price * (
                1 + risk.trailing_activation_pct / 100
                if is_long
                else 1 - risk.trailing_activation_pct / 100
            )
            activated = (
                (is_long and price >= activation_px)
                or (not is_long and price <= activation_px)
            )
            if activated:
                callback  = risk.trailing_callback_pct / 100
                new_sl    = price * (1 - callback if is_long else 1 + callback)
                if is_long and new_sl > trader.sl:
                    trader.sl = new_sl
                    logger.debug("[%s] Trailing SL → %.4f", self.symbol, trader.sl)
                elif not is_long and new_sl < trader.sl:
                    trader.sl = new_sl
                    logger.debug("[%s] Trailing SL → %.4f", self.symbol, trader.sl)

        # ── Evaluar SL / TP ───────────────────────────────────────────────────
        sl_hit  = trader.sl  and ((is_long and price <= trader.sl)  or (not is_long and price >= trader.sl))
        tp3_hit = trader.tp3 and ((is_long and price >= trader.tp3) or (not is_long and price <= trader.tp3))
        tp1_hit = (
            trader.tp1
            and not trader.tp2
            and ((is_long and price >= trader.tp1) or (not is_long and price <= trader.tp1))
        )

        close_reason = "SL" if sl_hit else ("TP3" if tp3_hit else ("TP1" if tp1_hit else None))
        if not close_reason:
            return

        # Verificar posición en exchange antes de enviar cierre
        positions = await trader._get_positions()
        if not positions:
            logger.warning(
                "[%s] Cierre por %s: posición no encontrada en exchange (ya cerrada?).",
                self.symbol, close_reason,
            )
            self._reset_trader_state(trader)
            return

        qty = abs(float(positions[0].get("szi", 0)))
        if qty <= 0:
            logger.error("[%s] Cierre por %s: qty=0, sin orden.", self.symbol, close_reason)
            return

        close_side = "sell" if is_long else "buy"
        if not trader.dry_run:
            await trader._place_order(close_side, qty, reduce_only=True)

        pnl_usd = (pnl_pct / 100) * trader._open_notional
        if pnl_usd > 0:
            trader.win_count += 1
        trader.total_pnl += pnl_usd
        logger.info(
            "[%s] 🔒 Cerrado por %s · PnL=%.2f USDC (%.2f%%)",
            self.symbol, close_reason, pnl_usd, pnl_pct,
        )

        entry_copy = trader.entry_price
        pos_copy   = trader.position
        self._reset_trader_state(trader)

        await notify_close(
            symbol=self.symbol,
            side=pos_copy,
            entry=entry_copy,
            exit_price=price,
            pnl_usd=pnl_usd,
            reason=close_reason,
        )

    def _reset_trader_state(self, trader) -> None:
        """Limpia el estado en memoria y en disco del trader."""
        trader.position    = None
        trader.entry_price = None
        trader.sl          = None
        trader.tp1 = trader.tp2 = trader.tp3 = None
        trader.tp2_hit = False
        clear_position(self.symbol)
