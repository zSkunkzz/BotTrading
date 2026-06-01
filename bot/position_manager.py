"""
position_manager.py — Gestión de posición abierta.

Responsabilidades:
  - Detectar TP2 parcial
  - Calcular y actualizar trailing stop
  - Evaluar SL / TP1 / TP3 y emitir señal de cierre
  - Persistir y limpiar estado de posición
  - Notificar cierres y TP parciales via Telegram
  - Cancelar trigger orders huérfanos tras cierre/parcial
  - Liberar exposición en pretrade_risk al cerrar (register_close)  # FIX #2
  - Bloquear reapertura hasta nueva vela 15m tras cierre             # FIX cooldown
"""
from __future__ import annotations

import logging
import os

from bot.state import save_position, clear_position, mark_tp2_hit
from bot.telegram_bot import notify_close, notify_tp_partial
from bot.pretrade_risk import pretrade_risk
from bot.signal_cooldown import signal_cooldown

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
        trader,
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

                # FIX #5: proteger contra entry_price=0 o None
                entry_px = trader.entry_price
                open_notional = trader._open_notional or 0.0
                if entry_px and entry_px > 0 and open_notional > 0:
                    partial_qty = round(
                        (open_notional / entry_px) * TP2_PARTIAL_RATIO, 6
                    )
                else:
                    logger.warning(
                        "[%s] TP2 parcial: entry_price=%.6f o notional=%.2f inválidos — "
                        "saltando parcial para evitar ZeroDivisionError.",
                        self.symbol, entry_px or 0.0, open_notional,
                    )
                    partial_qty = 0

                if partial_qty > 0 and not trader.dry_run:
                    close_side = "sell" if is_long else "buy"

                    try:
                        trader._hl_client.cancel_all_open_tpsl()
                        logger.info(
                            "[%s] TP2 parcial: trigger orders cancelados antes del cierre parcial.",
                            self.symbol,
                        )
                    except Exception as e:
                        logger.warning(
                            "[%s] TP2 parcial: no se pudieron cancelar triggers: %s",
                            self.symbol, e,
                        )

                    r = await trader._place_order(close_side, partial_qty, reduce_only=True)
                    if r.get("status") == "ok":
                        logger.info(
                            "[%s] TP2 parcial ejecutado (%.0f%%)",
                            self.symbol, TP2_PARTIAL_RATIO * 100,
                        )
                        await notify_tp_partial(
                            self.symbol, trader.position, price, trader.tp2, partial_qty
                        )

                        remaining_notional = open_notional * (1 - TP2_PARTIAL_RATIO)
                        remaining_qty = round(remaining_notional / entry_px, 6) if entry_px > 0 else 0
                        if remaining_qty > 0 and (trader.tp3 or trader.sl):
                            try:
                                await trader._place_tpsl(remaining_qty, trader.sl, trader.tp3)
                                logger.info(
                                    "[%s] TP/SL re-colocados para qty restante=%.6f",
                                    self.symbol, remaining_qty,
                                )
                            except Exception as e:
                                logger.warning(
                                    "[%s] No se pudieron re-colocar TP/SL tras parcial: %s",
                                    self.symbol, e,
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

        positions = await trader._get_positions()
        if not positions:
            logger.warning(
                "[%s] Cierre por %s: posición no encontrada en exchange (ya cerrada?).",
                self.symbol, close_reason,
            )
            self._reset_trader_state(trader)
            signal_cooldown.mark_closed(self.symbol, close_reason)
            return

        qty = abs(float(positions[0].get("szi", 0)))
        if qty <= 0:
            logger.error("[%s] Cierre por %s: qty=0, sin orden.", self.symbol, close_reason)
            return

        close_side = "sell" if is_long else "buy"
        fill_price = price

        if not trader.dry_run:
            try:
                trader._hl_client.cancel_all_open_tpsl()
            except Exception as e:
                logger.warning("[%s] No se pudieron cancelar triggers antes del cierre: %s", self.symbol, e)

            result = await trader._place_order(close_side, qty, reduce_only=True)

            if result.get("status") == "ok":
                try:
                    fill_price = float(
                        result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                        .get("filled", {}).get("avgPx", price)
                    )
                except Exception:
                    pass

        pnl_pct_final = (
            (fill_price - trader.entry_price) / trader.entry_price
        ) * (1 if is_long else -1) * 100
        pnl_usd = (pnl_pct_final / 100) * trader._open_notional

        if pnl_usd > 0:
            trader.win_count += 1
        trader.total_pnl += pnl_usd
        logger.info(
            "[%s] 🔒 Cerrado por %s · fill=%.4f · PnL=%.2f USDC (%.2f%%)",
            self.symbol, close_reason, fill_price, pnl_usd, pnl_pct_final,
        )

        # FIX #2: liberar exposición en pretrade_risk al cerrar
        try:
            pretrade_risk.register_close(self.symbol, trader._open_notional)
        except Exception as e:
            logger.warning("[%s] pretrade_risk.register_close error: %s", self.symbol, e)

        entry_copy = trader.entry_price
        pos_copy   = trader.position
        self._reset_trader_state(trader)

        # FIX cooldown: bloquear reapertura hasta nueva vela 15m
        signal_cooldown.mark_closed(self.symbol, close_reason)

        await notify_close(
            symbol=self.symbol,
            side=pos_copy,
            entry=entry_copy,
            exit_price=fill_price,
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
        trader._open_notional = 0.0
        clear_position(self.symbol)
