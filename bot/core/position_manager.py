"""
position_manager.py — Gestión de posición abierta.

Responsabilidades:
  - Detectar TP1 y hacer SL → breakeven (#1)
  - Detectar TP2 parcial y hacer SL → TP1 (#1)
  - Calcular y actualizar trailing stop
  - Evaluar SL / TP1 / TP3 y emitir señal de cierre
  - Persistir y limpiar estado de posición
  - Notificar cierres y TP parciales via Telegram
  - Cancelar trigger orders huérfanos tras cierre/parcial
  - Llamar signal_cooldown.mark_closed() al cerrar (#4)
  - [V4] position_timeout, trailing_hl, correlation_guard on_close,
          daily_drawdown register_loss
  - Llamar decision_engine.on_position_closed() en todos los cierres

FIX v4.1: correlation_guard import corregido — el módulo expone funciones libres,
          no un singleton. on_close() no existía; se elimina esa llamada.
          _open_leverage incluido en save_position del breakeven.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

from bot.state import save_position, clear_position, mark_tp2_hit
from bot.telegram_bot import notify_close, notify_tp_partial
from bot.signal_cooldown import signal_cooldown
from bot.pretrade_risk import pretrade_risk

# ── V4 módulos ─────────────────────────────────────────────────────────────────────────
try:
    from bot.position_timeout import position_timeout
    _TIMEOUT_ENABLED = os.getenv("POSITION_TIMEOUT_ENABLED", "true").lower() == "true"
except ImportError:
    position_timeout = None
    _TIMEOUT_ENABLED = False

try:
    from bot.trailing_hl import trailing_hl
    _TRAILING_HL_ENABLED = os.getenv("TRAILING_HL_ENABLED", "false").lower() == "true"
except ImportError:
    trailing_hl = None
    _TRAILING_HL_ENABLED = False

# FIX: correlation_guard expone funciones libres, NO un singleton llamado
# `correlation_guard`. El módulo no tiene on_close() — la guard solo actua
# en la apertura (decision_engine). Importamos CORR_ENABLED para logging.
try:
    from bot.correlation_guard import CORR_ENABLED as _CORR_ENABLED
except ImportError:
    _CORR_ENABLED = False

try:
    from bot.daily_drawdown import daily_drawdown
    _DD_ENABLED = True
except ImportError:
    daily_drawdown = None
    _DD_ENABLED = False
# ──────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("PositionManager")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))


class PositionManager:
    """
    Gestiona el ciclo de vida de una posición abierta para un símbolo.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._entry_mode: str = ""
        self._open_ts: float = time.time()  # timestamp de apertura para timeout
        self._decision_engine = None        # inyectado por trading_loop

    def set_entry_mode(self, mode: str) -> None:
        """Llamar desde DecisionEngine al abrir posición."""
        self._entry_mode = (mode or "").upper()
        self._open_ts = time.time()

    def bind_decision_engine(self, de) -> None:
        """Inyecta referencia al DecisionEngine para llamar on_position_closed()."""
        self._decision_engine = de

    def get_all_positions(self) -> dict:
        """
        Devuelve dict {symbol: {side, entry, ...}} de posiciones abiertas.
        Usado por correlation_guard en decision_engine.
        Solo expone la posición de ESTE symbol si está abierta.
        En un sistema multi-symbol el trading_loop puede sobreescribir este método.
        """
        return {}

    async def manage(
        self,
        trader,
        price: float,
        risk,
    ) -> None:
        """
        Evalúa la posición abierta y actúa según precio actual.
        """
        if trader.position is None or trader.entry_price is None:
            return

        is_long = trader.position == "long"
        pnl_pct = (
            (price - trader.entry_price) / trader.entry_price
        ) * (1 if is_long else -1) * 100

        # ── [V4] Position Timeout ──────────────────────────────────────────────────
        if _TIMEOUT_ENABLED and position_timeout is not None:
            try:
                if position_timeout.is_expired(
                    symbol=self.symbol,
                    open_ts=self._open_ts,
                    tp1=trader.tp1,
                    price=price,
                    is_long=is_long,
                ):
                    logger.info(
                        "[%s] ⏱ Position timeout — cerrando posición estancada.",
                        self.symbol,
                    )
                    await self._close_position(trader, price, "TIMEOUT", risk)
                    return
            except Exception as e:
                logger.warning("[%s] position_timeout error: %s", self.symbol, e)

        # ── [V4] Trailing stop nativo en HL ────────────────────────────────────
        if _TRAILING_HL_ENABLED and trailing_hl is not None:
            try:
                await trailing_hl.update(
                    trader=trader,
                    symbol=self.symbol,
                    price=price,
                    is_long=is_long,
                )
            except Exception as e:
                logger.warning("[%s] trailing_hl error: %s", self.symbol, e)

        # ── #1 Breakeven en TP1 ─────────────────────────────────────────────────
        if (
            trader.tp1
            and trader.tp2
            and not getattr(trader, "_tp1_be_done", False)
            and trader.sl is not None
            and trader.entry_price is not None
        ):
            tp1_hit_be = (
                (is_long  and price >= trader.tp1)
                or (not is_long and price <= trader.tp1)
            )
            if tp1_hit_be:
                if is_long and trader.entry_price > trader.sl:
                    trader.sl = trader.entry_price
                    logger.info(
                        "[%s] 🟡 TP1 alcanzado → SL movido a breakeven (%.4f)",
                        self.symbol, trader.sl,
                    )
                    save_position(self.symbol, {
                        "side":        trader.position,
                        "entry":       trader.entry_price,
                        "sl":          trader.sl,
                        "tp1":         trader.tp1,
                        "tp2":         trader.tp2,
                        "tp3":         trader.tp3,
                        "tp2_hit":     trader.tp2_hit,
                        "usdc_amount": trader._open_notional,
                        "leverage":    getattr(trader, "_open_leverage", 1),
                    })
                elif not is_long and trader.entry_price < trader.sl:
                    trader.sl = trader.entry_price
                    logger.info(
                        "[%s] 🟡 TP1 alcanzado → SL movido a breakeven (%.4f)",
                        self.symbol, trader.sl,
                    )
                    save_position(self.symbol, {
                        "side":        trader.position,
                        "entry":       trader.entry_price,
                        "sl":          trader.sl,
                        "tp1":         trader.tp1,
                        "tp2":         trader.tp2,
                        "tp3":         trader.tp3,
                        "tp2_hit":     trader.tp2_hit,
                        "usdc_amount": trader._open_notional,
                        "leverage":    getattr(trader, "_open_leverage", 1),
                    })
                trader._tp1_be_done = True

        # ── TP2 parcial ────────────────────────────────────────────────────────────
        if trader.tp2 and not trader.tp2_hit:
            tp2_triggered = (
                (is_long and price >= trader.tp2)
                or (not is_long and price <= trader.tp2)
            )
            if tp2_triggered:
                trader.tp2_hit = True
                mark_tp2_hit(self.symbol)

                if trader.tp1 and trader.sl is not None:
                    move_sl = (
                        (is_long  and trader.tp1 > trader.sl)
                        or (not is_long and trader.tp1 < trader.sl)
                    )
                    if move_sl:
                        trader.sl = trader.tp1
                        logger.info(
                            "[%s] 🟠 TP2 alcanzado → SL movido a TP1 (%.4f)",
                            self.symbol, trader.sl,
                        )
                        save_position(self.symbol, {
                            "side":        trader.position,
                            "entry":       trader.entry_price,
                            "sl":          trader.sl,
                            "tp1":         trader.tp1,
                            "tp2":         trader.tp2,
                            "tp3":         trader.tp3,
                            "tp2_hit":     True,
                            "usdc_amount": trader._open_notional,
                            "leverage":    getattr(trader, "_open_leverage", 1),
                        })

                if trader.entry_price and trader.entry_price > 0:
                    partial_qty = round(
                        (trader._open_notional / trader.entry_price) * TP2_PARTIAL_RATIO, 6
                    )
                else:
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

                        # Llamar on_position_closed para TP2 parcial
                        if self._decision_engine is not None:
                            try:
                                margin = getattr(trader, "_open_margin", None)
                                self._decision_engine.on_position_closed(
                                    symbol=self.symbol,
                                    margin=margin,
                                    reason="TP2",
                                    entry_mode=self._entry_mode,
                                )
                            except Exception as e:
                                logger.debug("[%s] on_position_closed(TP2) error: %s", self.symbol, e)

                        remaining_notional = trader._open_notional * (1 - TP2_PARTIAL_RATIO)
                        remaining_qty = round(remaining_notional / trader.entry_price, 6)
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

        # ── Trailing stop ────────────────────────────────────────────────────────────
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

        # ── Evaluar SL / TP ────────────────────────────────────────────────────────────
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

        await self._close_position(trader, price, close_reason, risk)

    async def _close_position(
        self,
        trader,
        price: float,
        close_reason: str,
        risk=None,
    ) -> None:
        """Cierra la posición en el exchange y limpia el estado."""
        is_long = trader.position == "long"

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

        entry_copy    = trader.entry_price
        pos_copy      = trader.position
        notional_copy = trader._open_notional
        margin_copy   = getattr(trader, "_open_margin", None)
        entry_mode_copy = self._entry_mode

        self._reset_trader_state(trader)

        # Liberar exposición en pretrade_risk
        pretrade_risk.register_close(self.symbol, notional_copy)

        # [V4] Registrar pérdida en daily_drawdown
        if _DD_ENABLED and daily_drawdown is not None and pnl_usd < 0:
            try:
                daily_drawdown.register_loss(abs(pnl_usd))
            except Exception as e:
                logger.warning("[%s] daily_drawdown.register_loss error: %s", self.symbol, e)

        # Llamar on_position_closed en DecisionEngine
        if self._decision_engine is not None:
            try:
                self._decision_engine.on_position_closed(
                    symbol=self.symbol,
                    margin=margin_copy,
                    reason=close_reason,
                    entry_mode=entry_mode_copy,
                )
            except Exception as e:
                logger.debug("[%s] on_position_closed error: %s", self.symbol, e)

        # #4 Cooldown diferenciado por entry_mode
        signal_cooldown.mark_closed(
            self.symbol,
            entry_mode=entry_mode_copy,
            reason=close_reason,
        )

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
        trader._tp1_be_done = False
        self._entry_mode = ""
        self._open_ts = time.time()
        clear_position(self.symbol)
