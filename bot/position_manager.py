"""
position_manager.py — Gestión de posición abierta.

MEJORAS v4:
  BUG #1 FIX: Guard managed_by_trader para evitar doble gestión con trader.py
  BUG #2 FIX: qty post-TP1 actualizada desde exchange antes de _place_tpsl
  BUG #3 FIX: adopt_orphan() para posiciones huérfanas al rotar PairScanner

Responsabilidades:
  - Detectar TP2 parcial
  - Calcular y actualizar trailing stop
  - Evaluar SL / TP1 / TP3 y emitir señal de cierre
  - Persistir y limpiar estado de posición
  - Notificar cierres y TP parciales via Telegram
  - Cancelar trigger orders huérfanos tras cierre/parcial
  - Liberar exposición en pretrade_risk al cerrar (register_close)
  - Decrementar global_risk al cerrar
  - Bloquear reapertura hasta nueva vela 15m tras cierre
"""
from __future__ import annotations

import asyncio
import logging
import os

from bot.state import save_position, clear_position, mark_tp2_hit
from bot.telegram_bot import notify_close, notify_tp_partial
from bot.pretrade_risk import pretrade_risk
from bot.signal_cooldown import signal_cooldown

logger = logging.getLogger("PositionManager")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

# Trailing stop escalonado: activar o no cada nivel
TRAILING_BE       = os.getenv("TRAILING_BE",       "true").lower() != "false"  # SL→BE en TP1
TRAILING_TP1_LOCK = os.getenv("TRAILING_TP1_LOCK", "true").lower() != "false"  # SL→TP1 en TP2

# SL de emergencia para posiciones huérfanas adoptadas: % desde entry
_ORPHAN_SL_PCT = float(os.getenv("ORPHAN_SL_PCT", "3.0"))


class PositionManager:
    """
    Gestiona el ciclo de vida de una posición abierta para un símbolo.
    No hace llamadas HTTP directas — recibe el trader como contexto
    para poder colocar órdenes de cierre.

    BUG #1 FIX: si el trader ya gestiona la posición internamente (trader.py
    tiene su propio loop de TP/SL desde v3), el método manage() puede recibir
    managed_by_trader=True para saltar los bloques que duplicarían acciones.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._be_activated:       dict[str, bool] = {}
        self._tp1_lock_activated: dict[str, bool] = {}

    async def manage(
        self,
        trader,
        price: float,
        risk,
        global_risk=None,
        managed_by_trader: bool = True,  # BUG #1 FIX: default True porque trader.py v3 ya gestiona
    ) -> None:
        """
        Evalúa la posición abierta y actúa según precio actual.

        managed_by_trader=True (default): trader.py ya gestiona SL/TP en su
        propio _manage_open_position loop — este método solo actualiza el
        trailing stop en memoria y libera recursos al cerrar externamente.
        No coloca órdenes ni cierra posiciones para evitar duplicados.

        managed_by_trader=False: modo legacy donde PositionManager es el
        responsable único de gestionar la posición (compatible con código
        anterior que no use trader.py v3+).
        """
        if trader.position is None or trader.entry_price is None:
            return

        sym      = self.symbol
        is_long  = trader.position == "long"
        entry_px = trader.entry_price

        # ── #1 Trailing stop escalonado ───────────────────────────────────────
        # Se ejecuta siempre (actualiza estado en memoria), pero solo mueve el SL
        # del exchange cuando managed_by_trader=False para evitar conflicto con
        # el SL que ya colocó trader.py.
        if TRAILING_BE and trader.tp1 and trader.sl is not None:
            tp1_reached = (
                (is_long  and price >= trader.tp1)
                or (not is_long and price <= trader.tp1)
            )
            if tp1_reached and not self._be_activated.get(sym, False):
                be_sl = entry_px
                if (is_long and be_sl > trader.sl) or (not is_long and be_sl < trader.sl):
                    old_sl = trader.sl
                    trader.sl = be_sl
                    self._be_activated[sym] = True
                    logger.info(
                        "[%s] 🔒 SL → breakeven %.4f (era %.4f, TP1 tocado)",
                        sym, trader.sl, old_sl,
                    )
                    save_position(sym, {
                        "sl": trader.sl,
                        "tp1": trader.tp1, "tp2": trader.tp2, "tp3": trader.tp3,
                        "tp2_hit": trader.tp2_hit,
                        "entry_price": entry_px,
                        "position": trader.position,
                    })

        if TRAILING_TP1_LOCK and trader.tp2 and trader.tp1 and trader.sl is not None:
            tp2_reached_for_lock = (
                (is_long  and price >= trader.tp2)
                or (not is_long and price <= trader.tp2)
            )
            if tp2_reached_for_lock and not self._tp1_lock_activated.get(sym, False):
                lock_sl = trader.tp1
                if (is_long and lock_sl > trader.sl) or (not is_long and lock_sl < trader.sl):
                    old_sl = trader.sl
                    trader.sl = lock_sl
                    self._tp1_lock_activated[sym] = True
                    logger.info(
                        "[%s] 🔒 SL → TP1 %.4f (era %.4f, TP2 tocado)",
                        sym, trader.sl, old_sl,
                    )
                    save_position(sym, {
                        "sl": trader.sl,
                        "tp1": trader.tp1, "tp2": trader.tp2, "tp3": trader.tp3,
                        "tp2_hit": trader.tp2_hit,
                        "entry_price": entry_px,
                        "position": trader.position,
                    })

        # ── Si trader.py gestiona la posición, no duplicar órdenes ────────────
        if managed_by_trader:
            return

        # ── TP2 parcial ───────────────────────────────────────────────────────
        if trader.tp2 and not trader.tp2_hit:
            tp2_triggered = (
                (is_long and price >= trader.tp2)
                or (not is_long and price <= trader.tp2)
            )
            if tp2_triggered:
                trader.tp2_hit = True
                mark_tp2_hit(sym)

                # BUG #2 FIX: obtener qty real restante del exchange
                remaining_qty = await self._get_remaining_qty(trader)

                open_notional = trader._open_notional or 0.0
                if remaining_qty > 0:
                    partial_qty = round(remaining_qty * TP2_PARTIAL_RATIO, 6)
                elif entry_px and entry_px > 0 and open_notional > 0:
                    partial_qty = round(
                        (open_notional / entry_px) * TP2_PARTIAL_RATIO, 6
                    )
                else:
                    logger.warning(
                        "[%s] TP2 parcial: qty=0 y entry_price/notional inválidos — saltando.",
                        sym,
                    )
                    partial_qty = 0

                if partial_qty > 0 and not trader.dry_run:
                    close_side = "sell" if is_long else "buy"

                    try:
                        trader._hl_client.cancel_all_open_tpsl()
                        logger.info(
                            "[%s] TP2 parcial: trigger orders cancelados antes del cierre parcial.",
                            sym,
                        )
                    except Exception as e:
                        logger.warning(
                            "[%s] TP2 parcial: no se pudieron cancelar triggers: %s",
                            sym, e,
                        )

                    r = await trader._place_order(close_side, partial_qty, reduce_only=True)
                    if r.get("status") == "ok":
                        logger.info(
                            "[%s] TP2 parcial ejecutado (%.0f%%)",
                            sym, TP2_PARTIAL_RATIO * 100,
                        )
                        await notify_tp_partial(
                            sym, trader.position, price, trader.tp2, partial_qty
                        )

                        # BUG #2 FIX: actualizar _open_qty con qty real post-parcial
                        post_qty = await self._get_remaining_qty(trader)
                        if post_qty > 0:
                            trader._open_qty = post_qty
                            logger.info(
                                "[%s] _open_qty actualizada post-TP2: %.6f (desde exchange)",
                                sym, post_qty,
                            )

                        if post_qty > 0 and (trader.tp3 or trader.sl):
                            try:
                                await trader._place_tpsl(post_qty, trader.sl, trader.tp3)
                                logger.info(
                                    "[%s] TP/SL re-colocados para qty restante=%.6f",
                                    sym, post_qty,
                                )
                            except Exception as e:
                                logger.warning(
                                    "[%s] No se pudieron re-colocar TP/SL tras parcial: %s",
                                    sym, e,
                                )

        # ── Trailing stop dinámico (callback) ────────────────────────────────
        if risk.trailing_sl and trader.sl is not None:
            activation_px = entry_px * (
                1 + risk.trailing_activation_pct / 100
                if is_long
                else 1 - risk.trailing_activation_pct / 100
            )
            activated = (
                (is_long and price >= activation_px)
                or (not is_long and price <= activation_px)
            )
            if activated:
                callback = risk.trailing_callback_pct / 100
                new_sl   = price * (1 - callback if is_long else 1 + callback)
                if is_long and new_sl > trader.sl:
                    trader.sl = new_sl
                    logger.debug("[%s] Trailing SL dinámico → %.4f", sym, trader.sl)
                elif not is_long and new_sl < trader.sl:
                    trader.sl = new_sl
                    logger.debug("[%s] Trailing SL dinámico → %.4f", sym, trader.sl)

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
                sym, close_reason,
            )
            self._reset_trader_state(trader)
            signal_cooldown.mark_closed(sym, close_reason)
            if global_risk:
                try:
                    await global_risk.register_close(0.0, symbol=sym)
                except Exception as e:
                    logger.warning("[%s] global_risk.register_close error (pos no encontrada): %s", sym, e)
            return

        qty = abs(float(positions[0].get("szi", 0)))
        if qty <= 0:
            logger.error("[%s] Cierre por %s: qty=0, sin orden.", sym, close_reason)
            return

        close_side = "sell" if is_long else "buy"
        fill_price = price

        if not trader.dry_run:
            try:
                trader._hl_client.cancel_all_open_tpsl()
            except Exception as e:
                logger.warning("[%s] No se pudieron cancelar triggers antes del cierre: %s", sym, e)

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
            (fill_price - entry_px) / entry_px
        ) * (1 if is_long else -1) * 100

        open_margin = getattr(trader, "_open_margin", None) or trader._open_notional
        pnl_usd = (pnl_pct_final / 100) * (trader._open_notional or open_margin)

        if pnl_usd > 0:
            trader.win_count += 1
        trader.total_pnl += pnl_usd
        logger.info(
            "[%s] 🔒 Cerrado por %s · fill=%.4f · PnL=%.2f USDC (%.2f%%)",
            sym, close_reason, fill_price, pnl_usd, pnl_pct_final,
        )

        try:
            pretrade_risk.register_close(sym, open_margin)
        except Exception as e:
            logger.warning("[%s] pretrade_risk.register_close error: %s", sym, e)

        if global_risk:
            try:
                await global_risk.register_close(pnl_pct_final, symbol=sym)
            except Exception as e:
                logger.warning("[%s] global_risk.register_close error: %s", sym, e)

        entry_copy = entry_px
        pos_copy   = trader.position
        self._reset_trader_state(trader)
        self._be_activated.pop(sym, None)
        self._tp1_lock_activated.pop(sym, None)

        signal_cooldown.mark_closed(sym, close_reason)

        await notify_close(
            symbol=sym,
            side=pos_copy,
            entry=entry_copy,
            exit_price=fill_price,
            pnl_usd=pnl_usd,
            reason=close_reason,
        )

    # ── BUG #2 FIX: obtener qty real restante del exchange ────────────────────

    async def _get_remaining_qty(self, trader) -> float:
        """
        Consulta la qty real de la posición en el exchange.
        Actualiza trader._open_qty si el valor es válido.
        Devuelve 0.0 si no hay posición o hay error.
        """
        try:
            positions = await trader._get_positions()
            if positions:
                qty = abs(float(positions[0].get("szi", 0)))
                if qty > 0:
                    trader._open_qty = qty
                    return qty
        except Exception as e:
            logger.warning(
                "[%s] _get_remaining_qty: error consultando exchange: %s",
                self.symbol, e,
            )
        return trader._open_qty if trader._open_qty > 0 else 0.0

    # ── BUG #3 FIX: adoptar posición huérfana ────────────────────────────────

    async def adopt_orphan(
        self,
        trader,
        pos_data: dict,
        global_risk=None,
    ) -> bool:
        """
        Adopta una posición abierta en el exchange que no tiene estado local.
        Se llama cuando _init() detecta posición en exchange pero el fichero
        de estado fue borrado o corrompido.

        Registra la posición con SL de emergencia a _ORPHAN_SL_PCT% de entry.
        Devuelve True si la adopción fue exitosa.
        """
        sym = self.symbol
        try:
            szi        = float(pos_data.get("szi", 0))
            entry_px   = float(pos_data.get("entryPx") or 0)
            raw_side   = pos_data.get("side", "")
        except (TypeError, ValueError) as e:
            logger.error("[%s] adopt_orphan: datos de posición inválidos: %s", sym, e)
            return False

        if abs(szi) <= 0 or entry_px <= 0:
            logger.warning("[%s] adopt_orphan: qty=%.6f o entry=%.6f inválidos — ignorando.",
                           sym, szi, entry_px)
            return False

        is_long   = szi > 0
        qty       = abs(szi)
        side_str  = "long" if is_long else "short"
        sl_pct    = _ORPHAN_SL_PCT / 100.0
        emergency_sl = round(
            entry_px * (1 - sl_pct) if is_long else entry_px * (1 + sl_pct),
            6,
        )

        logger.warning(
            "[%s] ⚠️ Posición HUÉRFANA detectada: %s %.6f @ %.5f — "
            "adoptando con SL de emergencia=%.5f (%.1f%% desde entry)",
            sym, side_str, qty, entry_px, emergency_sl, _ORPHAN_SL_PCT,
        )

        trader.position       = side_str
        trader.entry_price    = entry_px
        trader.sl             = emergency_sl
        trader.tp1            = None
        trader.tp2            = None
        trader.tp3            = None
        trader.tp2_hit        = False
        trader._tp1_hit       = False
        trader._open_qty      = qty
        trader._open_notional = qty * entry_px
        trader._open_margin   = trader._open_notional / max(trader.leverage, 1)
        trader._protection_ok = False
        trader._last_tpsl_verify_at = 0.0

        save_position(sym, {
            "side":        side_str,
            "entry":       entry_px,
            "sl":          emergency_sl,
            "tp1":         None,
            "tp2":         None,
            "tp3":         None,
            "tp2_hit":     False,
            "tp1_hit":     False,
            "leverage":    trader.leverage,
            "usdc_amount": trader._open_notional,
            "margin_usdc": trader._open_margin,
            "qty":         qty,
            "entry_mode":  "orphan",
        })

        # Registrar en pretrade_risk
        try:
            pretrade_risk.confirm_order(sym, trader._open_margin)
        except Exception as e:
            logger.warning("[%s] adopt_orphan: pretrade_risk.confirm_order error: %s", sym, e)

        # Colocar SL de emergencia en exchange
        if not trader.dry_run:
            try:
                await trader._place_tpsl(qty=qty, sl=emergency_sl, tp=None, entry_px=entry_px)
                trader._protection_ok = True
                logger.info("[%s] ✅ SL de emergencia %.5f colocado en exchange.", sym, emergency_sl)
            except Exception as e:
                logger.error(
                    "[%s] ❌ No se pudo colocar SL de emergencia: %s — "
                    "_ensure_tpsl intentará reponerlo en <120s.",
                    sym, e,
                )

        return True

    def _reset_trader_state(self, trader) -> None:
        """Limpia el estado en memoria y en disco del trader."""
        trader.position    = None
        trader.entry_price = None
        trader.sl          = None
        trader.tp1 = trader.tp2 = trader.tp3 = None
        trader.tp2_hit = False
        trader._open_notional = 0.0
        if hasattr(trader, "_open_margin"):
            trader._open_margin = 0.0
        if hasattr(trader, "_open_qty"):
            trader._open_qty = 0.0
        clear_position(self.symbol)
