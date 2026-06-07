#!/usr/bin/env python3
"""
bot/position_manager.py — Gestión de protección SL/TP para posiciones abiertas.

Bug A (CRÍTICO) — _ensure_tpsl consultaba solo openOrders, pero en Hyperliquid
  los SL/TP colocados con place_sl/place_tp son TRIGGER ORDERS que viven en
  frontendOpenOrders. El resultado: _ensure_tpsl siempre los veía como
  "faltantes" y los recolocaba cada ~30s → spam infinito en logs.
  Fix OKX: consultar _get_open_trigger_orders_raw() que llama a
  GET /api/v5/trade/orders-algo-pending — donde viven los TP/SL en OKX.

Bug B — fallback por precio para detectar SL/TP cuando el campo tpsl no viene
  correctamente parseado.

Bug C — _place_emergency_sl_tp redondea qty antes de enviar la orden.

Bug I — _resolve_is_long tolera position como str o dict.

Bug J — TP dinámico cuando trader.tp es None (posiciones restauradas del state).

Bug 1+2 (OKX) — _update_sl_to_be usaba trader._hl_client que no existe en
  FuturesTrader. Ahora usa OKXClient.create(symbol) igual que ExecutionEngine.

Bug SL-SW (CRÍTICO) — _emergency_close llamaba trader._close_position() que
  no existe en FuturesTrader OKX. El nombre correcto del método público es
  close_position(). El SL por software se disparaba pero nunca cerraba la
  posición porque AttributeError era silenciado por el check callable().
  Fix: intentar close_position primero, luego _close_position como fallback.

FEAT BE (Break-Even) — Cuando el precio se aleja de la entrada un porcentaje
  configurable hacia el TP, el SL se mueve automáticamente a la entrada.
  Configurable con:
    BE_TRIGGER_PCT  (default 0.4) — % del recorrido entry→TP1 necesario para activar
    BE_OFFSET_PCT   (default 0.0) — offset sobre entry (0 = BE exacto, >0 = pequeño beneficio)
  El BE solo se activa una vez por posición (_tp1_be_done). Una vez activado,
  cancela el SL antiguo del exchange y coloca uno nuevo en entry+offset.

FEAT SCALING OUT (v20) — Al tocar TP1, cierra el 50% de la posición (configurable
  con SCALE_OUT_TP1_RATIO), mueve el SL a breakeven y deja correr el 50% restante
  hacia TP2. Esto mejora el R/R efectivo real sin cambiar la estrategia de entrada.
  Configurable con:
    SCALE_OUT_ENABLED     (default true)  — activa/desactiva el feature
    SCALE_OUT_TP1_RATIO   (default 0.5)   — fracción a cerrar en TP1 (0.5 = 50%)
    SCALE_OUT_BE_OFFSET_PCT (default 0.001) — offset de BE tras scale out (+0.1% sobre entry)
  Guard _tp1_scaled_out para evitar doble ejecución por tick.

v18 — reentry_guard hook:
  _emergency_close ahora llama reentry_guard.register_sl(symbol) cuando
  reason contiene 'SL'. Esto activa la reducción de size en el siguiente
  re-entry sobre el mismo par durante la ventana REENTRY_WINDOW_S.

Fix qty=0 loop — _ensure_tpsl ahora verifica si la posición sigue abierta
  en el exchange antes de intentar colocar SL/TP de emergencia. Si _open_qty
  es 0 o la posición ya no existe, limpia el estado del trader en lugar de
  entrar en un loop infinito de errores.

Fix BingX migration — _update_sl_to_be ahora usa BingXClient en lugar de
  OKXClient (el exchange migró a BingX). Usa trader._place_tpsl() directamente
  para mantener consistencia con el resto del sistema.

Fix v19 — _reset_trader_position_state: reemplaza asyncio.ensure_future() por
  asyncio.get_event_loop().create_task() con fallback a ensure_future() para
  mayor robustez en contextos donde el loop puede no estar activo en el hilo
  actual. Añade guard para evitar RuntimeError si no hay loop disponible.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

log = logging.getLogger(__name__)

# Margen de precio para el check de SL por software (evita doble cierre)
_SL_SW_MARGIN = float(os.getenv("SL_SW_MARGIN_PCT", "0.0005"))

# Intervalo entre verificaciones del SL/TP en el exchange (segundos)
_TPSL_VERIFY_INTERVAL_S = float(os.getenv("TPSL_VERIFY_INTERVAL_S", "60"))

# Máximo de reintentos al colocar SL/TP de emergencia
_EMERGENCY_TPSL_RETRIES = int(os.getenv("EMERGENCY_TPSL_RETRIES", "3"))

# RR mínimo para TP dinámico cuando trader.tp es None
_TP_FALLBACK_RR = float(os.getenv("TP_FALLBACK_RR", "1.5"))

# Break-Even: porcentaje del recorrido entry→TP1 para activar (0.4 = 40%)
_BE_TRIGGER_PCT = float(os.getenv("BE_TRIGGER_PCT", "0.4"))
# Break-Even: offset sobre entry en % (0.0 = BE exacto, 0.001 = +0.1% sobre entry)
_BE_OFFSET_PCT  = float(os.getenv("BE_OFFSET_PCT", "0.0"))

# ── SCALING OUT (v20) ─────────────────────────────────────────────────────────
# Al tocar TP1: cierra SCALE_OUT_TP1_RATIO de la posición, SL→BE, deja correr
# el resto hacia TP2. Mejora el R/R efectivo sin cambiar la estrategia de entrada.
_SCALE_OUT_ENABLED       = os.getenv("SCALE_OUT_ENABLED", "true").lower() == "true"
_SCALE_OUT_TP1_RATIO     = float(os.getenv("SCALE_OUT_TP1_RATIO", "0.5"))
_SCALE_OUT_BE_OFFSET_PCT = float(os.getenv("SCALE_OUT_BE_OFFSET_PCT", "0.001"))


def _get_tpsl_type(order: dict) -> Optional[str]:
    """
    Extrae 'sl', 'tp' o None del campo orderType.trigger.tpsl.
    Compatible con openOrders y frontendOpenOrders de Hyperliquid,
    y con algo-orders de OKX (campo algoType: 'sl' o 'tp').
    """
    algo_type = order.get("algoType", "").lower()
    if algo_type in ("sl", "tp"):
        return algo_type
    ot = order.get("orderType", {})
    if isinstance(ot, dict):
        trigger = ot.get("trigger", {})
        if isinstance(trigger, dict) and trigger.get("tpsl"):
            return trigger["tpsl"]
    t = order.get("type", {})
    if isinstance(t, dict):
        trigger = t.get("trigger", {})
        if isinstance(trigger, dict) and trigger.get("tpsl"):
            return trigger["tpsl"]
    return order.get("tpsl") or None


def _is_reduce_only(order: dict) -> bool:
    if order.get("reduceOnly"):
        return True
    if order.get("algoId"):
        return True
    return _get_tpsl_type(order) in ("sl", "tp")


def _round_qty_safe(trader, qty: float) -> float:
    """Bug C: redondea qty con el método del trader si está disponible."""
    if hasattr(trader, "_round_qty") and callable(trader._round_qty):
        try:
            return trader._round_qty(qty)
        except Exception:
            pass
    return round(qty, 4)


def _resolve_is_long(position) -> bool:
    """Bug I: soporta position como dict {'side': '...'} o str 'long'/'short'."""
    if isinstance(position, dict):
        return position.get("side", "").upper() == "LONG"
    if isinstance(position, str):
        return position.upper() == "LONG"
    return False


def _calc_fallback_tp(entry: float, sl: float, is_long: bool, rr: float) -> Optional[float]:
    """Bug J: TP dinámico cuando trader.tp es None."""
    if not entry or not sl or entry <= 0 or sl <= 0:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    tp = (entry + risk * rr) if is_long else (entry - risk * rr)
    return tp if tp > 0 else None


class PositionManager:
    """
    Gestiona el ciclo de vida de una posición abierta:
      - Check de SL por software (con margen anti-doble-cierre)
      - Scaling Out: cierra 50% en TP1, mueve SL a BE, deja correr hacia TP2
      - Break-Even automático cuando el precio avanza lo suficiente hacia el TP
      - Verificación periódica de SL/TP en el exchange
      - Colocación de emergencia si faltan SL/TP
    """

    def __init__(self, trader) -> None:
        self._trader = trader
        self._last_tpsl_check: float = 0.0

    async def manage(self) -> None:
        """Llamar cada tick mientras hay posición abierta."""
        now = time.monotonic()

        if await self._check_sl_software():
            return

        # SCALING OUT: check TP1 antes que BE para no interferir
        if _SCALE_OUT_ENABLED:
            await self._check_scale_out()

        # BE: mover SL a entrada cuando el precio avanza hacia el TP
        # Solo aplica si el scaling out NO está habilitado (el scale out ya mueve el SL)
        if not _SCALE_OUT_ENABLED:
            await self._check_break_even()

        if now - self._last_tpsl_check >= _TPSL_VERIFY_INTERVAL_S:
            self._last_tpsl_check = now
            await self._ensure_tpsl()

    # ── Scaling Out ────────────────────────────────────────────────────────────

    async def _check_scale_out(self) -> None:
        """
        FEAT SCALING OUT (v20):
        Cuando el precio toca TP1:
          1. Cierra SCALE_OUT_TP1_RATIO (default 50%) de la posición a mercado.
          2. Mueve el SL a breakeven + SCALE_OUT_BE_OFFSET_PCT (default +0.1%).
          3. Actualiza _open_qty, sl y tp en el trader para el resto de la posición.
          4. El TP restante (TP2) sigue activo en el exchange.
        Guard: _tp1_scaled_out — evita doble ejecución por múltiples ticks.
        """
        trader = self._trader

        if getattr(trader, "_tp1_scaled_out", False):
            return

        position = getattr(trader, "position", None)
        entry    = getattr(trader, "entry_price", None)
        tp1      = getattr(trader, "tp1", None)
        tp2      = getattr(trader, "tp2", None)
        price    = getattr(trader, "_last_price", None)
        symbol   = getattr(trader, "symbol", "?")
        open_qty = getattr(trader, "_open_qty", 0.0) or 0.0

        if not position or not entry or not tp1 or not price or open_qty <= 0:
            return

        is_long = _resolve_is_long(position)

        # ¿El precio tocó TP1?
        tp1_hit = (price >= tp1) if is_long else (price <= tp1)
        if not tp1_hit:
            return

        # Calcular qty a cerrar
        close_ratio = max(0.1, min(0.9, _SCALE_OUT_TP1_RATIO))
        qty_to_close = _round_qty_safe(trader, open_qty * close_ratio)
        qty_remaining = _round_qty_safe(trader, open_qty - qty_to_close)

        if qty_to_close <= 0:
            return

        # BE price para la posición restante
        be_price = round(
            entry * (1 + _SCALE_OUT_BE_OFFSET_PCT) if is_long
            else entry * (1 - _SCALE_OUT_BE_OFFSET_PCT),
            6,
        )

        pnl_est = abs(tp1 - entry) / entry * 100 * close_ratio * 100

        log.info(
            "[%s] 📤 SCALE OUT TP1: precio=%.6f tp1=%.6f | cerrando %.4f (%.0f%%) "
            "| restante=%.4f | SL→BE=%.6f | TP2=%s",
            symbol, price, tp1, qty_to_close, close_ratio * 100,
            qty_remaining, be_price, f"{tp2:.6f}" if tp2 else "N/A",
        )

        # Marcar ANTES de las llamadas async (evita doble ejecución)
        trader._tp1_scaled_out = True

        if getattr(trader, "dry_run", True):
            log.info("[%s] DRY_RUN: scale out TP1 simulado (sin orden real).", symbol)
            trader._open_qty = qty_remaining
            trader.sl = be_price
            trader._tp1_be_done = True
            await self._notify_scale_out(symbol, is_long, tp1, qty_to_close, be_price, tp2, pnl_est)
            return

        # 1. Cerrar parcialmente la posición a mercado
        close_fn = getattr(trader, "close_position", None) or getattr(trader, "_close_position", None)
        if callable(close_fn):
            try:
                # Intentamos cierre parcial si el trader lo soporta
                partial_fn = getattr(trader, "close_position_partial", None)
                if callable(partial_fn):
                    await partial_fn(qty=qty_to_close, reason="SCALE_OUT_TP1")
                else:
                    # Fallback: colocar market reduce-only por qty parcial
                    await self._place_partial_close(trader, qty_to_close, is_long, symbol)
            except Exception as e:
                log.error("[%s] scale_out: error cerrando parcial: %s", symbol, e)
                trader._tp1_scaled_out = False
                return
        else:
            log.warning("[%s] scale_out: trader no tiene close_position — usando market reduce-only", symbol)
            try:
                await self._place_partial_close(trader, qty_to_close, is_long, symbol)
            except Exception as e:
                log.error("[%s] scale_out: error market reduce-only: %s", symbol, e)
                trader._tp1_scaled_out = False
                return

        # 2. Actualizar estado del trader
        trader._open_qty = qty_remaining
        trader.sl = be_price
        trader._tp1_be_done = True  # evita que BE se ejecute de nuevo

        # 3. Mover SL a BE en el exchange
        place_tpsl_fn = getattr(trader, "_place_tpsl", None)
        if callable(place_tpsl_fn) and qty_remaining > 0:
            try:
                await place_tpsl_fn(
                    qty=qty_remaining,
                    sl_price=be_price,
                    tp_price=tp2 if tp2 else None,
                    is_long=is_long,
                    reduce_only=True,
                )
                log.info(
                    "[%s] scale_out: SL→BE=%.6f | TP2=%.6f colocado para qty_restante=%.4f",
                    symbol, be_price, tp2 or 0, qty_remaining,
                )
                trader._protection_ok = True
            except Exception as e:
                log.error("[%s] scale_out: error colocando SL BE / TP2: %s", symbol, e)

        await self._notify_scale_out(symbol, is_long, tp1, qty_to_close, be_price, tp2, pnl_est)

    async def _place_partial_close(self, trader, qty: float, is_long: bool, symbol: str) -> None:
        """
        Cierra qty a mercado (reduce-only) cuando el trader no tiene
        close_position_partial. Usa create_order del cliente subyacente.
        """
        side = "sell" if is_long else "buy"
        client = (
            getattr(trader, "_bingx_client", None)
            or getattr(trader, "_client", None)
            or getattr(trader, "_exchange", None)
        )
        if client is None:
            raise RuntimeError("No hay cliente de exchange disponible para cierre parcial")

        create_fn = getattr(client, "create_order", None)
        if not callable(create_fn):
            raise RuntimeError("El cliente no tiene create_order")

        await create_fn(
            symbol=symbol,
            type="market",
            side=side,
            amount=qty,
            params={"reduceOnly": True},
        )
        log.info("[%s] scale_out: market reduce-only %s qty=%.4f ejecutado", symbol, side, qty)

    async def _notify_scale_out(
        self,
        symbol: str,
        is_long: bool,
        tp1: float,
        qty_closed: float,
        be_price: float,
        tp2: Optional[float],
        pnl_est: float,
    ) -> None:
        """Notifica por Telegram el scale out realizado."""
        try:
            from bot.telegram_bot import send_message
            side_emoji = "🟢" if is_long else "🔴"
            tp2_txt = f"`{tp2:.6f}`" if tp2 else "N/A"
            await send_message(
                f"{side_emoji} *SCALE OUT TP1* `{symbol}`\n"
                f"✅ 50% cerrado en `{tp1:.6f}` — PnL ~`{pnl_est:.1f}%`\n"
                f"🛡️ SL movido a BE: `{be_price:.6f}`\n"
                f"🎯 Dejando correr hacia TP2: {tp2_txt}"
            )
        except Exception:
            pass

    # ── Break-Even ─────────────────────────────────────────────────────────────────

    async def _check_break_even(self) -> None:
        """
        Mueve el SL a break-even (entry + offset) cuando el precio alcanza
        BE_TRIGGER_PCT del recorrido entry → TP1.
        Solo se activa UNA VEZ por posición (_tp1_be_done).
        Solo aplica cuando SCALE_OUT_ENABLED=false.
        """
        trader = self._trader

        if getattr(trader, "_tp1_be_done", False):
            return

        position   = getattr(trader, "position", None)
        entry      = getattr(trader, "entry_price", None)
        tp1        = getattr(trader, "tp1", None)
        sl         = getattr(trader, "sl", None)
        price      = getattr(trader, "_last_price", None)
        symbol     = getattr(trader, "symbol", "?")

        if not position or not entry or not tp1 or not price:
            return

        is_long   = _resolve_is_long(position)
        recorrido = abs(tp1 - entry)
        if recorrido <= 0:
            return

        trigger_dist = recorrido * _BE_TRIGGER_PCT

        if is_long:
            triggered = price >= entry + trigger_dist
        else:
            triggered = price <= entry - trigger_dist

        if not triggered:
            return

        be_price = round(
            entry * (1 + _BE_OFFSET_PCT) if is_long else entry * (1 - _BE_OFFSET_PCT),
            6,
        )

        if sl is not None:
            if is_long and sl >= be_price:
                trader._tp1_be_done = True
                return
            if not is_long and sl <= be_price:
                trader._tp1_be_done = True
                return

        log.info(
            "[%s] 🟡 BREAK-EVEN activado: precio=%.4f trigger=%.4f (%.0f%% de %.4f recorrido) "
            "| SL anterior=%.4f → BE=%.4f",
            symbol, price, entry + (trigger_dist if is_long else -trigger_dist),
            _BE_TRIGGER_PCT * 100, recorrido,
            sl or 0, be_price,
        )

        trader._tp1_be_done = True
        trader.sl = be_price

        await self._update_sl_to_be(be_price, is_long, symbol)

    async def _update_sl_to_be(self, be_price: float, is_long: bool, symbol: str) -> None:
        """
        Cancela el SL antiguo y coloca uno nuevo en be_price.
        En dry_run solo logea.
        """
        trader = self._trader

        if getattr(trader, "dry_run", True):
            log.info("[%s] DRY_RUN: BE SL=%.4f omitido (sin orden real).", symbol, be_price)
            return

        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)
        if open_qty <= 0:
            log.warning("[%s] BE: qty=0 — no se puede colocar SL de BE.", symbol)
            return

        place_tpsl_fn = getattr(trader, "_place_tpsl", None)
        if not callable(place_tpsl_fn):
            log.error(
                "[%s] BE: trader no tiene _place_tpsl — no se puede colocar SL de BE.",
                symbol,
            )
            trader._tp1_be_done = False
            trader.sl = None
            return

        entry_price = getattr(trader, "entry_price", be_price)
        tp1 = getattr(trader, "tp1", None)

        try:
            await place_tpsl_fn(
                qty=open_qty,
                sl_price=be_price,
                tp_price=None,
                is_long=is_long,
                reduce_only=True,
            )
            log.info("[%s] BE: SL colocado en entrada (%.4f).", symbol, be_price)
        except Exception as e:
            log.error("[%s] BE: error colocando SL en BE: %s", symbol, e)
            trader._tp1_be_done = False
            trader.sl = None
            return

        if tp1 and tp1 > 0:
            try:
                await place_tpsl_fn(
                    qty=open_qty,
                    sl_price=None,
                    tp_price=tp1,
                    is_long=is_long,
                    reduce_only=True,
                )
                log.info("[%s] BE: TP1 recolocado en %.4f.", symbol, tp1)
            except Exception as e:
                log.warning("[%s] BE: error recolocando TP1: %s", symbol, e)

        trader._protection_ok = True

        try:
            from bot.telegram_bot import send_message
            side_emoji = "🟢" if is_long else "🔴"
            await send_message(
                f"{side_emoji} *BE activado* `{symbol}`\n"
                f"SL movido a entrada: `{be_price:.6f}` — posición sin riesgo 🛡️"
            )
        except Exception:
            pass

    # ── Check SL por software ───────────────────────────────────────────────────

    async def _check_sl_software(self) -> bool:
        trader = self._trader
        sl = getattr(trader, "sl", None)
        position = getattr(trader, "position", None)
        if not sl or not position:
            return False

        price = getattr(trader, "_last_price", None)
        if not price:
            return False

        is_long = _resolve_is_long(position)
        threshold = sl * (1.0 - _SL_SW_MARGIN) if is_long else sl * (1.0 + _SL_SW_MARGIN)
        triggered = (price <= threshold) if is_long else (price >= threshold)

        if not triggered:
            return False

        symbol = getattr(trader, "symbol", "?")
        log.warning(
            "[%s] SL SW disparado: precio=%.4f umbral=%.4f sl=%.4f margen=%.4f%%",
            symbol, price, threshold, sl, _SL_SW_MARGIN * 100,
        )

        if getattr(trader, "_protection_ok", False):
            log.info(
                "[%s] SL SW: precio cruzó umbral pero _protection_ok=True → esperando fill del exchange",
                symbol,
            )
            return False

        await self._emergency_close(reason="SL_SW")
        return True

    # ── Verificación SL/TP en exchange ──────────────────────────────────────────────

    async def _ensure_tpsl(self) -> None:
        """
        Verifica que SL y TP estén activos en el exchange.
        Fix qty=0: si _open_qty es 0, limpia el estado del trader.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)
        if open_qty <= 0:
            log.info(
                "[%s] _ensure_tpsl: _open_qty=0 — posición cerrada externamente. "
                "Limpiando estado del trader.",
                symbol,
            )
            _reset_trader_position_state(trader, symbol)
            return

        try:
            raw_orders = await trader._get_open_orders_raw() or []
        except Exception as e:
            log.warning("[%s] _ensure_tpsl: orders-pending error: %s", symbol, e)
            raw_orders = []

        trigger_orders: list[dict] = []
        get_trigger_fn = getattr(trader, "_get_open_trigger_orders_raw", None)
        if callable(get_trigger_fn):
            try:
                trigger_orders = await get_trigger_fn() or []
            except Exception as e:
                log.warning("[%s] _ensure_tpsl: orders-algo-pending error: %s", symbol, e)

        all_orders = raw_orders + trigger_orders
        inst_id = getattr(trader, "inst_id", symbol).upper()
        coin_orders = [
            o for o in all_orders
            if (
                str(o.get("instId", "")).upper() == inst_id
                or str(o.get("coin", "")).upper() == getattr(trader, "coin", symbol).upper()
            )
        ]

        has_sl = any(_get_tpsl_type(o) == "sl" for o in coin_orders)
        has_tp = any(_get_tpsl_type(o) == "tp" for o in coin_orders)

        if not has_sl or not has_tp:
            sl_price = getattr(trader, "sl", None)
            tp_price = getattr(trader, "tp1", None) or getattr(trader, "tp", None)
            for o in coin_orders:
                if not _is_reduce_only(o):
                    continue
                try:
                    opx = float(
                        o.get("triggerPx")
                        or o.get("limitPx")
                        or o.get("px")
                        or 0
                    )
                except (TypeError, ValueError):
                    opx = 0.0
                if not has_sl and sl_price and opx:
                    if abs(opx - sl_price) / sl_price < 0.002:
                        has_sl = True
                if not has_tp and tp_price and opx:
                    if abs(opx - tp_price) / tp_price < 0.002:
                        has_tp = True

        log.debug(
            "[%s] _ensure_tpsl: total=%d (pending=%d algo=%d) has_sl=%s has_tp=%s",
            symbol, len(all_orders), len(raw_orders), len(trigger_orders), has_sl, has_tp,
        )

        if has_sl and has_tp:
            trader._protection_ok = True
            return

        if getattr(trader, "_protection_ok", False):
            log.info(
                "[%s] _ensure_tpsl: no se detectan SL/TP pero _protection_ok=True "
                "(probablemente ejecutados). Saltando emergencia.",
                symbol,
            )
            return

        trader._protection_ok = False
        missing = []
        if not has_sl:
            missing.append("SL")
        if not has_tp:
            missing.append("TP")

        log.warning(
            "[%s] _ensure_tpsl: FALTAN órdenes: %s → colocando emergencia",
            symbol, ", ".join(missing),
        )
        await self._place_emergency_sl_tp(place_sl=not has_sl, place_tp=not has_tp)

    # ── Colocación de emergencia SL/TP ───────────────────────────────────────────────

    async def _place_emergency_sl_tp(self, place_sl: bool = True, place_tp: bool = True) -> None:
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        sl_price = getattr(trader, "sl", None)
        tp_price = getattr(trader, "tp1", None) or getattr(trader, "tp", None)
        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)

        if open_qty <= 0:
            log.error("[%s] _place_emergency_sl_tp: qty=0 — no se puede colocar orden", symbol)
            return

        position = getattr(trader, "position", None)
        is_long = _resolve_is_long(position)

        if place_tp and tp_price is None:
            entry_price = getattr(trader, "entry_price", None) or getattr(trader, "_entry_price", None)
            tp_price = _calc_fallback_tp(entry_price, sl_price, is_long, _TP_FALLBACK_RR)
            if tp_price is not None:
                trader.tp1 = tp_price
                log.info(
                    "[%s] TP calculado dinámicamente (entry=%.4f sl=%.4f rr=%.1f) → tp=%.4f",
                    symbol, entry_price, sl_price, _TP_FALLBACK_RR, tp_price,
                )
            else:
                log.warning(
                    "[%s] No se puede calcular TP dinámico: entry=%s sl=%s — saltando TP",
                    symbol, entry_price, sl_price,
                )
                place_tp = False

        for attempt in range(1, _EMERGENCY_TPSL_RETRIES + 1):
            try:
                if place_sl and sl_price:
                    await trader._place_tpsl(
                        qty=open_qty,
                        sl_price=sl_price,
                        tp_price=None,
                        is_long=is_long,
                        reduce_only=True,
                    )
                    log.info("[%s] SL emergencia colocado: %.4f (qty=%.4f)", symbol, sl_price, open_qty)

                if place_tp and tp_price:
                    await trader._place_tpsl(
                        qty=open_qty,
                        sl_price=None,
                        tp_price=tp_price,
                        is_long=is_long,
                        reduce_only=True,
                    )
                    log.info("[%s] TP emergencia colocado: %.4f (qty=%.4f)", symbol, tp_price, open_qty)

                trader._protection_ok = True
                break

            except Exception as e:
                log.warning(
                    "[%s] _place_emergency_sl_tp intento %d/%d falló: %s",
                    symbol, attempt, _EMERGENCY_TPSL_RETRIES, e,
                )
                if attempt < _EMERGENCY_TPSL_RETRIES:
                    await asyncio.sleep(2 ** attempt)

    # ── Cierre de emergencia ───────────────────────────────────────────────────────────────

    async def _emergency_close(self, reason: str = "EMERGENCY") -> None:
        """
        v18: llama reentry_guard.register_sl(symbol) cuando reason contiene 'SL'.
        FIX SL-SW: intenta close_position (FuturesTrader) luego _close_position (legacy).
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        if "SL" in reason.upper():
            try:
                from bot.reentry_guard import reentry_guard
                reentry_guard.register_sl(symbol)
                log.info(
                    "[%s] reentry_guard.register_sl llamado (reason=%s) — "
                    "size reducido en próximo re-entry",
                    symbol, reason,
                )
            except Exception as _e:
                log.debug("[%s] reentry_guard.register_sl error (ignorado): %s", symbol, _e)

        close_fn = getattr(trader, "close_position", None)

        if not callable(close_fn):
            close_fn = getattr(trader, "_close_position", None)

        if callable(close_fn):
            try:
                log.warning("[%s] Cierre de emergencia: %s", symbol, reason)
                await close_fn(reason=reason)
            except Exception as e:
                log.error("[%s] _emergency_close falló: %s", symbol, e)
        else:
            log.error(
                "[%s] _emergency_close: el trader no tiene close_position ni _close_position "
                "— posición SIN CERRAR",
                symbol,
            )


# ── Helpers ──────────────────────────────────────────────────────────────────────────────

def _reset_trader_position_state(trader, symbol: str) -> None:
    """
    Limpia el estado de posición del trader cuando se detecta que la posición
    fue cerrada externamente (qty=0 pero el bot sigue creyendo que está abierta).

    FIX v19: usa asyncio.get_event_loop().create_task() en lugar de
    asyncio.ensure_future() para mayor robustez.
    """
    log.warning(
        "[%s] Reseteando estado de posición (cerrada externamente): "
        "position=None, sl=None, tp1=None, _open_qty=0, _protection_ok=False",
        symbol,
    )
    trader.position = None
    trader.sl = None
    trader.tp1 = None
    if hasattr(trader, "tp"):
        trader.tp = None
    trader._open_qty = 0.0
    trader._protection_ok = False
    if hasattr(trader, "_tp1_be_done"):
        trader._tp1_be_done = False
    if hasattr(trader, "_tp1_scaled_out"):
        trader._tp1_scaled_out = False
    if hasattr(trader, "entry_price"):
        trader.entry_price = None
    if hasattr(trader, "_entry_price"):
        trader._entry_price = None

    try:
        from bot.telegram_bot import send_message
        msg = (
            f"⚠️ *Posición cerrada externamente detectada* `{symbol}`\n"
            f"Estado limpiado. El bot ya no gestionará esta posición."
        )
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(send_message(msg))
            else:
                loop.run_until_complete(send_message(msg))
        except RuntimeError:
            asyncio.ensure_future(send_message(msg))
    except Exception:
        pass
