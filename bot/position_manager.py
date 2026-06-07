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
  cancela TODAS las órdenes SL/TP activas del exchange antes de recolocar
  el nuevo SL en BE + el TP1 original, evitando duplicados en BingX.

v21 — Fix tp2/tp3 limpieza en _reset_trader_position_state:
  Cuando se detecta cierre externo con qty=0, ahora también se limpian
  trader.tp2 y trader.tp3 para evitar estado contaminado.

v20 — Fix BingX migration — _update_sl_to_be cancela todas las órdenes
  SL/TP pendientes antes de recolocar (evita duplicados en BingX).
  Usa cancel_all_orders() del BingXClient si está disponible; si no,
  cancela order by order con cancel_order().

v19 — _reset_trader_position_state: reemplaza asyncio.ensure_future() por
  asyncio.get_event_loop().create_task() con fallback a ensure_future() para
  mayor robustez en contextos donde el loop puede no estar activo en el hilo
  actual. Añade guard para evitar RuntimeError si no hay loop disponible.

v18 — reentry_guard hook:
  _emergency_close ahora llama reentry_guard.register_sl(symbol) cuando
  reason contiene 'SL'. Esto activa la reducción de size en el siguiente
  re-entry sobre el mismo par durante la ventana REENTRY_WINDOW_S.

Fix qty=0 loop — _ensure_tpsl ahora verifica si la posición sigue abierta
  en el exchange antes de intentar colocar SL/TP de emergencia. Si _open_qty
  es 0 o la posición ya no existe, limpia el estado del trader en lugar de
  entrar en un loop infinito de errores.
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


def _get_tpsl_type(order: dict) -> Optional[str]:
    """
    Extrae 'sl', 'tp' o None del campo orderType.trigger.tpsl.
    Compatible con openOrders y frontendOpenOrders de Hyperliquid,
    y con algo-orders de OKX (campo algoType: 'sl' o 'tp').
    """
    # OKX algo-orders usan 'algoType' con valores 'sl' o 'tp'
    algo_type = order.get("algoType", "").lower()
    if algo_type in ("sl", "tp"):
        return algo_type
    # HL compat
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
    # OKX algo-orders son siempre reduce-only por definición
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

        # BE: mover SL a entrada cuando el precio avanza hacia el TP
        await self._check_break_even()

        if now - self._last_tpsl_check >= _TPSL_VERIFY_INTERVAL_S:
            self._last_tpsl_check = now
            await self._ensure_tpsl()

    # ── Break-Even ─────────────────────────────────────────────────────────────────

    async def _check_break_even(self) -> None:
        """
        Mueve el SL a break-even (entry + offset) cuando el precio alcanza
        BE_TRIGGER_PCT del recorrido entry → TP1.
        Solo se activa UNA VEZ por posición (_tp1_be_done).
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

        # Marcar como hecho ANTES de la llamada async (evita doble ejecución)
        trader._tp1_be_done = True
        trader.sl = be_price

        await self._update_sl_to_be(be_price, is_long, symbol)

    async def _update_sl_to_be(self, be_price: float, is_long: bool, symbol: str) -> None:
        """
        v20: Cancela TODAS las órdenes SL/TP pendientes del exchange antes de
        recolocar el nuevo SL en BE + el TP1 original.

        Esto evita el problema de órdenes duplicadas en BingX que causaba
        comportamiento errático del SL/TP.

        Flujo:
          1. Cancelar todas las órdenes algo/trigger pendientes del símbolo.
          2. Colocar SL nuevo en be_price.
          3. Recolocar el TP1 original (único TP activo).

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

        # Verificar que trader tiene _place_tpsl
        place_tpsl_fn = getattr(trader, "_place_tpsl", None)
        if not callable(place_tpsl_fn):
            log.error(
                "[%s] BE: trader no tiene _place_tpsl — no se puede colocar SL de BE.",
                symbol,
            )
            trader._tp1_be_done = False
            trader.sl = None
            return

        tp1 = getattr(trader, "tp1", None)

        # ── 1. Cancelar todas las órdenes SL/TP pendientes ───────────────
        # Esto es CRÍTICO para evitar duplicados en BingX. Si el exchange tiene
        # ya un SL y/o TP activo, colocar uno nuevo sin cancelar el anterior
        # genera dos órdenes en el mismo lado → comportamiento impredecible.
        await self._cancel_all_tpsl_orders(symbol)

        # ── 2. Colocar SL nuevo en BE ─────────────────────────────────────
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

        # ── 3. Recolocar TP1 original (único TP activo) ───────────────────
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

        # Notificar por Telegram
        try:
            from bot.telegram_bot import send_message
            side_emoji = "🟢" if is_long else "🔴"
            await send_message(
                f"{side_emoji} *BE activado* `{symbol}`\n"
                f"SL movido a entrada: `{be_price:.6f}` — posición sin riesgo 🛡️"
            )
        except Exception:
            pass

    async def _cancel_all_tpsl_orders(self, symbol: str) -> None:
        """
        Cancela todas las órdenes SL/TP (trigger/algo) pendientes del símbolo
        en el exchange antes de recolocar nuevas órdenes de protección.

        Estrategia:
          1. Intentar cancel_all_orders() del BingXClient si está disponible
             (cancela de golpe todas las órdenes del símbolo).
          2. Si no existe, obtener la lista de trigger orders y cancelarlas
             una a una con cancel_order().
          3. Si ninguna opción funciona, logear warning y continuar
             (el paso posterior recolocará de todas formas).
        """
        trader = self._trader
        bingx  = getattr(trader, "_bingx_client", None)

        if bingx is None:
            log.debug("[%s] _cancel_all_tpsl_orders: sin cliente BingX — skip.", symbol)
            return

        # Opción A: cancel_all_orders() — cancela todo en una sola llamada
        if hasattr(bingx, "cancel_all_orders") and callable(bingx.cancel_all_orders):
            try:
                result = await asyncio.to_thread(bingx.cancel_all_orders)
                code = (result or {}).get("code", -1)
                if code in (0, "0", None):
                    log.info("[%s] BE: todas las órdenes SL/TP canceladas (cancel_all_orders).", symbol)
                else:
                    log.warning(
                        "[%s] BE: cancel_all_orders devolvió código %s: %s",
                        symbol, code, (result or {}).get("msg", ""),
                    )
                return
            except Exception as e:
                log.warning("[%s] BE: cancel_all_orders falló (%s) — intentando cancelación individual.", symbol, e)

        # Opción B: cancelar orden a orden
        get_trigger_fn = getattr(trader, "_get_open_trigger_orders_raw", None)
        cancel_fn_name = None
        for fn_name in ("cancel_order", "cancel_algo_order", "cancel_trigger_order"):
            if hasattr(bingx, fn_name) and callable(getattr(bingx, fn_name)):
                cancel_fn_name = fn_name
                break

        if not callable(get_trigger_fn) or cancel_fn_name is None:
            log.warning(
                "[%s] BE: no se puede cancelar órdenes individuales "
                "(get_trigger_fn=%s cancel_fn=%s) — continuando sin cancelar.",
                symbol, callable(get_trigger_fn), cancel_fn_name,
            )
            return

        try:
            orders = await get_trigger_fn() or []
        except Exception as e:
            log.warning("[%s] BE: no se pudo listar trigger orders: %s — skip cancelación.", symbol, e)
            return

        cancel_fn = getattr(bingx, cancel_fn_name)
        cancelled = 0
        for order in orders:
            order_id = order.get("orderId") or order.get("algoId") or order.get("id")
            if not order_id:
                continue
            try:
                await asyncio.to_thread(cancel_fn, order_id)
                cancelled += 1
            except Exception as e:
                log.debug("[%s] BE: error cancelando orden %s: %s", symbol, order_id, e)

        log.info("[%s] BE: %d orden(es) SL/TP cancelada(s) antes de recolocar.", symbol, cancelled)

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
        BUG 3 FIX: En OKX los SL/TP son algo-orders que viven en
        GET /api/v5/trade/orders-algo-pending, no en orders-pending.
        Se detectan via trader._get_open_trigger_orders_raw() que ahora
        está implementado en FuturesTrader consultando ese endpoint.

        Fix qty=0 loop: si _open_qty es 0 o la posición ya no existe en el
        exchange, limpiamos el estado del trader en lugar de intentar colocar
        órdenes de emergencia infinitamente.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        # ── Guardia: si qty=0, la posición fue cerrada externamente ──────────
        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)
        if open_qty <= 0:
            log.info(
                "[%s] _ensure_tpsl: _open_qty=0 — posición cerrada externamente. "
                "Limpiando estado del trader.",
                symbol,
            )
            _reset_trader_position_state(trader, symbol)
            return

        # 1. Órdenes normales (limit/market pending)
        try:
            raw_orders = await trader._get_open_orders_raw() or []
        except Exception as e:
            log.warning("[%s] _ensure_tpsl: orders-pending error: %s", symbol, e)
            raw_orders = []

        # 2. Algo orders (TP/SL) — en OKX viven en orders-algo-pending
        trigger_orders: list[dict] = []
        get_trigger_fn = getattr(trader, "_get_open_trigger_orders_raw", None)
        if callable(get_trigger_fn):
            try:
                trigger_orders = await get_trigger_fn() or []
            except Exception as e:
                log.warning("[%s] _ensure_tpsl: orders-algo-pending error: %s", symbol, e)

        # 3. Combinar y filtrar por instId
        all_orders = raw_orders + trigger_orders
        inst_id = getattr(trader, "inst_id", symbol).upper()
        coin_orders = [
            o for o in all_orders
            if (
                str(o.get("instId", "")).upper() == inst_id
                or str(o.get("coin", "")).upper() == getattr(trader, "coin", symbol).upper()
            )
        ]

        # 4. Detectar SL/TP por tipo de orden
        has_sl = any(_get_tpsl_type(o) == "sl" for o in coin_orders)
        has_tp = any(_get_tpsl_type(o) == "tp" for o in coin_orders)

        # 5. Bug B: fallback por precio si el campo algoType no viene
        if not has_sl or not has_tp:
            sl_price = getattr(trader, "sl", None)
            tp_price = getattr(trader, "tp1", None) or getattr(trader, "tp", None)
            for o in coin_orders:
                if not _is_reduce_only(o):
                    continue
                try:
                    # OKX algo-orders usan triggerPx; limit orders usan px
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

        # Bug J: TP dinámico cuando falta
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
        BUG SL-SW FIX: FuturesTrader OKX expone close_position() (sin guión bajo).
        El código anterior buscaba _close_position() → callable() devolvía False
        → el SL por software se disparaba en los logs pero nunca cerraba la posición.

        Se intenta close_position primero (FuturesTrader OKX), luego _close_position
        como fallback (compatibilidad con traders legacy).

        v18: llama reentry_guard.register_sl(symbol) cuando reason contiene 'SL'
        para activar la reducción de size en el siguiente re-entry.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        # v18: registrar la liquidación por SL antes de cerrar
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

        # Intentar primero el método público de FuturesTrader OKX
        close_fn = getattr(trader, "close_position", None)

        # Fallback: nombre alternativo usado en traders legacy
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
    Esto evita el loop infinito de ensure_tpsl / place_emergency_sl_tp.

    FIX v21: también limpia tp2/tp3 para evitar estado contaminado cuando
    el bot se reinicia con posición guardada que tenía múltiples TPs.

    FIX v19: usa asyncio.get_event_loop().create_task() en lugar de
    asyncio.ensure_future() para mayor robustez. Añade guard contra
    RuntimeError si no hay event loop disponible en el hilo actual.
    """
    log.warning(
        "[%s] Reseteando estado de posición (cerrada externamente): "
        "position=None, sl=None, tp1=None, tp2=None, tp3=None, _open_qty=0, _protection_ok=False",
        symbol,
    )
    trader.position = None
    trader.sl = None
    trader.tp1 = None
    # FIX v21: limpiar tp2/tp3 — ya no se usan, solo 1 TP activo
    if hasattr(trader, "tp2"):
        trader.tp2 = None
    if hasattr(trader, "tp3"):
        trader.tp3 = None
    if hasattr(trader, "tp"):
        trader.tp = None
    trader._open_qty = 0.0
    trader._protection_ok = False
    if hasattr(trader, "_tp1_be_done"):
        trader._tp1_be_done = False
    if hasattr(trader, "entry_price"):
        trader.entry_price = None
    if hasattr(trader, "_entry_price"):
        trader._entry_price = None

    # Notificar por Telegram de forma no bloqueante
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
            # No hay event loop en este hilo — último recurso
            asyncio.ensure_future(send_message(msg))
    except Exception:
        pass
