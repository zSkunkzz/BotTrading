#!/usr/bin/env python3
"""
bot/position_manager.py — Gestión de protección SL/TP para posiciones abiertas.

v29 — Fix Bug 2 (CRÍTICO): _ensure_tpsl filtraba órdenes por 'instId' y 'coin'
  pero BingX devuelve el campo 'symbol'. Resultado: coin_orders siempre vacío
  → has_sl=False, has_tp=False → spam de órdenes de emergencia cada 60s
  aunque SL/TP estuvieran correctamente colocados.
  Fix: añadir str(o.get('symbol','')).upper() == inst_id al filtro.

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
  cancela SOLO las órdenes de tipo SL antes de recolocar el nuevo SL en BE.
  El TP1 original NO se toca — sigue activo en el exchange sin recolocarse,
  evitando duplicados.

v26 — Fix BE sin duplicar TP:
  1. _update_sl_to_be ya NO cancela el TP activo ni lo recoloca.
     Solo cancela órdenes SL y coloca el nuevo SL en BE.
     Esto evita el bug de TP duplicado cuando cancel_all_orders tiene latencia.
  2. _cancel_sl_orders(): nuevo helper que cancela SOLO las órdenes de tipo SL
     (en lugar de cancel_all_orders que también mataba el TP único).
  3. _cancel_all_tpsl_orders() se mantiene para otros usos (limpieza total).

v25 — Fix SL/TP único y BE robusto:
  1. _ensure_tpsl y _place_emergency_sl_tp usan solo trader.tp1.
     Elimina fallback a trader.tp que reinyectaba TP2/TP3 desde estado antiguo.
  2. _check_break_even maneja sl=None (restart): si precio ya volvió a entry
     o peor, marca _tp1_be_done=True sin disparar el BE (evita doble BE).
  3. Persiste sl y be_done=True en bot_state.update_position al activar el BE.
  4. Sincroniza trader.sl = be_price tras colocar SL en BE con éxito.
  5. _ensure_tpsl detecta SL en entry_price (±0.1%) como SL válido,
     evitando spam de "emergencia" cuando el SL ya está en BE.

v27 — trailing SL ATR integrado:
  Tras _check_break_even(), si trader.trailing_sl_activated=True, se ejecuta
  compute_trailing_sl() de trailing_sl.py. El nuevo SL y el nuevo pico
  se persisten en trader.sl y trader._trailing_peak. Si el precio toca el
  trailing SL → cierre de emergencia 'TRAILING_SL'.
  Config: TRAILING_SL_MODE (default 'atr'), TRAILING_SL_ATR_MULT (1.5x),
          TRAILING_SL_PCT (0.015).

v28 — Fix 2 bugs trailing SL:
  Bug 1 — ATR siempre era 0.0:
    _check_trailing_sl() leía indicators['15m']['atr_val'] pero ese dict
    nunca se escribe en el trader. El ATR vive en trader.atr (seteado
    al abrir la posición desde signal.atr). Se lee ahora trader.atr
    directamente, con fallback a indicators.get('15m',{}).get('atr') (clave
    real usada por _compute_indicators en signal_engine.py).
  Bug 2 — trailing_sl_activated nunca se ponía a True:
    Ningún sitio del flujo activaba el flag tras el hit de BE. Se activa
    ahora al final de _check_break_even(), justo después de persistir
    be_done en state. También se persiste el ATR de entrada (trader.atr)
    para que _check_trailing_sl lo tenga disponible tras un reinicio.

v21 — Fix tp2/tp3 limpieza en _reset_trader_position_state.
v20 — Fix BingX migration — _update_sl_to_be cancela todas las órdenes.
v19 — _reset_trader_position_state robusto con asyncio.
v18 — reentry_guard hook en _emergency_close.
Fix qty=0 loop — _ensure_tpsl verifica posición abierta antes de emergencia.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

log = logging.getLogger(__name__)

_SL_SW_MARGIN           = float(os.getenv("SL_SW_MARGIN_PCT",           "0.0005"))
_TPSL_VERIFY_INTERVAL_S = float(os.getenv("TPSL_VERIFY_INTERVAL_S",     "60"))
_EMERGENCY_TPSL_RETRIES = int(os.getenv("EMERGENCY_TPSL_RETRIES",       "3"))
_TP_FALLBACK_RR         = float(os.getenv("TP_FALLBACK_RR",             "1.5"))
_BE_TRIGGER_PCT         = float(os.getenv("BE_TRIGGER_PCT",             "0.4"))
_BE_OFFSET_PCT          = float(os.getenv("BE_OFFSET_PCT",              "0.0"))


def _get_tpsl_type(order: dict) -> Optional[str]:
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
    if hasattr(trader, "_round_qty") and callable(trader._round_qty):
        try:
            return trader._round_qty(qty)
        except Exception:
            pass
    return round(qty, 4)


def _resolve_is_long(position) -> bool:
    if isinstance(position, dict):
        return position.get("side", "").upper() == "LONG"
    if isinstance(position, str):
        return position.upper() == "LONG"
    return False


def _calc_fallback_tp(entry: float, sl: float, is_long: bool, rr: float) -> Optional[float]:
    if not entry or not sl or entry <= 0 or sl <= 0:
        return None
    risk = abs(entry - sl)
    if risk <= 0:
        return None
    tp = (entry + risk * rr) if is_long else (entry - risk * rr)
    return tp if tp > 0 else None


def _read_atr(trader) -> float:
    """
    v28 fix Bug 1: lee el ATR del trader de forma robusta.

    Orden de prioridad:
      1. trader.atr           — seteado al abrir la posición desde signal.atr
                                (fuente más fiable y directa)
      2. indicators['15m']['atr'] — clave real en el dict de _compute_indicators
                                    (fallback si trader.atr no existe)
      3. 0.0                  — fallback final → trailing_sl usará modo 'pct'

    NOTA: la clave correcta en el dict de indicadores es 'atr', NO 'atr_val'.
    'atr_val' nunca existió en _compute_indicators de signal_engine.py.
    """
    # Fuente primaria: trader.atr (float simple, seteado en _do_open_order)
    atr = getattr(trader, "atr", None)
    if atr and float(atr) > 0:
        return float(atr)

    # Fallback: dict de indicadores (clave 'atr', no 'atr_val')
    try:
        indicators = getattr(trader, "indicators", {}) or {}
        atr_ind = float(indicators.get("15m", {}).get("atr") or 0.0)
        if atr_ind > 0:
            return atr_ind
    except Exception:
        pass

    return 0.0


class PositionManager:
    """
    Gestiona el ciclo de vida de una posición abierta:
      - Check de SL por software
      - Break-Even automático al 40% del recorrido entry→TP1
      - Trailing SL ATR (post-BE, cuando trailing_sl_activated=True)
      - Verificación periódica de SL/TP en el exchange
      - Colocación de emergencia si faltan SL/TP
    """

    def __init__(self, trader) -> None:
        self._trader = trader
        self._last_tpsl_check: float = 0.0

    async def manage(self) -> None:
        now = time.monotonic()
        if await self._check_sl_software():
            return
        await self._check_break_even()
        await self._check_trailing_sl()   # v27: trailing SL post-BE
        if now - self._last_tpsl_check >= _TPSL_VERIFY_INTERVAL_S:
            self._last_tpsl_check = now
            await self._ensure_tpsl()

    # ── Trailing SL (v27 + v28 fix) ────────────────────────────────────────────────

    async def _check_trailing_sl(self) -> None:
        """
        Si trader.trailing_sl_activated=True, actualiza el trailing SL usando
        compute_trailing_sl() de trailing_sl.py.

        v28 fix Bug 1:
          - El ATR se obtiene via _read_atr(trader), que lee trader.atr
            (fuente primaria) o indicators['15m']['atr'] (fallback).
            Ya NO usa la clave inexistente 'atr_val'.

        v28 fix Bug 2 (activación):
          - trailing_sl_activated=True se activa en _check_break_even()
            justo tras persistir be_done en state.
          - Esta función solo se ocupa de ejecutar el trailing una vez activo.

        - El pico favorable se persiste en trader._trailing_peak.
        - Si el precio toca el trailing SL → cierre de emergencia 'TRAILING_SL'.
        """
        trader = self._trader

        if not getattr(trader, "trailing_sl_activated", False):
            return

        position   = getattr(trader, "position",    None)
        price      = getattr(trader, "_last_price", None)
        current_sl = getattr(trader, "sl",          None)
        symbol     = getattr(trader, "symbol",      "?")

        if not position or not price or current_sl is None:
            return

        is_long    = _resolve_is_long(position)
        peak_price = getattr(trader, "_trailing_peak", None)
        if peak_price is None:
            peak_price = price
            trader._trailing_peak = peak_price

        # v28 fix Bug 1: usar _read_atr() en lugar de indicators['15m']['atr_val']
        atr_val = _read_atr(trader)
        if atr_val == 0.0:
            log.debug(
                "[%s] _check_trailing_sl: ATR=0 — usando modo pct como fallback",
                symbol,
            )

        try:
            from bot.trailing_sl import compute_trailing_sl, is_trailing_sl_hit
        except ImportError as e:
            log.error("[%s] trailing_sl import error: %s", symbol, e)
            return

        new_sl, new_peak = compute_trailing_sl(
            is_long=is_long,
            current_price=price,
            peak_price=peak_price,
            current_sl=current_sl,
            atr_val=atr_val,
        )

        # Persistir pico actualizado
        trader._trailing_peak = new_peak

        # Actualizar SL solo si mejoró (trailing nunca retrocede)
        if new_sl != current_sl:
            log.info(
                "[%s] 🟡 trailing SL: %.6f → %.6f (pico=%.6f atr=%.6f)",
                symbol, current_sl, new_sl, new_peak, atr_val,
            )
            trader.sl = new_sl

        # Verificar si el precio ha tocado el trailing SL
        if is_trailing_sl_hit(is_long=is_long, current_price=price, trailing_sl=new_sl):
            log.warning(
                "[%s] 🔴 TRAILING SL HIT: precio=%.6f sl=%.6f",
                symbol, price, new_sl,
            )
            await self._emergency_close(reason="TRAILING_SL")

    # ── Break-Even ─────────────────────────────────────────────────────────────────

    async def _check_break_even(self) -> None:
        """
        Mueve el SL a break-even cuando el precio alcanza BE_TRIGGER_PCT
        del recorrido entry → TP1. Solo se activa UNA VEZ por posición.

        v25: maneja sl=None tras restart. Si el precio ya retrocedió
        a entry o peor, no tiene sentido activar BE — marcar como hecho y salir.
        Persiste be_done en bot_state para sobrevivir reinicios.

        v28 fix Bug 2:
        Tras activar el BE con éxito, pone trader.trailing_sl_activated=True
        para que _check_trailing_sl() comience a ejecutarse en el siguiente
        ciclo. También persiste trader.atr en state para que el trailing
        tenga el ATR correcto tras un posible reinicio.
        """
        trader = self._trader

        if getattr(trader, "_tp1_be_done", False):
            return

        position = getattr(trader, "position",    None)
        entry    = getattr(trader, "entry_price", None)
        tp1      = getattr(trader, "tp1",         None)
        sl       = getattr(trader, "sl",          None)
        price    = getattr(trader, "_last_price", None)
        symbol   = getattr(trader, "symbol",      "?")

        if not position or not entry or not tp1 or not price:
            return

        is_long   = _resolve_is_long(position)
        recorrido = abs(tp1 - entry)
        if recorrido <= 0:
            return

        trigger_dist = recorrido * _BE_TRIGGER_PCT
        triggered    = (price >= entry + trigger_dist) if is_long else (price <= entry - trigger_dist)

        if not triggered:
            return

        be_price = round(
            entry * (1 + _BE_OFFSET_PCT) if is_long else entry * (1 - _BE_OFFSET_PCT),
            6,
        )

        if sl is not None:
            # SL ya está en BE o más allá — no hacer nada
            if (is_long and sl >= be_price) or (not is_long and sl <= be_price):
                trader._tp1_be_done = True
                # v28: si el SL ya está en BE pero trailing no está activo, activarlo
                if not getattr(trader, "trailing_sl_activated", False):
                    trader.trailing_sl_activated = True
                    log.info("[%s] trailing_sl_activated=True (BE ya estaba activo)", symbol)
                return
        else:
            # sl=None tras restart: si precio ya volvió a entry (o peor),
            # no tiene sentido activar BE — marcar como hecho y salir
            if (is_long and price <= entry) or (not is_long and price >= entry):
                trader._tp1_be_done = True
                return

        log.info(
            "[%s] 🟡 BREAK-EVEN activado: precio=%.4f trigger=%.4f (%.0f%% de %.4f) "
            "| SL anterior=%.4f → BE=%.4f",
            symbol, price, entry + (trigger_dist if is_long else -trigger_dist),
            _BE_TRIGGER_PCT * 100, recorrido, sl or 0, be_price,
        )

        # Marcar ANTES de la llamada async (evita doble ejecución)
        trader._tp1_be_done = True
        trader.sl = be_price

        # v28 fix Bug 2: activar trailing SL ahora que el BE está confirmado
        trader.trailing_sl_activated = True
        trader._trailing_peak = price   # inicializar pico con el precio actual
        log.info(
            "[%s] 🟢 trailing_sl_activated=True (ATR=%.6f) — trailing comenzará en próximo ciclo",
            symbol, _read_atr(trader),
        )

        # v25+v28: persistir sl, be_done=True y atr en state (sobrevive reinicios)
        try:
            from bot.state import bot_state as _bs
            await _bs.update_position(
                symbol,
                sl=be_price,
                be_done=True,
                trailing_activated=True,
                atr=_read_atr(trader),
            )
        except Exception as _e:
            log.debug("[%s] BE: no se pudo persistir be_done en state: %s", symbol, _e)

        await self._update_sl_to_be(be_price, is_long, symbol)

    async def _update_sl_to_be(self, be_price: float, is_long: bool, symbol: str) -> None:
        """
        v26: Cancela SOLO las órdenes SL pendientes y recoloca el SL en BE.
        El TP1 activo en el exchange NO se toca — sigue vigente sin recolocarse,
        evitando el bug de TP duplicado.
        En dry_run solo logea.
        """
        trader = self._trader

        if getattr(trader, "dry_run", True):
            log.info("[%s] DRY_RUN: BE SL=%.4f (TP1 sin tocar).", symbol, be_price)
            return

        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)
        if open_qty <= 0:
            log.warning("[%s] BE: qty=0 — no se puede colocar SL de BE.", symbol)
            return

        place_tpsl_fn = getattr(trader, "_place_tpsl", None)
        if not callable(place_tpsl_fn):
            log.error("[%s] BE: trader no tiene _place_tpsl.", symbol)
            trader._tp1_be_done = False
            trader.sl = None
            return

        # 1. Cancelar SOLO las órdenes SL pendientes (el TP queda intacto)
        await self._cancel_sl_orders(symbol)

        # 2. Colocar nuevo SL en BE (solo SL, tp_price=None)
        try:
            await place_tpsl_fn(
                qty=open_qty, sl_price=be_price, tp_price=None,
                is_long=is_long, reduce_only=True,
            )
            log.info("[%s] BE: SL movido a entrada (%.4f). TP1 sin cambios.", symbol, be_price)
            trader.sl = be_price   # sincronizar estado en memoria
        except Exception as e:
            log.error("[%s] BE: error colocando SL en BE: %s", symbol, e)
            trader._tp1_be_done = False
            trader.sl = None
            return

        trader._protection_ok = True

        try:
            from bot.telegram_bot import send_message
            emoji = "🟢" if is_long else "🔴"
            await send_message(
                f"{emoji} *BE activado* `{symbol}`\n"
                f"SL movido a entrada: `{be_price:.6f}` — posición sin riesgo 🛡️\n"
                f"Trailing SL ATR activado 🎯"
            )
        except Exception:
            pass

    async def _cancel_sl_orders(self, symbol: str) -> None:
        """
        v26: Cancela SOLO las órdenes de tipo SL pendientes para este símbolo.
        No toca los TP activos. Esto evita el bug de TP duplicado al activar BE.
        """
        trader = self._trader
        bingx  = getattr(trader, "_bingx_client", None)

        if bingx is None:
            log.debug("[%s] _cancel_sl_orders: sin cliente BingX — skip.", symbol)
            return

        get_trigger_fn = getattr(trader, "_get_open_trigger_orders_raw", None)
        cancel_fn_name = None
        for fn_name in ("cancel_order", "cancel_algo_order", "cancel_trigger_order"):
            if hasattr(bingx, fn_name) and callable(getattr(bingx, fn_name)):
                cancel_fn_name = fn_name
                break

        if not callable(get_trigger_fn) or cancel_fn_name is None:
            log.warning("[%s] _cancel_sl_orders: no se puede cancelar SL individual — skip.", symbol)
            return

        try:
            orders = await get_trigger_fn() or []
        except Exception as e:
            log.warning("[%s] _cancel_sl_orders: no se pudo listar trigger orders: %s", symbol, e)
            return

        cancel_fn = getattr(bingx, cancel_fn_name)
        cancelled = 0
        for order in orders:
            # Solo cancelar órdenes identificadas como SL
            if _get_tpsl_type(order) != "sl":
                continue
            order_id = order.get("orderId") or order.get("algoId") or order.get("id")
            if not order_id:
                continue
            try:
                await asyncio.to_thread(cancel_fn, order_id)
                cancelled += 1
                log.debug("[%s] SL cancelado: id=%s", symbol, order_id)
            except Exception as e:
                log.debug("[%s] _cancel_sl_orders: error cancelando %s: %s", symbol, order_id, e)
        log.info("[%s] BE: %d orden(es) SL cancelada(s) (TP intacto).", symbol, cancelled)

    async def _cancel_all_tpsl_orders(self, symbol: str) -> None:
        """
        Cancela TODAS las órdenes SL+TP pendientes.
        Mantenido para uso en limpieza total (close_position, emergencias).
        NO se usa en el flujo de BE (usa _cancel_sl_orders en su lugar).
        """
        trader = self._trader
        bingx  = getattr(trader, "_bingx_client", None)

        if bingx is None:
            log.debug("[%s] _cancel_all_tpsl_orders: sin cliente BingX — skip.", symbol)
            return

        if hasattr(bingx, "cancel_all_orders") and callable(bingx.cancel_all_orders):
            try:
                result = await asyncio.to_thread(bingx.cancel_all_orders)
                code = (result or {}).get("code", -1)
                if code in (0, "0", None):
                    log.info("[%s] Todas las órdenes canceladas (cancel_all_orders).", symbol)
                else:
                    log.warning("[%s] cancel_all_orders código %s: %s",
                                symbol, code, (result or {}).get("msg", ""))
                return
            except Exception as e:
                log.warning("[%s] cancel_all_orders falló (%s) — intentando individual.", symbol, e)

        get_trigger_fn = getattr(trader, "_get_open_trigger_orders_raw", None)
        cancel_fn_name = None
        for fn_name in ("cancel_order", "cancel_algo_order", "cancel_trigger_order"):
            if hasattr(bingx, fn_name) and callable(getattr(bingx, fn_name)):
                cancel_fn_name = fn_name
                break

        if not callable(get_trigger_fn) or cancel_fn_name is None:
            log.warning("[%s] _cancel_all_tpsl_orders: no se puede cancelar órdenes individuales — skip.", symbol)
            return

        try:
            orders = await get_trigger_fn() or []
        except Exception as e:
            log.warning("[%s] _cancel_all_tpsl_orders: no se pudo listar trigger orders: %s", symbol, e)
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
                log.debug("[%s] error cancelando orden %s: %s", symbol, order_id, e)
        log.info("[%s] %d orden(es) cancelada(s).", symbol, cancelled)

    # ── Check SL por software ──────────────────────────────────────────────────────

    async def _check_sl_software(self) -> bool:
        trader = self._trader
        sl     = getattr(trader, "sl",       None)
        position = getattr(trader, "position", None)
        if not sl or not position:
            return False
        price = getattr(trader, "_last_price", None)
        if not price:
            return False
        is_long   = _resolve_is_long(position)
        threshold = sl * (1.0 - _SL_SW_MARGIN) if is_long else sl * (1.0 + _SL_SW_MARGIN)
        triggered = (price <= threshold) if is_long else (price >= threshold)
        if not triggered:
            return False
        symbol = getattr(trader, "symbol", "?")
        log.warning("[%s] SL SW disparado: precio=%.4f umbral=%.4f sl=%.4f margen=%.4f%%",
                    symbol, price, threshold, sl, _SL_SW_MARGIN * 100)
        if getattr(trader, "_protection_ok", False):
            log.info("[%s] SL SW: _protection_ok=True → esperando fill del exchange", symbol)
            return False
        await self._emergency_close(reason="SL_SW")
        return True

    # ── Verificación SL/TP en exchange ────────────────────────────────────────────────

    async def _ensure_tpsl(self) -> None:
        """
        Verifica que haya SL y TP activos en el exchange.
        v25: solo usa trader.tp1 (sin fallback a trader.tp).
        v25: detecta SL en entry_price (±0.1%) como SL válido (BE activo).
        v29: añade filtro por campo 'symbol' para compatibilidad con BingX.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)
        if open_qty <= 0:
            log.info("[%s] _ensure_tpsl: qty=0 — posición cerrada externamente.", symbol)
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
                log.warning("[%s] _ensure_tpsl: trigger orders error: %s", symbol, e)

        all_orders = raw_orders + trigger_orders
        inst_id    = getattr(trader, "inst_id", symbol).upper()
        coin_orders = [
            o for o in all_orders
            if (
                str(o.get("instId", "")).upper() == inst_id
                or str(o.get("coin", "")).upper() == getattr(trader, "coin", symbol).upper()
                or str(o.get("symbol", "")).upper() == inst_id  # v29: campo BingX
            )
        ]

        has_sl = any(_get_tpsl_type(o) == "sl" for o in coin_orders)
        has_tp = any(_get_tpsl_type(o) == "tp" for o in coin_orders)

        # Fallback por precio cuando algoType no viene parseado
        # v25: solo tp1, sin fallback a trader.tp; detecta BE en entry_price
        if not has_sl or not has_tp:
            sl_price    = getattr(trader, "sl",          None)
            tp_price    = getattr(trader, "tp1",         None)  # solo tp1
            entry_price = getattr(trader, "entry_price", None)
            for o in coin_orders:
                if not _is_reduce_only(o):
                    continue
                try:
                    opx = float(o.get("triggerPx") or o.get("limitPx") or o.get("px") or 0)
                except (TypeError, ValueError):
                    opx = 0.0
                if not has_sl and opx:
                    if sl_price and abs(opx - sl_price) / sl_price < 0.002:
                        has_sl = True
                    elif entry_price and abs(opx - entry_price) / entry_price < 0.001:
                        # SL de break-even en entry_price — válido
                        has_sl = True
                if not has_tp and tp_price and opx:
                    if abs(opx - tp_price) / tp_price < 0.002:
                        has_tp = True

        log.debug("[%s] _ensure_tpsl: total=%d has_sl=%s has_tp=%s",
                  symbol, len(all_orders), has_sl, has_tp)

        if has_sl and has_tp:
            trader._protection_ok = True
            return

        if getattr(trader, "_protection_ok", False):
            log.info("[%s] _ensure_tpsl: no detectados pero _protection_ok=True — skip.", symbol)
            return

        trader._protection_ok = False
        missing = []
        if not has_sl:
            missing.append("SL")
        if not has_tp:
            missing.append("TP")
        log.warning("[%s] _ensure_tpsl: FALTAN %s → colocando emergencia",
                    symbol, ", ".join(missing))
        await self._place_emergency_sl_tp(place_sl=not has_sl, place_tp=not has_tp)

    # ── Emergencia SL/TP ─────────────────────────────────────────────────────────────────

    async def _place_emergency_sl_tp(self, place_sl: bool = True, place_tp: bool = True) -> None:
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        sl_price = getattr(trader, "sl",   None)
        tp_price = getattr(trader, "tp1",  None)   # v25: solo tp1
        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)

        if open_qty <= 0:
            log.error("[%s] _place_emergency_sl_tp: qty=0", symbol)
            return

        position = getattr(trader, "position", None)
        is_long  = _resolve_is_long(position)

        if place_tp and tp_price is None:
            entry_price = getattr(trader, "entry_price", None) or getattr(trader, "_entry_price", None)
            tp_price = _calc_fallback_tp(entry_price, sl_price, is_long, _TP_FALLBACK_RR)
            if tp_price is not None:
                trader.tp1 = tp_price
                log.info("[%s] TP dinámico: %.4f", symbol, tp_price)
            else:
                log.warning("[%s] No se puede calcular TP dinámico — saltando TP", symbol)
                place_tp = False

        for attempt in range(1, _EMERGENCY_TPSL_RETRIES + 1):
            try:
                if place_sl and sl_price:
                    await trader._place_tpsl(
                        qty=open_qty, sl_price=sl_price, tp_price=None,
                        is_long=is_long, reduce_only=True,
                    )
                    log.info("[%s] SL emergencia: %.4f", symbol, sl_price)
                if place_tp and tp_price:
                    await trader._place_tpsl(
                        qty=open_qty, sl_price=None, tp_price=tp_price,
                        is_long=is_long, reduce_only=True,
                    )
                    log.info("[%s] TP emergencia: %.4f", symbol, tp_price)
                trader._protection_ok = True
                break
            except Exception as e:
                log.warning("[%s] _place_emergency_sl_tp intento %d/%d: %s",
                            symbol, attempt, _EMERGENCY_TPSL_RETRIES, e)
                if attempt < _EMERGENCY_TPSL_RETRIES:
                    await asyncio.sleep(2 ** attempt)

    # ── Cierre de emergencia ─────────────────────────────────────────────────────────────

    async def _emergency_close(self, reason: str = "EMERGENCY") -> None:
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        if "SL" in reason.upper():
            try:
                from bot.reentry_guard import reentry_guard
                reentry_guard.register_sl(symbol)
            except Exception as _e:
                log.debug("[%s] reentry_guard.register_sl error: %s", symbol, _e)

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
            log.error("[%s] _emergency_close: sin close_position — posición SIN CERRAR", symbol)


# ── Helpers ──────────────────────────────────────────────────────────────────────────────

def _reset_trader_position_state(trader, symbol: str) -> None:
    """
    Limpia el estado de posición del trader cuando se detecta cierre externo.
    """
    log.warning(
        "[%s] Reset estado posición: position=None sl=None tp1=None qty=0",
        symbol,
    )
    trader.position      = None
    trader.sl            = None
    trader.tp1           = None
    if hasattr(trader, "tp2"):               trader.tp2 = None
    if hasattr(trader, "tp3"):               trader.tp3 = None
    if hasattr(trader, "tp"):                trader.tp  = None
    trader._open_qty     = 0.0
    trader._protection_ok = False
    if hasattr(trader, "_tp1_be_done"):      trader._tp1_be_done = False
    if hasattr(trader, "entry_price"):       trader.entry_price  = None
    if hasattr(trader, "_entry_price"):      trader._entry_price = None
    if hasattr(trader, "trailing_sl_activated"): trader.trailing_sl_activated = False
    if hasattr(trader, "_trailing_peak"):   trader._trailing_peak = None
    if hasattr(trader, "atr"):              trader.atr = 0.0

    try:
        from bot.telegram_bot import send_message
        msg = (
            f"⚠️ *Posición cerrada externamente* `{symbol}`\n"
            f"Estado limpiado."
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
