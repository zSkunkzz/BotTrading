#!/usr/bin/env python3
"""
bot/position_manager.py — Gestión de protección SL/TP para posiciones abiertas.

Bug A (CRÍTICO) — _ensure_tpsl consultaba solo openOrders, pero en Hyperliquid
  los SL/TP colocados con place_sl/place_tp son TRIGGER ORDERS que viven en
  frontendOpenOrders. El resultado: _ensure_tpsl siempre los veía como
  "faltantes" y los recolocaba cada ~30s → spam infinito en logs.
  Fix: consultar también _get_open_trigger_orders_raw() y combinar ambas listas.

Bug B — fallback por precio para detectar SL/TP cuando el campo tpsl no viene
  correctamente parseado por alguna variante de la respuesta de HL.

Bug C — _place_emergency_sl_tp redondea qty antes de enviar la orden.

Bug I — _resolve_is_long tolera position como str o dict.

Bug J — TP dinámico cuando trader.tp es None (posiciones restauradas del state).

FEAT BE (Break-Even) — Cuando el precio se aleja de la entrada un porcentaje
  configurable hacia el TP, el SL se mueve automáticamente a la entrada.
  Configurable con:
    BE_TRIGGER_PCT  (default 0.4) — % del recorrido entry→TP1 necesario para activar
    BE_OFFSET_PCT   (default 0.0) — offset sobre entry (0 = BE exacto, >0 = pequeño beneficio)
  El BE solo se activa una vez por posición (_tp1_be_done). Una vez activado,
  cancela el SL antiguo del exchange y coloca uno nuevo en entry+offset.
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
    Compatible con openOrders y frontendOpenOrders de Hyperliquid.
    """
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

        Ejemplo con BE_TRIGGER_PCT=0.4:
          LONG entry=100, TP1=110 → recorrido=10
          Trigger en precio >= 100 + 10*0.4 = 104
          SL se mueve a 100 * (1 + BE_OFFSET_PCT)

        Solo se activa UNA VEZ por posición (_tp1_be_done).
        """
        trader = self._trader

        # Ya activado o sin datos mínimos
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

        # ¿El precio alcanzó el trigger?
        if is_long:
            triggered = price >= entry + trigger_dist
        else:
            triggered = price <= entry - trigger_dist

        if not triggered:
            return

        # Calcular precio de BE
        be_price = round(entry * (1 + _BE_OFFSET_PCT) if is_long else entry * (1 - _BE_OFFSET_PCT), 6)

        # No mover si el SL ya está por encima (LONG) o por debajo (SHORT) del BE
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

        # Actualizar SL en el exchange
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

        hl = getattr(trader, "_hl_client", None)
        if hl is None:
            log.warning("[%s] BE: _hl_client no disponible — SL no actualizado.", symbol)
            return

        open_qty = _round_qty_safe(trader, getattr(trader, "_open_qty", 0.0) or 0.0)
        if open_qty <= 0:
            log.warning("[%s] BE: qty=0 — no se puede colocar SL de BE.", symbol)
            return

        # 1. Cancelar SL antiguo
        try:
            await asyncio.to_thread(hl.cancel_all_open_tpsl)
            log.info("[%s] BE: SL/TP anteriores cancelados.", symbol)
        except Exception as e:
            log.warning("[%s] BE: no se pudo cancelar SL antiguo: %s", symbol, e)

        # 2. Colocar SL nuevo en BE
        entry_price = getattr(trader, "entry_price", be_price)
        try:
            result = await asyncio.to_thread(
                hl.place_sl,
                not is_long,   # is_buy del SL es contrario a la posición
                open_qty,
                be_price,
                entry_price,
            )
            log.info("[%s] BE: SL colocado en entrada (%.4f): %s", symbol, be_price, result)
        except Exception as e:
            log.error("[%s] BE: error colocando SL en BE: %s", symbol, e)
            # Revertir flag para que lo reintente en el siguiente tick
            trader._tp1_be_done = False
            trader.sl = None
            return

        # 3. Recolocar TP1 (cancel_all_open_tpsl lo borró también)
        tp1 = getattr(trader, "tp1", None)
        if tp1 and tp1 > 0:
            try:
                result = await asyncio.to_thread(
                    hl.place_tp,
                    not is_long,
                    open_qty,
                    tp1,
                    None,
                    entry_price,
                )
                log.info("[%s] BE: TP1 recolocado en %.4f: %s", symbol, tp1, result)
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

        # Si _protection_ok=True, el exchange ya tiene la orden — esperar su fill
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
        Bug A fix: Los SL/TP de Hyperliquid son trigger orders.
        Se detectan en frontendOpenOrders, no en openOrders.
        Combinamos ambos endpoints para detectar correctamente.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        # 1. Órdenes normales (limit/market)
        try:
            raw_orders = await trader._get_open_orders_raw() or []
        except Exception as e:
            log.warning("[%s] _ensure_tpsl: openOrders error: %s", symbol, e)
            raw_orders = []

        # 2. Trigger orders (donde viven SL/TP en HL)
        trigger_orders: list[dict] = []
        get_trigger_fn = getattr(trader, "_get_open_trigger_orders_raw", None)
        if callable(get_trigger_fn):
            try:
                trigger_orders = await get_trigger_fn() or []
            except Exception as e:
                log.warning("[%s] _ensure_tpsl: frontendOpenOrders error: %s", symbol, e)

        # 3. Combinar y filtrar por coin
        all_orders = raw_orders + trigger_orders
        coin = getattr(trader, "coin", symbol).upper()
        coin_orders = [
            o for o in all_orders
            if str(o.get("coin", "")).upper() == coin
        ]

        # 4. Detectar SL/TP por tipo de orden
        has_sl = any(_get_tpsl_type(o) == "sl" for o in coin_orders)
        has_tp = any(_get_tpsl_type(o) == "tp" for o in coin_orders)

        # 5. Bug B: fallback por precio si el campo tpsl no viene parseado
        if not has_sl or not has_tp:
            sl_price = getattr(trader, "sl", None)
            tp_price = getattr(trader, "tp1", None) or getattr(trader, "tp", None)
            for o in coin_orders:
                if not _is_reduce_only(o):
                    continue
                try:
                    opx = float(o.get("limitPx") or o.get("triggerPx") or 0)
                except (TypeError, ValueError):
                    opx = 0.0
                if not has_sl and sl_price and opx:
                    if abs(opx - sl_price) / sl_price < 0.002:
                        has_sl = True
                if not has_tp and tp_price and opx:
                    if abs(opx - tp_price) / tp_price < 0.002:
                        has_tp = True

        log.debug(
            "[%s] _ensure_tpsl: total=%d (normal=%d trigger=%d) has_sl=%s has_tp=%s",
            symbol, len(all_orders), len(raw_orders), len(trigger_orders), has_sl, has_tp,
        )

        if has_sl and has_tp:
            trader._protection_ok = True
            return

        # Si _protection_ok ya era True y ahora no encontramos SL/TP,
        # probablemente ya se ejecutaron (fill). No spamear con emergencia.
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

                # Marcar protección OK tras colocación exitosa
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
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")
        close_fn = getattr(trader, "_close_position", None)
        if callable(close_fn):
            try:
                log.warning("[%s] Cierre de emergencia: %s", symbol, reason)
                await close_fn(reason=reason)
            except Exception as e:
                log.error("[%s] _emergency_close falló: %s", symbol, e)
        else:
            log.error(
                "[%s] _emergency_close: el trader no tiene _close_position — posición SIN CERRAR",
                symbol,
            )
