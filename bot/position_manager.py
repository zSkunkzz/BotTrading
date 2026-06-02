#!/usr/bin/env python3
"""
position_manager.py — Gestión de posiciones abiertas.

Fixes incluidos en esta versión:
  Bug A — _ensure_tpsl detecta SL/TP por tipo de orden (tpsl=="sl"/"tp"),
           no por conteo total de órdenes reduce_only.
  Bug B — check_sl_software añade margen configurable SL_SW_MARGIN_PCT
           para evitar doble cierre cuando el exchange ya ejecutó el SL.
  Bug C — _place_emergency_sl_tp redondea qty antes de enviar la orden.
  Bug H — _update_trailing_sl ahora invoca trailing_hl.update() para
           que el trailing stop nativo de HL esté activo en producción.
           Anteriormente solo delegaba en trader._do_trailing_sl_update()
           que no existe en ai_trader.py, dejando el trailing inactivo.
  Bug I — _check_sl_software y _place_emergency_sl_tp ahora toleran que
           trader.position sea un str ("long"/"short") además de un dict
           {"side": "long"/"short"}, evitando AttributeError: 'str' object
           has no attribute 'get'.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass

from bot.trailing_hl import trailing_hl  # Bug H fix

log = logging.getLogger(__name__)

# Bug B: margen de precio para el check de SL por software.
# Evita disparar cierre cuando el exchange ya ejecutó el SL (doble cierre).
# Valor: fracción del precio SL (0.0005 = 0.05%).
_SL_SW_MARGIN = float(os.getenv("SL_SW_MARGIN_PCT", "0.0005"))

# Intervalo entre checks de TPSL en el exchange (segundos)
_TPSL_VERIFY_INTERVAL_S = float(os.getenv("TPSL_VERIFY_INTERVAL_S", "60"))

# Máximo de reintentos para colocar SL/TP de emergencia
_EMERGENCY_TPSL_RETRIES = int(os.getenv("EMERGENCY_TPSL_RETRIES", "3"))


def _is_reduce_only(order: dict) -> bool:
    """True si la orden es reduce-only (cierre de posición)."""
    if order.get("reduceOnly"):
        return True
    ot = order.get("orderType", {})
    if isinstance(ot, dict):
        trigger = ot.get("trigger", {})
        if isinstance(trigger, dict) and trigger.get("tpsl") in ("sl", "tp"):
            return True
    return False


def _get_tpsl_type(order: dict) -> Optional[str]:
    """
    Retorna 'sl', 'tp', o None según el campo orderType.trigger.tpsl.
    Bug A fix: usamos este helper en lugar de contar reduce_only totales.
    """
    ot = order.get("orderType", {})
    if isinstance(ot, dict):
        trigger = ot.get("trigger", {})
        if isinstance(trigger, dict):
            return trigger.get("tpsl")  # 'sl', 'tp', o None
    return None


def _round_qty_safe(trader, qty: float) -> float:
    """
    Bug C fix: redondea qty usando el método del trader si existe,
    sino usa round() con 4 decimales como fallback seguro.
    """
    if hasattr(trader, "_round_qty") and callable(trader._round_qty):
        try:
            return trader._round_qty(qty)
        except Exception:
            pass
    return round(qty, 4)


def _resolve_is_long(position) -> bool:
    """
    Bug I fix: extrae el lado de la posición tanto si position es un dict
    {"side": "long"/"short"} como si es directamente un str "long"/"short".
    Devuelve True para LONG, False para SHORT.
    """
    if isinstance(position, dict):
        return position.get("side", "").upper() == "LONG"
    if isinstance(position, str):
        return position.upper() == "LONG"
    return False


class PositionManager:
    """
    Gestiona el ciclo de vida de una posición abierta:
      - Verificación periódica de órdenes SL/TP en el exchange
      - Colocación de emergencia si faltan SL/TP
      - Check de SL por software (con margen anti-doble-cierre)
      - Trailing SL nativo en HL (Bug H fix)
    """

    def __init__(self, trader) -> None:
        self._trader = trader
        self._last_tpsl_check: float = 0.0

    # ── API pública ────────────────────────────────────────────────────────────

    async def manage(self) -> None:
        """Llamar cada tick mientras hay posición abierta."""
        trader = self._trader
        now = time.monotonic()

        # 1. Check SL por software (con margen anti-doble-cierre)
        if await self._check_sl_software():
            return  # posición cerrada

        # 2. Verificación periódica de SL/TP en el exchange
        if now - self._last_tpsl_check >= _TPSL_VERIFY_INTERVAL_S:
            self._last_tpsl_check = now
            await self._ensure_tpsl()

        # 3. Trailing SL (si está activado)
        if getattr(trader, "_trailing_sl_active", False):
            await self._update_trailing_sl()

    # ── Check SL por software ──────────────────────────────────────────────────

    async def _check_sl_software(self) -> bool:
        """
        Bug B fix: comprueba si el precio actual ha cruzado el SL,
        aplicando un margen configurable (_SL_SW_MARGIN) para evitar
        disparar un cierre cuando el exchange ya ejecutó el SL.

        Bug I fix: tolera position como str o dict.

        Retorna True si se ha disparado el cierre, False en caso contrario.
        """
        trader = self._trader
        sl = getattr(trader, "sl", None)
        position = getattr(trader, "position", None)
        if not sl or not position:
            return False

        price = getattr(trader, "_last_price", None)
        if not price:
            return False

        # Bug I: usar helper que acepta str o dict
        is_long = _resolve_is_long(position)

        if is_long:
            # Long: SL se activa si el precio cae por debajo de sl × (1 - margen)
            threshold = sl * (1.0 - _SL_SW_MARGIN)
            triggered = price <= threshold
        else:
            # Short: SL se activa si el precio sube por encima de sl × (1 + margen)
            threshold = sl * (1.0 + _SL_SW_MARGIN)
            triggered = price >= threshold

        if not triggered:
            return False

        log.warning(
            "[%s] SL SW disparado: precio=%.4f umbral=%.4f sl=%.4f margen=%.4f%%",
            getattr(trader, "symbol", "?"),
            price, threshold, sl, _SL_SW_MARGIN * 100,
        )

        # Solo cerrar por software si el exchange NO tiene protección activa
        if not getattr(trader, "_protection_ok", False):
            await self._emergency_close(reason="SL_SW")
            return True
        else:
            log.info(
                "[%s] SL SW: precio cruzó umbral pero _protection_ok=True → esperando fill del exchange",
                getattr(trader, "symbol", "?"),
            )
            return False

    # ── Verificación SL/TP en exchange ─────────────────────────────────────────

    async def _ensure_tpsl(self) -> None:
        """
        Bug A fix: verifica que haya una orden SL Y una orden TP activas
        en el exchange usando el campo orderType.trigger.tpsl, NO por conteo
        de órdenes reduce_only totales.

        Si falta alguna, la coloca de emergencia.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        try:
            raw_orders = await trader._get_open_orders_raw()
        except Exception as e:
            log.warning("[%s] _ensure_tpsl: no se pudieron obtener órdenes: %s", symbol, e)
            return

        coin_orders = [
            o for o in (raw_orders or [])
            if o.get("coin", "").upper() == symbol.upper()
        ]

        # Bug A: filtrar por tipo explícito, no por conteo
        has_sl = any(_get_tpsl_type(o) == "sl" for o in coin_orders)
        has_tp = any(_get_tpsl_type(o) == "tp" for o in coin_orders)

        sl_count = sum(1 for o in coin_orders if _get_tpsl_type(o) == "sl")
        tp_count = sum(1 for o in coin_orders if _get_tpsl_type(o) == "tp")
        ro_count = sum(1 for o in coin_orders if _is_reduce_only(o))

        log.debug(
            "[%s] _ensure_tpsl: sl=%d tp=%d ro_total=%d → has_sl=%s has_tp=%s",
            symbol, sl_count, tp_count, ro_count, has_sl, has_tp,
        )

        if has_sl and has_tp:
            trader._protection_ok = True
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

    # ── Colocación de emergencia SL/TP ─────────────────────────────────────────

    async def _place_emergency_sl_tp(self, place_sl: bool = True, place_tp: bool = True) -> None:
        """
        Bug C fix: redondea qty antes de enviar la orden al exchange.
        Bug I fix: tolera position como str o dict.
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")

        sl_price = getattr(trader, "sl", None)
        tp_price = getattr(trader, "tp", None)
        open_qty_raw = getattr(trader, "_open_qty", 0.0) or 0.0

        # Bug C: redondear qty al número de decimales que el exchange acepta
        open_qty = _round_qty_safe(trader, open_qty_raw)

        if open_qty <= 0:
            log.error("[%s] _place_emergency_sl_tp: qty=0 — no se puede colocar orden", symbol)
            return

        position = getattr(trader, "position", None)
        # Bug I: usar helper que acepta str o dict
        is_long = _resolve_is_long(position)

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

                break  # éxito

            except Exception as e:
                log.warning(
                    "[%s] _place_emergency_sl_tp intento %d/%d falló: %s",
                    symbol, attempt, _EMERGENCY_TPSL_RETRIES, e,
                )
                if attempt < _EMERGENCY_TPSL_RETRIES:
                    await asyncio.sleep(2 ** attempt)  # backoff exponencial

    # ── Trailing SL ───────────────────────────────────────────────────────────

    async def _update_trailing_sl(self) -> None:
        """
        Bug H fix: invoca trailing_hl.update() para que el trailing stop
        nativo de Hyperliquid se coloque/actualice en el exchange.

        Antes solo delegaba en trader._do_trailing_sl_update() que no existe
        en ai_trader.py, dejando el trailing completamente inactivo.

        Prioridad:
          1. trailing_hl.update() — trailing nativo en HL (supervive crashes)
          2. trader._do_trailing_sl_update() — fallback legacy si existe
        """
        trader = self._trader
        symbol = getattr(trader, "symbol", "?")
        price  = getattr(trader, "_last_price", None)

        # 1. Trailing nativo HL (Bug H fix)
        if price:
            position = getattr(trader, "position", {}) or {}
            # Bug I: tolerar position como str
            if isinstance(position, str):
                side_raw = position.lower()
            else:
                side_raw = position.get("side", "long").lower()
            side = "long" if "long" in side_raw else "short"
            size = getattr(trader, "_open_qty", 0.0) or 0.0
            exch = getattr(trader, "_exch", None) or getattr(trader, "exchange", None)
            try:
                trail_px = await trailing_hl.update(
                    symbol=symbol,
                    current_price=price,
                    exch=exch,
                    size=size,
                )
                if trail_px is not None:
                    log.debug(
                        "[%s] trailing_hl activo → trail_px=%.4f",
                        symbol, trail_px,
                    )
            except Exception as e:
                log.debug("[%s] trailing_hl.update error: %s", symbol, e)

        # 2. Fallback legacy (compatibilidad)
        update_fn = getattr(trader, "_do_trailing_sl_update", None)
        if callable(update_fn):
            try:
                await update_fn()
            except Exception as e:
                log.debug("[%s] trailing SL legacy update error: %s", symbol, e)

    # ── Cierre de emergencia ──────────────────────────────────────────────────

    async def _emergency_close(self, reason: str = "EMERGENCY") -> None:
        """Cierra la posición a mercado como último recurso."""
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
