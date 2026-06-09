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

v27 — trailing SL ATR integrado.
v28 — Fix 2 bugs trailing SL.
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
      2. indicators['15m']['atr'] — fallback si trader.atr no existe
      3. 0.0                  — fallback final
    """
    atr = getattr(trader, "atr", None)
    if atr and float(atr) > 0:
        return float(atr)
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
        await self._check_trailing_sl()
        if now - self._last_tpsl_check >= _TPSL_VERIFY_INTERVAL_S:
            self._last_tpsl_check = now
            await self._ensure_tpsl()

    # ── Trailing SL ────────────────────────────────────────────────────────────────

    async def _check_trailing_sl(self) -> None:
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

        trader._trailing_peak = new_peak

        if new_sl != current_sl:
            log.info(
                "[%s] 🟡 trailing SL: %.6f → %.6f (pico=%.6f atr=%.6f)",
                symbol, current_sl, new_sl, new_peak, atr_val,
            )
            trader.sl = new_sl

        if is_trailing_sl_hit(is_long=is_long, current_price=price, trailing_sl=new_sl):
            log.warning(
                "[%s] 🔴 TRAILING SL HIT: precio=%.6f sl=%.6f",
                symbol, price, new_sl,
            )
            await self._emergency_close(reason="TRAILING_SL")

    # ── Break-Even ─────────────────────────────────────────────────────────────────

    async def _check_break_even(self) -> None:
        trader = self._trader

        if getattr(trader, "_tp1_be_done", False):
            return

        position = getattr(trader, "position",    None)
        entry    = getattr(trader, "entry_price", None)
        tp1      = getattr(trader, "tp1",         None)
        sl       = getattr(trader, "sl",          None)
        price    = getattr(trader, "_last_price", 