#!/usr/bin/env python3
"""
trailing_hl.py — Trailing stop nativo en Hyperliquid

Hyperliquid soporta trailing stop como orden trigger.
Esta capa envía/actualiza la orden de trailing directamente en el exchange,
de modo que si el bot crashea la posición sigue protegida.

API HL trailing:
  order_type = {"trigger": {"triggerPx": price, "isMarket": True, "tpsl": "sl"}}
  reduce_only = True

Config Railway:
  TRAILING_HL_ENABLED      → default true
  TRAILING_ACTIVATION_PCT  → % desde entrada para activar trailing (default 1.0)
  TRAILING_CALLBACK_PCT    → % de retroceso desde máximo (default 1.5)
"""
from __future__ import annotations

import logging
import os
import time
from typing import Dict, Optional

log = logging.getLogger(__name__)

TRAILING_HL_ENABLED    = os.getenv("TRAILING_HL_ENABLED",   "true").lower() != "false"
ACTIVATION_PCT         = float(os.getenv("TRAILING_ACTIVATION_PCT", "1.0"))  # 1%
CALLBACK_PCT           = float(os.getenv("TRAILING_CALLBACK_PCT",   "1.5"))  # 1.5%


class TrailingHLManager:
    """
    Gestiona trailing stops nativos en HL por posición.
    Mantiene estado en memoria: precio máximo favorable alcanzado por símbolo.
    """

    def __init__(self) -> None:
        # {symbol: {"peak": float, "trail_px": float, "activated": bool, "order_id": str|None}}
        self._state: Dict[str, dict] = {}

    def on_position_open(self, symbol: str, entry: float, side: str) -> None:
        """Registrar nueva posición."""
        self._state[symbol] = {
            "entry":     entry,
            "side":      side,
            "peak":      entry,
            "trail_px":  None,
            "activated": False,
            "order_id":  None,
        }
        log.debug("[trailing_hl] %s open — entry %.4f side %s", symbol, entry, side)

    def on_position_close(self, symbol: str) -> None:
        """Limpiar estado al cerrar posición."""
        self._state.pop(symbol, None)

    async def update(
        self,
        symbol: str,
        current_price: float,
        exch=None,
        size: float = 0.0,
    ) -> Optional[float]:
        """
        Actualiza el trailing stop para un símbolo.
        Si se activa o mueve, coloca/actualiza la orden en HL.

        Returns: nuevo precio de trailing stop, o None si no activado.
        """
        if not TRAILING_HL_ENABLED:
            return None

        st = self._state.get(symbol)
        if st is None:
            return None

        side  = st["side"]
        entry = st["entry"]
        peak  = st["peak"]

        # Actualizar pico favorable
        if side == "long":
            if current_price > peak:
                st["peak"] = current_price
                peak = current_price
        else:
            if current_price < peak:
                st["peak"] = current_price
                peak = current_price

        # ¿Activado?
        if side == "long":
            activation_px = entry * (1 + ACTIVATION_PCT / 100)
            activated = peak >= activation_px
        else:
            activation_px = entry * (1 - ACTIVATION_PCT / 100)
            activated = peak <= activation_px

        if not activated:
            return None

        # Calcular precio de trailing
        if side == "long":
            trail_px = peak * (1 - CALLBACK_PCT / 100)
        else:
            trail_px = peak * (1 + CALLBACK_PCT / 100)

        trail_px = round(trail_px, 6)

        # ¿Cambió suficiente para actualizar la orden en HL?
        prev_trail = st.get("trail_px")
        changed = (
            prev_trail is None
            or (side == "long"  and trail_px > prev_trail * 1.001)
            or (side == "short" and trail_px < prev_trail * 0.999)
        )

        if changed:
            st["trail_px"]  = trail_px
            st["activated"] = True

            if exch is not None and size > 0:
                await self._place_trailing_order(exch, symbol, side, trail_px, size, st)

            log.debug(
                "[trailing_hl] %s %s peak=%.4f trail=%.4f",
                symbol, side.upper(), peak, trail_px,
            )

        return trail_px

    async def _place_trailing_order(
        self,
        exch,
        symbol: str,
        side: str,
        trail_px: float,
        size: float,
        st: dict,
    ) -> None:
        """
        Coloca una orden de stop trigger en Hyperliquid.
        Si ya hay una orden previa, la cancela primero.
        """
        try:
            # Cancelar orden previa si existe
            if st.get("order_id"):
                try:
                    await exch.cancel_order(st["order_id"], symbol)
                except Exception:
                    pass
                st["order_id"] = None

            is_buy = side == "short"  # trailing de short cierra comprando

            # HL: orden trigger con tpsl="sl" y reduce_only=True
            order_params = {
                "reduceOnly":  True,
                "orderType":   "Stop Market",
                "triggerPrice": trail_px,
            }

            order = await exch.create_order(
                symbol=symbol,
                type="market",
                side="buy" if is_buy else "sell",
                amount=size,
                params=order_params,
            )

            if order and order.get("id"):
                st["order_id"] = order["id"]
                log.info(
                    "[trailing_hl] %s orden SL nativo colocada → %.4f (id=%s)",
                    symbol, trail_px, order["id"],
                )

        except Exception as e:
            log.warning("[trailing_hl] %s error colocando orden: %s", symbol, e)


# Singleton
trailing_hl = TrailingHLManager()
