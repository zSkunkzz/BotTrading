"""Escritura de órdenes — completamente aislado, exchange mockeado."""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class OrderExecutor:
    def __init__(self, exchange, dry_run: bool = False):
        self._ex = exchange
        self._dry = dry_run

    def open_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float | None = None,
        **kwargs,
    ) -> dict:
        if self._dry:
            log.info(
                "[order_executor] DRY RUN open_order %s %s %.4f",
                symbol,
                side,
                qty,
            )
            return {"orderId": "dry", "status": "Filled"}
        return self._ex.create_order(
            symbol,
            "limit" if price else "market",
            side,
            qty,
            price,
            params=kwargs,
        )

    def place_tpsl(
        self,
        symbol: str,
        side: str,
        qty: float,
        tp: float,
        sl: float,
    ) -> None:
        if self._dry:
            return
        self._ex.create_order(
            symbol,
            "limit",
            side,
            qty,
            tp,
            params={"reduceOnly": True, "tpTrigger": tp},
        )
        self._ex.create_order(
            symbol,
            "stop",
            side,
            qty,
            sl,
            params={"reduceOnly": True, "slTrigger": sl},
        )

    def cancel_all(self, symbol: str) -> None:
        try:
            self._ex.cancel_all_orders(symbol)
        except Exception as exc:
            log.warning("[order_executor] cancel_all error: %s", exc)
