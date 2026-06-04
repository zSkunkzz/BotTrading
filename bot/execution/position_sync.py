"""Lectura de posiciones abiertas — exchange mockeado en tests."""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


class PositionSync:
    def __init__(self, exchange):
        self._ex = exchange

    def get_open(self, symbol: str) -> dict | None:
        try:
            positions = self._ex.fetch_positions([symbol])
            for p in positions:
                if float(p.get("contracts", 0)) != 0:
                    return p
            return None
        except Exception as exc:
            log.error("[position_sync] error: %s", exc)
            return None

    def get_all_open(self) -> list[dict]:
        try:
            return [
                p
                for p in self._ex.fetch_positions()
                if float(p.get("contracts", 0)) != 0
            ]
        except Exception as exc:
            log.error("[position_sync] get_all_open error: %s", exc)
            return []
