#!/usr/bin/env python3
"""
trailing_hl.py — DESACTIVADO.

El trailing TP ha sido eliminado porque no funcionaba correctamente.
El bot ahora usa únicamente un TP fijo calculado por signal_engine
(basado en ATR con multiplicadores conservadores para mayor probabilidad de hit).

Este módulo se mantiene como stub vacío para no romper imports.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger(__name__)


class TrailingHLManager:
    """Stub vacío — trailing TP desactivado."""

    def on_position_open(self, symbol: str, entry: float, side: str) -> None:
        pass

    def on_position_close(self, symbol: str) -> None:
        pass

    async def update(
        self,
        symbol: str,
        current_price: float,
        exch=None,
        size: float = 0.0,
    ) -> Optional[float]:
        return None


# Singleton
trailing_hl = TrailingHLManager()
