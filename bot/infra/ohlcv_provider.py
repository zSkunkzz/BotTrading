"""Semáforo + retry sobre hl_http.get_candles."""
from __future__ import annotations

import asyncio
import logging

from bot.infra import hl_http

log = logging.getLogger(__name__)

_DEFAULT_RETRIES = 3


class OhlcvProvider:
    """Wrapper async con semáforo y reintentos sobre hl_http.get_candles."""

    def __init__(
        self,
        session,
        base_url: str,
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        self._session = session
        self._url = base_url
        self._sem = semaphore or asyncio.Semaphore(4)

    async def fetch(
        self,
        symbol: str,
        interval: str,
        limit: int = 200,
    ) -> list[dict]:
        """Devuelve barras OHLCV; lista vacía si todos los intentos fallan."""
        async with self._sem:
            for attempt in range(_DEFAULT_RETRIES):
                bars = hl_http.get_candles(
                    self._session, self._url, symbol, interval, limit
                )
                if bars:
                    return bars
                log.warning(
                    "[ohlcv] intento %d/%d vacío: %s %s",
                    attempt + 1,
                    _DEFAULT_RETRIES,
                    symbol,
                    interval,
                )
                await asyncio.sleep(0.5 * (attempt + 1))
        return []
