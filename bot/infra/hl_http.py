"""Funciones HTTP sin estado — sin dependencias internas del bot."""
from __future__ import annotations

import logging

import httpx

log = logging.getLogger(__name__)


def get_candles(
    session: httpx.Client,
    url: str,
    symbol: str,
    interval: str,
    limit: int,
) -> list[dict]:
    """GET /candle — devuelve lista cruda de barras o [] en error."""
    try:
        r = session.get(
            url,
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("result", {}).get("list", [])
    except Exception as exc:  # noqa: BLE001
        log.warning("[hl_http] get_candles error: %s", exc)
        return []


def get_ticker(session: httpx.Client, url: str, symbol: str) -> dict:
    """GET /ticker — devuelve dict de resultado o {} en error."""
    try:
        r = session.get(url, params={"symbol": symbol}, timeout=5)
        r.raise_for_status()
        return r.json().get("result", {})
    except Exception as exc:  # noqa: BLE001
        log.warning("[hl_http] get_ticker error: %s", exc)
        return {}
