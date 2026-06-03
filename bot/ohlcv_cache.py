"""
bot/ohlcv_cache.py — Caché de OHLCV con TTL por timeframe y LRU eviction.

v3 — TTL diferenciado por timeframe:
  15m → 12s  (igual que antes, velas de 15m se invalidan rápido)
  1h  → 60s  (velas de 1h cambian cada hora, no hace falta refrescar cada 12s)
  4h  → 180s (velas de 4h son muy estables, refrescar cada 3 min es suficiente)
  Otros TF → usa OHLCV_CACHE_TTL_S como fallback general

  Con 7 pares × 3 TF y el antiguo TTL universal de 12s, se hacían ~105 fetches
  por ciclo. Con TTL diferenciado baja a ~35 fetches efectivos.

v2 — BUG #9 FIX: sin límite de entradas → OOM tras días de rotación de pares
  MAX_OHLCV_CACHE_SYMBOLS (default 20) limita las entradas.
  LRU eviction: al superar el límite se elimina la entrada con
  último acceso más antiguo.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Callable, Dict, Optional

log = logging.getLogger(__name__)

_OHLCV_TTL_S       = float(os.getenv("OHLCV_CACHE_TTL_S",    "12"))   # fallback general
_TTL_15M           = float(os.getenv("OHLCV_CACHE_TTL_15M",  "12"))
_TTL_1H            = float(os.getenv("OHLCV_CACHE_TTL_1H",   "60"))
_TTL_4H            = float(os.getenv("OHLCV_CACHE_TTL_4H",   "180"))
_MAX_CACHE_SYMBOLS = int(os.getenv("MAX_OHLCV_CACHE_SYMBOLS", "20"))

_TTL_BY_TF: Dict[str, float] = {
    "1m":  _OHLCV_TTL_S,
    "3m":  _OHLCV_TTL_S,
    "5m":  _OHLCV_TTL_S,
    "15m": _TTL_15M,
    "30m": _OHLCV_TTL_S,
    "1h":  _TTL_1H,
    "2h":  _TTL_1H,
    "4h":  _TTL_4H,
    "8h":  _TTL_4H,
    "1d":  float(os.getenv("OHLCV_CACHE_TTL_1D", "600")),
}


def _ttl_for(tf: str) -> float:
    return _TTL_BY_TF.get(tf, _OHLCV_TTL_S)


class OHLCVCache:
    """
    Caché OHLCV con TTL por timeframe + LRU eviction por número de símbolos.
    """

    def __init__(self, max_symbols: int = _MAX_CACHE_SYMBOLS):
        self._max    = max_symbols
        self._cache: Dict[str, Dict[str, Any]] = {}   # key -> {data, ts, last_access, ttl}
        self._lock   = asyncio.Lock()

    async def get(
        self,
        coin: str,
        tf: str,
        fetch_fn: Callable[[str], Any],
    ) -> list:
        """
        Devuelve OHLCV desde caché si está fresco (TTL por TF), si no llama fetch_fn.
        """
        key = f"{coin}:{tf}"
        ttl = _ttl_for(tf)

        async with self._lock:
            entry = self._cache.get(key)
            now   = time.monotonic()
            if entry and (now - entry["ts"]) < ttl:
                entry["last_access"] = now
                return entry["data"]

        try:
            data = await fetch_fn(tf)
        except Exception as e:
            log.error("[OHLCVCache] fetch error %s/%s: %s", coin, tf, e)
            async with self._lock:
                entry = self._cache.get(key)
                if entry:
                    log.warning(
                        "[OHLCVCache] Devolviendo datos expirados para %s/%s (fetch falló)",
                        coin, tf,
                    )
                    return entry["data"]
            return []

        if not data:
            return []

        async with self._lock:
            now = time.monotonic()
            if key not in self._cache and len(self._cache) >= self._max:
                oldest_key = min(
                    self._cache,
                    key=lambda k: self._cache[k].get("last_access", self._cache[k]["ts"]),
                )
                del self._cache[oldest_key]
                log.debug(
                    "[OHLCVCache] LRU eviction: eliminado %s (cache lleno, max=%d)",
                    oldest_key, self._max,
                )
            self._cache[key] = {"data": data, "ts": now, "last_access": now, "ttl": ttl}

        return data

    async def invalidate(self, coin: str, tf: Optional[str] = None) -> None:
        async with self._lock:
            if tf:
                self._cache.pop(f"{coin}:{tf}", None)
            else:
                keys_to_del = [k for k in self._cache if k.startswith(f"{coin}:")]
                for k in keys_to_del:
                    del self._cache[k]

    async def stats(self) -> dict:
        async with self._lock:
            return {
                "entries": len(self._cache),
                "max":     self._max,
                "keys":    list(self._cache.keys()),
            }


ohlcv_cache = OHLCVCache()
