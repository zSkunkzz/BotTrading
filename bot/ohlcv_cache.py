"""
bot/ohlcv_cache.py — Caché de OHLCV con TTL y LRU eviction.

v2 — BUG #9 FIX: sin límite de entradas → OOM tras días de rotación de pares

  El _cache original era un dict que crecía indefinidamente.
  Con 7+ pares rotando cada 30 min y 200 velas por par, podía acumular
  cientos de MB hasta que Railway matara el proceso por OOM.

  Fix: MAX_OHLCV_CACHE_SYMBOLS (default 20) limita las entradas.
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

_OHLCV_TTL_S          = float(os.getenv("OHLCV_CACHE_TTL_S", "12"))
_MAX_CACHE_SYMBOLS    = int(os.getenv("MAX_OHLCV_CACHE_SYMBOLS", "20"))


class OHLCVCache:
    """
    Caché OHLCV con TTL + LRU eviction por número de símbolos.

    BUG #9 FIX: al superar _MAX_CACHE_SYMBOLS entradas, se elimina
    la entrada con timestamp de último acceso más antiguo.
    """

    def __init__(self, ttl_s: float = _OHLCV_TTL_S, max_symbols: int = _MAX_CACHE_SYMBOLS):
        self._ttl       = ttl_s
        self._max       = max_symbols
        self._cache:    Dict[str, Dict[str, Any]] = {}   # key -> {data, ts, last_access}
        self._lock      = asyncio.Lock()

    async def get(
        self,
        coin: str,
        tf: str,
        fetch_fn: Callable[[str], Any],
    ) -> list:
        """
        Devuelve OHLCV desde caché si está fresco, si no llama fetch_fn.
        BUG #9: aplica eviction LRU antes de insertar entrada nueva.
        """
        key = f"{coin}:{tf}"
        async with self._lock:
            entry = self._cache.get(key)
            now   = time.monotonic()
            if entry and (now - entry["ts"]) < self._ttl:
                entry["last_access"] = now
                return entry["data"]

        # Fetch fuera del lock para no bloquear otros lectores
        try:
            data = await fetch_fn(tf)
        except Exception as e:
            log.error("[OHLCVCache] fetch error %s/%s: %s", coin, tf, e)
            # Devolver datos expirados si los hay
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
            # BUG #9 FIX: eviction LRU si superamos el límite
            if key not in self._cache and len(self._cache) >= self._max:
                # Eliminar la entrada con last_access más antiguo
                oldest_key = min(
                    self._cache,
                    key=lambda k: self._cache[k].get("last_access", self._cache[k]["ts"]),
                )
                del self._cache[oldest_key]
                log.debug(
                    "[OHLCVCache] LRU eviction: eliminado %s (cache lleno, max=%d)",
                    oldest_key, self._max,
                )
            self._cache[key] = {"data": data, "ts": now, "last_access": now}

        return data

    async def invalidate(self, coin: str, tf: Optional[str] = None) -> None:
        """Invalidar una entrada o todas las del coin."""
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
