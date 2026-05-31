"""
ohlcv_cache.py — Cache OHLCV compartido entre todos los traders.

Problema que resuelve:
  Con 15 traders corriendo en paralelo, cada uno pidiendo velas en 3
  timeframes (15m, 1h, 4h), se generan ~45 requests simultáneos a
  api.hyperliquid.xyz/info, disparando 429s.

Solución:
  Un único OHLCVCache singleton con TTL de 60 segundos por (coin, tf).
  Si BTC/1h ya fue pedido hace 30s, los demás traders reciben el valor
  cacheado sin hacer una nueva request REST.

  Además, usa un lock por clave para evitar el problema de "thundering herd":
  si 10 traders piden el mismo (BTC, 1h) al mismo tiempo y el caché está
  vacío, solo 1 hace la request real — los otros 9 esperan y usan el resultado.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

logger = logging.getLogger("OHLCVCache")

# TTL aumentado a 60s para reducir requests REST con 15 traders activos.
# Las velas de 15m se actualizan cada 15min, por lo que 60s de caché no
# afecta la calidad de las señales pero reduce los 429s significativamente.
_DEFAULT_TTL = 60.0  # segundos


class _OHLCVCache:
    def __init__(self, ttl: float = _DEFAULT_TTL):
        self._ttl    = ttl
        self._cache: dict[tuple, tuple] = {}   # (coin, tf) → (data, timestamp)
        self._locks: dict[tuple, asyncio.Lock] = {}
        self._meta_lock = asyncio.Lock()  # protege creación de locks individuales

    async def _key_lock(self, key: tuple) -> asyncio.Lock:
        async with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    async def get(
        self,
        coin: str,
        tf:   str,
        fetch_fn: Callable[..., Awaitable[list]],
    ) -> list:
        """
        Devuelve velas OHLCV para (coin, tf).

        Si el caché tiene datos frescos, los devuelve directamente.
        Si no, llama fetch_fn() exactamente una vez aunque haya múltiples
        traders esperando la misma clave (thundering-herd prevention).

        Args:
            coin:     nombre normalizado, e.g. "BTC"
            tf:       timeframe, e.g. "15m", "1h", "4h"
            fetch_fn: corrutina que recibe (tf) y devuelve list[candle]
        """
        key = (coin, tf)
        now = time.monotonic()

        # Fast path: hit de caché sin lock
        entry = self._cache.get(key)
        if entry is not None:
            data, ts = entry
            if now - ts < self._ttl:
                logger.debug("[OHLCVCache] HIT %s/%s (age=%.1fs)", coin, tf, now - ts)
                return data

        # Slow path: adquirir lock por clave
        klock = await self._key_lock(key)
        async with klock:
            # Doble check tras adquirir el lock (otro trader pudo haberlo llenado)
            entry = self._cache.get(key)
            if entry is not None:
                data, ts = entry
                if time.monotonic() - ts < self._ttl:
                    logger.debug("[OHLCVCache] HIT(2) %s/%s", coin, tf)
                    return data

            # Fetch real
            try:
                data = await fetch_fn(tf)
            except Exception as e:
                logger.warning("[OHLCVCache] fetch error %s/%s: %s", coin, tf, e)
                # Devolver datos viejos si los hay, mejor que vacío
                if entry is not None:
                    return entry[0]
                return []

            if data:
                self._cache[key] = (data, time.monotonic())
                logger.debug("[OHLCVCache] MISS→stored %s/%s (%d candles)",
                             coin, tf, len(data))
            return data or []

    def invalidate(self, coin: Optional[str] = None, tf: Optional[str] = None):
        """Invalida entradas. Sin args = limpia todo el caché."""
        if coin is None:
            self._cache.clear()
            logger.debug("[OHLCVCache] caché completo invalidado")
        else:
            key = (coin, tf) if tf else None
            if key:
                self._cache.pop(key, None)
            else:
                # Invalida todas las TFs de este coin
                for k in list(self._cache.keys()):
                    if k[0] == coin:
                        self._cache.pop(k, None)


# Singleton compartido
ohlcv_cache = _OHLCVCache()
