"""
bot/ohlcv_cache.py — Caché de OHLCV con TTL por timeframe y LRU eviction.

v5 — Backoff exponencial + jitter en fetch, semáforo bajado a 3:
  Con 10 traders × 3 TF = hasta 30 requests OHLCV simultáneas a BingX,
  la API puede devolver None de forma masiva (rate limit silencioso).
  Cambios respecto a v4:
  - BINGX_OHLCV_CONCURRENCY default: 5 → 3 para reducir presión sobre BingX.
  - fetch_fn se llama con backoff exponencial (1s, 2s, 4s) + jitter ±0.3s.
    Si BingX devuelve lista vacía o None en el primer intento, se reintenta
    hasta OHLCV_FETCH_RETRIES veces antes de declarar fallo.
  - Si todos los intentos fallan y hay datos expirados en caché, se
    devuelven con WARNING (stale fallback) en lugar de lista vacía.

v4 — Semáforo global _BINGX_OHLCV_SEMAPHORE:
  Con 10 traders × 3 TF = hasta 30 requests OHLCV simultáneas a BingX,
  la API devuelve None de forma masiva (rate limit silencioso no documentado).
  Se añade asyncio.Semaphore(BINGX_OHLCV_CONCURRENCY) que limita las requests
  reales en vuelo. Las lecturas del caché caliente NO consumen el semáforo
  (el guard ocurre antes de llamar fetch_fn).
  Env var: BINGX_OHLCV_CONCURRENCY (int, default 3)
  Retrocompatibilidad: si HL_OHLCV_CONCURRENCY está definida se usa como
  fallback (para no romper deploys con la env antigua).

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
import random
import time
from typing import Any, Callable, Dict, Optional

log = logging.getLogger(__name__)

_OHLCV_TTL_S       = float(os.getenv("OHLCV_CACHE_TTL_S",    "12"))   # fallback general
_TTL_15M           = float(os.getenv("OHLCV_CACHE_TTL_15M",  "12"))
_TTL_1H            = float(os.getenv("OHLCV_CACHE_TTL_1H",   "60"))
_TTL_4H            = float(os.getenv("OHLCV_CACHE_TTL_4H",   "180"))
_MAX_CACHE_SYMBOLS = int(os.getenv("MAX_OHLCV_CACHE_SYMBOLS", "20"))

# ── Semáforo global: limita requests OHLCV reales en vuelo ──────────────────
# Con 10 traders × 3 TF = hasta 30 requests simultáneas → BingX puede limitar.
# Default 3 para reducir presión. Retrocompatible con env antigua HL_OHLCV_CONCURRENCY.
_BINGX_OHLCV_CONCURRENCY = int(
    os.getenv("BINGX_OHLCV_CONCURRENCY")
    or os.getenv("HL_OHLCV_CONCURRENCY")  # retrocompatibilidad
    or "3"
)
_BINGX_OHLCV_SEMAPHORE: Optional[asyncio.Semaphore] = None

# ── Backoff exponencial: reintentos dentro del semáforo ─────────────────────
# Si fetch_fn devuelve vacío o lanza excepción, se reintenta hasta este límite.
# Esperas: 1s, 2s, 4s (× 2^intento) + jitter uniforme ±_OHLCV_FETCH_JITTER_S.
_OHLCV_FETCH_RETRIES = int(os.getenv("OHLCV_FETCH_RETRIES",  "3"))
_OHLCV_FETCH_JITTER  = float(os.getenv("OHLCV_FETCH_JITTER", "0.3"))


def _get_semaphore() -> asyncio.Semaphore:
    """Lazy-init del semáforo dentro del event loop activo."""
    global _BINGX_OHLCV_SEMAPHORE
    if _BINGX_OHLCV_SEMAPHORE is None:
        _BINGX_OHLCV_SEMAPHORE = asyncio.Semaphore(_BINGX_OHLCV_CONCURRENCY)
        log.info(
            "[OHLCVCache] Semáforo OHLCV inicializado: max_concurrency=%d (env BINGX_OHLCV_CONCURRENCY)",
            _BINGX_OHLCV_CONCURRENCY,
        )
    return _BINGX_OHLCV_SEMAPHORE


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
    Caché OHLCV con TTL por timeframe + LRU eviction + semáforo de concurrencia
    + backoff exponencial con jitter en fetch + stale fallback.
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
        Devuelve OHLCV desde caché si está fresco (TTL por TF), si no llama
        fetch_fn con backoff exponencial + jitter.

        Estrategia de resiliencia:
          1. Lectura caliente del caché — no consume el semáforo.
          2. Cache miss/expirado → adquirir semáforo y llamar fetch_fn.
          3. Si fetch_fn devuelve vacío o lanza excepción, se reintenta
             hasta _OHLCV_FETCH_RETRIES veces con espera 2^i + jitter.
          4. Si todos los intentos fallan y hay datos expirados en caché,
             se devuelven como stale fallback con WARNING.
          5. Si no hay datos en absoluto, devuelve [].
        """
        key = f"{coin}:{tf}"
        ttl = _ttl_for(tf)

        async with self._lock:
            entry = self._cache.get(key)
            now   = time.monotonic()
            if entry and (now - entry["ts"]) < ttl:
                entry["last_access"] = now
                return entry["data"]

        # Cache miss o expirado → adquirir semáforo antes de fetch
        sem = _get_semaphore()
        data: list = []
        last_exc: Optional[Exception] = None

        try:
            async with sem:
                for attempt in range(_OHLCV_FETCH_RETRIES):
                    try:
                        result = await fetch_fn(tf)
                        if result:  # lista no vacía → éxito
                            data = result
                            break
                        # BingX devolvió [] o None — tratar como fallo transitorio
                        log.warning(
                            "[OHLCVCache] fetch vacío %s/%s (intento %d/%d)",
                            coin, tf, attempt + 1, _OHLCV_FETCH_RETRIES,
                        )
                    except Exception as e:
                        last_exc = e
                        log.warning(
                            "[OHLCVCache] fetch error %s/%s (intento %d/%d): %s",
                            coin, tf, attempt + 1, _OHLCV_FETCH_RETRIES, e,
                        )

                    if attempt < _OHLCV_FETCH_RETRIES - 1:
                        backoff = (2 ** attempt) + random.uniform(
                            -_OHLCV_FETCH_JITTER, _OHLCV_FETCH_JITTER
                        )
                        backoff = max(0.2, backoff)  # mínimo 0.2s
                        log.debug(
                            "[OHLCVCache] backoff %.2fs antes de reintento %d para %s/%s",
                            backoff, attempt + 2, coin, tf,
                        )
                        await asyncio.sleep(backoff)

        except Exception as e:
            # El semáforo mismo lanzó excepción (improbable)
            log.error("[OHLCVCache] semáforo/fetch error %s/%s: %s", coin, tf, e)

        if not data:
            # Stale fallback: devolver datos expirados si los hay
            async with self._lock:
                entry = self._cache.get(key)
                if entry:
                    stale_age = time.monotonic() - entry["ts"]
                    log.warning(
                        "[OHLCVCache] Devolviendo datos STALE para %s/%s "
                        "(edad=%.1fs, ttl=%.1fs) — fetch falló%s",
                        coin, tf, stale_age, ttl,
                        f": {last_exc}" if last_exc else " (vacío)",
                    )
                    return entry["data"]
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
