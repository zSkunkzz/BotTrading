"""
bot/balance_service.py

Servicio singleton de balance USDT para Bitget.
Un único fetch cada _TTL segundos compartido por TODOS los traders.
Elimina el problema de 429 causado por 15 traders haciendo fetches simultáneos.

Uso:
    from bot.balance_service import balance_svc
    balance_svc.init(api_key, api_secret, passphrase)
    bal = await balance_svc.get()   # todos los traders llaman esto
"""

import asyncio
import base64
import hashlib
import hmac
import json as _json
import logging
import os
import time

import aiohttp

logger = logging.getLogger("BalanceSvc")

_TTL      = int(os.getenv("BALANCE_CACHE_TTL", "60"))   # segundos entre fetches reales
_TIMEOUT  = float(os.getenv("BALANCE_TIMEOUT",  "10"))  # timeout HTTP por request


class BalanceService:
    """
    Singleton global de balance.
    - Un único asyncio.Lock garantiza que solo 1 coroutine hace el fetch a la vez.
    - El resto espera y reutiliza el resultado cacheado.
    - TTL configurable via env BALANCE_CACHE_TTL (default 60s).
    """

    def __init__(self):
        self._key:        str | None = None
        self._secret:     str | None = None
        self._passphrase: str | None = None
        self._value:      float | None = None
        self._ts:         float = 0.0
        self._lock:       asyncio.Lock | None = None
        self._ready:      bool = False
        self._warned_not_ready: bool = False

    # ── inicialización (llamar una vez al arrancar) ──────────────────────────

    def init(self, api_key: str, api_secret: str, passphrase: str):
        """Registra credenciales. Idempotente."""
        if self._ready:
            return
        self._key        = api_key
        self._secret     = api_secret
        self._passphrase = passphrase
        self._ready      = True
        logger.info("[BalanceSvc] ✅ Inicializado con credenciales OK")

    # ── API pública ──────────────────────────────────────────────────────────

    async def get(self) -> float | None:
        """Devuelve el balance USDT. Cachea TTL segundos. Thread-safe."""
        if not self._ready:
            if not self._warned_not_ready:
                logger.error("[BalanceSvc] ¡No inicializado! Llama balance_svc.init() primero.")
                self._warned_not_ready = True
            return None

        # Fast-path sin lock si la caché es fresca
        if self._value is not None and time.monotonic() - self._ts < _TTL:
            return self._value

        # Obtener lock (lazy-init seguro en asyncio)
        if self._lock is None:
            self._lock = asyncio.Lock()

        async with self._lock:
            # Re-check tras adquirir lock (otro task pudo haber actualizado)
            if self._value is not None and time.monotonic() - self._ts < _TTL:
                return self._value

            fresh = await self._fetch_with_retry()
            if fresh is not None:
                self._value = fresh
                self._ts    = time.monotonic()
                logger.info(f"[BalanceSvc] ✅ Balance actualizado: {fresh:.2f} USDT")
            elif self._value is not None:
                logger.warning(
                    f"[BalanceSvc] ⚠️ Fetch fallido — usando caché anterior: {self._value:.2f} USDT"
                )
            else:
                logger.error("[BalanceSvc] 🚨 Fetch fallido y sin caché previa")

            return self._value

    def invalidate(self):
        """Fuerza re-fetch en la próxima llamada a get()."""
        self._ts = 0.0

    # ── internos ─────────────────────────────────────────────────────────────

    def _sign(self, ts: str, method: str, path_qs: str, body: str = "") -> str:
        msg = ts + method.upper() + path_qs + body
        return base64.b64encode(
            hmac.new(self._secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _headers(self, method: str, path_qs: str, body: str = "") -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY":        self._key,
            "ACCESS-SIGN":       self._sign(ts, method, path_qs, body),
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }

    async def _get_json(self, path: str, qs: str = "") -> dict | None:
        """GET a Bitget. Maneja 429 con Retry-After. Devuelve None si no es JSON válido."""
        url = "https://api.bitget.com" + path + qs
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers=self._headers("GET", path + qs),
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT),
                ) as r:
                    if r.status == 429:
                        retry_after = float(r.headers.get("Retry-After", "5"))
                        logger.warning(
                            f"[BalanceSvc] ⚠️ Rate limit 429 en {path} — "
                            f"esperando {retry_after:.0f}s"
                        )
                        await asyncio.sleep(retry_after)
                        async with s.get(
                            url,
                            headers=self._headers("GET", path + qs),
                            timeout=aiohttp.ClientTimeout(total=_TIMEOUT),
                        ) as r2:
                            text = await r2.text()
                    else:
                        text = await r.text()
            stripped = text.strip()
            if not stripped.startswith("{") and not stripped.startswith("["):
                logger.debug(f"[BalanceSvc] {path} → no-JSON: {stripped[:120]}")
                return None
            data = _json.loads(stripped)
            if not isinstance(data, dict):
                logger.debug(f"[BalanceSvc] {path} → tipo inesperado: {type(data).__name__}")
                return None
            # Log detallado cuando el código no es éxito
            if data.get("code") != "00000":
                logger.warning(
                    f"[BalanceSvc] {path}{qs} → code={data.get('code')} msg={data.get('msg')} "
                    f"data={str(data.get('data', ''))[:120]}"
                )
            return data
        except Exception as e:
            logger.debug(f"[BalanceSvc] {path} excepción: {e}")
            return None

    async def _fetch_with_retry(self, max_retries: int = 3) -> float | None:
        """Llama a _fetch hasta max_retries veces con backoff exponencial."""
        for attempt in range(1, max_retries + 1):
            result = await self._fetch()
            if result is not None:
                return result
            wait = 2 ** attempt  # 2s, 4s, 8s
            logger.warning(
                f"[BalanceSvc] ⚠️ Intento {attempt}/{max_retries} fallido — "
                f"reintentando en {wait}s"
            )
            await asyncio.sleep(wait)
        return None

    async def _fetch(self) -> float | None:
        """
        Prueba endpoints en orden de fiabilidad para Bitget UA y Classic.

        Orden:
          1. /api/v2/account/all-account-balance  (Unified Account — campo 'available' en item coin=USDT)
          2. /api/v2/mix/account/accounts         (Classic USDT-FUTURES — campo 'available' en items[0])
          3. /api/v2/spot/account/assets          (último recurso — campo 'available' coin=USDT)
        """

        # ── Endpoint 1: UA all-account-balance ──────────────────────────────
        path = "/api/v2/account/all-account-balance"
        qs   = "?coin=USDT"
        data = await self._get_json(path, qs)
        if data and data.get("code") == "00000":
            items = data.get("data") or []
            items = items if isinstance(items, list) else []
            for item in items:
                if isinstance(item, dict) and item.get("coin") == "USDT":
                    try:
                        bal = float(item.get("available") or 0)
                        logger.debug(f"[BalanceSvc] ✔ all-account-balance(UA) → {bal:.2f}")
                        return bal
                    except (ValueError, TypeError):
                        pass

        # ── Endpoint 2: mix/account/accounts (Classic / fallback UA) ────────
        path = "/api/v2/mix/account/accounts"
        qs   = "?productType=USDT-FUTURES"
        data = await self._get_json(path, qs)
        if data and data.get("code") == "00000":
            items = data.get("data") or []
            items = items if isinstance(items, list) else []
            if items:
                try:
                    bal = float(items[0].get("available") or 0)
                    logger.debug(f"[BalanceSvc] ✔ mix/accounts → {bal:.2f}")
                    return bal
                except (ValueError, TypeError):
                    pass

        # ── Endpoint 3: spot/account/assets (último recurso) ─────────────────
        path = "/api/v2/spot/account/assets"
        qs   = "?coin=USDT"
        data = await self._get_json(path, qs)
        if data and data.get("code") == "00000":
            items = data.get("data") or []
            items = items if isinstance(items, list) else []
            for item in items:
                if isinstance(item, dict) and item.get("coin") == "USDT":
                    try:
                        bal = float(item.get("available") or 0)
                        logger.debug(f"[BalanceSvc] ✔ spot/assets → {bal:.2f}")
                        return bal
                    except (ValueError, TypeError):
                        pass

        logger.error("[BalanceSvc] 🚨 Todos los endpoints de balance fallaron")
        return None


balance_svc = BalanceService()
