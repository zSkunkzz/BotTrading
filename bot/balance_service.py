"""
bot/balance_service.py

Servicio singleton de balance USDT para Bitget (Unified Account).
Un único fetch cada BALANCE_TTL segundos compartido por TODOS los traders.
Uso:
    from bot.balance_service import balance_svc
    balance_svc.init(api_key, api_secret, passphrase)
    bal = await balance_svc.get()
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
_TIMEOUT  = float(os.getenv("BALANCE_TIMEOUT",  "10"))   # timeout HTTP por request


class BalanceService:
    """
    Singleton global de balance para Unified Account.
    - Un único asyncio.Lock garantiza que solo 1 coroutine hace el fetch a la vez.
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
            logger.error("[BalanceSvc] ¡No inicializado! Llama balance_svc.init() primero.")
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
        """GET a Bitget. Devuelve None si la respuesta no es JSON válida."""
        url = "https://api.bitget.com" + path + qs
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers=self._headers("GET", path + qs),
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT),
                ) as r:
                    # Manejo de rate limit 429
                    if r.status == 429:
                        retry_after = r.headers.get("Retry-After", "5")
                        logger.warning(f"[BalanceSvc] 429, esperando {retry_after}s...")
                        await asyncio.sleep(float(retry_after))
                        return await self._get_json(path, qs)
                    text = await r.text()
            stripped = text.strip()
            if not stripped.startswith("{") and not stripped.startswith("["):
                logger.debug(f"[BalanceSvc] {path} → no-JSON: {stripped[:120]}")
                return None
            return _json.loads(stripped)
        except Exception as e:
            logger.debug(f"[BalanceSvc] {path} excepción: {e}")
            return None

    async def _fetch_with_retry(self, max_retries: int = 3) -> float | None:
        """Reintenta la obtención del balance con backoff exponencial."""
        for attempt in range(1, max_retries + 1):
            result = await self._fetch()
            if result is not None:
                return result
            wait = 2 ** attempt
            logger.warning(f"[BalanceSvc] Intento {attempt}/{max_retries} falló, esperando {wait}s")
            await asyncio.sleep(wait)
        return None

    async def _fetch(self) -> float | None:
        """
        Obtiene el balance USDT desde Unified Account.
        Endpoint oficial: GET /api/v2/unified/account/assets?coin=USDT
        """
        # Endpoint para Unified Account
        path = "/api/v2/unified/account/assets"
        qs = "?coin=USDT"
        data = await self._get_json(path, qs)
        if data and data.get("code") == "00000":
            items = data.get("data", [])
            # Puede ser una lista de assets o un dict directamente
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if not isinstance(item, dict):
                    continue
                coin = item.get("coin") or item.get("currency") or ""
                if coin.upper() == "USDT":
                    # Probar diferentes nombres de campo
                    available = item.get("available") or item.get("free") or item.get("availableBalance")
                    if available is not None:
                        return float(available)
        else:
            code = data.get("code") if data else "?"
            msg = data.get("msg") if data else "No response"
            logger.warning(f"[BalanceSvc] unified/assets code={code} msg={msg}")

        # Fallback: all-account-balance (a veces funciona en UA)
        path2 = "/api/v2/account/all-account-balance"
        qs2 = "?coin=USDT"
        data2 = await self._get_json(path2, qs2)
        if data2 and data2.get("code") == "00000":
            items = data2.get("data", [])
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if not isinstance(item, dict):
                    continue
                coin = item.get("coin") or item.get("currency") or ""
                if coin.upper() == "USDT":
                    available = item.get("available") or item.get("free") or item.get("availableBalance")
                    if available is not None:
                        return float(available)
        else:
            code = data2.get("code") if data2 else "?"
            msg = data2.get("msg") if data2 else "No response"
            logger.warning(f"[BalanceSvc] all-account-balance code={code} msg={msg}")

        logger.error("[BalanceSvc] 🚨 Todos los endpoints fallaron para obtener balance")
        return None


# ── Singleton global ─────────────────────────────────────────────────────────
balance_svc = BalanceService()