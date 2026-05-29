"""
bot/balance_service.py - Servicio de balance para Unified Account
Endpoint correcto: /api/v2/unified/account/assets?coin=USDT
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

_TTL = int(os.getenv("BALANCE_CACHE_TTL", "60"))
_TIMEOUT = float(os.getenv("BALANCE_TIMEOUT", "10"))


class BalanceService:
    def __init__(self):
        self._key = None
        self._secret = None
        self._passphrase = None
        self._value = None
        self._ts = 0.0
        self._lock = None
        self._ready = False

    def init(self, api_key, api_secret, passphrase):
        if self._ready:
            return
        self._key = api_key
        self._secret = api_secret
        self._passphrase = passphrase
        self._ready = True
        logger.info("[BalanceSvc] ✅ Inicializado (Unified Account)")

    async def get(self):
        if not self._ready:
            return None
        if self._value is not None and time.monotonic() - self._ts < _TTL:
            return self._value
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._value is not None and time.monotonic() - self._ts < _TTL:
                return self._value
            fresh = await self._fetch_with_retry()
            if fresh is not None:
                self._value = fresh
                self._ts = time.monotonic()
                logger.info(f"[BalanceSvc] ✅ Balance: {fresh:.2f} USDT")
            elif self._value is not None:
                logger.warning(f"[BalanceSvc] ⚠️ Usando caché: {self._value:.2f} USDT")
            else:
                logger.error("[BalanceSvc] ❌ No se pudo obtener balance")
            return self._value

    def invalidate(self):
        self._ts = 0.0

    def _sign(self, ts, method, path_qs, body=""):
        msg = ts + method.upper() + path_qs + body
        return base64.b64encode(
            hmac.new(self._secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _headers(self, method, path_qs, body=""):
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY": self._key,
            "ACCESS-SIGN": self._sign(ts, method, path_qs, body),
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
        }

    async def _get_json(self, path, qs=""):
        url = f"https://api.bitget.com{path}{qs}"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers=self._headers("GET", path + qs),
                    timeout=aiohttp.ClientTimeout(total=_TIMEOUT),
                ) as r:
                    if r.status == 429:
                        retry = r.headers.get("Retry-After", "5")
                        await asyncio.sleep(float(retry))
                        return await self._get_json(path, qs)
                    text = await r.text()
                    if not text or not text.startswith(('{', '[')):
                        logger.warning(f"[BalanceSvc] Respuesta no JSON: {text[:100]}")
                        return None
                    return _json.loads(text)
        except Exception as e:
            logger.warning(f"[BalanceSvc] Error en {path}: {e}")
            return None

    async def _fetch_with_retry(self, max_retries=3):
        for attempt in range(1, max_retries + 1):
            result = await self._fetch()
            if result is not None:
                return result
            wait = 2 ** attempt
            logger.warning(f"[BalanceSvc] Intento {attempt}/{max_retries} falló, esperando {wait}s")
            await asyncio.sleep(wait)
        return None

    async def _fetch(self):
        # Endpoint correcto para Unified Account
        path = "/api/v2/unified/account/assets"
        qs = "?coin=USDT"
        data = await self._get_json(path, qs)
        if data and data.get("code") == "00000":
            items = data.get("data", [])
            if isinstance(items, list):
                for item in items:
                    if item.get("coin") == "USDT":
                        # El campo puede ser "available" o "free"
                        available = item.get("available") or item.get("free")
                        if available is not None:
                            return float(available)
            else:
                logger.warning(f"[BalanceSvc] Formato inesperado: {data}")
        else:
            code = data.get("code") if data else "?"
            msg = data.get("msg") if data else "No response"
            logger.warning(f"[BalanceSvc] unified/assets code={code} msg={msg}")

        # Fallback opcional (si el anterior no funciona)
        path2 = "/api/v2/unified/account/balance"
        data2 = await self._get_json(path2, qs)
        if data2 and data2.get("code") == "00000":
            items = data2.get("data", [])
            if isinstance(items, list):
                for item in items:
                    if item.get("coin") == "USDT":
                        available = item.get("available") or item.get("free")
                        if available is not None:
                            return float(available)
        else:
            code2 = data2.get("code") if data2 else "?"
            logger.warning(f"[BalanceSvc] unified/balance code={code2}")

        logger.error("[BalanceSvc] ❌ No se pudo obtener balance con endpoints UA")
        return None


balance_svc = BalanceService()