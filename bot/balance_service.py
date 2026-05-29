"""
bot/balance_service.py - Servicio de balance para Unified Account de Bitget.

Usa HTTP directo en lugar de ccxt.fetch_balance(), que llama al endpoint
Classic Account y falla con code 40085 en cuentas UA.

Endpoints probados (en orden):
  1. /api/mix/v1/account/accounts?productType=umcbl   → futuros USDT-M (V1, UA-compatible)
  2. /api/mix/v1/account/accounts?productType=dmcbl   → futuros COIN-M (V1, UA-compatible)
  3. /api/spot/v1/account/assets                      → spot (V1, UA-compatible)
  4. /api/spot/v1/account/assets-lite                 → spot lite (V1, UA-compatible)
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

_TTL = int(os.getenv("BALANCE_CACHE_TTL", "60"))  # segundos entre fetches reales


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class BalanceService:
    def __init__(self):
        self._api_key: str | None = None
        self._api_secret: str | None = None
        self._passphrase: str | None = None
        self._value: float | None = None
        self._ts: float = 0.0
        self._lock: asyncio.Lock | None = None
        self._ready: bool = False

    # ------------------------------------------------------------------
    def init(self, api_key: str, api_secret: str, passphrase: str):
        if self._ready:
            return
        self._api_key    = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._ready = True
        logger.info("[BalanceSvc] ✅ Inicializado (HTTP directo V1, UA compatible)")

    # ------------------------------------------------------------------
    def _sign(self, ts: str, method: str, path_with_qs: str, body: str = "") -> str:
        msg = ts + method.upper() + path_with_qs + body
        return base64.b64encode(
            hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _headers(self, method: str, path_with_qs: str, body: str = "") -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY":        self._api_key,
            "ACCESS-SIGN":       self._sign(ts, method, path_with_qs, body),
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }

    async def _http_get(self, path: str, params: dict | None = None) -> dict:
        qs = ""
        if params:
            qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        url = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=self._headers("GET", path + qs),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                text = await r.text()
                stripped = text.strip()
                if not stripped.startswith("{"):
                    raise ValueError(f"Respuesta no-JSON ({path}): {stripped[:200]}")
                return _json.loads(stripped)

    # ------------------------------------------------------------------
    def _extract_usdt(self, data) -> float | None:
        """
        Extrae el mayor balance USDT disponible de la respuesta.
        Soporta lista plana, lista de cuentas con 'list' anidado y dict directo.
        """
        keys = (
            "available", "availableBalance", "crossMaxAvailable",
            "equity", "usdtEquity", "isolatedMaxAvailable",
            "crossedMaxAvailable", "walletBalance", "balance", "free",
        )

        def _from_dict(d: dict) -> float | None:
            coin = (d.get("marginCoin") or d.get("coin") or d.get("asset") or "").upper()
            if coin == "USDT":
                for k in keys:
                    v = _to_float(d.get(k))
                    if v is not None and v >= 0:
                        return v
            return None

        def _scan_list(lst: list) -> float | None:
            best = None
            for item in lst:
                if not isinstance(item, dict):
                    continue
                v = _from_dict(item)
                if v is not None:
                    best = v if best is None else max(best, v)
                # UA anidado: {accountType: ..., list/assets/data: [{coin: USDT, ...}]}
                for nested_key in ("list", "assets", "data"):
                    nested = item.get(nested_key)
                    if isinstance(nested, list):
                        for sub in nested:
                            if isinstance(sub, dict):
                                v2 = _from_dict(sub)
                                if v2 is not None:
                                    best = v2 if best is None else max(best, v2)
            return best

        if isinstance(data, list):
            val = _scan_list(data)
            if val is not None:
                return val

        if isinstance(data, dict):
            for k in keys:
                v = _to_float(data.get(k))
                if v is not None and v >= 0:
                    return v
            for nested_key in ("list", "assets", "data"):
                nested = data.get(nested_key)
                if isinstance(nested, list):
                    val = _scan_list(nested)
                    if val is not None:
                        return val
        return None

    async def _fetch_balance(self) -> float | None:
        """
        Prueba endpoints V1 en orden, todos compatibles con Unified Account.
        Los endpoints V2 de mix/account y spot/account/assets son Classic Account
        y fallan con 40085 en cuentas UA.
        """
        endpoints = [
            # Futuros USDT-M (V1) — endpoint principal para cuentas con futuros
            ("/api/mix/v1/account/accounts", {"productType": "umcbl"}),
            # Futuros COIN-M (V1) — fallback por si el USDT está aquí
            ("/api/mix/v1/account/accounts", {"productType": "dmcbl"}),
            # Spot (V1) — balance en spot
            ("/api/spot/v1/account/assets", None),
            # Spot lite (V1) — versión reducida, más rápida
            ("/api/spot/v1/account/assets-lite", None),
        ]
        for path, params in endpoints:
            try:
                r = await self._http_get(path, params)
                code = r.get("code")
                if code == "00000":
                    val = self._extract_usdt(r.get("data"))
                    if val is not None:
                        logger.info(f"[BalanceSvc] ✅ Balance obtenido via {path}: {val:.2f} USDT")
                        return val
                    logger.info(
                        f"[BalanceSvc] ⚠️ {path} code=00000 pero sin USDT extraíble. "
                        f"data={str(r.get('data', ''))[:300]}"
                    )
                else:
                    logger.info(f"[BalanceSvc] ⚠️ {path} code={code} msg={r.get('msg')}")
            except Exception as e:
                logger.info(f"[BalanceSvc] ⚠️ {path} excepción: {e}")
        return None

    # ------------------------------------------------------------------
    async def get(self) -> float | None:
        if not self._ready:
            return None
        if self._value is not None and time.monotonic() - self._ts < _TTL:
            return self._value
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._value is not None and time.monotonic() - self._ts < _TTL:
                return self._value
            fresh = await self._fetch_balance()
            if fresh is not None:
                self._value = fresh
                self._ts = time.monotonic()
                logger.info(f"[BalanceSvc] ✅ Balance actualizado: {fresh:.2f} USDT")
            elif self._value is not None:
                logger.warning(f"[BalanceSvc] ⚠️ Usando caché: {self._value:.2f} USDT")
            else:
                logger.error("[BalanceSvc] ❌ No se pudo obtener balance")
            return self._value

    def invalidate(self):
        """Fuerza un re-fetch en la próxima llamada a get()."""
        self._ts = 0.0


balance_svc = BalanceService()
