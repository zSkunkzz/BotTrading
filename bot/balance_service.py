"""
bot/balance_service.py - Servicio de balance para Unified Account (UA) de Bitget.

Usa la API V3, que es la API nativa diseñada específicamente para cuentas
Unified Account. Los endpoints V1/V2 de mix/spot fallan con error 40085 en UA.

Endpoint principal:
  GET /api/v3/account/assets

Respuesta (AccountAssetsV3):
  {
    "code": "00000",
    "data": {
      "usdtEquity": "123.45",       <- balance total en USDT (nivel raiz)
      "accountEquity": "123.45",
      "assets": [
        { "coin": "USDT", "available": "100.00", "equity": "123.45", "balance": "123.45", ... },
        ...
      ]
    }
  }

Fallback:
  GET /api/v2/account/all-account-balance  (V2 UA, algunos usuarios)
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
        v = float(val)
        return v if v >= 0 else None
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
        logger.info("[BalanceSvc] ✅ Inicializado (API V3 Unified Account)")

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
        full_path = path + qs
        url = "https://api.bitget.com" + full_path
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=self._headers("GET", full_path),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                text = await r.text()
                stripped = text.strip()
                if not stripped.startswith("{"):
                    raise ValueError(f"Respuesta no-JSON ({path}): {stripped[:200]}")
                return _json.loads(stripped)

    # ------------------------------------------------------------------
    def _extract_usdt_from_v3(self, data: dict) -> float | None:
        """
        Extrae USDT de la respuesta AccountAssetsV3.

        La respuesta V3 tiene:
          data.usdtEquity          <- string, balance total en USDT
          data.assets[]            <- lista de AccountAssetV3
            .coin == "USDT"
            .available             <- disponible
            .equity                <- equity total
            .balance               <- balance bruto
        """
        if not isinstance(data, dict):
            return None

        # 1. Campo de nivel raiz: usdtEquity (el mas directo)
        v = _to_float(data.get("usdtEquity"))
        if v is not None:
            return v

        # 2. Buscar en assets[] el coin USDT
        assets = data.get("assets", [])
        if isinstance(assets, list):
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                if (asset.get("coin") or "").upper() == "USDT":
                    for field in ("available", "equity", "balance"):
                        v = _to_float(asset.get(field))
                        if v is not None:
                            return v
        return None

    def _extract_usdt_from_v2_all_balance(self, data) -> float | None:
        """
        Extrae USDT de la respuesta de /api/v2/account/all-account-balance.
        Devuelve una lista de {accountType, list: [{coin, available, ...}]}.
        """
        if not isinstance(data, list):
            # A veces es un dict con 'list' dentro
            if isinstance(data, dict):
                data = data.get("list") or data.get("data") or []
        best = None
        for account in (data if isinstance(data, list) else []):
            if not isinstance(account, dict):
                continue
            inner = account.get("list") or account.get("assets") or []
            for item in (inner if isinstance(inner, list) else []):
                if not isinstance(item, dict):
                    continue
                if (item.get("coin") or "").upper() == "USDT":
                    for field in ("available", "equity", "balance", "walletBalance"):
                        v = _to_float(item.get(field))
                        if v is not None:
                            best = v if best is None else max(best, v)
        return best

    async def _fetch_balance(self) -> float | None:
        """
        Intenta obtener el balance USDT usando los endpoints UA en orden:
          1. V3 /api/v3/account/assets          <- API nativa UA, la correcta
          2. V2 /api/v2/account/all-account-balance  <- fallback UA V2
        """
        # --- Intento 1: API V3 (UA nativa) ---
        try:
            r = await self._http_get("/api/v3/account/assets")
            code = r.get("code")
            if code == "00000":
                val = self._extract_usdt_from_v3(r.get("data", {}))
                if val is not None:
                    logger.info(f"[BalanceSvc] ✅ V3 /account/assets: {val:.2f} USDT")
                    return val
                logger.warning(
                    f"[BalanceSvc] ⚠️ V3 code=00000 pero sin USDT extraíble. "
                    f"data={str(r.get('data',''))[:400]}"
                )
            else:
                logger.warning(f"[BalanceSvc] ⚠️ V3 code={code} msg={r.get('msg')}")
        except Exception as e:
            logger.warning(f"[BalanceSvc] ⚠️ V3 excepción: {e}")

        # --- Intento 2: API V2 all-account-balance (fallback) ---
        try:
            r = await self._http_get("/api/v2/account/all-account-balance")
            code = r.get("code")
            if code == "00000":
                val = self._extract_usdt_from_v2_all_balance(r.get("data"))
                if val is not None:
                    logger.info(f"[BalanceSvc] ✅ V2 /all-account-balance: {val:.2f} USDT")
                    return val
                logger.warning(
                    f"[BalanceSvc] ⚠️ V2 code=00000 pero sin USDT extraíble. "
                    f"data={str(r.get('data',''))[:400]}"
                )
            else:
                logger.warning(f"[BalanceSvc] ⚠️ V2 fallback code={code} msg={r.get('msg')}")
        except Exception as e:
            logger.warning(f"[BalanceSvc] ⚠️ V2 fallback excepción: {e}")

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
                logger.warning(f"[BalanceSvc] ⚠️ Usando caché anterior: {self._value:.2f} USDT")
            else:
                logger.error("[BalanceSvc] ❌ No se pudo obtener el balance")
            return self._value

    def invalidate(self):
        """Fuerza un re-fetch en la próxima llamada a get()."""
        self._ts = 0.0


balance_svc = BalanceService()
