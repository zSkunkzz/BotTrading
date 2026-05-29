"""Singleton de balance USDT para futuros Bitget (UA).

Endpoints en orden de prioridad:
  1. /api/v3/account/assets          V3 UA nativa  ← primer intento
  2. /api/v2/account/all-account-balance  V2 UA    ← fallback

El endpoint V2 mix/account/accounts está bloqueado en Unified Account.
"""
import asyncio
import logging
import time
import base64
import hmac
import hashlib
import json as _json
import aiohttp

logger = logging.getLogger("BalanceSvc")

_CACHE_TTL = 30   # segundos entre refreshes

# (path, params, api_version)
_ENDPOINTS = [
    ("/api/v3/account/assets",              {},                          "v3"),
    ("/api/v2/account/all-account-balance", {"accountType": "futures"}, "v2"),
]


class _BalanceService:
    def __init__(self):
        self._key        = None
        self._secret     = None
        self._passphrase = None
        self._cache      = None
        self._ts         = 0.0
        self._lock       = asyncio.Lock()
        self._ready      = False

    def is_ready(self) -> bool:
        return self._ready

    def init(self, key: str, secret: str, passphrase: str):
        """Idempotente: solo actualiza credenciales la primera vez."""
        if self._ready:
            return
        self._key        = key
        self._secret     = secret
        self._passphrase = passphrase
        self._ready      = True
        logger.info("[BalanceSvc] Inicializado")

    def invalidate(self):
        """Fuerza refresco en la próxima llamada a get()."""
        self._ts = 0.0

    # ── HTTP ──────────────────────────────────────────────────────────────────

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

    async def _get(self, path: str, params: dict) -> dict | None:
        qs = "?" + "&".join(f"{k}={v}" for k, v in params.items()) if params else ""
        url = "https://api.bitget.com" + path + qs
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url,
                    headers=self._headers("GET", path + qs),
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    text = await r.text()
                    if not text.strip().startswith("{"):
                        return None
                    return _json.loads(text)
        except Exception as e:
            logger.debug(f"[BalanceSvc] _get {path}: {e}")
            return None

    # ── Extracción de USDT ────────────────────────────────────────────────────

    def _extract_v3(self, data: dict) -> float | None:
        """
        Parsea respuesta de /api/v3/account/assets.
        Estructura: data.usdtEquity  o  data.assets[].coin=="USDT"
        """
        inner = data.get("data")
        if not isinstance(inner, dict):
            return None

        # Campo directo usdtEquity
        ue = inner.get("usdtEquity")
        if ue is not None:
            try:
                v = float(ue)
                if v >= 0:
                    return v
            except (ValueError, TypeError):
                pass

        # Fallback: buscar en assets[]
        assets = inner.get("assets") or []
        for a in assets:
            if isinstance(a, dict) and (a.get("coin") or "").upper() == "USDT":
                for field in ("available", "equity"):
                    v = a.get(field)
                    if v is not None:
                        try:
                            return float(v)
                        except (ValueError, TypeError):
                            pass
        return None

    def _extract_v2(self, data: dict | list) -> float | None:
        """Busca balance USDT en respuesta V2 (estructura variable)."""
        if isinstance(data, list):
            for item in data:
                val = self._extract_v2(item)
                if val is not None:
                    return val
            return None

        if not isinstance(data, dict):
            return None

        coin = (data.get("coin") or data.get("marginCoin") or "").upper()
        if coin == "USDT":
            for field in ("available", "availableAmount", "crossMaxAvailable",
                          "fixedMaxAvailable", "equity", "usdtEquity"):
                v = data.get(field)
                if v is not None:
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        pass

        for key in ("data", "list", "assets", "balances"):
            sub = data.get(key)
            if sub:
                val = self._extract_v2(sub)
                if val is not None:
                    return val

        return None

    async def _fetch_balance_once(self) -> float | None:
        """Intenta obtener balance probando endpoints en orden."""
        for path, params, ver in _ENDPOINTS:
            r = await self._get(path, params)
            if not r or r.get("code") != "00000":
                logger.debug(f"[BalanceSvc] {path} code={r.get('code') if r else 'no-resp'}")
                continue

            val = self._extract_v3(r) if ver == "v3" else self._extract_v2(r)
            if val is not None:
                logger.debug(f"[BalanceSvc] Balance={val:.2f} USDT via {path}")
                return val

        logger.warning("[BalanceSvc] ⚠️ Todos los endpoints fallaron")
        return None

    # ── API pública ───────────────────────────────────────────────────────────

    async def get(self) -> float | None:
        """Devuelve balance cacheado o refresca si ha caducado."""
        if not self._ready:
            logger.warning("[BalanceSvc] get() llamado antes de init()")
            return None

        async with self._lock:
            if time.time() - self._ts < _CACHE_TTL and self._cache is not None:
                return self._cache
            val = await self._fetch_balance_once()
            if val is not None:
                self._cache = val
                self._ts    = time.time()
            return self._cache


balance_svc = _BalanceService()
