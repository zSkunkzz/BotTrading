"""Singleton de balance USDT para futuros Bitget (UA)."""
import asyncio
import logging
import time
import base64
import hmac
import hashlib
import json as _json
import aiohttp

logger = logging.getLogger("BalanceSvc")

_CACHE_TTL   = 30   # segundos entre refreshes
_ENDPOINTS   = [
    ("/api/v2/account/balance",             {"accountType": "futures"}),
    ("/api/v2/mix/account/accounts",        {"productType": "USDT-FUTURES"}),
    ("/api/v2/mix/account/account",         {"productType": "USDT-FUTURES", "marginCoin": "USDT"}),
    ("/api/v2/user/virtual-subaccount-list",{}),
    ("/api/v2/account/info",                {}),
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

    # ── Comprobación de estado ────────────────────────────────────────────────

    def is_ready(self) -> bool:
        """True si ya se han cargado las credenciales."""
        return self._ready

    # ── Init / invalidate ─────────────────────────────────────────────────────

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

    def _extract_usdt_balance(self, data: dict | list) -> float | None:
        """Busca el balance USDT disponible en la respuesta de cualquier endpoint."""
        if isinstance(data, list):
            for item in data:
                val = self._extract_usdt_balance(item)
                if val is not None:
                    return val
            return None

        if not isinstance(data, dict):
            return None

        # Nodo con campo 'coin' o 'marginCoin'
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

        # Recurrir en sub-listas
        for key in ("data", "list", "assets", "balances"):
            sub = data.get(key)
            if sub:
                val = self._extract_usdt_balance(sub)
                if val is not None:
                    return val

        return None

    async def _fetch_balance_once(self) -> float | None:
        """Intenta obtener el balance probando todos los endpoints."""
        for path, params in _ENDPOINTS:
            r = await self._get(path, params)
            if not r or r.get("code") != "00000":
                continue
            val = self._extract_usdt_balance(r)
            if val is not None:
                logger.debug(f"[BalanceSvc] Balance={val:.2f} USDT via {path}")
                return val
        return None

    # ── API pública ───────────────────────────────────────────────────────────

    async def get(self) -> float | None:
        """Devuelve el balance cacheado o lo refresca si ha caducado."""
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
            else:
                logger.warning("[BalanceSvc] ⚠️ Todos los endpoints fallaron")
            return self._cache  # puede ser None si nunca tuvo éxito


balance_svc = _BalanceService()
