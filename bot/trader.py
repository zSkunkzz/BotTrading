import asyncio
import base64
import logging
import os
import hmac
import hashlib
import time
import json as _json
import aiohttp
import ccxt.async_support as ccxt
from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close
from bot.state import (
    save_position, load_position, clear_position, mark_tp2_hit
)
from bot.telegram_bot import notify_tp_partial

logger = logging.getLogger("Trader")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

OHLCV_TF        = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT     = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS  = int(os.getenv("OHLCV_MIN_BARS", "55"))

# Máx reintentos de balance antes de saltarse el ciclo
_BALANCE_MAX_RETRIES = int(os.getenv("BALANCE_MAX_RETRIES", "5"))
_BALANCE_RETRY_SLEEP = float(os.getenv("BALANCE_RETRY_SLEEP", "3"))

_MIN_QTY_FALLBACK = {
    "BTCUSDT":   0.001,
    "ETHUSDT":   0.01,
    "SOLUSDT":   0.1,
    "XRPUSDT":   1.0,
    "SUIUSDT":   1.0,
    "NEARUSDT":  0.1,
    "XLMUSDT":   1.0,
    "XAUUSDT":   0.01,
    "XAUTUSDT":  0.001,
    "XAGUSDT":   0.1,
    "HYPEUSDT":  0.1,
    "FILOUSDT":  0.1,
    "FILUSDT":   0.1,
    "SOXLUSDT":  0.1,
    "ZECUSDT":   0.01,
    "WLDUSDT":   0.1,
    "BEATUSDT":  1.0,
    "BZUSDT":    1.0,
    "TAOUSDT":   0.001,
    "ADAUSDT":   1.0,
    "DOGEUSDTUSDT": 1.0,
    "BCHUSDT":   0.01,
    "DOGEUSDT":  1.0,
}

_min_qty_cache: dict = {}

_BALANCE_CACHE_TTL  = int(os.getenv("BALANCE_CACHE_TTL", "30"))
_balance_cache_value: float | None = None
_balance_cache_ts:    float = 0.0
_balance_fetch_lock:  asyncio.Lock = None


def _get_balance_lock() -> asyncio.Lock:
    global _balance_fetch_lock
    if _balance_fetch_lock is None:
        _balance_fetch_lock = asyncio.Lock()
    return _balance_fetch_lock


async def _safe_json(response) -> dict:
    data = await response.json(content_type=None)
    if not isinstance(data, dict):
        raise ValueError(f"Respuesta no-JSON (tipo {type(data).__name__}): {str(data)[:300]}")
    return data


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _extract_usdt_balance(item: dict) -> float | None:
    """
    Extrae balance USDT disponible de un dict de cuenta/asset.
    Itera todos los campos candidatos antes de rendirse.
    """
    if not isinstance(item, dict):
        return None

    candidates = [
        "available",
        "crossMaxAvailable",
        "usdtEquity",
        "isolatedMaxAvailable",
        "equity",
        "availableBalance",
        "availableMargin",
        "accountEquity",
    ]

    # Primera pasada: buscar cualquier valor > 0
    for field in candidates:
        v = _to_float(item.get(field))
        if v is not None and v > 0:
            logger.debug(f"[BalanceCache] campo={field} val={v}")
            return v

    # Segunda pasada: si todos son 0, retornar el primero que exista (puede ser 0.0 real)
    for field in candidates:
        v = _to_float(item.get(field))
        if v is not None:
            logger.debug(f"[BalanceCache] campo={field} val={v} (fallback-0)")
            return v

    return None


async def _fetch_balance_once(api_key, api_secret, passphrase) -> float | None:
    """
    Intenta obtener el balance USDT usando 4 endpoints en orden.

    FIX: Endpoints v3/* eliminados — no existen en Bitget Unified Account.
    Orden correcto para UA:
      1. v2/account/all-account-balance  → balance unificado UA (principal)
      2. v2/mix/account/accounts         → cuentas de futuros (lista)
      3. v2/mix/account/account          → cuenta futuros de un símbolo concreto
      4. v2/spot/account/assets          → activos spot (fallback)
    """
    global _balance_cache_value, _balance_cache_ts

    def _sign(ts, method, path_with_qs, body=""):
        msg = ts + method.upper() + path_with_qs + body
        return base64.b64encode(
            hmac.new(api_secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _headers(method, path_with_qs, body=""):
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY":        api_key,
            "ACCESS-SIGN":       _sign(ts, method, path_with_qs, body),
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }

    def _cache_and_return(val: float, source: str) -> float:
        global _balance_cache_value, _balance_cache_ts
        _balance_cache_value = val
        _balance_cache_ts    = time.monotonic()
        logger.info(f"[BalanceCache] ✅ Balance USDT ({source}): {val:.2f}")
        return val

    # ENDPOINT 1: v2/account/all-account-balance (UA — balance unificado)
    try:
        path = "/api/v2/account/all-account-balance"
        qs   = "?coin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        raw_data = data.get("data")
        if data.get("code") == "00000":
            if isinstance(raw_data, dict):
                items = [raw_data]
            elif isinstance(raw_data, list):
                items = raw_data
            else:
                items = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                coin = item.get("coin") or item.get("currency") or ""
                if coin.upper() == "USDT" or not coin:
                    bal = _extract_usdt_balance(item)
                    if bal is not None:
                        return _cache_and_return(bal, "v2/all-account-balance")
        else:
            logger.warning(
                f"[BalanceCache] ⚠️ v2/all-account-balance code={data.get('code')} "
                f"msg={data.get('msg')}"
            )
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2/all-account-balance respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2/all-account-balance excepción: {e}")

    # ENDPOINT 2: v2/mix/account/accounts (lista de cuentas futuros USDT)
    try:
        path = "/api/v2/mix/account/accounts"
        qs   = "?productType=USDT-FUTURES"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        if data.get("code") == "00000":
            items = data.get("data") or []
            if isinstance(items, list) and items:
                bal = _extract_usdt_balance(items[0])
                if bal is not None:
                    return _cache_and_return(bal, "v2/mix-accounts")
        else:
            code = data.get("code")
            if code not in ("40085", "40001"):
                logger.warning(
                    f"[BalanceCache] ⚠️ v2/mix-accounts code={code} msg={data.get('msg')}"
                )
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2/mix-accounts respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2/mix-accounts excepción: {e}")

    # ENDPOINT 3: v2/mix/account/account (cuenta futuros símbolo concreto)
    try:
        path = "/api/v2/mix/account/account"
        qs   = "?symbol=BTCUSDT&productType=USDT-FUTURES&marginCoin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        if data.get("code") == "00000":
            d = data.get("data") or {}
            if isinstance(d, dict):
                bal = _extract_usdt_balance(d)
                if bal is not None:
                    return _cache_and_return(bal, "v2/mix-account-btc")
        else:
            code = data.get("code")
            if code not in ("40085", "40001"):
                logger.warning(
                    f"[BalanceCache] ⚠️ v2/mix-account code={code} msg={data.get('msg')}"
                )
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2/mix-account respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2/mix-account excepción: {e}")

    # ENDPOINT 4: v2/spot/account/assets (activos spot — fallback)
    try:
        path = "/api/v2/spot/account/assets"
        qs   = "?coin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        raw_data = data.get("data")
        if data.get("code") == "00000":
            if isinstance(raw_data, dict):
                items = [raw_data]
            elif isinstance(raw_data, list):
                items = raw_data
            else:
                items = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                coin = item.get("coin") or item.get("currency") or ""
                if coin.upper() == "USDT" or len(items) == 1:
                    bal = _extract_usdt_balance(item)
                    if bal is not None:
                        return _cache_and_return(bal, "v2/spot-assets")
        else:
            code = data.get("code")
            if code not in ("40085", "40001"):
                logger.warning(
                    f"[BalanceCache] ⚠️ v2/spot-assets code={code} msg={data.get('msg')}"
                )
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2/spot-assets respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2/spot-assets excepción: {e}")

    logger.error(
        f"[BalanceCache] 🚨 Los 4 endpoints fallaron — balance NO actualizado. "
        f"Caché actual: {_balance_cache_value}. Se reintentará en el próximo ciclo."
    )
    return None


async def get_cached_balance(api_key, api_secret, passphrase) -> float | None:
    global _balance_cache_value, _balance_cache_ts
    lock = _get_balance_lock()
    now  = time.monotonic()

    if _balance_cache_value is not None and now - _balance_cache_ts < _BALANCE_CACHE_TTL:
        return _balance_cache_value

    async with lock:
        now = time.monotonic()
        if _balance_cache_value is not None and now - _balance_cache_ts < _BALANCE_CACHE_TTL:
            return _balance_cache_value
        result = await _fetch_balance_once(api_key, api_secret, passphrase)
        if result is None and _balance_cache_value is not None:
            logger.warning(
                f"[BalanceCache] ⚠️ API falló, usando caché anterior: {_balance_cache_value:.2f} USDT"
            )
            return _balance_cache_value
        return result


async def _wait_for_balance(
    api_key: str,
    api_secret: str,
    passphrase: str,
    symbol: str,
    max_retries: int = _BALANCE_MAX_RETRIES,
    sleep_s: float   = _BALANCE_RETRY_SLEEP,
) -> float | None:
    """
    Reintenta obtener el balance hasta max_retries veces con backoff lineal.
    Invalida la caché en cada intento para forzar un fetch real.
    """
    global _balance_cache_value, _balance_cache_ts

    for attempt in range(1, max_retries + 1):
        _balance_cache_value = None
        _balance_cache_ts    = 0.0

        bal = await get_cached_balance(api_key, api_secret, passphrase)
        if bal is not None and bal > 0:
            logger.info(f"[{symbol}] Balance OK en intento {attempt}: {bal:.2f} USDT")
            return bal

        wait = sleep_s * attempt
        logger.warning(
            f"[{symbol}] ⚠️ Balance={bal} (intento {attempt}/{max_retries}), "
            f"reintentando en {wait:.0f}s..."
        )
        await asyncio.sleep(wait)

    logger.error(
        f"[{symbol}] 🚨 Balance sigue siendo 0 o None tras {max_retries} intentos. "
        f"Saltando ciclo de trading hasta próxima vuelta."
    )
    return None


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol       = symbol
        self.leverage     = leverage
        self.margin_mode  = margin_mode or "isolated"
        self.dry_run      = dry_run
        self._api_key     = api_key
        self._api_secret  = api_secret
        self._passphrase  = passphrase
        self.position     = None
        self.entry_price  = None
        self.sl           = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit      = False
        self.trade_count  = 0
        self.win_count    = 0
        self.total_pnl    = 0.0
        self.exchange     = None
        self._api_version = None
        self._ua_pos_mode = None
        self._v2_pos_mode = None
        self._balance_ok  = False

    # ── HTTP HELPERS ────────────────────────────────────────────────────────────

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
                return await _safe_json(r)

    async def _http_post(self, path: str, payload: dict) -> dict:
        body = _json.dumps(payload)
        url  = "https://api.bitget.com" + path
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                headers=self._headers("POST", path, body),
                data=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                return await _safe_json(r)

    # ── INICIALIZACIÓN ────────────────────────────────────────────────────────

    async def _init(self, usdt_per_trade: float):
        self.exchange = ccxt.bitget({
            "apiKey":     self._api_key,
            "secret":     self._api_secret,
            "password":   self._passphrase,
            "options":    {"defaultType": "swap"},
        })
        saved = load_position(self.symbol)
        if saved:
            self.position    = saved["side"]
            self.entry_price = saved["entry"]
            self.sl          = saved.get("sl")
            self.tp1         = saved.get("tp1")
            self.tp2         = saved.get("tp2")
            self.tp3         = saved.get("tp3")
            self.tp2_hit     = saved.get("tp2_hit", False)
            logger.info(f"[{self.symbol}] 🔄 Posición restaurada: {self.position} @ {self.entry_price}")
        await self._detect_account_type()

    async def _detect_account_type(self):
        # Probe UA usando v2/account/all-account-balance (endpoint que sí existe)
        try:
            r = await self._http_get(
                "/api/v2/account/all-account-balance",
                {"coin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                # Intentar detectar pos_mode desde posiciones abiertas UA
                try:
                    rp = await self._http_get(
                        "/api/v2/mix/position/all-position",
                        {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
                    )
                    if rp.get("code") == "00000":
                        items = rp.get("data") or []
                        if items and isinstance(items, list):
                            self._ua_pos_mode = items[0].get("holdMode", "hedge")
                        else:
                            self._ua_pos_mode = "hedge"
                    else:
                        self._ua_pos_mode = "hedge"
                except Exception:
                    self._ua_pos_mode = "hedge"
                logger.info(
                    f"[{self.symbol}] ✅ Unified Account (UA). pos_mode={self._ua_pos_mode}"
                )
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] UA probe error: {e}")

        # Probe Classic v2
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
            r = await self._http_get(
                "/api/v2/mix/account/account",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "v2"
                d = r.get("data") or {}
                d = d if isinstance(d, dict) else {}
                self._v2_pos_mode = d.get("holdMode", "hedge")
                logger.info(f"[{self.symbol}] ✅ Classic Account (v2). pos_mode={self._v2_pos_mode}")
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] v2 probe error: {e}")

        logger.warning(f"[{self.symbol}] ⚠️ No se detectó tipo de cuenta, asumiendo UA.")
        self._api_version = "ua"
        self._ua_pos_mode = "hedge"

    # ── PRECIO, OHLCV Y BALANCE ────────────────────────────────────────────────

    async def get_price(self) -> float:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.is_price_fresh(sym_clean):
                price = ws_feed.get_price(sym_clean)
                if price and price > 0:
                    return price
        except Exception:
            pass
        ticker = await self.exchange.fetch_ticker(self.symbol)
        return float(ticker["last"])

    async def get_ohlcv(self, tf: str = OHLCV_TF) -> list:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.has_data(sym_clean, tf=tf, min_candles=OHLCV_MIN_BARS):
                df = ws_feed.get_ohlcv(sym_clean, tf)
                if not df.empty and len(df) >= OHLCV_MIN_BARS:
                    df_reset = df.reset_index()
                    bars = [
                        [
                            int(row["ts"].timestamp() * 1000),
                            float(row["open"]),
                            float(row["high"]),
                            float(row["low"]),
                            float(row["close"]),
                            float(row["volume"]),
                        ]
                        for _, row in df_reset.iterrows()
                    ]
                    logger.debug(f"[{self.symbol}] OHLCV desde WS ({len(bars)} velas)")
                    return bars
        except Exception as e:
            logger.debug(f"[{self.symbol}] get_ohlcv WS error: {e}")

        tf_ccxt = {"15m": "15m", "1h": "1h", "4h": "4h"}.get(tf, tf)
        logger.debug(f"[{self.symbol}] OHLCV fallback REST ({tf_ccxt})")
        bars = await self.exchange.fetch_ohlcv(self.symbol, tf_ccxt, limit=OHLCV_LIMIT)
        return bars

    async def get_balance(self) -> float | None:
        return await get_cached_balance(self._api_key, self._api_secret, self._passphrase)

    # ── LEVERAGE ─────────────────────────────────────────────────────────────

    async def set_leverage(self, leverage: int, side: str | None = None):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # UA siempre usa v2 para leverage (v3 endpoints eliminados)
        endpoint = "/api/v2/mix/account/set-leverage"
        pos_mode = self._ua_pos_mode or self._v2_pos_mode or "hedge"

        sides = ["long", "short"] if pos_mode == "hedge" else [side or "long"]

        for hold_side in sides:
            try:
                payload = {
                    "symbol":      sym_clean,
                    "productType": "USDT-FUTURES",
                    "marginCoin":  "USDT",
                    "leverage":    str(leverage),
                    "holdSide":    hold_side,
                }
                r = await self._http_post(endpoint, payload)
                if r.get("code") == "00000":
                    logger.debug(f"[{self.symbol}] Leverage {leverage}x ({hold_side}) OK")
                else:
                    logger.warning(
                        f"[{self.symbol}] set_leverage {hold_side} "
                        f"code={r.get('code')} msg={r.get('msg')}"
                    )
            except Exception as e:
                logger.warning(f"[{self.symbol}] set_leverage error: {e}")

    # ── MÍNIMOS DE QTY ─────────────────────────────────────────────────────────

    async def _get_min_qty(self) -> float:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean in _min_qty_cache:
            return _min_qty_cache[sym_clean]
        try:
            r = await self._http_get(
                "/api/v2/mix/market/contracts",
                {"symbol": sym_clean, "productType": "USDT-FUTURES"}
            )
            if r.get("code") == "00000":
                items = r.get("data") or []
                items = items if isinstance(items, list) else []
                if items:
                    min_qty = float(
                        items[0].get("minTradeNum") or
                        items[0].get("minOrderSize") or 0.001
                    )
                    _min_qty_cache[sym_clean] = min_qty
                    return min_qty
        except Exception as e:
            logger.debug(f"[{self.symbol}] _get_min_qty error: {e}")
        fallback = _MIN_QTY_FALLBACK.get(sym_clean, 0.001)
        _min_qty_cache[sym_clean] = fallback
        return fallback

    # ── POSICIONES ABIERTAS ────────────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # UA y Classic v2 usan el mismo endpoint v2
        try:
            r = await self._http_get(
                "/api/v2/mix/position/single-position",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and float(p.get("total") or p.get("contracts") or
                              p.get("size", 0)) > 0
                ]
            else:
                logger.debug(
                    f"[{self.symbol}] positions: code={r.get('code')} msg={r.get('msg')}"
                )
        except Exception as e:
            logger.debug(f"[{self.symbol}] positions error: {e}")

        # Fallback: all-position
        try:
            r = await self._http_get(
                "/api/v2/mix/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and p.get("symbol") == sym_clean
                    and float(p.get("total") or p.get("contracts") or
                              p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug(f"[{self.symbol}] all-positions error: {e}")

        logger.warning(
            f"[{self.symbol}] ⚠️ _get_positions falló — estado local preservado"
        )
        return None

    # ── COLOCAR / CERRAR ÓRDENES ──────────────────────────────────────────────────

    async def _place_order(self, side: str, trade_side: str, qty: float):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # UA y Classic v2 usan el mismo endpoint v2 para órdenes
        endpoint = "/api/v2/mix/order/place-order"
        pos_mode = self._ua_pos_mode or self._v2_pos_mode or "hedge"

        def _build_payload(mode: str) -> dict:
            p = {
                "symbol":      sym_clean,
                "productType": "USDT-FUTURES",
                "marginMode":  self.margin_mode,
                "marginCoin":  "USDT",
                "qty":         str(qty),
                "orderType":   "market",
                "side":        side,
            }
            if mode == "hedge":
                p["tradeSide"] = trade_side
            return p

        if self.dry_run:
            logger.info(f"[{self.symbol}] 🟡 DRY RUN: {side}/{trade_side} qty={qty}")
            return {"code": "00000", "data": {"orderId": "dry"}}

        payload = _build_payload(pos_mode)
        try:
            r = await self._http_post(endpoint, payload)
            if r.get("code") == "00000":
                return r
            if pos_mode == "hedge" and r.get("code") in ("40786", "40787", "40788"):
                logger.warning(
                    f"[{self.symbol}] Hedge order failed ({r.get('code')}), retrying one-way"
                )
                r2 = await self._http_post(endpoint, _build_payload("one_way"))
                if r2.get("code") == "00000":
                    return r2
            logger.error(
                f"[{self.symbol}] Order failed: code={r.get('code')} msg={r.get('msg')}"
            )
            return r
        except Exception as e:
            logger.error(f"[{self.symbol}] _place_order exception: {e}")
            return {"code": "ERROR", "msg": str(e)}

    async def _calc_qty(self, usdt_amount: float, price: float, leverage: int) -> float:
        effective_lev = leverage or self.leverage
        raw_qty = (usdt_amount * effective_lev) / price
        min_qty = await self._get_min_qty()
        qty = max(min_qty, round(raw_qty / min_qty) * min_qty)
        decimals = len(str(min_qty).rstrip("0").split(".")[-1]) if "." in str(min_qty) else 0
        qty = round(qty, decimals)
        return qty

    # ── ABRIR POSICIONES ────────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                        leverage=None):
        price = await self.get_price()
        lev   = leverage or self.leverage
        qty   = await self._calc_qty(usdt_amount, price, lev)
        await self.set_leverage(lev, side="long")
        r = await self._place_order("buy", "open", qty)
        if r.get("code") == "00000":
            self.position    = "long"
            self.entry_price = price
            self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
            self.tp2_hit = False
            save_position(self.symbol, "long", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(
                f"🟢 [{self.symbol}] LONG abierto @ {price:.4f} | "
                f"lev={lev}x | sl={sl} tp1={tp1} tp2={tp2} tp3={tp3}"
            )
            await notify_open(
                self.symbol, "long", price, lev,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run
            )
        else:
            logger.error(
                f"[{self.symbol}] open_long FAILED: code={r.get('code')} msg={r.get('msg')}"
            )

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                         leverage=None):
        price = await self.get_price()
        lev   = leverage or self.leverage
        qty   = await self._calc_qty(usdt_amount, price, lev)
        await self.set_leverage(lev, side="short")
        r = await self._place_order("sell", "open", qty)
        if r.get("code") == "00000":
            self.position    = "short"
            self.entry_price = price
            self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
            self.tp2_hit = False
            save_position(self.symbol, "short", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(
                f"🔴 [{self.symbol}] SHORT abierto @ {price:.4f} | "
                f"lev={lev}x | sl={sl} tp1={tp1} tp2={tp2} tp3={tp3}"
            )
            await notify_open(
                self.symbol, "short", price, lev,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run
            )
        else:
            logger.error(
                f"[{self.symbol}] open_short FAILED: code={r.get('code')} msg={r.get('msg')}"
            )

    async def close_position(self, reason: str = ""):
        if not self.position:
            return
        side       = "sell" if self.position == "long" else "buy"
        trade_side = "close"
        qty = None
        try:
            positions = await self._get_positions()
            if positions:
                qty = float(
                    positions[0].get("total") or
                    positions[0].get("contracts") or
                    positions[0].get("size") or 0
                )
        except Exception:
            pass

        if not qty or qty <= 0:
            logger.warning(f"[{self.symbol}] close_position: qty no disponible, usando 0")
            qty = 0

        exit_price = await self.get_price()
        pnl = 0.0
        if self.entry_price and exit_price:
            if self.position == "long":
                pnl = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                pnl = (self.entry_price - exit_price) / self.entry_price * 100

        if qty > 0:
            r = await self._place_order(side, trade_side, qty)
            if r.get("code") != "00000":
                logger.error(
                    f"[{self.symbol}] close_position FAILED: code={r.get('code')} msg={r.get('msg')}"
                )
                return

        old_pos = self.position
        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

        if pnl >= 0:
            self.win_count += 1
        self.trade_count += 1
        self.total_pnl   += pnl

        logger.warning(
            f"[{self.symbol}] 🟡 {old_pos.upper()} cerrado | razón={reason} | "
            f"pnl={pnl:+.2f}% | trades={self.trade_count} wins={self.win_count}"
        )
        await notify_close(
            self.symbol, old_pos, exit_price, pnl,
            reason=reason, dry_run=self.dry_run
        )

    async def partial_close(self, ratio: float = 0.5):
        if not self.position:
            return
        side       = "sell" if self.position == "long" else "buy"
        trade_side = "close"
        qty = None
        try:
            positions = await self._get_positions()
            if positions:
                total = float(
                    positions[0].get("total") or
                    positions[0].get("contracts") or
                    positions[0].get("size") or 0
                )
                min_qty = await self._get_min_qty()
                qty = max(min_qty, round((total * ratio) / min_qty) * min_qty)
        except Exception as e:
            logger.warning(f"[{self.symbol}] partial_close: {e}")
            return

        if not qty or qty <= 0:
            return

        r = await self._place_order(side, trade_side, qty)
        if r.get("code") == "00000":
            mark_tp2_hit(self.symbol)
            self.tp2_hit = True
            exit_price = await self.get_price()
            await notify_tp_partial(
                self.symbol, self.position, exit_price,
                ratio=ratio, dry_run=self.dry_run
            )
            logger.info(f"[{self.symbol}] ✂️ Cierre parcial {int(ratio*100)}% ejecutado")
        else:
            logger.warning(
                f"[{self.symbol}] partial_close FAILED: code={r.get('code')} msg={r.get('msg')}"
            )

    # ── LOOP PRINCIPAL ──────────────────────────────────────────────────────────

    async def run(self, risk: "RiskManager", global_risk: "GlobalRisk" = None):
        from bot.risk import RiskManager
        usdt_per_trade = risk.usdt_per_trade
        await self._init(usdt_per_trade)

        while True:
            try:
                price = await self.get_price()

                # ── Balance con retry ───────────────────────────────────────────
                balance = await self.get_balance()
                if (balance is None or balance <= 0) and not self._balance_ok:
                    balance = await _wait_for_balance(
                        self._api_key, self._api_secret, self._passphrase,
                        symbol=self.symbol,
                    )

                if balance is None or balance <= 0:
                    logger.warning(
                        f"[{self.symbol}] ⚠️ Balance {balance or 0:.2f} USDT — "
                        f"esperando {_BALANCE_RETRY_SLEEP * 2:.0f}s"
                    )
                    await asyncio.sleep(_BALANCE_RETRY_SLEEP * 2)
                    continue

                if not self._balance_ok:
                    self._balance_ok = True
                    logger.info(f"[{self.symbol}] ✅ Balance confirmado: {balance:.2f} USDT")

                # ── Gestión de posición abierta ──────────────────────────────
                if self.position:
                    if not self.tp2_hit and self.tp2:
                        if (self.position == "long"  and price >= self.tp2) or \
                           (self.position == "short" and price <= self.tp2):
                            await self.partial_close(ratio=TP2_PARTIAL_RATIO)

                    if self.sl and self.tp3:
                        hit_sl  = (self.position == "long"  and price <= self.sl) or \
                                  (self.position == "short" and price >= self.sl)
                        hit_tp3 = (self.position == "long"  and price >= self.tp3) or \
                                  (self.position == "short" and price <= self.tp3)
                        if hit_sl:
                            await self.close_position(reason="SL")
                        elif hit_tp3:
                            await self.close_position(reason="TP3")

                    await asyncio.sleep(2)
                    continue

                # ── Sin posición: buscar señal ───────────────────────────────
                if global_risk and not global_risk.can_open_trade():
                    await asyncio.sleep(2)
                    continue

                bars = await self.get_ohlcv()

                if not bars or len(bars) < OHLCV_MIN_BARS:
                    logger.debug(
                        f"[{self.symbol}] Esperando candles WS "
                        f"({len(bars) if bars else 0}/{OHLCV_MIN_BARS})"
                    )
                    await asyncio.sleep(2)
                    continue

                decision = await ai_decide(
                    symbol=self.symbol,
                    bars=bars,
                    position=self.position,
                    entry_price=self.entry_price,
                    leverage=self.leverage,
                )

                if decision.get("action") in ("LONG", "SHORT", "BUY", "SELL"):
                    action      = decision["action"]
                    usdt_amount = min(usdt_per_trade, balance * 0.95)
                    lev  = decision.get("leverage", self.leverage)
                    sl   = decision.get("sl")
                    tp1  = decision.get("tp1")
                    tp2  = decision.get("tp2")
                    tp3  = decision.get("tp3")

                    if global_risk:
                        global_risk.register_open_trade()

                    if action in ("LONG", "BUY"):
                        await self.open_long(usdt_amount, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)
                    else:
                        await self.open_short(usdt_amount, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)

                    if global_risk:
                        global_risk.register_close_trade()

                elif decision.get("action") == "CLOSE" and self.position:
                    await self.close_position(reason=decision.get("reasoning", "IA-CLOSE"))

            except asyncio.CancelledError:
                logger.info(f"[{self.symbol}] Trader cancelado.")
                break
            except Exception as e:
                logger.error(f"[{self.symbol}] run() error: {e}")

            await asyncio.sleep(2)
