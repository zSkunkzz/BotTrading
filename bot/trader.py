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
    """Extrae balance USDT disponible de un dict de cuenta/asset."""
    if not isinstance(item, dict):
        return None
    for field in ("available", "crossMaxAvailable", "usdtEquity",
                  "isolatedMaxAvailable", "equity", "availableBalance",
                  "availableMargin", "accountEquity"):
        v = _to_float(item.get(field))
        if v is not None and v > 0:
            logger.debug(f"[BalanceCache] campo={field} val={v}")
            return v
    # Aunque sea 0, si existe 'available' lo retornamos
    v = _to_float(item.get("available"))
    return v if v is not None else 0.0


async def _fetch_balance_once(api_key, api_secret, passphrase) -> float | None:
    """
    Intenta obtener balance USDT disponible.

    Orden de intentos:
      1. /api/v3/account/assets?coin=USDT          (UA, data puede ser list o dict)
      2. /api/v3/account/assets-detail?coin=USDT   (UA alternativo, siempre dict)
      3. /api/v2/mix/account/account               (Classic / fallback UA)
      4. /api/v2/mix/account/accounts              (Classic multi)

    Retorna float (puede ser 0.0 si cuenta vacía) o None si todo falla.
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

    # ── ENDPOINT 1: v3/account/assets (UA) ─────────────────────────────
    # FIX: data["data"] puede ser dict (un solo asset) o list en UA
    try:
        path = "/api/v3/account/assets"
        qs   = "?coin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        raw_data = data.get("data")
        logger.debug(
            f"[BalanceCache] v3/assets raw: code={data.get('code')} "
            f"data_type={type(raw_data).__name__}"
        )
        if data.get("code") == "00000":
            # Normalizar a lista independientemente de si es dict o list
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
                if coin.upper() == "USDT" or not items:  # si solo hay un item, asumirlo
                    bal = _extract_usdt_balance(item)
                    if bal is not None:
                        return _cache_and_return(bal, "v3/assets")

            # data OK pero sin ítem USDT identificable
            logger.warning(
                f"[BalanceCache] ⚠️ v3/assets OK pero sin ítem USDT "
                f"(data type={type(raw_data).__name__}, items={len(items)})"
            )
        else:
            logger.warning(f"[BalanceCache] ⚠️ v3/assets code={data.get('code')} msg={data.get('msg')}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v3/assets respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v3/assets excepción: {e}")

    # ── ENDPOINT 2: v3/account/assets-detail (UA alternativo) ──────────
    try:
        path = "/api/v3/account/assets-detail"
        qs   = "?coin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        raw_data = data.get("data")
        logger.debug(
            f"[BalanceCache] v3/assets-detail raw: code={data.get('code')} "
            f"data_type={type(raw_data).__name__}"
        )
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
                        return _cache_and_return(bal, "v3/assets-detail")
        else:
            code = data.get("code")
            if code not in ("40085", "40001"):
                logger.warning(f"[BalanceCache] ⚠️ v3/assets-detail code={code} msg={data.get('msg')}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v3/assets-detail respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v3/assets-detail excepción: {e}")

    # ── ENDPOINT 3: v2/mix/account/account (single) ─────────────────────
    try:
        path = "/api/v2/mix/account/account"
        qs   = "?symbol=USDTUSDT&productType=USDT-FUTURES&marginCoin=USDT"
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
                    return _cache_and_return(bal, "v2-single")
        else:
            code = data.get("code")
            if code == "40085":
                logger.debug("[BalanceCache] v2-single: 40085 (UA mode, esperado)")
            else:
                logger.warning(f"[BalanceCache] ⚠️ v2-single code={code} msg={data.get('msg')}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2-single respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2-single excepción: {e}")

    # ── ENDPOINT 4: v2/mix/account/accounts (plural) ─────────────────────
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
                    return _cache_and_return(bal, "v2-multi")
        else:
            code = data.get("code")
            if code == "40085":
                logger.debug("[BalanceCache] v2-multi: 40085 (UA mode, esperado)")
            else:
                logger.warning(f"[BalanceCache] ⚠️ v2-multi code={code} msg={data.get('msg')}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2-multi respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2-multi excepción: {e}")

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
        self._api_version = None   # "ua" | "v2"
        self._ua_pos_mode = None
        self._v2_pos_mode = None

    # ── HTTP HELPERS ─────────────────────────────────────────────────────

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

    # ── INICIALIZACIÓN ───────────────────────────────────────────────────

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
        """
        Detecta si la cuenta es Unified Account (UA) o Classic.
        UA: /api/v3/position/all-position responde con code=00000
        Classic: /api/v2/mix/account/account responde con code=00000

        FIX: En UA, /api/v2/mix/* devuelve 40085 siempre.
        Para posiciones, leverage y órdenes en UA usamos /api/v3/mix/*.
        """
        # Probe UA
        try:
            r = await self._http_get(
                "/api/v3/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                data = r.get("data") or []
                items = data if isinstance(data, list) else []
                if items:
                    self._ua_pos_mode = items[0].get("holdMode", "hedge")
                else:
                    try:
                        rc = await self._http_get(
                            "/api/v3/account/account",
                            {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
                        )
                        if rc.get("code") == "00000":
                            d = rc.get("data") or {}
                            d = d if isinstance(d, dict) else {}
                            self._ua_pos_mode = d.get("holdMode", "hedge")
                        else:
                            self._ua_pos_mode = "hedge"
                    except Exception:
                        self._ua_pos_mode = "hedge"
                logger.info(f"[{self.symbol}] ✅ Unified Account (v3). pos_mode={self._ua_pos_mode}")
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

    # ── PRECIO Y BALANCE ─────────────────────────────────────────────────

    async def get_price(self) -> float:
        ticker = await self.exchange.fetch_ticker(self.symbol)
        return float(ticker["last"])

    async def get_balance(self) -> float | None:
        return await get_cached_balance(self._api_key, self._api_secret, self._passphrase)

    # ── LEVERAGE ─────────────────────────────────────────────────────────
    # FIX: Unified Account usa /api/v3/mix/account/set-leverage
    # Classic usa /api/v2/mix/account/set-leverage
    # El error 40085 ocurría porque se enviaba siempre a v2.

    async def set_leverage(self, leverage: int, side: str | None = None):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        if self._api_version == "ua":
            endpoint = "/api/v3/mix/account/set-leverage"
            pos_mode = self._ua_pos_mode or "hedge"
        else:
            endpoint = "/api/v2/mix/account/set-leverage"
            pos_mode = self._v2_pos_mode or "hedge"

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

    # ── MÍNIMOS DE QTY ───────────────────────────────────────────────────

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

    # ── POSICIONES ABIERTAS ──────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # UA: /api/v3/position/all-position
        if self._api_version == "ua":
            try:
                r = await self._http_get(
                    "/api/v3/position/all-position",
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
                else:
                    logger.debug(
                        f"[{self.symbol}] UA positions: code={r.get('code')} msg={r.get('msg')}"
                    )
            except Exception as e:
                logger.debug(f"[{self.symbol}] UA positions error: {e}")
            # UA no tiene fallback a v2 para posiciones
            return None

        # Classic v2: /api/v2/mix/position/single-position
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
        except Exception as e:
            logger.debug(f"[{self.symbol}] v2 positions error: {e}")

        logger.warning(
            f"[{self.symbol}] ⚠️ _get_positions falló — estado local preservado"
        )
        return None

    # ── COLOCAR / CERRAR ÓRDENES ─────────────────────────────────────────
    # FIX: Bitget Unified Account usa /api/v3/mix/order/place-order
    # Classic Account usa /api/v2/mix/order/place-order
    # El error 40085 ocurría porque se enviaba siempre a v2.

    async def _place_order(self, side: str, trade_side: str, qty: float):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        if self._api_version == "ua":
            endpoint = "/api/v3/mix/order/place-order"
            pos_mode = self._ua_pos_mode or "hedge"
        else:
            endpoint = "/api/v2/mix/order/place-order"
            pos_mode = self._v2_pos_mode or "hedge"

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
            # Retry en one-way si hedge falló por modo incorrecto
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
        logger.debug(
            f"[{self.symbol}] calc_qty: usdt={usdt_amount} x lev={effective_lev} / price={price} "
            f"= raw {raw_qty:.6f} → qty={qty} (min={min_qty})"
        )
        return qty

    # ── ABRIR POSICIONES ─────────────────────────────────────────────────

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

    # ── CERRAR POSICIÓN ──────────────────────────────────────────────────

    async def close_position(self, reason: str = "Manual"):
        if not self.position:
            return {}
        price    = await self.get_price()
        side     = "sell" if self.position == "long" else "buy"
        qty_pos  = None
        positions = await self._get_positions()
        if positions:
            p       = positions[0]
            qty_pos = float(p.get("total") or p.get("contracts") or p.get("size") or 0)
        if not qty_pos:
            qty_pos = await self._calc_qty(10.0, price, self.leverage)
        r = await self._place_order(side, "close", qty_pos)
        if r.get("code") != "00000" and not self.dry_run:
            logger.error(
                f"[{self.symbol}] close_position FAILED: code={r.get('code')} msg={r.get('msg')}"
            )
        self.trade_count += 1
        pnl = 0.0
        if self.entry_price:
            if self.position == "long":
                pnl = (price - self.entry_price) / self.entry_price * 100
            else:
                pnl = (self.entry_price - price) / self.entry_price * 100
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1
        wr = self.win_count / self.trade_count * 100 if self.trade_count else 0
        logger.warning(
            f"🔒 [{self.symbol}] {self.position.upper()} cerrado | "
            f"{reason} | PnL: {pnl:+.2f}% | WR: {wr:.1f}%"
        )
        await notify_close(
            self.symbol, self.position, self.entry_price, price, pnl, reason, self.dry_run
        )
        result = {
            "symbol":  self.symbol,
            "side":    self.position,
            "entry":   self.entry_price,
            "exit":    price,
            "pnl_pct": round(pnl, 2),
            "reason":  reason,
        }
        self.position = self.entry_price = self.sl = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)
        return result

    # ── SL / TP PARCIAL ──────────────────────────────────────────────────

    async def _check_and_handle_sl_tp(self, risk, price: float) -> bool:
        if not self.position or not self.entry_price:
            return False

        if self.tp1 and not self.tp2_hit:
            if (self.position == "long"  and price >= self.tp1) or \
               (self.position == "short" and price <= self.tp1):
                logger.info(f"[{self.symbol}] 🎯 TP1 alcanzado @ {price}")

        if self.tp2 and not self.tp2_hit:
            if (self.position == "long"  and price >= self.tp2) or \
               (self.position == "short" and price <= self.tp2):
                logger.info(f"[{self.symbol}] 🎯 TP2 parcial @ {price}")
                positions = await self._get_positions()
                if positions:
                    total_qty = float(
                        positions[0].get("total") or
                        positions[0].get("contracts") or
                        positions[0].get("size") or 0
                    )
                    partial_qty = round(total_qty * TP2_PARTIAL_RATIO, 6)
                    if partial_qty > 0:
                        side = "sell" if self.position == "long" else "buy"
                        r = await self._place_order(side, "close", partial_qty)
                        if r.get("code") == "00000":
                            self.tp2_hit = True
                            mark_tp2_hit(self.symbol)
                            await notify_tp_partial(
                                self.symbol, self.position,
                                price, partial_qty, self.dry_run
                            )

        if self.tp3:
            if (self.position == "long"  and price >= self.tp3) or \
               (self.position == "short" and price <= self.tp3):
                await self.close_position(f"TP3 @ {price:.4f}")
                risk.on_trade_close(0)
                return True

        if self.sl:
            if (self.position == "long"  and price <= self.sl) or \
               (self.position == "short" and price >= self.sl):
                await self.close_position(f"SL @ {price:.4f}")
                risk.on_trade_close(0)
                return True

        should_exit, reason = risk.check_exit(price)
        if should_exit:
            result = await self.close_position(reason)
            risk.on_trade_close(result.get("pnl_pct", 0))
            return True

        return False

    # ── LOOP PRINCIPAL ───────────────────────────────────────────────────

    async def run(self, risk, global_risk=None):
        await self._init(risk.usdt_per_trade)
        interval = int(os.getenv("LOOP_INTERVAL", "60"))
        usdt = risk.usdt_per_trade

        while True:
            try:
                price = await self.get_price()

                if self.position:
                    closed = await self._check_and_handle_sl_tp(risk, price)
                    if closed:
                        await asyncio.sleep(interval)
                        continue

                # Sincronizar estado real con API
                positions = await self._get_positions()
                if positions is not None:
                    if positions and not self.position:
                        p = positions[0]
                        hold_side = p.get("holdSide", "").lower()
                        if hold_side in ("long", "short"):
                            self.position    = hold_side
                            self.entry_price = float(
                                p.get("openPriceAvg") or
                                p.get("avgOpenPrice") or price
                            )
                            logger.info(
                                f"[{self.symbol}] 🔄 Posición detectada en API: "
                                f"{hold_side} @ {self.entry_price}"
                            )
                    elif not positions and self.position:
                        logger.info(
                            f"[{self.symbol}] 🔄 Posición cerrada externamente — reseteando"
                        )
                        self.position = self.entry_price = self.sl = None
                        self.tp1 = self.tp2 = self.tp3 = None
                        clear_position(self.symbol)

                # Decisión de estrategia
                decision = await decide(
                    exch=self.exchange,
                    symbol=self.symbol,
                    ai_decide_fn=ai_decide,
                    has_open_position=self.position is not None,
                    current_pnl=None,
                )

                action = decision["action"]
                sig    = decision["signal"]
                reason = decision["reason"]

                if action == "CLOSE" and self.position:
                    result = await self.close_position("Señal CLOSE")
                    risk.on_trade_close(result.get("pnl_pct", 0))
                    if global_risk:
                        await global_risk.register_close(result.get("pnl_pct", 0))

                elif action == "BUY" and not self.position:
                    bal = await self.get_balance()
                    can_l, r1 = risk.can_open_trade(bal)
                    can_g, r2 = (
                        (True, "OK") if not global_risk
                        else await global_risk.can_open()
                    )
                    if can_l and can_g:
                        dyn_lev = sig.suggested_lev if sig and sig.suggested_lev else None
                        await self.open_long(
                            usdt,
                            sl=sig.sl   if sig else None,
                            tp1=sig.tp1 if sig else None,
                            tp2=sig.tp2 if sig else None,
                            tp3=sig.tp3 if sig else None,
                            leverage=dyn_lev,
                        )
                        risk.on_trade_open(self.entry_price, "long")
                        if global_risk:
                            await global_risk.register_open()
                    else:
                        logger.warning(
                            f"[{self.symbol}] ⛔ Trade bloqueado: {r1 if not can_l else r2}"
                        )

                elif action == "SELL" and not self.position:
                    bal = await self.get_balance()
                    can_l, r1 = risk.can_open_trade(bal)
                    can_g, r2 = (
                        (True, "OK") if not global_risk
                        else await global_risk.can_open()
                    )
                    if can_l and can_g:
                        dyn_lev = sig.suggested_lev if sig and sig.suggested_lev else None
                        await self.open_short(
                            usdt,
                            sl=sig.sl   if sig else None,
                            tp1=sig.tp1 if sig else None,
                            tp2=sig.tp2 if sig else None,
                            tp3=sig.tp3 if sig else None,
                            leverage=dyn_lev,
                        )
                        risk.on_trade_open(self.entry_price, "short")
                        if global_risk:
                            await global_risk.register_open()
                    else:
                        logger.warning(
                            f"[{self.symbol}] ⛔ Trade bloqueado: {r1 if not can_l else r2}"
                        )

                elif action in ("BUY", "SELL") and self.position:
                    opp = "long" if action == "SELL" else "short"
                    if self.position == opp:
                        result = await self.close_position(f"Regresión → {action}")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk:
                            await global_risk.register_close(result.get("pnl_pct", 0))

            except Exception as e:
                logger.error(f"[{self.symbol}] Loop error: {e}", exc_info=True)

            await asyncio.sleep(interval)
