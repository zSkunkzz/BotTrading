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

_BALANCE_RETRY_SLEEP = float(os.getenv("BALANCE_RETRY_SLEEP", "6"))
_BALANCE_CACHE_TTL   = float(os.getenv("BALANCE_CACHE_TTL", "30"))

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
    "ALOUSDT":   1.0,
}

_min_qty_cache: dict = {}


async def _safe_json(response) -> dict:
    text = await response.text()
    stripped = text.strip()
    if not stripped.startswith("{") and not stripped.startswith("["):
        raise ValueError(f"Respuesta no-JSON: {stripped[:200]}")
    try:
        data = _json.loads(stripped)
    except _json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido: {e} — contenido: {stripped[:200]}")
    if not isinstance(data, dict):
        raise ValueError(f"Respuesta inesperada (tipo {type(data).__name__}): {str(data)[:300]}")
    return data


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
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
        self._cached_balance: float | None = None
        self._balance_ts: float = 0.0

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

    # ------------------------------------------------------------------ balance

    async def _extract_usdt_balance(self, data) -> float | None:
        """
        Extrae el balance USDT disponible de la respuesta de la API de Bitget.

        Formatos soportados:
        1. Lista de assets directos: [{coin: USDT, available: X}, ...]
        2. Lista de cuentas UA con 'list' anidado: [{accountType: ..., list: [{coin: USDT, ...}]}]
        3. Diccionario directo con campos de balance
        4. Diccionario con 'list' anidado
        """
        balance_keys = (
            "available", "availableBalance", "crossMaxAvailable",
            "equity", "usdtEquity", "isolatedMaxAvailable",
            "crossedMaxAvailable", "walletBalance", "balance", "free",
        )

        def _extract_from_dict(d: dict) -> float | None:
            coin = d.get("marginCoin") or d.get("coin") or d.get("asset") or ""
            if coin.upper() == "USDT":
                for key in balance_keys:
                    v = _to_float(d.get(key))
                    if v is not None and v >= 0:
                        return v
            return None

        def _scan_list(lst: list) -> float | None:
            best = None
            for item in lst:
                if not isinstance(item, dict):
                    continue
                # Formato directo: {coin: USDT, available: X}
                v = _extract_from_dict(item)
                if v is not None:
                    if best is None or v > best:
                        best = v
                    continue
                # Formato UA anidado: {accountType: ..., list: [{coin: USDT, ...}]}
                nested = item.get("list") or item.get("assets") or []
                if isinstance(nested, list):
                    for sub in nested:
                        if isinstance(sub, dict):
                            v2 = _extract_from_dict(sub)
                            if v2 is not None:
                                if best is None or v2 > best:
                                    best = v2
            return best

        if isinstance(data, list):
            val = _scan_list(data)
            if val is not None:
                return val

        if isinstance(data, dict):
            # Directo
            for key in balance_keys:
                v = _to_float(data.get(key))
                if v is not None and v >= 0:
                    return v
            # 'list' anidado dentro del dict raíz
            nested = data.get("list") or data.get("assets") or []
            if isinstance(nested, list):
                val = _scan_list(nested)
                if val is not None:
                    return val

        return None

    async def _fetch_balance_once(self) -> float | None:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        endpoints = [
            ("/api/v2/mix/account/accounts",        {"productType": "USDT-FUTURES"}),
            ("/api/v2/account/all-account-balance", {"coin": "USDT"}),
            ("/api/v2/mix/account/account",         {"symbol": sym_clean, "productType": "USDT-FUTURES", "marginCoin": "USDT"}),
            ("/api/v2/spot/account/assets",         {"coin": "USDT"}),
            ("/api/v2/account/info",                None),
        ]
        for path, params in endpoints:
            try:
                r = await self._http_get(path, params)
                code = r.get("code")
                if code == "00000":
                    raw_data = r.get("data")
                    val = await self._extract_usdt_balance(raw_data)
                    if val is not None:
                        logger.info(f"[{self.symbol}] 💰 Balance USDT: {val:.2f} (vía {path})")
                        return val
                    else:
                        logger.info(
                            f"[{self.symbol}] ⚠️ {path} → code=00000 pero sin USDT extraíble. "
                            f"data={str(raw_data)[:300]}"
                        )
                else:
                    logger.info(
                        f"[{self.symbol}] ⚠️ {path} → code={code} msg={r.get('msg')} "
                        f"data={str(r.get('data', ''))[:200]}"
                    )
            except Exception as e:
                logger.info(f"[{self.symbol}] ⚠️ {path} excepción: {e}")
        return None

    async def get_cached_balance(self) -> float | None:
        now = time.time()
        if self._cached_balance is not None and (now - self._balance_ts) < _BALANCE_CACHE_TTL:
            return self._cached_balance
        val = await self._fetch_balance_once()
        if val is not None:
            self._cached_balance = val
            self._balance_ts = now
        return val

    async def _wait_for_balance(self, timeout: float = 60.0) -> float | None:
        deadline = time.time() + timeout
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            val = await self._fetch_balance_once()
            if val is not None:
                self._cached_balance = val
                self._balance_ts = time.time()
                self._balance_ok = True
                return val
            logger.warning(f"[{self.symbol}] ⏳ Balance no disponible (intento {attempt}), reintentando en {_BALANCE_RETRY_SLEEP}s…")
            await asyncio.sleep(_BALANCE_RETRY_SLEEP)
        logger.error(f"[{self.symbol}] ❌ No se pudo obtener balance tras {timeout}s.")
        return None

    # ------------------------------------------------------------------ init
    async def _init(self):
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
        # Probe 1: UA via mix/accounts (más fiable en Unified Account)
        try:
            r = await self._http_get(
                "/api/v2/mix/account/accounts",
                {"productType": "USDT-FUTURES"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                try:
                    rp = await self._http_get(
                        "/api/v2/mix/position/all-position",
                        {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
                    )
                    if rp.get("code") == "00000":
                        items = rp.get("data") or []
                        self._ua_pos_mode = items[0].get("holdMode", "single_hold") if items else "single_hold"
                    else:
                        self._ua_pos_mode = "single_hold"
                except Exception:
                    self._ua_pos_mode = "single_hold"
                logger.info(
                    f"[{self.symbol}] ✅ Unified Account (UA) via mix/accounts. pos_mode={self._ua_pos_mode}"
                )
                return
            else:
                logger.info(f"[{self.symbol}] mix/accounts → code={r.get('code')} msg={r.get('msg')}")
        except Exception as e:
            logger.info(f"[{self.symbol}] mix/accounts probe error: {e}")

        # Probe 2: UA via all-account-balance
        try:
            r = await self._http_get(
                "/api/v2/account/all-account-balance",
                {"coin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                self._ua_pos_mode = "single_hold"
                logger.info(f"[{self.symbol}] ✅ Unified Account (UA) via all-account-balance. pos_mode=single_hold")
                return
            else:
                logger.info(f"[{self.symbol}] all-account-balance → code={r.get('code')} msg={r.get('msg')}")
        except Exception as e:
            logger.info(f"[{self.symbol}] UA all-account-balance probe error: {e}")

        # Probe 3: spot assets
        try:
            r = await self._http_get(
                "/api/v2/spot/account/assets",
                {"coin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                self._ua_pos_mode = "single_hold"
                logger.info(f"[{self.symbol}] ✅ Cuenta detectada via spot/assets.")
                return
            else:
                logger.info(f"[{self.symbol}] spot/assets → code={r.get('code')} msg={r.get('msg')}")
        except Exception as e:
            logger.info(f"[{self.symbol}] spot probe error: {e}")

        # Probe 4: Classic Account v2
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
            else:
                logger.info(f"[{self.symbol}] mix/account/account → code={r.get('code')} msg={r.get('msg')}")
        except Exception as e:
            logger.info(f"[{self.symbol}] v2 probe error: {e}")

        logger.warning(f"[{self.symbol}] ⚠️ Tipo de cuenta no detectado, asumiendo UA single_hold.")
        self._api_version = "ua"
        self._ua_pos_mode = "single_hold"

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
                    return bars[-OHLCV_LIMIT:]
        except Exception:
            pass
        return await self.exchange.fetch_ohlcv(self.symbol, tf, limit=OHLCV_LIMIT)

    async def get_min_qty(self) -> float:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        if sym_clean in _min_qty_cache:
            return _min_qty_cache[sym_clean]
        try:
            r = await self._http_get(
                "/api/v2/mix/market/contracts",
                {"productType": "USDT-FUTURES", "symbol": sym_clean},
            )
            if r.get("code") == "00000":
                items = r.get("data") or []
                for item in (items if isinstance(items, list) else [items]):
                    v = _to_float(item.get("minTradeNum") or item.get("minQty"))
                    if v is not None and v > 0:
                        _min_qty_cache[sym_clean] = v
                        return v
        except Exception as e:
            logger.debug(f"[{self.symbol}] get_min_qty error: {e}")
        fallback = _MIN_QTY_FALLBACK.get(sym_clean, 0.01)
        _min_qty_cache[sym_clean] = fallback
        return fallback

    async def set_leverage(self, leverage: int, side: str | None = None):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        if self._api_version == "ua":
            sides = ["all"]
        else:
            sides = ["long", "short"] if self._v2_pos_mode == "hedge" else ["long"]
        for s in sides:
            payload = {
                "symbol":      sym_clean,
                "productType": "USDT-FUTURES",
                "marginCoin":  "USDT",
                "leverageVal": str(leverage),
                "holdSide":    s,
            }
            try:
                r = await self._http_post("/api/v2/mix/account/set-leverage", payload)
                if r.get("code") not in ("00000", "40919"):
                    logger.warning(f"[{self.symbol}] set_leverage {s}: {r.get('msg')}")
            except Exception as e:
                logger.warning(f"[{self.symbol}] set_leverage exception ({s}): {e}")

    async def set_margin_mode(self):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        payload = {
            "symbol":      sym_clean,
            "productType": "USDT-FUTURES",
            "marginCoin":  "USDT",
            "marginMode":  self.margin_mode,
        }
        try:
            r = await self._http_post("/api/v2/mix/account/set-margin-mode", payload)
            if r.get("code") not in ("00000", "40919"):
                logger.warning(f"[{self.symbol}] set_margin_mode: {r.get('msg')}")
        except Exception as e:
            logger.warning(f"[{self.symbol}] set_margin_mode exception: {e}")

    async def _place_order(self, side: str, trade_side: str, qty: float) -> dict:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        if self.dry_run:
            logger.info(f"[{self.symbol}] 🧪 DRY RUN: {side}/{trade_side} qty={qty}")
            return {"code": "00000", "data": {"orderId": "DRY"}}

        if self._api_version == "ua":
            if self._ua_pos_mode == "single_hold":
                payload = {
                    "symbol":      sym_clean,
                    "productType": "USDT-FUTURES",
                    "marginMode":  self.margin_mode,
                    "marginCoin":  "USDT",
                    "size":        str(qty),
                    "orderType":   "market",
                    "side":        side,
                    "holdMode":    "single_hold",
                }
            else:
                payload = {
                    "symbol":      sym_clean,
                    "productType": "USDT-FUTURES",
                    "marginMode":  self.margin_mode,
                    "marginCoin":  "USDT",
                    "size":        str(qty),
                    "orderType":   "market",
                    "side":        side,
                    "tradeSide":   trade_side,
                }
        else:
            payload = {
                "symbol":      sym_clean,
                "productType": "USDT-FUTURES",
                "marginMode":  self.margin_mode,
                "marginCoin":  "USDT",
                "size":        str(qty),
                "orderType":   "market",
                "side":        side,
                "tradeSide":   trade_side,
            }

        r = await self._http_post("/api/v2/mix/order/place-order", payload)
        if r.get("code") != "00000":
            raise RuntimeError(f"place_order error: {r.get('msg')} | payload={payload}")
        self._cached_balance = None
        return r

    async def open_long(self, qty: float) -> dict:
        return await self._place_order("buy", "open", qty)

    async def open_short(self, qty: float) -> dict:
        return await self._place_order("sell", "open", qty)

    async def close_long(self, qty: float) -> dict:
        return await self._place_order("sell", "close", qty)

    async def close_short(self, qty: float) -> dict:
        return await self._place_order("buy", "close", qty)

    async def get_open_position(self) -> dict | None:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        try:
            r = await self._http_get(
                "/api/v2/mix/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"},
            )
            if r.get("code") == "00000":
                for pos in (r.get("data") or []):
                    if pos.get("symbol", "").upper() == sym_clean.upper():
                        total = _to_float(pos.get("total"))
                        if total and total > 0:
                            return pos
        except Exception as e:
            logger.debug(f"[{self.symbol}] get_open_position error: {e}")
        return None

    async def _handle_tp2_partial(self, price: float, pos_side: str):
        if self.tp2_hit:
            return
        if self.tp2 is None:
            return
        hit = (pos_side == "long" and price >= self.tp2) or \
              (pos_side == "short" and price <= self.tp2)
        if not hit:
            return
        pos = await self.get_open_position()
        if not pos:
            return
        total_qty = _to_float(pos.get("total"))
        if not total_qty or total_qty <= 0:
            return
        partial_qty = round(total_qty * TP2_PARTIAL_RATIO, 6)
        min_qty = await self.get_min_qty()
        if partial_qty < min_qty:
            logger.warning(f"[{self.symbol}] TP2 partial qty {partial_qty} < min {min_qty}, skip.")
            return
        try:
            if pos_side == "long":
                await self.close_long(partial_qty)
            else:
                await self.close_short(partial_qty)
            self.tp2_hit = True
            mark_tp2_hit(self.symbol)
            logger.info(f"[{self.symbol}] 🎯 TP2 parcial ejecutado: {partial_qty} @ {price}")
            await notify_tp_partial(self.symbol, pos_side, price, partial_qty)
        except Exception as e:
            logger.error(f"[{self.symbol}] TP2 partial error: {e}")

    async def run(self, risk_manager=None, global_risk=None, interval: int = 60):
        """Bucle principal del trader.

        Args:
            risk_manager: instancia de RiskManager (o float usdt_per_trade por
                          compatibilidad con llamadas antiguas).
            global_risk:  instancia de GlobalRisk compartida entre traders.
            interval:     segundos entre ciclos.
        """
        # Compatibilidad: si se pasa un float directamente usará ese valor
        if isinstance(risk_manager, (int, float)):
            usdt_per_trade = float(risk_manager)
            sl_pct_default  = float(os.getenv("SL_PCT",  "0.015"))
            tp1_pct_default = float(os.getenv("TP1_PCT", "0.01"))
            tp2_pct_default = float(os.getenv("TP2_PCT", "0.025"))
            tp3_pct_default = float(os.getenv("TP3_PCT", "0.04"))
        else:
            rm = risk_manager
            usdt_per_trade  = getattr(rm, "usdt_per_trade",  float(os.getenv("USDT_PER_TRADE", "10")))
            sl_pct_default  = getattr(rm, "sl_pct",   float(os.getenv("SL_PCT",  "0.015")))
            tp1_pct_default = getattr(rm, "tp_pct",   float(os.getenv("TP1_PCT", "0.01")))  # tp1 ~ tp_pct/4
            tp2_pct_default = getattr(rm, "tp_pct",   float(os.getenv("TP2_PCT", "0.025"))) / 100 * float(os.getenv("TP2_MULT", "250")) / 100
            tp3_pct_default = getattr(rm, "tp_pct",   float(os.getenv("TP3_PCT", "0.04")))  / 100
            # Normalizar sl_pct a decimal si viene como porcentaje (e.g. 2.0 → 0.02)
            if sl_pct_default > 1:
                sl_pct_default /= 100
            if tp1_pct_default > 1:
                tp1_pct_default /= 100
            if tp2_pct_default > 1:
                tp2_pct_default /= 100
            if tp3_pct_default > 1:
                tp3_pct_default /= 100

        await self._init()
        await self.set_margin_mode()
        await self.set_leverage(self.leverage)

        balance = await self._wait_for_balance(timeout=120.0)
        if balance is None:
            logger.error(f"[{self.symbol}] ❌ No se pudo obtener balance. Abortando.")
            return
        logger.info(f"[{self.symbol}] 💰 Balance inicial: {balance:.2f} USDT")

        while True:
            try:
                # Respetar límite global de trades concurrentes
                if global_risk is not None:
                    can_open = getattr(global_risk, "can_open_trade", None)
                    if callable(can_open) and not can_open():
                        logger.debug(f"[{self.symbol}] GlobalRisk: límite alcanzado, esperando.")
                        await asyncio.sleep(interval)
                        continue

                price = await self.get_price()
                ohlcv = await self.get_ohlcv()

                if len(ohlcv) < OHLCV_MIN_BARS:
                    logger.warning(f"[{self.symbol}] ⚠️ OHLCV insuficiente ({len(ohlcv)} barras). Esperando…")
                    await asyncio.sleep(interval)
                    continue

                if self.position:
                    await self._handle_tp2_partial(price, self.position)
                    pos = await self.get_open_position()
                    if pos is None:
                        logger.info(f"[{self.symbol}] 📭 Posición cerrada externamente.")
                        if global_risk is not None:
                            close_fn = getattr(global_risk, "close_trade", None)
                            if callable(close_fn):
                                close_fn(self.symbol)
                        self.position = None
                        clear_position(self.symbol)
                    elif self.sl and (
                        (self.position == "long"  and price <= self.sl) or
                        (self.position == "short" and price >= self.sl)
                    ):
                        logger.info(f"[{self.symbol}] 🛑 SL alcanzado @ {price}")
                        qty = _to_float(pos.get("total")) or 0
                        if qty > 0:
                            if self.position == "long":
                                await self.close_long(qty)
                            else:
                                await self.close_short(qty)
                        pnl = (price - self.entry_price) * qty if self.position == "long" \
                              else (self.entry_price - price) * qty
                        self.trade_count += 1
                        self.total_pnl   += pnl
                        if global_risk is not None:
                            close_fn = getattr(global_risk, "close_trade", None)
                            if callable(close_fn):
                                close_fn(self.symbol)
                        await notify_close(self.symbol, self.position, price,
                                           self.entry_price, pnl,
                                           self.trade_count, self.win_count, self.total_pnl)
                        self.position = None
                        clear_position(self.symbol)
                    elif self.tp3 and (
                        (self.position == "long"  and price >= self.tp3) or
                        (self.position == "short" and price <= self.tp3)
                    ):
                        logger.info(f"[{self.symbol}] 🏆 TP3 alcanzado @ {price}")
                        qty = _to_float(pos.get("total")) or 0
                        if qty > 0:
                            if self.position == "long":
                                await self.close_long(qty)
                            else:
                                await self.close_short(qty)
                        pnl = (price - self.entry_price) * qty if self.position == "long" \
                              else (self.entry_price - price) * qty
                        self.trade_count += 1
                        self.win_count   += 1
                        self.total_pnl   += pnl
                        if global_risk is not None:
                            close_fn = getattr(global_risk, "close_trade", None)
                            if callable(close_fn):
                                close_fn(self.symbol)
                        await notify_close(self.symbol, self.position, price,
                                           self.entry_price, pnl,
                                           self.trade_count, self.win_count, self.total_pnl)
                        self.position = None
                        clear_position(self.symbol)
                else:
                    balance = await self.get_cached_balance()
                    if balance is None:
                        logger.warning(f"[{self.symbol}] ⚠️ Balance no disponible, skip ciclo.")
                        await asyncio.sleep(interval)
                        continue

                    signal = decide(ohlcv)
                    if signal == "hold":
                        signal = await ai_decide(ohlcv, self.symbol)

                    if signal in ("long", "short"):
                        trade_usdt = min(usdt_per_trade, balance)
                        if trade_usdt < 1.0:
                            logger.warning(f"[{self.symbol}] ⚠️ Balance insuficiente ({balance:.2f} USDT).")
                            await asyncio.sleep(interval)
                            continue

                        qty = trade_usdt * self.leverage / price
                        min_qty = await self.get_min_qty()
                        qty = max(round(qty, 6), min_qty)

                        sl_pct  = sl_pct_default
                        tp1_pct = tp1_pct_default
                        tp2_pct = tp2_pct_default
                        tp3_pct = tp3_pct_default

                        if signal == "long":
                            await self.open_long(qty)
                            self.sl  = price * (1 - sl_pct)
                            self.tp1 = price * (1 + tp1_pct)
                            self.tp2 = price * (1 + tp2_pct)
                            self.tp3 = price * (1 + tp3_pct)
                        else:
                            await self.open_short(qty)
                            self.sl  = price * (1 + sl_pct)
                            self.tp1 = price * (1 - tp1_pct)
                            self.tp2 = price * (1 - tp2_pct)
                            self.tp3 = price * (1 - tp3_pct)

                        self.position    = signal
                        self.entry_price = price
                        self.tp2_hit     = False

                        if global_risk is not None:
                            open_fn = getattr(global_risk, "open_trade", None)
                            if callable(open_fn):
                                open_fn(self.symbol)

                        save_position(self.symbol, signal, price,
                                      sl=self.sl, tp1=self.tp1,
                                      tp2=self.tp2, tp3=self.tp3)
                        logger.info(f"[{self.symbol}] 🚀 {signal.upper()} abierto @ {price} | qty={qty} | SL={self.sl:.4f} TP3={self.tp3:.4f}")
                        await notify_open(self.symbol, signal, price, qty,
                                          self.sl, self.tp1, self.tp2, self.tp3)

            except Exception as e:
                logger.error(f"[{self.symbol}] ❌ Error en ciclo: {e}", exc_info=True)

            await asyncio.sleep(interval)
