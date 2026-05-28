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

# ─────────────────────────────────────────────────────────────
# CONSTANTES GLOBALES
# ─────────────────────────────────────────────────────────────

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

# Mínimos de qty conocidos por símbolo (fallback si la API no responde)
_MIN_QTY_FALLBACK = {
    "BTCUSDT":   0.001,
    "ETHUSDT":   0.01,
    "SOLUSDT":   0.1,
    "XRPUSDT":   1.0,
    "SUIUSDT":   1.0,
    "NEARUSDT":  0.1,
    "XLMUSDT":   1.0,
    "XAUUSDT":   0.01,
    "XAGUSDT":   0.1,
    "HYPEUSDT":  0.1,
    "FILOUSDT":  0.1,
    "FILUSDT":   0.1,
    "SOXLUSDT":  0.1,
    "ZECUSDT":   0.01,
    "WLDUSDT":   0.1,
    "BEATUSDT":  1.0,
    "BZUSDT":    1.0,
}

# Cache de min_qty leídos desde API (sym → float)
_min_qty_cache: dict = {}


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol       = symbol
        self.leverage     = leverage
        self.margin_mode  = margin_mode or "isolated"
        self.dry_run      = dry_run
        self.position     = None
        self.entry_price  = None
        self.sl           = None
        self.tp1          = None
        self.tp2          = None
        self.tp3          = None
        self.tp2_hit      = False
        self.usdt_amount  = None
        self.trade_count  = 0
        self.win_count    = 0
        self.total_pnl    = 0.0
        self._api_key     = api_key
        self._api_secret  = api_secret
        self._passphrase  = passphrase
        # None = autodetectar, "v3" | "v2" | "ua" = fijado tras primer éxito
        self._api_version = None
        # None = no detectado, "hedge" | "one_way" = modo detectado en UA
        self._ua_pos_mode = None
        self.exchange = ccxt.bitget({
            "apiKey":   api_key,
            "secret":   api_secret,
            "password": passphrase,
            "options":  {
                "defaultType":    "swap",
                "defaultSubType": "linear",
            },
        })

    # ─────────────────────────────────────────────────────────────
    # FIRMA HTTP DIRECTA
    # ─────────────────────────────────────────────────────────────

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

    async def _http_get(self, path: str, params: dict = None) -> dict:
        qs = ""
        if params:
            qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        full_path = path + qs
        headers = self._headers("GET", full_path)
        url = "https://api.bitget.com" + full_path
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json()

    async def _http_post(self, path: str, body: dict) -> dict:
        body_str = _json.dumps(body)
        headers  = self._headers("POST", path, body_str)
        url = "https://api.bitget.com" + path
        async with aiohttp.ClientSession() as s:
            async with s.post(url, headers=headers, data=body_str,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json()

    # ─────────────────────────────────────────────────────────────
    # DETECCIÓN DE TIPO DE CUENTA
    # ─────────────────────────────────────────────────────────────

    async def _detect_account_type(self):
        """
        Detecta si la cuenta es Unified (UA / v3) o Clásica (v2).
        Fija self._api_version y, si es UA, detecta el pos_mode.
        """
        try:
            r = await self._http_get("/api/v3/account/assets", {"coin": "USDT"})
            if r.get("code") == "00000":
                self._api_version = "ua"
                logger.info(f"[{self.symbol}] 🔎 Cuenta Unified detectada (v3)")
                await self._detect_ua_pos_mode()
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] v3/assets error: {e}")

        try:
            r = await self._http_get(
                "/api/v2/mix/account/account",
                {"symbol": self.symbol.replace("/", "").replace(":USDT", ""),
                 "productType": "USDT-FUTURES",
                 "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "v2"
                logger.info(f"[{self.symbol}] 🔎 Cuenta Clásica detectada (v2)")
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] v2/account error: {e}")

        logger.warning(
            f"[{self.symbol}] ⚠️ account-mode error 40404: "
            "Request URL NOT FOUND — asumiendo one_way"
        )
        self._ua_pos_mode = "hedge"  # default seguro para UA

    async def _detect_ua_pos_mode(self):
        """
        Para cuentas UA: detecta si el par está en hedge o one_way.
        Consulta posiciones abiertas; si no hay, asume hedge (default de Bitget UA).
        """
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
            r = await self._http_get(
                "/api/v3/mix/position/single-position",
                {"symbol": sym_clean,
                 "productType": "USDT-FUTURES",
                 "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                if data:
                    hs = str(data[0].get("holdSide") or "").lower()
                    self._ua_pos_mode = "hedge" if hs in ("long", "short") else "one_way"
                else:
                    self._ua_pos_mode = "hedge"  # sin posición → hedge por defecto
                logger.info(
                    f"[{self.symbol}] 📌 UA pos_mode detectado: {self._ua_pos_mode}"
                )
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] _detect_ua_pos_mode error: {e}")
        self._ua_pos_mode = "hedge"

    # ─────────────────────────────────────────────────────────────
    # PRECIO Y BALANCE
    # ─────────────────────────────────────────────────────────────

    async def get_price(self) -> float:
        ticker = await self.exchange.fetch_ticker(self.symbol)
        return float(ticker["last"])

    async def get_balance(self) -> float:
        """Retorna el balance USDT disponible."""
        # Intenta primero v3 (UA)
        try:
            r = await self._http_get("/api/v3/account/assets", {"coin": "USDT"})
            if r.get("code") == "00000":
                data = r.get("data") or []
                for item in data:
                    if item.get("coin") == "USDT":
                        bal = float(item.get("available", 0) or item.get("crossMaxAvailable", 0))
                        logger.info(f"[{self.symbol}] ✅ Balance USDT (v3/assets): {bal}")
                        return bal
        except Exception:
            pass

        # Fallback v2
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
            r = await self._http_get(
                "/api/v2/mix/account/account",
                {"symbol": sym_clean,
                 "productType": "USDT-FUTURES",
                 "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                d = r.get("data", {})
                bal = float(d.get("available", 0))
                logger.info(f"[{self.symbol}] ✅ Balance USDT (v2): {bal}")
                return bal
        except Exception:
            pass

        logger.warning(f"[{self.symbol}] ⚠️ No se pudo obtener balance, retornando 0")
        return 0.0

    async def fetch_ohlcv(self, timeframe="15m", limit=100):
        return await self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=limit)

    # ─────────────────────────────────────────────────────────────
    # MIN QTY
    # ─────────────────────────────────────────────────────────────

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
                data = r.get("data") or []
                if data:
                    mq = float(data[0].get("minTradeNum", 1))
                    _min_qty_cache[sym_clean] = mq
                    return mq
        except Exception as e:
            logger.debug(f"[{self.symbol}] _get_min_qty error: {e}")
        fb = _MIN_QTY_FALLBACK.get(sym_clean, 1.0)
        _min_qty_cache[sym_clean] = fb
        return fb

    # ─────────────────────────────────────────────────────────────
    # SET LEVERAGE DINÁMICO — aplica el leverage del signal en Bitget
    # FIX: hard-cap con MAX_LEVERAGE (default 15x),
    #      warning detallado cuando falla, retorna leverage efectivo.
    # ─────────────────────────────────────────────────────────────

    async def _set_leverage_on_exchange(self, leverage: int) -> int:
        """
        Fija el leverage para el par en Bitget antes de abrir una posición.
        Actualiza self.leverage si tiene éxito.
        Retorna el leverage efectivo (el solicitado si OK, self.leverage si falla).
        """
        # Hard-cap: nunca superar MAX_LEVERAGE (default 15x)
        max_lev = int(os.getenv("MAX_LEVERAGE", "15"))
        leverage = min(leverage, max_lev)
        if self.dry_run:
            self.leverage = leverage
            return leverage

        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        lev_str = str(leverage)

        # Intentar v3 (UA)
        try:
            payload_v3 = {
                "symbol":      sym_clean,
                "productType": "USDT-FUTURES",
                "marginCoin":  "USDT",
                "leverage":    lev_str,
            }
            r_v3 = await self._http_post("/api/v3/account/set-leverage", payload_v3)
            if r_v3.get("code") == "00000":
                # Algunas cuentas UA requieren fijar long/short por separado
                for hold in ("long", "short"):
                    payload_v3_s = {**payload_v3, "holdSide": hold}
                    await self._http_post("/api/v3/account/set-leverage", payload_v3_s)
                self.leverage = leverage
                logger.info(
                    f"[{self.symbol}] ⚙️ Leverage fijado a x{leverage} (v3)"
                )
                return leverage
        except Exception as e:
            logger.debug(f"[{self.symbol}] set-leverage v3 error: {e}")

        # Fallback v2
        try:
            payload_v2 = {
                "symbol":      sym_clean,
                "productType": "USDT-FUTURES",
                "marginCoin":  "USDT",
                "leverage":    lev_str,
            }
            r_v2 = await self._http_post("/api/v2/mix/account/set-leverage", payload_v2)
            if r_v2.get("code") == "00000":
                for hold in ("long", "short"):
                    payload_v2_s = {**payload_v2, "holdSide": hold}
                    await self._http_post("/api/v2/mix/account/set-leverage", payload_v2_s)
                self.leverage = leverage
                logger.info(
                    f"[{self.symbol}] ⚙️ Leverage fijado a x{leverage} (v2)"
                )
                return leverage
        except Exception as e:
            logger.debug(f"[{self.symbol}] set-leverage v2 error: {e}")

        logger.warning(
            f"[{self.symbol}] ⚠️ set-leverage x{leverage} falló "
            f"en v3 y v2. "
            f"qty se calculará con self.leverage={self.leverage}x (cuenta)"
        )
        return self.leverage  # retorna el leverage actual, no el solicitado

    # ─────────────────────────────────────────────────────────────
    # POSICIONES ABIERTAS EN BITGET
    # ─────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list:
        """
        Retorna la lista de posiciones abiertas para self.symbol.
        Intenta v3 (UA) primero, luego v2 clásica.
        """
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # ── v3 Unified Account ──
        if self._api_version in ("ua", None):
            try:
                r = await self._http_get(
                    "/api/v3/mix/position/single-position",
                    {"symbol": sym_clean,
                     "productType": "USDT-FUTURES",
                     "marginCoin": "USDT"}
                )
                if r.get("code") == "00000":
                    data = r.get("data") or []
                    open_pos = [
                        p for p in data
                        if float(p.get("total") or p.get("contracts") or
                                 p.get("size", 0)) > 0
                    ]
                    if open_pos or self._api_version == "ua":
                        return open_pos
            except Exception as e:
                logger.debug(f"[{self.symbol}] v3 positions error: {e}")

        # ── v2 Classic Account ──
        try:
            r = await self._http_get(
                "/api/v2/mix/position/single-position",
                {"symbol": sym_clean,
                 "productType": "USDT-FUTURES",
                 "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                return [
                    p for p in data
                    if float(p.get("total") or p.get("contracts") or
                             p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug(f"[{self.symbol}] v2 positions error: {e}")

        return []

    # ─────────────────────────────────────────────────────────────
    # COLOCAR / CERRAR ÓRDENES
    # ─────────────────────────────────────────────────────────────

    async def _place_order(self, side: str, trade_side: str, qty: float):
        """
        Coloca una orden de futuros en Bitget.
        Detecta automáticamente v3 UA (hedge/one_way) vs v2 clásica.
        """
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # ── UA (v3) ──
        if self._api_version in ("ua", None):
            try:
                payload = {
                    "symbol":     sym_clean,
                    "category":   "USDT-FUTURES",
                    "marginMode": self.margin_mode,
                    "marginCoin": "USDT",
                    "qty":        str(qty),
                    "side":       side,       # buy / sell
                    "orderType":  "market",
                }
                if self._ua_pos_mode == "hedge":
                    pos_side = "long" if side == "buy" else "short"
                    if trade_side == "close":
                        pos_side = "short" if side == "buy" else "long"
                    payload["posSide"] = pos_side

                logger.info(
                    f"[{self.symbol}] 📤 order [ua-{self._ua_pos_mode or 'auto'}]: "
                    f"{payload}"
                )
                r = await self._http_post("/api/v3/mix/order/place-order", payload)
                logger.info(f"[{self.symbol}] 📥 response ua: {r}")
                if r.get("code") == "00000":
                    order_id = r.get("data", {}).get("orderId", "?")
                    logger.info(
                        f"[{self.symbol}] {side}/{trade_side} "
                        f"posSide={payload.get('posSide','n/a')} "
                        f"qty={qty} orderId={order_id} ua-{self._ua_pos_mode or 'auto'}"
                    )
                    if self._api_version is None:
                        self._api_version = "ua"
                    return
                # Si falla con error de tipo de posición, intentar alternar pos_mode
                if r.get("code") == "25236":
                    alt_mode = "one_way" if self._ua_pos_mode != "one_way" else "hedge"
                    logger.warning(
                        f"[{self.symbol}] ⚠️ 25236 con mode={self._ua_pos_mode} "
                        f"→ cambiando a {alt_mode} y reintentando"
                    )
                    self._ua_pos_mode = alt_mode
                    retry = dict(payload)
                    retry.pop("posSide", None)
                    if alt_mode == "hedge":
                        pos_side = "long" if side == "buy" else "short"
                        if trade_side == "close":
                            pos_side = "short" if side == "buy" else "long"
                        retry["posSide"] = pos_side
                    logger.info(
                        f"[{self.symbol}] 📤 retry [{alt_mode}]: {retry}"
                    )
                    r2 = await self._http_post("/api/v3/mix/order/place-order", retry)
                    logger.info(f"[{self.symbol}] 📥 retry response: {r2}")
                    if r2.get("code") == "00000":
                        order_id = r2.get("data", {}).get("orderId", "?")
                        logger.info(
                            f"[{self.symbol}] {side}/{trade_side} "
                            f"posSide={retry.get('posSide','n/a')} "
                            f"qty={qty} orderId={order_id} ua-{alt_mode}"
                        )
                        if self._api_version is None:
                            self._api_version = "ua"
                        return
                    raise Exception(
                        f"place-order {r2.get('code')}: {r2.get('msg')}"
                    )
                if r.get("code") not in ("00000",):
                    raise Exception(f"place-order {r.get('code')}: {r.get('msg')}")
            except Exception as e:
                if self._api_version == "ua":
                    raise
                logger.debug(f"[{self.symbol}] ua order error: {e}")

        # ── v2 Classic ──
        try:
            one_way = (self._ua_pos_mode or "one_way") == "one_way"
            payload_v2 = {
                "symbol":      sym_clean,
                "category":    "USDT-FUTURES",
                "marginMode":  self.margin_mode,
                "marginCoin":  "USDT",
                "qty":         str(qty),
                "side":        side,
                "orderType":   "market",
            }
            if not one_way:
                pos_side = "long" if side == "buy" else "short"
                if trade_side == "close":
                    pos_side = "short" if side == "buy" else "long"
                payload_v2["tradeSide"] = "open" if trade_side == "open" else "close"

            logger.info(f"[{self.symbol}] 📤 order [one_way]: {payload_v2}")
            r_v2 = await self._http_post(
                "/api/v2/mix/order/place-order", payload_v2
            )
            logger.info(f"[{self.symbol}] 📥 response: {r_v2}")

            if r_v2.get("code") == "00000":
                order_id = r_v2.get("data", {}).get("orderId", "?")
                logger.info(
                    f"[{self.symbol}] {side}/{trade_side} "
                    f"qty={qty} orderId={order_id} v2"
                )
                if self._api_version is None:
                    self._api_version = "v2"
                return

            if r_v2.get("code") == "25236":
                alt = "hedge"
                logger.warning(
                    f"[{self.symbol}] ⚠️ 25236 con mode=one_way "
                    f"→ cambiando a {alt} y reintentando"
                )
                self._ua_pos_mode = alt
                retry_v2 = dict(payload_v2)
                retry_v2["tradeSide"] = "open" if trade_side == "open" else "close"
                logger.info(f"[{self.symbol}] 📤 retry [{alt}]: {retry_v2}")
                r3 = await self._http_post(
                    "/api/v2/mix/order/place-order", retry_v2
                )
                logger.info(f"[{self.symbol}] 📥 retry response: {r3}")
                if r3.get("code") == "00000":
                    self._api_version = "v2"
                    return
                raise Exception(f"place-order {r3.get('code')}: {r3.get('msg')}")

            raise Exception(f"place-order {r_v2.get('code')}: {r_v2.get('msg')}")
        except Exception as e:
            raise

    # ─────────────────────────────────────────────────────────────
    # INIT
    # ─────────────────────────────────────────────────────────────

    async def _init(self, usdt_amount: float):
        await self.exchange.load_markets()

        # ── DETECTAR TIPO DE CUENTA ANTES DE OPERAR ──
        await self._detect_account_type()

        # Si la detección no fue concluyente pero el balance v3/assets funciona → es UA
        if self._api_version is None:
            try:
                r_check = await self._http_get("/api/v3/account/assets?coin=USDT")
                if r_check.get("code") == "00000":
                    self._api_version = "ua"
                    logger.info(
                        f"[{self.symbol}] 🔎 Forzando UA: /v3/account/assets OK "
                        f"(fallback en _init)"
                    )
                    await self._detect_ua_pos_mode()
            except Exception:
                pass

        saved = load_position(self.symbol)
        if saved:
            # Restaurar api_version y ua_pos_mode si fueron guardados
            if saved.get("api_version") and self._api_version is None:
                self._api_version = saved["api_version"]
                logger.info(
                    f"[{self.symbol}] 📌 API version restaurada: {self._api_version}"
                )
            if saved.get("ua_pos_mode") and self._ua_pos_mode is None:
                self._ua_pos_mode = saved["ua_pos_mode"]

            # ── Validar contra el exchange que la posición realmente existe ──
            real_positions = []
            try:
                real_positions = await self._get_positions()
            except Exception as e:
                logger.warning(
                    f"[{self.symbol}] No se pudo verificar posición real: {e}"
                )

            if not real_positions:
                # Estado guardado pero Bitget no muestra posición abierta → stale
                logger.warning(
                    f"[{self.symbol}] 🧹 Estado guardado ({saved.get('position')} "
                    f"@ {saved.get('entry_price')}) pero Bitget no tiene posición "
                    f"abierta → limpiando estado stale"
                )
                clear_position(self.symbol)
                self.usdt_amount = usdt_amount
            else:
                # Posición confirmada en Bitget → restaurar
                self.position    = saved["position"]
                self.entry_price = saved["entry_price"]
                self.sl          = saved.get("sl")
                self.tp1         = saved.get("tp1")
                self.tp2         = saved.get("tp2")
                self.tp3         = saved.get("tp3")
                self.tp2_hit     = saved.get("tp2_hit", False)
                self.usdt_amount = saved.get("usdt_amount", usdt_amount)
                # Restaurar leverage guardado si existe
                if saved.get("leverage"):
                    self.leverage = saved["leverage"]
                logger.warning(
                    f"[{self.symbol}] ♻️  Estado recuperado (confirmado en exchange): "
                    f"{self.position} @ {self.entry_price} | "
                    f"SL={self.sl} TP1={self.tp1} TP2={self.tp2} TP3={self.tp3}"
                )
        else:
            self.usdt_amount = usdt_amount

        mode = "🧪 DRY" if self.dry_run else "💰 REAL"
        logger.info(f"✅ [{self.symbol}] Listo | x{self.leverage} | "
                    f"{self.margin_mode.upper()} | {mode}")

    # ─────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────

    async def _open_order(self, side: str, usdt_amount: float, leverage: int = None):
        # FIX: _open_order ahora acepta leverage explícito para calcular
        #      qty con el apalancamiento dinámico correcto, no self.leverage.
        price        = await self.get_price()
        effective_lev = leverage if leverage is not None else self.leverage
        qty           = round((usdt_amount * effective_lev) / price, 4)
        min_qty       = await self._get_min_qty()
        if qty < min_qty:
            qty = min_qty
        logger.info(
            f"[{self.symbol}] qty={qty} "
            f"(usdt={usdt_amount} × lev={effective_lev}x ÷ price={price})"
        )
        await self._place_order(side, "open", qty)

    async def _close_order(self, pos_side: str, qty: float):
        side = "sell" if pos_side == "long" else "buy"
        await self._place_order(side, "close", qty)

    async def _partial_close_order(self, pos_side: str, ratio: float):
        """Cierra un porcentaje de la posición abierta."""
        positions = await self._get_positions()
        if not positions:
            logger.warning(
                f"[{self.symbol}] _partial_close_order: sin posición abierta"
            )
            return
        for p in positions:
            size = float(
                p.get("total") or p.get("contracts") or
                p.get("size", 0)
            )
            hs = str(
                p.get("holdSide") or p.get("posSide") or
                p.get("positionSide") or p.get("side") or ""
            ).lower()
            if size > 0:
                ps = "long" if hs in ("long", "buy") else "short"
                partial_qty = round(size * ratio, 4)
                min_qty = await self._get_min_qty()
                if partial_qty < min_qty:
                    logger.warning(
                        f"[{self.symbol}] partial_qty {partial_qty} < "
                        f"min_qty {min_qty} — saltando cierre parcial"
                    )
                    return
                logger.info(
                    f"[{self.symbol}] ✂️ Cierre parcial "
                    f"{ratio*100:.0f}% ({partial_qty} contratos)"
                )
                await self._close_order(ps, partial_qty)
                return

    async def _sync_closed_from_exchange(self, fill_price: float, reason: str):
        """Sincroniza el cierre cuando la posición ya fue cerrada en exchange."""
        if not self.position:
            return {}
        pnl = (
            (fill_price - self.entry_price) / self.entry_price * 100 * self.leverage
            if self.position == "long" else
            (self.entry_price - fill_price) / self.entry_price * 100 * self.leverage
        )
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1
        wr = (
            self.win_count / self.trade_count * 100
            if self.trade_count else 0
        )
        await notify_close(
            self.symbol, self.position, self.entry_price,
            fill_price, pnl, reason, self.dry_run
        )
        logger.warning(
            f"🔒 [{self.symbol}] {self.position.upper()} cerrado (sync) | "
            f"{reason} | PnL: {pnl:+.2f}% | WR: {wr:.1f}%"
        )
        result = {
            "symbol":  self.symbol,
            "side":    self.position,
            "entry":   self.entry_price,
            "exit":    fill_price,
            "pnl_pct": round(pnl, 2),
            "reason":  reason,
        }
        self.position = self.entry_price = self.sl = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)
        return result

    # ─────────────────────────────────────────────────────────────
    # ABRIR POSICIONES
    # FIX: leverage dinámico SIEMPRE llama _set_leverage_on_exchange
    #      notify_open recibe los 6 args correctos (no sl/tp)
    # ─────────────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                        leverage: int = None):
        # Aplicar leverage dinámico — siempre fijar en exchange si se especifica
        effective_lev = self.leverage
        if leverage:
            effective_lev = await self._set_leverage_on_exchange(leverage)
        await self._open_order("buy", usdt_amount, leverage=effective_lev)
        self.position    = "long"
        self.entry_price = await self.get_price()
        self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
        self.tp2_hit = False
        self.usdt_amount = usdt_amount
        self.trade_count += 1
        save_position(
            self.symbol, self.position, self.entry_price,
            sl, tp1, tp2, tp3, usdt_amount, self.leverage,
            api_version=self._api_version,
            ua_pos_mode=self._ua_pos_mode,
        )
        logger.warning(
            f"📈 [{self.symbol}] LONG @ {self.entry_price} | "
            f"x{self.leverage} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}"
        )
        await notify_open(
            self.symbol, "long", self.entry_price, self.leverage,
            usdt_amount, self.dry_run
        )

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                         leverage: int = None):
        # Aplicar leverage dinámico — siempre fijar en exchange si se especifica
        effective_lev = self.leverage
        if leverage:
            effective_lev = await self._set_leverage_on_exchange(leverage)
        await self._open_order("sell", usdt_amount, leverage=effective_lev)
        self.position    = "short"
        self.entry_price = await self.get_price()
        self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
        self.tp2_hit = False
        self.usdt_amount = usdt_amount
        self.trade_count += 1
        save_position(
            self.symbol, self.position, self.entry_price,
            sl, tp1, tp2, tp3, usdt_amount, self.leverage,
            api_version=self._api_version,
            ua_pos_mode=self._ua_pos_mode,
        )
        logger.warning(
            f"📉 [{self.symbol}] SHORT @ {self.entry_price} | "
            f"x{self.leverage} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}"
        )
        await notify_open(
            self.symbol, "short", self.entry_price, self.leverage,
            usdt_amount, self.dry_run
        )

    # ─────────────────────────────────────────────────────────────
    # SL/TP CHECK
    # ─────────────────────────────────────────────────────────────

    async def _check_and_handle_sl_tp(self, price, risk, global_risk=None):
        if not self.position or not self.entry_price:
            return False

        is_long = self.position == "long"

        if self.sl:
            sl_hit = (price <= self.sl) if is_long else (price >= self.sl)
            if sl_hit:
                result = await self.close_position(f"SL @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if self.tp2 and not self.tp2_hit:
            tp2_hit = (price >= self.tp2) if is_long else (price <= self.tp2)
            if tp2_hit:
                self.tp2_hit = True
                mark_tp2_hit(self.symbol)
                try:
                    await self._partial_close_order(self.position, TP2_PARTIAL_RATIO)
                    from bot.telegram_bot import notify_tp_partial
                    await notify_tp_partial(
                        self.symbol, self.position, price, 2, TP2_PARTIAL_RATIO
                    )
                except Exception:
                    pass
                return False

        if self.tp3:
            tp3_hit = (
                (price >= self.tp3) if is_long else (price <= self.tp3)
            )
            if tp3_hit:
                result = await self.close_position(f"TP3 @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if self.tp1 and not self.tp2:
            tp1_hit = (
                (price >= self.tp1) if is_long else (price <= self.tp1)
            )
            if tp1_hit:
                result = await self.close_position(f"TP1 @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if not self.sl and not self.tp1:
            pnl = (
                (price - self.entry_price) / self.entry_price * 100 * self.leverage
                if is_long else
                (self.entry_price - price) / self.entry_price * 100 * self.leverage
            )
            tp_pct = float(os.getenv("AI_TP_PCT",  "3.0"))
            sl_pct = float(os.getenv("AI_SL_PCT", "-1.5"))
            if pnl >= tp_pct or pnl <= sl_pct:
                tag = (
                    f"TP +{pnl:.2f}%" if pnl >= tp_pct
                    else f"SL {pnl:.2f}%"
                )
                result = await self.close_position(tag)
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True
        return False

    # ─────────────────────────────────────────────────────────────
    # CERRAR POSICIÓN — FIX: no limpiar estado si la orden falla
    # ─────────────────────────────────────────────────────────────

    async def close_position(self, reason=""):
        if not self.position:
            return {}
        price = await self.get_price()
        if not self.dry_run:
            order_executed = False
            try:
                positions = await self._get_positions()
                if not positions:
                    # No hay posición real en Bitget — estado stale, limpiar
                    logger.warning(
                        f"[{self.symbol}] ⚠️ close_position: no hay posición "
                        f"real en Bitget — limpiando estado stale ({reason})"
                    )
                    self.position = self.entry_price = self.sl = None
                    self.tp1 = self.tp2 = self.tp3 = None
                    self.tp2_hit = False
                    clear_position(self.symbol)
                    return {}
                for p in positions:
                    size = float(
                        p.get("total") or p.get("contracts") or
                        p.get("size", 0)
                    )
                    # FIX: incluir posSide para Unified Account (UA)
                    hs = (
                        str(p.get("holdSide") or p.get("posSide")
                            or p.get("positionSide") or p.get("side") or "").lower()
                    )
                    if size > 0:
                        ps = "long" if hs in ("long", "buy") else "short"
                        await self._close_order(ps, size)
                        order_executed = True
                        break
            except Exception as e:
                logger.error(
                    f"[{self.symbol}] ❌ CIERRE FALLIDO en Bitget: {e} | "
                    f"Razón: {reason} — posición SIGUE ABIERTA"
                )
                # Notificar urgente por Telegram sin limpiar estado
                try:
                    from bot.telegram_bot import notify_close_failed
                    await notify_close_failed(self.symbol, reason, str(e))
                except Exception:
                    pass
                # Re-lanzar para que el loop principal lo capture
                raise

            if not order_executed:
                logger.warning(
                    f"[{self.symbol}] ⚠️ close_position: ninguna posición "
                    f"ejecutada — posiciones vacías tras consulta ({reason})"
                )
                self.position = self.entry_price = self.sl = None
                self.tp1 = self.tp2 = self.tp3 = None
                self.tp2_hit = False
                clear_position(self.symbol)
                return {}

        pnl = (
            (price - self.entry_price) / self.entry_price * 100 * self.leverage
            if self.position == "long" else
            (self.entry_price - price) / self.entry_price * 100 * self.leverage
        )
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1
        wr = (
            self.win_count / self.trade_count * 100
            if self.trade_count else 0
        )
        logger.warning(
            f"🔒 [{self.symbol}] {self.position.upper()} cerrado | "
            f"{reason} | PnL: {pnl:+.2f}% | WR: {wr:.1f}%"
        )
        await notify_close(
            self.symbol, self.position, self.entry_price,
            price, pnl, reason, self.dry_run
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

    # ─────────────────────────────────────────────────────────────
    # LOOP PRINCIPAL
    # ─────────────────────────────────────────────────────────────

    async def run(self, risk, global_risk=None):
        await self._init(risk.usdt_per_trade)
        interval = int(os.getenv("LOOP_INTERVAL", "60"))
        usdt = risk.usdt_per_trade
        tf   = os.getenv("TIMEFRAME", "15m")

        while True:
            try:
                price = await self.get_price()

                if self.position:
                    closed = await self._check_and_handle_sl_tp(
                        price, risk, global_risk
                    )
                    if closed:
                        await asyncio.sleep(interval)
                        continue

                async def _ai_fn(sym, ctx):
                    bars = await self.fetch_ohlcv(tf, limit=100)
                    return (await ai_decide(
                        sym, bars, self.position, self.entry_price,
                        self.leverage, context_override=ctx,
                    ))["action"]

                decision = await decide(
                    exch=self.exchange, symbol=self.symbol,
                    ai_decide_fn=_ai_fn,
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
                        await global_risk.register_close(
                            result.get("pnl_pct", 0)
                        )

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
                        logger.info(
                            f"[{self.symbol}] ⛔ "
                            f"{r1 if not can_l else r2}"
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
                        logger.info(
                            f"[{self.symbol}] ⛔ "
                            f"{r1 if not can_l else r2}"
                        )

                elif action in ("BUY", "SELL") and self.position:
                    # Señal en dirección contraria — cerrar y reabrir
                    opp = "long" if action == "SELL" else "short"
                    if self.position == opp:
                        result = await self.close_position(f"Regresión → {action}")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk:
                            await global_risk.register_close(
                                result.get("pnl_pct", 0)
                            )

            except Exception as e:
                logger.error(f"[{self.symbol}] Loop error: {e}", exc_info=True)

            await asyncio.sleep(interval)
