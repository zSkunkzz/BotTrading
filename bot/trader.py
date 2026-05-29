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
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.execution_engine import execution_engine

logger = logging.getLogger("Trader")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

OHLCV_TF        = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT     = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS  = int(os.getenv("OHLCV_MIN_BARS", "55"))

# Cuantos errores 40085 consecutivos pausan el simbolo
_CB_40085_THRESHOLD = int(os.getenv("CB_40085_THRESHOLD", "3"))
# Tiempo de pausa en segundos tras superar el umbral
_CB_40085_PAUSE_S   = int(os.getenv("CB_40085_PAUSE_S", "300"))  # 5 min

# Permite sobreescribir la deteccion automatica de pos mode.
# Valores validos: "hedge" | "one_way" | "" (auto-detect)
_FORCE_POS_MODE = os.getenv("FORCE_POS_MODE", "").strip().lower()

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
    "HBARUSDT":  1.0,
    "ALLOUSDT":  1.0,
    "LABUSDT":   1.0,
    "INJUSDT":   0.01,
    "IDUSDT":    1.0,
    "PEPEUSDT":  1000.0,
    "UBUSDT":    1.0,
}

_min_qty_cache = {}

# Cache global del modo de posicion detectado para evitar consultas repetidas
# Valores: None (no detectado), "one_way", "hedge"
_pos_mode_cache: str | None = None
_pos_mode_detected_at: float = 0.0
_POS_MODE_CACHE_TTL = 300.0  # 5 minutos


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol       = symbol
        self.leverage     = leverage
        self.margin_mode  = os.getenv("MARGIN_MODE", margin_mode or "isolated").lower()
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
        self.sl_order_id    = None
        self.tp_order_id    = None
        self._protection_ok = False
        self._open_notional = 0.0
        self._open_leverage = 1
        # Circuit breaker para error 40085
        self._cb_40085_count   = 0
        self._cb_40085_paused_until = 0.0

    # -- Helpers HTTP ----------------------------------------------------------

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
                return await self._safe_json(r)

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
                return await self._safe_json(r)

    @staticmethod
    async def _safe_json(response) -> dict:
        text = await response.text()
        stripped = text.strip()
        if not stripped.startswith(("{" , "[")):
            raise ValueError(f"Respuesta no-JSON: {stripped[:200]}")
        try:
            data = _json.loads(stripped)
        except _json.JSONDecodeError as e:
            raise ValueError(f"JSON invalido: {e} -- contenido: {stripped[:200]}")
        if not isinstance(data, dict):
            raise ValueError(f"Respuesta inesperada: {str(data)[:300]}")
        return data

    # -- Simbolo limpio --------------------------------------------------------

    def _sym(self) -> str:
        """Devuelve el simbolo limpio p.ej. HBARUSDT sin slash ni sufijo."""
        s = self.symbol.replace("/", "").replace(":USDT", "")
        if s.endswith("USDTUSDT"):
            s = s[:-4]
        return s

    # -- Circuit breaker helper publico ----------------------------------------

    def is_cb_paused(self) -> bool:
        """True si el circuit breaker 40085 esta activo ahora mismo."""
        return self._cb_40085_paused_until > time.time()

    # -- Deteccion de modo de posicion (one-way vs hedge) ----------------------

    async def _detect_pos_mode(self) -> str:
        """
        Retorna 'one_way' o 'hedge'. Cachea el resultado 5 minutos.

        Orden de intentos:
          1. Variable de entorno FORCE_POS_MODE (sobreescritura manual)
          2. /api/v3/account/account-info  -> campo posMode  (UA v3, siempre disponible)
          3. /api/v2/mix/account/account   -> campo holdMode (Classic, fallback)
          4. /api/v3/position/current-position -> holdMode (solo si hay posicion abierta)
          5. Fallback hardcoded: hedge (configuracion conocida de esta cuenta)
        """
        global _pos_mode_cache, _pos_mode_detected_at
        now = time.time()

        # 1. Sobreescritura manual via env var
        if _FORCE_POS_MODE in ("hedge", "one_way"):
            if _pos_mode_cache != _FORCE_POS_MODE:
                logger.info(
                    "[%s] Modo posicion forzado por FORCE_POS_MODE: %s",
                    self.symbol, _FORCE_POS_MODE,
                )
                _pos_mode_cache = _FORCE_POS_MODE
                _pos_mode_detected_at = now
            return _FORCE_POS_MODE

        # Cache valido
        if _pos_mode_cache is not None and (now - _pos_mode_detected_at) < _POS_MODE_CACHE_TTL:
            return _pos_mode_cache

        sym = self._sym()

        # 2. UA v3: /api/v3/account/account-info
        try:
            r = await self._http_get("/api/v3/account/account-info")
            if r.get("code") == "00000":
                pos_mode_raw = (r.get("data") or {}).get("posMode", "")
                if pos_mode_raw:
                    mode = "hedge" if "hedge" in pos_mode_raw.lower() else "one_way"
                    _pos_mode_cache = mode
                    _pos_mode_detected_at = now
                    logger.info(
                        "[%s] Modo posicion detectado (v3 account-info posMode='%s'): %s",
                        self.symbol, pos_mode_raw, mode,
                    )
                    return mode
        except Exception as e:
            logger.debug("[%s] _detect_pos_mode v3 account-info error: %s", self.symbol, e)

        # 3. Classic fallback: /api/v2/mix/account/account
        try:
            r = await self._http_get(
                "/api/v2/mix/account/account",
                {"symbol": sym, "productType": "USDT-FUTURES", "marginCoin": "USDT"},
            )
            if r.get("code") == "00000":
                hold_mode = (r.get("data") or {}).get("holdMode", "")
                if hold_mode:
                    mode = "hedge" if "hedge" in hold_mode.lower() else "one_way"
                    _pos_mode_cache = mode
                    _pos_mode_detected_at = now
                    logger.info(
                        "[%s] Modo posicion detectado (v2 account holdMode): %s",
                        self.symbol, mode,
                    )
                    return mode
        except Exception as e:
            logger.debug("[%s] _detect_pos_mode v2 account error: %s", self.symbol, e)

        # 4. Posicion activa: /api/v3/position/current-position
        try:
            r = await self._http_get(
                "/api/v3/position/current-position",
                {"category": "USDT-FUTURES", "symbol": sym},
            )
            if r.get("code") == "00000":
                raw = r.get("data", {})
                items = raw.get("list") if isinstance(raw, dict) else raw
                if not isinstance(items, list):
                    items = []
                if items:
                    hold_mode = items[0].get("holdMode", "")
                    if hold_mode:
                        mode = "hedge" if "hedge" in hold_mode.lower() else "one_way"
                        _pos_mode_cache = mode
                        _pos_mode_detected_at = now
                        logger.info(
                            "[%s] Modo posicion detectado (v3 position holdMode): %s",
                            self.symbol, mode,
                        )
                        return mode
        except Exception as e:
            logger.debug("[%s] _detect_pos_mode v3 position error: %s", self.symbol, e)

        # 5. Fallback hardcoded: hedge
        logger.warning(
            "[%s] No se pudo detectar holdMode via API -- usando hedge como fallback "
            "(configuracion conocida). Puedes sobreescribir con FORCE_POS_MODE=hedge|one_way",
            self.symbol,
        )
        _pos_mode_cache = "hedge"
        _pos_mode_detected_at = now - (_POS_MODE_CACHE_TTL - 60)
        return "hedge"

    # -- Set margin mode -------------------------------------------------------

    async def set_margin_mode(self):
        logger.debug(
            "[%s] set_margin_mode: UA v3 -- margin mode '%s' se aplica "
            "por orden via campo marginMode en place-order (no requiere llamada separada)",
            self.symbol, self.margin_mode
        )

    # -- Inicializacion --------------------------------------------------------

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
            self._open_notional = saved.get("usdt_amount", 0.0)
            self._open_leverage = saved.get("leverage", self.leverage)
            self._protection_ok = True
            logger.info("[%s] Posicion restaurada: %s @ %s", self.symbol, self.position, self.entry_price)

        if not balance_svc.is_ready():
            logger.warning("[%s] balance_svc no listo -- init desde trader", self.symbol)
            balance_svc.init(self._api_key, self._api_secret, self._passphrase)

        self._api_version = "ua"
        detected_mode = await self._detect_pos_mode()
        self._ua_pos_mode = detected_mode
        logger.info("[%s] Modo cuenta Unified Account v3: %s", self.symbol, detected_mode)

        await self.set_margin_mode()

    # -- Precio, OHLCV y balance ----------------------------------------------

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
                    return bars
        except Exception as e:
            logger.debug("[%s] get_ohlcv WS error: %s", self.symbol, e)

        tf_ccxt = {"15m": "15m", "1h": "1h", "4h": "4h"}.get(tf, tf)
        bars = await self.exchange.fetch_ohlcv(self.symbol, tf_ccxt, limit=OHLCV_LIMIT)
        return bars

    async def get_balance(self) -> float | None:
        return await balance_svc.get()

    # -- Leverage --------------------------------------------------------------

    async def set_leverage(self, leverage: int, side: str | None = None):
        sym = self._sym()
        payload = {
            "symbol":      sym,
            "productType": "USDT-FUTURES",
            "marginCoin":  "USDT",
            "leverage":    str(leverage),
        }
        if self._ua_pos_mode == "hedge" and side:
            payload["holdSide"] = side
        try:
            r = await self._http_post("/api/v2/mix/account/set-leverage", payload)
            code = r.get("code", "")
            if code == "00000":
                logger.debug("[%s] Leverage %sx OK", self.symbol, leverage)
            elif code == "40085":
                logger.debug("[%s] set_leverage UA-skip (40085) -- lev gestionado por cuenta", self.symbol)
            else:
                logger.warning("[%s] set_leverage error inesperado: %s", self.symbol, r)
        except Exception as e:
            logger.debug("[%s] set_leverage exception (ignorado): %s", self.symbol, e)

    # -- Minimos de qty --------------------------------------------------------

    async def _get_min_qty(self) -> float:
        sym = self._sym()
        if sym in _min_qty_cache:
            return _min_qty_cache[sym]
        try:
            r = await self._http_get(
                "/api/v2/mix/market/contracts",
                {"symbol": sym, "productType": "USDT-FUTURES"}
            )
            if r.get("code") == "00000":
                items = r.get("data") or []
                items = items if isinstance(items, list) else []
                if items:
                    min_qty = float(
                        items[0].get("minTradeNum") or
                        items[0].get("minOrderSize") or 0.001
                    )
                    _min_qty_cache[sym] = min_qty
                    return min_qty
        except Exception as e:
            logger.debug("[%s] _get_min_qty error: %s", self.symbol, e)
        fallback = _MIN_QTY_FALLBACK.get(sym, 0.001)
        _min_qty_cache[sym] = fallback
        return fallback

    # -- Posiciones abiertas ---------------------------------------------------

    async def _get_positions(self) -> list | None:
        """
        Devuelve lista de posiciones abiertas para este simbolo.

        En UA v3 hedge mode los endpoints v2 devuelven lista vacia.
        Se intenta primero /api/v3/position/current-position,
        luego los fallbacks v2 para compatibilidad con cuentas clasicas.
        """
        sym = self._sym()
        is_hedge = (self._ua_pos_mode == "hedge")

        # 1. UA v3: /api/v3/position/current-position (funciona en hedge mode)
        try:
            params: dict = {"category": "USDT-FUTURES", "symbol": sym}
            if is_hedge and self.position:
                params["holdSide"] = self.position  # "long" | "short"
            r = await self._http_get("/api/v3/position/current-position", params)
            if r.get("code") == "00000":
                raw = r.get("data", {})
                items = raw.get("list") if isinstance(raw, dict) else raw
                if not isinstance(items, list):
                    items = []
                positions = [
                    p for p in items
                    if isinstance(p, dict)
                    and float(p.get("size") or p.get("total", 0)) > 0
                ]
                if positions:
                    logger.debug(
                        "[%s] _get_positions v3: %d posicion(es) encontradas",
                        self.symbol, len(positions),
                    )
                    return positions
                return []
        except Exception as e:
            logger.debug("[%s] _get_positions v3 error: %s", self.symbol, e)

        # 2. Classic v2 single-position
        try:
            r = await self._http_get(
                "/api/v2/mix/position/single-position",
                {"symbol": sym, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and float(p.get("total") or p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug("[%s] positions single error: %s", self.symbol, e)

        # 3. Classic v2 all-position
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
                    and p.get("symbol") == sym
                    and float(p.get("total") or p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug("[%s] all-positions error: %s", self.symbol, e)

        logger.warning("[%s] _get_positions fallo", self.symbol)
        return None

    # -- TPSL server-side (Unified Account v3) ---------------------------------
    # ENDPOINT: /api/v3/trade/place-tpsl-order
    # Documentacion Bitget v3 (Unified Account):
    # https://www.bitget.com/api-doc/contract/plan/Place-Tpsl-Order
    #
    # Campos clave UA v3:
    #   category        : "USDT-FUTURES"
    #   symbol          : "INJUSDT" (sin slash)
    #   tradeSide       : "buy" | "sell" (direccion de cierre)
    #   planType        : "pos_loss" (SL) | "pos_profit" (TP)
    #   triggerPrice    : precio de activacion (string)
    #   triggerType     : "mark_price" | "last_price"
    #   orderType       : "market" (executePrice=0) | "limit"
    #   posSide         : "long" | "short" (solo en hedge mode)
    #
    # NOTA: /api/v2/mix/order/place-tpsl-order da error 40085 en UA v3.
    # NOTA: En one_way mode NO se envia posSide.

    async def _place_pos_tpsl(self, sl: float | None = None, tp: float | None = None) -> dict:
        if not self.position:
            return {"code": "NO_POSITION", "msg": "No hay posicion abierta"}
        if not sl and not tp:
            return {"code": "NO_TPSL", "msg": "Sin niveles de proteccion"}

        sym = self._sym()
        is_long = (self.position == "long")
        # Al cerrar: long se cierra con sell, short con buy
        close_trade_side = "sell" if is_long else "buy"
        is_hedge = (self._ua_pos_mode == "hedge")

        if self.dry_run:
            logger.info("[%s] DRY RUN TPSL UA v3: sl=%s tp=%s posSide=%s mode=%s",
                        self.symbol, sl, tp,
                        self.position if is_hedge else "(omitido one_way)",
                        self._ua_pos_mode)
            self.sl_order_id = "dry-sl"
            self.tp_order_id = "dry-tp"
            return {"code": "00000", "data": {"orderId": "dry-tpsl"}}

        results = {}
        sl_placed = False
        tp_placed = False

        # -- SL ----------------------------------------------------------------
        if sl:
            payload_sl: dict = {
                "category":     "USDT-FUTURES",
                "symbol":       sym,
                "tradeSide":    close_trade_side,
                "planType":     "pos_loss",
                "triggerPrice": str(sl),
                "triggerType":  "mark_price",
                "orderType":    "market",
            }
            # posSide solo en hedge mode
            if is_hedge:
                payload_sl["posSide"] = self.position  # "long" | "short"
            try:
                r_sl = await self._http_post("/api/v3/trade/place-tpsl-order", payload_sl)
                results["sl"] = r_sl
                if r_sl.get("code") == "00000":
                    data = r_sl.get("data") or {}
                    self.sl_order_id = (
                        data.get("orderId") or data.get("clientOid") if isinstance(data, dict) else None
                    )
                    sl_placed = True
                    logger.info("[%s] TPSL SL OK (UA v3) -- orderId=%s mode=%s",
                                self.symbol, self.sl_order_id, self._ua_pos_mode)
                else:
                    logger.error("[%s] TPSL SL FAILED (UA v3) mode=%s payload=%s resp=%s",
                                 self.symbol, self._ua_pos_mode, payload_sl, r_sl)
            except Exception as e:
                logger.error("[%s] _place_pos_tpsl SL exception: %s", self.symbol, e)
                results["sl"] = {"code": "ERROR", "msg": str(e)}

        # -- TP ----------------------------------------------------------------
        if tp:
            payload_tp: dict = {
                "category":     "USDT-FUTURES",
                "symbol":       sym,
                "tradeSide":    close_trade_side,
                "planType":     "pos_profit",
                "triggerPrice": str(tp),
                "triggerType":  "mark_price",
                "orderType":    "market",
            }
            # posSide solo en hedge mode
            if is_hedge:
                payload_tp["posSide"] = self.position  # "long" | "short"
            try:
                r_tp = await self._http_post("/api/v3/trade/place-tpsl-order", payload_tp)
                results["tp"] = r_tp
                if r_tp.get("code") == "00000":
                    data = r_tp.get("data") or {}
                    self.tp_order_id = (
                        data.get("orderId") or data.get("clientOid") if isinstance(data, dict) else None
                    )
                    tp_placed = True
                    logger.info("[%s] TPSL TP OK (UA v3) -- orderId=%s mode=%s",
                                self.symbol, self.tp_order_id, self._ua_pos_mode)
                else:
                    logger.error("[%s] TPSL TP FAILED (UA v3) mode=%s payload=%s resp=%s",
                                 self.symbol, self._ua_pos_mode, payload_tp, r_tp)
            except Exception as e:
                logger.error("[%s] _place_pos_tpsl TP exception: %s", self.symbol, e)
                results["tp"] = {"code": "ERROR", "msg": str(e)}

        # -- Resultado: exito solo si el SL fue colocado realmente -------------
        sl_requested = sl is not None and sl != 0
        if sl_requested:
            if sl_placed:
                return {"code": "00000", "data": results}
            else:
                err = results.get("sl", {"code": "ERROR", "msg": "SL no colocado"})
                logger.error(
                    "[%s] _place_pos_tpsl: SL requerido pero NO colocado -- "
                    "posicion SIN proteccion en exchange. resp=%s",
                    self.symbol, err,
                )
                return err
        else:
            if tp_placed or not tp:
                return {"code": "00000", "data": results}
            else:
                return results.get("tp", {"code": "ERROR", "msg": "TP no colocado"})

    async def reconcile_position(self) -> bool:
        try:
            positions = await self._get_positions()
            has_pos   = bool(positions)
            sl_covered = bool(self.sl_order_id) or (self.sl is None)
            tp_covered = bool(self.tp_order_id) or (self.tp3 is None)
            self._protection_ok = has_pos and sl_covered and tp_covered

            if not has_pos:
                logger.error("[%s] Reconcile: posicion no encontrada en exchange", self.symbol)
                await kill_switch.on_state_mismatch(self.symbol)
            elif not (sl_covered and tp_covered):
                logger.error(
                    "[%s] Reconcile: faltan ordenes TPSL (sl_ok=%s tp_ok=%s)",
                    self.symbol, sl_covered, tp_covered
                )
            else:
                logger.info("[%s] Reconcile OK", self.symbol)

            return self._protection_ok
        except Exception as e:
            self._protection_ok = False
            logger.error("[%s] reconcile_position error: %s", self.symbol, e)
            return False

    # -- Ordenes Unified Account v3 -------------------------------------------
    # ENDPOINT: /api/v3/trade/place-order

    async def _place_order_raw(
        self,
        side: str,
        qty: float,
        order_type: str = "market",
        price: float | None = None,
        reduce_only: bool = False,
        sl: float | None = None,
        tp: float | None = None,
        trade_side: str = "open",
        pos_side: str | None = None,
    ) -> dict:
        sym = self._sym()

        if self._ua_pos_mode is None:
            self._ua_pos_mode = await self._detect_pos_mode()

        is_hedge = self._ua_pos_mode == "hedge"
        bg_margin_mode = "isolated" if self.margin_mode == "isolated" else "crossed"

        payload: dict = {
            "category":   "USDT-FUTURES",
            "symbol":     sym,
            "qty":        str(qty),
            "side":       side,
            "orderType":  order_type,
            "marginMode": bg_margin_mode,
            "reduceOnly": "YES" if reduce_only else "NO",
        }

        if is_hedge:
            if pos_side:
                payload["posSide"] = pos_side
            else:
                if not reduce_only:
                    payload["posSide"] = "long" if side == "buy" else "short"
                else:
                    payload["posSide"] = "long" if side == "sell" else "short"

        if order_type == "limit":
            payload["timeInForce"] = "gtc"
            if price is not None:
                payload["price"] = str(price)

        if self.dry_run:
            logger.info(
                "[%s] DRY RUN RAW: %s mode=%s marginMode=%s reduceOnly=%s posSide=%s %s qty=%s price=%s",
                self.symbol, side, self._ua_pos_mode, bg_margin_mode, reduce_only,
                payload.get("posSide"), order_type, qty, price
            )
            return {"code": "00000", "data": {"orderId": "dry"}}

        try:
            return await self._http_post("/api/v3/trade/place-order", payload)
        except Exception as e:
            logger.error("[%s] _place_order_raw exception: %s", self.symbol, e)
            return {"code": "ERROR", "msg": str(e)}

    async def _get_order_status(self, order_id: str) -> dict:
        sym = self._sym()
        try:
            r = await self._http_get(
                "/api/v3/trade/order-info",
                {
                    "category": "USDT-FUTURES",
                    "symbol":   sym,
                    "orderId":  order_id,
                },
            )
            if r.get("code") == "00000":
                data = r.get("data") or {}
                if isinstance(data, dict) and "orderStatus" in data:
                    data["state"] = data["orderStatus"]
                    r["data"] = data
            return r
        except Exception as e:
            logger.debug("[%s] _get_order_status error: %s", self.symbol, e)
            return {}

    async def _cancel_order(self, order_id: str) -> dict:
        sym = self._sym()
        try:
            return await self._http_post(
                "/api/v3/trade/cancel-order",
                {
                    "category": "USDT-FUTURES",
                    "symbol":   sym,
                    "orderId":  order_id,
                },
            )
        except Exception as e:
            logger.debug("[%s] _cancel_order error: %s", self.symbol, e)
            return {}

    async def _modify_order(
        self,
        order_id: str,
        qty: float | None = None,
        price: float | None = None,
        auto_cancel: bool = False,
    ) -> dict:
        sym = self._sym()
        payload: dict = {
            "category":  "USDT-FUTURES",
            "symbol":    sym,
            "orderId":   order_id,
            "autoCancel": "yes" if auto_cancel else "no",
        }
        if qty is not None:
            payload["qty"] = str(qty)
        if price is not None:
            payload["price"] = str(price)

        if self.dry_run:
            logger.info(
                "[%s] DRY RUN modify-order: orderId=%s qty=%s price=%s",
                self.symbol, order_id, qty, price,
            )
            return {"code": "00000", "data": {"orderId": order_id}}

        try:
            return await self._http_post("/api/v3/trade/modify-order", payload)
        except Exception as e:
            logger.debug("[%s] _modify_order error: %s", self.symbol, e)
            return {"code": "ERROR", "msg": str(e)}

    async def _place_order(
        self,
        side: str,
        qty: float,
        reduce_only: bool = False,
        sl: float | None = None,
        tp: float | None = None,
    ) -> dict:
        now = time.time()
        if self._cb_40085_paused_until > now:
            remaining = int(self._cb_40085_paused_until - now)
            logger.warning(
                "[%s] Circuit breaker 40085 activo -- pausado %ss mas",
                self.symbol, remaining
            )
            return {"code": "40085", "msg": "circuit_breaker_paused"}

        try:
            arrival_price = await self.get_price()
        except Exception:
            arrival_price = 0.0

        ask = bid = None
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
            from bot.ws_feed import ws_feed
            ob = ws_feed.get_orderbook_metrics(sym_clean)
            if ob:
                ask = ob.get("ask")
                bid = ob.get("bid")
        except Exception:
            pass

        trade_side = "close" if reduce_only else "open"

        r = await execution_engine.execute(
            trader=self,
            side=side,
            qty=qty,
            arrival_price=arrival_price,
            ask=ask,
            bid=bid,
            trade_side=trade_side,
            reduce_only=reduce_only,
            sl=sl,
            tp=tp,
        )

        rejected  = r.get("code") != "00000"
        err_code  = r.get("code", "")

        if err_code == "25236":
            global _pos_mode_cache, _pos_mode_detected_at
            logger.error(
                "[%s] Error 25236 (Incorrect position open type) -- "
                "invalidando cache pos_mode y re-detectando en proxima orden. "
                "Verifica si la cuenta tiene hedge-mode o one-way mode en Bitget.",
                self.symbol
            )
            _pos_mode_cache = None
            _pos_mode_detected_at = 0.0
            self._ua_pos_mode = None

        if err_code == "40085":
            self._cb_40085_count += 1
            logger.error(
                "[%s] Error 40085 en place-order (%s/%s): "
                "UA single_hold side incorrecto o API key sin permisos UA.",
                self.symbol, self._cb_40085_count, _CB_40085_THRESHOLD
            )
            if self._cb_40085_count >= _CB_40085_THRESHOLD:
                self._cb_40085_paused_until = time.time() + _CB_40085_PAUSE_S
                self._cb_40085_count = 0
                logger.critical(
                    "[%s] Circuit breaker 40085 activado -- "
                    "simbolo pausado %ss. "
                    "Verifica que la API key tenga permisos de trading en Unified Account.",
                    self.symbol, _CB_40085_PAUSE_S
                )
                try:
                    from bot.telegram_bot import send_message
                    await send_message(
                        "\U0001f6a8 <b>Circuit Breaker 40085</b>\n"
                        f"Par: <code>{self.symbol}</code>\n"
                        "Bitget rechaza la orden en Unified Account.\n"
                        f"El simbolo se pausa {_CB_40085_PAUSE_S // 60} min.\n"
                        "<b>Accion requerida:</b> Verifica que la API key tenga permisos "
                        "de Futures Trading en Unified Account."
                    )
                except Exception:
                    pass
        else:
            await kill_switch.on_order_result(rejected=rejected)
            if not rejected:
                balance_svc.invalidate()
                self._cb_40085_count = 0
            else:
                logger.error("[%s] Order failed: %s", self.symbol, r)
        return r

    async def _calc_qty(self, usdt_amount: float, price: float, leverage: int) -> float:
        effective_lev = leverage or self.leverage
        raw_qty = (usdt_amount * effective_lev) / price
        min_qty = await self._get_min_qty()
        qty = max(min_qty, round(raw_qty / min_qty) * min_qty)
        decimals = len(str(min_qty).rstrip("0").split(".")[-1]) if "." in str(min_qty) else 0
        qty = round(qty, decimals)
        return qty

    # -- Abrir posiciones ------------------------------------------------------

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None, leverage=None):
        if kill_switch.is_halted(self.symbol):
            logger.warning("[%s] open_long bloqueado por KillSwitch L%s", self.symbol, kill_switch.level())
            return

        price  = await self.get_price()
        lev    = leverage or self.leverage
        qty    = await self._calc_qty(usdt_amount, price, lev)
        balance = await self.get_balance() or 0.0

        ok, reason = await pretrade_risk.check(
            symbol=self.symbol, side="buy", notional=usdt_amount,
            price=price, balance=balance, sl=sl,
        )
        if not ok:
            logger.warning("[%s] open_long bloqueado por PreTradeRisk: %s", self.symbol, reason)
            return

        await self.set_leverage(lev, side="long")
        r = await self._place_order("buy", qty, reduce_only=False)
        if r.get("code") == "00000":
            self.position    = "long"
            self.entry_price = price
            self.sl   = sl
            self.tp1  = tp1
            self.tp2  = tp2
            self.tp3  = tp3
            self.tp2_hit        = False
            self._open_notional = usdt_amount
            self._open_leverage = lev
            save_position(
                self.symbol, "long", price,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                usdt_amount=usdt_amount, leverage=lev,
            )
            logger.warning("[%s] LONG @ %.4f lev=%sx", self.symbol, price, lev)
            await notify_open(self.symbol, "long", price, lev, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run)
            tpsl_r = await self._place_pos_tpsl(sl=sl, tp=tp3)
            if tpsl_r.get("code") == "00000":
                self._protection_ok = True
                logger.info("[%s] Proteccion TPSL confirmada (SL+TP en exchange)", self.symbol)
            else:
                self._protection_ok = False
                logger.error(
                    "[%s] ADVERTENCIA: posicion abierta SIN TPSL en exchange. "
                    "El bot usara el loop de precio como fallback (cada %ss). resp=%s",
                    self.symbol, os.getenv("LOOP_SLEEP", "10"), tpsl_r,
                )
            ok2 = await self.reconcile_position()
            if not ok2:
                logger.error("[%s] Posicion abierta pero sin proteccion confirmada", self.symbol)
        else:
            logger.error("[%s] open_long FAILED: %s", self.symbol, r)

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None, leverage=None):
        if kill_switch.is_halted(self.symbol):
            logger.warning("[%s] open_short bloqueado por KillSwitch L%s", self.symbol, kill_switch.level())
            return

        price  = await self.get_price()
        lev    = leverage or self.leverage
        qty    = await self._calc_qty(usdt_amount, price, lev)
        balance = await self.get_balance() or 0.0

        ok, reason = await pretrade_risk.check(
            symbol=self.symbol, side="sell", notional=usdt_amount,
            price=price, balance=balance, sl=sl,
        )
        if not ok:
            logger.warning("[%s] open_short bloqueado por PreTradeRisk: %s", self.symbol, reason)
            return

        await self.set_leverage(lev, side="short")
        r = await self._place_order("sell", qty, reduce_only=False)
        if r.get("code") == "00000":
            self.position    = "short"
            self.entry_price = price
            self.sl   = sl
            self.tp1  = tp1
            self.tp2  = tp2
            self.tp3  = tp3
            self.tp2_hit        = False
            self._open_notional = usdt_amount
            self._open_leverage = lev
            save_position(
                self.symbol, "short", price,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                usdt_amount=usdt_amount, leverage=lev,
            )
            logger.warning("[%s] SHORT @ %.4f lev=%sx", self.symbol, price, lev)
            await notify_open(self.symbol, "short", price, lev, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run)
            tpsl_r = await self._place_pos_tpsl(sl=sl, tp=tp3)
            if tpsl_r.get("code") == "00000":
                self._protection_ok = True
                logger.info("[%s] Proteccion TPSL confirmada (SL+TP en exchange)", self.symbol)
            else:
                self._protection_ok = False
                logger.error(
                    "[%s] ADVERTENCIA: posicion abierta SIN TPSL en exchange. "
                    "El bot usara el loop de precio como fallback (cada %ss). resp=%s",
                    self.symbol, os.getenv("LOOP_SLEEP", "10"), tpsl_r,
                )
            ok2 = await self.reconcile_position()
            if not ok2:
                logger.error("[%s] Posicion abierta pero sin proteccion confirmada", self.symbol)
        else:
            logger.error("[%s] open_short FAILED: %s", self.symbol, r)

    async def close_position(self, reason: str = ""):
        if not self.position:
            return
        side = "sell" if self.position == "long" else "buy"
        qty  = None
        try:
            positions = await self._get_positions()
            if positions:
                qty = float(positions[0].get("total") or positions[0].get("size", 0))
        except Exception:
            pass

        if not qty or qty <= 0:
            qty = 0

        exit_price = await self.get_price()
        pnl = 0.0
        if self.entry_price and exit_price:
            if self.position == "long":
                pnl = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                pnl = (self.entry_price - exit_price) / self.entry_price * 100

        if qty > 0:
            r = await self._place_order(side, qty, reduce_only=True)
            if r.get("code") != "00000":
                logger.error("[%s] close_position FAILED: %s", self.symbol, r)
                return

        old_pos = self.position
        pretrade_risk.register_close(self.symbol, self._open_notional)
        self._open_notional = 0.0
        self._open_leverage = 1

        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit      = False
        self.sl_order_id  = None
        self.tp_order_id  = None
        self._protection_ok = False
        clear_position(self.symbol)

        if pnl >= 0:
            self.win_count += 1
        self.trade_count += 1
        self.total_pnl   += pnl

        await kill_switch.on_trade_result(pnl)

        logger.warning("[%s] %s cerrado | razon=%s | pnl=%+.2f%%", self.symbol, old_pos.upper(), reason, pnl)
        await notify_close(self.symbol, old_pos, exit_price, pnl, reason=reason, dry_run=self.dry_run)

    async def partial_close(self, ratio: float = 0.5):
        if not self.position:
            return
        side = "sell" if self.position == "long" else "buy"
        qty  = None
        try:
            positions = await self._get_positions()
            if positions:
                total   = float(positions[0].get("total") or positions[0].get("size", 0))
                min_qty = await self._get_min_qty()
                qty     = max(min_qty, round((total * ratio) / min_qty) * min_qty)
        except Exception as e:
            logger.warning("[%s] partial_close: %s", self.symbol, e)
            return

        if not qty or qty <= 0:
            return

        r = await self._place_order(side, qty, reduce_only=True)
        if r.get("code") == "00000":
            freed = self._open_notional * ratio
            pretrade_risk.register_close(self.symbol, freed)
            self._open_notional = max(0.0, self._open_notional - freed)

            mark_tp2_hit(self.symbol)
            self.tp2_hit = True
            exit_price   = await self.get_price()
            await notify_tp_partial(self.symbol, self.position, exit_price, ratio=ratio, dry_run=self.dry_run)
            logger.info("[%s] Cierre parcial %s%%", self.symbol, int(ratio * 100))
        else:
            logger.warning("[%s] partial_close FAILED: %s", self.symbol, r)

    # -- Loop principal --------------------------------------------------------

    async def run(self, risk: "RiskManager", global_risk: "GlobalRisk" = None):
        from bot.risk import RiskManager
        usdt_per_trade = risk.usdt_per_trade
        await self._init(usdt_per_trade)

        async def _ai_decide_fn(symbol: str, context: dict) -> str:
            result = await ai_decide(
                symbol=symbol,
                bars=None,
                position=self.position,
                entry_price=self.entry_price,
                leverage=self.leverage,
                context_override=context,
            )
            return result.get("action", "HOLD")

        while True:
            try:
                if kill_switch.is_hard_killed():
                    logger.critical("[%s] KillSwitch HARD -- bot detenido permanentemente", self.symbol)
                    return

                if kill_switch.is_halted(self.symbol):
                    logger.warning("[%s] KillSwitch activo -- esperando %ss", self.symbol,
                                   int(os.getenv("LOOP_SLEEP", "10")))
                    await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))
                    continue

                price = await self.get_price()
                bars  = await self.get_ohlcv()

                if len(bars) < OHLCV_MIN_BARS:
                    logger.warning("[%s] Barras insuficientes (%s < %s)", self.symbol, len(bars), OHLCV_MIN_BARS)
                    await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))
                    continue

                # -- Gestion de posicion abierta --------------------------------
                if self.position:
                    sl  = self.sl
                    tp1 = self.tp1
                    tp2 = self.tp2
                    tp3 = self.tp3

                    # TP2 parcial
                    if tp2 and not self.tp2_hit:
                        if (self.position == "long"  and price >= tp2) or \
                           (self.position == "short" and price <= tp2):
                            logger.info("[%s] TP2 alcanzado -- cierre parcial %.0f%%",
                                        self.symbol, TP2_PARTIAL_RATIO * 100)
                            await self.partial_close(ratio=TP2_PARTIAL_RATIO)

                    # Fallback SL/TP en loop (si no hay TPSL en exchange)
                    if not self._protection_ok:
                        close_reason = None
                        if sl:
                            if (self.position == "long"  and price <= sl) or \
                               (self.position == "short" and price >= sl):
                                close_reason = "SL"
                        if tp3 and not close_reason:
                            if (self.position == "long"  and price >= tp3) or \
                               (self.position == "short" and price <= tp3):
                                close_reason = "TP3"
                        if close_reason:
                            await self.close_position(reason=close_reason)
                            await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))
                            continue

                    # Senal de cierre por estrategia (nueva firma: exch, symbol, ai_fn, has_open)
                    result = await decide(
                        self.exchange,
                        self.symbol,
                        _ai_decide_fn,
                        has_open_position=True,
                    )
                    action = result.get("action", "HOLD")
                    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
                        await self.close_position(reason="strategy")

                # -- Sin posicion: buscar entrada --------------------------------
                else:
                    balance = await self.get_balance() or 0.0

                    # Nueva firma de decide: exch, symbol, ai_fn, has_open_position
                    result = await decide(
                        self.exchange,
                        self.symbol,
                        _ai_decide_fn,
                        has_open_position=False,
                    )
                    action = result.get("action", "HOLD")
                    signal = result.get("signal")

                    usdt = risk.usdt_per_trade
                    lev  = self.leverage

                    if signal:
                        sl  = signal.sl
                        tp1 = signal.tp1
                        tp2 = signal.tp2
                        atr = signal.atr
                        tp3 = round(signal.entry + 3.0 * atr, 6) if atr and signal.entry else None
                    else:
                        sl = tp1 = tp2 = tp3 = atr = None

                    if action == "BUY":
                        await self.open_long(usdt, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)

                    elif action == "SELL":
                        await self.open_short(usdt, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)

            except asyncio.CancelledError:
                logger.info("[%s] Loop cancelado", self.symbol)
                return
            except Exception as e:
                logger.error("[%s] Loop error: %s", self.symbol, e, exc_info=True)

            await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))


# -- Helpers fuera de clase ----------------------------------------------------

def _calc_atr(bars: list, period: int = 14) -> float | None:
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        high  = bars[i][2]
        low   = bars[i][3]
        close_prev = bars[i - 1][4]
        trs.append(max(high - low, abs(high - close_prev), abs(low - close_prev)))
    atr = sum(trs[-period:]) / period
    return atr if atr > 0 else None
