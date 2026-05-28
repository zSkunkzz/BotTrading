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
}

# Cache de min_qty leídos desde API (sym → float)
_min_qty_cache: dict = {}

# ─────────────────────────────────────────────────────────────
# CACHÉ GLOBAL DE BALANCE (compartido por todos los traders)
# TTL solo se actualiza cuando la llamada HTTP TIENE ÉXITO.
# Si falla, se reintenta en el próximo ciclo sin bloquear.
# ─────────────────────────────────────────────────────────────
_BALANCE_CACHE_TTL  = int(os.getenv("BALANCE_CACHE_TTL", "30"))   # segundos
_balance_cache_value: float | None = None   # None = nunca obtenido con éxito
_balance_cache_ts:    float = 0.0           # solo se actualiza en éxito
_balance_fetch_lock:  asyncio.Lock = None   # lazy-init


def _get_balance_lock() -> asyncio.Lock:
    global _balance_fetch_lock
    if _balance_fetch_lock is None:
        _balance_fetch_lock = asyncio.Lock()
    return _balance_fetch_lock


async def _safe_json(response) -> dict:
    """
    Parsea r.json() de forma segura.
    - Fuerza content_type=None para evitar error cuando Bitget devuelve
      text/html o text/plain en errores HTTP.
    - Si el resultado no es un dict, lanza ValueError con el contenido.
    """
    data = await response.json(content_type=None)
    if not isinstance(data, dict):
        raise ValueError(f"Respuesta no-JSON (tipo {type(data).__name__}): {str(data)[:300]}")
    return data


async def _fetch_balance_once(api_key, api_secret, passphrase) -> float | None:
    """
    Llama a la API de Bitget y devuelve el balance USDT disponible.
    Actualiza el caché global SOLO si la llamada tiene éxito.

    Retorna:
      - float  : balance real obtenido de la API
      - None   : los 3 endpoints fallaron — el caller debe tratar como 'desconocido'

    NUNCA devuelve 0.0 para indicar fallo (0.0 significa cuenta vacía real).
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

    # ─────────────────────────────────────────────────────────────
    # ENDPOINT 1: v3/account/assets — Unified Account
    # BUG FIX: "available" puede ser "0" (string) en UA aunque haya fondos.
    # Probamos en orden: available → crossMaxAvailable → usdtEquity
    # Usamos conversión segura: _to_float() evita que "0" truthy-falsy engañe.
    # ─────────────────────────────────────────────────────────────
    def _to_float(val) -> float | None:
        """Convierte val a float; devuelve None si es None/vacío, float si es número."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    try:
        path = "/api/v3/account/assets"
        qs   = "?coin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        logger.debug(f"[BalanceCache] v3 raw response: {data}")
        if data.get("code") == "00000":
            for item in (data.get("data") or []):
                if item.get("coin") == "USDT":
                    # Probar campos en orden de prioridad para Unified Account
                    bal = None
                    for field in ("available", "crossMaxAvailable", "usdtEquity",
                                  "isolatedMaxAvailable", "equity"):
                        v = _to_float(item.get(field))
                        if v is not None and v > 0:
                            bal = v
                            logger.debug(f"[BalanceCache] v3 campo usado: {field}={v}")
                            break
                    # Si todos son 0.0 explícitamente → cuenta vacía real
                    if bal is None:
                        bal = _to_float(item.get("available")) or 0.0
                    _balance_cache_value = bal
                    _balance_cache_ts    = time.monotonic()
                    logger.info(f"[BalanceCache] ✅ Balance USDT (v3): {bal:.2f}")
                    return bal
            logger.warning("[BalanceCache] ⚠️ v3 OK pero sin ítem USDT en data")
        else:
            logger.warning(
                f"[BalanceCache] ⚠️ v3 code={data.get('code')} msg={data.get('msg')}"
            )
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v3 respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v3 excepción: {e}")

    # ─────────────────────────────────────────────────────────────
    # ENDPOINT 2: v2/mix/account/account (singular) — UA + Classic
    # Este endpoint devuelve el balance real de la cuenta de futuros USDT.
    # Funciona en Unified Account cuando v3/assets devuelve available=0.
    # ─────────────────────────────────────────────────────────────
    try:
        path = "/api/v2/mix/account/account"
        qs   = "?symbol=USDTUSDT&productType=USDT-FUTURES&marginCoin=USDT"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        logger.debug(f"[BalanceCache] v2-single raw response: {data}")
        if data.get("code") == "00000":
            d = data.get("data") or {}
            for field in ("available", "crossMaxAvailable", "usdtEquity", "equity"):
                v = _to_float(d.get(field))
                if v is not None and v > 0:
                    _balance_cache_value = v
                    _balance_cache_ts    = time.monotonic()
                    logger.info(f"[BalanceCache] ✅ Balance USDT (v2-single/{field}): {v:.2f}")
                    return v
            # Todos los campos son 0 → cuenta de futuros vacía real
            bal = _to_float(d.get("available")) or 0.0
            _balance_cache_value = bal
            _balance_cache_ts    = time.monotonic()
            logger.info(f"[BalanceCache] ✅ Balance USDT (v2-single vacía): {bal:.2f}")
            return bal
        else:
            code = data.get("code")
            msg  = data.get("msg")
            if code == "40085":
                logger.debug("[BalanceCache] v2-single: 40085 (esperado en algunas cuentas)")
            else:
                logger.warning(f"[BalanceCache] ⚠️ v2-single code={code} msg={msg}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2-single respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2-single excepción: {e}")

    # ─────────────────────────────────────────────────────────────
    # ENDPOINT 3: v2/mix/account/accounts (plural) — Classic fallback
    # ─────────────────────────────────────────────────────────────
    try:
        path = "/api/v2/mix/account/accounts"
        qs   = "?productType=USDT-FUTURES"
        url  = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=_headers("GET", path + qs),
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await _safe_json(r)
        logger.debug(f"[BalanceCache] v2-multi raw response: {data}")
        if data.get("code") == "00000":
            items = data.get("data") or []
            if items:
                d = items[0]
                for field in ("available", "crossMaxAvailable", "usdtEquity"):
                    v = _to_float(d.get(field))
                    if v is not None and v > 0:
                        _balance_cache_value = v
                        _balance_cache_ts    = time.monotonic()
                        logger.info(f"[BalanceCache] ✅ Balance USDT (v2-multi/{field}): {v:.2f}")
                        return v
                bal = _to_float(d.get("available")) or 0.0
                _balance_cache_value = bal
                _balance_cache_ts    = time.monotonic()
                logger.info(f"[BalanceCache] ✅ Balance USDT (v2-multi vacía): {bal:.2f}")
                return bal
        else:
            code = data.get("code")
            msg  = data.get("msg")
            if code == "40085":
                logger.debug("[BalanceCache] v2-multi: 40085 (Unified Account, esperado)")
            else:
                logger.warning(f"[BalanceCache] ⚠️ v2-multi code={code} msg={msg}")
    except ValueError as e:
        logger.warning(f"[BalanceCache] ⚠️ v2-multi respuesta inesperada: {e}")
    except Exception as e:
        logger.warning(f"[BalanceCache] ❌ v2-multi excepción: {e}")

    # ── Los 3 endpoints fallaron ──
    logger.error(
        "[BalanceCache] 🚨 Los 3 endpoints fallaron — balance NO actualizado. "
        f"Valor en caché: {_balance_cache_value}. "
        "Se reintentará en el próximo ciclo."
    )
    return None


async def get_cached_balance(api_key, api_secret, passphrase) -> float | None:
    """
    Devuelve el balance USDT desde caché global.
    Solo llama a la API si han pasado más de BALANCE_CACHE_TTL segundos
    desde la ÚLTIMA LLAMADA EXITOSA.

    Retorna:
      - float  : balance conocido (puede ser 0.0 si cuenta vacía)
      - None   : API nunca respondió con éxito o caché expiró y fallo nuevo

    El caller (trader.py) debe tratar None como 'balance desconocido',
    NO como 'balance = 0'.
    """
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
        if result is None:
            if _balance_cache_value is not None:
                logger.warning(
                    f"[BalanceCache] ⚠️ API falló, usando caché anterior: "
                    f"{_balance_cache_value:.2f} USDT"
                )
                return _balance_cache_value
            return None
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

    # ─────────────────────────────────────────────────────────────
    # HTTP HELPERS
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

    # ─────────────────────────────────────────────────────────────
    # INICIALIZACIÓN
    # ─────────────────────────────────────────────────────────────

    async def _init(self, usdt_per_trade: float):
        """
        Inicializa el exchange ccxt y restaura posición guardada si la hay.
        """
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
            logger.info(
                f"[{self.symbol}] 🔄 Posición restaurada: "
                f"{self.position} @ {self.entry_price}"
            )
        await self._detect_account_type()

    async def _detect_account_type(self):
        """
        Detecta si la cuenta es Unified Account (UA) o Classic.
        UA usa /api/v3/... para algunas operaciones.
        Classic usa /api/v2/mix/...

        En ambos casos, las órdenes van por /api/v2/mix/order/place-order.
        Lo que cambia es el endpoint para consultar posición y pos_mode.
        """
        # Intentar UA primero
        try:
            r = await self._http_get(
                "/api/v3/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                # Detectar pos_mode desde la respuesta UA
                data = r.get("data") or []
                if data:
                    self._ua_pos_mode = data[0].get("holdMode", "hedge")
                else:
                    # Sin posiciones abiertas: intentar leer pos_mode desde account config
                    try:
                        rc = await self._http_get(
                            "/api/v3/account/account",
                            {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
                        )
                        if rc.get("code") == "00000":
                            d = rc.get("data") or {}
                            self._ua_pos_mode = d.get("holdMode", "hedge")
                        else:
                            self._ua_pos_mode = "hedge"
                    except Exception:
                        self._ua_pos_mode = "hedge"
                logger.info(
                    f"[{self.symbol}] ✅ Unified Account detectada. "
                    f"pos_mode={self._ua_pos_mode}"
                )
                return
            # Codigo de error específico para UA no disponible — caemos a v2
        except Exception as e:
            logger.debug(f"[{self.symbol}] UA probe failed: {e}")

        # Fallback a Classic v2
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
            r = await self._http_get(
                "/api/v2/mix/account/account",
                {"symbol":      sym_clean,
                 "productType": "USDT-FUTURES",
                 "marginCoin":  "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "v2"
                d = r.get("data") or {}
                self._v2_pos_mode = d.get("holdMode", "hedge")
                logger.info(
                    f"[{self.symbol}] ✅ Classic Account detectada. "
                    f"pos_mode={self._v2_pos_mode}"
                )
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] v2 account probe failed: {e}")

        # Si ambos fallaron, asumir UA como default (más común)
        logger.warning(
            f"[{self.symbol}] ⚠️ No se pudo detectar tipo de cuenta. "
            "Asumiendo Unified Account."
        )
        self._api_version = "ua"
        self._ua_pos_mode = "hedge"

    # ─────────────────────────────────────────────────────────────
    # PRECIO Y BALANCE
    # ─────────────────────────────────────────────────────────────

    async def get_price(self) -> float:
        ticker = await self.exchange.fetch_ticker(self.symbol)
        return float(ticker["last"])

    async def get_balance(self) -> float | None:
        """
        Retorna el balance USDT desde caché global.
        El caller debe tratar None como 'balance desconocido', no como 0.
        """
        return await get_cached_balance(self._api_key, self._api_secret, self._passphrase)

    # ─────────────────────────────────────────────────────────────
    # LEVERAGE
    # ─────────────────────────────────────────────────────────────

    async def set_leverage(self, leverage: int, side: str | None = None):
        """
        Establece el apalancamiento.
        En modo hedge se llama dos veces (long/short).
        """
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        for hold_side in (["long", "short"] if self.margin_mode != "isolated" else [side or "long"]):
            try:
                payload = {
                    "symbol":      sym_clean,
                    "productType": "USDT-FUTURES",
                    "marginCoin":  "USDT",
                    "leverage":    str(leverage),
                    "holdSide":    hold_side,
                }
                r = await self._http_post("/api/v2/mix/account/set-leverage", payload)
                if r.get("code") == "00000":
                    logger.debug(f"[{self.symbol}] Leverage {leverage}x ({hold_side}) OK")
                else:
                    logger.warning(
                        f"[{self.symbol}] set_leverage {hold_side} "
                        f"code={r.get('code')} msg={r.get('msg')}"
                    )
            except Exception as e:
                logger.warning(f"[{self.symbol}] set_leverage error: {e}")

    # ─────────────────────────────────────────────────────────────
    # MÍNIMOS DE QTY
    # ─────────────────────────────────────────────────────────────

    async def _get_min_qty(self) -> float:
        """
        Obtiene el tamaño mínimo de contrato para el símbolo.
        Cachea el resultado para no llamar a la API en cada ciclo.
        """
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
                if items:
                    min_qty = float(items[0].get("minTradeNum") or
                                   items[0].get("minOrderSize") or 0.001)
                    _min_qty_cache[sym_clean] = min_qty
                    logger.debug(f"[{self.symbol}] min_qty API: {min_qty}")
                    return min_qty
        except Exception as e:
            logger.debug(f"[{self.symbol}] _get_min_qty error: {e}")

        fallback = _MIN_QTY_FALLBACK.get(sym_clean, 0.001)
        _min_qty_cache[sym_clean] = fallback
        logger.debug(f"[{self.symbol}] min_qty fallback: {fallback}")
        return fallback

    # ─────────────────────────────────────────────────────────────
    # POSICIONES ABIERTAS
    # ─────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        """
        Obtiene posiciones abiertas para este símbolo.
        Intenta UA primero, luego Classic v2.
        Retorna lista (puede ser vacía) o None si ambas APIs fallan.
        """
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        last_error = ""

        # Intento UA
        if self._api_version == "ua":
            try:
                r = await self._http_get(
                    "/api/v3/position/all-position",
                    {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
                )
                if r.get("code") == "00000":
                    data = r.get("data") or []
                    return [
                        p for p in data
                        if p.get("symbol") == sym_clean
                        and float(p.get("total") or p.get("contracts") or
                                  p.get("size", 0)) > 0
                    ]
                last_error = r.get("msg", "unknown UA error")
            except Exception as e:
                last_error = str(e)
                logger.debug(f"[{self.symbol}] UA positions error: {e}")

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
            last_error = r.get("msg", "unknown v2 error")
        except Exception as e:
            last_error = str(e)
            logger.debug(f"[{self.symbol}] v2 positions error: {e}")

        logger.warning(
            f"[{self.symbol}] ⚠️ _get_positions: ambos endpoints fallaron "
            f"({last_error}) — retornando None (estado local preservado)"
        )
        return None

    # ─────────────────────────────────────────────────────────────
    # COLOCAR / CERRAR ÓRDENES
    # FIX: Bitget Unified Account — el endpoint de órdenes es SIEMPRE
    # /api/v2/mix/order/place-order (tanto para UA como Classic).
    # /api/v3/mix/order/place-order NO existe → 40404.
    # ─────────────────────────────────────────────────────────────

    async def _place_order(self, side: str, trade_side: str, qty: float):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        # ── Determinar pos_mode ──
        # UA usa ua_pos_mode, Classic usa v2_pos_mode.
        # Ambos modos comparten el mismo endpoint v2 para órdenes.
        if self._api_version == "ua":
            pos_mode = self._ua_pos_mode or "hedge"
        else:
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
            logger.info(
                f"[{self.symbol}] 🟡 DRY RUN: {side}/{trade_side} qty={qty}"
            )
            return {"code": "00000", "data": {"orderId": "dry"}}

        payload = _build_payload(pos_mode)
        try:
            r = await self._http_post("/api/v2/mix/order/place-order", payload)
            if r.get("code") == "00000":
                return r
            # ── Retry en one-way si hedge falló por modo incorrecto ──
            if pos_mode == "hedge" and r.get("code") in ("40786", "40787", "40788"):
                logger.warning(
                    f"[{self.symbol}] Hedge order failed ({r.get('code')}), "
                    "retrying as one-way"
                )
                r2 = await self._http_post(
                    "/api/v2/mix/order/place-order",
                    _build_payload("one_way")
                )
                if r2.get("code") == "00000":
                    return r2
            logger.error(
                f"[{self.symbol}] Order failed: code={r.get('code')} msg={r.get('msg')}"
            )
            return r
        except Exception as e:
            logger.error(f"[{self.symbol}] _place_order exception: {e}")
            return {"code": "ERROR", "msg": str(e)}

    async def _calc_qty(self, usdt_amount: float, price: float,
                        leverage: int) -> float:
        """
        Calcula el número de contratos a abrir.
        qty = (usdt * leverage) / price, redondeado al mínimo permitido.
        """
        effective_lev = leverage or self.leverage
        raw_qty = (usdt_amount * effective_lev) / price
        min_qty = await self._get_min_qty()

        # Redondear hacia abajo al múltiplo de min_qty
        qty = max(min_qty, round(raw_qty / min_qty) * min_qty)

        # Precisión: determinar número de decimales del min_qty
        decimals = len(str(min_qty).rstrip("0").split(".")[-1]) if "." in str(min_qty) else 0
        qty = round(qty, decimals)

        logger.debug(
            f"[{self.symbol}] calc_qty: "
            f"(usdt={usdt_amount} × lev={effective_lev}x ÷ price={price}) "
            f"= raw {raw_qty:.6f} → qty={qty} (min={min_qty})"
        )
        return qty

    # ─────────────────────────────────────────────────────────────
    # ABRIR POSICIONES
    # ─────────────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                        leverage=None):
        price  = await self.get_price()
        lev    = leverage or self.leverage
        qty    = await self._calc_qty(usdt_amount, price, lev)
        await self.set_leverage(lev, side="long")
        r = await self._place_order("buy", "open", qty)
        if r.get("code") == "00000":
            self.position    = "long"
            self.entry_price = price
            self.sl  = sl
            self.tp1 = tp1
            self.tp2 = tp2
            self.tp3 = tp3
            self.tp2_hit = False
            save_position(self.symbol, "long", price, sl=sl,
                          tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(
                f"🟢 [{self.symbol}] LONG abierto @ {price:.4f} | "
                f"lev={lev}x | sl={sl} tp1={tp1} tp2={tp2} tp3={tp3}"
            )
            await notify_open(
                self.symbol, "long", price, lev,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                dry_run=self.dry_run
            )
        else:
            logger.error(
                f"[{self.symbol}] open_long FAILED: "
                f"code={r.get('code')} msg={r.get('msg')}"
            )

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                         leverage=None):
        price  = await self.get_price()
        lev    = leverage or self.leverage
        qty    = await self._calc_qty(usdt_amount, price, lev)
        await self.set_leverage(lev, side="short")
        r = await self._place_order("sell", "open", qty)
        if r.get("code") == "00000":
            self.position    = "short"
            self.entry_price = price
            self.sl  = sl
            self.tp1 = tp1
            self.tp2 = tp2
            self.tp3 = tp3
            self.tp2_hit = False
            save_position(self.symbol, "short", price, sl=sl,
                          tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(
                f"🔴 [{self.symbol}] SHORT abierto @ {price:.4f} | "
                f"lev={lev}x | sl={sl} tp1={tp1} tp2={tp2} tp3={tp3}"
            )
            await notify_open(
                self.symbol, "short", price, lev,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                dry_run=self.dry_run
            )
        else:
            logger.error(
                f"[{self.symbol}] open_short FAILED: "
                f"code={r.get('code')} msg={r.get('msg')}"
            )

    # ─────────────────────────────────────────────────────────────
    # CERRAR POSICIÓN
    # ─────────────────────────────────────────────────────────────

    async def close_position(self, reason: str = "Manual"):
        if not self.position:
            return {}
        price = await self.get_price()
        side  = "sell" if self.position == "long" else "buy"
        qty_pos = None
        positions = await self._get_positions()
        if positions:
            p = positions[0]
            qty_pos = float(
                p.get("total") or p.get("contracts") or p.get("size") or 0
            )
        if not qty_pos:
            qty_pos = await self._calc_qty(
                10.0, price, self.leverage
            )
        r = await self._place_order(side, "close", qty_pos)
        if r.get("code") != "00000" and not self.dry_run:
            logger.error(
                f"[{self.symbol}] close_position FAILED: "
                f"code={r.get('code')} msg={r.get('msg')}"
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
    # SL / TP PARCIAL
    # ─────────────────────────────────────────────────────────────

    async def _check_and_handle_sl_tp(self, risk, price: float) -> bool:
        """
        Comprueba SL/TP y cierra si es necesario.
        Retorna True si se cerró la posición.
        """
        if not self.position or not self.entry_price:
            return False

        # ── TP1 parcial ──
        if self.tp1 and not self.tp2_hit:
            if (self.position == "long"  and price >= self.tp1) or \
               (self.position == "short" and price <= self.tp1):
                logger.info(f"[{self.symbol}] 🎯 TP1 alcanzado @ {price}")
                # No cerrar aún, solo loguear

        # ── TP2 parcial ──
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

        # ── TP3 → cierre total ──
        if self.tp3:
            if (self.position == "long"  and price >= self.tp3) or \
               (self.position == "short" and price <= self.tp3):
                await self.close_position(f"TP3 @ {price:.4f}")
                risk.on_trade_close(0)
                return True

        # ── SL ──
        if self.sl:
            if (self.position == "long"  and price <= self.sl) or \
               (self.position == "short" and price >= self.sl):
                await self.close_position(f"SL @ {price:.4f}")
                risk.on_trade_close(0)
                return True

        # ── Risk Manager check ──
        should_exit, reason = risk.check_exit(price)
        if should_exit:
            result = await self.close_position(reason)
            risk.on_trade_close(result.get("pnl_pct", 0))
            return True

        return False

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
                    closed = await self._check_and_handle_sl_tp(risk, price)
                    if closed:
                        await asyncio.sleep(interval)
                        continue

                # ── Sincronizar estado real con API ──
                positions = await self._get_positions()
                if positions is not None:
                    if positions and not self.position:
                        p = positions[0]
                        hold_side = p.get("holdSide", "").lower()
                        if hold_side in ("long", "short"):
                            self.position    = hold_side
                            self.entry_price = float(p.get("openPriceAvg") or
                                                     p.get("avgOpenPrice") or
                                                     price)
                            logger.info(
                                f"[{self.symbol}] 🔄 Posición detectada en API: "
                                f"{hold_side} @ {self.entry_price}"
                            )
                    elif not positions and self.position:
                        logger.info(
                            f"[{self.symbol}] 🔄 Posición cerrada externamente — "
                            "reseteando estado"
                        )
                        self.position = self.entry_price = self.sl = None
                        self.tp1 = self.tp2 = self.tp3 = None
                        clear_position(self.symbol)

                # ── Obtener datos OHLCV ──
                try:
                    ohlcv = await self.exchange.fetch_ohlcv(
                        self.symbol, tf, limit=200
                    )
                except Exception as e:
                    logger.warning(f"[{self.symbol}] OHLCV error: {e}")
                    await asyncio.sleep(interval)
                    continue

                if len(ohlcv) < 50:
                    await asyncio.sleep(interval)
                    continue

                # ── Decisión IA ──
                def _ai_fn(candles, sym, sig):
                    return ai_decide(candles, sym, sig)

                decision = await decide(
                    ohlcv=ohlcv,
                    price=price,
                    exch=self.exchange, symbol=self.symbol,
                    ai_decide_fn=_ai_fn,
                    has_open_position=self.position is not None,
                    current_pnl=None,
                )

                action = decision["action"]
                sig    = decision["signal"]
                reason = decision["reason"]

                if action == "CLOSE" and self.position:
                    result = await self.close_position("Última señal CLOSE")
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
                        logger.warning(
                            f"[{self.symbol}] ⛔ Trade bloqueado: "
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
                        logger.warning(
                            f"[{self.symbol}] ⛔ Trade bloqueado: "
                            f"{r1 if not can_l else r2}"
                        )

                elif action in ("BUY", "SELL") and self.position:
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
