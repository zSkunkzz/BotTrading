"""
bot/core/bingx_client.py — Cliente BingX Perpetuos USDT-margined.

Drop-in replacement de OKXClient para el exchange BingX.
Expone exactamente la misma interfaz pública:

  Construcción:
    client = await BingXClient.create(symbol)   # e.g. "BTC" o "BTC/USDT:USDT"

  Métodos de orden:
    place_market(is_buy, sz, reduce_only, ref_price)
    place_limit(is_buy, sz, price, reduce_only, tif)
    place_tp(is_buy, sz, trigger_px, limit_px, entry_px)
    place_sl(is_buy, sz, trigger_px, entry_px)
    cancel_all_open_tpsl() -> list[dict]
    cancel_order(order_id) -> dict
    place_bulk(orders)  # NotImplementedError

  Consultas:
    get_positions() -> list[dict]
    get_open_orders() -> list
    get_balance_usdc() -> float
    get_user_state() -> dict
    set_leverage(coin, leverage, is_cross)
    get_sz_decimals() -> int
    get_px_decimals() -> int
    get_tick_size() -> float
    get_max_leverage() -> int
    round_px(price) -> float
    round_sz(sz) -> float
    all_mids() -> dict[str, float]

Variables de entorno requeridas:
  BINGX_API_KEY, BINGX_API_SECRET

Opcionales:
  BINGX_TESTNET=true         (usa open-api-vst.bingx.com)
  BINGX_DEFAULT_LEVERAGE=10
  BINGX_MARGIN_MODE=isolated|cross

Notas BingX vs OKX:
  - BingX perpetuos USDT-M: 1 contrato = 1 unidad del coin base (ctVal=1 siempre).
  - instId es "{COIN}-USDT", no "{COIN}-USDT-SWAP".
  - SL/TP son órdenes tipo STOP_MARKET / TAKE_PROFIT_MARKET con stopPrice.
  - No hay posSide; el lado de cierre se infiere de reduceOnly=True.

Fixes (2026-06-06):
  - place_tp: fuerza siempre TAKE_PROFIT_MARKET (BingX rechaza TAKE_PROFIT
    con reduceOnly=true en one-way mode según docs oficiales).
  - cancel_all_open_tpsl: usa DELETE /allOpenOrders (batch atómico) en lugar
    de loop orden-a-orden, evitando fallos parciales.
  - get_balance_usdc: usa availableMargin como campo primario (campo real de
    la API según docs: data.balance.availableMargin).

Fixes (2026-06-06 v2) — revisión doc oficial BingX:
  - _post: parámetros firmados van en el BODY como application/x-www-form-urlencoded,
    no en la query string. La doc oficial exige body-signed para todos los POST.
    Ref: BingX API docs — Authentication & Quick Start (fetchSigned con body=signed).
  - set_leverage: en one-way mode BingX requiere side="BOTH". Llamar con
    side="LONG" o "SHORT" genera error 80012 en one-way mode.
    Ref: BingX swap leverage API — side param: LONG|SHORT|BOTH.
  - place_tp / place_sl: añadido workingType=MARK_PRICE para que el trigger
    use el precio de marca en lugar de CONTRACT_PRICE (last price). Sin este
    campo los stops pueden activarse por spikes del last price.
    Ref: BingX swap trade params — workingType: MARK_PRICE | CONTRACT_PRICE.
  - _warm_cache: popula _max_leverage_cache desde /quote/contracts para evitar
    la llamada HTTP extra por símbolo en cada get_max_leverage().
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import math
import os
import time
import urllib.parse
from typing import Optional

import requests

logger = logging.getLogger("BingXClient")

# ── Env vars ──────────────────────────────────────────────────────────────────
_API_KEY      = os.getenv("BINGX_API_KEY",    "").strip()
_API_SECRET   = os.getenv("BINGX_API_SECRET", "").strip()
_USE_TESTNET  = os.getenv("BINGX_TESTNET",   "").lower() in ("true", "1", "yes")
_DEFAULT_LEV  = int(os.getenv("BINGX_DEFAULT_LEVERAGE", "10"))
_MARGIN_MODE  = os.getenv("BINGX_MARGIN_MODE", "isolated").strip().lower()

_BASE_URL = (
    "https://open-api-vst.bingx.com"
    if _USE_TESTNET
    else "https://open-api.bingx.com"
)


# ── Helpers de símbolo ────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    """Normaliza cualquier formato de símbolo al coin base (e.g. 'BTC')."""
    s = symbol.upper()
    for rm in ("/USDT:USDT", "-USDT-SWAP", "-USDT", "/USDT", "USDT", "SWAP"):
        s = s.replace(rm, "")
    return s.strip("-/")


def _to_symbol(symbol: str) -> str:
    """Devuelve el símbolo BingX: 'BTC-USDT'."""
    return f"{_norm_coin(symbol)}-USDT"


# ── Firma HMAC-SHA256 ─────────────────────────────────────────────────────────

def _sign(params: dict, secret: str) -> str:
    qs = urllib.parse.urlencode(sorted(params.items()))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def _build_signed_body(params: dict, secret: str) -> str:
    """
    Construye la query string firmada para enviar en el body de un POST.
    Según la doc BingX, el body debe ser: 'param1=v1&param2=v2&...&sign=<hex>'
    con los parámetros ordenados alfabéticamente antes de firmar.
    """
    p = dict(params)
    p["timestamp"] = str(int(time.time() * 1000))
    qs   = urllib.parse.urlencode(sorted(p.items()))
    sig  = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return f"{qs}&sign={sig}"


# ── _BingXCore (singleton) ────────────────────────────────────────────────────

class _BingXCore:
    _instance:   "_BingXCore | None"    = None
    _init_lock:  "asyncio.Lock | None"  = None

    def __init__(self) -> None:
        if not _API_KEY or not _API_SECRET:
            raise ValueError(
                "BINGX_API_KEY y BINGX_API_SECRET son obligatorias."
            )
        self._session = requests.Session()
        self._session.headers.update({
            "X-BX-APIKEY": _API_KEY,
        })

        # Caches
        self._tick_size_cache:    dict[str, float] = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._sz_decimals_cache:  dict[str, int]   = {}
        self._max_leverage_cache: dict[str, int]   = {}

        self._warm_cache()

    def _warm_cache(self) -> None:
        """Carga tick sizes, step sizes y max leverage de todos los contratos."""
        try:
            resp = self._session.get(
                f"{_BASE_URL}/openApi/swap/v2/quote/contracts",
                timeout=10,
            )
            data = resp.json().get("data", [])
        except Exception as exc:
            logger.warning("[BingXCore] warm_cache falló: %s", exc)
            return

        for c in data:
            sym = c.get("symbol", "")
            if not sym.endswith("-USDT"):
                continue
            try:
                tick_sz = float(c.get("pricePrecision") or 0.01)
                # pricePrecision en BingX es un entero (número de decimales)
                if tick_sz >= 1:  # es número de decimales, convertir
                    px_dec  = int(tick_sz)
                    tick_sz = 10 ** (-px_dec)
                else:
                    px_dec = max(0, round(-math.log10(tick_sz))) if tick_sz > 0 else 2
            except Exception:
                tick_sz = 0.01
                px_dec  = 2

            try:
                qty_prec = int(c.get("quantityPrecision") or 0)
                sz_dec   = qty_prec
            except Exception:
                sz_dec = 0

            # FIX: popula max_leverage_cache desde warm_cache para evitar
            # una llamada HTTP extra por símbolo en get_max_leverage().
            try:
                max_lev = int(c.get("maxLeverage") or _DEFAULT_LEV)
            except Exception:
                max_lev = _DEFAULT_LEV

            self._tick_size_cache[sym]   = tick_sz
            self._px_decimals_cache[sym] = px_dec
            self._sz_decimals_cache[sym] = sz_dec
            self._max_leverage_cache[sym] = max_lev

        logger.info(
            "[BingXCore] Caché lista: %d contratos cargados",
            len(self._tick_size_cache),
        )

    # ── HTTP firmado ──────────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        p = dict(params or {})
        p["timestamp"] = str(int(time.time() * 1000))
        p["sign"]      = _sign(p, _API_SECRET)
        r = self._session.get(f"{_BASE_URL}{path}", params=p, timeout=10)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, params: dict) -> dict:
        """
        FIX: según la doc oficial BingX, los POST deben enviarse con los
        parámetros firmados en el BODY como application/x-www-form-urlencoded,
        NO en la query string (params=). Antes se usaba params=p lo que ponía
        todo en la URL y podía causar rechazos silenciosos o errores 400.
        Ref: BingX API Quick Start — fetchSigned (body = signed query string).
        """
        body = _build_signed_body(params, _API_SECRET)
        r = self._session.post(
            f"{_BASE_URL}{path}",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str, params: dict) -> dict:
        p = dict(params)
        p["timestamp"] = str(int(time.time() * 1000))
        p["sign"]      = _sign(p, _API_SECRET)
        r = self._session.delete(f"{_BASE_URL}{path}", params=p, timeout=10)
        r.raise_for_status()
        return r.json()

    @classmethod
    async def get_async(cls) -> "_BingXCore":
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        if cls._instance is not None:
            return cls._instance
        async with cls._init_lock:
            if cls._instance is not None:
                return cls._instance
            logger.info("[BingXCore] Inicializando cliente BingX…")
            try:
                cls._instance = await asyncio.to_thread(cls)
            except Exception as exc:
                logger.error("[BingXCore] Falló la inicialización: %s", exc, exc_info=True)
                raise
            return cls._instance


# ── BingXClient ───────────────────────────────────────────────────────────────

class BingXClient:
    """
    Cliente BingX Perpetuos USDT-M.
    Interfaz idéntica a OKXClient para ser drop-in replacement.
    """

    def __init__(self, symbol: str, core: "_BingXCore | None" = None) -> None:
        self.symbol  = symbol
        self.coin    = _norm_coin(symbol)
        self.inst_id = _to_symbol(symbol)   # "BTC-USDT"

        if core is None:
            if _BingXCore._instance is None:
                raise RuntimeError(
                    f"[BingXClient] {symbol}: _BingXCore no inicializado. "
                    "Usar BingXClient.create(symbol) (async)."
                )
            core = _BingXCore._instance
        self._core = core

    @classmethod
    async def create(cls, symbol: str) -> "BingXClient":
        core = await _BingXCore.get_async()
        return cls(symbol, core=core)

    # ── Metadatos ─────────────────────────────────────────────────────────

    def get_sz_decimals(self) -> int:
        return self._core._sz_decimals_cache.get(self.inst_id, 0)

    def get_px_decimals(self) -> int:
        return self._core._px_decimals_cache.get(self.inst_id, 2)

    def get_tick_size(self) -> float:
        return self._core._tick_size_cache.get(self.inst_id, 0.01)

    def get_max_leverage(self) -> int:
        """
        FIX: usa _max_leverage_cache populado en warm_cache desde
        /quote/contracts (campo maxLeverage). Evita la llamada HTTP extra
        por símbolo que antes se hacía en cada llamada a este método.
        Solo hace HTTP fallback si el símbolo no está en caché.
        """
        cached = self._core._max_leverage_cache.get(self.inst_id)
        if cached:
            return cached
        # Fallback HTTP si warm_cache no tenía el símbolo
        try:
            resp = self._core._get(
                "/openApi/swap/v2/trade/leverage",
                {"symbol": self.inst_id},
            )
            lev = int(resp.get("data", {}).get("maxLeverage", _DEFAULT_LEV) or _DEFAULT_LEV)
        except Exception as exc:
            logger.warning("[%s] get_max_leverage: %s", self.inst_id, exc)
            lev = _DEFAULT_LEV
        self._core._max_leverage_cache[self.inst_id] = lev
        return lev

    def get_ct_val(self) -> float:
        """BingX: 1 contrato = 1 unidad coin base (ct_val siempre 1.0)."""
        return 1.0

    def round_px(self, price: float) -> float:
        return round(price, self.get_px_decimals())

    def round_sz(self, sz: float) -> float:
        dec = self.get_sz_decimals()
        if dec == 0:
            return float(math.floor(sz))
        factor = 10 ** dec
        return math.floor(sz * factor) / factor

    def usdc_to_contracts(self, usdc: float, price: float) -> float:
        """BingX: contratos = USDT / precio (ct_val=1)."""
        if price <= 0:
            return 0.0
        return usdc / price

    def contracts_to_usdc(self, contracts: float, price: float) -> float:
        return contracts * price

    # ── Precios ───────────────────────────────────────────────────────────

    def all_mids(self) -> dict[str, float]:
        try:
            resp    = self._core._session.get(
                f"{_BASE_URL}/openApi/swap/v2/quote/ticker",
                timeout=10,
            )
            tickers = resp.json().get("data", [])
            result  = {}
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("-USDT"):
                    continue
                coin = sym.replace("-USDT", "")
                bid  = float(t.get("bidPrice") or 0)
                ask  = float(t.get("askPrice") or 0)
                last = float(t.get("lastPrice") or 0)
                if bid > 0 and ask > 0:
                    result[coin] = (bid + ask) / 2
                elif last > 0:
                    result[coin] = last
            return result
        except Exception as exc:
            logger.warning("[%s] all_mids error: %s", self.inst_id, exc)
            return {}

    # ── Leverage ──────────────────────────────────────────────────────────

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = False) -> dict:
        """
        FIX: en one-way mode (modo por defecto del bot) BingX requiere
        side="BOTH". Llamar con side="LONG" o "SHORT" genera error 80012
        en one-way mode. Solo en hedge mode se usan LONG/SHORT por separado.
        Ref: BingX swap leverage API — side: LONG|SHORT|BOTH.
        """
        try:
            resp = self._core._post(
                "/openApi/swap/v2/trade/leverage",
                {"symbol": self.inst_id, "side": "BOTH", "leverage": str(leverage)},
            )
            logger.info("[%s] Leverage: %dx (side=BOTH)", self.inst_id, leverage)
            return resp
        except Exception as exc:
            logger.warning("[%s] set_leverage error: %s", self.inst_id, exc)
            return {}

    # ── Helpers respuesta BingX ───────────────────────────────────────────

    @staticmethod
    def _bx_ok(resp: dict) -> bool:
        """BingX devuelve code=0 en éxito."""
        return str(resp.get("code", "-1")) == "0"

    def _wrap(self, resp: dict) -> dict:
        """Convierte respuesta BingX al formato OKX esperado por execution_engine."""
        if self._bx_ok(resp):
            data = resp.get("data", {})
            order_id = (
                str(data.get("orderId", ""))
                or str(data.get("order", {}).get("orderId", ""))
                or ""
            )
            return {
                "code": "0",
                "data": [{"ordId": order_id, "sCode": "0", "sMsg": ""}],
                "_raw": resp,
            }
        return {
            "code": str(resp.get("code", "-1")),
            "msg":  resp.get("msg", "error desconocido"),
            "data": [],
            "_raw": resp,
        }

    def _wrap_algo(self, resp: dict, order_id: str = "") -> dict:
        """Wrapper para SL/TP — reutiliza campo algoId mapeado desde orderId."""
        if self._bx_ok(resp):
            data = resp.get("data", {})
            oid  = (
                str(data.get("orderId", order_id))
                or str(data.get("order", {}).get("orderId", ""))
                or order_id
            )
            return {
                "code": "0",
                "data": [{"algoId": oid, "sCode": "0", "sMsg": ""}],
                "_raw": resp,
            }
        return {
            "code": str(resp.get("code", "-1")),
            "msg":  resp.get("msg", "error desconocido"),
            "data": [],
            "_raw": resp,
        }

    # ── Órdenes ───────────────────────────────────────────────────────────

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        sz_r = self.round_sz(sz)
        side = "BUY" if is_buy else "SELL"
        params = {
            "symbol":     self.inst_id,
            "side":       side,
            "type":       "MARKET",
            "quantity":   str(sz_r),
            "reduceOnly": "true" if reduce_only else "false",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_market: %s %.6f reduceOnly=%s",
                self.inst_id, side, sz_r, reduce_only,
            )
            return self._wrap(resp)
        except Exception as exc:
            logger.error("[%s] place_market error: %s", self.inst_id, exc)
            return {"code": "-1", "msg": str(exc), "data": []}

    def place_limit(
        self,
        is_buy: bool,
        sz: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "GTC",
    ) -> dict:
        sz_r = self.round_sz(sz)
        px_r = self.round_px(price)
        side = "BUY" if is_buy else "SELL"
        params = {
            "symbol":      self.inst_id,
            "side":        side,
            "type":        "LIMIT",
            "quantity":    str(sz_r),
            "price":       str(px_r),
            "timeInForce": tif.upper(),
            "reduceOnly":  "true" if reduce_only else "false",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_limit: %s %.6f @ %.6f (%s)",
                self.inst_id, side, sz_r, px_r, tif,
            )
            return self._wrap(resp)
        except Exception as exc:
            logger.error("[%s] place_limit error: %s", self.inst_id, exc)
            return {"code": "-1", "msg": str(exc), "data": []}

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px:   Optional[float] = None,
        entry_px:   Optional[float] = None,
    ) -> dict:
        """
        Coloca una orden Take Profit en BingX.

        FIX (v1): siempre usa TAKE_PROFIT_MARKET (tipo market al trigger).
        BingX rechaza TAKE_PROFIT (limit) con reduceOnly=true en one-way mode
        porque requeriría posSide=LONG/SHORT, que solo existe en hedge mode.

        FIX (v2): añadido workingType=MARK_PRICE para que el trigger use el
        precio de marca. Sin este campo BingX usa CONTRACT_PRICE (last price)
        por defecto, lo que puede causar activaciones prematuras por spikes.
        Ref: BingX swap trade params — workingType: MARK_PRICE | CONTRACT_PRICE.
        """
        sz_r  = self.round_sz(sz)
        tpx   = self.round_px(trigger_px)
        side  = "BUY" if is_buy else "SELL"
        params: dict = {
            "symbol":      self.inst_id,
            "side":        side,
            "type":        "TAKE_PROFIT_MARKET",
            "quantity":    str(sz_r),
            "stopPrice":   str(tpx),
            "workingType": "MARK_PRICE",
            "reduceOnly":  "true",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_tp: %s %.6f @ trigger=%.6f (MARK_PRICE)",
                self.inst_id, side, sz_r, tpx,
            )
            return self._wrap_algo(resp)
        except Exception as exc:
            logger.error("[%s] place_tp error: %s", self.inst_id, exc)
            return {"code": "-1", "msg": str(exc), "data": []}

    def place_sl(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        entry_px:   Optional[float] = None,
    ) -> dict:
        """
        FIX: añadido workingType=MARK_PRICE para que el trigger use el precio
        de marca. Sin este campo BingX usa CONTRACT_PRICE (last price) por
        defecto, lo que puede causar activaciones prematuras del stop loss
        por spikes del spread o del last price.
        Ref: BingX swap trade params — workingType: MARK_PRICE | CONTRACT_PRICE.
        """
        sz_r  = self.round_sz(sz)
        tpx   = self.round_px(trigger_px)
        side  = "BUY" if is_buy else "SELL"
        params = {
            "symbol":      self.inst_id,
            "side":        side,
            "type":        "STOP_MARKET",
            "quantity":    str(sz_r),
            "stopPrice":   str(tpx),
            "workingType": "MARK_PRICE",
            "reduceOnly":  "true",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_sl: %s %.6f @ trigger=%.6f (MARK_PRICE)",
                self.inst_id, side, sz_r, tpx,
            )
            return self._wrap_algo(resp)
        except Exception as exc:
            logger.error("[%s] place_sl error: %s", self.inst_id, exc)
            return {"code": "-1", "msg": str(exc), "data": []}

    def place_bulk(self, orders: list[dict]) -> dict:
        raise NotImplementedError("place_bulk no implementado para BingXClient.")

    # ── Cuenta ────────────────────────────────────────────────────────────

    def get_user_state(self) -> dict:
        try:
            return self._core._get("/openApi/swap/v2/user/balance") or {}
        except Exception as exc:
            logger.warning("[%s] get_user_state error: %s", self.inst_id, exc)
            return {}

    def get_balance_usdc(self) -> float:
        """
        Devuelve el balance disponible en USDT.

        FIX: usa availableMargin como campo primario según la respuesta real
        de /openApi/swap/v2/user/balance:
          { "data": { "balance": { "availableMargin": "...", "equity": "...", ... } } }
        """
        try:
            resp = self._core._get("/openApi/swap/v2/user/balance")
            data = resp.get("data", {}).get("balance", {})
            equity = float(
                data.get("availableMargin")
                or data.get("equity")
                or data.get("balance")
                or 0
            )
            return equity
        except Exception as exc:
            logger.warning("[%s] get_balance_usdc error: %s", self.inst_id, exc)
        return 0.0

    def get_positions(self) -> list[dict]:
        try:
            resp = self._core._get(
                "/openApi/swap/v2/user/positions",
                {"symbol": self.inst_id},
            )
            raw = resp.get("data", [])
        except Exception as exc:
            logger.warning("[%s] get_positions error: %s", self.inst_id, exc)
            return []
        result = []
        for p in raw:
            pos_amt = float(p.get("positionAmt", 0) or 0)
            if pos_amt == 0:
                continue
            side = "long" if pos_amt > 0 else "short"
            result.append({
                "side":          side,
                "entryPx":       float(p.get("avgPrice", 0) or 0),
                "size":          abs(pos_amt),
                "unrealizedPnl": float(p.get("unrealizedProfit", 0) or 0),
                "lever":         int(float(p.get("leverage", 0) or 0)),
            })
        return result

    def get_open_orders(self) -> list:
        try:
            resp = self._core._get(
                "/openApi/swap/v2/trade/openOrders",
                {"symbol": self.inst_id},
            )
            orders = resp.get("data", {}).get("orders", []) or resp.get("data", [])
            # Normalizar al formato OKX esperado por execution_engine (campo ordId)
            return [
                {"ordId": str(o.get("orderId", "")), **o}
                for o in orders
            ]
        except Exception as exc:
            logger.warning("[%s] get_open_orders error: %s", self.inst_id, exc)
            return []

    def cancel_order(self, order_id) -> dict:
        try:
            resp = self._core._delete(
                "/openApi/swap/v2/trade/order",
                {"symbol": self.inst_id, "orderId": str(order_id)},
            )
            return self._wrap(resp)
        except Exception as exc:
            logger.warning("[%s] cancel_order %s error: %s", self.inst_id, order_id, exc)
            return {"code": "-1", "msg": str(exc), "data": []}

    def cancel_all_open_tpsl(self) -> list[dict]:
        """
        Cancela todas las órdenes abiertas del símbolo usando el endpoint
        batch DELETE /allOpenOrders (atómico, una sola llamada API).

        FIX: reemplaza el loop orden-a-orden anterior que podía fallar
        parcialmente y generaba múltiples llamadas. El endpoint
        DELETE /openApi/swap/v2/trade/allOpenOrders cancela todo de golpe.
        """
        try:
            resp = self._core._delete(
                "/openApi/swap/v2/trade/allOpenOrders",
                {"symbol": self.inst_id},
            )
            if self._bx_ok(resp):
                logger.info(
                    "[%s] cancel_all_open_tpsl: todas las órdenes canceladas (batch).",
                    self.inst_id,
                )
            else:
                logger.warning(
                    "[%s] cancel_all_open_tpsl: respuesta inesperada: %s",
                    self.inst_id, resp,
                )
            return [resp]
        except Exception as exc:
            logger.warning("[%s] cancel_all_open_tpsl error: %s", self.inst_id, exc)
            return []
