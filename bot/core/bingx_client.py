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
  - positionSide en one-way mode:
      * Apertura (reduce_only=False): LONG si BUY, SHORT si SELL.
      * Cierre  (reduce_only=True):  BOTH siempre.
      * SL/TP son cierres → positionSide="BOTH".
    Ref: BingX swap trade docs — positionSide: LONG | SHORT | BOTH.

Fixes (2026-06-06 v4) — revisión doc oficial BingX (bingx-api/api-ai-skills):

  Fix #1 — sign → signature (🔴 CRÍTICO, rompía TODAS las peticiones)
    La doc oficial BingX usa '&signature=<hex>', no '&sign=<hex>'.
    Con 'sign' el servidor devuelve 401/400 en cada llamada.
    Corregido en _build_signed_qs y _build_signed_body.
    Ref: bingx-api/api-ai-skills fetchSigned — qs + '&signature=' + sig.

  Fix #2 — positionSide dinámico en MARKET/LIMIT (🔴 CRÍTICO)
    Doc oficial: en one-way mode, las aperturas requieren positionSide=LONG
    (BUY) o positionSide=SHORT (SELL). 'BOTH' solo aplica a reduceOnly=True.
    place_market y place_limit ahora calculan positionSide dinámicamente.
    Ref: bingx-api/api-ai-skills — Common Calls, place market buy order.

  Fix #3 — set_leverage: side=LONG+SHORT (🟡 IMPORTANTE)
    La doc oficial muestra side='LONG' y side='SHORT' (no 'BOTH').
    En one-way mode se llama dos veces para cubrir ambos lados.
    El issue ccxt #22237 confirma error 109400 al enviar side='BOTH'.
    Ref: bingx-api/api-ai-skills — Set leverage example.

  Fix #4 — get_balance_usdc: v3 con fallback v2 (🟡 IMPORTANTE)
    /openApi/swap/v3/user/balance es el endpoint actual según doc oficial.
    Implementado con fallback a v2 si v3 falla.
    Ref: bingx-api/api-ai-skills swap-account — balance endpoint table.

Fixes anteriores (v3, 2026-06-06):
  - hmac.HMAC() explícito.
  - positionSide="BOTH" en todas las órdenes (parcialmente corregido en v4).
  - _delete: firma en query string.
  - _build_signed_qs / _build_signed_body helpers.

Fixes anteriores (v2, 2026-06-06):
  - place_tp: fuerza TAKE_PROFIT_MARKET.
  - cancel_all_open_tpsl: usa DELETE /allOpenOrders (batch atómico).
  - get_balance_usdc: usa availableMargin como campo primario.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac as _hmac
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

def _hmac_sign(qs: str, secret: str) -> str:
    """
    Firma HMAC-SHA256. Usa hmac.HMAC() explícito (hmac.new es alias no
    documentado). Ref: Python docs — hmac.HMAC(key, msg, digestmod).
    """
    return _hmac.HMAC(
        secret.encode(),
        qs.encode(),
        hashlib.sha256,
    ).hexdigest()


def _build_signed_qs(params: dict, secret: str) -> str:
    """
    Construye la query string firmada para GET y DELETE.

    FIX #1 (v4): campo de firma es 'signature', no 'sign'.
    Ref: bingx-api/api-ai-skills fetchSigned:
      const signed = `${qs}&signature=${sig}`;
    """
    p = dict(params)
    p["timestamp"] = str(int(time.time() * 1000))
    qs  = urllib.parse.urlencode(sorted(p.items()))
    sig = _hmac_sign(qs, secret)
    return f"{qs}&signature={sig}"


def _build_signed_body(params: dict, secret: str) -> str:
    """
    Construye la query string firmada para enviar en el BODY de un POST.

    FIX #1 (v4): campo de firma es 'signature', no 'sign'.
    Ref: bingx-api/api-ai-skills fetchSigned:
      body: method === "POST" ? signed : undefined
      donde signed = `${qs}&signature=${sig}`
    """
    p = dict(params)
    p["timestamp"] = str(int(time.time() * 1000))
    qs  = urllib.parse.urlencode(sorted(p.items()))
    sig = _hmac_sign(qs, secret)
    return f"{qs}&signature={sig}"


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

            try:
                max_lev = int(c.get("maxLeverage") or _DEFAULT_LEV)
            except Exception:
                max_lev = _DEFAULT_LEV

            self._tick_size_cache[sym]    = tick_sz
            self._px_decimals_cache[sym]  = px_dec
            self._sz_decimals_cache[sym]  = sz_dec
            self._max_leverage_cache[sym] = max_lev

        logger.info(
            "[BingXCore] Caché lista: %d contratos cargados",
            len(self._tick_size_cache),
        )

    # ── HTTP firmado ──────────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        """GET firmado: parámetros en la query string."""
        signed_qs = _build_signed_qs(params or {}, _API_SECRET)
        r = self._session.get(f"{_BASE_URL}{path}?{signed_qs}", timeout=10)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, params: dict) -> dict:
        """POST firmado: parámetros firmados en el BODY como
        application/x-www-form-urlencoded."""
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
        """DELETE firmado con parámetros en la QUERY STRING (igual que GET).
        Ref: BingX auth docs — DELETE usa query params firmados."""
        signed_qs = _build_signed_qs(params, _API_SECRET)
        r = self._session.delete(f"{_BASE_URL}{path}?{signed_qs}", timeout=10)
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
        Usa _max_leverage_cache populado en warm_cache desde /quote/contracts
        (campo maxLeverage). Solo hace HTTP fallback si el símbolo no está en caché.
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
        Establece el leverage para el símbolo.

        FIX #3 (v4): La doc oficial BingX usa side='LONG' y side='SHORT',
        NO 'BOTH'. Se llama dos veces (LONG + SHORT) para cubrir ambos lados
        en one-way mode. El issue ccxt #22237 confirma error 109400 con 'BOTH'.
        Ref: bingx-api/api-ai-skills — Set leverage example: side='LONG'.
        """
        results = {}
        for side in ("LONG", "SHORT"):
            try:
                resp = self._core._post(
                    "/openApi/swap/v2/trade/leverage",
                    {"symbol": self.inst_id, "side": side, "leverage": str(leverage)},
                )
                results[side] = resp
                if str(resp.get("code", "-1")) == "0":
                    logger.info(
                        "[%s] Leverage %dx seteado (side=%s)", self.inst_id, leverage, side
                    )
                else:
                    logger.warning(
                        "[%s] set_leverage side=%s: %s", self.inst_id, side, resp.get("msg", "")
                    )
            except Exception as exc:
                logger.warning("[%s] set_leverage side=%s error: %s", self.inst_id, side, exc)
                results[side] = {}
        # Devolver el resultado de LONG como representativo (mismo formato que antes)
        return results.get("LONG", {})

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

    @staticmethod
    def _position_side(is_buy: bool, reduce_only: bool) -> str:
        """
        Calcula positionSide correcto para one-way mode en BingX.

        FIX #2 (v4): Doc oficial BingX:
          - Apertura (reduce_only=False): LONG si BUY, SHORT si SELL.
          - Cierre   (reduce_only=True):  BOTH siempre.
        Ref: bingx-api/api-ai-skills — Common Calls, place market buy order:
          positionSide: "LONG" (no "BOTH").
        """
        if reduce_only:
            return "BOTH"
        return "LONG" if is_buy else "SHORT"

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        """
        Orden MARKET en BingX.

        positionSide calculado dinámicamente (FIX #2 v4):
          - Apertura: LONG (BUY) o SHORT (SELL)
          - Cierre:   BOTH (reduce_only=True)
        """
        sz_r     = self.round_sz(sz)
        side     = "BUY" if is_buy else "SELL"
        pos_side = self._position_side(is_buy, reduce_only)
        params = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "MARKET",
            "quantity":     str(sz_r),
            "reduceOnly":   "true" if reduce_only else "false",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_market: %s positionSide=%s %.6f reduceOnly=%s",
                self.inst_id, side, pos_side, sz_r, reduce_only,
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
        """
        Orden LIMIT en BingX.

        positionSide calculado dinámicamente (FIX #2 v4):
          - Apertura: LONG (BUY) o SHORT (SELL)
          - Cierre:   BOTH (reduce_only=True)
        """
        sz_r     = self.round_sz(sz)
        px_r     = self.round_px(price)
        side     = "BUY" if is_buy else "SELL"
        pos_side = self._position_side(is_buy, reduce_only)
        params = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "LIMIT",
            "quantity":     str(sz_r),
            "price":        str(px_r),
            "timeInForce":  tif.upper(),
            "reduceOnly":   "true" if reduce_only else "false",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_limit: %s positionSide=%s %.6f @ %.6f (%s)",
                self.inst_id, side, pos_side, sz_r, px_r, tif,
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

        - Siempre usa TAKE_PROFIT_MARKET (BingX rechaza TAKE_PROFIT con
          reduceOnly=true en one-way mode).
        - positionSide="BOTH": correcto para órdenes de cierre en one-way mode.
          Las órdenes TP/SL son siempre reduce_only=True, por eso BOTH.
        - workingType=MARK_PRICE: el trigger usa precio de marca, no last price.
          Evita activaciones prematuras por spikes del spread.
        Ref: BingX swap trade docs — TAKE_PROFIT_MARKET con reduceOnly=true.
        """
        sz_r  = self.round_sz(sz)
        tpx   = self.round_px(trigger_px)
        side  = "BUY" if is_buy else "SELL"
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": "BOTH",
            "type":         "TAKE_PROFIT_MARKET",
            "quantity":     str(sz_r),
            "stopPrice":    str(tpx),
            "workingType":  "MARK_PRICE",
            "reduceOnly":   "true",
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
        Coloca una orden Stop Loss en BingX.

        - positionSide="BOTH": correcto para órdenes de cierre en one-way mode.
          Las órdenes SL son siempre reduce_only=True, por eso BOTH.
        - workingType=MARK_PRICE: el trigger usa precio de marca, no last price.
          Evita activaciones prematuras del SL por spikes del spread.
        Ref: BingX swap trade docs — STOP_MARKET con reduceOnly=true.
        """
        sz_r  = self.round_sz(sz)
        tpx   = self.round_px(trigger_px)
        side  = "BUY" if is_buy else "SELL"
        params = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": "BOTH",
            "type":         "STOP_MARKET",
            "quantity":     str(sz_r),
            "stopPrice":    str(tpx),
            "workingType":  "MARK_PRICE",
            "reduceOnly":   "true",
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

        FIX #4 (v4): usa /openApi/swap/v3/user/balance (endpoint actual según
        doc oficial bingx-api/api-ai-skills) con fallback a v2 si falla.
        Campo: data.balance.availableMargin.
        Ref: bingx-api/api-ai-skills swap-account — balance endpoint.
        """
        for version in ("v3", "v2"):
            try:
                resp = self._core._get(f"/openApi/swap/{version}/user/balance")
                data = resp.get("data", {}).get("balance", {})
                equity = float(
                    data.get("availableMargin")
                    or data.get("equity")
                    or data.get("balance")
                    or 0
                )
                if equity > 0 or version == "v2":
                    return equity
            except Exception as exc:
                logger.warning(
                    "[%s] get_balance_usdc (%s) error: %s", self.inst_id, version, exc
                )
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
        Ref: BingX — DELETE /openApi/swap/v2/trade/allOpenOrders.
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
