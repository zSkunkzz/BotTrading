"""
bot/core/bingx_client.py — Cliente BingX Perpetuos USDT-margined.

Drop-in replacement de OKXClient para el exchange BingX.
Expone exactamente la misma interfaz pública:

  Construcción:
    client = await BingXClient.create(symbol)   # e.g. "BTC" o "BTC/USDT:USDT"

  Métodos de orden:
    place_market(is_buy, sz, reduce_only, ref_price)
    place_market_with_tpsl(is_buy, sz, sl_px, tp_px, ref_price)  ← NUEVO v6
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
  - positionSide en ONE-WAY MODE:
      * Siempre "BOTH" — tanto apertura como cierre.
      * "LONG"/"SHORT" solo aplican en HEDGE MODE.
    Ref: BingX swap-trade SKILL.md — positionSide: LONG | SHORT | BOTH
    Ref: BingX one-way mode doc — BOTH para cualquier orden en one-way.

Fixes (2026-06-06 v6) — re-auditoría doc oficial BingX (bingx-api/api-ai-skills):

  Fix #8 — domain fallback .pro obligatorio (🔴 CRÍTICO)
    Doc oficial BingX SKILL.md fetchSigned:
      BASE = { "prod-live": ["https://open-api.bingx.com", "https://open-api.bingx.pro"] }
      "Domain priority: .com is mandatory primary; .pro is fallback for
      network/timeout errors ONLY."
    _BingXCore implementa _request() con loop de dominios idéntico al
    fetchSigned oficial: intenta .com primero, si falla con error de red/timeout
    reintenta con .pro. Aplicado a _get, _post, _delete.
    Ref: bingx-api/api-ai-skills fetchSigned — isNetworkOrTimeout check.

  Fix #9 — stopLoss/takeProfit embebidos en MARKET (🔴 CRÍTICO)
    Doc oficial BingX:
      "stopLoss/takeProfit objects are ONLY supported on MARKET or LIMIT order types."
      Estructura: {"type":"STOP_MARKET","stopPrice":X,"workingType":"MARK_PRICE"}
    Nuevo método place_market_with_tpsl() adjunta SL y TP a la orden MARKET
    en una sola llamada API, eliminando la race condition de órdenes separadas.
    Los objetos JSON se URL-encodean correctamente en el body POST.
    place_sl()/place_tp() se mantienen para compatibilidad con flujos existentes.
    Ref: bingx-api/api-ai-skills — Stop-Loss/Take-Profit Object Structure.

  Fix #10 — get_user_state usa v3 (🟡 IMPORTANTE)
    get_user_state() usaba /v2/user/balance en lugar del endpoint actual.
    Ahora usa /openApi/swap/v3/user/balance (igual que get_balance_usdc).
    Ref: bingx-api/api-ai-skills swap-account — /openApi/swap/v3/user/balance.

Fixes (2026-06-06 v5) — re-auditoría doc oficial BingX (bingx-api/api-ai-skills):

  Fix #5 — positionSide en one-way mode: siempre BOTH (🔴 CRÍTICO)
    En ONE-WAY MODE: usar BOTH en TODAS las órdenes (apertura y cierre).
    LONG/SHORT solo aplican en HEDGE MODE.
    BingX devuelve error 80014 si envías LONG/SHORT en one-way mode.

  Fix #6 — X-SOURCE-KEY header obligatorio (🔴 CRÍTICO)
    SKILL.md: "MUST include X-SOURCE-KEY: BX-AI-SKILL header on every request".

  Fix #7 — timestamp incluido en sort() correctamente (🟡 IMPORTANTE)
    fetchSigned oficial: sort con timestamp incluido antes del join.

Fixes (2026-06-06 v4):
  Fix #1 — sign → signature (🔴 CRÍTICO)
  Fix #3 — set_leverage: side=LONG+SHORT (🟡 IMPORTANTE)
  Fix #4 — get_balance_usdc: v3 con fallback v2 (🟡 IMPORTANTE)

Fixes anteriores (v2/v3):
  - hmac.HMAC() explícito.
  - _delete: firma en query string.
  - place_tp: fuerza TAKE_PROFIT_MARKET.
  - cancel_all_open_tpsl: DELETE /allOpenOrders (batch atómico).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac as _hmac
import json
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

# Fix #8 (v6): domain fallback .pro — doc oficial BingX SKILL.md fetchSigned:
# "Domain priority: .com is mandatory primary; .pro is fallback for
# network/timeout errors ONLY."
# BASE["prod-live"] = ["https://open-api.bingx.com", "https://open-api.bingx.pro"]
# BASE["prod-vst"]  = ["https://open-api-vst.bingx.com", "https://open-api-vst.bingx.pro"]
if _USE_TESTNET:
    _BASE_URLS = [
        "https://open-api-vst.bingx.com",
        "https://open-api-vst.bingx.pro",
    ]
else:
    _BASE_URLS = [
        "https://open-api.bingx.com",
        "https://open-api.bingx.pro",
    ]


def _is_network_error(exc: Exception) -> bool:
    """
    Fix #8 (v6): detecta errores de red/timeout para activar fallback .pro.
    Ref: bingx-api/api-ai-skills fetchSigned — isNetworkOrTimeout:
      if (e instanceof TypeError) return true;      // network failure
      if (e instanceof DOMException && e.name === "AbortError") return true;  // abort
      if (e instanceof Error && e.name === "TimeoutError") return true;        // timeout
    """
    if isinstance(exc, (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.ConnectTimeout,
                        requests.exceptions.ReadTimeout)):
        return True
    return False


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

    Fix #1 (v4): campo de firma es 'signature', no 'sign'.
    Fix #7 (v5): timestamp incluido en el sort() igual que fetchSigned oficial.
    Ref: bingx-api/api-ai-skills fetchSigned:
      const all = { ...params, timestamp: Date.now() };
      const qs = Object.keys(all).sort().map(k => `${k}=${all[k]}`).join("&");
      const sig = crypto.createHmac("sha256", secretKey).update(qs).digest("hex");
      const signed = `${qs}&signature=${sig}`;
    """
    p = {**params, "timestamp": str(int(time.time() * 1000))}
    qs  = "&".join(f"{k}={v}" for k, v in sorted(p.items()))
    sig = _hmac_sign(qs, secret)
    return f"{qs}&signature={sig}"


def _build_signed_body(params: dict, secret: str) -> str:
    """
    Construye la query string firmada para enviar en el BODY de un POST.

    Fix #1 (v4): campo de firma es 'signature', no 'sign'.
    Fix #7 (v5): timestamp incluido en el sort(), idéntico a fetchSigned oficial.
    Fix #9 (v6): los valores que sean dict/list se serializan como JSON string
    antes de incluirlos en la query string. Esto permite enviar objetos
    stopLoss/takeProfit embebidos según el formato oficial BingX.
    Ref: bingx-api/api-ai-skills fetchSigned body + Stop-Loss/TP Object Structure.
    """
    # Serializar valores que sean dict/list a JSON string
    serialized: dict = {}
    for k, v in params.items():
        if isinstance(v, (dict, list)):
            serialized[k] = json.dumps(v, separators=(",", ":"))
        else:
            serialized[k] = v

    p = {**serialized, "timestamp": str(int(time.time() * 1000))}
    qs  = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in sorted(p.items()))
    sig = _hmac_sign(qs, _API_SECRET)
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
            "X-BX-APIKEY":  _API_KEY,
            # Fix #6 (v5): header obligatorio según doc oficial BingX ai-skills.
            # SKILL.md: "MUST include X-SOURCE-KEY: BX-AI-SKILL header on every request"
            "X-SOURCE-KEY": "BX-AI-SKILL",
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
            resp = self._request("GET", "/openApi/swap/v2/quote/contracts", {})
            data = resp.get("data", [])
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

    # ── HTTP firmado con domain fallback ──────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: dict,
        body: Optional[str] = None,
    ) -> dict:
        """
        Fix #8 (v6): ejecuta la petición HTTP con fallback .pro.
        Loop idéntico al fetchSigned oficial:
          for (const base of urls) {
            try { ... }
            catch (e) {
              if (!isNetworkOrTimeout(e) || base === urls[urls.length-1]) throw e;
            }
          }
        Ref: bingx-api/api-ai-skills fetchSigned — domain fallback loop.
        """
        last_exc: Optional[Exception] = None
        for i, base_url in enumerate(_BASE_URLS):
            is_last = (i == len(_BASE_URLS) - 1)
            try:
                if method == "GET":
                    signed_qs = _build_signed_qs(params, _API_SECRET)
                    r = self._session.get(
                        f"{base_url}{path}?{signed_qs}", timeout=10
                    )
                elif method == "POST":
                    signed_body = body or _build_signed_body(params, _API_SECRET)
                    r = self._session.post(
                        f"{base_url}{path}",
                        data=signed_body,
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=10,
                    )
                elif method == "DELETE":
                    signed_qs = _build_signed_qs(params, _API_SECRET)
                    r = self._session.delete(
                        f"{base_url}{path}?{signed_qs}", timeout=10
                    )
                else:
                    raise ValueError(f"Método HTTP no soportado: {method}")

                r.raise_for_status()
                return r.json()

            except Exception as exc:
                last_exc = exc
                if not _is_network_error(exc) or is_last:
                    raise
                logger.warning(
                    "[BingXCore] %s %s — error de red en %s, reintentando con %s: %s",
                    method, path, base_url,
                    _BASE_URLS[i + 1] if not is_last else "(último dominio)",
                    exc,
                )

        raise last_exc  # nunca debería llegar aquí

    def _get(self, path: str, params: dict | None = None) -> dict:
        """GET firmado con domain fallback."""
        return self._request("GET", path, params or {})

    def _post(self, path: str, params: dict, body: Optional[str] = None) -> dict:
        """POST firmado con domain fallback."""
        return self._request("POST", path, params, body=body)

    def _delete(self, path: str, params: dict) -> dict:
        """DELETE firmado con domain fallback."""
        return self._request("DELETE", path, params)

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
            resp    = self._core._get("/openApi/swap/v2/quote/ticker")
            tickers = resp.get("data", [])
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

        Fix #3 (v4): La doc oficial BingX usa side='LONG' y side='SHORT',
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

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        """
        Orden MARKET en BingX.

        Fix #5 (v5): positionSide siempre "BOTH" en one-way mode.
        Doc oficial BingX: en one-way mode, BOTH es el valor correcto
        para cualquier orden (apertura y cierre). LONG/SHORT solo en hedge mode.
        Refs: BingX swap-trade SKILL.md params, BingX one-way mode docs.
        """
        sz_r = self.round_sz(sz)
        side = "BUY" if is_buy else "SELL"
        params = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": "BOTH",
            "type":         "MARKET",
            "quantity":     str(sz_r),
            "reduceOnly":   "true" if reduce_only else "false",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_market: %s positionSide=BOTH %.6f reduceOnly=%s",
                self.inst_id, side, sz_r, reduce_only,
            )
            return self._wrap(resp)
        except Exception as exc:
            logger.error("[%s] place_market error: %s", self.inst_id, exc)
            return {"code": "-1", "msg": str(exc), "data": []}

    def place_market_with_tpsl(
        self,
        is_buy: bool,
        sz: float,
        sl_px: Optional[float] = None,
        tp_px: Optional[float] = None,
        ref_price: Optional[float] = None,
    ) -> dict:
        """
        Fix #9 (v6): Orden MARKET con SL y TP embebidos en una sola llamada API.

        Doc oficial BingX:
          "stopLoss/takeProfit objects are ONLY supported on MARKET or LIMIT order types."
          Estructura:
            stopLoss:   {"type": "STOP_MARKET",       "stopPrice": X, "workingType": "MARK_PRICE"}
            takeProfit: {"type": "TAKE_PROFIT_MARKET", "stopPrice": X, "workingType": "MARK_PRICE"}
          Ventaja: atómica — elimina la race condition de órdenes separadas.
        Ref: bingx-api/api-ai-skills swap-trade — Stop-Loss/Take-Profit Object Structure.

        Los objetos JSON se URL-encodean en el body POST via _build_signed_body.
        """
        sz_r = self.round_sz(sz)
        side = "BUY" if is_buy else "SELL"
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": "BOTH",
            "type":         "MARKET",
            "quantity":     str(sz_r),
        }
        if sl_px is not None:
            params["stopLoss"] = {
                "type":        "STOP_MARKET",
                "stopPrice":   self.round_px(sl_px),
                "workingType": "MARK_PRICE",
                "stopGuaranteed": False,
            }
        if tp_px is not None:
            params["takeProfit"] = {
                "type":        "TAKE_PROFIT_MARKET",
                "stopPrice":   self.round_px(tp_px),
                "workingType": "MARK_PRICE",
                "stopGuaranteed": False,
            }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_market_with_tpsl: %s positionSide=BOTH %.6f sl=%s tp=%s",
                self.inst_id, side, sz_r,
                self.round_px(sl_px) if sl_px else None,
                self.round_px(tp_px) if tp_px else None,
            )
            return self._wrap(resp)
        except Exception as exc:
            logger.error("[%s] place_market_with_tpsl error: %s", self.inst_id, exc)
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

        Fix #5 (v5): positionSide siempre "BOTH" en one-way mode.
        """
        sz_r = self.round_sz(sz)
        px_r = self.round_px(price)
        side = "BUY" if is_buy else "SELL"
        params = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": "BOTH",
            "type":         "LIMIT",
            "quantity":     str(sz_r),
            "price":        str(px_r),
            "timeInForce":  tif.upper(),
            "reduceOnly":   "true" if reduce_only else "false",
        }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_limit: %s positionSide=BOTH %.6f @ %.6f (%s)",
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
        Coloca una orden Take Profit independiente en BingX.

        Nota: para nuevas posiciones, preferir place_market_with_tpsl() que
        adjunta el TP de forma atómica junto a la orden de entrada (Fix #9 v6).
        Este método se mantiene para TP adicionales sobre posiciones ya abiertas.

        - Siempre usa TAKE_PROFIT_MARKET.
        - positionSide="BOTH": correcto para one-way mode (fix #5 v5).
        - workingType=MARK_PRICE: trigger usa precio de marca, evita spikes.
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
        Coloca una orden Stop Loss independiente en BingX.

        Nota: para nuevas posiciones, preferir place_market_with_tpsl() que
        adjunta el SL de forma atómica junto a la orden de entrada (Fix #9 v6).
        Este método se mantiene para SL adicionales sobre posiciones ya abiertas.

        - positionSide="BOTH": correcto para one-way mode (fix #5 v5).
        - workingType=MARK_PRICE: evita activaciones prematuras.
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
        """
        Fix #10 (v6): usa /openApi/swap/v3/user/balance (endpoint actual).
        Antes usaba v2 por error. v3 es el endpoint correcto según
        bingx-api/api-ai-skills swap-account Quick Reference.
        Ref: /openApi/swap/v3/user/balance — "Account balance, equity, margin info".
        """
        try:
            return self._core._get("/openApi/swap/v3/user/balance") or {}
        except Exception as exc:
            logger.warning("[%s] get_user_state error: %s", self.inst_id, exc)
            return {}

    def get_balance_usdc(self) -> float:
        """
        Devuelve el balance disponible en USDT.

        Fix #4 (v4): usa /openApi/swap/v3/user/balance (endpoint actual según
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
        """
        Consulta posiciones abiertas.
        Ref: bingx-api/api-ai-skills swap-account — /openApi/swap/v2/user/positions.
        """
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
