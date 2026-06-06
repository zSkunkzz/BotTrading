"""
bot/core/bingx_client.py — Cliente BingX Perpetuos USDT-margined.

Drop-in replacement de OKXClient para el exchange BingX.
Expone exactamente la misma interfaz pública:

  Construcción:
    client = await BingXClient.create(symbol)   # e.g. "BTC" o "BTC/USDT:USDT"

  Métodos de orden:
    place_market(is_buy, sz, reduce_only, ref_price)
    place_market_with_tpsl(is_buy, sz, sl_px, tp_px, ref_price)
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
  BINGX_POSITION_MODE=hedge  (fuerza hedge mode sin consultar la API)

Notas positionSide:
  - ONE-WAY MODE:  positionSide="BOTH" en todas las órdenes.
  - HEDGE MODE:    positionSide="LONG" (BUY) o "SHORT" (SELL).
  _BingXCore detecta automáticamente el modo al inicializar consultando
  /openApi/swap/v1/positionSide/dual. La variable de entorno
  BINGX_POSITION_MODE=hedge permite forzarlo sin llamada API.

Fixes (2026-06-06 v8) — set_leverage robusto:

  Fix #12 — set_leverage: lanza RuntimeError si BingX rechaza el cambio (🔴 CRÍTICO)
    Anteriormente set_leverage capturaba todos los errores en except y los
    logueaba como WARNING, de modo que si BingX rechazaba la llamada (e.g.
    "leverage out of range", error de firma, etc.) el bot continuaba con el
    leverage por defecto del contrato (5x) sin ningún aviso claro.
    Ahora:
      - Si AMBOS sides (LONG y SHORT) fallan → lanza RuntimeError.
      - Si solo uno falla → loguea ERROR (no lanza, para no bloquear one-way mode).
      - Loguea el leverage confirmado por BingX desde la respuesta.

Fixes (2026-06-06 v7) — soporte Hedge Mode:

  Fix #11 — positionSide dinámico según modo de cuenta (🔴 CRÍTICO)
    BingX devuelve "In the Hedge mode, the 'PositionSide' field can only
    be set to LONG or SHORT." cuando la cuenta está en HEDGE MODE y se
    envía positionSide=BOTH.
    _BingXCore detecta el modo (hedge_mode flag) al inicializar.
    Todos los métodos de orden usan _pos_side(is_buy) que devuelve:
      - "BOTH"  en one-way mode
      - "LONG"  en hedge mode si is_buy=True
      - "SHORT" en hedge mode si is_buy=False
    En hedge mode, reduce_only se omite (no compatible) y las órdenes
    de cierre usan el positionSide opuesto a la posición.
    Ref: BingX docs — positionSide: LONG | SHORT | BOTH

Fixes (2026-06-06 v6) — re-auditoría doc oficial BingX (bingx-api/api-ai-skills):

  Fix #8 — domain fallback .pro obligatorio (🔴 CRÍTICO)
  Fix #9 — stopLoss/takeProfit embebidos en MARKET (🔴 CRÍTICO)
  Fix #10 — get_user_state usa v3 (🟡 IMPORTANTE)

Fixes (2026-06-06 v5) — re-auditoría doc oficial BingX (bingx-api/api-ai-skills):

  Fix #5 — positionSide en one-way mode: siempre BOTH (🔴 CRÍTICO)
  Fix #6 — X-SOURCE-KEY header obligatorio (🔴 CRÍTICO)
  Fix #7 — timestamp incluido en sort() correctamente (🟡 IMPORTANTE)

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
# BINGX_POSITION_MODE=hedge fuerza hedge mode sin consultar la API.
_FORCE_HEDGE  = os.getenv("BINGX_POSITION_MODE", "").strip().lower() == "hedge"

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
    return _hmac.HMAC(
        secret.encode(),
        qs.encode(),
        hashlib.sha256,
    ).hexdigest()


def _build_signed_qs(params: dict, secret: str) -> str:
    p = {**params, "timestamp": str(int(time.time() * 1000))}
    qs  = "&".join(f"{k}={v}" for k, v in sorted(p.items()))
    sig = _hmac_sign(qs, secret)
    return f"{qs}&signature={sig}"


def _build_signed_body(params: dict, secret: str) -> str:
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
            "X-SOURCE-KEY": "BX-AI-SKILL",
        })

        # Caches
        self._tick_size_cache:    dict[str, float] = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._sz_decimals_cache:  dict[str, int]   = {}
        self._max_leverage_cache: dict[str, int]   = {}

        # Fix #11 (v7): detectar hedge mode al inicializar.
        # En hedge mode positionSide debe ser LONG o SHORT, nunca BOTH.
        self.hedge_mode: bool = self._detect_hedge_mode()

        self._warm_cache()

    def _detect_hedge_mode(self) -> bool:
        """
        Fix #11 (v7): consulta /openApi/swap/v1/positionSide/dual para
        determinar si la cuenta está en HEDGE MODE (dualSidePosition=true)
        o ONE-WAY MODE (dualSidePosition=false).

        La variable de entorno BINGX_POSITION_MODE=hedge permite forzarlo
        sin llamada API (útil si el endpoint no está disponible).

        Ref: BingX docs — GET /openApi/swap/v1/positionSide/dual
        """
        if _FORCE_HEDGE:
            logger.info("[BingXCore] Hedge mode FORZADO por BINGX_POSITION_MODE=hedge")
            return True
        try:
            resp = self._request("GET", "/openApi/swap/v1/positionSide/dual", {})
            dual = resp.get("data", {}).get("dualSidePosition", False)
            mode = "HEDGE" if dual else "ONE-WAY"
            logger.info("[BingXCore] Modo de posición detectado: %s", mode)
            return bool(dual)
        except Exception as exc:
            logger.warning(
                "[BingXCore] No se pudo detectar position mode (%s). "
                "Asumiendo ONE-WAY. Si la cuenta está en hedge mode, "
                "establece BINGX_POSITION_MODE=hedge.",
                exc,
            )
            return False

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
                if tick_sz >= 1:
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

        raise last_exc

    def _get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", path, params or {})

    def _post(self, path: str, params: dict, body: Optional[str] = None) -> dict:
        return self._request("POST", path, params, body=body)

    def _delete(self, path: str, params: dict) -> dict:
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
    Soporta ONE-WAY MODE y HEDGE MODE automáticamente.
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

    # ── Fix #11 (v7): helper positionSide dinámico ────────────────────────

    def _pos_side(self, is_buy: bool) -> str:
        """
        Devuelve el valor correcto de positionSide según el modo de la cuenta:
          - ONE-WAY MODE: "BOTH" (siempre)
          - HEDGE MODE:   "LONG" si is_buy=True, "SHORT" si is_buy=False

        Para órdenes de CIERRE en hedge mode el caller debe pasar
        is_buy=False para long positions (SELL cierra LONG) y
        is_buy=True para short positions (BUY cierra SHORT).
        La lógica de cierre ya lo hace correctamente porque reduce_only
        invierte el side.
        """
        if self._core.hedge_mode:
            return "LONG" if is_buy else "SHORT"
        return "BOTH"

    def _pos_side_close(self, is_buy: bool) -> str:
        """
        positionSide para órdenes de CIERRE en hedge mode.
        Para cerrar una LONG se envía SELL → positionSide="LONG".
        Para cerrar una SHORT se envía BUY  → positionSide="SHORT".
        En one-way mode devuelve "BOTH" igual que _pos_side.
        """
        if self._core.hedge_mode:
            # is_buy=True → cerrando SHORT → positionSide=SHORT
            # is_buy=False → cerrando LONG  → positionSide=LONG
            return "SHORT" if is_buy else "LONG"
        return "BOTH"

    # ── Metadatos ─────────────────────────────────────────────────────────

    def get_sz_decimals(self) -> int:
        return self._core._sz_decimals_cache.get(self.inst_id, 0)

    def get_px_decimals(self) -> int:
        return self._core._px_decimals_cache.get(self.inst_id, 2)

    def get_tick_size(self) -> float:
        return self._core._tick_size_cache.get(self.inst_id, 0.01)

    def get_max_leverage(self) -> int:
        cached = self._core._max_leverage_cache.get(self.inst_id)
        if cached:
            return cached
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
        Fix #12 (v8): ahora lanza RuntimeError si BingX rechaza el cambio
        de leverage en AMBOS sides (LONG y SHORT), de modo que _set_leverage
        en trader.py pueda loguearlo como ERROR y el operador lo vea claramente.

        Si solo uno de los sides falla (raro, pero posible en one-way mode),
        se loguea como ERROR pero no lanza para no bloquear la operativa.

        Loguea el leverage confirmado por BingX desde la respuesta si está
        disponible (campo data.leverage).
        """
        errors: dict[str, str] = {}
        ok_sides: list[str] = []

        for side in ("LONG", "SHORT"):
            try:
                resp = self._core._post(
                    "/openApi/swap/v2/trade/leverage",
                    {"symbol": self.inst_id, "side": side, "leverage": str(leverage)},
                )
                code = str(resp.get("code", "-1"))
                if code == "0":
                    confirmed_lev = (
                        resp.get("data", {}).get("leverage")
                        or resp.get("data", {}).get("longLeverage")
                        or resp.get("data", {}).get("shortLeverage")
                    )
                    if confirmed_lev:
                        logger.info(
                            "[%s] set_leverage side=%s: OK — leverage confirmado por BingX: %sx",
                            self.inst_id, side, confirmed_lev,
                        )
                    else:
                        logger.info(
                            "[%s] set_leverage side=%s: OK (code=0, lev=%dx solicitado)",
                            self.inst_id, side, leverage,
                        )
                    ok_sides.append(side)
                else:
                    msg = resp.get("msg", f"code={code}")
                    logger.error(
                        "[%s] set_leverage side=%s RECHAZADO por BingX: %s "
                        "(leverage=%dx — verifica que el leverage sea válido para este contrato)",
                        self.inst_id, side, msg, leverage,
                    )
                    errors[side] = msg
            except Exception as exc:
                logger.error(
                    "[%s] set_leverage side=%s excepción: %s",
                    self.inst_id, side, exc,
                )
                errors[side] = str(exc)

        if not ok_sides:
            # Ambos sides fallaron → lanzar para que trader.py lo maneje
            raise RuntimeError(
                f"[{self.inst_id}] set_leverage({leverage}x) falló en ambos sides: "
                f"LONG={errors.get('LONG', '?')} / SHORT={errors.get('SHORT', '?')}"
            )

        return {}

    # ── Helpers respuesta BingX ───────────────────────────────────────────

    @staticmethod
    def _bx_ok(resp: dict) -> bool:
        return str(resp.get("code", "-1")) == "0"

    def _wrap(self, resp: dict) -> dict:
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
        Fix #11 (v7): positionSide dinámico según modo de cuenta.
          - ONE-WAY: BOTH + reduceOnly
          - HEDGE:   LONG/SHORT según is_buy; reduceOnly omitido
            (en hedge mode el cierre se indica via positionSide opuesto).
        """
        sz_r = self.round_sz(sz)
        side = "BUY" if is_buy else "SELL"
        # En hedge mode con reduce_only, el cierre usa positionSide opuesto.
        if self._core.hedge_mode and reduce_only:
            pos_side = self._pos_side_close(is_buy)
        else:
            pos_side = self._pos_side(is_buy)
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "MARKET",
            "quantity":     str(sz_r),
        }
        # reduceOnly solo en one-way mode (hedge mode lo ignora / da error)
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true" if reduce_only else "false"
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

    def place_market_with_tpsl(
        self,
        is_buy: bool,
        sz: float,
        sl_px: Optional[float] = None,
        tp_px: Optional[float] = None,
        ref_price: Optional[float] = None,
    ) -> dict:
        """
        Orden MARKET con SL y TP embebidos en una sola llamada API.
        Fix #11 (v7): positionSide dinámico según modo de cuenta.
        """
        sz_r     = self.round_sz(sz)
        side     = "BUY" if is_buy else "SELL"
        pos_side = self._pos_side(is_buy)
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "MARKET",
            "quantity":     str(sz_r),
        }
        if sl_px is not None:
            params["stopLoss"] = {
                "type":           "STOP_MARKET",
                "stopPrice":      self.round_px(sl_px),
                "workingType":    "MARK_PRICE",
                "stopGuaranteed": False,
            }
        if tp_px is not None:
            params["takeProfit"] = {
                "type":           "TAKE_PROFIT_MARKET",
                "stopPrice":      self.round_px(tp_px),
                "workingType":    "MARK_PRICE",
                "stopGuaranteed": False,
            }
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_market_with_tpsl: %s positionSide=%s %.6f sl=%s tp=%s",
                self.inst_id, side, pos_side, sz_r,
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
        Fix #11 (v7): positionSide dinámico según modo de cuenta.
        """
        sz_r = self.round_sz(sz)
        px_r = self.round_px(price)
        side = "BUY" if is_buy else "SELL"
        if self._core.hedge_mode and reduce_only:
            pos_side = self._pos_side_close(is_buy)
        else:
            pos_side = self._pos_side(is_buy)
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "LIMIT",
            "quantity":     str(sz_r),
            "price":        str(px_r),
            "timeInForce":  tif.upper(),
        }
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true" if reduce_only else "false"
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
        Take Profit independiente.
        Fix #11 (v7): positionSide dinámico.
        En hedge mode, el TP cierra la posición opuesta al side de entrada:
          - TP de LONG (is_buy=True): orden SELL → positionSide=LONG
          - TP de SHORT (is_buy=False): orden BUY → positionSide=SHORT
        """
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "BUY" if is_buy else "SELL"
        # El TP cierra la posición → positionSide del lado que se está cerrando
        pos_side = self._pos_side_close(is_buy) if self._core.hedge_mode else "BOTH"
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "TAKE_PROFIT_MARKET",
            "quantity":     str(sz_r),
            "stopPrice":    str(tpx),
            "workingType":  "MARK_PRICE",
        }
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true"
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_tp: %s positionSide=%s %.6f @ trigger=%.6f",
                self.inst_id, side, pos_side, sz_r, tpx,
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
        Stop Loss independiente.
        Fix #11 (v7): positionSide dinámico.
        En hedge mode, el SL cierra la posición opuesta (mismo que TP).
        """
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "BUY" if is_buy else "SELL"
        pos_side = self._pos_side_close(is_buy) if self._core.hedge_mode else "BOTH"
        params: dict = {
            "symbol":       self.inst_id,
            "side":         side,
            "positionSide": pos_side,
            "type":         "STOP_MARKET",
            "quantity":     str(sz_r),
            "stopPrice":    str(tpx),
            "workingType":  "MARK_PRICE",
        }
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true"
        try:
            resp = self._core._post("/openApi/swap/v2/trade/order", params)
            logger.info(
                "[%s] place_sl: %s positionSide=%s %.6f @ trigger=%.6f",
                self.inst_id, side, pos_side, sz_r, tpx,
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
            return self._core._get("/openApi/swap/v3/user/balance") or {}
        except Exception as exc:
            logger.warning("[%s] get_user_state error: %s", self.inst_id, exc)
            return {}

    def get_balance_usdc(s