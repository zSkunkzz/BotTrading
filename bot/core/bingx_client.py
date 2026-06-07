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
    set_margin_mode(symbol, margin_type) -> dict
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
  BINGX_POSITIONS_CACHE_TTL  (segundos de caché para get_positions; default=4)

Notas positionSide:
  - ONE-WAY MODE:  positionSide="BOTH" en todas las órdenes.
  - HEDGE MODE:    positionSide="LONG" (BUY) o "SHORT" (SELL).
  _BingXCore detecta automáticamente el modo al inicializar consultando
  /openApi/swap/v1/positionSide/dual. La variable de entorno
  BINGX_POSITION_MODE=hedge permite forzarlo sin llamada API.

Fixes (2026-06-07 v12) — fix 429 Too Many Requests:

  Fix #24 — _request: retry con backoff exponencial + jitter para HTTP 429 (🔴 CRÍTICO)
    r.raise_for_status() en 429 propagaba el error inmediatamente.
    Ahora se detecta el código 429 antes de raise_for_status y se reintenta
    hasta _MAX_RETRIES_429 veces (default 4) con espera exponencial + jitter
    (0.5s, 1s, 2s, 4s + jitter ±0.3s) antes de relanzar el error.

  Fix #25 — get_positions: caché compartida por símbolo con TTL (🔴 CRÍTICO)
    Con N pares activos, cada instancia de BingXClient llamaba get_positions()
    de forma independiente → N requests simultáneos → 429.
    Solución: _BingXCore almacena un cache de posiciones por símbolo con TTL
    configurable (BINGX_POSITIONS_CACHE_TTL, default 4 segundos). Todas las
    instancias comparten el singleton _BingXCore, por lo que el endpoint solo
    se consulta una vez por ventana de TTL aunque haya 50 pares activos.

Fixes (2026-06-07 v11) — auditoría profunda doc oficial BingX:

  Fix #18 — _build_signed_body usaba _API_SECRET global en lugar del parámetro secret (🔴 CRÍTICO)
  Fix #19 — _warm_cache: pricePrecision es número de decimales, no tick size (🟡 IMPORTANTE)
  Fix #20 — get_balance_usdc: priorizar availableMargin sobre balance total (🔴 CRÍTICO)
  Fix #21 — cancel_all_open_tpsl: también cancela órdenes TP/SL standalone (🔴 CRÍTICO)
  Fix #22 — get_open_orders: incluye órdenes TP/SL standalone (🔴 CRÍTICO)
  Fix #23 — Nuevo método set_margin_mode() (🟡 IMPORTANTE)

Fixes anteriores (v10 y previos): ver historial git.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac as _hmac
import json
import logging
import math
import os
import random
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

# Fix #24: parámetros de retry para 429
_MAX_RETRIES_429  = int(os.getenv("BINGX_MAX_RETRIES_429", "4"))
_RETRY_BASE_DELAY = float(os.getenv("BINGX_RETRY_BASE_DELAY", "0.5"))  # segundos

# Fix #25: TTL de caché de posiciones (segundos)
_POSITIONS_CACHE_TTL = float(os.getenv("BINGX_POSITIONS_CACHE_TTL", "4"))

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
    """
    Fix #18 (v11): usa el parámetro 'secret' (antes firmaba con _API_SECRET
    global ignorando el parámetro, lo que rompía tests y uso multi-cuenta).
    """
    serialized: dict = {}
    for k, v in params.items():
        if isinstance(v, (dict, list)):
            serialized[k] = json.dumps(v, separators=(",", ":"))
        else:
            serialized[k] = v

    p = {**serialized, "timestamp": str(int(time.time() * 1000))}
    qs  = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in sorted(p.items()))
    # Fix #18: usar 'secret' parámetro, no la variable global _API_SECRET
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
            "X-BX-APIKEY":  _API_KEY,
            "X-SOURCE-KEY": "BX-AI-SKILL",
        })

        # Caches de metadatos
        self._tick_size_cache:    dict[str, float] = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._sz_decimals_cache:  dict[str, int]   = {}
        self._max_leverage_cache: dict[str, int]   = {}

        # Fix #25: caché de posiciones por símbolo con TTL
        # { inst_id: {"data": list[dict], "ts": float} }
        self._positions_cache: dict[str, dict] = {}

        # Fix #11 (v7): detectar hedge mode al inicializar.
        self.hedge_mode: bool = self._detect_hedge_mode()

        self._warm_cache()

    def _detect_hedge_mode(self) -> bool:
        """
        Fix #11 (v7): consulta /openApi/swap/v1/positionSide/dual para
        determinar si la cuenta está en HEDGE MODE (dualSidePosition=true)
        o ONE-WAY MODE (dualSidePosition=false).
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
        """
        Carga tick sizes, step sizes y max leverage de todos los contratos.

        Fix #19 (v11): pricePrecision en BingX es un entero que representa
        el número de decimales (ej: 2 → tick_sz=0.01, px_dec=2).
        La lógica anterior era innecesariamente compleja y tenía una rama
        else inalcanzable. Simplificado a: px_dec = int(pricePrecision).
        """
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

            # Fix #19: pricePrecision es número de decimales (entero)
            try:
                px_dec  = int(c.get("pricePrecision") or 2)
                tick_sz = 10 ** (-px_dec) if px_dec >= 0 else 1.0
            except Exception:
                px_dec  = 2
                tick_sz = 0.01

            try:
                sz_dec = int(c.get("quantityPrecision") or 0)
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

    # ── HTTP firmado con domain fallback y retry 429 ──────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: dict,
        body: Optional[str] = None,
    ) -> dict:
        """
        Fix #24 (v12): retry con backoff exponencial + jitter para HTTP 429.
        Antes de probar el dominio de fallback se reintenta hasta
        _MAX_RETRIES_429 veces en el mismo dominio con espera creciente.
        Los errores de red siguen propagándose al dominio de fallback como antes.
        """
        last_exc: Optional[Exception] = None
        for i, base_url in enumerate(_BASE_URLS):
            is_last = (i == len(_BASE_URLS) - 1)

            for attempt in range(_MAX_RETRIES_429 + 1):
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

                    # Fix #24: manejar 429 antes de raise_for_status
                    if r.status_code == 429:
                        if attempt < _MAX_RETRIES_429:
                            delay = _RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.3)
                            logger.warning(
                                "[BingXCore] 429 en %s %s — reintento %d/%d en %.2fs",
                                method, path, attempt + 1, _MAX_RETRIES_429, delay,
                            )
                            time.sleep(delay)
                            continue
                        # Agotados los reintentos en este dominio
                        r.raise_for_status()

                    r.raise_for_status()
                    return r.json()

                except Exception as exc:
                    last_exc = exc
                    # Errores de red: pasar al dominio de fallback (no reintentar aquí)
                    if _is_network_error(exc):
                        if not is_last:
                            logger.warning(
                                "[BingXCore] %s %s — error de red en %s, reintentando con %s: %s",
                                method, path, base_url, _BASE_URLS[i + 1], exc,
                            )
                        break  # salir del loop de intentos, probar siguiente dominio
                    # Cualquier otro error: propagar inmediatamente
                    raise

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

    # ── Leverage & Margin ─────────────────────────────────────────────────

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = False) -> dict:
        """
        Fix #17 (v10): en ONE-WAY MODE solo se llama con side='LONG'
        (BingX no acepta SHORT en one-way). En HEDGE MODE se llaman
        ambos sides (LONG y SHORT) como antes.

        Fix #12 (v8): lanza RuntimeError si BingX rechaza el cambio
        de leverage en todos los sides aplicables.
        """
        sides = ("LONG", "SHORT") if self._core.hedge_mode else ("LONG",)

        errors: dict[str, str] = {}
        ok_sides: list[str] = []

        for side in sides:
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
            raise RuntimeError(
                f"[{self.inst_id}] set_leverage({leverage}x) falló en todos los sides: "
                + " / ".join(f"{s}={errors.get(s, '?')}" for s in sides)
            )

        return {}

    def set_margin_mode(self, symbol: str, margin_type: str = "ISOLATED") -> dict:
        """
        Fix #23 (v11): configura el modo de margen del contrato via API.
        BingX endpoint: POST /openApi/swap/v2/trade/marginType
        margin_type: "ISOLATED" | "CROSSED"
        Lanza RuntimeError si BingX rechaza el cambio.
        """
        mt = margin_type.upper()
        # BingX acepta "ISOLATED" o "CROSSED" (no "CROSS")
        if mt == "CROSS":
            mt = "CROSSED"
        try:
            resp = self._core._post(
                "/openApi/swap/v2/trade/marginType",
                {"symbol": self.inst_id, "marginType": mt},
            )
            code = str(resp.get("code", "-1"))
            if code == "0":
                logger.info("[%s] set_margin_mode: OK → %s", self.inst_id, mt)
                return resp
            msg = resp.get("msg", f"code={code}")
            logger.error("[%s] set_margin_mode RECHAZADO: %s", self.inst_id, msg)
            raise RuntimeError(
                f"[{self.inst_id}] set_margin_mode({mt}) rechazado por BingX: {msg}"
            )
        except RuntimeError:
            raise
        except Exception as exc:
            logger.error("[%s] set_margin_mode excepción: %s", self.inst_id, exc)
            raise

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
        sz_r = self.round_sz(sz)
        side = "BUY" if is_buy else "SELL"
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
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true" if reduce_only else "false"
        try:
            resp = self._core._post("/openApi/swap/v3/trade/order", params)
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
        # Fix #16 (v10): stopGuaranteed como string, price='0' obligatorio,
        # serializado como JSON string explícito.
        if sl_px is not None:
            params["stopLoss"] = json.dumps({
                "type":           "STOP_MARKET",
                "stopPrice":      str(self.round_px(sl_px)),
                "price":          "0",
                "workingType":    "MARK_PRICE",
                "stopGuaranteed": "false",
            }, separators=(",", ":"))
        if tp_px is not None:
            params["takeProfit"] = json.dumps({
                "type":           "TAKE_PROFIT_MARKET",
                "stopPrice":      str(self.round_px(tp_px)),
                "price":          "0",
                "workingType":    "MARK_PRICE",
                "stopGuaranteed": "false",
            }, separators=(",", ":"))
        try:
            resp = self._core._post("/openApi/swap/v3/trade/order", params)
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
            resp = self._core._post("/openApi/swap/v3/trade/order", params)
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
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "BUY" if is_buy else "SELL"
        pos_side = self._pos_side_close(is_buy) if self._core.hedge_mode else "BOTH"
        params: dict = {
            "symbol":         self.inst_id,
            "side":           side,
            "positionSide":   pos_side,
            "type":           "TAKE_PROFIT_MARKET",
            "quantity":       str(sz_r),
            "stopPrice":      str(tpx),
            "workingType":    "MARK_PRICE",
            "stopGuaranteed": "false",
        }
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true"
        try:
            resp = self._core._post("/openApi/swap/v3/trade/order", params)
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
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "BUY" if is_buy else "SELL"
        pos_side = self._pos_side_close(is_buy) if self._core.hedge_mode else "BOTH"
        params: dict = {
            "symbol":         self.inst_id,
            "side":           side,
            "positionSide":   pos_side,
            "type":           "STOP_MARKET",
            "quantity":       str(sz_r),
            "stopPrice":      str(tpx),
            "workingType":    "MARK_PRICE",
            "stopGuaranteed": "false",
        }
        if not self._core.hedge_mode:
            params["reduceOnly"] = "true"
        try:
            resp = self._core._post("/openApi/swap/v3/trade/order", params)
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

    def get_balance_usdc(self) -> float:
        """
        Fix #20 (v11): prioriza availableMargin (margen libre) sobre balance
        total. Para un bot de trading el saldo útil es el disponible, no el
        total que incluye margen en uso.

        Estructura real BingX v3: data.balance.availableMargin / data.balance.balance
        Fallback v2: data[0].availableMargin / data[0].balance
        """
        endpoints = [
            "/openApi/swap/v3/user/balance",
            "/openApi/swap/v2/user/balance",
        ]
        for ep in endpoints:
            try:
                resp = self._core._get(ep)
                data = resp.get("data", {})
                bal = None
                if isinstance(data, dict):
                    # v3: data.balance.{availableMargin,balance}
                    inner = data.get("balance", {})
                    if isinstance(inner, dict):
                        bal = (
                            inner.get("availableMargin")
                            or inner.get("balance")
                        )
                    # fallback: campos directos en data
                    if bal is None:
                        bal = (
                            data.get("availableMargin")
                            or data.get("balance")
                        )
                elif isinstance(data, list) and data:
                    # v2: data[0].{availableMargin,balance}
                    bal = (
                        data[0].get("availableMargin")
                        or data[0].get("balance")
                    )
                if bal is not None:
                    return float(bal)
            except Exception as exc:
                logger.warning("[%s] get_balance_usdc %s error: %s", self.inst_id, ep, exc)
        logger.error("[%s] get_balance_usdc: no se pudo obtener el balance", self.inst_id)
        return 0.0

    # ── Posiciones ────────────────────────────────────────────────────────

    def get_positions(self) -> list[dict]:
        """
        Fix #13 (v9): devuelve claves compatibles con trading_loop.py.
        Fix #14 (v10): endpoint migrado a v3.
        Fix #25 (v12): caché compartida por símbolo en _BingXCore con TTL
        configurable (BINGX_POSITIONS_CACHE_TTL, default 4s).

        Con N pares activos en paralelo, todas las instancias de BingXClient
        comparten el singleton _BingXCore. Si el caché del símbolo es válido
        (ts + TTL > now), se devuelven los datos cacheados sin hacer request.
        Esto reduce N llamadas simultáneas a 1 llamada por ventana de TTL,
        eliminando el 429 por saturación del endpoint /swap/v3/user/positions.
        """
        now = time.time()
        cached = self._core._positions_cache.get(self.inst_id)
        if cached and (now - cached["ts"]) < _POSITIONS_CACHE_TTL:
            logger.debug(
                "[%s] get_positions: caché hit (age=%.2fs, TTL=%.1fs)",
                self.inst_id, now - cached["ts"], _POSITIONS_CACHE_TTL,
            )
            return cached["data"]

        try:
            resp = self._core._get(
                "/openApi/swap/v3/user/positions",
                {"symbol": self.inst_id},
            )
            raw = resp.get("data", []) or []
            positions = []
            for p in raw:
                size = float(p.get("positionAmt") or p.get("availableAmt") or 0)
                if size == 0:
                    continue
                bx_side = p.get("positionSide", "BOTH")
                if bx_side == "LONG":
                    is_long = True
                elif bx_side == "SHORT":
                    is_long = False
                else:  # BOTH (one-way)
                    is_long = size > 0

                side_str = "long" if is_long else "short"
                entry_px = float(p.get("avgPrice") or p.get("avgCost") or 0)
                size_abs = abs(size)

                positions.append({
                    # Claves que espera trading_loop.py
                    "side":    side_str,
                    "entryPx": entry_px,
                    "size":    size_abs,
                    # Claves OKX-compatibles
                    "instId":   self.inst_id,
                    "pos":      str(size_abs),
                    "posSide":  side_str,
                    "avgPx":    str(entry_px),
                    "upl":      str(p.get("unrealizedProfit") or 0),
                    "lever":    str(p.get("leverage") or _DEFAULT_LEV),
                    "liqPx":    str(p.get("liquidationPrice") or 0),
                    "margin":   str(p.get("initialMargin") or p.get("margin") or 0),
                    "mgnMode":  "cross" if p.get("marginType", "").lower() == "cross" else "isolated",
                    "_raw":     p,
                })

            # Actualizar caché
            self._core._positions_cache[self.inst_id] = {
                "data": positions,
                "ts":   now,
            }
            return positions
        except Exception as exc:
            logger.warning("[%s] get_positions error: %s", self.inst_id, exc)
            # En caso de error, devolver caché expirado si existe (mejor que nada)
            if cached:
                logger.warning(
                    "[%s] get_positions: usando caché expirado (age=%.2fs) por error de red",
                    self.inst_id, now - cached["ts"],
                )
                return cached["data"]
            return []

    # ── Órdenes abiertas ──────────────────────────────────────────────────

    def get_open_orders(self) -> list:
        """
        Fix #22 (v11): incluye órdenes TP/SL standalone.
        /openApi/swap/v3/trade/openOrders → órdenes normales (LIMIT, MARKET pendientes).
        /openApi/swap/v2/trade/openOrders → órdenes algo (STOP_MARKET, TAKE_PROFIT_MARKET).
        Se consultan ambos y se mergea el resultado.
        """
        orders: list = []

        # Órdenes normales (v3)
        try:
            resp = self._core._get(
                "/openApi/swap/v3/trade/openOrders",
                {"symbol": self.inst_id},
            )
            normal = resp.get("data", {}).get("orders", []) or []
            orders.extend(normal)
        except Exception as exc:
            logger.warning("[%s] get_open_orders (v3 normal) error: %s", self.inst_id, exc)

        # Órdenes TP/SL standalone (v2 algo orders)
        try:
            resp2 = self._core._get(
                "/openApi/swap/v2/trade/openOrders",
                {"symbol": self.inst_id},
            )
            algo = resp2.get("data", {}).get("orders", []) or []
            # Evitar duplicados por orderId
            existing_ids = {str(o.get("orderId", "")) for o in orders}
            for o in algo:
                if str(o.get("orderId", "")) not in existing_ids:
                    orders.append(o)
        except Exception as exc:
            logger.warning("[%s] get_open_orders (v2 algo) error: %s", self.inst_id, exc)

        return orders

    def cancel_all_open_tpsl(self) -> list[dict]:
        """
        Fix #21 (v11): cancela TODAS las órdenes abiertas incluyendo TP/SL standalone.
        - DELETE /openApi/swap/v3/trade/allOpenOrders → órdenes normales.
        - DELETE /openApi/swap/v2/trade/allOpenOrders → órdenes algo (TP/SL standalone).
        Se ejecutan ambas y se devuelve la lista combinada de cancelaciones.
        """
        cancelled: list[dict] = []

        # Cancelar órdenes normales (v3)
        try:
            resp = self._core._delete(
                "/openApi/swap/v3/trade/allOpenOrders",
                {"symbol": self.inst_id},
            )
            batch = resp.get("data", {}).get("orders", []) or []
            cancelled.extend(batch)
            logger.info(
                "[%s] cancel_all_open_tpsl (v3 normal): %d canceladas",
                self.inst_id, len(batch),
            )
        except Exception as exc:
            logger.error("[%s] cancel_all_open_tpsl (v3 normal) error: %s", self.inst_id, exc)

        # Cancelar órdenes TP/SL standalone (v2 algo)
        try:
            resp2 = self._core._delete(
                "/openApi/swap/v2/trade/allOpenOrders",
                {"symbol": self.inst_id},
            )
            batch2 = resp2.get("data", {}).get("orders", []) or []
            cancelled.extend(batch2)
            logger.info(
                "[%s] cancel_all_open_tpsl (v2 algo TP/SL): %d canceladas",
                self.inst_id, len(batch2),
            )
        except Exception as exc:
            logger.error("[%s] cancel_all_open_tpsl (v2 algo) error: %s", self.inst_id, exc)

        logger.info(
            "[%s] cancel_all_open_tpsl: total %d órdenes canceladas",
            self.inst_id, len(cancelled),
        )
        return cancelled

    def cancel_order(self, order_id: str) -> dict:
        try:
            resp = self._core._delete(
                "/openApi/swap/v3/trade/order",
                {"symbol": self.inst_id, "orderId": str(order_id)},
            )
            logger.info("[%s] cancel_order %s: %s", self.inst_id, order_id, resp.get("code"))
            return resp
        except Exception as exc:
            logger.error("[%s] cancel_order %s error: %s", self.inst_id, order_id, exc)
            return {"code": "-1", "msg": str(exc)}
