"""exchange.py — Cliente BingX Perpetual Futures (swap v2).

FIXES aplicados:
  1. _post() enviaba params en el body (data=); BingX los exige en la query string.
  2. La firma se calcula sobre la query string real (urllib.parse.urlencode sin sorted).
  3. Reintentos automáticos con timestamp renovado en cada intento.
  4. calc_qty() aplica floor al step size.
  5. cancel_all_orders usa DELETE.
  6. get_ohlcv incluye el timestamp 'ts' en cada vela.
  7. get_all_positions() batch en 1 sola llamada.
  8. get_closed_reason() lookback ampliado a 30 minutos.
  9. get_spread_pct() consulta el orderbook nivel 1.
  10. _request() lanza RuntimeError explícito si todos los reintentos fallan.
  11. set_leverage() añadida.
  12. open_order(), place_stop_order() y place_tp_order() añadidas.
  13. open_order() separada en MARKET + SL/TP independientes.
  14. FIX CRÍTICO: _request() ahora valida code != 0 en el body (HTTP 200 con error).
  15. quantity y stopPrice enviados como strings formateados (evita notación científica).
  16. FIX: añadido positionSide a todas las órdenes — la cuenta está en modo Hedge,
      que requiere LONG/SHORT explícito. La orden de apertura usa el lado de la
      posición. Las órdenes de cierre (SL/TP) usan el lado opuesto y eliminan
      reduceOnly (incompatible con positionSide en BingX).
"""
import hashlib
import hmac
import time
import urllib.parse
import logging

import httpx

import config

log = logging.getLogger("exchange")

# ── Firma ─────────────────────────────────────────────────────────────────────────────

def _sign(params: dict) -> str:
    payload = urllib.parse.urlencode(params)
    return hmac.new(
        config.API_SECRET.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()


def _headers() -> dict:
    return {"X-BX-APIKEY": config.API_KEY}


# ── HTTP helpers con reintentos ──────────────────────────────────────────────

_RETRIES    = 3
_RETRY_WAIT = 1.0


def _request(method: str, path: str, params: dict) -> dict:
    base_params = {k: v for k, v in params.items() if k not in ("timestamp", "signature")}
    url = config.BASE_URL + path

    last_exc: Exception = RuntimeError(f"All {_RETRIES} attempts failed for {path}")
    for attempt in range(1, _RETRIES + 1):
        signed = dict(base_params)
        signed["timestamp"] = int(time.time() * 1000)
        signed["signature"] = _sign(signed)

        try:
            r = httpx.request(method, url, params=signed, headers=_headers(), timeout=10)
            r.raise_for_status()
            body = r.json()

            code = body.get("code", 0)
            if code != 0:
                msg = body.get("msg", "sin mensaje")
                log.error("[BingX] Error en %s: code=%s msg=%s | params=%s",
                          path, code, msg, base_params)
                raise RuntimeError(f"BingX error {code}: {msg}")

            return body

        except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as exc:
            last_exc = exc
            log.warning("Intento %d/%d fallido para %s: %s", attempt, _RETRIES, path, exc)
            if attempt < _RETRIES:
                time.sleep(_RETRY_WAIT)
        except httpx.HTTPStatusError as exc:
            log.error("HTTP %s en %s: %s", exc.response.status_code, path, exc.response.text)
            raise

    raise last_exc


def _get(path: str, params: dict = None) -> dict:
    return _request("GET", path, params or {})

def _post(path: str, params: dict = None) -> dict:
    return _request("POST", path, params or {})

def _delete(path: str, params: dict = None) -> dict:
    return _request("DELETE", path, params or {})


# ── Helpers de formato ───────────────────────────────────────────────────────────

def _fmt_qty(qty: float) -> str:
    """Sin notación científica (BingX rechaza '1e-04')."""
    return f"{qty:.8f}".rstrip("0").rstrip(".")

def _fmt_price(price: float) -> str:
    return f"{price:.8f}".rstrip("0").rstrip(".")


# ── Leverage ──────────────────────────────────────────────────────────────────────────

def set_leverage(symbol: str, leverage: int) -> None:
    for side in ("LONG", "SHORT"):
        _post("/openApi/swap/v2/trade/leverage", {
            "symbol":   symbol,
            "side":     side,
            "leverage": leverage,
        })
    log.debug("[%s] Leverage seteado a %dx (LONG+SHORT)", symbol, leverage)


# ── Precio ───────────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    return float(data["data"]["price"])


# ── Spread / liquidez ────────────────────────────────────────────────────────────

def get_spread_pct(symbol: str) -> float:
    try:
        data = _get("/openApi/swap/v2/quote/depth", {"symbol": symbol, "limit": 5})
        book = data.get("data", {})
        asks = book.get("asks") or []
        bids = book.get("bids") or []
        if not asks or not bids:
            return 999.0
        best_ask = float(asks[0][0])
        best_bid = float(bids[0][0])
        if best_ask <= 0 or best_bid <= 0:
            return 999.0
        mid = (best_ask + best_bid) / 2
        return (best_ask - best_bid) / mid * 100
    except Exception as exc:
        log.warning("[%s] get_spread_pct error: %s", symbol, exc)
        return 999.0


# ── OHLCV ───────────────────────────────────────────────────────────────────────────

def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    symbol   = symbol or config.SYMBOL
    interval = interval or config.TIMEFRAME
    data = _get("/openApi/swap/v3/quote/klines", {
        "symbol":   symbol,
        "interval": interval,
        "limit":    limit,
    })
    candles = []
    for c in data["data"]:
        candles.append({
            "ts":     int(c.get("time", c.get("t", 0))),
            "open":   float(c["open"]),
            "high":   float(c["high"]),
            "low":    float(c["low"]),
            "close":  float(c["close"]),
            "volume": float(c["volume"]),
            "closed": True,
        })
    return candles


# ── Info de contrato (step size / min qty) ────────────────────────────────────

_contract_info_cache: dict[str, dict] = {}

def _get_contract_info(symbol: str) -> dict:
    if symbol in _contract_info_cache:
        return _contract_info_cache[symbol]
    try:
        data = _get("/openApi/swap/v2/quote/contracts", {"symbol": symbol})
        contracts = data.get("data") or []
        for c in contracts:
            if c.get("symbol") == symbol:
                info = {
                    "stepSize": float(c.get("tradeMinQuantity", 0.001)),
                    "minQty":   float(c.get("tradeMinQuantity", 0.001)),
                    "pricePrecision": int(c.get("pricePrecision", 6)),
                }
                _contract_info_cache[symbol] = info
                return info
    except Exception as exc:
        log.warning("No se pudo obtener info contrato %s: %s", symbol, exc)

    default = {"stepSize": 0.001, "minQty": 0.001, "pricePrecision": 6}
    _contract_info_cache[symbol] = default
    return default


def floor_qty(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    factor = 1.0 / step
    return int(qty * factor) / factor


def min_notional_ok(qty: float, price: float, min_usdt: float = 5.0) -> bool:
    return (qty * price) >= min_usdt


# ── Órdenes ──────────────────────────────────────────────────────────────────────────

# Mapa: lado de la posición (bot) → positionSide de BingX (Hedge mode)
_POSITION_SIDE = {"long": "LONG", "short": "SHORT"}


def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    """Abre posición MARKET en modo Hedge (positionSide requerido por BingX).

    Paso 1: orden MARKET de apertura con positionSide.
    Paso 2: SL y TP con llamadas separadas tras 0.5s de espera.
    """
    symbol       = symbol or config.SYMBOL
    bingx_side   = "BUY" if side == "long" else "SELL"
    position_side = _POSITION_SIDE[side]

    info = _get_contract_info(symbol)
    qty  = floor_qty(qty, info["stepSize"])

    log.info("[%s] Abriendo MARKET %s qty=%s sl=%s tp=%s",
             symbol, side.upper(), _fmt_qty(qty), _fmt_price(sl), _fmt_price(tp))

    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         bingx_side,
        "positionSide": position_side,
        "type":         "MARKET",
        "quantity":     _fmt_qty(qty),
    })
    log.info("[%s] MARKET ejecutada: %s orderId=%s",
             symbol, side.upper(),
             resp.get("data", {}).get("order", {}).get("orderId", "?"))

    time.sleep(0.5)
    try:
        place_stop_order(symbol, side, qty, sl)
    except Exception as e:
        log.warning("[%s] Error colocando SL: %s", symbol, e)
    try:
        place_tp_order(symbol, side, qty, tp)
    except Exception as e:
        log.warning("[%s] Error colocando TP: %s", symbol, e)

    return resp


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> dict:
    """SL (STOP_MARKET) en modo Hedge.

    En Hedge mode la orden de cierre lleva el positionSide de la posición
    que queremos cerrar (no el lado de la orden). reduceOnly es incompatible
    con positionSide en BingX y se omite.
    """
    # En Hedge: para cerrar un LONG se vende con side=SELL + positionSide=LONG
    close_side    = "SELL" if side == "long" else "BUY"
    position_side = _POSITION_SIDE[side]   # el lado de la posición que cierro

    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         close_side,
        "positionSide": position_side,
        "type":         "STOP_MARKET",
        "quantity":     _fmt_qty(qty),
        "stopPrice":    _fmt_price(stop_price),
        "workingType":  "MARK_PRICE",
    })
    log.info("[%s] SL colocado: %s qty=%s @ %s",
             symbol, side.upper(), _fmt_qty(qty), _fmt_price(stop_price))
    return resp


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> dict:
    """TP (TAKE_PROFIT_MARKET) en modo Hedge.

    Igual que place_stop_order: positionSide del lado que cierro, sin reduceOnly.
    """
    close_side    = "SELL" if side == "long" else "BUY"
    position_side = _POSITION_SIDE[side]

    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         close_side,
        "positionSide": position_side,
        "type":         "TAKE_PROFIT_MARKET",
        "quantity":     _fmt_qty(qty),
        "stopPrice":    _fmt_price(tp_price),
        "workingType":  "MARK_PRICE",
    })
    log.info("[%s] TP colocado: %s qty=%s @ %s",
             symbol, side.upper(), _fmt_qty(qty), _fmt_price(tp_price))
    return resp


def cancel_all_orders(symbol: str) -> dict:
    try:
        resp = _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
        log.debug("[%s] Órdenes canceladas", symbol)
        return resp
    except Exception as exc:
        log.warning("[%s] cancel_all_orders error: %s", symbol, exc)
        return {}


# ── Posiciones ─────────────────────────────────────────────────────────────────────

def _parse_position(p: dict) -> dict:
    return {
        "side":  "long" if float(p["positionAmt"]) > 0 else "short",
        "entry": float(p["avgPrice"]),
        "size":  abs(float(p["positionAmt"])),
        "sl":    float(p.get("stopLossPrice") or 0) or None,
        "tp":    float(p.get("takeProfitPrice") or 0) or None,
    }


def get_all_positions() -> dict[str, dict]:
    data = _get("/openApi/swap/v2/user/positions", {})
    result: dict[str, dict] = {}
    for p in (data.get("data") or []):
        if float(p.get("positionAmt", 0)) != 0:
            result[p["symbol"]] = _parse_position(p)
    return result


def get_position(symbol: str = None) -> dict | None:
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/user/positions", {"symbol": symbol})
    for p in (data.get("data") or []):
        if float(p.get("positionAmt", 0)) != 0:
            return _parse_position(p)
    return None


# ── Historial de cierre real ────────────────────────────────────────────────────

_CLOSE_ORDER_TYPES = {"TAKE_PROFIT_MARKET", "STOP_MARKET"}
_CLOSE_LOOK_BACK  = 30 * 60 * 1000

def get_closed_reason(symbol: str) -> tuple[str, float]:
    try:
        start_ts = int(time.time() * 1000) - _CLOSE_LOOK_BACK
        data = _get("/openApi/swap/v2/trade/allOrders", {
            "symbol":    symbol,
            "startTime": start_ts,
            "limit":     20,
        })
        orders = data.get("data", {}).get("orders") or []
        filled = [
            o for o in orders
            if o.get("status") == "FILLED"
            and o.get("type") in _CLOSE_ORDER_TYPES
        ]
        if not filled:
            log.warning("[%s] get_closed_reason: sin órdenes FILLED en últimos 30min", symbol)
            return None, 0.0

        filled.sort(key=lambda o: int(o.get("updateTime", 0)), reverse=True)
        latest     = filled[0]
        order_type = latest.get("type", "")
        avg_price  = float(latest.get("avgPrice") or latest.get("stopPrice") or 0)
        reason     = "TP" if order_type == "TAKE_PROFIT_MARKET" else "SL"
        log.info("[%s] Cierre detectado: %s @ %.6f", symbol, reason, avg_price)
        return reason, avg_price

    except Exception as exc:
        log.warning("[%s] get_closed_reason error: %s", symbol, exc)
        return None, 0.0
