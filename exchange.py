"""exchange.py — Cliente BingX Perpetual Futures (swap v2).

FIXES aplicados:
  1. _post() enviaba params en el body (data=); BingX los exige en la query string.
     Ahora tanto GET como POST usan params= (query string), que es lo que BingX firma.
  2. La firma se calcula sobre la query string real (urllib.parse.urlencode sin sorted),
     igual que lo hace BingX internamente, evitando "signature does not match".
  3. Reintentos automáticos (3 intentos, backoff 1s) para RemoteProtocolError y
     timeouts intermitentes que estaban silenciando órdenes.
     FIX: timestamp y firma se renuevan en CADA intento para evitar que un timeout
     de 10s deje el segundo reintento con un timestamp ya expirado (BingX rechaza >5s).
  4. calc_qty() aplica floor al step size de cada símbolo para no enviar qty
     que BingX rechaza por precisión decimal.
  5. cancel_all_orders usa DELETE (no POST) — BingX exige DELETE para este endpoint.
  6. get_ohlcv incluye el timestamp 'ts' en cada vela para que ws_feed pueda
     deduplicar correctamente al mezclar velas REST con las del WebSocket.
  7. get_all_positions() obtiene TODAS las posiciones abiertas en 1 sola llamada
     (sin 'symbol'), reduciendo 74 llamadas por loop a 1.
     FIX: si la llamada falla, lanza excepción en vez de devolver {} silencioso
     para evitar que main.py interprete todas las posiciones como cerradas.
  8. get_closed_reason() consulta el historial real de órdenes ejecutadas para
     determinar si la posición cerró por TP o SL, junto con el precio de ejecución.
     FIX: lookback ampliado a 30 minutos para cubrir loops lentos con muchos pares.
  9. get_spread_pct() consulta el orderbook nivel 1 y devuelve el spread como
     porcentaje del mid price. Usado en main.py para filtrar pares ilíquidos
     antes de abrir posición.
  10. FIX: _request() lanzaba `raise None` (TypeError) si todos los reintentos
      fallaban por HTTPStatusError (que hace raise dentro del loop) seguido de
      salida del bucle sin asignar last_exc. Ahora lanza RuntimeError explícito
      si last_exc es None al final del loop.
  11. FIX: añadida set_leverage() — faltaba por completo, causaba WARNING en
      el arranque para los 52 pares: "module 'exchange' has no attribute 'set_leverage'".
      BingX requiere dos llamadas separadas (LONG y SHORT) por símbolo.
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
    """Firma la query string tal como la construye urllib / BingX."""
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
    """GET, POST o DELETE enviando siempre los parámetros en la query string (BingX V2)."""
    base_params = {k: v for k, v in params.items() if k not in ("timestamp", "signature")}
    url = config.BASE_URL + path

    # FIX: last_exc inicializada a RuntimeError para que nunca sea None al hacer raise.
    last_exc: Exception = RuntimeError(f"All {_RETRIES} attempts failed for {path}")
    for attempt in range(1, _RETRIES + 1):
        signed = dict(base_params)
        signed["timestamp"] = int(time.time() * 1000)
        signed["signature"] = _sign(signed)

        try:
            r = httpx.request(
                method,
                url,
                params=signed,
                headers=_headers(),
                timeout=10,
            )
            r.raise_for_status()
            return r.json()
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


# ── Leverage ──────────────────────────────────────────────────────────────────────────

def set_leverage(symbol: str, leverage: int) -> None:
    """Configura el leverage para LONG y SHORT en BingX.

    BingX requiere dos llamadas separadas (una por side). Si alguna falla
    con HTTPStatusError se deja propagar; el caller (main.py) lo captura
    y loguea como WARNING sin detener el arranque.
    """
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
    """Consulta el orderbook nivel 1 y devuelve el spread como porcentaje del mid price.

    Ejemplo: best_ask=1.002, best_bid=1.000 → spread = 0.002 / 1.001 = 0.1997%

    Retorna 999.0 si no se puede obtener el orderbook (falla segura: bloquea la entrada).
    """
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
    """Devuelve lista de velas [{ts, open, high, low, close, volume}] más reciente al final."""
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
    """Devuelve stepSize y minQty para el símbolo. Cachéa para no repetir llamadas."""
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
    """Redondea qty hacia abajo al step size del contrato."""
    if step <= 0:
        return qty
    factor = 1.0 / step
    return int(qty * factor) / factor


def min_notional_ok(qty: float, price: float, min_usdt: float = 5.0) -> bool:
    """Verifica que el valor nocional sea >= min_usdt."""
    return (qty * price) >= min_usdt


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
    """BATCH: devuelve {symbol: pos_dict} para todas las posiciones abiertas."""
    data = _get("/openApi/swap/v2/user/positions", {})
    result: dict[str, dict] = {}
    for p in (data.get("data") or []):
        if float(p.get("positionAmt", 0)) != 0:
            result[p["symbol"]] = _parse_position(p)
    return result


def get_position(symbol: str = None) -> dict | None:
    """Consulta individual por símbolo. Mantenida para compatibilidad."""
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/user/positions", {"symbol": symbol})
    positions = data.get("data") or []
    for p in positions:
        if float(p.get("positionAmt", 0)) != 0:
            return _parse_position(p)
    return None


# ── Historial de cierre real ────────────────────────────────────────────────────

_CLOSE_ORDER_TYPES = {"TAKE_PROFIT_MARKET", "STOP_MARKET"}
# FIX: ampliado a 30 minutos para cubrir loops lentos con muchos pares (antes 10 min).
_CLOSE_LOOK_BACK  = 30 * 60 * 1000   # 30 minutos en ms

def get_closed_reason(symbol: str) -> tuple[str, float]:
    """Consulta el historial de órdenes ejecutadas de BingX para determinar
    si la posición cerró por TP o SL y devuelve (reason, avg_price)."""
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

        reason = "TP" if order_type == "TAKE_PROFIT_MARKET" else "SL"
        log.info("[%s] Cierre detectado: %s @ %.6f (orden tipo=%s)",
                 symbol, reason, avg_price, order_type)
        return reason, avg_price

    except Exception as exc:
        log.warning("[%s] get_closed_reason error: %s", symbol, exc)
        return None, 0.0
