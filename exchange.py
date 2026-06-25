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
  8. _parse_position() prioriza positionSide (campo explícito en hedge mode) sobre
     la inferencia por signo de positionAmt. En hedge mode BingX siempre devuelve
     positionSide="LONG"|"SHORT"; positionAmt puede ser negativo o transitoriamente
     cero en LONG si la orden aún no está completamente liquidada.
     Fallback a positionAmt si positionSide no está disponible (one-way mode).
  9. get_closed_orders() añadida (FIX #2): antes no existía y _get_real_exit_price()
     en main.py lanzaba AttributeError silenciado, devolviendo siempre precio estimado.
"""
import hashlib
import hmac
import time
import urllib.parse
import logging

import httpx

import config

log = logging.getLogger("exchange")

# ── Firma ─────────────────────────────────────────────────────────────────────────────────

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


# ── HTTP helpers con reintentos ────────────────────────────────────────────────────

_RETRIES    = 3
_RETRY_WAIT = 1.0


def _request(method: str, path: str, params: dict) -> dict:
    """GET, POST o DELETE enviando siempre los parámetros en la query string (BingX V2).

    FIX: timestamp y firma se renuevan en cada intento para que un timeout en el
    primer intento no deje el segundo con un timestamp expirado (BingX rechaza >5s).
    """
    base_params = {k: v for k, v in params.items() if k not in ("timestamp", "signature")}
    url = config.BASE_URL + path

    last_exc: Exception | None = None
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


# ── Precio ──────────────────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    return float(data["data"]["price"])


# ── OHLCV ────────────────────────────────────────────────────────────────────────────────────

def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    """Devuelve lista de velas [{ts, open, high, low, close, volume}] más reciente al final.

    FIX: se incluye 'ts' (timestamp en ms) para que ws_feed pueda deduplicar
    correctamente las velas precargadas REST con las que llegan por WebSocket.
    """
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


# ── Info de contrato (step size / min qty) ─────────────────────────────────────────────

_contract_info_cache: dict[str, dict] = {}

def _get_contract_info(symbol: str) -> dict:
    """Devuelve stepSize y minQty para el símbolo. Cachea para no repetir llamadas."""
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
    """Verifica que el valor nocional sea ≥ min_usdt (BingX exige mínimo ~5 USDT)."""
    return (qty * price) >= min_usdt


# ── Posiciones ─────────────────────────────────────────────────────────────────────────────────

# Mapa de positionSide (hedge mode) a valor interno del bot
_POSITION_SIDE_MAP = {
    "LONG":  "long",
    "SHORT": "short",
    "long":  "long",
    "short": "short",
}


def _parse_position(p: dict) -> dict | None:
    """Convierte un objeto de posición raw de BingX al formato interno del bot.

    BingX hedge mode devuelve siempre positionSide="LONG"|"SHORT" — este campo
    es el indicador canónico del lado de la posición y se usa con prioridad.

    En one-way mode positionSide puede venir como "BOTH" o ausente; en ese caso
    se infiere el lado por el signo de positionAmt (positivo → long, negativo → short).

    Si ninguna de las dos fuentes produce un side válido, se devuelve None y
    la posición se descarta con un warning — esto evita que un payload malformado
    de BingX inyecte un side incorrecto en el estado del bot.
    """
    raw_side     = str(p.get("positionSide") or "").upper()
    position_amt = float(p.get("positionAmt") or 0)

    if raw_side in ("LONG", "SHORT"):
        # Hedge mode: positionSide es explícito y fiable
        side = _POSITION_SIDE_MAP[raw_side]
    elif position_amt > 0:
        # One-way mode long o fallback
        side = "long"
    elif position_amt < 0:
        # One-way mode short o fallback
        side = "short"
    else:
        # positionAmt == 0 y sin positionSide válido → posición vacía o en tránsito
        log.warning(
            "Posición descartada — positionSide=%r positionAmt=%s (símbolo=%s)",
            p.get("positionSide"), p.get("positionAmt"), p.get("symbol"),
        )
        return None

    return {
        "side":  side,
        "entry": float(p.get("avgPrice") or 0),
        "size":  abs(position_amt),
        "sl":    float(p.get("stopLossPrice") or 0) or None,
        "tp":    float(p.get("takeProfitPrice") or 0) or None,
    }


def get_all_positions() -> dict[str, dict]:
    """BATCH: devuelve {symbol: pos_dict} para todas las posiciones abiertas.

    FIX: ya no captura la excepción silenciosamente. Si la llamada falla, lanza
    la excepción para que main.py la capture en su try/except de loop y salte
    la iteración completa, evitando que un {} vacío provoque el cierre falso
    de todas las posiciones rastreadas.
    """
    data = _get("/openApi/swap/v2/user/positions", {})
    result: dict[str, dict] = {}
    for p in (data.get("data") or []):
        if float(p.get("positionAmt", 0)) == 0:
            continue
        parsed = _parse_position(p)
        if parsed is not None:
            result[p["symbol"]] = parsed
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


# ── Apalancamiento ───────────────────────────────────────────────────────────────────────────────

def set_leverage(symbol: str = None, leverage: int = None) -> None:
    symbol   = symbol or config.SYMBOL
    leverage = leverage or config.LEVERAGE
    _post("/openApi/swap/v2/trade/leverage", {
        "symbol":   symbol,
        "side":     "LONG",
        "leverage": leverage,
    })
    _post("/openApi/swap/v2/trade/leverage", {
        "symbol":   symbol,
        "side":     "SHORT",
        "leverage": leverage,
    })
    log.info("Leverage seteado a %dx en %s", leverage, symbol)


# ── Abrir orden ───────────────────────────────────────────────────────────────────────────────────

def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    symbol   = symbol or config.SYMBOL
    bx_side  = "BUY"  if side == "long" else "SELL"
    pos_side = "LONG" if side == "long" else "SHORT"

    info = _get_contract_info(symbol)
    qty  = floor_qty(qty, info["stepSize"])

    if qty <= 0 or not min_notional_ok(qty, get_price(symbol)):
        raise ValueError(
            f"qty={qty} inválido para {symbol} (step={info['stepSize']}). "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         bx_side,
        "positionSide": pos_side,
        "type":         "MARKET",
        "quantity":     qty,
    })
    log.info("Orden abierta: %s %s qty=%.4f", side.upper(), symbol, qty)

    place_stop_order(symbol, side, qty, sl)
    place_tp_order(symbol, side, qty, tp)

    return resp


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> None:
    """Coloca (o reemplaza) la stop-loss order. Usada también por el trailing."""
    sl_side  = "SELL" if side == "long" else "BUY"
    pos_side = "LONG" if side == "long" else "SHORT"
    _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          sl_side,
        "positionSide":  pos_side,
        "type":          "STOP_MARKET",
        "stopPrice":     stop_price,
        "quantity":      qty,
        "closePosition": "true",
    })
    log.info("SL colocado en %.6f (%s %s)", stop_price, side.upper(), symbol)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    """Coloca la take-profit order."""
    sl_side  = "SELL" if side == "long" else "BUY"
    pos_side = "LONG" if side == "long" else "SHORT"
    _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          sl_side,
        "positionSide":  pos_side,
        "type":          "TAKE_PROFIT_MARKET",
        "stopPrice":     tp_price,
        "quantity":      qty,
        "closePosition": "true",
    })
    log.info("TP colocado en %.6f (%s %s)", tp_price, side.upper(), symbol)


# ── Cerrar posición ────────────────────────────────────────────────────────────────────────────────

def close_position(side: str, qty: float, symbol: str = None) -> dict:
    symbol   = symbol or config.SYMBOL
    bx_side  = "SELL" if side == "long" else "BUY"
    pos_side = "LONG" if side == "long" else "SHORT"
    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         bx_side,
        "positionSide": pos_side,
        "type":         "MARKET",
        "quantity":     qty,
    })
    log.info("Posición cerrada: %s %s", side.upper(), symbol)
    return resp


# ── Cancelar órdenes abiertas ────────────────────────────────────────────────────────────────────

def cancel_all_orders(symbol: str = None) -> None:
    """FIX: BingX exige DELETE (no POST) para cancelar todas las órdenes abiertas.
    Con POST la API devuelve 405 silencioso, dejando SL/TP duplicados en el exchange.
    """
    symbol = symbol or config.SYMBOL
    _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
    log.info("Órdenes canceladas para %s", symbol)


# ── Historial de órdenes cerradas ───────────────────────────────────────────────────────────────────

def get_closed_orders(symbol: str = None, limit: int = 10) -> list[dict]:
    """FIX #2: Devuelve órdenes ejecutadas/cerradas del símbolo para obtener el
    precio real de salida.

    Antes esta función no existía. _get_real_exit_price() en main.py la llamaba
    y lanzaba AttributeError silenciado por el except Exception, cayendo siempre
    al precio estimado. El CSV y los mensajes de Telegram nunca reflejaban el
    precio real de cierre del exchange.

    BingX endpoint: GET /openApi/swap/v2/trade/allOrders
    Se filtran solo órdenes en estado terminal (FILLED, CANCELED, etc.).
    Si el endpoint falla o devuelve vacío, _get_real_exit_price() usa el fallback.
    """
    symbol = symbol or config.SYMBOL
    try:
        data = _get("/openApi/swap/v2/trade/allOrders", {"symbol": symbol, "limit": limit})
    except Exception as exc:
        log.debug("[%s] get_closed_orders falló: %s", symbol, exc)
        return []

    raw = data.get("data") or []
    if isinstance(raw, dict):
        raw = raw.get("orders") or raw.get("list") or []

    terminal = {"FILLED", "CANCELED", "PARTIALLY_FILLED", "PARTIALLY_CANCELED"}
    return [o for o in raw if str(o.get("status", "")).upper() in terminal]
