"""exchange.py — Cliente BingX Perpetual Futures (swap v2)."""
import hashlib
import hmac
import time
import urllib.parse
import logging

import httpx

import config

log = logging.getLogger("exchange")


def _sign(params: dict) -> str:
    payload = urllib.parse.urlencode(sorted(params.items()))
    return hmac.new(config.API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()


def _headers() -> dict:
    return {"X-BX-APIKEY": config.API_KEY}


def _get(path: str, params: dict = None) -> dict:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    r = httpx.get(config.BASE_URL + path, params=params, headers=_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


def _post(path: str, params: dict = None) -> dict:
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    params["signature"] = _sign(params)
    r = httpx.post(config.BASE_URL + path, data=params, headers=_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


# ── Precio ────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    return float(data["data"]["price"])


# ── OHLCV ─────────────────────────────────────────────────────────────────────

def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    """Devuelve lista de velas [{open, high, low, close, volume}] más reciente al final."""
    symbol   = symbol or config.SYMBOL
    interval = interval or config.TIMEFRAME
    data = _get("/openApi/swap/v3/quote/klines", {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    })
    candles = []
    for c in data["data"]:
        candles.append({
            "open":   float(c["open"]),
            "high":   float(c["high"]),
            "low":    float(c["low"]),
            "close":  float(c["close"]),
            "volume": float(c["volume"]),
        })
    return candles


# ── Posición abierta ──────────────────────────────────────────────────────────

def get_position(symbol: str = None) -> dict | None:
    """Devuelve la posición abierta o None si no hay ninguna."""
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/user/positions", {"symbol": symbol})
    positions = data.get("data") or []
    for p in positions:
        if float(p.get("positionAmt", 0)) != 0:
            return {
                "side":  "long" if float(p["positionAmt"]) > 0 else "short",
                "entry": float(p["avgPrice"]),
                "size":  abs(float(p["positionAmt"])),
                "sl":    float(p.get("stopLossPrice") or 0) or None,
                "tp":    float(p.get("takeProfitPrice") or 0) or None,
            }
    return None


# ── Apalancamiento ────────────────────────────────────────────────────────────

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


# ── Abrir orden ───────────────────────────────────────────────────────────────

def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    """
    Abre una orden market con SL y TP como stop-orders separadas.
    side: 'long' | 'short'
    """
    symbol    = symbol or config.SYMBOL
    bx_side   = "BUY"  if side == "long" else "SELL"
    pos_side  = "LONG" if side == "long" else "SHORT"

    # Orden market de entrada
    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":       symbol,
        "side":         bx_side,
        "positionSide": pos_side,
        "type":         "MARKET",
        "quantity":     qty,
    })
    log.info("Orden abierta: %s %s qty=%.4f", side.upper(), symbol, qty)

    # Stop Loss
    sl_side = "SELL" if side == "long" else "BUY"
    _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          sl_side,
        "positionSide":  pos_side,
        "type":          "STOP_MARKET",
        "stopPrice":     sl,
        "quantity":      qty,
        "closePosition": "true",
    })
    log.info("SL colocado en %.4f", sl)

    # Take Profit
    _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          sl_side,
        "positionSide":  pos_side,
        "type":          "TAKE_PROFIT_MARKET",
        "stopPrice":     tp,
        "quantity":      qty,
        "closePosition": "true",
    })
    log.info("TP colocado en %.4f", tp)

    return resp


# ── Cerrar posición ───────────────────────────────────────────────────────────

def close_position(side: str, qty: float, symbol: str = None) -> dict:
    """Cierra la posición con una orden market."""
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


# ── Cancelar todas las órdenes abiertas ───────────────────────────────────────

def cancel_all_orders(symbol: str = None) -> None:
    symbol = symbol or config.SYMBOL
    _post("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
    log.info("Órdenes abiertas canceladas para %s", symbol)
