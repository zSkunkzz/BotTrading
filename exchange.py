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
  6. get_ohlcv: el endpoint /openApi/swap/v3/quote/klines devuelve cada vela como un
     ARRAY de 11 elementos (doc oficial BingX):
       [0]=open_time [1]=open [2]=high [3]=low [4]=close [5]=volume
       [6]=close_time [7]=quote_vol [8]=trades [9]=taker_buy_base [10]=taker_buy_quote
     Antes se accedía a c["open"] etc. como si fuera un dict → KeyError silenciado.
     Ahora se parsea por índice. Se incluye 'ts' (= c[0]) para deduplicación en ws_feed.
  7. get_all_positions() obtiene TODAS las posiciones abiertas en 1 sola llamada
     (sin 'symbol'), reduciendo 74 llamadas por loop a 1.
     FIX: si la llamada falla, lanza excepción en vez de devolver {} silencioso
     para evitar que main.py interprete todas las posiciones como cerradas.
  8. _parse_position() prioriza positionSide (campo explícito en hedge mode) sobre
     la inferencia por signo de positionAmt. En hedge mode BingX siempre devuelve
     positionSide="LONG"|"SHORT"; positionAmt puede ser negativo o transitoriamente
     cero en LONG si la orden aún no está completamente liquidada.
     Fallback a positionAmt si positionSide no está disponible (one-way mode).
  9. FIX #2: get_closed_orders() implementada — antes no existía, causando
     AttributeError silenciado en _get_real_exit_price() y precios de cierre
     siempre estimados. Ahora consulta /openApi/swap/v2/trade/allOrders.
 10. get_balance() corregida según doc oficial BingX (BingX-API/api-ai-skills):
     - Endpoint v3: /openApi/swap/v3/user/balance
     - data es un ARRAY de objetos, no un dict — se filtra por asset=USDT
     - Campos: equity (balance + unrealizedPnL), balance, availableMargin
 11. FIX A — place_stop_order / place_tp_order:
     quantity + closePosition="true" juntos → BingX rechaza con error 101400.
     Doc oficial: "closePosition cannot be used with quantity".
     Eliminado el campo quantity cuando closePosition=true está presente.
 12. FIX B — get_closed_orders: parseo de respuesta corregido.
     Doc oficial: data es directamente un array de órdenes, sin wrapping.
     Eliminado el bloque isinstance(raw, dict) con .orders/.list inexistentes.
 13. FIX C — get_closed_orders: añadidos startTime/endTime obligatorios.
     Doc oficial: sin rango temporal BingX puede devolver error 109400.
     Se incluye startTime=ahora-7d, endTime=ahora en cada llamada.
 14. FIX D — get_ohlcv: velas son arrays, no dicts.
     Doc oficial GET /openApi/swap/v3/quote/klines: cada vela es un array de 11
     elementos indexados por posición, NO un objeto con claves "open"/"high"/etc.
     Se parsea ahora por índice: c[0]=ts, c[1]=open, c[2]=high, c[3]=low, c[4]=close,
     c[5]=volume. Esto afectaba a TODO el bot (signals, risk, trailing, BE) porque
     ninguna vela tenía datos reales — todas las velas eran dicts vacíos.
 15. BUG-13 FIX — get_closed_orders: eliminado PARTIALLY_FILLED del set terminal.
     PARTIALLY_FILLED aparecía antes que FILLED en el historial de órdenes. Si
     _get_real_exit_price() lo encontraba primero, calculaba el PnL con precio
     parcial incorrecto. Solo FILLED es ejecución completa válida.
"""
import hashlib
import hmac
import time
import urllib.parse
import logging

import httpx

import config

log = logging.getLogger("exchange")

# ── Firma ────────────────────────────────────────────────────────────────────────────────────────

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


# ── HTTP helpers con reintentos ────────────────────────────────────────────

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


# ── Balance real ───────────────────────────────────────────────────────────────────

def get_balance() -> float:
    """Devuelve el equity real (balance + PnL no realizado) de la cuenta USDT.

    Endpoint oficial: GET /openApi/swap/v3/user/balance
    Respuesta: { "data": [ { "asset": "USDT", "equity": "196.74", ... } ] }

    data es un ARRAY — se busca el objeto con asset=USDT y se lee 'equity'.
    Fallback: 'balance' si no existe 'equity'. Devuelve 0.0 si falla la llamada
    para que bot_state.py use el capital ficticio como backup.

    Fuente: BingX-API/api-ai-skills/skills/swap-account/api-reference.md
    """
    try:
        resp = _get("/openApi/swap/v3/user/balance", {})
        items = resp.get("data") or []
        usdt = next(
            (x for x in items if str(x.get("asset", "")).upper() == "USDT"),
            items[0] if items else None,
        )
        if usdt is None:
            log.warning("get_balance: payload vacío de BingX: %s", resp)
            return 0.0
        val = usdt.get("equity") or usdt.get("balance") or usdt.get("availableMargin")
        if val is not None:
            return float(val)
        log.warning("get_balance: ningún campo de balance en: %s", usdt)
        return 0.0
    except Exception as exc:
        log.warning("get_balance falló: %s — se usará capital ficticio como fallback", exc)
        return 0.0


# ── Precio ────────────────────────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    symbol = symbol or config.SYMBOL
    data = _get("/openApi/swap/v2/quote/price", {"symbol": symbol})
    return float(data["data"]["price"])


# ── OHLCV ──────────────────────────────────────────────────────────────────────────────────────────

def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    """Devuelve lista de velas [{ts, open, high, low, close, volume}] más reciente al final.

    FIX D — Doc oficial BingX GET /openApi/swap/v3/quote/klines:
    Cada vela es un ARRAY de 11 elementos indexados por posición:
      c[0] = open time (ms)
      c[1] = open price
      c[2] = high price
      c[3] = low price
      c[4] = close price
      c[5] = volume (base asset)
      c[6] = close time (ms)
      c[7] = quote asset volume
      c[8] = number of trades
      c[9] = taker buy base asset volume
      c[10]= taker buy quote asset volume

    Antes se accedía a c["open"], c["high"]... como si fuera un dict.
    Ese error hacía que todas las velas tuvieran campos con valor 0 (KeyError silenciado),
    rompiendo silenciosamente signals, risk, trailing y break-even.
    """
    symbol   = symbol or config.SYMBOL
    interval = interval or config.TIMEFRAME
    data = _get("/openApi/swap/v3/quote/klines", {
        "symbol":   symbol,
        "interval": interval,
        "limit":    limit,
    })
    candles = []
    for c in (data.get("data") or []):
        # c es una lista: [open_time, open, high, low, close, volume, close_time, ...]
        try:
            candles.append({
                "ts":     int(c[0]),
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": float(c[5]),
                "closed": True,
            })
        except (IndexError, TypeError, ValueError) as exc:
            log.warning("get_ohlcv: vela malformada ignorada: %s — %s", c, exc)
    return candles


# ── Info de contrato (step size / min qty) ──────────────────────────────────────────────

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
    """Verifica que el valor nocional sea ≥ min_usdt (BingX exige mínimo ~5 USDT)."""
    return (qty * price) >= min_usdt


# ── Posiciones ──────────────────────────────────────────────────────────────────────────────────────────

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
        side = _POSITION_SIDE_MAP[raw_side]
    elif position_amt > 0:
        side = "long"
    elif position_amt < 0:
        side = "short"
    else:
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


# ── Apalancamiento ──────────────────────────────────────────────────────────────────────────────────────────

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


# ── Abrir orden ──────────────────────────────────────────────────────────────────────────────────────────────

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
    """Coloca (o reemplaza) la stop-loss order. Usada también por el trailing.

    FIX A — doc oficial BingX: closePosition y quantity son mutuamente excluyentes.
    "closePosition=true: all position squaring after triggering — cannot be used
    with quantity." (error 101400 si se envían juntos).
    Se usa closePosition=true sin quantity para cerrar la posición completa.
    """
    sl_side  = "SELL" if side == "long" else "BUY"
    pos_side = "LONG" if side == "long" else "SHORT"
    _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          sl_side,
        "positionSide":  pos_side,
        "type":          "STOP_MARKET",
        "stopPrice":     stop_price,
        "closePosition": "true",
    })
    log.info("SL colocado en %.6f (%s %s)", stop_price, side.upper(), symbol)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    """Coloca la take-profit order.

    FIX A — doc oficial BingX: closePosition y quantity son mutuamente excluyentes.
    "closePosition=true: all position squaring after triggering — cannot be used
    with quantity." (error 101400 si se envían juntos).
    Se usa closePosition=true sin quantity para cerrar la posición completa.
    """
    sl_side  = "SELL" if side == "long" else "BUY"
    pos_side = "LONG" if side == "long" else "SHORT"
    _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          sl_side,
        "positionSide":  pos_side,
        "type":          "TAKE_PROFIT_MARKET",
        "stopPrice":     tp_price,
        "closePosition": "true",
    })
    log.info("TP colocado en %.6f (%s %s)", tp_price, side.upper(), symbol)


# ── Cerrar posición ────────────────────────────────────────────────────────────────────────────────────────────

def close_position(side: str = None, qty: float = None, symbol: str = None) -> dict:
    """Cierra la posición abierta del símbolo con una orden MARKET.

    Acepta side/qty opcionales para compatibilidad con el rollback en main.py,
    pero usa closePosition=true para garantizar el cierre completo independientemente
    de la qty exacta (evita errores por qty desactualizada).
    """
    symbol = symbol or config.SYMBOL
    if side is None:
        pos = get_position(symbol)
        if pos is None:
            log.info("close_position: no hay posición abierta en %s", symbol)
            return {}
        side = pos["side"]

    bx_side  = "SELL" if side == "long" else "BUY"
    pos_side = "LONG" if side == "long" else "SHORT"
    resp = _post("/openApi/swap/v2/trade/order", {
        "symbol":        symbol,
        "side":          bx_side,
        "positionSide":  pos_side,
        "type":          "MARKET",
        "closePosition": "true",
    })
    log.info("Posición cerrada: %s %s", side.upper(), symbol)
    return resp


# ── Cancelar órdenes abiertas ────────────────────────────────────────────────────────────────────────────────

def cancel_all_orders(symbol: str = None) -> None:
    """FIX: BingX exige DELETE (no POST) para cancelar todas las órdenes abiertas.
    Con POST la API devuelve 405 silencioso, dejando SL/TP duplicados en el exchange.
    """
    symbol = symbol or config.SYMBOL
    _delete("/openApi/swap/v2/trade/allOpenOrders", {"symbol": symbol})
    log.info("Órdenes canceladas para %s", symbol)


# ── Historial de órdenes cerradas ────────────────────────────────────────────────────────────────────────────

def get_closed_orders(symbol: str = None, limit: int = 20) -> list[dict]:
    """Devuelve órdenes ejecutadas/cerradas del símbolo para obtener el precio real
    de salida en _get_real_exit_price().

    FIX B — parseo corregido según doc oficial BingX:
      GET /openApi/swap/v2/trade/allOrders → data es directamente un array de
      órdenes. No existe ningún campo .orders ni .list en la respuesta de este
      endpoint. El bloque isinstance(raw, dict) anterior era incorrecto y se ha
      eliminado.

    FIX C — startTime/endTime obligatorios según doc oficial:
      Sin rango temporal BingX puede devolver error 109400. Se incluyen
      startTime=ahora-7d y endTime=ahora en cada llamada. El rango máximo
      permitido es 7 días; nunca se supera.

    BUG-13 FIX — eliminado PARTIALLY_FILLED del set terminal.
      PARTIALLY_FILLED aparecía antes que FILLED en el historial. Si
      _get_real_exit_price() lo encontraba primero, calculaba el PnL con
      precio parcial incorrecto. Solo FILLED es ejecución completa válida
      para calcular precio de salida real.

    limit es obligatorio (doc oficial); valor por defecto elevado a 20 para
    mayor cobertura en _get_real_exit_price() y _get_position_open_ts().
    """
    symbol = symbol or config.SYMBOL
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - 7 * 24 * 60 * 60 * 1000  # 7 días atrás
    try:
        data = _get("/openApi/swap/v2/trade/allOrders", {
            "symbol":    symbol,
            "limit":     limit,
            "startTime": start_ms,
            "endTime":   now_ms,
        })
    except Exception as exc:
        log.debug("[%s] get_closed_orders falló: %s", symbol, exc)
        return []

    # data es directamente un array — doc oficial BingX allOrders v2
    raw = data.get("data") or []

    # BUG-13 FIX: PARTIALLY_FILLED eliminado — solo FILLED es ejecución completa válida.
    # PARTIALLY_CANCELED mantenido porque indica cierre parcial + cancelación del resto.
    terminal = {"FILLED", "CANCELED", "PARTIALLY_CANCELED"}
    return [o for o in raw if str(o.get("status", "")).upper() in terminal]
