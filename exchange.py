"""exchange.py — Cliente Hyperliquid Perpetual Futures.

Migrado desde BingX. Hyperliquid es un DEX L1 sin KYC/restricciones MiCA.

Dependencias:
    pip install hyperliquid-python-sdk eth-account httpx

Variables de entorno:
    HYPERLIQUID_PRIVATE_KEY  : clave privada EVM en hex (con o sin 0x)
    HYPERLIQUID_WALLET_ADDRESS: opcional, se deriva de la pk si no se pone
    HL_MAINNET               : "true" prod, "false" testnet (default: true)

Nombres de símbolos:
    Hyperliquid usa el token base sin quote: "BTC", "ETH", "SOL".
    Todas las funciones públicas aceptan tanto "BTC-USDT" como "BTC".
    _hl_symbol() normaliza internamente.

BUGS corregidos respecto a la versión BingX original:
  1. Exchange.__init__ requiere account, base_url y wallet_address en ese orden.
     La signatura correcta es Exchange(account, base_url, account_address=...).
  2. market_open/market_close NO existen en el SDK oficial. La API correcta es
     _exchange.order() con orderType={"market": {}} o {"trigger": {...}}.
     Se usa slippage=0.005 (0.5%) como precio límite para market IOC orders.
  3. update_leverage() en el SDK se llama con (leverage, coin, is_cross).
     is_cross=False = isolated (correcto para este bot).
  4. cancel() del SDK requiere (coin, oids: list[int]), no lista de dicts.
  5. candles_snapshot requiere name_to_coin lookup interno — el SDK lo hace
     en info.candles_snapshot(name, ...) usando self.name_to_coin.
     Con skip_ws=True el lookup sí funciona (se carga en __init__).
  6. user_state() devuelve posición con campo 'szi' (str, no float).
     Los campos stopLossPx / takeProfitPx NO existen en user_state.
     Hyperliquid no reporta SL/TP en la posición; se rastrean en bot_state.py.
  7. get_price() usa all_mids() que devuelve {coin: str_price}.
     Fallback a l2_snapshot que usa name_to_coin internamente.
  8. floor_qty ahora recibe symbol y calcula sz_decimals desde el SDK.
  9. Info(skip_ws=True) es correcto para no abrir WS desde el hilo principal
     (ws_feed.py gestiona su propio WS de forma independiente).
 10. get_closed_orders: usa fill['dir'] ('Close Long'/'Close Short') para
     clasificar SL vs TP en lugar de la heurística 'crossed' (incorrecta).
     'crossed' indica si la orden cruzó el book, no si es stop o tp trigger.
 11. set_leverage: leverage se capa a MAX_LEVERAGE (10x) como máximo para
     proteger contra configuraciones de entorno incorrectas o llamadas externas.
 12. open_order: se valida statuses[0] de la respuesta HL para detectar rechazos
     internos (IocCancel, MinTradeNtl, BadTriggerPx, PerpMargin...) que llegan
     con status='ok' en el nivel superior pero con error en statuses.
 13. get_fills(): nueva función que expone los fills con only_close=False para
     recuperar aperturas (dir='Open Long/Short'), usada en _get_position_open_ts.
     El campo de precio se expone tanto como 'px' como 'avgPrice' para
     compatibilidad con _get_real_exit_price en main.py.
 14. closedPnl > 0 → TP (antes >= 0 clasificaba trades flat como TP).
 15. _MARKET_SLIPPAGE: reducido de 5% a 0.5% (5% era excesivo para IOC).
"""
import logging
import os
import time
from typing import Optional

import config

log = logging.getLogger("exchange")

# Apalancamiento máximo permitido — nunca superar este valor aunque config lo indique
MAX_LEVERAGE = 10

# ── SDK imports ──────────────────────────────────────────────────────────────
try:
    import eth_account
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants as hl_constants
except ImportError as _e:
    raise ImportError(
        "SDK de Hyperliquid no instalado. Ejecuta: pip install hyperliquid-python-sdk eth-account"
    ) from _e

# ── Inicializar clientes ─────────────────────────────────────────────────────
_pk = os.environ["HYPERLIQUID_PRIVATE_KEY"]
if not _pk.startswith("0x"):
    _pk = "0x" + _pk

_account          = eth_account.Account.from_key(_pk)
_WALLET_ADDRESS   = (os.environ.get("HYPERLIQUID_WALLET_ADDRESS") or _account.address).lower()
_MAINNET          = os.environ.get("HL_MAINNET", "true").lower() == "true"
_HL_URL           = hl_constants.MAINNET_API_URL if _MAINNET else hl_constants.TESTNET_API_URL

# skip_ws=True: no abre WS desde aquí; ws_feed.py gestiona su propio WS
_info     = Info(_HL_URL, skip_ws=True)
# BUG 1 fix: signatura correcta Exchange(account, base_url, account_address=wallet)
_exchange = Exchange(_account, _HL_URL, account_address=_WALLET_ADDRESS)

log.info("Hyperliquid inicializado | wallet=%s | mainnet=%s", _WALLET_ADDRESS, _MAINNET)


# ── Utilidades de símbolo ────────────────────────────────────────────────────

def _hl_symbol(symbol: str) -> str:
    """Convierte 'BTC-USDT' o 'BTC' a 'BTC' (formato Hyperliquid)."""
    return symbol.split("-")[0]


# ── sz_decimals (precisión de cantidad) ────────────────────────────────────────

def _sz_decimals(symbol: str) -> int:
    # BUG 8 fix: usar asset_to_sz_decimals del SDK, que ya lo carga en __init__
    coin = _hl_symbol(symbol)
    asset = _info.coin_to_asset.get(coin)
    if asset is not None:
        return _info.asset_to_sz_decimals.get(asset, 3)
    return 3


def floor_qty(qty: float, symbol: str) -> float:
    """Redondea qty hacia abajo según szDecimals del contrato."""
    dec    = _sz_decimals(symbol)
    factor = 10 ** dec
    return int(qty * factor) / factor


def min_notional_ok(qty: float, price: float, min_usdt: float = 10.0) -> bool:
    return (qty * price) >= min_usdt


# ── Precio límite para órdenes de mercado (slippage permitido) ────────────────
# BUG 2+15 fix: market_open/close no existen — simular con limit IOC + 0.5% slippage
# 0.5% es suficiente para garantizar fill en líquidos sin desperdiciar precio.
_MARKET_SLIPPAGE = 0.005  # 0.5% de slippage máximo para market IOC

def _market_price(coin: str, is_buy: bool) -> float:
    """Devuelve precio límite para market IOC con slippage de 0.5%."""
    mids = _info.all_mids()
    mid  = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    return mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)


# ── Balance ───────────────────────────────────────────────────────────────────

def get_balance() -> float:
    """Devuelve el equity total en USDT (marginSummary.accountValue).

    user_state() devuelve marginSummary: {accountValue, totalMarginUsed, ...}
    accountValue = balance + unrealizedPnl (equivalente al 'equity' de BingX).
    """
    try:
        state = _info.user_state(_WALLET_ADDRESS)
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as exc:
        log.warning("get_balance falló: %s", exc)
        return 0.0


# ── Precio ────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    # BUG 7 fix: all_mids devuelve {coin: str}, no float
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        mids = _info.all_mids()
        if coin in mids:
            return float(mids[coin])
        # Fallback: L2 orderbook mid
        book = _info.l2_snapshot(coin)
        bid  = float(book["levels"][0][0]["px"])
        ask  = float(book["levels"][1][0]["px"])
        return (bid + ask) / 2
    except Exception as exc:
        log.warning("get_price(%s) falló: %s", coin, exc)
        return 0.0


# ── OHLCV ─────────────────────────────────────────────────────────────────────

_TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    """Devuelve velas OHLCV en el mismo formato que usaba la implementación BingX.

    candles_snapshot() campos según doc oficial:
      t: open time (ms), T: close time (ms)
      o, h, l, c: OHLC (strings)
      v: volumen base (string)
      n: número de trades
      s: symbol
      i: interval

    BUG 5 fix: candles_snapshot(name, ...) usa name_to_coin internamente —
    pasar el nombre limpio ('BTC') no 'BTC-USDT'.
    """
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    interval = interval or config.TIMEFRAME
    tf_secs  = _TF_SECONDS.get(interval, 900)
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - tf_secs * limit * 1000

    try:
        raw = _info.candles_snapshot(coin, interval, start_ms, end_ms)
    except Exception as exc:
        log.warning("get_ohlcv(%s %s) falló: %s", coin, interval, exc)
        return []

    candles = []
    for c in raw:
        open_time = int(c["t"])
        vol       = float(c["v"])
        close     = float(c["c"])
        # Hyperliquid no tiene quote_volume directo — aproximar como vol * close
        candles.append({
            "ts":           open_time,
            "open_time":    open_time,
            "open":         float(c["o"]),
            "high":         float(c["h"]),
            "low":          float(c["l"]),
            "close":        close,
            "volume":       vol,
            "quote_volume": vol * close,
            "closed":       True,
        })
    return candles[-limit:]


# ── Posiciones ────────────────────────────────────────────────────────────────

def _parse_hl_position(pos: dict) -> dict | None:
    """Convierte una posición HL al formato interno del bot.

    BUG 6 fix: 'szi' es string, no float. stopLossPx/takeProfitPx NO existen
    en user_state — Hyperliquid no los reporta en la posición. SL/TP se
    rastrean en bot_state.py a partir de los valores con que se abrieron.
    """
    szi = float(pos.get("szi", 0))
    if szi == 0:
        return None
    return {
        "side":  "long" if szi > 0 else "short",
        "entry": float(pos.get("entryPx") or 0),
        "size":  abs(szi),
        "sl":    None,   # no disponible en HL user_state — usar bot_state.py
        "tp":    None,
    }


def get_all_positions() -> dict[str, dict]:
    """Devuelve {symbol_bot: pos_dict} para todas las posiciones abiertas.

    user_state() estructura:
      assetPositions: [ {position: {coin, szi, entryPx, ...}, type: 'oneWay'} ]
    """
    state  = _info.user_state(_WALLET_ADDRESS)
    # Mapa inverso: coin_hl -> symbol_bot
    hl_to_bot = {_hl_symbol(s): s for s in config.SYMBOLS}
    result: dict[str, dict] = {}
    for entry in state.get("assetPositions", []):
        pos     = entry.get("position", {})
        coin    = pos.get("coin", "")
        sym_bot = hl_to_bot.get(coin)
        if sym_bot is None:
            continue
        parsed = _parse_hl_position(pos)
        if parsed:
            result[sym_bot] = parsed
    return result


def get_position(symbol: str = None) -> dict | None:
    symbol = symbol or config.SYMBOLS[0]
    return get_all_positions().get(symbol)


# ── Apalancamiento ────────────────────────────────────────────────────────────

def set_leverage(symbol: str = None, leverage: int = None) -> None:
    # BUG 3 fix: update_leverage(leverage, coin, is_cross) — is_cross=False = isolated
    # BUG 11 fix: cap a MAX_LEVERAGE (10x) — nunca enviar un valor superior al exchange
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    leverage = leverage or config.LEVERAGE
    leverage = min(int(leverage), MAX_LEVERAGE)
    try:
        resp = _exchange.update_leverage(leverage, coin, is_cross=False)
        if resp.get("status") == "ok":
            log.info("Leverage seteado a %dx en %s (isolated)", leverage, coin)
        else:
            log.warning("set_leverage(%s): respuesta inesperada: %s", coin, resp)
    except Exception as exc:
        log.warning("set_leverage(%s) falló: %s", coin, exc)


# ── Abrir orden ───────────────────────────────────────────────────────────────

def _check_order_response(resp: dict, context: str) -> None:
    """Valida la respuesta de _exchange.order() a nivel de statuses.

    BUG 12 fix: Hyperliquid devuelve status='ok' en el nivel superior incluso
    cuando la orden ha sido rechazada internamente. El error real aparece en:
      resp['response']['data']['statuses'][0] == {'error': 'IocCancel'} (u otro)

    Códigos de rechazo habituales según doc oficial:
      IocCancel      — IOC sin fill (liquidez insuficiente al precio límite)
      MinTradeNtl    — notional < mínimo ($10 en perpetuos)
      PerpMargin     — margen insuficiente
      BadTriggerPx   — precio de trigger inválido para SL/TP
      ReductionOnly  — reduce_only rechazado (no hay posición que reducir)
    """
    status = resp.get("status")
    if status != "ok":
        raise RuntimeError(f"{context}: status={status!r} — {resp}")

    statuses = (
        ((resp.get("response") or {})
         .get("data") or {})
        .get("statuses") or []
    )
    if not statuses:
        # Sin statuses no podemos validar más — asumir OK
        return

    first = statuses[0]
    if "error" in first:
        raise RuntimeError(f"{context} rechazada por HL: {first['error']} — {resp}")
    if not any(k in first for k in ("filled", "resting")):
        raise RuntimeError(f"{context}: respuesta sin filled/resting — {resp}")


def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    sym_bot = symbol or config.SYMBOLS[0]
    coin    = _hl_symbol(sym_bot)
    is_buy  = side == "long"

    qty = floor_qty(qty, sym_bot)
    price = get_price(sym_bot)
    if qty <= 0 or not min_notional_ok(qty, price):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot}. "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    set_leverage(sym_bot, config.LEVERAGE)

    # BUG 2 fix: usar order() con IOC (Immediate-Or-Cancel = market order)
    limit_px = _market_price(coin, is_buy)
    resp = _exchange.order(
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
    )
    # BUG 12 fix: validar statuses internos, no solo status=='ok'
    _check_order_response(resp, f"open_order {side.upper()} {coin} qty={qty}")
    log.info("Orden abierta: %s %s qty=%.4f @ ~%.4f", side.upper(), coin, qty, limit_px)

    place_stop_order(sym_bot, side, qty, sl)
    place_tp_order(sym_bot, side, qty, tp)
    return resp


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> None:
    """Coloca stop-loss como orden trigger reduce-only.

    BUG 2 fix: no existe market_open — usar order() con orderType trigger.
    Estructura correcta según SDK:
      orderType = {"trigger": {"triggerPx": float, "isMarket": True, "tpsl": "sl"}}
    """
    coin   = _hl_symbol(symbol)
    is_buy = side == "short"  # cerrar long = vender; cerrar short = comprar
    try:
        resp = _exchange.order(
            coin, is_buy, qty, stop_price,
            {"trigger": {"triggerPx": stop_price, "isMarket": True, "tpsl": "sl"}},
            reduce_only=True,
        )
        if resp.get("status") == "ok":
            log.info("SL colocado en %.6f (%s %s)", stop_price, side.upper(), coin)
        else:
            log.warning("place_stop_order(%s): %s", coin, resp)
    except Exception as exc:
        log.warning("place_stop_order(%s) falló: %s", coin, exc)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    """Coloca take-profit como orden trigger reduce-only."""
    coin   = _hl_symbol(symbol)
    is_buy = side == "short"
    try:
        resp = _exchange.order(
            coin, is_buy, qty, tp_price,
            {"trigger": {"triggerPx": tp_price, "isMarket": True, "tpsl": "tp"}},
            reduce_only=True,
        )
        if resp.get("status") == "ok":
            log.info("TP colocado en %.6f (%s %s)", tp_price, side.upper(), coin)
        else:
            log.warning("place_tp_order(%s): %s", coin, resp)
    except Exception as exc:
        log.warning("place_tp_order(%s) falló: %s", coin, exc)


# ── Cerrar posición ───────────────────────────────────────────────────────────

def close_position(side: str, qty: float, symbol: str = None) -> dict:
    coin    = _hl_symbol(symbol or config.SYMBOLS[0])
    is_buy  = side == "short"  # cerrar short = comprar
    limit_px = _market_price(coin, is_buy)
    resp = _exchange.order(
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        reduce_only=True,
    )
    log.info("Posición cerrada: %s %s", side.upper(), coin)
    return resp


# ── Cancelar órdenes abiertas ─────────────────────────────────────────────────

def cancel_all_orders(symbol: str = None) -> None:
    # BUG 4 fix: cancel(coin, oids: list[int]) — no lista de dicts
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        orders = _info.open_orders(_WALLET_ADDRESS)
        oids   = [o["oid"] for o in orders if o["coin"] == coin]
        if oids:
            resp = _exchange.cancel(coin, oids)
            log.info("Órdenes canceladas para %s (%d)", coin, len(oids))
        else:
            log.debug("cancel_all_orders(%s): no había órdenes abiertas", coin)
    except Exception as exc:
        log.warning("cancel_all_orders(%s) falló: %s", coin, exc)


# ── Historial de fills ────────────────────────────────────────────────────────

def _normalize_fill(f: dict) -> dict:
    """Normaliza un fill de Hyperliquid al formato interno del bot.

    Campos del fill según doc oficial:
      coin, closedPnl, crossed, dir, hash, oid, px, side, startPosition, sz, time

    side: 'B' (buy) | 'A' (ask/sell)
    dir:  'Open Long' | 'Close Long' | 'Open Short' | 'Close Short'

    BUG 13 fix: el precio se expone como 'px' Y 'avgPrice' para compatibilidad
    con _get_real_exit_price en main.py (que lee order['px']).

    BUG 14 fix: closedPnl > 0 → TP, <= 0 → SL
    (antes >= 0 clasificaba trades flat como TP, lo que es incorrecto).
    """
    fill_dir = f.get("dir", "")
    if "Long" in fill_dir:
        normalized_side = "SELL"   # cierre/apertura de long
    else:
        normalized_side = "BUY"    # cierre/apertura de short

    closed_pnl = float(f.get("closedPnl") or 0)
    # BUG 14: > 0 para TP, cualquier otro caso (incluido 0 flat) → SL
    order_type = "TAKE_PROFIT_MARKET" if closed_pnl > 0 else "STOP_MARKET"

    px_str = str(f.get("px", 0))
    return {
        "side":       normalized_side,
        "type":       order_type,
        "px":         px_str,       # BUG 13 fix: campo 'px' que lee main.py
        "avgPrice":   px_str,       # alias para compatibilidad
        "time":       int(f.get("time", 0)),
        "updateTime": int(f.get("time", 0)),
        "status":     "FILLED",
        "dir":        fill_dir,
        "closedPnl":  closed_pnl,
    }


def get_fills(
    symbol: str = None,
    limit: int = 20,
    only_close: bool = True,
) -> list[dict]:
    """Devuelve fills recientes del símbolo normalizados.

    BUG 13 fix: nueva función que acepta only_close=False para recuperar
    aperturas ('Open Long'/'Open Short'), necesario en _get_position_open_ts
    de main.py. get_closed_orders() delega en esta función con only_close=True.

    Filtra TODOS los fills del símbolo antes de aplicar el límite para evitar
    que fills de otros pares acaparen el slice y oculten los del par buscado.
    """
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        now_ms   = int(time.time() * 1000)
        start_ms = now_ms - 7 * 24 * 60 * 60 * 1000
        raw_fills = _info.user_fills_by_time(_WALLET_ADDRESS, start_ms, now_ms)
    except Exception as exc:
        log.debug("get_fills(%s) falló: %s", coin, exc)
        return []

    result = []
    for f in raw_fills:
        if f.get("coin") != coin:
            continue
        fill_dir = f.get("dir", "")
        if only_close and "Close" not in fill_dir:
            continue
        result.append(_normalize_fill(f))
        if len(result) >= limit:
            break

    return result


def get_closed_orders(symbol: str = None, limit: int = 20) -> list[dict]:
    """Devuelve fills de cierre recientes del símbolo normalizados para main.py.

    Delega en get_fills(only_close=True). Mantenido por compatibilidad con
    las llamadas existentes en main.py y tg_commands.py.
    """
    return get_fills(symbol=symbol, limit=limit, only_close=True)
