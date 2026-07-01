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

v2 — Rate limiting (fix 429):
  _hl_call() envuelve TODAS las llamadas HTTP a Hyperliquid con:
    • Reintentos exponenciales: 1s, 2s, 4s (3 intentos)
    • Jitter ±20% para evitar sincronización de reintentos entre pares
    • En 429 específicamente: espera extra de 5s antes de reintentar
    • Máximo 3 reintentos — tras el 3º lanza la excepción original

v3 — Fix Invalid leverage value:
  set_leverage() hace bisect automático cuando HL rechaza el leverage:
  prueba 5x → 3x → 2x → 1x hasta encontrar el máximo aceptado.
  Pares con max_leverage < 10x (LIT, VVV, MORPHO, DYDX, JTO, XLM, ZRO,
  PYTH, WIF, TAO, GRASS, AERO, FET, HBAR, VIRTUAL, EIGEN, OP, PENDLE,
  INJ, XMR, SEI, TIA, LDO) ya no generan WARNING en cada reinicio.
"""
import logging
import os
import random
import time
from typing import Optional

import config

log = logging.getLogger("exchange")

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

_info     = Info(_HL_URL, skip_ws=True)
_exchange = Exchange(_account, _HL_URL, account_address=_WALLET_ADDRESS)

log.info("Hyperliquid inicializado | wallet=%s | mainnet=%s", _WALLET_ADDRESS, _MAINNET)


# ── Rate limiting: exponential backoff con jitter ─────────────────────────────

_RL_MAX_RETRIES   = 3
_RL_BASE_DELAY    = 1.0
_RL_JITTER        = 0.2
_RL_429_EXTRA     = 5.0


def _is_429(exc: Exception) -> bool:
    msg = str(exc)
    if "429" in msg:
        return True
    code = getattr(exc, "status_code", None) or getattr(exc, "response", None)
    if code == 429:
        return True
    if isinstance(exc, (tuple, list)) and len(exc) > 0 and exc[0] == 429:
        return True
    return False


def _hl_call(fn, *args, context: str = "", **kwargs):
    """Llama fn(*args, **kwargs) con reintentos exponenciales ante 429."""
    last_exc = None
    for attempt in range(1, _RL_MAX_RETRIES + 2):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt > _RL_MAX_RETRIES:
                break

            is_429 = _is_429(exc)
            base_wait = _RL_BASE_DELAY * (2 ** (attempt - 1))
            jitter    = base_wait * _RL_JITTER * (random.random() * 2 - 1)
            wait      = base_wait + jitter + (_RL_429_EXTRA if is_429 else 0)

            log.warning(
                "%s: error en intento %d/%d%s — reintentando en %.1fs | %s",
                context or fn.__name__, attempt, _RL_MAX_RETRIES,
                " [429 rate limit]" if is_429 else "",
                wait, exc,
            )
            time.sleep(wait)

    raise last_exc


# ── Utilidades de símbolo ────────────────────────────────────────────────────

def _hl_symbol(symbol: str) -> str:
    return symbol.split("-")[0]


# ── sz_decimals ───────────────────────────────────────────────────────────────

def _sz_decimals(symbol: str) -> int:
    coin  = _hl_symbol(symbol)
    asset = _info.coin_to_asset.get(coin)
    if asset is not None:
        return _info.asset_to_sz_decimals.get(asset, 3)
    return 3


def floor_qty(qty: float, symbol: str) -> float:
    dec    = _sz_decimals(symbol)
    factor = 10 ** dec
    return int(qty * factor) / factor


def min_notional_ok(qty: float, price: float, min_usdt: float = 10.0) -> bool:
    return (qty * price) >= min_usdt


# ── Precio límite para órdenes de mercado ─────────────────────────────────────
_MARKET_SLIPPAGE = 0.005

def _market_price(coin: str, is_buy: bool) -> float:
    mids = _hl_call(_info.all_mids, context=f"_market_price({coin})")
    mid  = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    return mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)


# ── Balance ───────────────────────────────────────────────────────────────────

def get_balance() -> float:
    try:
        state = _hl_call(_info.user_state, _WALLET_ADDRESS, context="get_balance")
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as exc:
        log.warning("get_balance falló: %s", exc)
        return 0.0


# ── Precio ────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        mids = _hl_call(_info.all_mids, context=f"get_price({coin})")
        if coin in mids:
            return float(mids[coin])
        book = _hl_call(_info.l2_snapshot, coin, context=f"get_price_l2({coin})")
        bid  = float(book["levels"][0][0]["px"])
        ask  = float(book["levels"][1][0]["px"])
        return (bid + ask) / 2
    except Exception as exc:
        log.warning("get_price(%s) falló: %s", coin, exc)
        return 0.0


# ── OHLCV ─────────────────────────────────────────────────────────────────────

_TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    interval = interval or config.TIMEFRAME
    tf_secs  = _TF_SECONDS.get(interval, 900)
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - tf_secs * limit * 1000

    try:
        raw = _hl_call(
            _info.candles_snapshot, coin, interval, start_ms, end_ms,
            context=f"get_ohlcv({coin},{interval})",
        )
    except Exception as exc:
        log.warning("get_ohlcv(%s %s) falló: %s", coin, interval, exc)
        return []

    candles = []
    for c in raw:
        open_time = int(c["t"])
        vol       = float(c["v"])
        close     = float(c["c"])
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
    szi = float(pos.get("szi", 0))
    if szi == 0:
        return None
    return {
        "side":  "long" if szi > 0 else "short",
        "entry": float(pos.get("entryPx") or 0),
        "size":  abs(szi),
        "sl":    None,
        "tp":    None,
    }


def get_all_positions() -> dict[str, dict]:
    state     = _hl_call(_info.user_state, _WALLET_ADDRESS, context="get_all_positions")
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

# Cache: coin → leverage efectivo confirmado por HL
_leverage_cache: dict[str, int] = {}

# Candidatos para bisect cuando HL rechaza el leverage solicitado
_LEVERAGE_FALLBACKS = [5, 3, 2, 1]


def set_leverage(symbol: str = None, leverage: int = None) -> None:
    """Setea leverage en isolated. Si HL rechaza el valor, hace bisect automático.

    v3 fix: pares con max_leverage < 10x (LIT, VVV, MORPHO, DYDX, JTO, XLM,
    ZRO, PYTH, WIF, TAO, GRASS, AERO, FET, HBAR, VIRTUAL, EIGEN, OP, PENDLE,
    INJ, XMR, SEI, TIA, LDO...) ya no generan WARNING repetido — el primer
    arranque descubre su max y lo cachea para siempre.
    """
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    leverage = min(int(leverage or config.LEVERAGE), MAX_LEVERAGE)

    # Si ya tenemos el leverage efectivo cacheado, usarlo directamente
    if coin in _leverage_cache:
        cached = _leverage_cache[coin]
        if cached == leverage:
            return  # ya está seteado
        # Si el cached es menor que el pedido, usar el cached (sabemos que el
        # pedido fue rechazado antes)
        leverage = min(leverage, cached)

    candidates = [leverage] + [f for f in _LEVERAGE_FALLBACKS if f < leverage]

    for lev in candidates:
        try:
            resp = _hl_call(
                _exchange.update_leverage, lev, coin, False,
                context=f"set_leverage({coin},{lev}x)",
            )
            if resp.get("status") == "ok":
                if lev < leverage:
                    log.info(
                        "set_leverage(%s): max permitido es %dx (pedido %dx) — usando %dx",
                        coin, lev, leverage, lev,
                    )
                else:
                    log.info("Leverage seteado a %dx en %s (isolated)", lev, coin)
                _leverage_cache[coin] = lev
                return
            # HL devolvió 'err' (ej: 'Invalid leverage value') — probar siguiente
            log.debug(
                "set_leverage(%s) rechazado a %dx: %s — probando menor",
                coin, lev, resp.get("response", ""),
            )
        except Exception as exc:
            log.warning("set_leverage(%s @%dx) falló: %s", coin, lev, exc)
            # No hacer break — intentar con leverage menor

    log.error("set_leverage(%s): no se pudo setear ningún leverage válido", coin)


# ── Abrir orden ───────────────────────────────────────────────────────────────

def _check_order_response(resp: dict, context: str) -> None:
    status = resp.get("status")
    if status != "ok":
        raise RuntimeError(f"{context}: status={status!r} — {resp}")

    statuses = (
        ((resp.get("response") or {})
         .get("data") or {})
        .get("statuses") or []
    )
    if not statuses:
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

    qty   = floor_qty(qty, sym_bot)
    price = get_price(sym_bot)
    if qty <= 0 or not min_notional_ok(qty, price):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot}. "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    set_leverage(sym_bot, config.LEVERAGE)

    limit_px = _market_price(coin, is_buy)
    resp = _hl_call(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"open_order {side.upper()} {coin} qty={qty}",
    )
    _check_order_response(resp, f"open_order {side.upper()} {coin} qty={qty}")
    log.info("Orden abierta: %s %s qty=%.4f @ ~%.4f", side.upper(), coin, qty, limit_px)

    place_stop_order(sym_bot, side, qty, sl)
    place_tp_order(sym_bot, side, qty, tp)
    return resp


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> None:
    coin   = _hl_symbol(symbol)
    is_buy = side == "short"
    try:
        resp = _hl_call(
            _exchange.order,
            coin, is_buy, qty, stop_price,
            {"trigger": {"triggerPx": stop_price, "isMarket": True, "tpsl": "sl"}},
            context=f"place_stop_order({coin},{stop_price})",
            reduce_only=True,
        )
        if resp.get("status") == "ok":
            log.info("SL colocado en %.6f (%s %s)", stop_price, side.upper(), coin)
        else:
            log.warning("place_stop_order(%s): %s", coin, resp)
    except Exception as exc:
        log.warning("place_stop_order(%s) falló: %s", coin, exc)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    coin   = _hl_symbol(symbol)
    is_buy = side == "short"
    try:
        resp = _hl_call(
            _exchange.order,
            coin, is_buy, qty, tp_price,
            {"trigger": {"triggerPx": tp_price, "isMarket": True, "tpsl": "tp"}},
            context=f"place_tp_order({coin},{tp_price})",
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
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    is_buy   = side == "short"
    limit_px = _market_price(coin, is_buy)
    resp = _hl_call(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"close_position({coin})",
        reduce_only=True,
    )
    log.info("Posición cerrada: %s %s", side.upper(), coin)
    return resp


# ── Cancelar órdenes abiertas ─────────────────────────────────────────────────

def cancel_all_orders(symbol: str = None) -> None:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        orders = _hl_call(_info.open_orders, _WALLET_ADDRESS, context=f"open_orders({coin})")
        oids   = [o["oid"] for o in orders if o["coin"] == coin]
        if oids:
            _hl_call(_exchange.cancel, coin, oids, context=f"cancel_all_orders({coin})")
            log.info("Órdenes canceladas para %s (%d)", coin, len(oids))
        else:
            log.debug("cancel_all_orders(%s): no había órdenes abiertas", coin)
    except Exception as exc:
        log.warning("cancel_all_orders(%s) falló: %s", coin, exc)


# ── Historial de fills ────────────────────────────────────────────────────────

def _normalize_fill(f: dict) -> dict:
    fill_dir = f.get("dir", "")
    if "Long" in fill_dir:
        normalized_side = "SELL"
    else:
        normalized_side = "BUY"

    closed_pnl = float(f.get("closedPnl") or 0)
    order_type = "TAKE_PROFIT_MARKET" if closed_pnl > 0 else "STOP_MARKET"

    px_str = str(f.get("px", 0))
    return {
        "side":       normalized_side,
        "type":       order_type,
        "order_type": order_type,
        "px":         px_str,
        "avgPrice":   px_str,
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
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        now_ms    = int(time.time() * 1000)
        start_ms  = now_ms - 7 * 24 * 60 * 60 * 1000
        raw_fills = _hl_call(
            _info.user_fills_by_time, _WALLET_ADDRESS, start_ms, now_ms,
            context=f"get_fills({coin})",
        )
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
    return get_fills(symbol=symbol, limit=limit, only_close=True)
