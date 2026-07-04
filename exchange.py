"""exchange.py — Cliente Hyperliquid Perpetual Futures.

v2  — Rate limiting (fix 429)
v3  — Fix Invalid leverage value (bisect automático)
v4  — Fix cancel_all_orders (cancel uno por uno)
v5  — Fix reduce_only no llegaba al SDK (arg posicional)
v6  — Fix _check_order_response demasiado estricta para SL/TP
v7  — Fix "Order has invalid price" en SL/TP (tickSize rounding)
v8  — Fix "Order has invalid price" en open_order (entrada IOC)
v9  — Fix "floattowire causes rounding" (Decimal quantize)
v10 — Fix race condition en open_order (único _market_price)
v11 — Revert accidental (restaurado floor_qty + set_leverage)
v12 — Limpieza de logs redundantes
v13 — Segunda unificación _market_price (fix definitivo race)
v14 — Fix 4 bugs doc oficial HL:
       limit_px extremo en trigger market, frontend_open_orders,
       bulk normalTpsl, get/cancel_trigger_orders
v15 — Logs detallados en _place_sl_tp_bulk
v16 — Fix string indices + _check_bulk_response + fallback precios redondeados
v17 — Fix raíz de "string indices must be integers":
       El SDK _exchange.bulk_orders() procesa internamente la respuesta
       de HL iterando statuses con acceso por clave string, lo que rompe
       cuando HL devuelve el nuevo formato de respuesta.
       Fix: _place_sl_tp_bulk ahora firma y envía el POST directamente
       vía requests, sin pasar por la capa de procesamiento del SDK.
       Fix fallback individual: limit_px = triggerPx (HL rechaza 0/INT_MAX
       en llamadas individuales fuera de bulk).
"""
import json
import logging
import math
import os
import random
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import config

log = logging.getLogger("exchange")

MAX_LEVERAGE = 10

# ── SDK imports ─────────────────────────────────────────────────────────
try:
    import eth_account
    import requests
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants as hl_constants
    from hyperliquid.utils.signing import sign_l1_action, get_timestamp_ms, float_to_wire, order_type_to_wire
except ImportError as _e:
    raise ImportError(
        "SDK de Hyperliquid no instalado. Ejecuta: pip install hyperliquid-python-sdk eth-account requests"
    ) from _e

# ── Inicializar clientes ─────────────────────────────────────────────────
_pk = os.environ["HYPERLIQUID_PRIVATE_KEY"]
if not _pk.startswith("0x"):
    _pk = "0x" + _pk

_account        = eth_account.Account.from_key(_pk)
_WALLET_ADDRESS = (os.environ.get("HYPERLIQUID_WALLET_ADDRESS") or _account.address).lower()
_MAINNET        = os.environ.get("HL_MAINNET", "true").lower() == "true"
_HL_URL         = hl_constants.MAINNET_API_URL if _MAINNET else hl_constants.TESTNET_API_URL

_info     = Info(_HL_URL, skip_ws=True)
_exchange = Exchange(_account, _HL_URL, account_address=_WALLET_ADDRESS)

log.info("Hyperliquid inicializado | wallet=%s | mainnet=%s", _WALLET_ADDRESS, _MAINNET)


# ── Rate limiting ────────────────────────────────────────────────────────
_RL_MAX_RETRIES = 3
_RL_BASE_DELAY  = 1.0
_RL_JITTER      = 0.2
_RL_429_EXTRA   = 5.0


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
    last_exc = None
    for attempt in range(1, _RL_MAX_RETRIES + 2):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt > _RL_MAX_RETRIES:
                break
            is_429    = _is_429(exc)
            base_wait = _RL_BASE_DELAY * (2 ** (attempt - 1))
            jitter    = base_wait * _RL_JITTER * (random.random() * 2 - 1)
            wait      = base_wait + jitter + (_RL_429_EXTRA if is_429 else 0)
            log.warning(
                "%s: error en intento %d/%d%s — reintentando en %.1fs | %s",
                context or fn.__name__, attempt, _RL_MAX_RETRIES,
                " [429 rate limit]" if is_429 else "", wait, exc,
            )
            time.sleep(wait)
    raise last_exc


# ── Utilidades de símbolo ────────────────────────────────────────────────
def _hl_symbol(symbol: str) -> str:
    return symbol.split("-")[0]


# ── sz_decimals ──────────────────────────────────────────────────────────
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


# ── tickSize / price rounding ────────────────────────────────────────────
_tick_decimals: dict[str, int] = {}


def _get_tick_decimals(coin: str) -> int:
    if coin in _tick_decimals:
        return _tick_decimals[coin]
    try:
        meta = _info.meta()
        for asset_info in meta.get("universe", []):
            if asset_info.get("name") == coin:
                tick_sz = float(asset_info.get("tickSz", 0.0001))
                dec = max(0, round(-math.log10(tick_sz)))
                _tick_decimals[coin] = dec
                log.debug("tick_decimals(%s): tickSz=%s → %d decimales", coin, tick_sz, dec)
                return dec
    except Exception as exc:
        log.debug("_get_tick_decimals(%s) falló: %s — usando 6 dec", coin, exc)
    _tick_decimals[coin] = 6
    return 6


def _round_price(coin: str, price: float) -> float:
    dec       = _get_tick_decimals(coin)
    quantizer = Decimal(10) ** -dec
    rounded   = Decimal(str(price)).quantize(quantizer, rounding=ROUND_HALF_UP)
    return float(rounded)


# ── Precio de mercado ────────────────────────────────────────────────────
_MARKET_SLIPPAGE = 0.005


def _market_price(coin: str, is_buy: bool) -> float:
    mids = _hl_call(_info.all_mids, context=f"_market_price({coin})")
    mid  = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    raw = mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)
    return _round_price(coin, raw)


# ── Precio extremo para trigger market (bulk solamente) ──────────────────
# Solo se usa cuando se envía vía POST directo (bulk).  En llamadas
# individuales SDK, limit_px debe ser == triggerPx.
_TRIGGER_MARKET_SELL_PX = 0
_TRIGGER_MARKET_BUY_PX  = 2_147_483_647


# ── Balance ──────────────────────────────────────────────────────────────
def get_balance() -> float:
    try:
        state = _hl_call(_info.user_state, _WALLET_ADDRESS, context="get_balance")
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as exc:
        log.warning("get_balance falló: %s", exc)
        return 0.0


# ── Precio puntual ───────────────────────────────────────────────────────
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


# ── OHLCV ────────────────────────────────────────────────────────────────
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
            "ts": open_time, "open_time": open_time,
            "open": float(c["o"]), "high": float(c["h"]),
            "low":  float(c["l"]), "close": close,
            "volume": vol, "quote_volume": vol * close, "closed": True,
        })
    return candles[-limit:]


# ── Posiciones ───────────────────────────────────────────────────────────
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


# ── Apalancamiento ───────────────────────────────────────────────────────
_leverage_cache: dict[str, int] = {}
_LEVERAGE_FALLBACKS = [5, 3, 2, 1]


def set_leverage(symbol: str = None, leverage: int = None) -> None:
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    leverage = min(int(leverage or config.LEVERAGE), MAX_LEVERAGE)
    if coin in _leverage_cache:
        cached = _leverage_cache[coin]
        if cached == leverage:
            return
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
                    log.info("set_leverage(%s): max permitido es %dx — usando %dx", coin, lev, lev)
                else:
                    log.info("Leverage seteado a %dx en %s (isolated)", lev, coin)
                _leverage_cache[coin] = lev
                return
            log.debug("set_leverage(%s) rechazado a %dx: %s", coin, lev, resp.get("response", ""))
        except Exception as exc:
            log.warning("set_leverage(%s @%dx) falló: %s", coin, lev, exc)
    log.error("set_leverage(%s): no se pudo setear ningún leverage válido", coin)


# ── Validación respuesta individual ──────────────────────────────────────
def _check_order_response(resp: dict, context: str) -> None:
    status = resp.get("status")
    if status != "ok":
        raise RuntimeError(f"{context}: status={status!r} — {resp}")
    statuses = (
        ((resp.get("response") or {})
         .get("data") or {})
        .get("statuses") or []
    )
    if statuses:
        first = statuses[0]
        if isinstance(first, dict) and "error" in first:
            raise RuntimeError(f"{context} rechazada por HL: {first['error']} — {resp}")


# ── POST directo a /exchange (bypass SDK bulk_orders) ────────────────────
# El SDK _exchange.bulk_orders() hace procesamiento interno de la respuesta
# que explota con 'string indices must be integers' en versiones recientes
# de la API de HL.  Enviamos el mismo payload firmado pero procesamos
# nosotros el JSON crudo.

def _get_asset_id(coin: str) -> int:
    """Devuelve el asset index numérico de HL para `coin`."""
    asset = _info.coin_to_asset.get(coin)
    if asset is None:
        raise ValueError(f"coin desconocido: {coin}")
    return asset


def _order_wire(coin: str, is_buy: bool, sz: float, limit_px: float,
                order_type: dict, reduce_only: bool) -> dict:
    """Serializa una orden al formato 'wire' que espera la API de HL."""
    asset = _get_asset_id(coin)
    # order_type_to_wire convierte {"trigger": {...}} → formato interno
    ot_wire = order_type_to_wire(order_type)
    return {
        "a": asset,
        "b": is_buy,
        "p": float_to_wire(limit_px),
        "s": float_to_wire(sz),
        "r": reduce_only,
        "t": ot_wire,
    }


def _post_bulk_orders_raw(orders_wire: list[dict], grouping: str) -> dict:
    """Firma y envía bulk orders directamente via requests, sin pasar
    por _exchange.bulk_orders() del SDK.  Devuelve el JSON de respuesta.

    v17: esto evita el 'string indices must be integers' que ocurre dentro
    del SDK cuando procesa la respuesta bulk de HL.
    """
    nonce  = get_timestamp_ms()
    action = {"type": "order", "orders": orders_wire, "grouping": grouping}
    signature = sign_l1_action(
        _account,
        action,
        None,   # vault_address
        nonce,
        _MAINNET,
    )
    payload = {
        "action":       action,
        "nonce":        nonce,
        "signature":    signature,
        "vaultAddress": None,
    }
    url  = f"{_HL_URL}/exchange"
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _check_bulk_raw_response(resp: dict, context: str) -> None:
    """Valida el JSON crudo devuelto por _post_bulk_orders_raw."""
    if not isinstance(resp, dict):
        log.warning("%s: respuesta inesperada (no dict): %s", context, resp)
        return
    status = resp.get("status")
    if status != "ok":
        raise RuntimeError(f"{context}: status={status!r} — {resp}")
    statuses = (
        ((resp.get("response") or {})
         .get("data") or {})
        .get("statuses") or []
    )
    for i, s in enumerate(statuses):
        if isinstance(s, dict) and "error" in s:
            raise RuntimeError(f"{context} orden[{i}] rechazada: {s['error']} — {resp}")
    log.debug("%s: bulk OK (%d statuses)", context, len(statuses))


def _order_reduce_only(coin, is_buy, qty, price, order_type):
    """Wrapper que pasa reduce_only=True como 6º arg posicional al SDK."""
    return _exchange.order(coin, is_buy, qty, price, order_type, True)


# ── Abrir orden con SL+TP ────────────────────────────────────────────────
def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    sym_bot  = symbol or config.SYMBOLS[0]
    coin     = _hl_symbol(sym_bot)
    is_buy   = side == "long"
    qty      = floor_qty(qty, sym_bot)
    limit_px = _market_price(coin, is_buy)

    if qty <= 0 or not min_notional_ok(qty, limit_px):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot}. "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    set_leverage(sym_bot, config.LEVERAGE)

    resp = _hl_call(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"open_order {side.upper()} {coin} qty={qty}",
    )
    _check_order_response(resp, f"open_order {side.upper()} {coin} qty={qty}")
    log.info("Orden abierta: %s %s qty=%.4f @ ~%.4f", side.upper(), coin, qty, limit_px)

    _place_sl_tp_bulk(sym_bot, side, qty, sl, tp)
    return resp


def _place_sl_tp_bulk(symbol: str, side: str, qty: float, sl: float, tp: float) -> None:
    """Coloca SL y TP via POST directo (bypass SDK bulk_orders).

    v17 fix raíz:
    - _exchange.bulk_orders() procesa internamente la respuesta de HL
      con statuses[i]["resting"] etc., lo que explota cuando el formato
      de respuesta cambia → 'string indices must be integers'.
    - Solución: serializar las órdenes con _order_wire() y firmar+enviar
      vía _post_bulk_orders_raw(), procesando nosotros el JSON crudo.
    - Fallback individual: limit_px = triggerPx (no 0/INT_MAX, que HL
      rechaza en llamadas individuales fuera de bulk).
    """
    coin   = _hl_symbol(symbol)
    is_buy = side == "short"  # cerrar long → vender (is_buy=False); cerrar short → comprar
    sl_px  = _round_price(coin, sl)
    tp_px  = _round_price(coin, tp)
    limit_px_close = _TRIGGER_MARKET_BUY_PX if is_buy else _TRIGGER_MARKET_SELL_PX

    log.info(
        "_place_sl_tp_bulk(%s): side=%s is_buy=%s sl_px=%.6f tp_px=%.6f limit_px_close=%s qty=%.6f",
        coin, side, is_buy, sl_px, tp_px, limit_px_close, qty,
    )

    sl_ot = {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}}
    tp_ot = {"trigger": {"triggerPx": tp_px, "isMarket": True, "tpsl": "tp"}}

    try:
        sl_wire = _order_wire(coin, is_buy, qty, limit_px_close, sl_ot, True)
        tp_wire = _order_wire(coin, is_buy, qty, limit_px_close, tp_ot, True)
        log.debug("_place_sl_tp_bulk(%s): sl_wire=%s tp_wire=%s", coin, sl_wire, tp_wire)

        resp = _post_bulk_orders_raw([sl_wire, tp_wire], "normalTpsl")
        log.info("_place_sl_tp_bulk(%s): respuesta HL = %s", coin, resp)
        _check_bulk_raw_response(resp, f"_place_sl_tp_bulk({coin})")
        log.info("SL+TP colocados (bulk normalTpsl): %s SL=%.6f TP=%.6f", coin, sl_px, tp_px)
        return
    except Exception as exc:
        log.error(
            "_place_sl_tp_bulk(%s) FALLÓ bulk POST: %s — entrando en fallback individual",
            coin, exc,
        )

    # ── Fallback individual ──────────────────────────────────────────────
    # v17: limit_px = triggerPx (HL rechaza 0/INT_MAX en llamadas individuales)
    try:
        log.info("_place_sl_tp_bulk(%s): fallback → SL=%.6f", coin, sl_px)
        sl_ot_ind = {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}}
        resp_sl = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, sl_px, sl_ot_ind,
            context=f"fallback_sl({coin},{sl_px})",
        )
        log.info("_place_sl_tp_bulk(%s): fallback SL respuesta = %s", coin, resp_sl)
        _check_order_response(resp_sl, f"fallback_sl({coin},{sl_px})")
        log.info("SL colocado (fallback individual): %s SL=%.6f", coin, sl_px)
    except Exception as sl_exc:
        log.error("_place_sl_tp_bulk(%s): fallback SL FALLÓ: %s", coin, sl_exc)

    try:
        log.info("_place_sl_tp_bulk(%s): fallback → TP=%.6f", coin, tp_px)
        tp_ot_ind = {"trigger": {"triggerPx": tp_px, "isMarket": True, "tpsl": "tp"}}
        resp_tp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, tp_px, tp_ot_ind,
            context=f"fallback_tp({coin},{tp_px})",
        )
        log.info("_place_sl_tp_bulk(%s): fallback TP respuesta = %s", coin, resp_tp)
        _check_order_response(resp_tp, f"fallback_tp({coin},{tp_px})")
        log.info("TP colocado (fallback individual): %s TP=%.6f", coin, tp_px)
    except Exception as tp_exc:
        log.error("_place_sl_tp_bulk(%s): fallback TP FALLÓ: %s", coin, tp_exc)


# ── SL y TP individuales (trailing, breakeven, restore) ──────────────────
def _place_stop_order_rounded(symbol: str, side: str, qty: float, stop_price_rounded: float) -> None:
    """v17: limit_px = stop_price_rounded (no 0/INT_MAX en llamadas individuales)."""
    coin       = _hl_symbol(symbol)
    is_buy     = side == "short"
    order_type = {"trigger": {"triggerPx": stop_price_rounded, "isMarket": True, "tpsl": "sl"}}
    log.info("place_stop_order(%s): side=%s is_buy=%s stop=%.6f qty=%.6f",
             coin, side, is_buy, stop_price_rounded, qty)
    resp = _hl_call(
        _order_reduce_only,
        coin, is_buy, qty, stop_price_rounded, order_type,
        context=f"place_stop_order({coin},{stop_price_rounded})",
    )
    log.info("place_stop_order(%s): respuesta HL = %s", coin, resp)
    _check_order_response(resp, f"place_stop_order({coin},{stop_price_rounded})")
    log.info("SL colocado en %.6f (%s %s)", stop_price_rounded, side.upper(), coin)


def _place_tp_order_rounded(symbol: str, side: str, qty: float, tp_price_rounded: float) -> None:
    """v17: limit_px = tp_price_rounded (no 0/INT_MAX en llamadas individuales)."""
    coin       = _hl_symbol(symbol)
    is_buy     = side == "short"
    order_type = {"trigger": {"triggerPx": tp_price_rounded, "isMarket": True, "tpsl": "tp"}}
    log.info("place_tp_order(%s): side=%s is_buy=%s tp=%.6f qty=%.6f",
             coin, side, is_buy, tp_price_rounded, qty)
    resp = _hl_call(
        _order_reduce_only,
        coin, is_buy, qty, tp_price_rounded, order_type,
        context=f"place_tp_order({coin},{tp_price_rounded})",
    )
    log.info("place_tp_order(%s): respuesta HL = %s", coin, resp)
    _check_order_response(resp, f"place_tp_order({coin},{tp_price_rounded})")
    log.info("TP colocado en %.6f (%s %s)", tp_price_rounded, side.upper(), coin)


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> None:
    coin       = _hl_symbol(symbol)
    stop_price = _round_price(coin, stop_price)
    _place_stop_order_rounded(symbol, side, qty, stop_price)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    coin     = _hl_symbol(symbol)
    tp_price = _round_price(coin, tp_price)
    _place_tp_order_rounded(symbol, side, qty, tp_price)


# ── Cerrar posición ──────────────────────────────────────────────────────
def close_position(side: str, qty: float, symbol: str = None) -> dict:
    coin     = _hl_symbol(symbol or config.SYMBOLS[0])
    is_buy   = side == "short"
    limit_px = _market_price(coin, is_buy)
    resp = _hl_call(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"close_position({coin})",
    )
    log.info("Posición cerrada: %s %s", side.upper(), coin)
    return resp


# ── Cancelar órdenes ─────────────────────────────────────────────────────
def get_open_trigger_orders(symbol: str) -> list[dict]:
    coin = _hl_symbol(symbol)
    try:
        all_orders = _hl_call(
            _info.frontend_open_orders, _WALLET_ADDRESS,
            context=f"get_open_trigger_orders({coin})",
        )
        triggers = []
        for o in all_orders:
            if o.get("coin") != coin:
                continue
            ot = o.get("orderType", "")
            if "stop" in ot.lower() or "take profit" in ot.lower() or o.get("triggerPx"):
                triggers.append(o)
        log.debug("get_open_trigger_orders(%s): %d triggers — %s", coin, len(triggers), triggers)
        return triggers
    except Exception as exc:
        log.warning("get_open_trigger_orders(%s) falló: %s", coin, exc)
        return []


def cancel_trigger_orders(symbol: str) -> None:
    coin     = _hl_symbol(symbol)
    triggers = get_open_trigger_orders(symbol)
    if not triggers:
        log.debug("cancel_trigger_orders(%s): sin triggers activos", coin)
        return
    cancelled = 0
    for o in triggers:
        oid = o.get("oid")
        if oid is None:
            continue
        try:
            _hl_call(_exchange.cancel, coin, oid, context=f"cancel_trigger({coin},{oid})")
            cancelled += 1
        except Exception as exc:
            log.warning("cancel_trigger(%s, %s) falló: %s", coin, oid, exc)
    log.info("Triggers cancelados para %s (%d/%d)", coin, cancelled, len(triggers))


def cancel_all_orders(symbol: str = None) -> None:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        all_orders = _hl_call(
            _info.frontend_open_orders, _WALLET_ADDRESS,
            context=f"frontend_open_orders({coin})",
        )
        oids = [
            o["oid"] for o in all_orders
            if o.get("coin") == coin and o.get("oid") is not None
        ]
        if not oids:
            log.debug("cancel_all_orders(%s): no había órdenes abiertas", coin)
            return
        cancelled = 0
        for oid in oids:
            try:
                _hl_call(_exchange.cancel, coin, oid, context=f"cancel_order({coin},{oid})")
                cancelled += 1
            except Exception as exc:
                log.warning("cancel_order(%s, %s) falló: %s", coin, oid, exc)
        log.info("Órdenes canceladas para %s (%d/%d)", coin, cancelled, len(oids))
    except Exception as exc:
        log.warning("cancel_all_orders(%s) falló: %s", coin, exc)


# ── Historial de fills ───────────────────────────────────────────────────
def _normalize_fill(f: dict) -> dict:
    fill_dir       = f.get("dir", "")
    normalized_side = "SELL" if "Long" in fill_dir else "BUY"
    closed_pnl     = float(f.get("closedPnl") or 0)
    order_type     = "TAKE_PROFIT_MARKET" if closed_pnl > 0 else "STOP_MARKET"
    px_str         = str(f.get("px", 0))
    return {
        "side": normalized_side, "type": order_type, "order_type": order_type,
        "px": px_str, "avgPrice": px_str,
        "time": int(f.get("time", 0)), "updateTime": int(f.get("time", 0)),
        "status": "FILLED", "dir": fill_dir, "closedPnl": closed_pnl,
    }


def get_fills(symbol: str = None, limit: int = 20, only_close: bool = True) -> list[dict]:
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
