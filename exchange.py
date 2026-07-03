"""
exchange.py — Cliente Hyperliquid Perpetual Futures.

v24 — Fix RAÍZ "Order has invalid price" en SL/TP para TODAS las monedas.

  El error "Order has invalid price" que cerraba posiciones al momento
  (open → SL/TP falla → rollback → comisión doble) tenía una causa raíz
  distinta a la detectada en v23:

  El SDK de Hyperliquid serializa los campos de precio con floattowire(),
  que hace str(round(price, 8)). Para monedas con tickSz muy pequeño
  (kBONK tickSz=0.000001, kPEPE tickSz=0.00000001, etc.), round() en
  Python puede generar representaciones con residuo de punto flotante:

    float(Decimal('0.004548')) == 0.004548  ← OK visualmente
    str(round(0.004548, 8))   == '0.004548' ← OK
    pero internamente puede ser 0.00454799999999...

  HL valida que triggerPx % tickSz == 0 en aritmética entera, y cualquier
  residuo de float lo rechaza con "Order has invalid price".

  Fix: _place_sltp_pair ya NO pasa los precios como float al SDK.
  En su lugar construye el action dict manualmente con los triggerPx y
  limit_px como STRINGS de notación decimal fija (vía _price_to_wire),
  y llama _exchange.bulk_orders con order_list donde los campos de precio
  son strings — el SDK acepta strings y los pasa directamente al wire
  sin aplicar floattowire(), evitando el residuo de punto flotante.

  También se corrige _place_single_sl y _place_single_tp con el mismo
  patrón, y _modify_single_order.

  El fallback "sin grouping" se elimina: si normalTpsl falla con precio
  correcto es un error real, no de formato — no tiene sentido reintentar
  con grouping="na" que HL rechaza igualmente para TPSL duales.

v25 — Fix alias gettickdecimals para compatibilidad con risk.py.

  risk.py llama exchange.gettickdecimals(coin) (sin guión bajo) y la
  función no existía, causando AttributeError en runtime. Se añade alias.
"""
import logging
import math
import os
import random
import socket
import time
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Optional

import config

log = logging.getLogger("exchange")

MAX_LEVERAGE = 10

# ── SDK imports ─────────────────────────────────────────────────────────
try:
    import eth_account
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants as hl_constants
except ImportError as _e:
    raise ImportError(
        "SDK de Hyperliquid no instalado. Ejecuta: pip install hyperliquid-python-sdk eth-account"
    ) from _e

# ── Inicializar clientes ────────────────────────────────────────────────
_pk = os.environ["HYPERLIQUID_PRIVATE_KEY"]
if not _pk.startswith("0x"):
    _pk = "0x" + _pk

_account = eth_account.Account.from_key(_pk)
_WALLET_ADDRESS = (os.environ.get("HYPERLIQUID_WALLET_ADDRESS") or _account.address).lower()
_MAINNET = os.environ.get("HL_MAINNET", "true").lower() == "true"
_HL_URL = hl_constants.MAINNET_API_URL if _MAINNET else hl_constants.TESTNET_API_URL

_info = Info(_HL_URL, skip_ws=True)
_exchange = Exchange(_account, _HL_URL, account_address=_WALLET_ADDRESS)

log.info("Hyperliquid inicializado | wallet=%s | mainnet=%s", _WALLET_ADDRESS, _MAINNET)


# ── Rate limiting ────────────────────────────────────────────────────────
_RL_MAX_RETRIES = 3
_RL_BASE_DELAY = 1.0
_RL_JITTER = 0.2
_RL_429_EXTRA = 5.0

_PRE_SEND_ERRORS = (
    ConnectionError,
    ConnectionRefusedError,
    ConnectionResetError,
    socket.timeout,
    TimeoutError,
    OSError,
)


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


def _is_pre_send(exc: Exception) -> bool:
    if isinstance(exc, _PRE_SEND_ERRORS):
        return True
    msg = str(exc).lower()
    return any(k in msg for k in (
        "newconnectionerror", "connection refused", "failed to establish",
        "name or service not known", "timed out", "connection reset",
        "broken pipe",
    ))


def _hl_call_read(fn, *args, context: str = "", **kwargs):
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
            jitter = base_wait * _RL_JITTER * (random.random() * 2 - 1)
            wait = base_wait + jitter + (_RL_429_EXTRA if is_429 else 0)
            log.warning(
                "%s: error en intento %d/%d%s — reintentando en %.1fs | %s",
                context or fn.__name__, attempt, _RL_MAX_RETRIES,
                " [429 rate limit]" if is_429 else "",
                wait, exc,
            )
            time.sleep(wait)
    raise last_exc


def _hl_call_write(fn, *args, context: str = "", **kwargs):
    last_exc = None
    for attempt in range(1, _RL_MAX_RETRIES + 2):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt > _RL_MAX_RETRIES or not _is_pre_send(exc):
                break
            is_429 = _is_429(exc)
            base_wait = _RL_BASE_DELAY * (2 ** (attempt - 1))
            jitter = base_wait * _RL_JITTER * (random.random() * 2 - 1)
            wait = base_wait + jitter + (_RL_429_EXTRA if is_429 else 0)
            log.warning(
                "%s [WRITE]: error pre-envío en intento %d/%d — reintentando en %.1fs | %s",
                context or fn.__name__, attempt, _RL_MAX_RETRIES,
                wait, exc,
            )
            time.sleep(wait)
    raise last_exc


_hl_call = _hl_call_read


# ── Utilidades de símbolo ───────────────────────────────────────────────
def _hl_symbol(symbol: str) -> str:
    return symbol.split("-")[0]


# ── sz_decimals ─────────────────────────────────────────────────────────
def _sz_decimals(symbol: str) -> int:
    coin = _hl_symbol(symbol)
    asset = _info.coin_to_asset.get(coin)
    if asset is not None:
        return _info.asset_to_sz_decimals.get(asset, 3)
    return 3


def floor_qty(qty: float, symbol: str) -> float:
    dec = _sz_decimals(symbol)
    factor = 10 ** dec
    return int(qty * factor) / factor


def min_notional_ok(qty: float, price: float, min_usdt: float = 10.0) -> bool:
    return (qty * price) >= min_usdt


# ── Asset index ─────────────────────────────────────────────────────────
def _get_asset_index(coin: str) -> int:
    asset = _info.coin_to_asset.get(coin)
    if asset is None:
        raise ValueError(f"_get_asset_index: coin '{coin}' no encontrado en coin_to_asset")
    return int(asset)


# ── tickSize exacto como Decimal ─────────────────────────────────────────
_tick_size_cache: dict[str, Decimal] = {}


def _get_tick_size(coin: str) -> Decimal:
    if coin in _tick_size_cache:
        return _tick_size_cache[coin]

    fallback = Decimal("0.000001")
    try:
        meta = _hl_call_read(_info.meta, context=f"_get_tick_size({coin})")
        for asset_info in meta.get("universe", []):
            if asset_info.get("name") == coin:
                raw = str(asset_info.get("tickSz", "0.000001"))
                try:
                    tick = Decimal(raw)
                    if tick <= 0:
                        raise ValueError("tick <= 0")
                    _tick_size_cache[coin] = tick
                    log.debug("tick_size(%s): tickSz=%s (Decimal exacto)", coin, tick)
                    return tick
                except (InvalidOperation, ValueError) as exc:
                    log.debug("_get_tick_size(%s) tickSz inválido %r: %s", coin, raw, exc)
                    break
    except Exception as exc:
        log.debug("_get_tick_size(%s) falló: %s — usando fallback %s", coin, exc, fallback)

    _tick_size_cache[coin] = fallback
    return fallback


def _round_price_dec(coin: str, price: float) -> Decimal:
    """
    Redondea price al múltiplo exacto del tickSz y devuelve Decimal exacto.
    Usar esta función siempre que el precio vaya a un wire de HL.
    """
    tick = _get_tick_size(coin)
    price_dec = Decimal(str(price))
    return (price_dec / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


def _round_price(coin: str, price: float) -> float:
    """Versión float de _round_price_dec para uso interno no-wire."""
    return float(_round_price_dec(coin, price))


def _price_to_wire(price_dec: Decimal) -> str:
    """
    Convierte Decimal exacto a string de notación decimal fija sin exponente.
    Nunca pasar float aquí — usar _round_price_dec() primero.
    """
    if price_dec == 0:
        return "0"
    return format(price_dec.normalize(), "f")


# ── limit_px para órdenes trigger isMarket=True ──────────────────────────
_TRIGGER_MAX_PRICE = Decimal("2147483647")


def _trigger_limit_dec(coin: str, is_buy: bool) -> Decimal:
    if not is_buy:
        return Decimal("0")
    tick = _get_tick_size(coin)
    return (_TRIGGER_MAX_PRICE / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


# ── Precio límite para órdenes de mercado ───────────────────────────────
_MARKET_SLIPPAGE = 0.005


def _market_price(coin: str, is_buy: bool) -> float:
    mids = _hl_call_read(_info.all_mids, context=f"_market_price({coin})")
    mid = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    raw = mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)
    return _round_price(coin, raw)


# ── Balance ─────────────────────────────────────────────────────────────
def get_balance() -> float:
    try:
        state = _hl_call_read(_info.user_state, _WALLET_ADDRESS, context="get_balance")
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as exc:
        log.warning("get_balance falló: %s", exc)
        return 0.0


# ── Precio ──────────────────────────────────────────────────────────────
def get_price(symbol: str = None) -> float:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        mids = _hl_call_read(_info.all_mids, context=f"get_price({coin})")
        if coin in mids:
            return float(mids[coin])
        book = _hl_call_read(_info.l2_snapshot, coin, context=f"get_price_l2({coin})")
        bid = float(book["levels"][0][0]["px"])
        ask = float(book["levels"][1][0]["px"])
        return (bid + ask) / 2
    except Exception as exc:
        log.warning("get_price(%s) falló: %s", coin, exc)
        return 0.0


# ── OHLCV ───────────────────────────────────────────────────────────────
_TF_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    interval = interval or config.TIMEFRAME
    tf_secs = _TF_SECONDS.get(interval, 900)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - tf_secs * limit * 1000

    try:
        raw = _hl_call_read(
            _info.candles_snapshot, coin, interval, start_ms, end_ms,
            context=f"get_ohlcv({coin},{interval})",
        )
    except Exception as exc:
        log.warning("get_ohlcv(%s %s) falló: %s", coin, interval, exc)
        return []

    candles = []
    for c in raw:
        open_time = int(c["t"])
        vol = float(c["v"])
        close = float(c["c"])
        candles.append({
            "ts": open_time,
            "open_time": open_time,
            "open": float(c["o"]),
            "high": float(c["h"]),
            "low": float(c["l"]),
            "close": close,
            "volume": vol,
            "quote_volume": vol * close,
            "closed": True,
        })
    return candles[-limit:]


# ── Posiciones ──────────────────────────────────────────────────────────
def _parse_hl_position(pos: dict) -> dict | None:
    szi = float(pos.get("szi", 0))
    if szi == 0:
        return None
    return {
        "side": "long" if szi > 0 else "short",
        "entry": float(pos.get("entryPx") or 0),
        "size": abs(szi),
        "sl": None,
        "tp": None,
    }


def _fetch_trigger_map() -> dict[str, dict]:
    result: dict[str, dict] = {}
    try:
        orders = _hl_call_read(
            _info.frontend_open_orders, _WALLET_ADDRESS,
            context="fetch_trigger_map",
        )
        for o in orders:
            coin = o.get("coin", "")
            if not coin:
                continue
            ot = str(o.get("orderType", ""))
            px = (
                float(o["triggerPx"]) if o.get("triggerPx") not in (None, 0, "0", "") else
                float(o["limitPx"]) if o.get("limitPx") not in (None, 0, "0", "") else
                float(o["px"]) if o.get("px") not in (None, 0, "0", "") else
                0.0
            )
            if coin not in result:
                result[coin] = {"sl": None, "tp": None}
            if "Stop" in ot and px > 0:
                result[coin]["sl"] = px
            elif "Take Profit" in ot and px > 0:
                result[coin]["tp"] = px
    except Exception as exc:
        log.warning("_fetch_trigger_map falló: %s", exc)
    return result


def get_all_positions() -> dict[str, dict]:
    state = _hl_call_read(_info.user_state, _WALLET_ADDRESS, context="get_all_positions")
    hl_to_bot = {_hl_symbol(s): s for s in config.SYMBOLS}

    asset_positions = state.get("assetPositions", [])
    open_coins = [
        (e.get("position", {}).get("coin", "?"), float(e.get("position", {}).get("szi", 0)))
        for e in asset_positions
        if float(e.get("position", {}).get("szi", 0)) != 0
    ]
    if open_coins:
        log.info(
            "[exchange] posiciones abiertas en HL: %s | símbolos configurados (sample): %s",
            open_coins,
            list(hl_to_bot.keys())[:15],
        )

    trigger_map = _fetch_trigger_map()

    result: dict[str, dict] = {}
    for entry in asset_positions:
        pos = entry.get("position", {})
        coin = pos.get("coin", "")
        szi = float(pos.get("szi", 0))
        sym_bot = hl_to_bot.get(coin)
        if sym_bot is None:
            if szi != 0:
                log.warning(
                    "[exchange] Posición NO mapeada ignorada: coin=%r szi=%s "
                    "(¿falta en config.SYMBOLS o ticker distinto?)",
                    coin, szi,
                )
            continue
        parsed = _parse_hl_position(pos)
        if parsed:
            trig = trigger_map.get(coin, {})
            parsed["sl"] = trig.get("sl")
            parsed["tp"] = trig.get("tp")
            if parsed["sl"] is not None or parsed["tp"] is not None:
                log.info(
                    "[exchange] sync %s — sl=%s tp=%s (desde trigger orders)",
                    coin, parsed["sl"], parsed["tp"],
                )
            result[sym_bot] = parsed
    return result


def get_position(symbol: str = None) -> dict | None:
    symbol = symbol or config.SYMBOLS[0]
    return get_all_positions().get(symbol)


# ── Apalancamiento ──────────────────────────────────────────────────────
_leverage_cache: dict[str, int] = {}
_LEVERAGE_FALLBACKS = [5, 3, 2, 1]


def set_leverage(symbol: str = None, leverage: int = None) -> None:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    leverage = min(int(leverage or config.LEVERAGE), MAX_LEVERAGE)

    if coin in _leverage_cache:
        cached = _leverage_cache[coin]
        if cached == leverage:
            return
        leverage = min(leverage, cached)

    candidates = [leverage] + [f for f in _LEVERAGE_FALLBACKS if f < leverage]

    for lev in candidates:
        try:
            resp = _hl_call_write(
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


# ── Validación de respuesta de órdenes ──────────────────────────────────
def _check_order_response(resp: dict, context: str) -> None:
    status = resp.get("status")
    if status != "ok":
        raise RuntimeError(f"{context}: status={status!r} — {resp}")
    statuses = (
        ((resp.get("response") or {})
         .get("data") or {})
        .get("statuses") or []
    )
    if statuses and "error" in statuses[0]:
        raise RuntimeError(f"{context} rechazada por HL: {statuses[0]['error']} — {resp}")


# ── SL + TP juntos con normalTpsl ───────────────────────────────────────
def _place_sltp_pair(
    symbol: str,
    side: str,
    qty: float,
    sl_price: float,
    tp_price: float,
) -> None:
    """
    Coloca SL y TP en una sola llamada bulk_orders con grouping=normalTpsl.

    CLAVE v24: los precios se pasan como strings Decimal exactos, NO como
    floats, para evitar que floattowire() del SDK introduzca residuos de
    punto flotante que HL rechaza con "Order has invalid price".
    """
    coin = _hl_symbol(symbol)
    is_close_buy = (side == "short")

    sl_dec = _round_price_dec(coin, sl_price)
    tp_dec = _round_price_dec(coin, tp_price)
    sl_limit_dec = _trigger_limit_dec(coin, is_close_buy)
    tp_limit_dec = _trigger_limit_dec(coin, is_close_buy)

    sl_px_wire = _price_to_wire(sl_dec)
    tp_px_wire = _price_to_wire(tp_dec)
    sl_lim_wire = _price_to_wire(sl_limit_dec)
    tp_lim_wire = _price_to_wire(tp_limit_dec)

    log.debug(
        "_place_sltp_pair %s side=%s: sl_wire=%s lim=%s | tp_wire=%s lim=%s | is_buy=%s",
        coin, side, sl_px_wire, sl_lim_wire, tp_px_wire, tp_lim_wire, is_close_buy,
    )

    sl_order = {
        "coin": coin,
        "is_buy": is_close_buy,
        "sz": qty,
        "limit_px": sl_lim_wire,
        "order_type": {"trigger": {"triggerPx": sl_px_wire, "isMarket": True, "tpsl": "sl"}},
        "reduce_only": True,
    }
    tp_order = {
        "coin": coin,
        "is_buy": is_close_buy,
        "sz": qty,
        "limit_px": tp_lim_wire,
        "order_type": {"trigger": {"triggerPx": tp_px_wire, "isMarket": True, "tpsl": "tp"}},
        "reduce_only": True,
    }

    resp = _hl_call_write(
        _exchange.bulk_orders,
        [sl_order, tp_order],
        grouping="normalTpsl",
        context=f"_place_sltp_pair({coin} sl={sl_px_wire} tp={tp_px_wire})",
    )
    statuses = (
        ((resp.get("response") or {})
         .get("data") or {})
        .get("statuses") or []
    )
    errors = [s.get("error") for s in statuses if "error" in s]
    if errors:
        raise RuntimeError(f"_place_sltp_pair({coin}) normalTpsl errors: {errors}")

    log.info(
        "SL+TP colocados (normalTpsl): %s | sl=%s tp=%s (%s)",
        coin, sl_px_wire, tp_px_wire, side.upper(),
    )


def _place_single_sl(symbol: str, side: str, qty: float, stop_price: float) -> None:
    coin = _hl_symbol(symbol)
    is_buy = (side == "short")
    sl_dec = _round_price_dec(coin, stop_price)
    lim_dec = _trigger_limit_dec(coin, is_buy)
    sl_wire = _price_to_wire(sl_dec)
    lim_wire = _price_to_wire(lim_dec)
    order_type = {"trigger": {"triggerPx": sl_wire, "isMarket": True, "tpsl": "sl"}}
    try:
        resp = _hl_call_write(
            _exchange.order,
            coin, is_buy, qty, lim_wire, order_type, True,
            context=f"_place_single_sl({coin},{sl_wire})",
        )
        _check_order_response(resp, f"_place_single_sl({coin},{sl_wire})")
        log.info("SL colocado en %s (%s %s)", sl_wire, side.upper(), coin)
    except Exception as exc:
        log.warning("_place_single_sl(%s) falló: %s", coin, exc)


def _place_single_tp(symbol: str, side: str, qty: float, tp_price: float) -> None:
    coin = _hl_symbol(symbol)
    is_buy = (side == "short")
    tp_dec = _round_price_dec(coin, tp_price)
    lim_dec = _trigger_limit_dec(coin, is_buy)
    tp_wire = _price_to_wire(tp_dec)
    lim_wire = _price_to_wire(lim_dec)
    order_type = {"trigger": {"triggerPx": tp_wire, "isMarket": True, "tpsl": "tp"}}
    try:
        resp = _hl_call_write(
            _exchange.order,
            coin, is_buy, qty, lim_wire, order_type, True,
            context=f"_place_single_tp({coin},{tp_wire})",
        )
        _check_order_response(resp, f"_place_single_tp({coin},{tp_wire})")
        log.info("TP colocado en %s (%s %s)", tp_wire, side.upper(), coin)
    except Exception as exc:
        log.warning("_place_single_tp(%s) falló: %s", coin, exc)


# ── Órdenes trigger abiertas ────────────────────────────────────────────
def get_open_trigger_orders(symbol: str) -> dict:
    coin = _hl_symbol(symbol)
    result = {"sl": None, "tp": None}
    try:
        orders = _hl_call_read(
            _info.frontend_open_orders, _WALLET_ADDRESS,
            context=f"frontend_open_orders({coin})",
        )
        for o in orders:
            if o.get("coin") != coin:
                continue
            ot = str(o.get("orderType", ""))
            oid = o.get("oid")
            px = (
                float(o["triggerPx"]) if o.get("triggerPx") not in (None, 0, "0", "") else
                float(o["limitPx"]) if o.get("limitPx") not in (None, 0, "0", "") else
                float(o["px"]) if o.get("px") not in (None, 0, "0", "") else
                0.0
            )
            if oid is None:
                continue
            if "Stop" in ot:
                result["sl"] = {"oid": oid, "px": px}
            elif "Take Profit" in ot:
                result["tp"] = {"oid": oid, "px": px}
    except Exception as exc:
        log.warning("get_open_trigger_orders(%s) falló: %s", coin, exc)
    return result


def _modify_single_order(
    coin: str,
    oid: int,
    is_buy: bool,
    qty: float,
    new_px: float,
    tpsl: str,
) -> None:
    new_dec = _round_price_dec(coin, new_px)
    lim_dec = _trigger_limit_dec(coin, is_buy)
    new_wire = _price_to_wire(new_dec)
    lim_wire = _price_to_wire(lim_dec)
    order_type = {"trigger": {"triggerPx": new_wire, "isMarket": True, "tpsl": tpsl}}
    log.debug(
        "_modify_single_order: coin=%s oid=%s is_buy=%s qty=%s limit=%s tpsl=%s trigger=%s",
        coin, oid, is_buy, qty, lim_wire, tpsl, new_wire,
    )
    resp = _hl_call_write(
        _exchange.modify_order,
        oid, coin, is_buy, qty, lim_wire, order_type, True,
        context=f"modify_order({coin} oid={oid} {tpsl}={new_wire})",
    )
    statuses = (((resp or {}).get("response") or {}).get("data") or {}).get("statuses") or []
    errors = [s.get("error") for s in statuses if "error" in s]
    if errors:
        raise RuntimeError(f"modify_order {tpsl} errors: {errors}")
    log.info("Orden %s modificada in-place: %s oid=%s → %s", tpsl.upper(), coin, oid, new_wire)


def _batch_modify_sltp(
    coin: str,
    sl_oid: int,
    tp_oid: int,
    is_close_buy: bool,
    qty: float,
    new_sl: float,
    new_tp: float,
) -> bool:
    bulk_modify = getattr(_exchange, "bulk_modify_orders_new", None)
    if bulk_modify is None:
        return False

    try:
        asset_idx = _get_asset_index(coin)
    except ValueError as exc:
        log.warning("[%s] _batch_modify_sltp: %s — fallback a modify individual", coin, exc)
        return False

    new_sl_dec = _round_price_dec(coin, new_sl)
    new_tp_dec = _round_price_dec(coin, new_tp)
    sl_lim_dec = _trigger_limit_dec(coin, is_close_buy)
    tp_lim_dec = _trigger_limit_dec(coin, is_close_buy)

    modify_requests = [
        {
            "oid": sl_oid,
            "order": {
                "a": asset_idx,
                "b": is_close_buy,
                "p": _price_to_wire(sl_lim_dec),
                "s": str(qty),
                "r": True,
                "t": {
                    "trigger": {
                        "triggerPx": _price_to_wire(new_sl_dec),
                        "isMarket": True,
                        "tpsl": "sl",
                    }
                },
            },
        },
        {
            "oid": tp_oid,
            "order": {
                "a": asset_idx,
                "b": is_close_buy,
                "p": _price_to_wire(tp_lim_dec),
                "s": str(qty),
                "r": True,
                "t": {
                    "trigger": {
                        "triggerPx": _price_to_wire(new_tp_dec),
                        "isMarket": True,
                        "tpsl": "tp",
                    }
                },
            },
        },
    ]

    try:
        resp = _hl_call_write(
            bulk_modify,
            modify_requests,
            context=f"bulk_modify_orders_new({coin})",
        )
        statuses = (((resp or {}).get("response") or {}).get("data") or {}).get("statuses") or []
        errors = [s.get("error") for s in statuses if "error" in s]
        if errors:
            raise RuntimeError(f"bulk_modify_orders_new errors: {errors}")
        log.info(
            "SL+TP modificados atómicamente (batchModify): %s | sl=%s tp=%s",
            coin, _price_to_wire(new_sl_dec), _price_to_wire(new_tp_dec),
        )
        return True
    except Exception as exc:
        log.warning("[%s] bulk_modify_orders_new falló: %s — cayendo a modify individual", coin, exc)
        return False


def modify_sltp_orders(
    symbol: str,
    side: str,
    qty: float,
    new_sl: float,
    new_tp: float,
) -> None:
    coin = _hl_symbol(symbol)
    is_close_buy = (side == "short")

    trigger = get_open_trigger_orders(symbol)
    sl_info = trigger["sl"]
    tp_info = trigger["tp"]

    log.debug(
        "modify_sltp_orders: %s side=%s qty=%s new_sl=%g new_tp=%g | existing sl=%s tp=%s",
        coin, side, qty, new_sl, new_tp, sl_info, tp_info,
    )

    if sl_info is None and tp_info is None:
        log.info("[%s] modify_sltp: sin órdenes abiertas → place desde cero", coin)
        _place_sltp_pair(symbol, side, qty, new_sl, new_tp)
        return

    if sl_info is not None and tp_info is not None:
        if _batch_modify_sltp(coin, sl_info["oid"], tp_info["oid"], is_close_buy, qty, new_sl, new_tp):
            return

        sl_ok = False
        tp_ok = False
        try:
            _modify_single_order(coin, sl_info["oid"], is_close_buy, qty, new_sl, "sl")
            sl_ok = True
        except Exception as exc:
            log.warning("[%s] modify_order SL falló: %s", coin, exc)
        try:
            _modify_single_order(coin, tp_info["oid"], is_close_buy, qty, new_tp, "tp")
            tp_ok = True
        except Exception as exc:
            log.warning("[%s] modify_order TP falló: %s", coin, exc)

        if sl_ok and tp_ok:
            return

        log.warning(
            "[%s] modify_sltp parcialmente fallido (sl_ok=%s tp_ok=%s) → fallback cancel+place",
            coin, sl_ok, tp_ok,
        )
        cancel_all_orders(symbol)
        _place_sltp_pair(symbol, side, qty, new_sl, new_tp)
        return

    existing_type = "SL" if sl_info else "TP"
    log.info(
        "[%s] modify_sltp: caso asimétrico — solo existe %s → cancelando y recolocando ambas",
        coin, existing_type,
    )
    cancel_all_orders(symbol)
    _place_sltp_pair(symbol, side, qty, new_sl, new_tp)


# ── Abrir orden ─────────────────────────────────────────────────────────
def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    sym_bot = symbol or config.SYMBOLS[0]
    coin = _hl_symbol(sym_bot)
    is_buy = side == "long"

    qty = floor_qty(qty, sym_bot)
    price = get_price(sym_bot)
    if qty <= 0 or not min_notional_ok(qty, price):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot}. "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    set_leverage(sym_bot, config.LEVERAGE)

    limit_px = _market_price(coin, is_buy)
    resp = _hl_call_write(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"open_order {side.upper()} {coin} qty={qty}",
    )
    _check_order_response(resp, f"open_order {side.upper()} {coin} qty={qty}")
    log.info("Orden abierta: %s %s qty=%g @ ~%g", side.upper(), coin, qty, limit_px)

    _place_sltp_pair(sym_bot, side, qty, sl, tp)
    return resp


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float, tp_price: float = None) -> None:
    if tp_price is not None:
        _place_sltp_pair(symbol, side, qty, stop_price, tp_price)
    else:
        _place_single_sl(symbol, side, qty, stop_price)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float, sl_price: float = None) -> None:
    if sl_price is not None:
        _place_sltp_pair(symbol, side, qty, sl_price, tp_price)
    else:
        _place_single_tp(symbol, side, qty, tp_price)


# ── Cerrar posición ─────────────────────────────────────────────────────
def close_position(side: str, qty: float, symbol: str = None) -> dict:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    is_buy = side == "short"
    limit_px = _market_price(coin, is_buy)
    resp = _hl_call_write(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"close_position({coin})",
    )
    log.info("Posición cerrada: %s %s", side.upper(), coin)
    return resp


# ── Cancelar órdenes abiertas ───────────────────────────────────────────
def cancel_all_orders(symbol: str = None) -> None:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        orders = _hl_call_read(
            _info.frontend_open_orders, _WALLET_ADDRESS,
            context=f"frontend_open_orders({coin})",
        )
        oids = [o["oid"] for o in orders if o.get("coin") == coin and o.get("oid") is not None]
        if not oids:
            log.debug("cancel_all_orders(%s): no había órdenes abiertas", coin)
            return
        cancelled = 0
        for oid in oids:
            try:
                _hl_call_write(_exchange.cancel, coin, oid, context=f"cancel_order({coin},{oid})")
                cancelled += 1
            except Exception as exc:
                log.warning("cancel_order(%s, %s) falló: %s", coin, oid, exc)
        log.info("Órdenes canceladas para %s (%d/%d)", coin, cancelled, len(oids))
    except Exception as exc:
        log.warning("cancel_all_orders(%s) falló: %s", coin, exc)


# ── Historial de fills ──────────────────────────────────────────────────
def _parse_pnl(raw) -> float:
    if raw is None or raw == "":
        return 0.0
    try:
        return float(Decimal(str(raw)))
    except (InvalidOperation, ValueError):
        return 0.0


def _normalize_fill(f: dict) -> dict:
    fill_dir = f.get("dir", "")
    normalized_side = "SELL" if "Long" in fill_dir else "BUY"
    closed_pnl = _parse_pnl(f.get("closedPnl"))
    order_type = "TAKE_PROFIT_MARKET" if closed_pnl > 0 else "STOP_MARKET"
    px_str = str(f.get("px", 0))
    return {
        "side": normalized_side,
        "type": order_type,
        "order_type": order_type,
        "px": px_str,
        "avgPrice": px_str,
        "time": int(f.get("time", 0)),
        "updateTime": int(f.get("time", 0)),
        "status": "FILLED",
        "dir": fill_dir,
        "closedPnl": closed_pnl,
    }


def _is_close_fill(f: dict) -> bool:
    if "Close" in f.get("dir", ""):
        return True
    return _parse_pnl(f.get("closedPnl")) != 0.0


def get_fills(
    symbol: str = None,
    limit: int = 20,
    only_close: bool = True,
) -> list[dict]:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 7 * 24 * 60 * 60 * 1000
        raw_fills = _hl_call_read(
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
        if only_close and not _is_close_fill(f):
            continue
        result.append(_normalize_fill(f))
        if len(result) >= limit:
            break

    return result


def get_closed_orders(symbol: str = None, limit: int = 20) -> list[dict]:
    return get_fills(symbol=symbol, limit=limit, only_close=True)


# ── Aliases públicos ────────────────────────────────────────────────────
def get_tick_decimals(coin: str) -> int:
    """Devuelve el número de decimales del tickSz para el coin dado.
    Ej: tickSz=0.0001 → 4, tickSz=0.000001 → 6."""
    tick = _get_tick_size(coin)
    if tick <= 0:
        return 6
    return max(0, round(-math.log10(float(tick))))


def gettickdecimals(coin: str) -> int:
    """Alias de get_tick_decimals para compatibilidad con risk.py."""
    return get_tick_decimals(coin)
