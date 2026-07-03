"""exchange.py — Cliente Hyperliquid Perpetual Futures.

v2 — Rate limiting (fix 429)
v3 — Fix Invalid leverage value (bisect automático)
v4 — Fix cancel_all_orders (cancel uno por uno)
v5 — Fix reduce_only no llegaba al SDK (arg posicional)
v6 — Fix _check_order_response demasiado estricta para SL/TP
v7 — Fix "Order has invalid price" en SL/TP:
  HL exige que triggerPx respete el tickSize del activo.
  risk.py redondea a 8 decimales, pero ONDO/PYTH/etc. tienen tickSize
  de 4 decimales — 0.32632714 es inválido, necesita 0.3263.
  Fix: _round_price(coin, price) lee el campo 'tickSize' del meta de HL
  y redondea al número correcto de decimales antes de cada place_stop_order
  y place_tp_order. Cache en _tick_decimals para no consultar meta en cada
  orden.
v8 — Fix "Order has invalid price" en open_order (entrada IOC):
  _market_price() devuelve mid * (1 ± slippage) sin redondear, lo que
  genera precios como 0.3371775 o 0.33712222499999994 que HL rechaza.
  Fix: aplicar _round_price(coin, limit_px) antes del order() de entrada,
  igual que ya se hacía en SL/TP.
v9 — Fix "floattowire causes rounding":
  El SDK serializa el precio vía floattowire y rechaza cualquier float que
  no sea exactamente representable con los decimales del tickSize.
  round() en Python puede dejar residuos de punto flotante
  (ej: round(0.33767..., 4) → 0.3377000000000001).
  Fix: usar Decimal con quantize() para obtener una representación exacta,
  y convertir a float solo al final. Aplica en _round_price() para cubrir
  _market_price, place_stop_order y place_tp_order.
v10 — Fix race condition en open_order (mismo precio para notional check y limit_px).
  Se unificó la llamada a _market_price: un único valor limit_px se usa tanto
  para la validación min_notional_ok como para el precio de la orden IOC,
  eliminando la doble llamada a all_mids() que podía diferir 20-50 ms.
v11 — Revert accidental: restaurado floor_qty y set_leverage antes de la orden.
  (regresión introducida en v10 por simplificación excesiva)
v12 — Limpieza de logs redundantes introducidos en v10/v11.
v13 — Segunda unificación de _market_price en open_order (fix definitivo race).
v14 — Fix 4 bugs detectados comparando con documentación oficial HL:
  1. limit_px en SL/TP era triggerPx — HL trigger market exige precio extremo:
     - Cerrar long (vender):  limit_px = 0
     - Cerrar short (comprar): limit_px = 2_147_483_647
     Pasar triggerPx como limit_px hacía que HL tratara la orden como límite
     y la rechazaba con "Order has invalid price" en el momento del trigger.
  2. cancel_all_orders usaba open_orders() que NO devuelve trigger orders (SL/TP).
     Fix: usar frontend_open_orders() que incluye tanto órdenes límite como triggers.
     Esto era la causa real de que el trailing y breakeven acumularan SL/TP duplicados.
  3. open_order enviaba place_stop_order + place_tp_order en llamadas separadas.
     Fix: usar bulk_orders con grouping="normalTpsl" para enviar SL y TP juntos,
     tal como exige HL para que reconozca la relación entre ellos y no rechace
     el segundo con "Invalid TPSL price".
  4. Añadidas get_open_trigger_orders() y cancel_trigger_orders() para poder
     consultar y cancelar exclusivamente las trigger orders de un símbolo,
     útil para trailing y breakeven que solo necesitan reemplazar SL/TP.
"""
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
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants as hl_constants
except ImportError as _e:
    raise ImportError(
        "SDK de Hyperliquid no instalado. Ejecuta: pip install hyperliquid-python-sdk eth-account"
    ) from _e

# ── Inicializar clientes ───────────────────────────────────────────────────────────
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


# ── Rate limiting: exponential backoff con jitter ───────────────────────────────────

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


# ── Utilidades de símbolo ───────────────────────────────────────────────────────────

def _hl_symbol(symbol: str) -> str:
    return symbol.split("-")[0]


# ── sz_decimals ────────────────────────────────────────────────────────────────

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


# ── tickSize / price rounding ────────────────────────────────────────────────────────
# Cache: coin → número de decimales del tickSize de precio
_tick_decimals: dict[str, int] = {}


def _get_tick_decimals(coin: str) -> int:
    """Devuelve los decimales del tickSize de precio para `coin`.

    Lee _info.meta (dato estático cargado en arranque) y cachea el resultado.
    Si no encuentra la info cae a 6 decimales (seguro para cualquier par).
    """
    if coin in _tick_decimals:
        return _tick_decimals[coin]

    try:
        meta = _info.meta()  # {"universe": [{"name", "szDecimals", "tickSz", ...}, ...]}
        for asset_info in meta.get("universe", []):
            if asset_info.get("name") == coin:
                tick_sz = float(asset_info.get("tickSz", 0.0001))
                # número de decimales = cuantos ceros tras el punto tiene el tickSize
                # ej: 0.0001 -> 4, 0.001 -> 3, 0.00001 -> 5, 1 -> 0
                dec = max(0, round(-math.log10(tick_sz)))
                _tick_decimals[coin] = dec
                log.debug("tick_decimals(%s): tickSz=%s → %d decimales", coin, tick_sz, dec)
                return dec
    except Exception as exc:
        log.debug("_get_tick_decimals(%s) falló: %s — usando 6 dec", coin, exc)

    _tick_decimals[coin] = 6  # fallback conservador
    return 6


def _round_price(coin: str, price: float) -> float:
    """Redondea `price` al tickSize del par usando Decimal para evitar
    residuos de punto flotante que el SDK rechaza en floattowire."""
    dec = _get_tick_decimals(coin)
    # Usar Decimal con quantize garantiza representación exacta
    quantizer = Decimal(10) ** -dec  # ej: dec=4 → Decimal('0.0001')
    rounded = Decimal(str(price)).quantize(quantizer, rounding=ROUND_HALF_UP)
    return float(rounded)


# ── Precio límite para órdenes de mercado ─────────────────────────────────────────
_MARKET_SLIPPAGE = 0.005

def _market_price(coin: str, is_buy: bool) -> float:
    mids = _hl_call(_info.all_mids, context=f"_market_price({coin})")
    mid  = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    raw = mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)
    return _round_price(coin, raw)  # v8+v9: Decimal rounding


# ── Precio extremo para trigger market orders (SL/TP) ────────────────────────────
# v14: HL docs exigen que limit_px en una trigger market order sea un precio
# extremo que garantice ejecución inmediata al dispararse el trigger.
# NO debe ser el triggerPx — ese error hace que HL la trate como límite.
_TRIGGER_MARKET_SELL_PX = 0              # cerrar long (vender) → precio mínimo posible
_TRIGGER_MARKET_BUY_PX  = 2_147_483_647  # cerrar short (comprar) → precio máximo posible


# ── Balance ───────────────────────────────────────────────────────────────────

def get_balance() -> float:
    try:
        state = _hl_call(_info.user_state, _WALLET_ADDRESS, context="get_balance")
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as exc:
        log.warning("get_balance falló: %s", exc)
        return 0.0


# ── Precio ──────────────────────────────────────────────────────────────────────

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


# ── OHLCV ───────────────────────────────────────────────────────────────────────

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


# ── Posiciones ────────────────────────────────────────────────────────────────────

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


# ── Apalancamiento ────────────────────────────────────────────────────────────────

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


# ── Validación de respuesta de órdenes ───────────────────────────────────────────────

def _check_order_response(resp: dict, context: str) -> None:
    """Lanza RuntimeError solo si HL devuelve error explícito."""
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


def _order_reduce_only(coin, is_buy, qty, price, order_type):
    """Wrapper que pasa reduce_only=True como 6º arg posicional al SDK."""
    return _exchange.order(coin, is_buy, qty, price, order_type, True)


# ── Abrir orden con SL+TP en una sola llamada bulk ──────────────────────────────────
# v14: SL y TP se envían juntos con grouping="normalTpsl" para que HL reconozca
# la relación entre ellos. Enviarlos separados causaba "Invalid TPSL price" en
# el segundo al no encontrar la contraparte en el mismo batch.

def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    sym_bot = symbol or config.SYMBOLS[0]
    coin    = _hl_symbol(sym_bot)
    is_buy  = side == "long"

    qty      = floor_qty(qty, sym_bot)
    limit_px = _market_price(coin, is_buy)  # una sola llamada → sin race condition

    if qty <= 0 or not min_notional_ok(qty, limit_px):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot}. "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    set_leverage(sym_bot, config.LEVERAGE)

    # 1) Orden de entrada IOC
    resp = _hl_call(
        _exchange.order,
        coin, is_buy, qty, limit_px,
        {"limit": {"tif": "Ioc"}},
        context=f"open_order {side.upper()} {coin} qty={qty}",
    )
    _check_order_response(resp, f"open_order {side.upper()} {coin} qty={qty}")
    log.info("Orden abierta: %s %s qty=%.4f @ ~%.4f", side.upper(), coin, qty, limit_px)

    # 2) SL + TP juntos en bulk (v14)
    _place_sl_tp_bulk(sym_bot, side, qty, sl, tp)
    return resp


def _place_sl_tp_bulk(symbol: str, side: str, qty: float, sl: float, tp: float) -> None:
    """Coloca SL y TP en una sola llamada bulk_orders con grouping='normalTpsl'.

    v14: limit_px para trigger market orders es un precio extremo (0 para vender,
    INT_MAX para comprar), NO el triggerPx. Usar triggerPx como limit_px hacía que
    HL tratara la orden como límite y la rechazara al momento del trigger.
    """
    coin   = _hl_symbol(symbol)
    is_buy = side == "short"  # cerrar long → vender; cerrar short → comprar

    sl_px  = _round_price(coin, sl)
    tp_px  = _round_price(coin, tp)

    # v14: precio extremo según dirección de cierre
    limit_px_close = _TRIGGER_MARKET_BUY_PX if is_buy else _TRIGGER_MARKET_SELL_PX

    sl_order = {
        "coin":        coin,
        "is_buy":      is_buy,
        "sz":          qty,
        "limit_px":    limit_px_close,
        "order_type":  {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}},
        "reduce_only": True,
    }
    tp_order = {
        "coin":        coin,
        "is_buy":      is_buy,
        "sz":          qty,
        "limit_px":    limit_px_close,
        "order_type":  {"trigger": {"triggerPx": tp_px, "isMarket": True, "tpsl": "tp"}},
        "reduce_only": True,
    }

    try:
        resp = _hl_call(
            _exchange.bulk_orders,
            [sl_order, tp_order],
            "normalTpsl",
            context=f"_place_sl_tp_bulk({coin} SL={sl_px} TP={tp_px})",
        )
        _check_order_response(resp, f"_place_sl_tp_bulk({coin})")
        log.info("SL+TP colocados (bulk normalTpsl): %s SL=%.6f TP=%.6f", coin, sl_px, tp_px)
    except Exception as exc:
        log.warning("_place_sl_tp_bulk(%s) falló: %s — intentando colocación individual", coin, exc)
        # Fallback: colocación individual si bulk falla (ej: SDK antiguo sin bulk_orders)
        place_stop_order(symbol, side, qty, sl)
        place_tp_order(symbol, side, qty, tp)


# ── SL y TP individuales (usados por trailing, breakeven, restore) ───────────────────

def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> None:
    """Coloca un SL trigger market.

    v14: limit_px es precio extremo (0 para sell, INT_MAX para buy), NO stop_price.
    Pasar stop_price como limit_px hacía que HL rechazara la orden al ejecutarse.
    """
    coin       = _hl_symbol(symbol)
    is_buy     = side == "short"  # cerrar long → vender; cerrar short → comprar
    stop_price = _round_price(coin, stop_price)
    # v14: precio extremo según dirección
    limit_px   = _TRIGGER_MARKET_BUY_PX if is_buy else _TRIGGER_MARKET_SELL_PX
    order_type = {"trigger": {"triggerPx": stop_price, "isMarket": True, "tpsl": "sl"}}
    try:
        resp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, limit_px, order_type,
            context=f"place_stop_order({coin},{stop_price})",
        )
        _check_order_response(resp, f"place_stop_order({coin},{stop_price})")
        log.info("SL colocado en %s (%s %s)", stop_price, side.upper(), coin)
    except Exception as exc:
        log.warning("place_stop_order(%s) falló: %s", coin, exc)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    """Coloca un TP trigger market.

    v14: limit_px es precio extremo (0 para sell, INT_MAX para buy), NO tp_price.
    Pasar tp_price como limit_px hacía que HL rechazara la orden al ejecutarse.
    """
    coin     = _hl_symbol(symbol)
    is_buy   = side == "short"  # cerrar long → vender; cerrar short → comprar
    tp_price = _round_price(coin, tp_price)
    # v14: precio extremo según dirección
    limit_px   = _TRIGGER_MARKET_BUY_PX if is_buy else _TRIGGER_MARKET_SELL_PX
    order_type = {"trigger": {"triggerPx": tp_price, "isMarket": True, "tpsl": "tp"}}
    try:
        resp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, limit_px, order_type,
            context=f"place_tp_order({coin},{tp_price})",
        )
        _check_order_response(resp, f"place_tp_order({coin},{tp_price})")
        log.info("TP colocado en %s (%s %s)", tp_price, side.upper(), coin)
    except Exception as exc:
        log.warning("place_tp_order(%s) falló: %s", coin, exc)


# ── Cerrar posición ──────────────────────────────────────────────────────────────────

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


# ── Cancelar órdenes abiertas ──────────────────────────────────────────────────────────
# v14: frontend_open_orders() devuelve TODAS las órdenes del usuario (límite + trigger).
# open_orders() solo devuelve órdenes límite — NO incluye SL/TP (trigger orders).
# Usar open_orders() era la causa de que el trailing y breakeven acumularan
# SL/TP duplicados: se "cancelaba" pero los triggers quedaban intactos.

def get_open_trigger_orders(symbol: str) -> list[dict]:
    """Devuelve las trigger orders (SL/TP) abiertas para un símbolo.

    v14: usa frontend_open_orders que incluye trigger orders.
    Filtra solo las que son trigger (tienen campo 'triggerPx' o 'orderType' trigger).
    """
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
            # HL devuelve "Stop Market", "Take Profit Market", etc. para triggers
            if "stop" in ot.lower() or "take profit" in ot.lower() or o.get("triggerPx"):
                triggers.append(o)
        return triggers
    except Exception as exc:
        log.warning("get_open_trigger_orders(%s) falló: %s", coin, exc)
        return []


def cancel_trigger_orders(symbol: str) -> None:
    """Cancela solo las trigger orders (SL/TP) de un símbolo.

    Útil para trailing y breakeven que solo necesitan reemplazar las triggers,
    sin cancelar órdenes límite normales.
    """
    coin = _hl_symbol(symbol)
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
    """Cancela todas las órdenes de un símbolo: límite Y trigger (SL/TP).

    v14: usa frontend_open_orders() en vez de open_orders() para incluir
    también las trigger orders. open_orders() solo devuelve órdenes límite.
    """
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        all_orders = _hl_call(
            _info.frontend_open_orders, _WALLET_ADDRESS,
            context=f"frontend_open_orders({coin})",
        )
        oids = [o["oid"] for o in all_orders if o.get("coin") == coin and o.get("oid") is not None]
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


# ── Historial de fills ────────────────────────────────────────────────────────────────

def _normalize_fill(f: dict) -> dict:
    fill_dir = f.get("dir", "")
    normalized_side = "SELL" if "Long" in fill_dir else "BUY"
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
