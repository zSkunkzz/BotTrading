"""exchange.py — Cliente Hyperliquid Perpetual Futures.

v2 — Rate limiting (fix 429)
v3 — Fix Invalid leverage value (bisect automático)
v4 — Fix cancel_all_orders (cancel uno por uno)
v5 — Fix reduce_only no llegaba al SDK (arg posicional)
v6 — Fix _check_order_response demasiado estricta para SL/TP
v7 — Fix "Order has invalid price" en SL/TP
v8 — Fix "Order has invalid price" en open_order (entrada IOC)
v9 — Fix "floattowire causes rounding"
v10-diag — Log diagnóstico en get_all_positions()
v11 — Fix "Invalid TPSL price" al colocar SL+TP por separado:
  Hyperliquid rechaza añadir un TP como orden independiente cuando ya
  existe un SL activo sobre la misma posición (y viceversa).
  Fix: _place_sltp_pair() envía SL y TP juntos en una sola llamada
  bulk_orders con grouping="normalTpsl". open_order, place_stop_order
  y place_tp_order usan esta función internamente.
v12 — Fix CRÍTICO grouping="normalTpsl" nunca se pasaba a bulk_orders:
  La firma del SDK es bulk_orders(order_requests, builder=None, grouping="na").
  _place_sltp_pair llamaba bulk_orders([sl, tp]) sin el kwarg grouping,
  por lo que HL recibía siempre grouping="na" y rechazaba la segunda orden
  como orden independiente conflictiva.
  Fix: pasar grouping="normalTpsl" explícitamente.
  También se corrige _restore_sl_tp_on_sync en main.py que llamaba
  place_stop_order + place_tp_order por separado en lugar de _place_sltp_pair.
v13 — Fix "Order has invalid price" en open_order (doble llamada de precio):
  open_order llamaba primero get_price() para validar el notional y luego
  _market_price() para la orden de entrada. Ambas hacen all_mids() por separado,
  y si el precio cambia entre las dos llamadas (20-50ms de diferencia en BTC),
  el limit_px calculado puede quedar fuera del ask en el momento de la firma,
  generando "Order has invalid price" aunque la orden parezca válida.
  Fix: calcular limit_px UNA SOLA VEZ con _market_price() y usar ese valor
  también para el check de notional mínimo. Se elimina la llamada separada
  a get_price() en open_order.
v14 — Fix "Order has invalid price" en open_order (slippage demasiado alto):
  _MARKET_SLIPPAGE=0.5% hacía que limit_px quedara ~$310 sobre el ask real
  en BTC (precio ~62 626), lo que excede el límite de desviación que aplica
  Hyperliquid respecto al mark price en activos muy líquidos.
  Fix: slippage dinámico por activo en _market_price():
    - BTC, ETH          → 0.10 %  (spread ultra-tight)
    - HYPE, SOL, BNB,
      XRP, DOGE, ADA,
      AVAX, DOT, LTC,
      LINK               → 0.20 %  (majors con alta liquidez)
    - resto              → 0.30 %  (altcoins con spread mayor)
  Se elimina la constante global _MARKET_SLIPPAGE y se introduce
  _SLIPPAGE_BY_COIN (dict) + _DEFAULT_SLIPPAGE.
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
    dec = _get_tick_decimals(coin)
    quantizer = Decimal(10) ** -dec
    rounded = Decimal(str(price)).quantize(quantizer, rounding=ROUND_HALF_UP)
    return float(rounded)


# ── Slippage dinámico por activo (v14) ──────────────────────────────────────────────
# Hyperliquid rechaza limit_px que supere el mark price en más de ~0.3 % en activos
# muy líquidos. Usamos un slippage mínimo en majors para evitar el rechazo y un
# slippage ligeramente mayor en altcoins donde el spread es más amplio.

_SLIPPAGE_BY_COIN: dict[str, float] = {
    # Ultra-tight spread — 0.10 %
    "BTC":  0.001,
    "ETH":  0.001,
    # Majors con alta liquidez — 0.20 %
    "HYPE": 0.002,
    "SOL":  0.002,
    "BNB":  0.002,
    "XRP":  0.002,
    "DOGE": 0.002,
    "ADA":  0.002,
    "AVAX": 0.002,
    "DOT":  0.002,
    "LTC":  0.002,
    "LINK": 0.002,
    "TRX":  0.002,
    "SUI":  0.002,
    "TRUMP":0.002,
}
_DEFAULT_SLIPPAGE = 0.003  # 0.30 % para el resto de altcoins


def _get_slippage(coin: str) -> float:
    return _SLIPPAGE_BY_COIN.get(coin, _DEFAULT_SLIPPAGE)


# ── Precio límite para órdenes de mercado ─────────────────────────────────────────

def _market_price(coin: str, is_buy: bool) -> float:
    mids = _hl_call(_info.all_mids, context=f"_market_price({coin})")
    mid  = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    slippage = _get_slippage(coin)
    raw = mid * (1 + slippage) if is_buy else mid * (1 - slippage)
    log.debug("_market_price(%s) mid=%.6f slippage=%.3f%% limit_px=%.6f", coin, mid, slippage * 100, raw)
    return _round_price(coin, raw)


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

    asset_positions = state.get("assetPositions", [])
    open_coins = [
        (e.get("position", {}).get("coin", "?"), float(e.get("position", {}).get("szi", 0)))
        for e in asset_positions
        if float(e.get("position", {}).get("szi", 0)) != 0
    ]
    if open_coins:
        log.warning(
            "[exchange] RAW posiciones abiertas en HL: %s | hl_to_bot keys (sample): %s",
            open_coins,
            list(hl_to_bot.keys())[:15],
        )

    result: dict[str, dict] = {}
    for entry in asset_positions:
        pos     = entry.get("position", {})
        coin    = pos.get("coin", "")
        szi     = float(pos.get("szi", 0))
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
    return _exchange.order(coin, is_buy, qty, price, order_type, True)


# ── SL + TP juntos con normalTpsl ───────────────────────────────────────

def _place_sltp_pair(
    symbol: str,
    side: str,
    qty: float,
    sl_price: float,
    tp_price: float,
) -> None:
    coin     = _hl_symbol(symbol)
    is_close = side == "short"

    sl_px = _round_price(coin, sl_price)
    tp_px = _round_price(coin, tp_price)

    sl_order = {
        "coin":        coin,
        "is_buy":      is_close,
        "sz":          qty,
        "limit_px":    sl_px,
        "order_type":  {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}},
        "reduce_only": True,
    }
    tp_order = {
        "coin":        coin,
        "is_buy":      is_close,
        "sz":          qty,
        "limit_px":    tp_px,
        "order_type":  {"trigger": {"triggerPx": tp_px, "isMarket": True, "tpsl": "tp"}},
        "reduce_only": True,
    }

    try:
        resp = _hl_call(
            _exchange.bulk_orders,
            [sl_order, tp_order],
            grouping="normalTpsl",
            context=f"_place_sltp_pair({coin} sl={sl_px} tp={tp_px})",
        )
        statuses = (
            ((resp.get("response") or {})
             .get("data") or {})
            .get("statuses") or []
        )
        errors = [s.get("error") for s in statuses if "error" in s]
        if errors:
            raise RuntimeError(f"bulk_orders normalTpsl errors: {errors}")
        log.info(
            "SL+TP colocados juntos (normalTpsl): %s | sl=%.4f tp=%.4f (%s)",
            coin, sl_px, tp_px, side.upper(),
        )
    except Exception as exc:
        log.warning(
            "_place_sltp_pair(%s) falló: %s — intentando órdenes separadas",
            coin, exc,
        )
        _place_single_sl(symbol, side, qty, sl_price)
        _place_single_tp(symbol, side, qty, tp_price)


def _place_single_sl(symbol: str, side: str, qty: float, stop_price: float) -> None:
    coin       = _hl_symbol(symbol)
    is_buy     = side == "short"
    stop_price = _round_price(coin, stop_price)
    order_type = {"trigger": {"triggerPx": stop_price, "isMarket": True, "tpsl": "sl"}}
    try:
        resp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, stop_price, order_type,
            context=f"_place_single_sl({coin},{stop_price})",
        )
        _check_order_response(resp, f"_place_single_sl({coin},{stop_price})")
        log.info("SL colocado en %s (%s %s)", stop_price, side.upper(), coin)
    except Exception as exc:
        log.warning("_place_single_sl(%s) falló: %s", coin, exc)


def _place_single_tp(symbol: str, side: str, qty: float, tp_price: float) -> None:
    coin       = _hl_symbol(symbol)
    is_buy     = side == "short"
    tp_price   = _round_price(coin, tp_price)
    order_type = {"trigger": {"triggerPx": tp_price, "isMarket": True, "tpsl": "tp"}}
    try:
        resp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, tp_price, order_type,
            context=f"_place_single_tp({coin},{tp_price})",
        )
        _check_order_response(resp, f"_place_single_tp({coin},{tp_price})")
        log.info("TP colocado en %s (%s %s)", tp_price, side.upper(), coin)
    except Exception as exc:
        log.warning("_place_single_tp(%s) falló: %s", coin, exc)


# ── Abrir orden ──────────────────────────────────────────────────────────────────

def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    """Abre una posición IOC y coloca SL+TP juntos con normalTpsl.

    v13 FIX: se usa limit_px (de _market_price) también para el check de
    notional mínimo, eliminando la llamada separada a get_price().

    v14 FIX: slippage dinámico por activo (_get_slippage) para que limit_px
    nunca exceda el umbral de desviación que aplica Hyperliquid sobre el
    mark price en activos muy líquidos (BTC/ETH usan 0.1%, resto hasta 0.3%).
    """
    sym_bot = symbol or config.SYMBOLS[0]
    coin    = _hl_symbol(sym_bot)
    is_buy  = side == "long"

    qty = floor_qty(qty, sym_bot)

    limit_px = _market_price(coin, is_buy)

    if qty <= 0 or not min_notional_ok(qty, limit_px):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot} (notional={qty*limit_px:.2f} USDT). "
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

def cancel_all_orders(symbol: str = None) -> None:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        orders = _hl_call(_info.open_orders, _WALLET_ADDRESS, context=f"open_orders({coin})")
        oids   = [o["oid"] for o in orders if o["coin"] == coin]
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
