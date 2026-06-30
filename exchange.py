"""exchange.py — Cliente Hyperliquid Perpetual Futures.

Reemplaza la implementación BingX por Hyperliquid usando el SDK oficial.
Hyperliquid es un DEX L1 sin KYC/restricciones geográficas (compatible con MiCA).

Dependencias:
    pip install hyperliquid-python-sdk

Autenticación:
    Requiere HYPERLIQUID_PRIVATE_KEY (clave privada EVM, hex con o sin 0x)
    y opcionalmente HYPERLIQUID_WALLET_ADDRESS (si no se pone, se deriva de la pk).

Nombres de símbolos:
    Hyperliquid usa el token base sin quote ni guión: "BTC", "ETH", "SOL".
    Las funciones públicas aceptan tanto "BTC-USDT" como "BTC" — se normaliza
    internamente con _hl_symbol().

Diferencias respecto a BingX:
    - No hay concepto de positionSide (LONG/SHORT) independiente; cada coin
      tiene una sola posición neta (long positivo, short negativo).
    - El apalancamiento se setea por coin en el momento de abrir la posición
      (isolated margin por defecto).
    - SL y TP se colocan como órdenes separadas de tipo trigger (stop-market).
    - Los precios de órdenes cerradas se obtienen del historial de fills.
"""
import logging
import os
import time
from typing import Optional

import config

log = logging.getLogger("exchange")

# ── Importar SDK ──────────────────────────────────────────────────────────────
try:
    import eth_account
    from hyperliquid.info import Info
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants as hl_constants
except ImportError as _e:
    raise ImportError(
        "SDK de Hyperliquid no instalado. Ejecuta: pip install hyperliquid-python-sdk"
    ) from _e

# ── Inicializar clientes ──────────────────────────────────────────────────────

_PRIVATE_KEY = os.environ["HYPERLIQUID_PRIVATE_KEY"]
if not _PRIVATE_KEY.startswith("0x"):
    _PRIVATE_KEY = "0x" + _PRIVATE_KEY

_account = eth_account.Account.from_key(_PRIVATE_KEY)
_WALLET_ADDRESS = os.environ.get("HYPERLIQUID_WALLET_ADDRESS") or _account.address

# mainnet=True para produccion, False para testnet
_MAINNET = os.environ.get("HL_MAINNET", "true").lower() == "true"
_HL_URL   = hl_constants.MAINNET_API_URL if _MAINNET else hl_constants.TESTNET_API_URL

_info     = Info(_HL_URL, skip_ws=True)
_exchange = Exchange(_account, _HL_URL, account_address=_WALLET_ADDRESS)

log.info("Hyperliquid cliente inicializado | wallet=%s | mainnet=%s",
         _WALLET_ADDRESS, _MAINNET)


# ── Utilidades de símbolo ────────────────────────────────────────────────────

def _hl_symbol(symbol: str) -> str:
    """Convierte 'BTC-USDT' o 'BTC' a 'BTC' (formato Hyperliquid)."""
    return symbol.split("-")[0].replace("1000SHIB", "1000SHIB")


# ── Meta-info de contratos (sz_decimals, tick_size) ───────────────────────────

_meta_cache: dict = {}

def _get_meta() -> dict:
    global _meta_cache
    if not _meta_cache:
        meta = _info.meta()
        for asset in meta.get("universe", []):
            name = asset["name"]
            _meta_cache[name] = asset
    return _meta_cache


def _asset_info(symbol: str) -> dict:
    sym = _hl_symbol(symbol)
    meta = _get_meta()
    return meta.get(sym, {"szDecimals": 3, "maxLeverage": 50})


def _sz_decimals(symbol: str) -> int:
    return int(_asset_info(symbol).get("szDecimals", 3))


def floor_qty(qty: float, symbol: str) -> float:
    """Redondea qty hacia abajo según szDecimals del contrato."""
    dec = _sz_decimals(symbol)
    factor = 10 ** dec
    return int(qty * factor) / factor


def min_notional_ok(qty: float, price: float, min_usdt: float = 10.0) -> bool:
    return (qty * price) >= min_usdt


# ── Balance ───────────────────────────────────────────────────────────────────

def get_balance() -> float:
    """Devuelve el equity total en USDT (marginSummary.accountValue)."""
    try:
        state = _info.user_state(_WALLET_ADDRESS)
        val = float(state.get("marginSummary", {}).get("accountValue", 0))
        return val
    except Exception as exc:
        log.warning("get_balance fallu00f3: %s", exc)
        return 0.0


# ── Precio ────────────────────────────────────────────────────────────────────

def get_price(symbol: str = None) -> float:
    sym = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        all_mids = _info.all_mids()
        return float(all_mids[sym])
    except Exception as exc:
        log.warning("get_price(%s) fallu00f3: %s", sym, exc)
        # Fallback: L2 orderbook mid
        book = _info.l2_snapshot(sym)
        bid  = float(book["levels"][0][0]["px"])
        ask  = float(book["levels"][1][0]["px"])
        return (bid + ask) / 2


# ── OHLCV ─────────────────────────────────────────────────────────────────────

# Mapeo de timeframes del bot a intervalos Hyperliquid
_TF_MAP = {
    "1m":  "1m",
    "5m":  "5m",
    "15m": "15m",
    "1h":  "1h",
    "4h":  "4h",
    "1d":  "1d",
}


def get_ohlcv(symbol: str = None, interval: str = None, limit: int = 100) -> list[dict]:
    """Devuelve velas OHLCV en el mismo formato que la implementación BingX."""
    sym = _hl_symbol(symbol or config.SYMBOLS[0])
    interval = interval or config.TIMEFRAME
    hl_interval = _TF_MAP.get(interval, interval)

    # Calcular rango temporal para cubrir 'limit' velas
    tf_seconds = {
        "1m": 60, "5m": 300, "15m": 900,
        "1h": 3600, "4h": 14400, "1d": 86400,
    }
    tf_secs  = tf_seconds.get(interval, 900)
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - tf_secs * limit * 1000

    try:
        raw = _info.candles_snapshot(sym, hl_interval, start_ms, end_ms)
    except Exception as exc:
        log.warning("get_ohlcv(%s, %s) fallu00f3: %s", sym, interval, exc)
        return []

    candles = []
    for c in raw:
        open_time    = int(c["t"])
        vol          = float(c.get("v", 0))
        close        = float(c["c"])
        quote_volume = float(c.get("vw", vol * close))  # valor nocional
        candles.append({
            "ts":           open_time,
            "open_time":    open_time,
            "open":         float(c["o"]),
            "high":         float(c["h"]),
            "low":          float(c["l"]),
            "close":        close,
            "volume":       vol,
            "quote_volume": quote_volume,
            "closed":       True,
        })
    # Devolver las últimas 'limit' velas, más reciente al final
    return candles[-limit:]


# ── Posiciones ────────────────────────────────────────────────────────────────

def _parse_hl_position(pos: dict, symbol_raw: str) -> dict | None:
    """Convierte una posición Hyperliquid al formato interno del bot."""
    szi = float(pos.get("szi", 0))
    if szi == 0:
        return None
    side = "long" if szi > 0 else "short"
    return {
        "side":  side,
        "entry": float(pos.get("entryPx") or 0),
        "size":  abs(szi),
        "sl":    float(pos.get("stopLossPx") or 0) or None,
        "tp":    float(pos.get("takeProfitPx") or 0) or None,
    }


def get_all_positions() -> dict[str, dict]:
    """Devuelve {symbol_bot: pos_dict} para todas las posiciones abiertas."""
    state   = _info.user_state(_WALLET_ADDRESS)
    result  = {}
    meta    = _get_meta()
    # Construir mapa inverso: nombre_hl -> symbol_bot
    hl_to_bot: dict[str, str] = {}
    for sym_bot in config.SYMBOLS:
        hl_to_bot[_hl_symbol(sym_bot)] = sym_bot

    for pos_entry in state.get("assetPositions", []):
        pos      = pos_entry.get("position", {})
        coin     = pos.get("coin", "")
        sym_bot  = hl_to_bot.get(coin)
        if sym_bot is None:
            continue
        parsed = _parse_hl_position(pos, sym_bot)
        if parsed:
            result[sym_bot] = parsed
    return result


def get_position(symbol: str = None) -> dict | None:
    symbol = symbol or config.SYMBOLS[0]
    all_pos = get_all_positions()
    return all_pos.get(symbol)


# ── Apalancamiento ────────────────────────────────────────────────────────────

def set_leverage(symbol: str = None, leverage: int = None) -> None:
    sym      = _hl_symbol(symbol or config.SYMBOLS[0])
    leverage = leverage or config.LEVERAGE
    try:
        resp = _exchange.update_leverage(leverage, sym, is_cross=False)
        if resp.get("status") == "ok":
            log.info("Leverage seteado a %dx en %s", leverage, sym)
        else:
            log.warning("set_leverage(%s): respuesta inesperada: %s", sym, resp)
    except Exception as exc:
        log.warning("set_leverage(%s) fallu00f3: %s", sym, exc)


# ── Abrir orden ───────────────────────────────────────────────────────────────

def open_order(side: str, qty: float, sl: float, tp: float, symbol: str = None) -> dict:
    sym_bot  = symbol or config.SYMBOLS[0]
    sym_hl   = _hl_symbol(sym_bot)
    is_buy   = side == "long"

    qty = floor_qty(qty, sym_bot)
    if qty <= 0 or not min_notional_ok(qty, get_price(sym_bot)):
        raise ValueError(
            f"qty={qty} inválido para {sym_bot}. "
            "Aumenta MARGIN_USDT o reduce el número de pares."
        )

    # Setear leverage antes de abrir
    set_leverage(sym_bot, config.LEVERAGE)

    # Orden de mercado
    resp = _exchange.market_open(sym_hl, is_buy, qty)
    if resp.get("status") != "ok":
        raise RuntimeError(f"open_order {sym_hl} fallida: {resp}")
    log.info("Orden abierta: %s %s qty=%.4f", side.upper(), sym_hl, qty)

    # Colocar SL y TP
    place_stop_order(sym_bot, side, qty, sl)
    place_tp_order(sym_bot, side, qty, tp)

    return resp


def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> None:
    """Coloca una stop-loss order (trigger reduce-only)."""
    sym_hl  = _hl_symbol(symbol)
    is_buy  = side == "short"  # para cerrar un long hay que vender, y viceversa
    trigger = {
        "triggerPx": str(round(stop_price, 6)),
        "isMarket":  True,
        "tpsl":      "sl",
    }
    try:
        resp = _exchange.order(
            sym_hl, is_buy, qty,
            None,           # price=None para market
            {"trigger": trigger},
            reduce_only=True,
        )
        if resp.get("status") == "ok":
            log.info("SL colocado en %.6f (%s %s)", stop_price, side.upper(), sym_hl)
        else:
            log.warning("place_stop_order(%s): %s", sym_hl, resp)
    except Exception as exc:
        log.warning("place_stop_order(%s) fallu00f3: %s", sym_hl, exc)


def place_tp_order(symbol: str, side: str, qty: float, tp_price: float) -> None:
    """Coloca una take-profit order (trigger reduce-only)."""
    sym_hl  = _hl_symbol(symbol)
    is_buy  = side == "short"
    trigger = {
        "triggerPx": str(round(tp_price, 6)),
        "isMarket":  True,
        "tpsl":      "tp",
    }
    try:
        resp = _exchange.order(
            sym_hl, is_buy, qty,
            None,
            {"trigger": trigger},
            reduce_only=True,
        )
        if resp.get("status") == "ok":
            log.info("TP colocado en %.6f (%s %s)", tp_price, side.upper(), sym_hl)
        else:
            log.warning("place_tp_order(%s): %s", sym_hl, resp)
    except Exception as exc:
        log.warning("place_tp_order(%s) fallu00f3: %s", sym_hl, exc)


# ── Cerrar posición ───────────────────────────────────────────────────────────

def close_position(side: str, qty: float, symbol: str = None) -> dict:
    sym_hl = _hl_symbol(symbol or config.SYMBOLS[0])
    is_buy = side == "short"  # cerrar short = comprar
    resp   = _exchange.market_close(sym_hl, qty)
    log.info("Posición cerrada: %s %s", side.upper(), sym_hl)
    return resp


# ── Cancelar órdenes abiertas ─────────────────────────────────────────────────

def cancel_all_orders(symbol: str = None) -> None:
    sym_hl = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        open_orders = _info.open_orders(_WALLET_ADDRESS)
        oids_to_cancel = [
            {"coin": o["coin"], "oid": o["oid"]}
            for o in open_orders
            if o["coin"] == sym_hl
        ]
        if oids_to_cancel:
            resp = _exchange.cancel(sym_hl, [o["oid"] for o in oids_to_cancel])
            log.info("Órdenes canceladas para %s (%d)", sym_hl, len(oids_to_cancel))
        else:
            log.debug("cancel_all_orders(%s): no había órdenes abiertas", sym_hl)
    except Exception as exc:
        log.warning("cancel_all_orders(%s) fallu00f3: %s", sym_hl, exc)


# ── Historial de fills (sustituye a get_closed_orders) ────────────────────────

def get_closed_orders(symbol: str = None, limit: int = 20) -> list[dict]:
    """Devuelve fills recientes del símbolo en formato compatible con main.py.

    Hyperliquid no tiene concept de 'closed orders' como BingX — se usan fills.
    Se normaliza el output para que main.py pueda extraer avgPrice, side y type.
    """
    sym_hl = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        fills = _info.user_fills(_WALLET_ADDRESS)
    except Exception as exc:
        log.debug("get_closed_orders(%s) fallu00f3: %s", sym_hl, exc)
        return []

    result = []
    for f in fills:
        if f.get("coin") != sym_hl:
            continue
        # Normalizar al formato que espera main.py
        side_hl  = f.get("side", "")  # "B" buy / "A" ask/sell
        bx_side  = "BUY" if side_hl == "B" else "SELL"
        # Inferir tipo por dir campo 'liquidation' o 'crossed'
        order_type = "STOP_MARKET" if f.get("crossed", False) else "TAKE_PROFIT_MARKET"
        result.append({
            "side":       bx_side,
            "type":       order_type,
            "avgPrice":   str(f.get("px", 0)),
            "time":       int(f.get("time", 0)),
            "updateTime": int(f.get("time", 0)),
            "status":     "FILLED",
        })
        if len(result) >= limit:
            break
    return result
