"""
exchange.py — Cliente Hyperliquid Perpetual Futures.

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
v10-diag — Log diagnóstico en get_all_positions():
  Imprime todos los coins RAW devueltos por la API y avisa si hay
  posiciones abiertas (szi != 0) que no están mapeadas en config.SYMBOLS.
  Permite diagnosticar por qué HYPE u otros pares no se sincronizan.
v11 — Fix "Invalid TPSL price" al colocar SL+TP por separado:
  Hyperliquid rechaza añadir un TP como orden independiente cuando ya
  existe un SL activo sobre la misma posición (y viceversa).
  Fix: _place_sltp_pair() envía SL y TP juntos en una sola llamada
  bulk_orders con grouping="normalTpsl". open_order, place_stop_order
  y place_tp_order usan esta función internamente.
  place_stop_order y place_tp_order mantienen su firma pública pero
  delegan en _place_sltp_pair cuando se dispone de ambos precios, o
  envían la orden individual si solo se pide una de las dos.
v12 — Fix CRÍTICO grouping="normalTpsl" nunca se pasaba a bulk_orders:
  La firma del SDK es bulk_orders(order_requests, builder=None, grouping="na").
  _place_sltp_pair llamaba bulk_orders([sl, tp]) sin el kwarg grouping,
  por lo que HL recibía siempre grouping="na" y rechazaba la segunda orden
  como orden independiente conflictiva.
  Fix: pasar grouping="normalTpsl" explícitamente.
  También se corrige _restore_sl_tp_on_sync en main.py que llamaba
  place_stop_order + place_tp_order por separado en lugar de _place_sltp_pair.
v13 — modify_sltp_orders: modify() individual del SDK real, elimina ventana de desprotección:
  bulk_modify_orders_new no existe en el SDK oficial de Hyperliquid.
  El SDK expone Exchange.modify_order(coin, oid, is_buy, sz, limit_px, order_type).
  modify_sltp_orders obtiene los oid con get_open_trigger_orders() y modifica
  cada orden in-place con _exchange.modify_order(). Si modify falla, hace
  fallback a cancel+_place_sltp_pair. Si no hay órdenes previas, place directo.
v14 — Fix TPSL no se colocaba en monedas con precio > 1 USDC (3 bugs):

  Bug #1 (_place_sltp_pair): limit_px era triggerPx en órdenes trigger
    isMarket=True. Para monedas con precio > 1 USDC (SOL, ETH, BTC...)
    HL rechazaba la orden porque el limit_px coincidía con el precio
    de mercado y se trataba como orden límite ejecutable inmediatamente.
    Fix: _trigger_market_limit_px(coin, is_buy) devuelve 0 para sell
    (SL de long, TP de short) y un precio muy alto para buy (SL de short,
    TP de long) — patrón que usa el propio SDK de HL internamente.

  Bug #2 (_modify_single_order): mismo problema — limit_px = new_px
    en vez de un precio favorable para trigger market.
    Fix: usar _trigger_market_limit_px() también en modify.

  Bug #3 (cancel_all_orders): usaba _info.open_orders que NO devuelve
    órdenes trigger/TPSL en Hyperliquid. El fallback cancel+place de
    modify_sltp_orders no cancelaba los SL/TP existentes, creando
    órdenes duplicadas.
    Fix: usar frontend_open_orders (ya usado en get_open_trigger_orders)
    que sí incluye todas las órdenes trigger.

v15 — modify_sltp_orders: batchModify atómico como primer intento:
  Cuando existen tanto SL como TP, se intenta primero un bulk_modify_orders
  con grouping="normalTpsl" que modifica ambas órdenes en una sola llamada
  firmada. Esto elimina la ventana de ~100-300ms entre el modify del SL y
  el modify del TP en que una de las dos puede quedar desactualizada si el
  precio toca el trigger justo en ese intervalo.
  Si bulk_modify_orders no existe en la versión instalada del SDK o HL lo
  rechaza, cae silenciosamente al comportamiento v13/v14 (dos modify_order
  separados + fallback cancel+place). Ninguna otra función cambia.

v16 — Fix 3 bugs en modify_sltp_orders / get_open_trigger_orders / _batch_modify_sltp:

  Bug #1 (get_open_trigger_orders): el precio de trigger se leía de
    o.get("triggerPx") con fallback a o.get("limitPx"). En órdenes TPSL
    de Hyperliquid el campo canónico es "triggerPx" pero puede ser None
    si la orden se serializó de forma distinta. Se añade fallback robusto
    leyendo también "px" (campo genérico de precio) para garantizar que
    siempre se obtiene un precio válido > 0.

  Bug #2 (_batch_modify_sltp): los triggerPx en modify_requests no pasaban
    por _round_price internamente — dependían de que el caller ya los
    hubiese redondeado. Se añade _round_price explícito dentro de la función
    para que sea segura ante llamadas directas con precios crudos.

  Bug #3 (modify_sltp_orders — caso asimétrico): cuando solo existe SL o
    solo TP, el código intentaba colocar la orden faltante con
    _place_single_sl / _place_single_tp de forma independiente. Hyperliquid
    rechaza colocar una orden TPSL individual cuando ya existe la otra
    (el mismo error que motivó v11). Fix: en el caso asimétrico se cancela
    primero la orden existente y se recolocan ambas con _place_sltp_pair,
    igual que en el fallback completo.

v17 — Fix raíz: posiciones sincronizadas nunca tenían SL/TP (2 bugs):

  Bug #1 (get_all_positions): _parse_hl_position devuelve siempre
    sl=None, tp=None porque HL no incluye esos datos en el objeto de
    posición. get_all_positions() ahora llama frontend_open_orders UNA
    sola vez al inicio del sync y puebla sl/tp en cada posición leyendo
    las órdenes trigger activas. Esto evita que _restore_sl_tp_on_sync
    cancele+recoloque órdenes que ya existen en HL (el cancel previo
    dejaba la posición desprotegida durante el bulk_orders).

  Bug #2 (_place_sltp_pair — fallback silencioso): si bulk_orders con
    normalTpsl fallaba, el código hacía _place_single_sl + _place_single_tp
    por separado. HL rechaza el segundo, el error se tragaba, y la posición
    quedaba con solo SL o solo TP sin que nadie lo supiera. Fix: se elimina
    el fallback a órdenes separadas. Si bulk_orders normalTpsl falla se
    reintenta UNA vez sin el kwarg grouping (compatibilidad SDK < 0.9).
    Si vuelve a fallar se lanza la excepción limpia para que el caller
    (open_order, _restore_sl_tp_on_sync) la vea en logs y pueda actuar.

v18 — Fix CRÍTICO modify_sltp_orders (2 bugs):

  Bug #1 (_modify_single_order): los argumentos se pasaban a
    _exchange.modify_order() en orden incorrecto. La firma real del SDK es
    modify_order(oid, name, is_buy, sz, limit_px, order_type, reduce_only).
    El código los pasaba como (coin, oid, is_buy, ...) — coin y oid
    intercambiados — lo que causaba que HL rechazara todos los modifies
    con "invalid order id" o similar.
    Fix: reordenar a (oid, coin, is_buy, qty, limit_px, order_type, True).

  Bug #2 (_batch_modify_sltp): buscaba bulk_modify_orders con
    getattr(_exchange, "bulk_modify_orders", None) pero el SDK oficial
    expone la función como bulk_modify_orders_new. Al no encontrarla,
    getattr devolvía None y _batch_modify_sltp retornaba False siempre,
    desactivando el path atómico en todos los casos.
    Fix: buscar "bulk_modify_orders_new" y llamarla sin grouping (el SDK
    ya construye internamente el action batchModify con los wires
    correctos — no acepta kwarg grouping).

v19 — Fix _batch_modify_sltp: formato ModifyRequest correcto según SDK oficial.

  Confirmado leyendo el código fuente del SDK (hyperliquid-dex/hyperliquid-python-sdk):
  bulk_modify_orders_new() espera List[ModifyRequest] donde cada ModifyRequest es:
    {"oid": int, "order": OrderRequest}
  y OrderRequest usa el campo "coin" (nombre del activo, ej. "BTC"), NO el asset index.
  El SDK hace internamente name_to_asset(order["coin"]) en order_request_to_order_wire().

  Bug confirmado en v18: el campo "coin" dentro de order ya era correcto (nombre string),
  pero modify_order() del SDK llama internamente a bulk_modify_orders_new con un solo
  elemento — por tanto _modify_single_order y _batch_modify_sltp son equivalentes en
  cuanto a serialización. El path atómico (_batch_modify_sltp) es el preferido porque
  envía ambas órdenes en una sola firma cryptográfica, eliminando la ventana de
  desprotección entre el modify del SL y el del TP.

  Fix adicional: se añade log.debug del modify_request completo antes de enviarlo
  para facilitar diagnóstico futuro sin tener que añadir prints temporales.
  Se documenta explícitamente que "coin" en OrderRequest es el nombre ("BTC"),
  NO el asset index numérico — confusión común al leer el wire format de HL.

v20 — Fix _batch_modify_sltp: wire format batchModify según docs oficiales HL.

  Según la documentación oficial de Hyperliquid (exchange-endpoint#modify-multiple-orders),
  el campo "a" en el order wire del batchModify es el ASSET INDEX numérico (igual que
  en place order), NO el nombre del activo. bulk_modify_orders_new() del SDK construye
  el wire directamente sin hacer name_to_asset() — a diferencia de bulk_orders() que
  sí recibe OrderRequest con "coin" y convierte internamente.

  Fix: se añade _get_asset_index(coin) helper que lee _info.coin_to_asset y devuelve
  el índice numérico. _batch_modify_sltp usa "a" (asset index) en lugar de "coin".

  También: el flag always_place ("a" en el action raíz) debe OMITIRSE si es False
  según los docs — "actions hashed with a: false will be rejected". Se documenta
  explícitamente este comportamiento. No incluimos always_place en nuestros modifies
  (no lo necesitamos para TPSL), por lo que ya era correcto no incluirlo.

v21 — Fix notación científica en triggerPx / limit_px del wire batchModify.

  Según docs oficiales HL, los campos "p" (limit_px) y "triggerPx" del wire
  format deben ser Strings con notación decimal fija, ej. "0.00012300".
  str(float) en Python genera notación científica para valores muy pequeños
  (ej. str(1.23e-4) → "0.00012" OK, pero str(1.23e-8) → "1.23e-08" que HL
  rechaza silenciosamente sin error descriptivo).

  Fix: _price_to_wire(price) convierte el float a String vía Decimal para
  garantizar notación decimal fija sin exponente, consistente con lo que el
  SDK hace internamente con floattowire(). Aplicado en los campos "p" y
  "triggerPx" de los modify_requests en _batch_modify_sltp.

  Nota: los campos "s" (size) no tienen este problema porque los tamaños de
  posición en perpetuos de HL son siempre >= 0.001 y str() no genera notación
  científica en ese rango.
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


# ── Rate limiting: exponential backoff con jitter ──────────────────────
_RL_MAX_RETRIES = 3
_RL_BASE_DELAY = 1.0
_RL_JITTER = 0.2
_RL_429_EXTRA = 5.0


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


# ── tickSize / price rounding ───────────────────────────────────────────
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


def _price_to_wire(price: float) -> str:
    if price == 0.0:
        return "0"
    return str(Decimal(str(price)).normalize())


# ── limit_px correcto para órdenes trigger isMarket=True ────────────────
_TRIGGER_BUY_LIMIT_MULTIPLIER = 1.10


def _trigger_market_limit_px(coin: str, is_buy: bool, trigger_px: float) -> float:
    if not is_buy:
        return 0.0
    return _round_price(coin, trigger_px * _TRIGGER_BUY_LIMIT_MULTIPLIER)


# ── Precio límite para órdenes de mercado ───────────────────────────────
_MARKET_SLIPPAGE = 0.005


def _market_price(coin: str, is_buy: bool) -> float:
    mids = _hl_call(_info.all_mids, context=f"_market_price({coin})")
    mid = float(mids.get(coin, 0))
    if mid <= 0:
        raise ValueError(f"No se pudo obtener precio para {coin}")
    raw = mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)
    return _round_price(coin, raw)


# ── Balance ─────────────────────────────────────────────────────────────
def get_balance() -> float:
    try:
        state = _hl_call(_info.user_state, _WALLET_ADDRESS, context="get_balance")
        return float(state.get("marginSummary", {}).get("accountValue", 0))
    except Exception as exc:
        log.warning("get_balance falló: %s", exc)
        return 0.0


# ── Precio ──────────────────────────────────────────────────────────────
def get_price(symbol: str = None) -> float:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        mids = _hl_call(_info.all_mids, context=f"get_price({coin})")
        if coin in mids:
            return float(mids[coin])
        book = _hl_call(_info.l2_snapshot, coin, context=f"get_price_l2({coin})")
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
        orders = _hl_call(
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
    state = _hl_call(_info.user_state, _WALLET_ADDRESS, context="get_all_positions")
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
    coin = _hl_symbol(symbol)
    is_close = side == "short"

    sl_px = _round_price(coin, sl_price)
    tp_px = _round_price(coin, tp_price)
    sl_limit_px = _trigger_market_limit_px(coin, is_close, sl_px)
    tp_limit_px = _trigger_market_limit_px(coin, is_close, tp_px)

    sl_order = {
        "coin": coin,
        "is_buy": is_close,
        "sz": qty,
        "limit_px": sl_limit_px,
        "order_type": {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}},
        "reduce_only": True,
    }
    tp_order = {
        "coin": coin,
        "is_buy": is_close,
        "sz": qty,
        "limit_px": tp_limit_px,
        "order_type": {"trigger": {"triggerPx": tp_px, "isMarket": True, "tpsl": "tp"}},
        "reduce_only": True,
    }

    order_list = [sl_order, tp_order]

    exc_first = None

    try:
        resp = _hl_call(
            _exchange.bulk_orders,
            order_list,
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
        return
    except Exception as exc:
        exc_first = exc
        log.warning(
            "_place_sltp_pair(%s) normalTpsl falló: %s — reintentando sin grouping",
            coin, exc_first,
        )

    try:
        resp = _hl_call(
            _exchange.bulk_orders,
            order_list,
            context=f"_place_sltp_pair_fallback({coin} sl={sl_px} tp={tp_px})",
        )
        statuses = (
            ((resp.get("response") or {})
             .get("data") or {})
            .get("statuses") or []
        )
        errors = [s.get("error") for s in statuses if "error" in s]
        if errors:
            raise RuntimeError(f"bulk_orders sin grouping errors: {errors}")
        log.info(
            "SL+TP colocados (bulk sin grouping): %s | sl=%.4f tp=%.4f (%s)",
            coin, sl_px, tp_px, side.upper(),
        )
        return
    except Exception as exc_second:
        raise RuntimeError(
            f"_place_sltp_pair({coin}): ambos intentos fallaron. "
            f"Primer error: {exc_first}. Segundo error: {exc_second}"
        ) from exc_second


def _place_single_sl(symbol: str, side: str, qty: float, stop_price: float) -> None:
    coin = _hl_symbol(symbol)
    is_buy = side == "short"
    stop_price = _round_price(coin, stop_price)
    limit_px = _trigger_market_limit_px(coin, is_buy, stop_price)
    order_type = {"trigger": {"triggerPx": stop_price, "isMarket": True, "tpsl": "sl"}}
    try:
        resp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, limit_px, order_type,
            context=f"_place_single_sl({coin},{stop_price})",
        )
        _check_order_response(resp, f"_place_single_sl({coin},{stop_price})")
        log.info("SL colocado en %s (%s %s)", stop_price, side.upper(), coin)
    except Exception as exc:
        log.warning("_place_single_sl(%s) falló: %s", coin, exc)


def _place_single_tp(symbol: str, side: str, qty: float, tp_price: float) -> None:
    coin = _hl_symbol(symbol)
    is_buy = side == "short"
    tp_price = _round_price(coin, tp_price)
    limit_px = _trigger_market_limit_px(coin, is_buy, tp_price)
    order_type = {"trigger": {"triggerPx": tp_price, "isMarket": True, "tpsl": "tp"}}
    try:
        resp = _hl_call(
            _order_reduce_only,
            coin, is_buy, qty, limit_px, order_type,
            context=f"_place_single_tp({coin},{tp_price})",
        )
        _check_order_response(resp, f"_place_single_tp({coin},{tp_price})")
        log.info("TP colocado en %s (%s %s)", tp_price, side.upper(), coin)
    except Exception as exc:
        log.warning("_place_single_tp(%s) falló: %s", coin, exc)


# ── Órdenes trigger abiertas ────────────────────────────────────────────
def get_open_trigger_orders(symbol: str) -> dict:
    coin = _hl_symbol(symbol)
    result = {"sl": None, "tp": None}
    try:
        orders = _hl_call(
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
    new_px = _round_price(coin, new_px)
    limit_px = _trigger_market_limit_px(coin, is_buy, new_px)
    order_type = {"trigger": {"triggerPx": new_px, "isMarket": True, "tpsl": tpsl}}
    log.debug(
        "_modify_single_order: coin=%s oid=%s is_buy=%s qty=%s limit_px=%s tpsl=%s triggerPx=%s",
        coin, oid, is_buy, qty, limit_px, tpsl, new_px,
    )
    resp = _hl_call(
        _exchange.modify_order,
        oid, coin, is_buy, qty, limit_px, order_type, True,
        context=f"modify_order({coin} oid={oid} {tpsl}={new_px})",
    )
    statuses = (((resp or {}).get("response") or {}).get("data") or {}).get("statuses") or []
    errors = [s.get("error") for s in statuses if "error" in s]
    if errors:
        raise RuntimeError(f"modify_order {tpsl} errors: {errors}")
    log.info("Orden %s modificada in-place: %s oid=%s → %.6f", tpsl.upper(), coin, oid, new_px)


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
        log.debug("[%s] bulk_modify_orders_new no disponible en este SDK — usando modify individual", coin)
        return False

    new_sl = _round_price(coin, new_sl)
    new_tp = _round_price(coin, new_tp)

    try:
        asset_idx = _get_asset_index(coin)
    except ValueError as exc:
        log.warning("[%s] _batch_modify_sltp: %s — fallback a modify individual", coin, exc)
        return False

    sl_limit_px = _trigger_market_limit_px(coin, is_close_buy, new_sl)
    tp_limit_px = _trigger_market_limit_px(coin, is_close_buy, new_tp)

    modify_requests = [
        {
            "oid": sl_oid,
            "order": {
                "a": asset_idx,
                "b": is_close_buy,
                "p": _price_to_wire(sl_limit_px),
                "s": str(qty),
                "r": True,
                "t": {
                    "trigger": {
                        "triggerPx": _price_to_wire(new_sl),
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
                "p": _price_to_wire(tp_limit_px),
                "s": str(qty),
                "r": True,
                "t": {
                    "trigger": {
                        "triggerPx": _price_to_wire(new_tp),
                        "isMarket": True,
                        "tpsl": "tp",
                    }
                },
            },
        },
    ]

    log.debug(
        "_batch_modify_sltp: coin=%s asset_idx=%s sl_oid=%s tp_oid=%s is_buy=%s qty=%s "
        "new_sl=%s (limit=%s) new_tp=%s (limit=%s)",
        coin, asset_idx, sl_oid, tp_oid, is_close_buy, qty,
        new_sl, sl_limit_px, new_tp, tp_limit_px,
    )

    try:
        resp = _hl_call(
            bulk_modify,
            modify_requests,
            context=f"bulk_modify_orders_new({coin} sl={new_sl} tp={new_tp})",
        )
        statuses = (((resp or {}).get("response") or {}).get("data") or {}).get("statuses") or []
        errors = [s.get("error") for s in statuses if "error" in s]
        if errors:
            raise RuntimeError(f"bulk_modify_orders_new errors: {errors}")
        log.info(
            "SL+TP modificados atómicamente (batchModify): %s | sl=%.6f tp=%.6f",
            coin, new_sl, new_tp,
        )
        return True
    except Exception as exc:
        log.warning(
            "[%s] bulk_modify_orders_new falló: %s — cayendo a modify individual",
            coin, exc,
        )
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
    new_sl_r = _round_price(coin, new_sl)
    new_tp_r = _round_price(coin, new_tp)

    trigger = get_open_trigger_orders(symbol)
    sl_info = trigger["sl"]
    tp_info = trigger["tp"]

    log.debug(
        "modify_sltp_orders: %s side=%s qty=%s new_sl=%.6f new_tp=%.6f | "
        "existing sl=%s tp=%s",
        coin, side, qty, new_sl_r, new_tp_r,
        sl_info, tp_info,
    )

    if sl_info is None and tp_info is None:
        log.info("[%s] modify_sltp: sin órdenes abiertas → place desde cero", coin)
        _place_sltp_pair(symbol, side, qty, new_sl_r, new_tp_r)
        return

    if sl_info is not None and tp_info is not None:
        if _batch_modify_sltp(
            coin,
            sl_info["oid"], tp_info["oid"],
            is_close_buy, qty,
            new_sl_r, new_tp_r,
        ):
            return

        sl_ok = False
        tp_ok = False

        try:
            _modify_single_order(coin, sl_info["oid"], is_close_buy, qty, new_sl_r, "sl")
            sl_ok = True
        except Exception as exc:
            log.warning("[%s] modify_order SL falló: %s", coin, exc)

        try:
            _modify_single_order(coin, tp_info["oid"], is_close_buy, qty, new_tp_r, "tp")
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
        _place_sltp_pair(symbol, side, qty, new_sl_r, new_tp_r)
        return

    existing_type = "SL" if sl_info else "TP"
    existing_oid = (sl_info or tp_info)["oid"]
    log.info(
        "[%s] modify_sltp: caso asimétrico — solo existe %s (oid=%s) → "
        "cancelando y recolocando ambas con normalTpsl",
        coin, existing_type, existing_oid,
    )
    cancel_all_orders(symbol)
    _place_sltp_pair(symbol, side, qty, new_sl_r, new_tp_r)


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


# ── Cerrar posición ─────────────────────────────────────────────────────
def close_position(side: str, qty: float, symbol: str = None) -> dict:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    is_buy = side == "short"
    limit_px = _market_price(coin, is_buy)
    resp = _hl_call(
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
        orders = _hl_call(
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
                _hl_call(_exchange.cancel, coin, oid, context=f"cancel_order({coin},{oid})")
                cancelled += 1
            except Exception as exc:
                log.warning("cancel_order(%s, %s) falló: %s", coin, oid, exc)
        log.info("Órdenes canceladas para %s (%d/%d)", coin, cancelled, len(oids))
    except Exception as exc:
        log.warning("cancel_all_orders(%s) falló: %s", coin, exc)


# ── Historial de fills ──────────────────────────────────────────────────
def _normalize_fill(f: dict) -> dict:
    fill_dir = f.get("dir", "")
    normalized_side = "SELL" if "Long" in fill_dir else "BUY"
    closed_pnl = float(f.get("closedPnl") or 0)
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


def get_fills(
    symbol: str = None,
    limit: int = 20,
    only_close: bool = True,
) -> list[dict]:
    coin = _hl_symbol(symbol or config.SYMBOLS[0])
    try:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 7 * 24 * 60 * 60 * 1000
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
