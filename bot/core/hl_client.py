"""
hl_client.py — Cliente Hyperliquid basado en el SDK oficial.

FIX CRÍTICO (2026-06-01):
  Causa raíz del error "Invalid TPSL price":
  1. round_px usaba math.floor → para SL en LONG con precios de pocos decimales,
     el floor podía dejar el precio igual al entry o incluso superior, y HL lo rechaza.
  2. pxDecimals se infería incorrectamente por rango de precio. Hyperliquid
     determina los decimales válidos a partir del markPx real del asset (string
     con precisión exacta). Si el markPx es "1.9061", pxDecimals=4.
  3. place_sl / place_tp no validaban que SL < entry (LONG) / SL > entry (SHORT)
     antes de enviar.

  Solución:
  - round_px ahora usa round() (redondeo estándar) en lugar de math.floor.
  - _warm_cache() lee markPx string de meta_and_asset_ctxs() para derivar
    los decimales exactos que HL acepta internamente.
  - place_sl / place_tp reciben opcionalmente entry_px y validan lógica antes
    de enviar (ajustando 1 tick si es necesario).

Autenticación soportada:
  Opción A (recomendada): API Wallet
    HL_API_PRIVATE_KEY     — private key del agente aprobado en app.hyperliquid.xyz
    HL_API_WALLET_ADDRESS  — dirección del wallet PRINCIPAL (el que tiene fondos)

  Opción B: Private key directa
    HL_PRIVATE_KEY         — private key del wallet principal
    HL_ACCOUNT_ADDR        — dirección pública (opcional, se deriva automáticamente)

Opcionales:
  HL_TESTNET             — "true" para testnet
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Optional

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

logger = logging.getLogger("HLClient")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_BASE_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

_MARKET_SLIPPAGE = 0.03
_TP_LIMIT_BUFFER = 0.001

POST_FILL_CONFIRM_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "6"))
POST_FILL_CONFIRM_DELAY   = float(os.getenv("POST_FILL_CONFIRM_DELAY", "3.0"))


def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    for suffix in ("USDTUSDT", "USDT"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _px_decimals_from_tick(tick: float) -> int:
    """
    Deriva pxDecimals a partir del tick size real.
    tick=0.0001 → 4, tick=0.001 → 3, tick=0.01 → 2, etc.
    Clamp entre 0 y 6.
    """
    if tick <= 0:
        return 4
    dec = max(0, min(6, round(-math.log10(tick))))
    return dec


# ─────────────────────────────────────────────────────────────────
# _HLCore: singleton que contiene el Exchange + Info compartidos
# ─────────────────────────────────────────────────────────────────

class _HLCore:
    """
    Singleton que mantiene UNA instancia de Exchange + Info.
    Pre-carga szDecimals, pxDecimals y maxLeverage al arrancar.
    """

    _instance: "_HLCore | None" = None

    def __init__(self) -> None:
        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError("HL_API_WALLET_ADDRESS es obligatoria en modo agente.")
            wallet             = Account.from_key(api_pk)
            self.account_addr  = api_wallet
            self.agent_addr    = wallet.address
            self.agent_mode    = True
            exchange_wallet    = wallet
            exchange_kwargs    = {"account_address": api_wallet}
        else:
            pk = os.getenv("HL_PRIVATE_KEY", "").strip()
            if not pk:
                raise ValueError("Sin clave configurada. Define HL_API_PRIVATE_KEY o HL_PRIVATE_KEY.")
            wallet             = Account.from_key(pk)
            addr               = os.getenv("HL_ACCOUNT_ADDR", "").strip() or wallet.address
            self.account_addr  = addr
            self.agent_addr    = ""
            self.agent_mode    = False
            exchange_wallet    = wallet
            exchange_kwargs    = {}

        self.exchange = self._build_exchange_with_retry(exchange_wallet, exchange_kwargs)
        self.info     = self._build_info_with_retry()

        self._sz_decimals_cache:  dict[str, int]   = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._max_leverage_cache: dict[str, int]   = {}
        self._tick_size_cache:    dict[str, float] = {}

        self._warm_cache()

        logger.info(
            "[HLCore] SDK Exchange+Info inicializados | addr=%s | agente=%s | "
            "coins cacheados: sz=%d px=%d lev=%d",
            self.account_addr[:10] + "...",
            self.agent_mode,
            len(self._sz_decimals_cache),
            len(self._px_decimals_cache),
            len(self._max_leverage_cache),
        )

    def _warm_cache(self) -> None:
        """
        Pre-carga szDecimals, pxDecimals (desde markPx string del contexto perp),
        tick_size y maxLeverage para todos los coins.

        Fuente de pxDecimals (en orden de prioridad):
          1. meta.universe[i]['maxDecimals']  — cuando HL lo expone directamente.
          2. Conteo de decimales del string markPx en meta_and_asset_ctxs().
             El markPx está redondeado al tick exacto que HL usa internamente.
             Ejemplo: markPx="1.9061" → 4 decimales → pxDecimals=4.
          3. Inferencia conservadora por rango de mid price (fallback).
        """
        try:
            meta     = self.info.meta()
            universe = meta.get("universe", [])
        except Exception as exc:
            logger.warning("[HLCore] No se pudo obtener meta para caché: %s", exc)
            return

        mid_prices: dict[str, float] = {}
        try:
            mids = self.info.all_mids()
            mid_prices = {k: float(v) for k, v in mids.items()}
        except Exception as exc:
            logger.debug("[HLCore] all_mids no disponible: %s", exc)

        # Contextos perp: markPx como string con precisión exacta de HL
        perp_ctx: dict[str, dict] = {}
        try:
            ctxs = self.info.meta_and_asset_ctxs()
            if isinstance(ctxs, (list, tuple)) and len(ctxs) == 2:
                ctx_list = ctxs[1]
                for i, asset in enumerate(universe):
                    coin = asset.get("name", "")
                    if coin and i < len(ctx_list):
                        perp_ctx[coin] = ctx_list[i] or {}
        except Exception as exc:
            logger.debug("[HLCore] meta_and_asset_ctxs no disponible: %s", exc)

        for asset in universe:
            coin = asset.get("name", "")
            if not coin:
                continue

            # szDecimals
            self._sz_decimals_cache[coin] = int(asset.get("szDecimals", 4))

            # maxLeverage
            raw_lev = asset.get("maxLeverage") or asset.get("leverage", {}).get("max") or 20
            self._max_leverage_cache[coin] = int(raw_lev)

            # ── pxDecimals ──────────────────────────────────────────────
            # Prioridad 1: campo explícito en meta
            if "maxDecimals" in asset:
                px_dec = int(asset["maxDecimals"])
                self._px_decimals_cache[coin] = px_dec
                self._tick_size_cache[coin]   = 10 ** (-px_dec)
                continue

            # Prioridad 2: contar decimales del string markPx
            ctx = perp_ctx.get(coin, {})
            mark_px_str = str(ctx.get("markPx") or ctx.get("mark_px") or "")
            if mark_px_str and mark_px_str not in ("None", ""):
                try:
                    float(mark_px_str)  # validar que es número
                    if "." in mark_px_str:
                        dec_part = mark_px_str.rstrip("0").split(".")[1]
                        px_dec   = len(dec_part) if dec_part else 0
                    else:
                        px_dec = 0
                    # Sanity: precios < 100 deben tener al menos 2 decimales
                    mid = mid_prices.get(coin, 0.0)
                    if mid < 100 and px_dec < 2:
                        px_dec = 2
                    if mid < 10 and px_dec < 3:
                        px_dec = 3
                    self._px_decimals_cache[coin] = px_dec
                    self._tick_size_cache[coin]   = 10 ** (-px_dec)
                    continue
                except Exception:
                    pass

            # Prioridad 3: inferencia por rango (fallback conservador)
            mid = mid_prices.get(coin, 0.0)
            if mid >= 10_000:
                px_dec = 1
            elif mid >= 1_000:
                px_dec = 2
            elif mid >= 100:
                px_dec = 3
            elif mid >= 10:
                px_dec = 4
            elif mid >= 1:
                px_dec = 4  # FIX: era 5, HL usa 4 para la mayoría de assets 1-10
            else:
                px_dec = 5

            self._px_decimals_cache[coin] = px_dec
            self._tick_size_cache[coin]   = 10 ** (-px_dec)

        logger.info(
            "[HLCore] Caché pre-cargado: %d coins (szDecimals + pxDecimals + maxLeverage listos)",
            len(universe),
        )

    @classmethod
    def get(cls) -> "_HLCore":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @staticmethod
    def _build_exchange_with_retry(wallet, kwargs: dict, retries: int = 6) -> Exchange:
        delay = 2.0
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return Exchange(wallet=wallet, base_url=_BASE_URL, **kwargs)
            except Exception as exc:
                err = str(exc)
                if "429" in err or "ClientError" in type(exc).__name__:
                    logger.warning(
                        "[HLCore] Exchange init 429 (intento %d/%d) — reintentando en %.1fs",
                        attempt + 1, retries, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    last_exc = exc
                else:
                    raise
        raise RuntimeError(f"[HLCore] No se pudo inicializar Exchange tras {retries} intentos") from last_exc

    @staticmethod
    def _build_info_with_retry(retries: int = 6) -> Info:
        delay = 2.0
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return Info(base_url=_BASE_URL, skip_ws=True)
            except Exception as exc:
                err = str(exc)
                if "429" in err or "ClientError" in type(exc).__name__:
                    logger.warning(
                        "[HLCore] Info init 429 (intento %d/%d) — reintentando en %.1fs",
                        attempt + 1, retries, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    last_exc = exc
                else:
                    raise
        raise RuntimeError(f"[HLCore] No se pudo inicializar Info tras {retries} intentos") from last_exc


# ─────────────────────────────────────────────────────────────────
# HLClient: un cliente ligero por symbol, comparte _HLCore
# ─────────────────────────────────────────────────────────────────

class HLClient:
    """
    Cliente ligero por symbol. Comparte Exchange + Info via _HLCore singleton.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.coin   = _norm_coin(symbol)
        core = _HLCore.get()
        self._exchange     = core.exchange
        self._info         = core.info
        self._account_addr = core.account_addr
        self._agent_addr   = core.agent_addr
        self._agent_mode   = core.agent_mode
        self._core         = core

    # ── METADATOS ─────────────────────────────────────────────────

    def _get_meta_asset(self) -> dict:
        try:
            meta = self._info.meta()
            for asset in meta.get("universe", []):
                if asset.get("name") == self.coin:
                    return asset
        except Exception as exc:
            logger.warning("[%s] No se pudo obtener meta: %s", self.coin, exc)
        return {}

    def get_sz_decimals(self) -> int:
        cache = self._core._sz_decimals_cache
        if self.coin in cache:
            return cache[self.coin]
        asset = self._get_meta_asset()
        dec = int(asset.get("szDecimals", 4))
        cache[self.coin] = dec
        return dec

    def get_px_decimals(self) -> int:
        """Devuelve pxDecimals desde caché (pre-cargado al arrancar)."""
        cache = self._core._px_decimals_cache
        if self.coin in cache:
            return cache[self.coin]
        # Fallback individual
        dec = self._infer_px_decimals_from_l2()
        cache[self.coin] = dec
        return dec

    def _infer_px_decimals_from_l2(self) -> int:
        """
        Lee el L2 snapshot y determina pxDecimals a partir del tick size real:
        el menor incremento de precio entre niveles del orderbook.
        Fallback conservador: 4.
        """
        try:
            l2   = self._info.l2_snapshot(self.coin)
            bids = l2.get("levels", [[], []])[0]
            asks = l2.get("levels", [[], []])[1]
            all_px = [float(p["px"]) for p in (bids[:5] + asks[:5]) if "px" in p]
            if len(all_px) >= 2:
                all_px_sorted = sorted(set(all_px))
                diffs = [
                    abs(all_px_sorted[i+1] - all_px_sorted[i])
                    for i in range(len(all_px_sorted) - 1)
                    if all_px_sorted[i+1] - all_px_sorted[i] > 1e-9
                ]
                if diffs:
                    tick = min(diffs)
                    dec  = _px_decimals_from_tick(tick)
                    self._core._tick_size_cache[self.coin] = tick
                    logger.debug("[%s] pxDecimals=%d (tick=%.8f desde L2)", self.coin, dec, tick)
                    return dec
            # Un solo nivel: contar decimales del string
            if all_px:
                px_str = str(bids[0]["px"]) if bids else str(asks[0]["px"])
                if "." in px_str:
                    return max(1, len(px_str.rstrip("0").split(".")[1]))
        except Exception as exc:
            logger.warning("[%s] No se pudo inferir pxDecimals desde L2: %s", self.coin, exc)
        return 4

    def get_tick_size(self) -> float:
        """Devuelve el tick size (mínimo incremento de precio) para este coin."""
        cache = self._core._tick_size_cache
        if self.coin in cache:
            return cache[self.coin]
        dec  = self.get_px_decimals()
        tick = 10 ** (-dec)
        cache[self.coin] = tick
        return tick

    def get_max_leverage(self) -> int:
        cache = self._core._max_leverage_cache
        if self.coin in cache:
            return cache[self.coin]
        asset = self._get_meta_asset()
        lev = int(
            asset.get("maxLeverage")
            or asset.get("leverage", {}).get("max")
            or 20
        )
        cache[self.coin] = lev
        return lev

    def round_px(self, price: float) -> float:
        """
        Redondea un precio al tick size de Hyperliquid.

        FIX: usa round() en lugar de math.floor().
        math.floor causaba que SL redondeado == entry en casos límite,
        provocando 'Invalid TPSL price' en Hyperliquid.
        """
        dec = self.get_px_decimals()
        return round(price, dec)

    def _adjust_sl_px(self, trigger_px: float, entry_px: Optional[float], is_long: bool) -> float:
        """
        Garantiza que el SL sea estrictamente válido:
          - LONG:  SL < entry_px  (se dispara si el precio CAE hasta trigger)
          - SHORT: SL > entry_px  (se dispara si el precio SUBE hasta trigger)
        Si tras el redondeo queda en el lado incorrecto, ajusta 1 tick.
        """
        dec  = self.get_px_decimals()
        tick = self.get_tick_size()
        px   = round(trigger_px, dec)

        if entry_px and entry_px > 0:
            if is_long:
                while px >= entry_px:
                    px = round(px - tick, dec)
            else:
                while px <= entry_px:
                    px = round(px + tick, dec)

        return px

    def _adjust_tp_px(self, trigger_px: float, entry_px: Optional[float], is_long: bool) -> float:
        """
        Garantiza que el TP sea estrictamente válido:
          - LONG:  TP > entry_px
          - SHORT: TP < entry_px
        """
        dec  = self.get_px_decimals()
        tick = self.get_tick_size()
        px   = round(trigger_px, dec)

        if entry_px and entry_px > 0:
            if is_long:
                while px <= entry_px:
                    px = round(px + tick, dec)
            else:
                while px >= entry_px:
                    px = round(px - tick, dec)

        return px

    # ── ÓRDENES BÁSICAS ─────────────────────────────────────────────

    def place_limit(
        self,
        is_buy: bool,
        sz: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> dict:
        price = self.round_px(price)
        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=price,
            order_type={"limit": {"tif": tif}},
            reduce_only=reduce_only,
        )

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        if ref_price is None or ref_price <= 0:
            try:
                l2 = self._info.l2_snapshot(self.coin)
                best_ask = float(l2["levels"][1][0]["px"])
                best_bid = float(l2["levels"][0][0]["px"])
                ref_price = (best_ask + best_bid) / 2
            except Exception:
                ref_price = 0.0

        if ref_price and ref_price > 0:
            if is_buy:
                slippage_px = self.round_px(ref_price * (1 + _MARKET_SLIPPAGE))
            else:
                slippage_px = self.round_px(ref_price * (1 - _MARKET_SLIPPAGE))
        else:
            slippage_px = 999_999_999.0 if is_buy else 0.000001

        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=slippage_px,
            order_type={"limit": {"tif": "Ioc"}},
            reduce_only=reduce_only,
        )

    # ── TRIGGER ORDERS — TP / SL ──────────────────────────────────

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px:  Optional[float] = None,
        entry_px:  Optional[float] = None,
    ) -> dict:
        """
        Coloca un Take Profit trigger order.

        is_buy   — dirección de CIERRE (False=vender para cerrar LONG, True=comprar para cerrar SHORT)
        entry_px — precio de entrada de la posición (para validación lógica)
        """
        is_long    = not is_buy
        trigger_px = self._adjust_tp_px(trigger_px, entry_px, is_long)
        is_market  = limit_px is None

        if is_market:
            effective_limit_px = trigger_px
        else:
            if not is_buy:
                effective_limit_px = self.round_px(trigger_px * (1 - _TP_LIMIT_BUFFER))
            else:
                effective_limit_px = self.round_px(trigger_px * (1 + _TP_LIMIT_BUFFER))

        logger.debug(
            "[%s] place_tp: is_buy=%s trigger=%.6f limit=%.6f entry=%s",
            self.coin, is_buy, trigger_px, effective_limit_px,
            f"{entry_px:.6f}" if entry_px else "N/A",
        )

        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=effective_limit_px,
            order_type={
                "trigger": {
                    "triggerPx": trigger_px,
                    "isMarket":  is_market,
                    "tpsl":      "tp",
                }
            },
            reduce_only=True,
        )

    def place_sl(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        entry_px:   Optional[float] = None,
    ) -> dict:
        """
        Coloca un Stop Loss trigger order.

        is_buy   — dirección de CIERRE (False=vender para cerrar LONG, True=comprar para cerrar SHORT)
        entry_px — precio de entrada de la posición (para validación lógica)
        """
        is_long    = not is_buy
        trigger_px = self._adjust_sl_px(trigger_px, entry_px, is_long)

        logger.debug(
            "[%s] place_sl: is_buy=%s trigger=%.6f entry=%s",
            self.coin, is_buy, trigger_px,
            f"{entry_px:.6f}" if entry_px else "N/A",
        )

        return self._exchange.order(
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=trigger_px,
            order_type={
                "trigger": {
                    "triggerPx": trigger_px,
                    "isMarket":  True,
                    "tpsl":      "sl",
                }
            },
            reduce_only=True,
        )

    def place_bulk(self, orders: list[dict]) -> dict:
        cleaned = []
        for o in orders:
            o  = dict(o)
            ot = o.get("order_type", {})
            if isinstance(ot, dict) and "trigger" in ot:
                trig = dict(ot["trigger"])
                if "triggerPx" in trig:
                    trig["triggerPx"] = self.round_px(float(trig["triggerPx"]))
                ot = dict(ot)
                ot["trigger"] = trig
                o["order_type"] = ot
            if "limit_px" in o and o["limit_px"] is not None:
                o["limit_px"] = self.round_px(float(o["limit_px"]))
            cleaned.append(o)
        return self._exchange.bulk_orders(cleaned)

    # ── CONSULTAS INFO ────────────────────────────────────────────────

    def get_user_state(self) -> dict:
        return self._info.user_state(self._account_addr)

    def get_open_orders(self) -> list:
        return self._info.open_orders(self._account_addr)

    def get_positions(self) -> list:
        state = self.get_user_state()
        return [
            p for p in state.get("assetPositions", [])
            if p.get("position", {}).get("coin") == self.coin
               and float(p.get("position", {}).get("szi", 0)) != 0
        ]

    def get_balance_usdc(self) -> float:
        state = self.get_user_state()
        return float(state.get("crossMarginSummary", {}).get("accountValue", 0.0))

    def cancel_order(self, order_id: int) -> dict:
        return self._exchange.cancel(self.coin, order_id)

    def cancel_all_open_tpsl(self) -> list[dict]:
        orders  = self.get_open_orders()
        results = []
        for o in orders:
            if o.get("coin") != self.coin:
                continue
            ot = o.get("orderType", "")
            is_tpsl = False
            if isinstance(ot, dict):
                trigger  = ot.get("trigger", {})
                tpsl_val = trigger.get("tpsl", "")
                is_tpsl  = tpsl_val in ("tp", "sl")
            elif isinstance(ot, str):
                is_tpsl = any(
                    kw in ot
                    for kw in ("Trigger", "Stop", "Take Profit", "trigger", "stop", "tp", "sl")
                )
            if is_tpsl:
                oid = o.get("oid")
                if oid:
                    r = self.cancel_order(oid)
                    results.append(r)
                    logger.info(
                        "[%s] Trigger order cancelada: oid=%s type=%s",
                        self.coin, oid, ot,
                    )
        return results
