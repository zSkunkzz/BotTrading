"""
hl_client.py — Cliente Hyperliquid basado en el SDK oficial.

Cada instancia de HLClient (una por symbol/coin) comparte un único objeto
Exchange + Info cargado al inicio (singleton _HLCore). Esto evita el
problema de rate-limiting (429) que ocurría cuando se instanciaban 15+
traders simultáneamente y cada uno hacía sus propias llamadas HTTP a
/info (meta + spot_meta) al construir Exchange e Info.

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


# ─────────────────────────────────────────────────────────────────
# _HLCore: singleton que contiene el Exchange + Info compartidos
# ─────────────────────────────────────────────────────────────────

class _HLCore:
    """
    Singleton que mantiene UNA instancia de Exchange + Info.

    Al inicializarse pre-carga szDecimals, pxDecimals y maxLeverage
    para TODOS los coins del universo Hyperliquid en una sola llamada
    a /info meta.
    """

    _instance: "_HLCore | None" = None

    def __init__(self) -> None:
        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError(
                    "HL_API_WALLET_ADDRESS es obligatoria en modo agente."
                )
            wallet             = Account.from_key(api_pk)
            self.account_addr  = api_wallet
            self.agent_addr    = wallet.address
            self.agent_mode    = True
            exchange_wallet    = wallet
            exchange_kwargs    = {"account_address": api_wallet}
        else:
            pk = os.getenv("HL_PRIVATE_KEY", "").strip()
            if not pk:
                raise ValueError(
                    "Sin clave configurada. Define HL_API_PRIVATE_KEY o HL_PRIVATE_KEY."
                )
            wallet             = Account.from_key(pk)
            addr               = os.getenv("HL_ACCOUNT_ADDR", "").strip() or wallet.address
            self.account_addr  = addr
            self.agent_addr    = ""
            self.agent_mode    = False
            exchange_wallet    = wallet
            exchange_kwargs    = {}

        self.exchange = self._build_exchange_with_retry(exchange_wallet, exchange_kwargs)
        self.info     = self._build_info_with_retry()

        self._sz_decimals_cache:  dict[str, int] = {}
        self._px_decimals_cache:  dict[str, int] = {}
        self._max_leverage_cache: dict[str, int] = {}  # coin -> maxLeverage

        self._warm_decimals_cache()

        logger.info(
            "[HLCore] SDK Exchange+Info inicializados | addr=%s | agente=%s | "
            "coins cacheados: sz=%d px=%d lev=%d",
            self.account_addr[:10] + "...",
            self.agent_mode,
            len(self._sz_decimals_cache),
            len(self._px_decimals_cache),
            len(self._max_leverage_cache),
        )

    def _warm_decimals_cache(self) -> None:
        """
        Pre-carga szDecimals, pxDecimals y maxLeverage para todos los coins
        de una sola llamada a /info meta.

        maxLeverage: campo 'maxLeverage' del asset en meta().universe
          Si no existe, asume 20 (valor conservador).

        pxDecimals: campo 'maxDecimals' si existe, o se infiere por rango
          de precio usando markPx de all_mids.
        """
        try:
            meta = self.info.meta()
            universe = meta.get("universe", [])
        except Exception as exc:
            logger.warning("[HLCore] No se pudo obtener meta para pre-caché: %s", exc)
            return

        mid_prices: dict[str, float] = {}
        try:
            mids = self.info.all_mids()
            mid_prices = {k: float(v) for k, v in mids.items()}
        except Exception as exc:
            logger.debug("[HLCore] all_mids no disponible para warm cache: %s", exc)

        for asset in universe:
            coin = asset.get("name", "")
            if not coin:
                continue

            # szDecimals
            self._sz_decimals_cache[coin] = int(asset.get("szDecimals", 4))

            # maxLeverage — Hyperliquid lo expone en meta.universe como 'maxLeverage'
            raw_lev = asset.get("maxLeverage") or asset.get("leverage", {}).get("max") or 20
            self._max_leverage_cache[coin] = int(raw_lev)

            # pxDecimals
            if "maxDecimals" in asset:
                px_dec = int(asset["maxDecimals"])
            else:
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
                    px_dec = 5
                else:
                    px_dec = 6

            self._px_decimals_cache[coin] = px_dec

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
        logger.debug("[%s] szDecimals=%d (cargado individualmente)", self.coin, dec)
        return dec

    def get_px_decimals(self) -> int:
        cache = self._core._px_decimals_cache
        if self.coin in cache:
            return cache[self.coin]

        asset = self._get_meta_asset()
        if "maxDecimals" in asset:
            dec = int(asset["maxDecimals"])
            cache[self.coin] = dec
            logger.debug("[%s] pxDecimals=%d (meta.maxDecimals)", self.coin, dec)
            return dec

        dec = 5
        try:
            l2  = self._info.l2_snapshot(self.coin)
            ask = float(l2["levels"][1][0]["px"])
            bid = float(l2["levels"][0][0]["px"])
            mid = (ask + bid) / 2
            if mid >= 10_000:
                dec = 1
            elif mid >= 1_000:
                dec = 2
            elif mid >= 100:
                dec = 3
            elif mid >= 10:
                dec = 4
            elif mid >= 1:
                dec = 5
            else:
                dec = 6
        except Exception as exc:
            logger.warning("[%s] No se pudo inferir pxDecimals: %s — usando %d", self.coin, exc, dec)

        cache[self.coin] = dec
        logger.debug("[%s] pxDecimals=%d (inferido individualmente)", self.coin, dec)
        return dec

    def get_max_leverage(self) -> int:
        """
        Devuelve el leverage máximo permitido por Hyperliquid para este coin.
        Pre-cargado al arrancar; fallback conservador de 20 si no está en caché.
        """
        cache = self._core._max_leverage_cache
        if self.coin in cache:
            return cache[self.coin]
        # Fallback: consulta individual
        asset = self._get_meta_asset()
        lev = int(
            asset.get("maxLeverage")
            or asset.get("leverage", {}).get("max")
            or 20
        )
        cache[self.coin] = lev
        logger.debug("[%s] maxLeverage=%d (cargado individualmente)", self.coin, lev)
        return lev

    def round_px(self, price: float) -> float:
        dec = self.get_px_decimals()
        factor = 10 ** dec
        return math.floor(price * factor) / factor

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
        limit_px: Optional[float] = None,
    ) -> dict:
        trigger_px = self.round_px(trigger_px)
        is_market  = limit_px is None

        if is_market:
            effective_limit_px = trigger_px
        else:
            if not is_buy:
                effective_limit_px = self.round_px(trigger_px * (1 - _TP_LIMIT_BUFFER))
            else:
                effective_limit_px = self.round_px(trigger_px * (1 + _TP_LIMIT_BUFFER))

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
    ) -> dict:
        trigger_px = self.round_px(trigger_px)
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
