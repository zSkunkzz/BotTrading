"""
bot/core/okx_client.py — Cliente OKX Futures Perpetuos (USDT-margined).

Drop-in replacement de HLClient para el exchange OKX.
Expone exactamente la misma interfaz pública que HLClient:

  Construcción:
    client = await OKXClient.create(symbol)   # e.g. "BTC/USDT:USDT"

  Métodos de orden:
    place_market(is_buy, sz, reduce_only, ref_price)
    place_limit(is_buy, sz, price, reduce_only, tif)
    place_tp(is_buy, sz, trigger_px, limit_px, entry_px)
    place_sl(is_buy, sz, trigger_px, entry_px)
    place_bulk(orders)                         # no soportado aún → raises NotImplementedError

  Consultas:
    get_positions() → list[dict]               # mismo schema que HLClient
    get_open_orders() → list
    get_balance_usdc() → float
    cancel_order(order_id) → dict
    cancel_all_open_tpsl() → list[dict]
    get_user_state() → dict
    set_leverage(coin, leverage, is_cross)
    get_sz_decimals() → int
    get_px_decimals() → int
    get_tick_size() → float
    get_max_leverage() → int
    round_px(price) → float
    round_sz(sz) → float
    all_mids() → dict[str, float]              # para get_price en trader.py

Variables de entorno requeridas:
  OKX_API_KEY         — API key
  OKX_API_SECRET      — API secret
  OKX_PASSPHRASE      — passphrase de la API key

Opcionales:
  OKX_TESTNET         — "true" → sandbox (demo.okx.com)
  OKX_INFO_CONCURRENCY — máx. requests simultáneas info (default 4)

Dependencias:
  pip install python-okx
  (ya incluido en requirements si lo añades: okx>=0.1.9)

NOTAS DE MIGRACIÓN en trader.py:
  1. Reemplazar:
       from bot.core.hl_client import HLClient, _HLCore, _norm_coin
     Por:
       from bot.core.okx_client import OKXClient as HLClient, _OKXCore as _HLCore, _norm_coin

  2. En get_price(): cambiar self._hl_client._info.all_mids()
     por            self._hl_client.all_mids()
     (OKXClient.all_mids() ya está normalizado)

  3. El semáforo de concurrencia:
       _HLCore.get_info_semaphore()
     funciona igual — _OKXCore expone get_info_semaphore().

  4. Instrumentos OKX: el símbolo se normaliza internamente.
     "BTC/USDT:USDT" → instId = "BTC-USDT-SWAP"
     "ETH/USDT:USDT" → instId = "ETH-USDT-SWAP"

FIX v2 (2026-06-06):
  - BUG 5: get_max_leverage() usaba get_max_order_size() (no existe en
    python-okx AccountAPI). Reemplazado por get_leverage() según doc v5:
    GET /api/v5/account/leverage-info
  - BUG 6: place_market() y place_limit() pasaban reduceOnly='true'/'false'
    como kwarg de place_order(). La API OKX v5 NO acepta reduceOnly en
    ese endpoint; la dirección se controla con posSide (hedge mode).
    Eliminado el parámetro inválido de ambos métodos.
  - BUG 7: place_limit() pasaba timeInForce= que python-okx TradeAPI
    no acepta. El parámetro correcto según SDK es simplemente no pasarlo
    (GTC es default) o usar clOrdId. Eliminado el kwarg inválido.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from typing import Optional

logger = logging.getLogger("OKXClient")

# ── Env vars ──────────────────────────────────────────────────────────────────
_OKX_API_KEY     = os.getenv("OKX_API_KEY",     "").strip()
_OKX_API_SECRET  = os.getenv("OKX_API_SECRET",  "").strip()
_OKX_PASSPHRASE  = os.getenv("OKX_PASSPHRASE",  "").strip()
_USE_TESTNET     = os.getenv("OKX_TESTNET",     "").lower() in ("true", "1", "yes")
_INFO_CONCURRENCY = int(os.getenv("OKX_INFO_CONCURRENCY", "4"))

_FLAG = "1" if _USE_TESTNET else "0"   # "1" = demo/sandbox en python-okx


# ── Helpers de símbolo ────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    """Normaliza un símbolo a coin base (e.g. 'BTC/USDT:USDT' → 'BTC')."""
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    for suffix in ("USDTUSDT", "USDT", "SWAP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _to_inst_id(symbol: str) -> str:
    """
    Convierte cualquier formato de símbolo al instId de OKX.
    'BTC/USDT:USDT' | 'BTCUSDT' | 'BTC' → 'BTC-USDT-SWAP'
    """
    coin = _norm_coin(symbol)
    return f"{coin}-USDT-SWAP"


# ── _OKXCore: singleton con los clientes SDK ──────────────────────────────────

class _OKXCore:
    """
    Singleton que mantiene instancias de los módulos python-okx:
      - TradeAPI   → place/cancel orders
      - AccountAPI → leverage, balance, positions
      - MarketAPI  → ticker, candles, instruments metadata
      - PublicAPI  → instruments info (tick sizes, sz decimals)

    Patrón idéntico a _HLCore: usar get_async() desde código async.
    """

    _instance:        "_OKXCore | None"         = None
    _init_lock:       "asyncio.Lock | None"      = None
    _info_semaphore:  "asyncio.Semaphore | None" = None

    def __init__(self) -> None:
        if not _OKX_API_KEY or not _OKX_API_SECRET or not _OKX_PASSPHRASE:
            raise ValueError(
                "OKX_API_KEY, OKX_API_SECRET y OKX_PASSPHRASE son obligatorias. "
                "Defínelas en las variables de entorno."
            )

        try:
            import okx.Trade   as _Trade
            import okx.Account as _Account
            import okx.Market  as _Market
            import okx.PublicData as _Public
        except ImportError as e:
            raise ImportError(
                "Módulo 'python-okx' no encontrado. Instálalo con: pip install python-okx"
            ) from e

        self.trade   = _Trade.TradeAPI(
            _OKX_API_KEY, _OKX_API_SECRET, _OKX_PASSPHRASE, False, _FLAG
        )
        self.account = _Account.AccountAPI(
            _OKX_API_KEY, _OKX_API_SECRET, _OKX_PASSPHRASE, False, _FLAG
        )
        self.market  = _Market.MarketAPI(
            _OKX_API_KEY, _OKX_API_SECRET, _OKX_PASSPHRASE, False, _FLAG
        )
        self.public  = _Public.PublicAPI(
            _OKX_API_KEY, _OKX_API_SECRET, _OKX_PASSPHRASE, False, _FLAG
        )

        # Caché de metadatos por coin (inst_id → valor)
        self._sz_decimals_cache:  dict[str, int]   = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._tick_size_cache:    dict[str, float] = {}
        self._ct_val_cache:       dict[str, float] = {}
        self._max_leverage_cache: dict[str, int]   = {}

        self._warm_cache()

    # ── Caché de instrumentos ─────────────────────────────────────

    def _warm_cache(self) -> None:
        """Pre-carga metadatos de todos los instrumentos SWAP (USDT-margined)."""
        try:
            resp = self.public.get_instruments(instType="SWAP", uly="", instFamily="")
            instruments = (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[OKXCore] get_instruments falló: %s", exc)
            return

        for inst in instruments:
            inst_id = inst.get("instId", "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue

            # Tamaño de tick de precio
            try:
                tick_sz = float(inst.get("tickSz", 0.01))
            except (ValueError, TypeError):
                tick_sz = 0.01
            px_dec = max(0, min(8, round(-math.log10(tick_sz)))) if tick_sz > 0 else 2

            # Tamaño mínimo de contrato (ctVal = valor en base coin por contrato)
            try:
                ct_val = float(inst.get("ctVal", 1.0))
            except (ValueError, TypeError):
                ct_val = 1.0

            # Decimales de cantidad
            try:
                lot_sz = float(inst.get("lotSz", 1.0))
                sz_dec = max(0, min(8, round(-math.log10(lot_sz)))) if lot_sz > 0 else 0
            except (ValueError, TypeError):
                sz_dec = 0

            self._tick_size_cache[inst_id]    = tick_sz
            self._px_decimals_cache[inst_id]  = px_dec
            self._sz_decimals_cache[inst_id]  = sz_dec
            self._ct_val_cache[inst_id]        = ct_val

        logger.info(
            "[OKXCore] Caché de instrumentos lista: %d SWAP cargados",
            len(self._tick_size_cache),
        )

    # ── Acceso async ──────────────────────────────────────────────

    @classmethod
    async def get_async(cls) -> "_OKXCore":
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        if cls._instance is not None:
            return cls._instance
        async with cls._init_lock:
            if cls._instance is not None:
                return cls._instance
            logger.info("[OKXCore] Inicializando clientes OKX en hilo separado…")
            cls._instance = await asyncio.to_thread(cls)
            return cls._instance

    # ── Semáforo de concurrencia ──────────────────────────────────

    @classmethod
    def get_info_semaphore(cls) -> asyncio.Semaphore:
        if cls._info_semaphore is None:
            cls._info_semaphore = asyncio.Semaphore(_INFO_CONCURRENCY)
            logger.info(
                "[OKXCore] Semáforo info inicializado: max_concurrency=%d",
                _INFO_CONCURRENCY,
            )
        return cls._info_semaphore


# ── OKXClient: un cliente ligero por symbol ────────────────────────────────────

class OKXClient:
    """
    Drop-in replacement de HLClient para OKX.

    Usar siempre OKXClient.create(symbol) (async) para instanciar.

    Normalización interna:
      symbol = 'BTC/USDT:USDT'  →  inst_id = 'BTC-USDT-SWAP'
      sz en OKX = número de contratos (cada contrato = ctVal coins base)

    Conversión size ↔ contratos:
      contracts = usdc_qty / (price * ct_val)
      Los métodos place_* esperan sz en CONTRATOS (igual que HL espera sz en coins).
      trader.py ya calcula qty en la unidad base del exchange — ajusta allí si
      necesitas convertir de USDC a contratos antes de llamar place_market.
    """

    def __init__(self, symbol: str, core: "_OKXCore | None" = None) -> None:
        self.symbol  = symbol
        self.coin    = _norm_coin(symbol)
        self.inst_id = _to_inst_id(symbol)

        if core is None:
            if _OKXCore._instance is None:
                raise RuntimeError(
                    f"[OKXClient] {symbol}: _OKXCore no inicializado. "
                    "Usar OKXClient.create(symbol) (async)."
                )
            core = _OKXCore._instance

        self._trade   = core.trade
        self._account = core.account
        self._market  = core.market
        self._public  = core.public
        self._core    = core

        # Alias de compatibilidad con trader.py que accede a self._hl_client._info
        # OKXClient expone .all_mids() directamente, pero para el acceso ._info.all_mids()
        # creamos un namespace mínimo.
        self._info = self

    @classmethod
    async def create(cls, symbol: str) -> "OKXClient":
        core = await _OKXCore.get_async()
        return cls(symbol, core=core)

    # ── Metadatos de instrumento ──────────────────────────────────

    def get_sz_decimals(self) -> int:
        return self._core._sz_decimals_cache.get(self.inst_id, 0)

    def get_px_decimals(self) -> int:
        return self._core._px_decimals_cache.get(self.inst_id, 2)

    def get_tick_size(self) -> float:
        return self._core._tick_size_cache.get(self.inst_id, 0.01)

    def get_max_leverage(self) -> int:
        """
        FIX v2 (BUG 5): el método correcto según doc OKX v5 es
        GET /api/v5/account/leverage-info → AccountAPI.get_leverage()
        El anterior get_max_order_size() no existe en python-okx AccountAPI
        y lanzaba AttributeError silenciado.
        """
        cached = self._core._max_leverage_cache.get(self.inst_id)
        if cached:
            return cached
        try:
            resp = self._account.get_leverage(
                instId=self.inst_id,
                mgnMode="cross",
            )
            data = (resp or {}).get("data", [{}])
            lev  = int(float((data[0] if data else {}).get("lever", 20)))
        except Exception as exc:
            logger.warning("[%s] get_max_leverage error: %s — usando default 20",
                           self.inst_id, exc)
            lev = 20
        self._core._max_leverage_cache[self.inst_id] = lev
        return lev

    def get_ct_val(self) -> float:
        """Valor en coin base de 1 contrato (e.g. BTC: 0.001, ETH: 0.01)."""
        return self._core._ct_val_cache.get(self.inst_id, 1.0)

    def round_px(self, price: float) -> float:
        dec = self.get_px_decimals()
        return round(price, dec)

    def round_sz(self, sz: float) -> float:
        """Redondea sz (contratos) al lotSz del instrumento."""
        dec = self.get_sz_decimals()
        if dec == 0:
            return float(math.floor(sz))
        factor = 10 ** dec
        return math.floor(sz * factor) / factor

    # ── Conversión USDC ↔ contratos ───────────────────────────────

    def usdc_to_contracts(self, usdc: float, price: float) -> float:
        """Convierte capital en USDC a número de contratos para OKX."""
        ct_val = self.get_ct_val()
        if price <= 0 or ct_val <= 0:
            return 0.0
        return usdc / (price * ct_val)

    def contracts_to_usdc(self, contracts: float, price: float) -> float:
        """Convierte contratos a valor nocional en USDC."""
        return contracts * self.get_ct_val() * price

    # ── Precios / all_mids ────────────────────────────────────────

    def all_mids(self) -> dict[str, float]:
        """
        Retorna un dict {coin: mid_price} para todos los SWAP activos.
        Compatible con la llamada self._hl_client._info.all_mids() en trader.py.
        """
        try:
            resp   = self._market.get_tickers(instType="SWAP")
            tickers = (resp or {}).get("data", [])
            result  = {}
            for t in tickers:
                inst = t.get("instId", "")
                if not inst.endswith("-USDT-SWAP"):
                    continue
                coin = inst.replace("-USDT-SWAP", "")
                bid  = float(t.get("bidPx") or 0)
                ask  = float(t.get("askPx") or 0)
                if bid > 0 and ask > 0:
                    result[coin] = (bid + ask) / 2
                elif t.get("last"):
                    result[coin] = float(t["last"])
            return result
        except Exception as exc:
            logger.warning("[%s] all_mids error: %s", self.inst_id, exc)
            return {}

    # ── Leverage ──────────────────────────────────────────────────

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = False) -> dict:
        mgnMode = "cross" if is_cross else "isolated"
        try:
            resp = self._account.set_leverage(
                instId=self.inst_id,
                lever=str(leverage),
                mgnMode=mgnMode,
            )
            logger.info("[%s] Leverage configurado: %dx (%s)", self.inst_id, leverage, mgnMode)
            return resp
        except Exception as exc:
            logger.warning("[%s] set_leverage error: %s", self.inst_id, exc)
            return {}

    # ── Órdenes de mercado y límite ───────────────────────────────

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        """
        FIX v2 (BUG 6): eliminado el parámetro reduceOnly de place_order().
        La API OKX v5 NO acepta reduceOnly en /api/v5/trade/order.
        El cierre de posición se controla con posSide (hedge mode):
          - Abrir long:  side=buy,  posSide=long
          - Cerrar long: side=sell, posSide=long  (reduce_only=True)
          - Abrir short: side=sell, posSide=short
          - Cerrar short:side=buy,  posSide=short (reduce_only=True)
        """
        sz_r     = self.round_sz(sz)
        side     = "buy" if is_buy else "sell"
        pos_side = self._infer_pos_side(is_buy, reduce_only)
        try:
            resp = self._trade.place_order(
                instId=self.inst_id,
                tdMode="cross",
                side=side,
                posSide=pos_side,
                ordType="market",
                sz=str(sz_r),
            )
            logger.info(
                "[%s] place_market: %s %.6f contratos | posSide=%s reduce=%s",
                self.inst_id, side.upper(), sz_r, pos_side, reduce_only,
            )
            return resp
        except Exception as exc:
            logger.error("[%s] place_market error: %s", self.inst_id, exc)
            return {"error": str(exc)}

    def place_limit(
        self,
        is_buy: bool,
        sz: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> dict:
        """
        FIX v2 (BUG 6 + BUG 7):
          - Eliminado reduceOnly (no existe en OKX v5 place_order)
          - Eliminado timeInForce (no existe en python-okx TradeAPI.place_order)
            El SDK de python-okx usa ordType para controlar el comportamiento:
            'limit' = GTC por defecto, 'post_only' = maker only.
            Para IOC/FOK usar ordType='limit' con la flag correspondiente
            pero el SDK actual no lo expone como kwarg separado.
        """
        sz_r     = self.round_sz(sz)
        px_r     = self.round_px(price)
        side     = "buy" if is_buy else "sell"
        pos_side = self._infer_pos_side(is_buy, reduce_only)

        # Mapear tif a ordType compatible con python-okx
        # 'post_only' es el único ordType alternativo soportado para limit
        ord_type = "post_only" if tif.upper() in ("POST_ONLY", "POSTONLY") else "limit"

        try:
            resp = self._trade.place_order(
                instId=self.inst_id,
                tdMode="cross",
                side=side,
                posSide=pos_side,
                ordType=ord_type,
                px=str(px_r),
                sz=str(sz_r),
            )
            logger.info(
                "[%s] place_limit: %s %.6f @ %.6f | ordType=%s posSide=%s reduce=%s",
                self.inst_id, side.upper(), sz_r, px_r, ord_type, pos_side, reduce_only,
            )
            return resp
        except Exception as exc:
            logger.error("[%s] place_limit error: %s", self.inst_id, exc)
            return {"error": str(exc)}

    # ── TP / SL (Algo orders) ─────────────────────────────────────

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px:  Optional[float] = None,
        entry_px:  Optional[float] = None,
    ) -> dict:
        """
        Take-Profit en OKX usando ordType='conditional' (algo order).
        is_buy=True  → cierra un SHORT (tp de short)
        is_buy=False → cierra un LONG  (tp de long)
        """
        sz_r       = self.round_sz(sz)
        tpx        = self.round_px(trigger_px)
        side       = "buy" if is_buy else "sell"
        pos_side   = "short" if is_buy else "long"

        # OKX: tpOrdPx="-1" = ejecutar a mercado cuando se activa el trigger
        ord_px = "-1" if limit_px is None else str(self.round_px(limit_px))

        try:
            resp = self._trade.place_algo_order(
                instId=self.inst_id,
                tdMode="cross",
                side=side,
                posSide=pos_side,
                ordType="conditional",
                sz=str(sz_r),
                tpTriggerPx=str(tpx),
                tpOrdPx=ord_px,
            )
            logger.info(
                "[%s] place_tp: %s %.6f @ trigger=%.6f ord_px=%s",
                self.inst_id, side.upper(), sz_r, tpx, ord_px,
            )
            return resp
        except Exception as exc:
            logger.error("[%s] place_tp error: %s", self.inst_id, exc)
            return {"error": str(exc)}

    def place_sl(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        entry_px:   Optional[float] = None,
    ) -> dict:
        """
        Stop-Loss en OKX usando ordType='conditional' (algo order).
        is_buy=True  → cierra un SHORT
        is_buy=False → cierra un LONG
        slOrdPx="-1" = ejecutar a mercado (market SL)
        """
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "buy" if is_buy else "sell"
        pos_side = "short" if is_buy else "long"

        try:
            resp = self._trade.place_algo_order(
                instId=self.inst_id,
                tdMode="cross",
                side=side,
                posSide=pos_side,
                ordType="conditional",
                sz=str(sz_r),
                slTriggerPx=str(tpx),
                slOrdPx="-1",
            )
            logger.info(
                "[%s] place_sl: %s %.6f @ trigger=%.6f",
                self.inst_id, side.upper(), sz_r, tpx,
            )
            return resp
        except Exception as exc:
            logger.error("[%s] place_sl error: %s", self.inst_id, exc)
            return {"error": str(exc)}

    def place_bulk(self, orders: list[dict]) -> dict:
        raise NotImplementedError(
            "place_bulk no está implementado para OKXClient. "
            "Usa place_market/place_limit/place_tp/place_sl individualmente."
        )

    # ── Consultas de cuenta ───────────────────────────────────────

    def get_user_state(self) -> dict:
        """Devuelve el estado de la cuenta (compatibilidad con HLClient)."""
        try:
            return self._account.get_account_balance() or {}
        except Exception as exc:
            logger.warning("[%s] get_user_state error: %s", self.inst_id, exc)
            return {}

    def get_balance_usdc(self) -> float:
        """Balance total de la cuenta en USDT."""
        try:
            resp   = self._account.get_account_balance(ccy="USDT")
            data   = (resp or {}).get("data", [{}])
            details = (data[0] if data else {}).get("details", [])
            for d in details:
                if d.get("ccy") == "USDT":
                    return float(d.get("cashBal", 0))
        except Exception as exc:
            logger.warning("[%s] get_balance_usdc error: %s", self.inst_id, exc)
        return 0.0

    def get_positions(self) -> list[dict]:
        """
        Posiciones abiertas para self.inst_id.

        Retorna lista con el mismo schema que HLClient.get_positions():
          [{"side": "long"|"short", "entryPx": float, "size": float}]
        """
        try:
            resp = self._account.get_positions(instId=self.inst_id)
            raw  = (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[%s] get_positions error: %s", self.inst_id, exc)
            return []

        result = []
        for p in raw:
            pos_sz   = float(p.get("pos", 0) or 0)
            entry_px = float(p.get("avgPx", 0) or 0)
            if pos_sz == 0:
                continue
            # OKX posSide: 'long' | 'short' | 'net'
            pos_side = p.get("posSide", "")
            if pos_side not in ("long", "short"):
                pos_side = "long" if pos_sz > 0 else "short"
            result.append({
                "side":    pos_side,
                "entryPx": entry_px,
                "size":    abs(pos_sz),
                # Extras opcionales (no rompen nada en HLClient compatibility)
                "unrealizedPnl": float(p.get("upl", 0) or 0),
                "lever":         int(float(p.get("lever", 0) or 0)),
            })
        return result

    def get_open_orders(self) -> list:
        """Órdenes abiertas normales (no algo) para este instrumento."""
        try:
            resp = self._trade.get_order_list(instId=self.inst_id)
            return (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[%s] get_open_orders error: %s", self.inst_id, exc)
            return []

    def _get_open_algo_orders(self) -> list:
        """Algo orders (TP/SL conditional) pendientes para este instrumento."""
        try:
            resp = self._trade.get_algo_order_list(
                ordType="conditional", instId=self.inst_id
            )
            return (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[%s] get_algo_orders error: %s", self.inst_id, exc)
            return []

    def cancel_order(self, order_id) -> dict:
        """Cancela una orden normal por ordId."""
        try:
            resp = self._trade.cancel_order(
                instId=self.inst_id, ordId=str(order_id)
            )
            return resp or {}
        except Exception as exc:
            logger.warning("[%s] cancel_order %s error: %s", self.inst_id, order_id, exc)
            return {"error": str(exc)}

    def _cancel_algo_order(self, algo_id: str) -> dict:
        """Cancela una algo order (TP/SL) por algoId."""
        try:
            resp = self._trade.cancel_algo_order(
                [{"instId": self.inst_id, "algoId": algo_id}]
            )
            return resp or {}
        except Exception as exc:
            logger.warning("[%s] cancel_algo %s error: %s", self.inst_id, algo_id, exc)
            return {"error": str(exc)}

    def cancel_all_open_tpsl(self) -> list[dict]:
        """
        Cancela todas las algo orders de TP/SL para este instrumento.
        Equivalente a HLClient.cancel_all_open_tpsl().
        """
        algo_orders = self._get_open_algo_orders()
        results = []
        for o in algo_orders:
            algo_id = o.get("algoId")
            if not algo_id:
                continue
            r = self._cancel_algo_order(algo_id)
            results.append(r)
            logger.info("[%s] Cancelada algo order (TP/SL) algoId=%s", self.inst_id, algo_id)
        return results

    # ── Helper interno: posSide ───────────────────────────────────

    @staticmethod
    def _infer_pos_side(is_buy: bool, reduce_only: bool) -> str:
        """
        OKX hedge mode:
          Abrir long:   is_buy=True,  reduce=False → posSide='long'
          Abrir short:  is_buy=False, reduce=False → posSide='short'
          Cerrar long:  is_buy=False, reduce=True  → posSide='long'
          Cerrar short: is_buy=True,  reduce=True  → posSide='short'
        """
        if not reduce_only:
            return "long" if is_buy else "short"
        else:
            return "long" if not is_buy else "short"
