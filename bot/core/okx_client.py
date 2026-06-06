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
    place_bulk(orders)                         # no soportado → raises NotImplementedError

  Consultas:
    get_positions() → list[dict]
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
    all_mids() → dict[str, float]

Variables de entorno requeridas:
  OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE

Opcionales:
  OKX_TESTNET=true, OKX_MARGIN_MODE=isolated|cross, OKX_INFO_CONCURRENCY=4

Dependencias:
  pip install python-okx>=0.4.1

CHANGELOG:
  FIX v2: get_max_leverage, reduceOnly, timeInForce
  FIX v3: tdMode hardcoded, _warm_cache kwargs, _cancel_algo_order
  FIX v4: _check_okx_import detallado, _cancel_algo_order lista dicts, get_async log
  FIX v5 (2026-06-06): okx.Market → okx.MarketData (breaking change en python-okx v0.4.x)
                        requirements.txt: python-okx>=0.4.1
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
from typing import Optional

logger = logging.getLogger("OKXClient")

# ── Env vars ──────────────────────────────────────────────────────────────────
_OKX_API_KEY      = os.getenv("OKX_API_KEY",      "").strip()
_OKX_API_SECRET   = os.getenv("OKX_API_SECRET",   "").strip()
_OKX_PASSPHRASE   = os.getenv("OKX_PASSPHRASE",   "").strip()
_USE_TESTNET      = os.getenv("OKX_TESTNET",      "").lower() in ("true", "1", "yes")
_DEFAULT_MGN_MODE = os.getenv("OKX_MARGIN_MODE",  "isolated").strip().lower()
_INFO_CONCURRENCY = int(os.getenv("OKX_INFO_CONCURRENCY", "4"))

_FLAG = "1" if _USE_TESTNET else "0"


# ── Helpers de símbolo ────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    for suffix in ("USDTUSDT", "USDT", "SWAP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _to_inst_id(symbol: str) -> str:
    return f"{_norm_coin(symbol)}-USDT-SWAP"


def _check_okx_import() -> None:
    """
    Verifica que python-okx v0.4.x esté instalado.
    En v0.4.x: okx.Market fue renombrado a okx.MarketData.
    """
    missing = []
    for mod in ("okx.Trade", "okx.Account", "okx.MarketData", "okx.PublicData"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)

    if not missing:
        return

    version_info = "desconocida"
    try:
        import importlib.metadata
        version_info = importlib.metadata.version("python-okx")
    except Exception:
        pass

    raise ImportError(
        f"Módulo(s) de python-okx no encontrados: {missing}. "
        f"Versión instalada: {version_info}. "
        f"Requiere python-okx>=0.4.1 (en v0.4.x okx.Market pasó a llamarse okx.MarketData). "
        f"Instala con: pip install python-okx>=0.4.1"
    )


# ── _OKXCore ───────────────────────────────────────────────────────────────────

class _OKXCore:
    _instance:        "_OKXCore | None"         = None
    _init_lock:       "asyncio.Lock | None"      = None
    _info_semaphore:  "asyncio.Semaphore | None" = None

    def __init__(self) -> None:
        if not _OKX_API_KEY or not _OKX_API_SECRET or not _OKX_PASSPHRASE:
            raise ValueError(
                "OKX_API_KEY, OKX_API_SECRET y OKX_PASSPHRASE son obligatorias."
            )

        _check_okx_import()

        # FIX v5: okx.MarketData (antes okx.Market en v0.2.x/v0.3.x)
        import okx.Trade      as _Trade
        import okx.Account    as _Account
        import okx.MarketData as _Market      # <—— breaking change v0.4.x
        import okx.PublicData as _Public

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

        self._sz_decimals_cache:  dict[str, int]   = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._tick_size_cache:    dict[str, float] = {}
        self._ct_val_cache:       dict[str, float] = {}
        self._max_leverage_cache: dict[str, int]   = {}

        self._warm_cache()

    def _warm_cache(self) -> None:
        try:
            resp = self.public.get_instruments(instType="SWAP")
            instruments = (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[OKXCore] get_instruments falló: %s", exc)
            return

        for inst in instruments:
            inst_id = inst.get("instId", "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue

            try:
                tick_sz = float(inst.get("tickSz", 0.01))
            except (ValueError, TypeError):
                tick_sz = 0.01
            px_dec = max(0, min(8, round(-math.log10(tick_sz)))) if tick_sz > 0 else 2

            try:
                ct_val = float(inst.get("ctVal", 1.0))
            except (ValueError, TypeError):
                ct_val = 1.0

            try:
                lot_sz = float(inst.get("lotSz", 1.0))
                sz_dec = max(0, min(8, round(-math.log10(lot_sz)))) if lot_sz > 0 else 0
            except (ValueError, TypeError):
                sz_dec = 0

            self._tick_size_cache[inst_id]   = tick_sz
            self._px_decimals_cache[inst_id] = px_dec
            self._sz_decimals_cache[inst_id] = sz_dec
            self._ct_val_cache[inst_id]       = ct_val

        logger.info(
            "[OKXCore] Caché lista: %d SWAP cargados",
            len(self._tick_size_cache),
        )

    @classmethod
    async def get_async(cls) -> "_OKXCore":
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        if cls._instance is not None:
            return cls._instance
        async with cls._init_lock:
            if cls._instance is not None:
                return cls._instance
            logger.info("[OKXCore] Inicializando clientes OKX…")
            try:
                cls._instance = await asyncio.to_thread(cls)
            except Exception as exc:
                logger.error("[OKXCore] Falló la inicialización: %s", exc, exc_info=True)
                raise
            return cls._instance

    @classmethod
    def get_info_semaphore(cls) -> asyncio.Semaphore:
        if cls._info_semaphore is None:
            cls._info_semaphore = asyncio.Semaphore(_INFO_CONCURRENCY)
        return cls._info_semaphore


# ── OKXClient ───────────────────────────────────────────────────────────────────

class OKXClient:
    def __init__(
        self,
        symbol: str,
        core: "_OKXCore | None" = None,
        margin_mode: str = _DEFAULT_MGN_MODE,
    ) -> None:
        self.symbol  = symbol
        self.coin    = _norm_coin(symbol)
        self.inst_id = _to_inst_id(symbol)
        self.td_mode = "isolated" if margin_mode == "isolated" else "cross"

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
        self._info    = self  # alias compatibilidad

    @classmethod
    async def create(
        cls,
        symbol: str,
        margin_mode: str = _DEFAULT_MGN_MODE,
    ) -> "OKXClient":
        core = await _OKXCore.get_async()
        return cls(symbol, core=core, margin_mode=margin_mode)

    # ── Metadatos ──────────────────────────────────────────────────

    def get_sz_decimals(self) -> int:
        return self._core._sz_decimals_cache.get(self.inst_id, 0)

    def get_px_decimals(self) -> int:
        return self._core._px_decimals_cache.get(self.inst_id, 2)

    def get_tick_size(self) -> float:
        return self._core._tick_size_cache.get(self.inst_id, 0.01)

    def get_max_leverage(self) -> int:
        cached = self._core._max_leverage_cache.get(self.inst_id)
        if cached:
            return cached
        try:
            resp = self._account.get_leverage(
                instId=self.inst_id, mgnMode=self.td_mode
            )
            data = (resp or {}).get("data", [{}])
            lev  = int(float((data[0] if data else {}).get("lever", 20)))
        except Exception as exc:
            logger.warning("[%s] get_max_leverage error: %s", self.inst_id, exc)
            lev = 20
        self._core._max_leverage_cache[self.inst_id] = lev
        return lev

    def get_ct_val(self) -> float:
        return self._core._ct_val_cache.get(self.inst_id, 1.0)

    def round_px(self, price: float) -> float:
        return round(price, self.get_px_decimals())

    def round_sz(self, sz: float) -> float:
        dec = self.get_sz_decimals()
        if dec == 0:
            return float(math.floor(sz))
        factor = 10 ** dec
        return math.floor(sz * factor) / factor

    # ── Conversión ────────────────────────────────────────────────

    def usdc_to_contracts(self, usdc: float, price: float) -> float:
        ct_val = self.get_ct_val()
        if price <= 0 or ct_val <= 0:
            return 0.0
        return usdc / (price * ct_val)

    def contracts_to_usdc(self, contracts: float, price: float) -> float:
        return contracts * self.get_ct_val() * price

    # ── Precios ─────────────────────────────────────────────────────

    def all_mids(self) -> dict[str, float]:
        try:
            resp    = self._market.get_tickers(instType="SWAP")
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

    # ── Leverage ───────────────────────────────────────────────────

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = False) -> dict:
        mgnMode = "cross" if is_cross else "isolated"
        try:
            resp = self._account.set_leverage(
                instId=self.inst_id, lever=str(leverage), mgnMode=mgnMode
            )
            logger.info("[%s] Leverage: %dx (%s)", self.inst_id, leverage, mgnMode)
            return resp
        except Exception as exc:
            logger.warning("[%s] set_leverage error: %s", self.inst_id, exc)
            return {}

    # ── Órdenes ─────────────────────────────────────────────────────

    @staticmethod
    def _infer_pos_side(is_buy: bool, reduce_only: bool) -> str:
        if not reduce_only:
            return "long" if is_buy else "short"
        return "long" if not is_buy else "short"

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        sz_r     = self.round_sz(sz)
        side     = "buy" if is_buy else "sell"
        pos_side = self._infer_pos_side(is_buy, reduce_only)
        try:
            resp = self._trade.place_order(
                instId=self.inst_id, tdMode=self.td_mode,
                side=side, posSide=pos_side,
                ordType="market", sz=str(sz_r),
            )
            logger.info("[%s] place_market: %s %.6f | posSide=%s tdMode=%s",
                        self.inst_id, side.upper(), sz_r, pos_side, self.td_mode)
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
        sz_r     = self.round_sz(sz)
        px_r     = self.round_px(price)
        side     = "buy" if is_buy else "sell"
        pos_side = self._infer_pos_side(is_buy, reduce_only)
        ord_type = "post_only" if tif.upper() in ("POST_ONLY", "POSTONLY") else "limit"
        try:
            resp = self._trade.place_order(
                instId=self.inst_id, tdMode=self.td_mode,
                side=side, posSide=pos_side,
                ordType=ord_type, px=str(px_r), sz=str(sz_r),
            )
            logger.info("[%s] place_limit: %s %.6f @ %.6f | %s tdMode=%s",
                        self.inst_id, side.upper(), sz_r, px_r, ord_type, self.td_mode)
            return resp
        except Exception as exc:
            logger.error("[%s] place_limit error: %s", self.inst_id, exc)
            return {"error": str(exc)}

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px:  Optional[float] = None,
        entry_px:  Optional[float] = None,
    ) -> dict:
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "buy" if is_buy else "sell"
        pos_side = "short" if is_buy else "long"
        ord_px   = "-1" if limit_px is None else str(self.round_px(limit_px))
        try:
            resp = self._trade.place_algo_order(
                instId=self.inst_id, tdMode=self.td_mode,
                side=side, posSide=pos_side,
                ordType="conditional", sz=str(sz_r),
                tpTriggerPx=str(tpx), tpOrdPx=ord_px,
            )
            logger.info("[%s] place_tp: %s %.6f @ %.6f tdMode=%s",
                        self.inst_id, side.upper(), sz_r, tpx, self.td_mode)
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
        sz_r     = self.round_sz(sz)
        tpx      = self.round_px(trigger_px)
        side     = "buy" if is_buy else "sell"
        pos_side = "short" if is_buy else "long"
        try:
            resp = self._trade.place_algo_order(
                instId=self.inst_id, tdMode=self.td_mode,
                side=side, posSide=pos_side,
                ordType="conditional", sz=str(sz_r),
                slTriggerPx=str(tpx), slOrdPx="-1",
            )
            logger.info("[%s] place_sl: %s %.6f @ %.6f tdMode=%s",
                        self.inst_id, side.upper(), sz_r, tpx, self.td_mode)
            return resp
        except Exception as exc:
            logger.error("[%s] place_sl error: %s", self.inst_id, exc)
            return {"error": str(exc)}

    def place_bulk(self, orders: list[dict]) -> dict:
        raise NotImplementedError(
            "place_bulk no implementado para OKXClient."
        )

    # ── Cuenta ─────────────────────────────────────────────────────────

    def get_user_state(self) -> dict:
        try:
            return self._account.get_account_balance() or {}
        except Exception as exc:
            logger.warning("[%s] get_user_state error: %s", self.inst_id, exc)
            return {}

    def get_balance_usdc(self) -> float:
        try:
            resp    = self._account.get_account_balance(ccy="USDT")
            data    = (resp or {}).get("data", [{}])
            details = (data[0] if data else {}).get("details", [])
            for d in details:
                if d.get("ccy") == "USDT":
                    return float(d.get("cashBal", 0))
        except Exception as exc:
            logger.warning("[%s] get_balance_usdc error: %s", self.inst_id, exc)
        return 0.0

    def get_positions(self) -> list[dict]:
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
            pos_side = p.get("posSide", "")
            if pos_side not in ("long", "short"):
                pos_side = "long" if pos_sz > 0 else "short"
            result.append({
                "side":          pos_side,
                "entryPx":       entry_px,
                "size":          abs(pos_sz),
                "unrealizedPnl": float(p.get("upl", 0) or 0),
                "lever":         int(float(p.get("lever", 0) or 0)),
            })
        return result

    def get_open_orders(self) -> list:
        try:
            resp = self._trade.get_order_list(instId=self.inst_id)
            return (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[%s] get_open_orders error: %s", self.inst_id, exc)
            return []

    def _get_open_algo_orders(self) -> list:
        try:
            resp = self._trade.get_algo_order_list(
                ordType="conditional", instId=self.inst_id
            )
            return (resp or {}).get("data", [])
        except Exception as exc:
            logger.warning("[%s] get_algo_orders error: %s", self.inst_id, exc)
            return []

    def cancel_order(self, order_id) -> dict:
        try:
            resp = self._trade.cancel_order(
                instId=self.inst_id, ordId=str(order_id)
            )
            return resp or {}
        except Exception as exc:
            logger.warning("[%s] cancel_order %s error: %s", self.inst_id, order_id, exc)
            return {"error": str(exc)}

    def _cancel_algo_order(self, algo_id: str) -> dict:
        """cancel_algo_order espera lista de dicts [{instId, algoId}]"""
        try:
            resp = self._trade.cancel_algo_order(
                [{"instId": self.inst_id, "algoId": algo_id}]
            )
            return resp or {}
        except Exception as exc:
            logger.warning("[%s] cancel_algo %s error: %s", self.inst_id, algo_id, exc)
            return {"error": str(exc)}

    def cancel_all_open_tpsl(self) -> list[dict]:
        results = []
        for o in self._get_open_algo_orders():
            algo_id = o.get("algoId")
            if not algo_id:
                continue
            r = self._cancel_algo_order(algo_id)
            results.append(r)
            logger.info("[%s] Cancelada algo order algoId=%s", self.inst_id, algo_id)
        return results
