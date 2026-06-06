#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

v2 — OKX migration (2026-06-06):
  Sustituye HLClient / _HLCore por python-okx:
    - okx.Trade.TradeAPI      → open_order / close_order
    - okx.Account.AccountAPI  → _set_leverage / _get_positions
    - okx.PublicData          → get_price via tickers
    - okx.MarketData          → get_ohlcv via candles

  Formato de instrumento OKX: {COIN}-USDT-SWAP  (ej. BTC-USDT-SWAP)
  Los símbolos que llegan del scanner (ej. "BTC") se convierten
  internamente con _to_inst_id().

v3 — OKX Bug 3 fix (2026-06-06):
  Añadidos métodos requeridos por PositionManager:
    - _get_open_orders_raw()         → GET /api/v5/trade/orders-pending
    - _get_open_trigger_orders_raw() → GET /api/v5/trade/orders-algo-pending
      (donde viven los TP/SL algo-orders en OKX)
    - _place_tpsl()                  → coloca SL o TP via OKXClient

v4 — get_price fix (2026-06-06):
  - Corregido IndexError cuando OKX devuelve data=[] (instrumento no
    disponible en demo: OPN, MON, PENGU, TURBO, MEME, NEIRO, etc.)
  - Añadida validación explícita de lista vacía antes de acceder a [0]
  - Traders con instrumento inválido en demo se marcan como skip silencioso
"""
from __future__ import annotations

import asyncio
import functools
import logging
import os
import time
from typing import Callable, Optional

from bot.core.trading_loop import TradingLoop
from bot.ohlcv_cache import ohlcv_cache
from bot.state import save_position

logger = logging.getLogger("FuturesTrader")

_USE_DEMO           = os.getenv("OKX_DEMO", "false").lower() in ("true", "1", "yes")
_FLAG               = "1" if _USE_DEMO else "0"   # 1=demo, 0=live

_OHLCV_BARS             = int(os.getenv("BARS_NEEDED",            "100"))
_PRICE_FETCH_RETRIES    = int(os.getenv("PRICE_FETCH_RETRIES",    "3"))
_SET_LEVERAGE_TIMEOUT_S = float(os.getenv("SET_LEVERAGE_TIMEOUT_S", "15"))

_TF_OKX = {
    "1m":  "1m",  "3m":  "3m",  "5m":  "5m",  "15m": "15m",
    "30m": "30m", "1h":  "1H",  "2h":  "2H",  "4h":  "4H",
    "8h":  "8H",  "1d":  "1Dutc",
}

_FILL_RETRIES        = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))
_FILL_DELAY          = float(os.getenv("POST_FILL_CONFIRM_DELAY", "2.0"))
_MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "3.0")) / 100.0


def _to_inst_id(symbol: str) -> str:
    """Convierte 'BTC' o 'BTC-USDT' → 'BTC-USDT-SWAP'."""
    s = symbol.upper().replace("/", "-")
    if s.endswith("-SWAP"):
        return s
    if "-USDT-" in s:
        return s + "-SWAP" if not s.endswith("-SWAP") else s
    base = s.split("-")[0]
    return f"{base}-USDT-SWAP"


def _check_price_staleness(
    signal: dict,
    ref_price: float,
    is_long: bool,
) -> Optional[str]:
    entry_signal = float(signal.get("entry") or 0)
    if entry_signal <= 0:
        return None
    drift     = (ref_price - entry_signal) / entry_signal
    abs_drift = abs(drift)
    threshold = _MAX_ENTRY_DRIFT_PCT
    if abs_drift > threshold * 2:
        return (
            f"\u26a0\ufe0f Precio actual ({ref_price:.4f}) se alejó {drift*100:+.2f}% del entry "
            f"({entry_signal:.4f}) — supera ±{threshold*200:.1f}% — entrada cancelada"
        )
    if abs_drift <= threshold:
        return None
    if is_long:
        if drift > 0:
            return f"\u23eb [LONG] precio {ref_price:.4f} subió {drift*100:+.2f}% — demasiado caro, cancelado"
        return f"\u23ea [LONG] precio {ref_price:.4f} cayó {drift*100:+.2f}% — setup roto, cancelado"
    else:
        if drift < 0:
            return f"\u23ea [SHORT] precio {ref_price:.4f} bajó {drift*100:+.2f}% — cancelado"
        return f"\u23eb [SHORT] precio {ref_price:.4f} subió {drift*100:+.2f}% — setup roto, cancelado"


def _adjust_levels_to_fill(
    signal: dict,
    filled_price: float,
    ref_price: float,
) -> tuple[float, float, float]:
    sl_px  = float(signal.get("sl")  or 0)
    tp1_px = float(signal.get("tp1") or 0)
    tp2_px = float(signal.get("tp2") or 0)
    base   = float(signal.get("entry") or 0) or ref_price
    if abs(filled_price - base) / base < 0.0005:
        return sl_px, tp1_px, tp2_px
    def _rescale(level: float) -> float:
        if level <= 0:
            return level
        return filled_price * (1.0 + (level - base) / base)
    sl_adj  = _rescale(sl_px)
    tp1_adj = _rescale(tp1_px)
    tp2_adj = _rescale(tp2_px)
    logger.info(
        "Ajuste SL/TP por desfase de fill: base=%.4f → filled=%.4f (%.2f%%) | "
        "SL %.4f→%.4f | TP1 %.4f→%.4f | TP2 %.4f→%.4f",
        base, filled_price, (filled_price - base) / base * 100,
        sl_px, sl_adj, tp1_px, tp1_adj, tp2_px, tp2_adj,
    )
    return sl_adj, tp1_adj, tp2_adj


class FuturesTrader:
    """Orquestador principal de un par de trading en OKX (perpetuos USDT)."""

    def __init__(
        self,
        api_key: Optional[str],
        api_secret: str,
        passphrase: Optional[str],
        symbol: str,
        leverage: int = 5,
        margin_mode: str = "isolated",
        dry_run: bool = True,
    ) -> None:
        self.symbol      = symbol
        self.inst_id     = _to_inst_id(symbol)   # ej. BTC-USDT-SWAP
        self.coin        = symbol.upper().split("-")[0]  # ej. BTC
        self.leverage    = leverage
        self.margin_mode = margin_mode
        self.dry_run     = dry_run

        self.position:       Optional[str]   = None
        self.entry_price:    Optional[float] = None
        self.sl:             Optional[float] = None
        self.tp1:            Optional[float] = None
        self.tp2:            Optional[float] = None
        self.tp3:            Optional[float] = None
        self.tp2_hit:        bool            = False
        self._open_notional: float           = 0.0
        self._open_leverage: int             = leverage
        self._open_qty:      float           = 0.0
        self._protection_ok: bool            = False
        self._tp1_be_done:   bool            = False
        self._last_price:    float           = 0.0

        # Marca el instrumento como no disponible en demo para skip silencioso
        self._instrument_unavailable: bool   = False

        self._api_key    = api_key    or os.getenv("OKX_API_KEY",    "")
        self._api_secret = api_secret or os.getenv("OKX_API_SECRET", "")
        self._passphrase = passphrase or os.getenv("OKX_PASSPHRASE",  "")

        # APIs python-okx (se crean en _init_okx_apis)
        self._trade_api:   object = None
        self._account_api: object = None
        self._market_api:  object = None

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)

    # ── Interfaz pública ──────────────────────────────────────────

    async def run(self, risk, *, global_risk=None) -> None:
        try:
            await self._trading_loop.run(self, risk, global_risk=global_risk)
        except asyncio.CancelledError:
            logger.info("[%s] FuturesTrader cancelado.", self.symbol)
        finally:
            self._stopped_event.set()

    async def cleanup(self) -> None:
        try:
            from bot.ai_trader import close_sessions
            await close_sessions()
        except Exception as e:
            logger.debug("[%s] cleanup ai_trader sessions: %s", self.symbol, e)
        self._stopped_event.set()

    # ── Init OKX APIs ─────────────────────────────────────────────

    async def _get_ccxt(self) -> None:
        """Inicializa las APIs de OKX (equivale al antiguo _get_ccxt de HL)."""
        if self._trade_api is not None:
            return
        try:
            logger.info("[%s] Inicializando OKX APIs (inst=%s, demo=%s)…",
                        self.symbol, self.inst_id, _USE_DEMO)
            await asyncio.to_thread(self._init_okx_apis)
            logger.info("[%s] OKX APIs listas.", self.symbol)
        except Exception as e:
            logger.error("[%s] _get_ccxt (OKX init) error: %s", self.symbol, e)
            raise

    def _init_okx_apis(self) -> None:
        """Síncrono — llamar siempre desde asyncio.to_thread."""
        import okx.Trade      as Trade
        import okx.Account    as Account
        import okx.MarketData as MarketData

        self._trade_api   = Trade.TradeAPI(
            self._api_key, self._api_secret, self._passphrase,
            False, _FLAG,
        )
        self._account_api = Account.AccountAPI(
            self._api_key, self._api_secret, self._passphrase,
            False, _FLAG,
        )
        self._market_api  = MarketData.MarketAPI(
            self._api_key, self._api_secret, self._passphrase,
            False, _FLAG,
        )

    # ── get_price ─────────────────────────────────────────────────

    async def get_price(self) -> float:
        if self._market_api is None:
            if self._last_price > 0:
                return self._last_price
            raise RuntimeError(f"[{self.symbol}] get_price: APIs no inicializadas")

        # Si el instrumento ya fue marcado como no disponible, no reintentar
        if self._instrument_unavailable:
            raise RuntimeError(
                f"[{self.symbol}] get_price: instrumento {self.inst_id} "
                f"no disponible en OKX {'demo' if _USE_DEMO else 'live'} — skip"
            )

        last_exc: Exception | None = None
        delays = [0.4 * (2 ** i) for i in range(_PRICE_FETCH_RETRIES)]

        for attempt, delay in enumerate(delays):
            try:
                resp = await asyncio.to_thread(
                    self._market_api.get_ticker, self.inst_id
                )
                data = resp.get("data", [])

                # FIX: OKX devuelve data=[] cuando el instrumento no existe
                # en el entorno demo (OPN, MON, PENGU, TURBO, MEME, NEIRO, etc.)
                # El acceso original [0] daba IndexError — ahora validamos explícitamente.
                if not data:
                    # Marcar instrumento como no disponible para no seguir reintentando
                    self._instrument_unavailable = True
                    raise ValueError(
                        f"{self.inst_id} no disponible en OKX "
                        f"{'demo' if _USE_DEMO else 'live'} (data=[])"
                    )

                ticker = data[0]
                price  = float(ticker.get("last") or ticker.get("askPx") or 0)
                if price <= 0:
                    raise ValueError(f"ticker con precio cero para {self.inst_id}")
                self._last_price = price
                return price
            except Exception as exc:
                last_exc = exc
                # Si el instrumento se marcó como no disponible, salir sin reintentar
                if self._instrument_unavailable:
                    break
                if attempt < len(delays) - 1:
                    logger.debug(
                        "[%s] get_price intento %d/%d fallido (%s) — reintentando en %.1fs",
                        self.symbol, attempt + 1, _PRICE_FETCH_RETRIES, exc, delay,
                    )
                    await asyncio.sleep(delay)

        if self._last_price > 0:
            logger.warning(
                "[%s] get_price fallido tras %d intentos (%s) — usando precio stale %.4f",
                self.symbol, _PRICE_FETCH_RETRIES, last_exc, self._last_price,
            )
            return self._last_price

        raise RuntimeError(
            f"[{self.symbol}] get_price: sin precio tras {_PRICE_FETCH_RETRIES} intentos: {last_exc}"
        )

    # ── _set_leverage ─────────────────────────────────────────────

    async def _set_leverage(self, leverage: int) -> None:
        if self.dry_run:
            logger.info("[%s] [DRY-RUN] _set_leverage(%dx)", self.symbol, leverage)
            self._open_leverage = leverage
            return
        if self._account_api is None:
            logger.warning("[%s] _set_leverage: account_api no inicializado — skip.", self.symbol)
            return
        mgnMode = "isolated" if self.margin_mode == "isolated" else "cross"
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    self._account_api.set_leverage,
                    lever=str(leverage),
                    mgnMode=mgnMode,
                    instId=self.inst_id,
                ),
                timeout=_SET_LEVERAGE_TIMEOUT_S,
            )
            self._open_leverage = leverage
            logger.info("[%s] Apalancamiento configurado: %dx (%s)",
                        self.symbol, leverage, mgnMode)
        except asyncio.TimeoutError:
            logger.warning("[%s] _set_leverage timeout (%ss)",
                           self.symbol, _SET_LEVERAGE_TIMEOUT_S)
        except Exception as e:
            logger.warning("[%s] _set_leverage error: %s", self.symbol, e)

    # ── OHLCV ─────────────────────────────────────────────────────

    async def get_ohlcv(self, timeframe: str, n: Optional[int] = None) -> list:
        bars_needed = n or _OHLCV_BARS
        async def _fetch(tf: str) -> list:
            return await self._fetch_candles(tf, bars_needed)
        return await ohlcv_cache.get(self.coin, timeframe, _fetch)

    def get_ohlcv_fn(self) -> Callable:
        return functools.partial(self.get_ohlcv)

    # ── _get_positions ────────────────────────────────────────────

    async def _get_positions(self) -> list[dict]:
        if self._account_api is None:
            return []
        try:
            resp = await asyncio.to_thread(
                self._account_api.get_positions, instId=self.inst_id
            )
            positions = resp.get("data", [])
        except Exception as e:
            logger.warning("[%s] _get_positions fetch error: %s", self.symbol, e)
            return []

        result = []
        for p in positions:
            pos_side = p.get("posSide", "")   # long / short / net
            pos_qty  = float(p.get("pos") or 0)
            if pos_qty == 0:
                continue
            if pos_side == "net":
                side = "long" if pos_qty > 0 else "short"
            else:
                side = pos_side
            result.append({
                "side":    side,
                "entryPx": float(p.get("avgPx") or 0),
                "size":    abs(pos_qty),
            })
        return result

    # ── _get_open_orders_raw ──────────────────────────────────────

    async def _get_open_orders_raw(self) -> list[dict]:
        """
        Devuelve las órdenes limit/market pendientes de este instrumento.
        Consulta GET /api/v5/trade/orders-pending via python-okx TradeAPI.
        Requerido por PositionManager._ensure_tpsl().
        """
        if self._trade_api is None:
            return []
        try:
            resp = await asyncio.to_thread(
                self._trade_api.get_order_list,
                instType="SWAP",
                instId=self.inst_id,
            )
            return resp.get("data", []) or []
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e)
            return []

    # ── _get_open_trigger_orders_raw ──────────────────────────────

    async def _get_open_trigger_orders_raw(self) -> list[dict]:
        """
        Devuelve las algo-orders (TP/SL) pendientes de este instrumento.
        Consulta GET /api/v5/trade/orders-algo-pending via python-okx TradeAPI.
        En OKX los SL/TP son 'conditional' algo-orders — viven aquí, NO en
        orders-pending. Requerido por PositionManager._ensure_tpsl().

        Cada item tiene campos:
          algoId, instId, algoType ('sl' | 'tp'), triggerPx, sz, posSide, ...
        """
        if self._trade_api is None:
            return []
        try:
            resp = await asyncio.to_thread(
                self._trade_api.get_algo_order_list,
                ordType="conditional",
                instType="SWAP",
                instId=self.inst_id,
            )
            return resp.get("data", []) or []
        except Exception as e:
            logger.warning("[%s] _get_open_trigger_orders_raw error: %s", self.symbol, e)
            return []

    # ── _place_tpsl ───────────────────────────────────────────────

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> None:
        """
        Coloca SL y/o TP como algo-orders en OKX via OKXClient.
        Requerido por PositionManager._place_emergency_sl_tp().
        """
        from bot.core.okx_client import OKXClient
        client = await OKXClient.create(self.symbol)
        entry_px = self.entry_price

        if sl_price and sl_price > 0:
            try:
                result = await asyncio.to_thread(
                    client.place_sl,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=sl_price,
                    entry_px=entry_px,
                )
                if result and result.get("code") == "0":
                    logger.info("[%s] _place_tpsl: SL=%.5f colocado.", self.symbol, sl_price)
                else:
                    err = (result or {}).get("msg", "error")
                    logger.error("[%s] _place_tpsl: SL rechazado: %s", self.symbol, err)
            except Exception as e:
                logger.error("[%s] _place_tpsl: SL exception: %s", self.symbol, e)
                raise

        if tp_price and tp_price > 0:
            try:
                result = await asyncio.to_thread(
                    client.place_tp,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=tp_price,
                    limit_px=tp_price,
                    entry_px=entry_px,
                )
                if result and result.get("code") == "0":
                    logger.info("[%s] _place_tpsl: TP=%.5f colocado.", self.symbol, tp_price)
                else:
                    err = (result or {}).get("msg", "error")
                    logger.error("[%s] _place_tpsl: TP rechazado: %s", self.symbol, err)
            except Exception as e:
                logger.error("[%s] _place_tpsl: TP exception: %s", self.symbol, e)
                raise

    # ── _fetch_candles ────────────────────────────────────────────

    async def _fetch_candles(self, timeframe: str, n: int) -> list:
        """Usa MarketData.get_candlesticks de python-okx."""
        if self._market_api is None:
            return []
        bar = _TF_OKX.get(timeframe, timeframe)
        try:
            resp = await asyncio.to_thread(
                self._market_api.get_candlesticks,
                instId=self.inst_id,
                bar=bar,
                limit=str(min(n, 300)),  # OKX max 300 velas por petición
            )
        except Exception as e:
            logger.warning("[%s] _fetch_candles error: %s", self.symbol, e)
            return []

        raw = resp.get("data", [])
        result = []
        for c in raw:
            # OKX devuelve: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            try:
                result.append([
                    int(c[0]),    # timestamp ms
                    float(c[1]),  # open
                    float(c[2]),  # high
                    float(c[3]),  # low
                    float(c[4]),  # close
                    float(c[5]),  # volume (contratos)
                ])
            except (IndexError, TypeError, ValueError):
                continue
        # OKX devuelve velas de más reciente a más antigua — invertir
        result.reverse()
        return result
