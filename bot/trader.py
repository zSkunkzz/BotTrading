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

v5 — _fetch_candles fix (2026-06-06):
  - Problema: _fetch_candles capturaba "Server disconnected" y devolvía []
    silenciosamente. OHLCVCache no lo ve como excepción sino como "fetch
    vacío" y no registra last_exc — el backoff exponencial no reíntenta
    correctamente ni el stale fallback muestra la causa real.
  - Fix: re-raise de la excepción en vez de retornar [] cuando falla la
    llamada HTTP a get_candlesticks. OHLCVCache ahora recibe la excepción,
    la registra como last_exc, hace backoff 1s/2s/4s real, y si agota los
    reintentos devuelve datos stale con el error visible en el WARNING.
  - Solo los errores de parseo de velas individuales (IndexError, TypeError,
    ValueError) siguen siendo silenciados con continue — son datos corruptos
    puntuales, no errores de red.

v6 — open_order / close_position (2026-06-06):
  - BUG: trading_loop.py llamaba trader.open_order(signal, risk) pero el
    método nunca fue añadido a FuturesTrader durante la migración OKX.
  - Añadido open_order(signal, risk): orquesta apertura completa.
  - Añadido close_position(reason): cierre market completo de la posición
    activa con cancelación de TP/SL huérfanos.

v7 — bug fixes (2026-06-06):
  BUG1 FIX: save_position(symbol, data_dict) — se pasaba como kwargs
    individuales; corregido construyendo el dict explícitamente.
  BUG2 FIX: _cancel_tpsl_safe es coroutine async — se awaita directamente
    en lugar de envolvería en asyncio.to_thread(lambda: ...) que solo
    devolvería la coroutine sin ejecutarla.
  BUG3 FIX: OKXClient.create() se llamaba dos veces en open_order (una
    para ctVal/sz_dec y otra para place_market). Ahora se crea una sola
    instancia y se reutiliza en todo el flujo.
  BUG4 FIX: place_market en close_position no pasaba ref_price. Añadido
    get_price() antes del market-close y pasado como ref_price.
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

                # FIX v4: OKX devuelve data=[] cuando el instrumento no existe
                # en el entorno demo (OPN, MON, PENGU, TURBO, MEME, NEIRO, etc.)
                if not data:
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

    # ── open_order ────────────────────────────────────────────────

    async def open_order(self, signal: dict, risk) -> None:
        """
        Abre una posición en OKX según la señal del DecisionEngine.

        Flujo:
          1. Staleness check: aborta si el precio se alejó demasiado del entry.
          2. Cálculo de qty en contratos OKX:
               contratos = (usdc_per_trade * leverage) / (price * ctVal)
          3. place_market() via OKXClient — orden market de apertura.
          4. Confirm fill: reintenta _get_positions hasta encontrar la posición
             abierta (max _FILL_RETRIES intentos con delay _FILL_DELAY).
          5. Ajusta SL/TP al filled_price real si difiere del entry señal.
          6. place_sl / place_tp via _place_tpsl().
          7. Actualiza estado interno del trader.
          8. Persiste posición en disco (save_position).

        En dry_run=True: simula todo sin llamar a la API de órdenes.
        """
        is_long = signal.get("side") == "long"
        action  = signal.get("action", "BUY" if is_long else "SELL")

        # ── 1. Precio actual y staleness check ────────────────────
        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio: %s", self.symbol, e)
            return

        stale_reason = _check_price_staleness(signal, ref_price, is_long)
        if stale_reason:
            logger.warning("[%s] open_order cancelado: %s", self.symbol, stale_reason)
            return

        # ── 2. Calcular qty en contratos ──────────────────────────
        usdc_per_trade = float(getattr(risk, "usdc_per_trade", 0) or 0)
        leverage       = self.leverage
        notional_usdc  = usdc_per_trade * leverage

        # BUG3 FIX: crear OKXClient UNA sola vez y reutilizarlo en todo el flujo
        try:
            from bot.core.okx_client import OKXClient
            _client = await OKXClient.create(self.symbol)
            ct_val  = _client.get_ct_val()
            sz_dec  = _client.get_sz_decimals()
        except Exception as e:
            logger.warning("[%s] open_order: error obteniendo ctVal: %s — usando 1.0",
                           self.symbol, e)
            _client = None
            ct_val = 1.0
            sz_dec = 0

        if ref_price > 0 and ct_val > 0:
            raw_qty = notional_usdc / (ref_price * ct_val)
        else:
            logger.error("[%s] open_order: precio o ctVal inválido (price=%.4f ctVal=%.6f)",
                         self.symbol, ref_price, ct_val)
            return

        # Redondeo al lotSz
        import math as _math
        if sz_dec == 0:
            qty = float(_math.floor(raw_qty))
        else:
            factor = 10 ** sz_dec
            qty = _math.floor(raw_qty * factor) / factor

        if qty <= 0:
            logger.warning(
                "[%s] open_order: qty calculada = 0 "
                "(notional=%.2f USDC lev=%dx price=%.4f ctVal=%.6f raw=%.6f) — abortando.",
                self.symbol, usdc_per_trade, leverage, ref_price, ct_val, raw_qty,
            )
            return

        logger.info(
            "[%s] open_order: %s | qty=%.6f contratos | notional=%.2fUSDC | "
            "lev=%dx | price=%.4f | ctVal=%.6f | dry_run=%s",
            self.symbol, action, qty, usdc_per_trade, leverage,
            ref_price, ct_val, self.dry_run,
        )

        # ── 3. place_market ───────────────────────────────────────
        filled_price = ref_price  # fallback antes del confirm

        if self.dry_run:
            logger.info("[%s] [DRY-RUN] open_order: simulando place_market %s %.6f @ %.4f",
                        self.symbol, action, qty, ref_price)
        else:
            if _client is None:
                logger.error("[%s] open_order: OKXClient no disponible — abortando.", self.symbol)
                return
            try:
                place_resp = await asyncio.to_thread(
                    _client.place_market,
                    is_buy=is_long,
                    sz=qty,
                    reduce_only=False,
                    ref_price=ref_price,
                )
                code = (place_resp or {}).get("code", "-1")
                if str(code) != "0":
                    msg = (place_resp or {}).get("msg", str(place_resp))
                    logger.error("[%s] open_order: place_market rechazado: %s",
                                 self.symbol, msg)
                    return
                logger.info("[%s] open_order: place_market OK (code=0)", self.symbol)
            except Exception as e:
                logger.error("[%s] open_order: place_market excepción: %s", self.symbol, e)
                return

            # ── 4. Confirm fill ───────────────────────────────────
            for attempt in range(_FILL_RETRIES):
                await asyncio.sleep(_FILL_DELAY)
                try:
                    positions = await self._get_positions()
                    open_side = "long" if is_long else "short"
                    for p in positions:
                        if p.get("side") == open_side and p.get("size", 0) > 0:
                            filled_price = float(p.get("entryPx") or ref_price)
                            qty          = float(p.get("size") or qty)
                            logger.info(
                                "[%s] open_order: fill confirmado (intento %d/%d) — "
                                "entryPx=%.4f qty=%.6f",
                                self.symbol, attempt + 1, _FILL_RETRIES,
                                filled_price, qty,
                            )
                            break
                    else:
                        logger.debug(
                            "[%s] open_order: posición aún no visible (intento %d/%d)",
                            self.symbol, attempt + 1, _FILL_RETRIES,
                        )
                        continue
                    break
                except Exception as e:
                    logger.warning("[%s] open_order: confirm fill error: %s", self.symbol, e)

        # ── 5. Ajustar SL/TP al fill real ─────────────────────────
        sl_px, tp1_px, tp2_px = _adjust_levels_to_fill(signal, filled_price, ref_price)
        tp3_px = float(signal.get("tp3") or 0)
        if tp3_px > 0 and abs(filled_price - ref_price) / max(ref_price, 1e-9) >= 0.0005:
            base = float(signal.get("entry") or 0) or ref_price
            tp3_px = filled_price * (1.0 + (tp3_px - base) / base) if base > 0 else tp3_px

        # ── 6. Colocar SL y TP1 ───────────────────────────────────
        if not self.dry_run:
            try:
                await self._place_tpsl(
                    qty=qty,
                    sl_price=sl_px  if sl_px  > 0 else None,
                    tp_price=tp1_px if tp1_px > 0 else None,
                    is_long=is_long,
                )
            except Exception as e:
                logger.error("[%s] open_order: _place_tpsl error: %s", self.symbol, e)
                # No abortar — la posición ya está abierta, el PM se encargará
        else:
            if sl_px > 0:
                logger.info("[%s] [DRY-RUN] SL simulado @ %.4f", self.symbol, sl_px)
            if tp1_px > 0:
                logger.info("[%s] [DRY-RUN] TP1 simulado @ %.4f", self.symbol, tp1_px)

        # ── 7. Actualizar estado ──────────────────────────────────
        self.position       = "long" if is_long else "short"
        self.entry_price    = filled_price
        self.sl             = sl_px  if sl_px  > 0 else None
        self.tp1            = tp1_px if tp1_px > 0 else None
        self.tp2            = tp2_px if tp2_px > 0 else None
        self.tp3            = tp3_px if tp3_px > 0 else None
        self.tp2_hit        = False
        self._open_qty      = qty
        self._open_notional = usdc_per_trade
        self._open_leverage = leverage
        self._protection_ok = (sl_px > 0 or tp1_px > 0)
        self._tp1_be_done   = False

        # ── 8. Persistir en disco ─────────────────────────────────
        # BUG1 FIX: save_position(symbol, data_dict) — se construye el dict
        # explícitamente en lugar de pasar kwargs que no acepta la función.
        try:
            save_position(
                self.symbol,
                {
                    "side":        self.position,
                    "entry":       filled_price,
                    "sl":          self.sl,
                    "tp1":         self.tp1,
                    "tp2":         self.tp2,
                    "tp3":         self.tp3,
                    "leverage":    leverage,
                    "usdc_amount": usdc_per_trade,
                    "qty":         qty,
                },
            )
        except Exception as e:
            logger.warning("[%s] open_order: save_position error: %s", self.symbol, e)

        logger.info(
            "[%s] ✅ Posición abierta: %s @ %.4f | qty=%.6f | "
            "SL=%.4f | TP1=%.4f | TP2=%.4f",
            self.symbol, self.position.upper(), filled_price, qty,
            self.sl or 0, self.tp1 or 0, self.tp2 or 0,
        )

    # ── close_position ────────────────────────────────────────────

    async def close_position(self, reason: str = "manual") -> None:
        """
        Cierra la posición activa con una orden market de reducción.
        Cancela primero todas las algo-orders (TP/SL) huérfanas.
        Compatible con PositionManager y kill_switch.
        """
        if self.position is None:
            logger.debug("[%s] close_position: sin posición activa.", self.symbol)
            return

        side    = self.position
        qty     = self._open_qty
        is_long = side == "long"

        logger.info(
            "[%s] close_position: cerrando %s qty=%.6f | reason=%s | dry_run=%s",
            self.symbol, side.upper(), qty, reason, self.dry_run,
        )

        if not self.dry_run and qty > 0:
            # BUG2 FIX: _cancel_tpsl_safe es async — se awaita directamente,
            # NO se envuelve en asyncio.to_thread(lambda: ...) que solo
            # devolvería la coroutine sin ejecutarla.
            try:
                from bot.core.trading_loop import _cancel_tpsl_safe
                await _cancel_tpsl_safe(self)
            except Exception as e:
                logger.warning("[%s] close_position: cancel_tpsl error: %s", self.symbol, e)

            # BUG4 FIX: place_market requiere ref_price — obtener precio actual
            # antes de emitir la orden de cierre.
            try:
                close_ref_price = await self.get_price()
            except Exception as e:
                logger.warning(
                    "[%s] close_position: no se pudo obtener precio para cierre (%s) "
                    "— usando last_price=%.4f",
                    self.symbol, e, self._last_price,
                )
                close_ref_price = self._last_price

            try:
                from bot.core.okx_client import OKXClient
                _client = await OKXClient.create(self.symbol)
                resp = await asyncio.to_thread(
                    _client.place_market,
                    is_buy=not is_long,
                    sz=qty,
                    reduce_only=True,
                    ref_price=close_ref_price,
                )
                code = (resp or {}).get("code", "-1")
                if str(code) == "0":
                    logger.info("[%s] close_position: market close OK.", self.symbol)
                else:
                    msg = (resp or {}).get("msg", str(resp))
                    logger.error("[%s] close_position: rechazado: %s", self.symbol, msg)
            except Exception as e:
                logger.error("[%s] close_position: excepción: %s", self.symbol, e)
        else:
            if self.dry_run:
                logger.info("[%s] [DRY-RUN] close_position simulado.", self.symbol)
            elif qty <= 0:
                logger.warning("[%s] close_position: qty=0, skip market order.", self.symbol)

        # Limpiar estado independientemente del resultado de la API
        self.position    = None
        self.entry_price = None
        self.sl          = None
        self.tp1         = None
        self.tp2         = None
        self.tp3         = None
        self._open_qty   = 0.0
        self._protection_ok = False

        from bot.state import clear_position
        try:
            clear_position(self.symbol)
        except Exception as e:
            logger.warning("[%s] close_position: clear_position error: %s", self.symbol, e)

    # ── _fetch_candles ────────────────────────────────────────────

    async def _fetch_candles(self, timeframe: str, n: int) -> list:
        """
        Obtiene velas OHLCV de OKX via MarketData.get_candlesticks.

        FIX v5: Las excepciones de red (ServerDisconnectedError, TimeoutError,
        aiohttp.ClientError, etc.) se re-lanzan en lugar de devolverse como []
        silenciosamente.
        """
        if self._market_api is None:
            return []

        bar = _TF_OKX.get(timeframe, timeframe)

        resp = await asyncio.to_thread(
            self._market_api.get_candlesticks,
            instId=self.inst_id,
            bar=bar,
            limit=str(min(n, 300)),
        )

        raw = resp.get("data", [])
        result = []
        for c in raw:
            try:
                result.append([
                    int(c[0]),
                    float(c[1]),
                    float(c[2]),
                    float(c[3]),
                    float(c[4]),
                    float(c[5]),
                ])
            except (IndexError, TypeError, ValueError):
                continue
        result.reverse()
        return result
