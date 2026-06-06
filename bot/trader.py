#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

v15 — Fix leverage no aplicado (2026-06-06):
  - open_order ahora llama _set_leverage(leverage) antes de enviar la orden
    de mercado. Sin esta llamada, BingX usaba el leverage por defecto del
    contrato (5x) ignorando la variable LEVERAGE de Railway.

v14 — open_order atómico con place_market_with_tpsl (2026-06-06):
  - open_order usa place_market_with_tpsl() para adjuntar SL+TP1 en una
    única llamada API, eliminando la race condition que existía con la
    secuencia place_market() + _place_tpsl() separadas (Fix #9 bingx_client v6).
  - Si place_market_with_tpsl falla, fallback automático a place_market
    + _place_tpsl para no perder la entrada.
  - _place_tpsl se mantiene para TP2/TP3 y re-colocación de stops.

v13 — Fix #4 (2026-06-06):
  - _release_pretrade_margin: elimina kwarg redundante notional_or_margin=0.0.

v12 — Fix _fetch_candles() BingX klines v3 (2026-06-06).
v11 — Migración OKX → BingX (2026-06-06).
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

_USE_TESTNET = os.getenv("BINGX_TESTNET", "false").lower() in ("true", "1", "yes")

_OHLCV_BARS             = int(os.getenv("BARS_NEEDED",            "100"))
_PRICE_FETCH_RETRIES    = int(os.getenv("PRICE_FETCH_RETRIES",    "3"))
_SET_LEVERAGE_TIMEOUT_S = float(os.getenv("SET_LEVERAGE_TIMEOUT_S", "15"))

# Mapa de timeframe → intervalo BingX (coincide con parámetro "interval")
_TF_BINGX = {
    "1m":  "1m",  "3m":  "3m",  "5m":  "5m",  "15m": "15m",
    "30m": "30m", "1h":  "1h",  "2h":  "2h",  "4h":  "4h",
    "6h":  "6h",  "8h":  "8h",  "12h": "12h", "1d":  "1d",
    "1w":  "1w",  "1M":  "1M",
}

_FILL_RETRIES        = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))
_FILL_DELAY          = float(os.getenv("POST_FILL_CONFIRM_DELAY", "2.0"))
_MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "3.0")) / 100.0

_BASE_URL = (
    "https://open-api-vst.bingx.com"
    if _USE_TESTNET
    else "https://open-api.bingx.com"
)


def _to_inst_id(symbol: str) -> str:
    """Convierte 'BTC' o 'BTC/USDT:USDT' → 'BTC-USDT'."""
    s = symbol.upper()
    for rm in ("/USDT:USDT", "-USDT-SWAP", "/USDT"):
        s = s.replace(rm, "")
    base = s.split("-")[0]
    return f"{base}-USDT"


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
            f"({entry_signal:.4f}) — supera \u00b1{threshold*200:.1f}% — entrada cancelada"
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
    """Orquestador principal de un par de trading en BingX (perpetuos USDT)."""

    def __init__(
        self,
        api_key: Optional[str],
        api_secret: str,
        passphrase: Optional[str] = None,   # ignorado en BingX (sin passphrase)
        symbol: str = "BTC",
        leverage: int = 5,
        margin_mode: str = "isolated",
        dry_run: bool = True,
    ) -> None:
        self.symbol      = symbol
        self.inst_id     = _to_inst_id(symbol)   # "BTC-USDT"
        self.coin        = symbol.upper().split("-")[0]
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
        self._instrument_unavailable: bool   = False

        self._api_key    = api_key    or os.getenv("BINGX_API_KEY",    "")
        self._api_secret = api_secret or os.getenv("BINGX_API_SECRET", "")

        self._bingx_client = None

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)

    # ── Interfaz pública ──────────────────────────────────────────────────────────────────

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

    # ── Init BingX ─────────────────────────────────────────────────────────

    async def _get_ccxt(self) -> None:
        if self._bingx_client is not None:
            return
        try:
            from bot.core.bingx_client import BingXClient
            logger.info("[%s] Inicializando BingXClient (inst=%s, testnet=%s)…",
                        self.symbol, self.inst_id, _USE_TESTNET)
            self._bingx_client = await BingXClient.create(self.symbol)
            logger.info("[%s] BingXClient listo.", self.symbol)
        except Exception as e:
            logger.error("[%s] _get_ccxt error: %s", self.symbol, e)
            raise

    @property
    def _okx_client(self):
        """Alias de compatibilidad: devuelve el BingXClient."""
        return self._bingx_client

    # ── get_price ───────────────────────────────────────────────────────────────────

    async def get_price(self) -> float:
        if self._bingx_client is None:
            if self._last_price > 0:
                return self._last_price
            raise RuntimeError(f"[{self.symbol}] get_price: BingXClient no inicializado")

        if self._instrument_unavailable:
            raise RuntimeError(
                f"[{self.symbol}] get_price: instrumento {self.inst_id} "
                f"no disponible en BingX {'testnet' if _USE_TESTNET else 'live'} — skip"
            )

        last_exc: Exception | None = None
        delays = [0.4 * (2 ** i) for i in range(_PRICE_FETCH_RETRIES)]

        for attempt, delay in enumerate(delays):
            try:
                import requests as _req
                resp = await asyncio.to_thread(
                    lambda: _req.get(
                        f"{_BASE_URL}/openApi/swap/v2/quote/ticker",
                        params={"symbol": self.inst_id},
                        timeout=8,
                    ).json()
                )
                data = resp.get("data", {})
                if isinstance(data, list):
                    data = data[0] if data else {}
                if not data:
                    self._instrument_unavailable = True
                    raise ValueError(
                        f"{self.inst_id} no disponible en BingX "
                        f"{'testnet' if _USE_TESTNET else 'live'} (data vacía)"
                    )
                last_p = float(data.get("lastPrice") or data.get("price") or 0)
                bid    = float(data.get("bidPrice") or 0)
                ask    = float(data.get("askPrice") or 0)
                price  = (bid + ask) / 2 if bid > 0 and ask > 0 else last_p
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

    # ── _set_leverage ─────────────────────────────────────────────────────────────────

    async def _set_leverage(self, leverage: int) -> None:
        if self.dry_run:
            logger.info("[%s] [DRY-RUN] _set_leverage(%dx)", self.symbol, leverage)
            self._open_leverage = leverage
            return
        if self._bingx_client is None:
            logger.warning("[%s] _set_leverage: BingXClient no inicializado — skip.", self.symbol)
            return
        is_cross = self.margin_mode == "cross"
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    self._bingx_client.set_leverage,
                    coin=self.coin,
                    leverage=leverage,
                    is_cross=is_cross,
                ),
                timeout=_SET_LEVERAGE_TIMEOUT_S,
            )
            self._open_leverage = leverage
            logger.info("[%s] _set_leverage: leverage configurado a %dx en BingX.",
                        self.symbol, leverage)
        except asyncio.TimeoutError:
            logger.warning("[%s] _set_leverage timeout (%ss)",
                           self.symbol, _SET_LEVERAGE_TIMEOUT_S)
        except Exception as e:
            logger.warning("[%s] _set_leverage error: %s", self.symbol, e)

    # ── OHLCV ───────────────────────────────────────────────────────────────────

    async def get_ohlcv(self, timeframe: str, n: Optional[int] = None) -> list:
        bars_needed = n or _OHLCV_BARS
        async def _fetch(tf: str) -> list:
            return await self._fetch_candles(tf, bars_needed)
        return await ohlcv_cache.get(self.coin, timeframe, _fetch)

    def get_ohlcv_fn(self) -> Callable:
        return functools.partial(self.get_ohlcv)

    # ── _fetch_candles ────────────────────────────────────────────────────────────────────

    async def _fetch_candles(self, timeframe: str, n: int) -> list:
        """
        Descarga velas OHLCV de BingX usando el endpoint de klines v3.

        Formato de respuesta BingX klines v3:
          { "code": 0, "data": [ {"open": "...", "high": "...", "low": "...",
            "close": "...", "volume": "...", "time": 1234567890000}, ... ] }
        """
        interval = _TF_BINGX.get(timeframe, timeframe)
        limit    = min(n, 1000)

        try:
            import requests as _req
            resp = await asyncio.to_thread(
                lambda: _req.get(
                    f"{_BASE_URL}/openApi/swap/v3/quote/klines",
                    params={
                        "symbol":   self.inst_id,
                        "interval": interval,
                        "limit":    str(limit),
                    },
                    timeout=10,
                ).json()
            )
        except Exception as e:
            raise ValueError(f"_fetch_candles HTTP error {self.symbol}/{timeframe}: {e}") from e

        code = resp.get("code", 0)
        if code != 0:
            msg = resp.get("msg", "unknown error")
            raise ValueError(
                f"_fetch_candles BingX error {self.symbol}/{timeframe}: "
                f"code={code} msg={msg}"
            )

        raw = resp.get("data") or []
        if not raw:
            raise ValueError(
                f"_fetch_candles: BingX devolvió data vacía "
                f"para {self.inst_id}/{interval}"
            )

        result = []
        for c in raw:
            try:
                if isinstance(c, dict):
                    result.append([
                        int(c.get("time", c.get("openTime", 0))),
                        float(c["open"]),
                        float(c["high"]),
                        float(c["low"]),
                        float(c["close"]),
                        float(c.get("volume", 0)),
                    ])
                elif isinstance(c, (list, tuple)) and len(c) >= 6:
                    result.append([
                        int(c[0]),
                        float(c[1]),
                        float(c[2]),
                        float(c[3]),
                        float(c[4]),
                        float(c[5]),
                    ])
            except (KeyError, IndexError, TypeError, ValueError):
                continue

        if not result:
            raise ValueError(
                f"_fetch_candles: no se pudieron parsear candles "
                f"para {self.inst_id}/{interval} (raw[0]={raw[0] if raw else 'N/A'})"
            )

        result.sort(key=lambda x: x[0])
        return result

    # ── _get_positions ───────────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list[dict]:
        if self._bingx_client is None:
            return []
        try:
            return await asyncio.to_thread(self._bingx_client.get_positions)
        except Exception as e:
            logger.warning("[%s] _get_positions error: %s", self.symbol, e)
            return []

    async def _get_open_orders_raw(self) -> list[dict]:
        if self._bingx_client is None:
            return []
        try:
            return await asyncio.to_thread(self._bingx_client.get_open_orders)
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e)
            return []

    async def _get_open_trigger_orders_raw(self) -> list[dict]:
        return await self._get_open_orders_raw()

    # ── _place_tpsl ──────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> None:
        """
        Coloca SL y TP como órdenes independientes (place_sl / place_tp).

        Uso principal:
          - Recolocar SL/TP tras un parcial (TP2, TP3).
          - Fallback si place_market_with_tpsl falla.
          - open_order ya NO lo llama en el caso normal (usa place_market_with_tpsl).

        Para nuevas entradas, preferir place_market_with_tpsl() que adjunta
        SL+TP de forma atómica a la orden MARKET (Fix #9 bingx_client v6).
        """
        if self._bingx_client is None:
            logger.error("[%s] _place_tpsl: BingXClient no inicializado", self.symbol)
            return
        entry_px = self.entry_price

        if sl_price and sl_price > 0:
            try:
                result = await asyncio.to_thread(
                    self._bingx_client.place_sl,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=sl_price,
                    entry_px=entry_px,
                )
                if result and result.get("code") == "0":
                    logger.info("[%s] _place_tpsl: SL=%.5f colocado en BingX.", self.symbol, sl_price)
                else:
                    err = (result or {}).get("msg", "error")
                    logger.error("[%s] _place_tpsl: SL rechazado por BingX: %s", self.symbol, err)
            except Exception as e:
                logger.error("[%s] _place_tpsl: SL exception: %s", self.symbol, e)
                raise

        if tp_price and tp_price > 0:
            try:
                result = await asyncio.to_thread(
                    self._bingx_client.place_tp,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=tp_price,
                    limit_px=tp_price,
                    entry_px=entry_px,
                )
                if result and result.get("code") == "0":
                    logger.info("[%s] _place_tpsl: TP=%.5f colocado en BingX.", self.symbol, tp_price)
                else:
                    err = (result or {}).get("msg", "error")
                    logger.error("[%s] _place_tpsl: TP rechazado por BingX: %s", self.symbol, err)
            except Exception as e:
                logger.error("[%s] _place_tpsl: TP exception: %s", self.symbol, e)
                raise

    # ── open_order ────────────────────────────────────────────────────────────────

    async def open_order(self, signal: dict, risk) -> None:
        """
        Abre una posición en BingX.

        v15: llama _set_leverage(leverage) antes de enviar la orden de mercado
        para que BingX aplique el leverage correcto configurado en LEVERAGE.
        Sin esta llamada BingX usaba el leverage por defecto del contrato (5x).

        v14: usa place_market_with_tpsl() — adjunta SL+TP1 a la orden MARKET
        en una única llamada API (atómica). Si falla, fallback automático a
        place_market() + _place_tpsl() para no perder la entrada.
        """
        is_long = signal.get("side") == "long"
        action  = signal.get("action", "BUY" if is_long else "SELL")

        if self._bingx_client is None:
            try:
                await self._get_ccxt()
            except Exception as e:
                logger.error("[%s] open_order: no se pudo inicializar BingXClient: %s",
                             self.symbol, e)
                self._release_pretrade_margin()
                return

        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio: %s", self.symbol, e)
            self._release_pretrade_margin()
            return

        stale_reason = _check_price_staleness(signal, ref_price, is_long)
        if stale_reason:
            logger.warning("[%s] open_order cancelado: %s", self.symbol, stale_reason)
            self._release_pretrade_margin()
            return

        usdc_per_trade = float(getattr(risk, "usdc_per_trade", 0) or 0)
        leverage       = self.leverage
        notional_usdc  = usdc_per_trade * leverage

        ct_val = self._bingx_client.get_ct_val()
        sz_dec = self._bingx_client.get_sz_decimals()

        if ref_price > 0 and ct_val > 0:
            raw_qty = notional_usdc / (ref_price * ct_val)
        else:
            logger.error("[%s] open_order: precio o ctVal inválido (price=%.4f ctVal=%.6f)",
                         self.symbol, ref_price, ct_val)
            self._release_pretrade_margin()
            return

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
            self._release_pretrade_margin()
            return

        logger.info(
            "[%s] open_order: %s | qty=%.6f | notional=%.2fUSDC | "
            "lev=%dx | price=%.4f | dry_run=%s",
            self.symbol, action, qty, usdc_per_trade, leverage,
            ref_price, self.dry_run,
        )

        # ── Aplicar leverage en BingX ANTES de enviar la orden ───────────────────
        await self._set_leverage(leverage)

        # Calcular SL/TP1 preliminares (se reajustan tras confirmar fill)
        sl_px_pre  = float(signal.get("sl")  or 0)
        tp1_px_pre = float(signal.get("tp1") or 0)

        filled_price = ref_price

        if self.dry_run:
            logger.info("[%s] [DRY-RUN] open_order: simulando place_market %s %.6f @ %.4f",
                        self.symbol, action, qty, ref_price)
        else:
            # ── Intento atómico: MARKET + SL + TP en una sola llamada ────────────────
            atomic_ok = False
            try:
                place_resp = await asyncio.to_thread(
                    self._bingx_client.place_market_with_tpsl,
                    is_buy=is_long,
                    sz=qty,
                    sl_px=sl_px_pre  if sl_px_pre  > 0 else None,
                    tp_px=tp1_px_pre if tp1_px_pre > 0 else None,
                    ref_price=ref_price,
                )
                code = (place_resp or {}).get("code", "-1")
                if str(code) == "0":
                    logger.info(
                        "[%s] open_order: place_market_with_tpsl OK (atómico, code=0)",
                        self.symbol,
                    )
                    atomic_ok = True
                else:
                    msg = (place_resp or {}).get("msg", str(place_resp))
                    logger.warning(
                        "[%s] open_order: place_market_with_tpsl rechazado (%s) "
                        "— reintentando con place_market + _place_tpsl",
                        self.symbol, msg,
                    )
            except Exception as e:
                logger.warning(
                    "[%s] open_order: place_market_with_tpsl excepción (%s) "
                    "— reintentando con place_market + _place_tpsl",
                    self.symbol, e,
                )

            # ── Fallback: place_market separado si el atómico falló ────────────────
            if not atomic_ok:
                try:
                    place_resp = await asyncio.to_thread(
                        self._bingx_client.place_market,
                        is_buy=is_long,
                        sz=qty,
                        reduce_only=False,
                        ref_price=ref_price,
                    )
                    code = (place_resp or {}).get("code", "-1")
                    if str(code) != "0":
                        msg = (place_resp or {}).get("msg", str(place_resp))
                        logger.error("[%s] open_order: place_market rechazado por BingX: %s",
                                     self.symbol, msg)
                        self._release_pretrade_margin()
                        return
                    logger.info("[%s] open_order: place_market (fallback) OK (code=0)", self.symbol)
                except Exception as e:
                    logger.error("[%s] open_order: place_market excepción: %s", self.symbol, e)
                    self._release_pretrade_margin()
                    return

            # ── Confirmar fill leyéndolo de posiciones activas ────────────────────
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

        # ── Reajustar SL/TP al precio real de fill ───────────────────────────────
        sl_px, tp1_px, tp2_px = _adjust_levels_to_fill(signal, filled_price, ref_price)
        tp3_px = float(signal.get("tp3") or 0)
        if tp3_px > 0 and abs(filled_price - ref_price) / max(ref_price, 1e-9) >= 0.0005:
            base = float(signal.get("entry") or 0) or ref_price
            tp3_px = filled_price * (1.0 + (tp3_px - base) / base) if base > 0 else tp3_px

        if not self.dry_run:
            # Si el atómico fue exitoso pero el fill cambió los niveles >0.05%,
            # recolocar SL/TP con los niveles ajustados al precio real.
            fill_drift = abs(filled_price - ref_price) / max(ref_price, 1e-9)
            if atomic_ok and fill_drift >= 0.0005:
                logger.info(
                    "[%s] open_order: fill drift %.4f%% — recolocando SL/TP ajustados",
                    self.symbol, fill_drift * 100,
                )
                try:
                    await asyncio.to_thread(
                        self._bingx_client.cancel_all_open_tpsl
                    )
                except Exception as e:
                    logger.warning("[%s] open_order: cancel_tpsl antes de recolocar: %s",
                                   self.symbol, e)
                try:
                    await self._place_tpsl(
                        qty=qty,
                        sl_price=sl_px  if sl_px  > 0 else None,
                        tp_price=tp1_px if tp1_px > 0 else None,
                        is_long=is_long,
                    )
                except Exception as e:
                    logger.error("[%s] open_order: recolocación SL/TP error: %s", self.symbol, e)

            elif not atomic_ok:
                # Fallback: colocar SL/TP como órdenes independientes
                try:
                    await self._place_tpsl(
                        qty=qty,
                        sl_price=sl_px  if sl_px  > 0 else None,
                        tp_price=tp1_px if tp1_px > 0 else None,
                        is_long=is_long,
                    )
                except Exception as e:
                    logger.error("[%s] open_order: _place_tpsl (fallback) error: %s",
                                 self.symbol, e)
        else:
            if sl_px > 0:
                logger.info("[%s] [DRY-RUN] SL simulado @ %.4f", self.symbol, sl_px)
            if tp1_px > 0:
                logger.info("[%s] [DRY-RUN] TP1 simulado @ %.4f", self.symbol, tp1_px)

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
            "[%s] \u2705 Posición abierta en BingX: %s @ %.4f | qty=%.6f | "
            "SL=%.4f | TP1=%.4f | TP2=%.4f",
            self.symbol, self.position.upper(), filled_price, qty,
            self.sl or 0, self.tp1 or 0, self.tp2 or 0,
        )

    # ── _release_pretrade_margin ─────────────────────────────────────────────────

    def _release_pretrade_margin(self) -> None:
        """
        Libera el margen reservado por pretrade_risk cuando la orden no se ejecuta.
        register_close_safe(symbol) extrae el margen exacto desde
        _open_margin_by_symbol[symbol] internamente.
        """
        try:
            from bot.pretrade_risk import pretrade_risk
            pretrade_risk.register_close_safe(self.symbol)
            logger.info(
                "[%s] _release_pretrade_margin: margen liberado (orden no ejecutada).",
                self.symbol,
            )
        except Exception as e:
            logger.warning(
                "[%s] _release_pretrade_margin: error al liberar margen: %s",
                self.symbol, e,
            )

    # ── close_position ───────────────────────────────────────────────────────────────────

    async def close_position(self, reason: str = "manual") -> None:
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
            if self._bingx_client is not None:
                try:
                    cancelled = await asyncio.to_thread(
                        self._bingx_client.cancel_all_open_tpsl
                    )
                    if cancelled:
                        logger.info("[%s] close_position: órdenes SL/TP canceladas (batch).",
                                    self.symbol)
                except Exception as e:
                    logger.warning("[%s] close_position: cancel_tpsl error: %s", self.symbol, e)

            try:
                close_ref_price = await self.get_price()
            except Exception as e:
                logger.warning(
                    "[%s] close_position: no se pudo obtener precio (%s) — usando %.4f",
                    self.symbol, e, self._last_price,
                )
                close_ref_price = self._last_price

            if self._bingx_client is None:
                logger.error("[%s] close_position: BingXClient no disponible.", self.symbol)
            else:
                try:
                    resp = await asyncio.to_thread(
                        self._bingx_client.place_market,
                        is_buy=not is_long,
                        sz=qty,
                        reduce_only=True,
                        ref_price=close_ref_price,
                    )
                    code = (resp or {}).get("code", "-1")
                    if str(code) == "0":
                        logger.info("[%s] close_position: market close OK en BingX.", self.symbol)
                    else:
                        msg = (resp or {}).get("msg", str(resp))
                        logger.error("[%s] close_position: rechazado por BingX: %s", self.symbol, msg)
                except Exception as e:
                    logger.error("[%s] close_position: excepción: %s", self.symbol, e)
        else:
            if self.dry_run:
                logger.info("[%s] [DRY-RUN] close_position simulado.", self.symbol)
            elif qty <= 0:
                logger.warning("[%s] close_position: qty=0, skip market order.", self.symbol)

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
