#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

FIX KELLY (#2 2026-06-03):
  open_order ahora aplica kelly_multiplier() al usdc_per_trade base.

FIX FREEZE (2026-06-03):
  __init__: _hl_client = None. Creación real en _get_ccxt() (async).

FIX DEADLOCK (2026-06-03):
  Todas las llamadas al SDK síncrono se envuelven en asyncio.to_thread().

FIX get_price (2026-06-05):
  Implementado con retry + stale fallback.

FIX _set_leverage (2026-06-05):
  Corregido a HLClient.set_leverage(coin, leverage).

FIX OHLCV semáforo → ohlcv_cache (2026-06-05):
  get_ohlcv() delega en ohlcv_cache.get().

FIX get_ohlcv_fn ausente (2026-06-05):
  Añadido get_ohlcv_fn().

FIX HLClient.create() firma (2026-06-05):
  HLClient.create(self.coin) — sin api_key/api_secret.

FIX _get_positions ausente (2026-06-05):
  Implementado delegando en HLClient.get_positions().

FIX semáforo info — 429 masivos (2026-06-05):
  CAUSA RAÍZ: 12 traders × (get_price + _get_positions) = 24 requests SDK
  simultáneas a HL sin semáforo → CloudFront devuelve 429 en cascada.
  SOLUCIÓN: get_price() y _get_positions() adquieren
  _HLCore.get_info_semaphore() (Semaphore(HL_INFO_CONCURRENCY, default 4))
  antes de cualquier llamada a self._info.* vía asyncio.to_thread().
  Los reintentos de get_price también están dentro del semáforo para que
  el backoff no libere el slot prematuramente.
"""
from __future__ import annotations

import asyncio
import functools
import logging
import os
import time
from typing import Callable, Optional

import aiohttp

from bot.core.hl_client import HLClient, _HLCore, _norm_coin
from bot.core.trading_loop import TradingLoop
from bot.ohlcv_cache import ohlcv_cache
from bot.state import save_position

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

_OHLCV_BARS             = int(os.getenv("BARS_NEEDED",            "100"))
_OHLCV_MAX_CONCURRENCY  = int(os.getenv("OHLCV_MAX_CONCURRENCY",  "3"))
_PRICE_FETCH_RETRIES    = int(os.getenv("PRICE_FETCH_RETRIES",    "3"))
_SET_LEVERAGE_TIMEOUT_S = float(os.getenv("SET_LEVERAGE_TIMEOUT_S", "15"))

_TF_MINUTES = {
    "1m":  1,  "3m":  3,  "5m":  5,  "15m": 15,
    "30m": 30, "1h":  60, "2h":  120, "4h":  240,
    "8h":  480, "1d": 1440,
}

_FILL_RETRIES        = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))
_FILL_DELAY          = float(os.getenv("POST_FILL_CONFIRM_DELAY", "2.0"))
_MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "3.0")) / 100.0


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
            f"⚠️ Precio actual ({ref_price:.4f}) se alejó {drift*100:+.2f}% del entry "
            f"({entry_signal:.4f}) — supera ±{threshold*200:.1f}% — entrada cancelada"
        )
    if abs_drift <= threshold:
        return None
    if is_long:
        if drift > 0:
            return f"⏫ [LONG] precio {ref_price:.4f} subió {drift*100:+.2f}% — demasiado caro, cancelado"
        return f"⏪ [LONG] precio {ref_price:.4f} cayó {drift*100:+.2f}% — setup roto, cancelado"
    else:
        if drift < 0:
            return f"⏪ [SHORT] precio {ref_price:.4f} bajó {drift*100:+.2f}% — cancelado"
        return f"⏫ [SHORT] precio {ref_price:.4f} subió {drift*100:+.2f}% — setup roto, cancelado"


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
    """Orquestador principal de un par de trading en Hyperliquid."""

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
        self.coin        = _norm_coin(symbol)
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

        self._api_key    = api_key or ""
        self._api_secret = api_secret or ""

        self._hl_client:   Optional[HLClient] = None
        self._master_addr: str  = ""
        self._agent_mode:  bool = False

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)
        self._ccxt_exchange  = None

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

    # ── _get_ccxt ─────────────────────────────────────────────────

    async def _get_ccxt(self) -> None:
        if self._hl_client is not None:
            return
        try:
            logger.info("[%s] Inicializando HLClient (async)…", self.symbol)
            self._hl_client   = await HLClient.create(self.coin)
            self._master_addr = getattr(self._hl_client, "_account_addr", "") or ""
            self._agent_mode  = getattr(self._hl_client, "_agent_mode",   False)
            logger.info(
                "[%s] HLClient listo — addr=%s agent=%s",
                self.symbol,
                (self._master_addr[:10] + "…") if self._master_addr else "N/A",
                self._agent_mode,
            )
        except Exception as e:
            logger.error("[%s] _get_ccxt error: %s", self.symbol, e)
            raise

    # ── get_price ─────────────────────────────────────────────────

    async def get_price(self) -> float:
        """
        Precio mid via allMids.

        FIX 429 (2026-06-05): adquiere _HLCore.get_info_semaphore() antes
        de cada llamada SDK. Los reintentos están dentro del semáforo para
        no liberar el slot durante el backoff.
        """
        if self._hl_client is None:
            if self._last_price > 0:
                return self._last_price
            raise RuntimeError(f"[{self.symbol}] get_price: _hl_client no inicializado")

        last_exc: Exception | None = None
        delays = [0.4 * (2 ** i) for i in range(_PRICE_FETCH_RETRIES)]

        sem = _HLCore.get_info_semaphore()
        async with sem:
            for attempt, delay in enumerate(delays):
                try:
                    data = await asyncio.to_thread(self._hl_client._info.all_mids)
                    if not isinstance(data, dict):
                        raise ValueError(f"all_mids devolvió {type(data).__name__}: {str(data)[:80]}")
                    raw = data.get(self.coin)
                    if raw is None:
                        raise ValueError(f"coin '{self.coin}' no encontrado en allMids")
                    price = float(raw)
                    self._last_price = price
                    return price
                except Exception as exc:
                    last_exc = exc
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
        if self._hl_client is None:
            logger.warning("[%s] _set_leverage: _hl_client no inicializado — skip.", self.symbol)
            return
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._hl_client.set_leverage, self.coin, leverage),
                timeout=_SET_LEVERAGE_TIMEOUT_S,
            )
            self._open_leverage = leverage
            logger.info("[%s] Apalancamiento configurado: %dx", self.symbol, leverage)
        except asyncio.TimeoutError:
            logger.warning("[%s] _set_leverage timeout (%ss)", self.symbol, _SET_LEVERAGE_TIMEOUT_S)
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
        """
        Posiciones abiertas para self.coin.

        FIX 429 (2026-06-05): adquiere _HLCore.get_info_semaphore() antes
        de llamar user_state() vía get_positions() del SDK.
        """
        if self._hl_client is None:
            return []

        sem = _HLCore.get_info_semaphore()
        try:
            async with sem:
                raw_positions = await asyncio.to_thread(self._hl_client.get_positions)
        except Exception as e:
            logger.warning("[%s] _get_positions fetch error: %s", self.symbol, e)
            return []

        result = []
        for p in raw_positions:
            pos = p.get("position", {})
            szi = float(pos.get("szi", 0))
            if szi == 0:
                continue
            result.append({
                "side":    "long" if szi > 0 else "short",
                "entryPx": float(pos.get("entryPx") or 0),
                "size":    abs(szi),
            })
        return result

    # ── _fetch_candles ────────────────────────────────────────────

    async def _fetch_candles(self, timeframe: str, n: int) -> list:
        """HTTP directo a candleSnapshot — sin retry ni semáforo (ohlcv_cache lo gestiona)."""
        tf_min   = _TF_MINUTES.get(timeframe, 15)
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - n * tf_min * 60 * 1000

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin":      self.coin,
                "interval":  timeframe,
                "startTime": start_ms,
                "endTime":   end_ms,
            },
        }

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{_API_URL}/info", json=payload) as resp:
                raw = await resp.json(content_type=None)

        if not isinstance(raw, list):
            logger.warning(
                "[%s] candleSnapshot/%s: respuesta no-lista (%s)",
                self.coin, timeframe, str(raw)[:120],
            )
            return []

        result = []
        for c in raw:
            try:
                result.append([
                    int(c["t"]), float(c["o"]), float(c["h"]),
                    float(c["l"]), float(c["c"]), float(c["v"]),
                ])
            except (KeyError, TypeError, ValueError):
                continue
        return result
