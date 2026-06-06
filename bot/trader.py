#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

v16 — Fix #14 leverage cap interno (2026-06-06):
  - _set_leverage() ahora consulta _bingx_client.get_max_leverage() antes
    de enviar la llamada al exchange. Si el valor configurado supera el
    máximo real del par, se capea en silencio (log INFO). Elimina el error
    "Invalid leverage value" que ocurría cuando LEVERAGE > max del par.

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

        # Fix #16: flag que indica que hay una orden en vuelo (open_order en ejecución).
        # _stop_pair_with_cleanup() en main.py espera a que baje antes de cancelar la tarea.
        self._pending_order: bool = False

        # Fix #15: expuesto para que _idle_rotation_loop pueda rotarlo inmediatamente.
        self._force_idle_rotate: bool = False

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
        """
        Fix #14: antes de llamar al exchange, consulta el leverage máximo
        real del par y capea el valor. Evita el error "Invalid leverage value"
        cuando LEVERAGE > max permitido por BingX para ese símbolo.
        """
        if self.dry_run:
            logger.info("[%s] [DRY-RUN] _set_leverage(%dx)", self.symbol, leverage)
            self._open_leverage = leverage
            return
        if self._bingx_client is None:
            logger.warning("[%s] _set_leverage: BingXClient no inicializado — skip.", self.symbol)
            return

        # ── Fix #14: capping interno ─────────────────────────────────────
        effective_leverage = leverage
        try:
            if hasattr(self._bingx_client, "get_max_leverage"):
                max_lev = await asyncio.to_thread(
                    self._bingx_client.get_max_leverage, self.inst_id
                )
                if max_lev and isinstance(max_lev, int) and max_lev > 0:
                    if leverage > max_lev:
                        logger.info(
                            "[%s] ⚙️  Leverage %dx supera el máximo del par (%dx) — capando a %dx.",
                            self.symbol, leverage, max_lev, max_lev,
                        )
                        effective_leverage = max_lev
        except Exception as e:
            logger.debug(
                "[%s] _set_leverage: no se pudo consultar get_max_leverage (%s) — "
                "usando leverage configurado %dx sin verificar.",
                self.symbol, e, leverage,
            )
        # ─────────────────────────────────────────────────────────────────

        is_cross = self.margin_mode == "cross"
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    self._bingx_client.set_leverage,
                    coin=self.coin,
                    leverage=effective_leverage,
                    is_cross=is_cross,
                ),
                timeout=_SET_LEVERAGE_TIMEOUT_S,
            )
            self._open_leverage = effective_leverage
            logger.info(
                "[%s] Leverage establecido: %dx (solicitado: %dx, mode=%s)",
                self.symbol, effective_leverage, leverage, self.margin_mode,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "[%s] _set_leverage timeout (%.0fs) — continuando con leverage previo.",
                self.symbol, _SET_LEVERAGE_TIMEOUT_S,
            )
        except Exception as e:
            logger.warning(
                "[%s] _set_leverage error (%s) — continuando sin cambiar leverage.",
                self.symbol, e,
            )
