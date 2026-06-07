#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

v22 — Fix Bug Crítico 1: _do_open_order() ahora llama pretrade_risk.confirm_order() (2026-06-07):
  - Tras confirmar fill y persistir estado, se registra el margen en PreTradeRisk
    usando signal._confirm_margin (o usdc_per_trade como fallback).
  - Sin este registro, _open_margin se queda en 0.0 → Gate 2 (límite 500 USDC) nunca
    se alcanza y register_close_safe() libera margen que nunca fue registrado.

v21 — Fix Bug Menor 4: guardia dura SL/TP + cierre de emergencia si SL falla (2026-06-07):
  - _confirm_margin(): aborta la apertura si falta SL o TP antes de tocar el exchange.
    Espeja la guardia dura de ExecutionEngine.execute() (trade_side=="open").
  - _place_tpsl() ahora devuelve bool: True si el SL fue colocado, False si falló.
    Era void → el camino fallback (place_market + _place_tpsl) nunca podía detectar
    que el SL no se colocó.
  - _do_open_order() fallback: si _place_tpsl() devuelve False, cierra la posición
    recién abierta con close_position(reason="NO_SL") y aborta sin guardar estado.
    Espeja ExecutionEngine._place_tpsl() → execute() cierre de emergencia.
  - TODO (deuda técnica): migrar open_order() para delegar a
    execution_engine.execute() como único punto de entrada, eliminando la
    dualidad place_market_with_tpsl / place_market + _place_tpsl.

v20 — Fix 4 bugs internos (2026-06-07):
  1. _set_leverage: get_max_leverage() no acepta argumentos — se llamaba
     get_max_leverage(self.inst_id) → TypeError. Corregido a get_max_leverage().
  2. _get_positions: reemplazado HMAC manual duplicado por
     BingXClient.get_positions() (ya probado y correcto).
  3. _get_open_orders_raw: reemplazado HMAC manual duplicado por
     BingXClient.get_open_orders() (incluye TP/SL algo orders, fix #22).
  4. _get_open_trigger_orders_raw: reemplazado HMAC manual duplicado por
     BingXClient.get_open_orders() filtrando por tipo trigger/algo.

v19 — Fix CRÍTICO: corregir kwargs qty= → sz= en llamadas a BingXClient (2026-06-07):
  - BingXClient.place_market firma usa `sz` (no `qty`).
  - BingXClient.place_market_with_tpsl firma usa `sz`, `sl_px`, `tp_px` (no `qty`, `sl_price`, `tp_price`).
  - trader.py llamaba todos estos métodos con kwargs incorrectos → TypeError en cada orden.
  - TODAS las órdenes fallaban silenciosamente (logs: "got an unexpected keyword argument 'qty'").
  - Fix: renombrar kwargs en _do_open_order() y close_position() para que coincidan
    con la firma real de BingXClient.

v18 — Fix CRÍTICO: añade open_order() (2026-06-07):
  - trading_loop.py llama a trader.open_order(signal, risk) en línea 553
    pero el método no existía → AttributeError bloqueaba TODA ejecución de órdenes.
  - open_order() implementa el flujo completo:
      1. Extrae side, entry, sl, tp1/tp2/tp3 del signal dict.
      2. Calcula qty = (usdc_per_trade * leverage) / entry_price.
      3. Comprueba staleness del precio (reutiliza _check_price_staleness).
      4. En dry_run: simula apertura y actualiza estado en memoria.
      5. En live: llama _set_leverage(), luego place_market_with_tpsl()
         (atómico); si falla, fallback a place_market() + _place_tpsl().
      6. Confirma fill con get_price() (retries), ajusta SL/TP al precio
         real de fill (_adjust_levels_to_fill), persiste en state.py.
      7. Coloca TP2/TP3 con _place_tpsl() si vienen en el signal.
      8. Sets self._pending_order = True/False como bracket para que
         _stop_pair_with_cleanup() en main.py espere el vuelo.

v17 — Fix get_ohlcv_fn + _get_positions (2026-06-07).
v16 — Fix #14 leverage cap interno (2026-06-06).
v15 — Fix leverage no aplicado (2026-06-06).
v14 — open_order atómico con place_market_with_tpsl (2026-06-06).
v13 — Fix #4 (2026-06-06).
v12 — Fix _fetch_candles() BingX klines v3 (2026-06-06).
v11 — Migración OKX → BingX (2026-06-06).
"""
from __future__ import annotations

import asyncio
import functools
import logging
import math
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

    # ── get_ohlcv_fn ────────────────────────────────────────────────────────────────────

    def get_ohlcv_fn(self) -> Callable:
        """
        Retorna un callable async que TradingLoop puede invocar para obtener
        datos OHLCV del exchange.

        Uso en TradingLoop:
            ohlcv_fn = trader.get_ohlcv_fn()
            candles  = await ohlcv_fn(timeframe="15m", limit=100)

        Retorna lista de dicts con claves: timestamp, open, high, low, close, volume.
        Si BingXClient no está inicializado, inicializa antes de la primera llamada.
        """
        async def _ohlcv_fn(timeframe: str = "15m", limit: int = _OHLCV_BARS) -> list[dict]:
            if self._bingx_client is None:
                await self._get_ccxt()

            interval = _TF_BINGX.get(timeframe, timeframe)
            try:
                import requests as _req
                resp = await asyncio.to_thread(
                    lambda: _req.get(
                        f"{_BASE_URL}/openApi/swap/v3/quote/klines",
                        params={
                            "symbol": self.inst_id,
                            "interval": interval,
                            "limit": limit,
                        },
                        timeout=10,
                    ).json()
                )
                raw = resp.get("data") or []
                candles: list[dict] = []
                for bar in raw:
                    candles.append({
                        "timestamp": int(bar.get("time") or bar.get("t") or 0),
                        "open":      float(bar.get("open")  or bar.get("o") or 0),
                        "high":      float(bar.get("high")  or bar.get("h") or 0),
                        "low":       float(bar.get("low")   or bar.get("l") or 0),
                        "close":     float(bar.get("close") or bar.get("c") or 0),
                        "volume":    float(bar.get("volume") or bar.get("v") or 0),
                    })
                logger.debug(
                    "[%s] get_ohlcv_fn: %d barras (%s) recibidas.",
                    self.symbol, len(candles), timeframe,
                )
                return candles
            except Exception as e:
                logger.warning("[%s] get_ohlcv_fn error (%s) — retornando lista vacía.", self.symbol, e)
                return []

        return _ohlcv_fn

    # ── _get_positions ──────────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list[dict]:
        """
        Consulta las posiciones abiertas en BingX para este símbolo.

        v20 fix: reemplaza la implementación manual con HMAC duplicado por
        BingXClient.get_positions(), que ya tiene la lógica correcta y probada
        (Fix #13, v9: claves compatibles con trading_loop.py).

        Retorna lista de dicts normalizados:
          {
            "symbol":      str,   # p.ej. "BTC-USDT"
            "side":        str,   # "long" | "short"
            "qty":         float, # tamaño en contratos (positivo)
            "entry_price": float,
            "mark_price":  float,
            "pnl":         float, # PnL no realizado en USDT
            "leverage":    int,
            "margin_mode": str,   # "isolated" | "cross"
          }

        Filtra posiciones con qty == 0 (cerradas).
        Retorna [] si el cliente no está listo o hay error de red.
        """
        if self._bingx_client is None:
            try:
                await self._get_ccxt()
            except Exception as e:
                logger.warning("[%s] _get_positions: no se pudo inicializar BingXClient: %s", self.symbol, e)
                return []

        try:
            raw_positions = await asyncio.to_thread(self._bingx_client.get_positions)
            positions: list[dict] = []
            for p in raw_positions:
                qty = abs(float(p.get("size") or p.get("pos") or 0))
                if qty == 0:
                    continue
                side = str(p.get("side") or p.get("posSide") or "").lower()
                positions.append({
                    "symbol":      self.inst_id,
                    "side":        side,
                    "qty":         qty,
                    "entry_price": float(p.get("entryPx") or p.get("avgPx") or 0),
                    "mark_price":  float(p.get("markPx") or 0),
                    "pnl":         float(p.get("upl") or 0),
                    "leverage":    int(float(p.get("lever") or self._open_leverage)),
                    "margin_mode": str(p.get("mgnMode") or "isolated").lower(),
                })
            logger.debug(
                "[%s] _get_positions: %d posición(es) abiertas.",
                self.symbol, len(positions),
            )
            return positions
        except Exception as e:
            logger.warning("[%s] _get_positions error (%s) — retornando [].", self.symbol, e)
            return []

    # ── _set_leverage ─────────────────────────────────────────────────────────────────

    async def _set_leverage(self, leverage: int) -> None:
        """
        Fix #14: antes de llamar al exchange, consulta el leverage máximo
        real del par y capea el valor. Evita el error "Invalid leverage value"
        cuando LEVERAGE > max permitido por BingX para ese símbolo.

        v20 fix: get_max_leverage() no acepta argumentos (solo self).
        Era: self._bingx_client.get_max_leverage(self.inst_id) → TypeError.
        Fix: self._bingx_client.get_max_leverage()
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
                # v20 fix: get_max_leverage() no acepta argumentos
                max_lev = await asyncio.to_thread(
                    self._bingx_client.get_max_leverage
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

    # ── _confirm_margin ────────────────────────────────────────────────────────────────
    # v21 FIX (Bug Menor 4): guardia dura pre-apertura. Espeja ExecutionEngine.execute()
    # trade_side=="open". Llamar ANTES de cualquier llamada al exchange.

    def _confirm_margin(
        self,
        sl_price: Optional[float],
        tp1_price: Optional[float],
    ) -> Optional[str]:
        """
        Verifica que los niveles mínimos de protección estén presentes antes de abrir.
        Devuelve un mensaje de error si falta alguno, None si todo está OK.

        Por qué aquí y no solo en ExecutionEngine:
          - trader.py usa place_market_with_tpsl / place_market + _place_tpsl
            directamente, sin pasar por ExecutionEngine.execute().
          - TODO: cuando se migre a execution_engine.execute() como único punto
            de entrada, esta guardia se puede eliminar (ExecutionEngine ya la tiene).
        """
        missing = []
        if not sl_price or sl_price <= 0:
            missing.append("SL")
        if not tp1_price or tp1_price <= 0:
            missing.append("TP1")
        if missing:
            return (
                f"Apertura bloqueada — faltan: {', '.join(missing)}. "
                f"No se abre ninguna posición sin SL y TP."
            )
        return None

    # ── open_order ────────────────────────────────────────────────────────────────────
    # v18 FIX CRÍTICO: trading_loop.py línea 553 llama trader.open_order(signal, risk).
    # Este método no existía → AttributeError bloqueaba toda ejecución de órdenes.

    async def open_order(self, signal: dict, risk) -> None:
        """
        Abre una posición de mercado en BingX a partir de un signal dict.

        signal esperado:
          {
            "side":   "long" | "short",
            "action": "BUY"  | "SELL",
            "entry":  float,   # precio de referencia de la señal
            "sl":     float,
            "tp1":    float,
            "tp2":    float | None,
            "tp3":    float | None,
            "_confirm_margin": float | None,  # margen a registrar en pretrade_risk
          }

        risk debe tener .usdc_per_trade (float).
        """
        if self.position is not None:
            logger.info(
                "[%s] open_order: ya hay posición abierta (%s) — ignorando señal.",
                self.symbol, self.position,
            )
            return

        self._pending_order = True
        try:
            await self._do_open_order(signal, risk)
        finally:
            self._pending_order = False

    async def _do_open_order(self, signal: dict, risk) -> None:
        """Lógica interna de open_order (separada para el bracket _pending_order)."""

        # ── 1. Extraer parámetros del signal ─────────────────────────────
        raw_side = str(signal.get("side") or signal.get("action") or "").lower()
        is_long  = raw_side in ("long", "buy")
        side_str = "long" if is_long else "short"

        sl_price  = float(signal.get("sl")  or 0) or None
        tp1_price = float(signal.get("tp1") or 0) or None
        tp2_price = float(signal.get("tp2") or 0) or None
        tp3_price = float(signal.get("tp3") or 0) or None

        usdc_per_trade = float(getattr(risk, "usdc_per_trade", 20.0))
        leverage       = int(getattr(risk, "leverage", self.leverage) or self.leverage)

        # ── 2. GUARDIA DURA: SL y TP1 obligatorios antes de tocar el exchange ──
        # v21: espeja ExecutionEngine.execute() trade_side=="open".
        # TODO: eliminar cuando se migre a execution_engine.execute().
        if not self.dry_run:
            guard_err = self._confirm_margin(sl_price, tp1_price)
            if guard_err:
                logger.error("[%s] 🚫 %s", self.symbol, guard_err)
                return

        # ── 3. Obtener precio de referencia ───────────────────────────────
        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio: %s — abortando.", self.symbol, e)
            return

        # ── 4. Validar staleness ──────────────────────────────────────────
        stale_msg = _check_price_staleness(signal, ref_price, is_long)
        if stale_msg:
            logger.warning("[%s] open_order cancelado: %s", self.symbol, stale_msg)
            return

        # ── 5. Calcular qty ───────────────────────────────────────────────
        notional = usdc_per_trade * leverage
        raw_qty  = notional / ref_price
        # Redondear a 4 decimales como mínimo seguro; BingXClient refina si expone sz_decimals
        qty = round(raw_qty, 4)
        if hasattr(self._bingx_client, "get_sz_decimals"):
            try:
                dec = self._bingx_client.get_sz_decimals()
                if dec == 0:
                    qty = float(math.floor(raw_qty))
                else:
                    factor = 10 ** dec
                    qty = math.floor(raw_qty * factor) / factor
            except Exception:
                pass

        if qty <= 0:
            logger.error(
                "[%s] open_order: qty calculado es 0 (usdc=%.2f lev=%dx price=%.4f) — abortando.",
                self.symbol, usdc_per_trade, leverage, ref_price,
            )
            return

        logger.info(
            "[%s] 🚀 open_order: %s | price=%.4f | qty=%.6f | "
            "notional=%.2f USDC | lev=%dx | SL=%.4f | TP1=%.4f",
            self.symbol, side_str.upper(), ref_price, qty,
            usdc_per_trade, leverage,
            sl_price or 0, tp1_price or 0,
        )

        # ── 6. DRY-RUN: simular apertura ──────────────────────────────────
        if self.dry_run:
            self.position       = side_str
            self.entry_price    = ref_price
            self.sl             = sl_price
            self.tp1            = tp1_price
            self.tp2            = tp2_price
            self.tp3            = tp3_price
            self._open_qty      = qty
            self._open_notional = usdc_per_trade
            self._open_leverage = leverage
            self._protection_ok = True
            self._tp1_be_done   = False
            save_position(
                self.symbol,
                side=side_str,
                entry=ref_price,
                sl=sl_price,
                tp1=tp1_price,
                tp2=tp2_price,
                tp3=tp3_price,
                qty=qty,
                usdc_amount=usdc_per_trade,
                leverage=leverage,
            )
            logger.info(
                "[%s] [DRY-RUN] Posición simulada: %s @ %.4f (qty=%.6f)",
                self.symbol, side_str.upper(), ref_price, qty,
            )
            # v22 FIX BUG CRÍTICO 1: registrar margen en pretrade_risk también en dry-run
            # para que Gate 2 funcione correctamente en simulación.
            _confirm_margin_amt = float(signal.get("_confirm_margin") or usdc_per_trade)
            try:
                from bot.pretrade_risk import pretrade_risk as _pt
                _pt.confirm_order(self.symbol, _confirm_margin_amt)
                logger.debug(
                    "[%s] [DRY-RUN] pretrade_risk.confirm_order(%.2f USDC)",
                    self.symbol, _confirm_margin_amt,
                )
            except Exception as _pt_err:
                logger.warning(
                    "[%s] pretrade_risk.confirm_order falló (dry-run): %s",
                    self.symbol, _pt_err,
                )
            return

        # ── 7. LIVE: set leverage + colocar orden ─────────────────────────
        if self._bingx_client is None:
            await self._get_ccxt()

        await self._set_leverage(leverage)

        filled_price: Optional[float] = None

        # v19 FIX: place_market_with_tpsl usa sz= (no qty=) y sl_px=/tp_px= (no sl_price=/tp_price=)
        # Intento atómico: place_market_with_tpsl (SL+TP1 en una sola llamada)
        if (
            hasattr(self._bingx_client, "place_market_with_tpsl")
            and sl_price
            and tp1_price
        ):
            try:
                result = await asyncio.to_thread(
                    self._bingx_client.place_market_with_tpsl,
                    is_long,          # is_buy (posicional)
                    qty,              # sz (posicional) — fix v19: era qty=qty
                    sl_price,         # sl_px (posicional) — fix v19: era sl_price=sl_price
                    tp1_price,        # tp_px (posicional) — fix v19: era tp_price=tp1_price
                )
                if result and result.get("code") in (0, "0", None):
                    logger.info(
                        "[%s] place_market_with_tpsl OK: %s",
                        self.symbol, result,
                    )
                    filled_price = float(
                        (result.get("data") or [{}])[0].get("price")
                        or (result.get("data") or [{}])[0].get("avgPrice")
                        or ref_price
                    )
                    self._protection_ok = True
                else:
                    err = (result or {}).get("msg", "sin respuesta")
                    raise RuntimeError(f"place_market_with_tpsl rechazado: {err}")
            except Exception as e:
                logger.warning(
                    "[%s] place_market_with_tpsl falló (%s) — usando fallback place_market.",
                    self.symbol, e,
                )
                filled_price = None

        # v19 FIX: place_market usa sz= (no qty=)
        # Fallback: place_market separado + _place_tpsl
        if filled_price is None:
            try:
                result = await asyncio.to_thread(
                    self._bingx_client.place_market,
                    is_long,   # is_buy (posicional)
                    qty,       # sz (posicional) — fix v19: era qty=qty
                )
                if result and result.get("code") in (0, "0", None):
                    filled_price = float(
                        (result.get("data") or [{}])[0].get("price")
                        or (result.get("data") or [{}])[0].get("avgPrice")
                        or ref_price
                    )
                    logger.info(
                        "[%s] place_market OK: filled_price=%.4f",
                        self.symbol, filled_price,
                    )
                else:
                    err = (result or {}).get("msg", "sin respuesta")
                    logger.error(
                        "[%s] open_order: place_market rechazado: %s — abortando.",
                        self.symbol, err,
                    )
                    return
            except Exception as e:
                logger.error("[%s] open_order: place_market error: %s — abortando.", self.symbol, e)
                return

            # Confirmar fill con retries
            for attempt in range(_FILL_RETRIES):
                try:
                    confirmed = await self.get_price()
                    if confirmed > 0:
                        filled_price = confirmed
                        break
                except Exception:
                    pass
                await asyncio.sleep(_FILL_DELAY)

            # Colocar SL+TP1 separados
            # v21 FIX (Bug Menor 4): _place_tpsl ahora devuelve bool.
            # Si SL falla → cerrar posición para no quedar expuestos sin stop.
            if sl_price or tp1_price:
                sl_placed = await self._place_tpsl(
                    qty=qty,
                    sl_price=sl_price,
                    tp_price=tp1_price,
                    is_long=is_long,
                    reduce_only=True,
                )
                if sl_placed:
                    self._protection_ok = True
                else:
                    # SL no colocado → cierre de emergencia (espeja ExecutionEngine)
                    logger.error(
                        "[%s] 🚨 SL NO colocado en fallback — cerrando posición "
                        "para evitar exposición sin stop loss.",
                        self.symbol,
                    )
                    try:
                        await self.close_position(reason="NO_SL")
                        logger.warning(
                            "[%s] Posición cerrada preventivamente por falta de SL.",
                            self.symbol,
                        )
                    except Exception as close_exc:
                        logger.critical(
                            "[%s] ❌❌ FALLO CRÍTICO: no se pudo colocar SL NI cerrar la posición: %s",
                            self.symbol, close_exc,
                        )
                    # Notificar por Telegram
                    try:
                        from bot.telegram_bot import send_message
                        await send_message(
                            f"🚨 *ALERTA CRÍTICA* `{self.symbol}`\n"
                            f"SL no pudo colocarse tras el fallback place_market.\n"
                            f"Posición cerrada preventivamente."
                        )
                    except Exception:
                        pass
                    # No guardar estado ni continuar: posición cerrada
                    return

        # ── 8. Ajustar niveles al fill real ───────────────────────────────
        sl_adj, tp1_adj, tp2_adj = _adjust_levels_to_fill(signal, filled_price, ref_price)
        if sl_adj:
            sl_price  = sl_adj
        if tp1_adj:
            tp1_price = tp1_adj
        if tp2_adj and tp2_price:
            tp2_price = tp2_adj

        # ── 9. Actualizar estado ──────────────────────────────────────────
        self.position       = side_str
        self.entry_price    = filled_price
        self.sl             = sl_price
        self.tp1            = tp1_price
        self.tp2            = tp2_price
        self.tp3            = tp3_price
        self._open_qty      = qty
        self._open_notional = usdc_per_trade
        self._open_leverage = leverage
        self._tp1_be_done   = False
        self._last_price    = filled_price

        save_position(
            self.symbol,
            side=side_str,
            entry=filled_price,
            sl=sl_price,
            tp1=tp1_price,
            tp2=tp2_price,
            tp3=tp3_price,
            qty=qty,
            usdc_amount=usdc_per_trade,
            leverage=leverage,
        )

        logger.info(
            "[%s] ✅ Posición abierta: %s @ %.4f | SL=%.4f | TP1=%.4f | qty=%.6f",
            self.symbol, side_str.upper(), filled_price,
            sl_price or 0, tp1_price or 0, qty,
        )

        # ── 9b. v22 FIX BUG CRÍTICO 1: registrar margen en pretrade_risk ─────────────
        # decision_engine.evaluate() pone _confirm_margin en el signal dict.
        # Sin esta llamada, pretrade_risk._open_margin se queda en 0.0 y:
        #   - Gate 2 (límite 500 USDC) nunca se alcanza → el bot puede abrir sin límite.
        #   - register_close_safe() libera margen que nunca fue registrado → estado corrupto.
        _confirm_margin_amt = float(signal.get("_confirm_margin") or usdc_per_trade)
        try:
            from bot.pretrade_risk import pretrade_risk as _pt
            _pt.confirm_order(self.symbol, _confirm_margin_amt)
            logger.info(
                "[%s] pretrade_risk.confirm_order(%.2f USDC) — _open_margin actualizado.",
                self.symbol, _confirm_margin_amt,
            )
        except Exception as _pt_err:
            logger.warning(
                "[%s] pretrade_risk.confirm_order falló: %s — Gate 2 puede no funcionar correctamente.",
                self.symbol, _pt_err,
            )

        # ── 10. Colocar TP2/TP3 adicionales ───────────────────────────────
        if tp2_price:
            try:
                await self._place_tpsl(
                    qty=qty,
                    sl_price=None,
                    tp_price=tp2_price,
                    is_long=is_long,
                    reduce_only=True,
                )
                logger.info("[%s] TP2 colocado: %.4f", self.symbol, tp2_price)
            except Exception as e:
                logger.warning("[%s] TP2 no colocado: %s", self.symbol, e)

        if tp3_price:
            try:
                await self._place_tpsl(
                    qty=qty,
                    sl_price=None,
                    tp_price=tp3_price,
                    is_long=is_long,
                    reduce_only=True,
                )
                logger.info("[%s] TP3 colocado: %.4f", self.symbol, tp3_price)
            except Exception as e:
                logger.warning("[%s] TP3 no colocado: %s", self.symbol, e)

    # ── _place_tpsl ───────────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> bool:
        """
        Coloca SL y/o TP como órdenes separadas usando BingXClient.
        Usado para TP2/TP3, re-colocación de stops y fallback de open_order.

        v21 FIX (Bug Menor 4): ahora devuelve bool.
          - True  → SL colocado con éxito (o no había SL que colocar).
          - False → se intentó colocar SL y falló.
        El llamador (_do_open_order fallback) usa este valor para decidir
        si cerrar la posición de emergencia.
        """
        if self.dry_run:
            logger.info(
                "[%s] [DRY-RUN] _place_tpsl: SL=%.4f TP=%.4f qty=%.6f",
                self.symbol, sl_price or 0, tp_price or 0, qty,
            )
            return True  # dry-run siempre "OK"

        if self._bingx_client is None:
            logger.warning("[%s] _place_tpsl: BingXClient no inicializado — skip.", self.symbol)
            # Si hay SL pendiente, considerar fallo para que el llamador actúe
            return sl_price is None or sl_price <= 0

        sl_placed = True  # optimista: si no hay SL que colocar, no es un fallo

        if sl_price and sl_price > 0:
            sl_placed = False  # ahora sí hay SL que colocar → asumir fallo hasta confirmar
            try:
                result = await asyncio.to_thread(
                    self._bingx_client.place_sl,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=sl_price,
                    entry_px=self.entry_price or sl_price,
                )
                code = (result or {}).get("code", -1)
                if code in (0, "0", None):
                    logger.info("[%s] SL colocado: %.4f", self.symbol, sl_price)
                    sl_placed = True
                else:
                    logger.warning(
                        "[%s] SL rechazado (code=%s): %s",
                        self.symbol, code, (result or {}).get("msg", ""),
                    )
            except Exception as e:
                logger.warning("[%s] _place_tpsl SL error: %s", self.symbol, e)

        if tp_price and tp_price > 0:
            try:
                result = await asyncio.to_thread(
                    self._bingx_client.place_tp,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=tp_price,
                    limit_px=tp_price,
                    entry_px=self.entry_price or tp_price,
                )
                code = (result or {}).get("code", -1)
                if code in (0, "0", None):
                    logger.info("[%s] TP colocado: %.4f", self.symbol, tp_price)
                else:
                    logger.warning(
                        "[%s] TP rechazado (code=%s): %s",
                        self.symbol, code, (result or {}).get("msg", ""),
                    )
            except Exception as e:
                logger.warning("[%s] _place_tpsl TP error: %s", self.symbol, e)

        return sl_placed

    # ── _get_open_orders_raw ──────────────────────────────────────────────────────────

    async def _get_open_orders_raw(self) -> list[dict]:
        """
        Retorna las órdenes pendientes activas para este símbolo desde BingX.
        Usado por PositionManager._ensure_tpsl().

        v20 fix: reemplaza HMAC manual duplicado por BingXClient.get_open_orders()
        que ya incluye tanto órdenes normales como TP/SL standalone (fix #22).
        """
        if self._bingx_client is None:
            return []
        try:
            orders = await asyncio.to_thread(self._bingx_client.get_open_orders)
            return list(orders)
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e)
            return []

    async def _get_open_trigger_orders_raw(self) -> list[dict]:
        """
        Retorna las trigger orders (SL/TP algo-orders) pendientes para este símbolo.
        Usado por PositionManager._ensure_tpsl().

        v20 fix: reemplaza HMAC manual duplicado por BingXClient.get_open_orders()
        filtrando únicamente las órdenes de tipo trigger (STOP_MARKET, TAKE_PROFIT_MARKET).
        """
        if self._bingx_client is None:
            return []
        try:
            all_orders = await asyncio.to_thread(self._bingx_client.get_open_orders)
            normalized = []
            for o in all_orders:
                otype = str(o.get("type") or o.get("orderType") or "").upper()
                # Solo incluir órdenes trigger (SL/TP standalone)
                if not any(t in otype for t in ("STOP", "TAKE_PROFIT")):
                    continue
                o_norm = dict(o)
                if "STOP" in otype or "SL" in otype:
                    o_norm["algoType"] = "sl"
                elif "TAKE_PROFIT" in otype or "TP" in otype:
                    o_norm["algoType"] = "tp"
                normalized.append(o_norm)
            return normalized
        except Exception as e:
            logger.warning("[%s] _get_open_trigger_orders_raw error: %s", self.symbol, e)
            return []

    async def close_position(self, reason: str = "MANUAL") -> None:
        """
        Cierra la posición abierta con una orden de mercado en BingX.
        Llamado por PositionManager._emergency_close() y externamente.
        """
        if self.position is None:
            logger.info("[%s] close_position: no hay posición abierta.", self.symbol)
            return

        side_str = self.position
        is_long  = side_str == "long"
        qty      = self._open_qty

        logger.warning(
            "[%s] close_position: cerrando %s (qty=%.6f) — reason=%s",
            self.symbol, side_str.upper(), qty, reason,
        )

        if self.dry_run:
            logger.info("[%s] [DRY-RUN] close_position simulado.", self.symbol)
        else:
            if self._bingx_client is None:
                await self._get_ccxt()
            if qty > 0 and hasattr(self._bingx_client, "place_market"):
                try:
                    # v19 FIX: place_market usa sz= (no qty=)
                    result = await asyncio.to_thread(
                        self._bingx_client.place_market,
                        not is_long,   # is_buy (posicional) — cierre = lado opuesto
                        qty,           # sz (posicional) — fix v19: era qty=qty
                        True,          # reduce_only (posicional)
                    )
                    code = (result or {}).get("code", -1)
                    if code in (0, "0", None):
                        logger.info("[%s] Posición cerrada en exchange.", self.symbol)
                    else:
                        logger.error(
                            "[%s] close_position rechazado (code=%s): %s",
                            self.symbol, code, (result or {}).get("msg", ""),
                        )
                except Exception as e:
                    logger.error("[%s] close_position error: %s", self.symbol, e)

        # Limpiar estado local
        from bot.state import clear_position as _clear
        self.position       = None
        self.entry_price    = None
        self.sl             = None
        self.tp1            = None
        self.tp2            = None
        self.tp3            = None
        self._open_qty      = 0.0
        self._open_notional = 0.0
        self._protection_ok = False
        self._tp1_be_done   = False
        _clear(self.symbol)
