#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

v34 — Fix 3 bugs (2026-06-09):
  Bug 1 (CRÍTICO): filled_price podía ser None si place_market devolvía
    price=0/None y todos los reintentos de get_price() fallaban.
    _adjust_levels_to_fill recibía None → TypeError.
    Fix: filled_price = filled_price or ref_price antes de llamar
    _adjust_levels_to_fill.

  Bug 3 (MEDIO): _ohlcv_fn ignoraba el parámetro limit. Siempre usaba
    _OHLCV_BARS del env aunque el caller pidiera más/menos velas.
    Fix: limit se pasa a _raw_fetch y a ohlcv_cache.get().

v33 — Fix SL/TP + margen fijo 20 USDT (2026-06-09).
v32 — Fix CRÍTICO: guardia dura de side en _do_open_order.
v31 — Fix CRÍTICO: side leído desde 'signal' (campo de SignalResult).
v30 — Fix CRÍTICO: guardia SL/TP obligatoria también en dry-run (2026-06-09).
v29 — Fix kelly_multiplier: propagar min_ratio desde signal (2026-06-08).
v28 — Fix: self.atr no se seteaba al abrir posición (2026-06-08).
v27 — Conectar kelly_multiplier al sizing (2026-06-08).
v26 — Fix get_ohlcv_fn: delegar en ohlcv_cache (2026-06-08).
v25 — Persistencia BE y restauración tras restart (2026-06-07).
v24 — Rebase automático de niveles al precio de mercado (2026-06-07).
v23 — Solo un TP; SL a Break-Even al 40% entry→TP1 (2026-06-07).
v22 — Fix Bug Crítico 1: _do_open_order() ahora llama pretrade_risk.confirm_order() (2026-06-07).
v21 — Fix Bug Menor 4: guardia dura SL/TP + cierre de emergencia si SL falla (2026-06-07).
v20 — Fix 4 bugs internos (2026-06-07).
v19 — Fix CRÍTICO: corregir kwargs qty= → sz= en llamadas a BingXClient (2026-06-07).
v18 — Fix CRÍTICO: añade open_order() (2026-06-07).
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
from bot.state import save_position, load_position

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("BINGX_TESTNET", "false").lower() in ("true", "1", "yes")

_OHLCV_BARS             = int(os.getenv("BARS_NEEDED",            "100"))
_PRICE_FETCH_RETRIES    = int(os.getenv("PRICE_FETCH_RETRIES",    "3"))
_SET_LEVERAGE_TIMEOUT_S = float(os.getenv("SET_LEVERAGE_TIMEOUT_S", "15"))

# v33: margen fijo por operación — ignora cualquier valor dinámico de risk
_FIXED_USDC_PER_TRADE: float = float(os.getenv("USDC_PER_TRADE", "20.0"))

# Mapa de timeframe → intervalo BingX (coincide con parámetro "interval")
_TF_BINGX = {
    "1m":  "1m",  "3m":  "3m",  "5m":  "5m",  "15m": "15m",
    "30m": "30m", "1h":  "1h",  "2h":  "2h",  "4h":  "4h",
    "6h":  "6h",  "8h":  "8h",  "12h": "12h", "1d":  "1d",
    "1w":  "1w",  "1M":  "1M",
}

_FILL_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))
_FILL_DELAY   = float(os.getenv("POST_FILL_CONFIRM_DELAY", "2.0"))

# v24: límite absoluto de drift antes de considerar los datos de la señal corruptos.
_MAX_REBASE_DRIFT_PCT = float(os.getenv("MAX_REBASE_DRIFT_PCT", "30.0")) / 100.0

_BASE_URL = (
    "https://open-api-vst.bingx.com"
    if _USE_TESTNET
    else "https://open-api.bingx.com"
)

# v32: valores de side válidos — cualquier otro valor aborta la orden
_VALID_LONG_SIDES  = frozenset({"long", "buy"})
_VALID_SHORT_SIDES = frozenset({"short", "sell"})
_VALID_SIDES       = _VALID_LONG_SIDES | _VALID_SHORT_SIDES


def _to_inst_id(symbol: str) -> str:
    """Convierte 'BTC' o 'BTC/USDT:USDT' → 'BTC-USDT'."""
    s = symbol.upper()
    for rm in ("/USDT:USDT", "-USDT-SWAP", "/USDT"):
        s = s.replace(rm, "")
    base = s.split("-")[0]
    return f"{base}-USDT"


def _rebase_signal_to_market(
    signal: dict,
    ref_price: float,
    symbol: str = "",
) -> Optional[str]:
    """
    v24: Reescala entry/sl/tp1 del signal al precio real de mercado (ref_price),
    preservando los mismos ratios porcentuales entry→SL y entry→TP1.

    Modifica signal in-place. Devuelve None si todo va bien, o un mensaje
    de error string si el drift es tan grande que parece datos corruptos
    (>MAX_REBASE_DRIFT_PCT, por defecto 30%).
    """
    entry_signal = float(signal.get("entry") or 0)

    if entry_signal <= 0:
        signal["entry"] = ref_price
        logger.debug("[%s] rebase: sin entry en signal → usando ref_price=%.4f", symbol, ref_price)
        return None

    drift = (ref_price - entry_signal) / entry_signal
    abs_drift = abs(drift)

    if abs_drift > _MAX_REBASE_DRIFT_PCT:
        return (
            f"🛑 Drift del precio ({drift*100:+.2f}%) supera el límite de "
            f"±{_MAX_REBASE_DRIFT_PCT*100:.0f}% — posibles datos corruptos — entrada cancelada"
        )

    if abs_drift < 0.0005:
        return None

    sl_orig  = float(signal.get("sl")  or 0)
    tp1_orig = float(signal.get("tp1") or 0)

    def _rescale(level: float) -> float:
        if level <= 0:
            return level
        ratio = (level - entry_signal) / entry_signal
        return ref_price * (1.0 + ratio)

    sl_new  = _rescale(sl_orig)
    tp1_new = _rescale(tp1_orig)

    logger.info(
        "[%s] 🔄 Rebase de señal: entry %.4f → %.4f (%+.2f%%) | "
        "SL %.4f→%.4f | TP1 %.4f→%.4f",
        symbol,
        entry_signal, ref_price, drift * 100,
        sl_orig, sl_new,
        tp1_orig, tp1_new,
    )

    signal["entry"] = ref_price
    if sl_new > 0:
        signal["sl"]  = sl_new
    if tp1_new > 0:
        signal["tp1"] = tp1_new

    return None


def _adjust_levels_to_fill(
    signal: dict,
    filled_price: float,
    ref_price: float,
) -> tuple[float, float]:
    """
    v23: devuelve solo (sl_adj, tp1_adj). tp2 eliminado — ya no se usa.
    """
    sl_px  = float(signal.get("sl")  or 0)
    tp1_px = float(signal.get("tp1") or 0)
    base   = float(signal.get("entry") or 0) or ref_price
    if abs(filled_price - base) / base < 0.0005:
        return sl_px, tp1_px
    def _rescale(level: float) -> float:
        if level <= 0:
            return level
        return filled_price * (1.0 + (level - base) / base)
    sl_adj  = _rescale(sl_px)
    tp1_adj = _rescale(tp1_px)
    logger.info(
        "Ajuste SL/TP por desfase de fill: base=%.4f → filled=%.4f (%.2f%%) | "
        "SL %.4f→%.4f | TP1 %.4f→%.4f",
        base, filled_price, (filled_price - base) / base * 100,
        sl_px, sl_adj, tp1_px, tp1_adj,
    )
    return sl_adj, tp1_adj


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
        self.tp2:            Optional[float] = None   # solo estado interno, NO se coloca en exchange
        self.tp3:            Optional[float] = None   # solo estado interno, NO se coloca en exchange
        self.tp2_hit:        bool            = False
        self._open_notional: float           = 0.0
        self._open_leverage: int             = leverage
        self._open_qty:      float           = 0.0
        self._protection_ok: bool            = False
        self._tp1_be_done:   bool            = False
        self._last_price:    float           = 0.0
        self._instrument_unavailable: bool   = False

        # v28: atributos para trailing SL ATR
        self.atr:                    float           = 0.0
        self.trailing_sl_activated:  bool            = False
        self._trailing_peak:         Optional[float] = None

        # Fix #16: flag que indica que hay una orden en vuelo (open_order en ejecución).
        self._pending_order: bool = False

        # Fix #15: expuesto para que _idle_rotation_loop pueda rotarlo inmediatamente.
        self._force_idle_rotate: bool = False

        self._api_key    = api_key    or os.getenv("BINGX_API_KEY",    "")
        self._api_secret = api_secret or os.getenv("BINGX_API_SECRET", "")

        self._bingx_client = None

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)

        # v25: restaurar _tp1_be_done desde state persistido
        try:
            _saved = load_position(self.symbol)
            if _saved:
                if _saved.get("be_done", False):
                    self._tp1_be_done = True
                    logger.info(
                        "[%s] BE ya activado en sesión anterior — restaurado desde state.",
                        self.symbol,
                    )
                if _saved.get("trailing_activated", False):
                    self.trailing_sl_activated = True
                    _saved_atr = float(_saved.get("atr") or 0.0)
                    if _saved_atr > 0:
                        self.atr = _saved_atr
                    logger.info(
                        "[%s] trailing_sl_activated=True restaurado desde state (ATR=%.6f).",
                        self.symbol, self.atr,
                    )
        except Exception:
            pass

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
        v26 FIX: Retorna un callable async que delega en ohlcv_cache.get().
        v34 FIX Bug 3: el parámetro limit ahora se respeta — se pasa a
          _raw_fetch y a ohlcv_cache.get() en lugar de ignorarlo siempre.
        """
        symbol  = self.symbol
        inst_id = self.inst_id

        async def _raw_fetch(timeframe: str, limit: int = _OHLCV_BARS) -> list[dict]:
            if self._bingx_client is None:
                await self._get_ccxt()

            interval = _TF_BINGX.get(timeframe, timeframe)
            import requests as _req
            resp = await asyncio.to_thread(
                lambda: _req.get(
                    f"{_BASE_URL}/openApi/swap/v3/quote/klines",
                    params={
                        "symbol":   inst_id,
                        "interval": interval,
                        "limit":    limit,
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
                "[%s] _raw_fetch: %d barras (%s, limit=%d) recibidas.",
                symbol, len(candles), timeframe, limit,
            )
            return candles

        async def _ohlcv_fn(timeframe: str = "15m", limit: int = _OHLCV_BARS) -> list[dict]:
            # v34 fix Bug 3: pasar limit a ohlcv_cache para que sea respetado
            return await ohlcv_cache.get(
                coin=symbol,
                tf=timeframe,
                fetch_fn=lambda tf: _raw_fetch(tf, limit),
                limit=limit,
            )

        return _ohlcv_fn

    # ── _get_positions ──────────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list[dict]:
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
        if self.dry_run:
            logger.info("[%s] [DRY-RUN] _set_leverage(%dx)", self.symbol, leverage)
            self._open_leverage = leverage
            return
        if self._bingx_client is None:
            logger.warning("[%s] _set_leverage: BingXClient no inicializado — skip.", self.symbol)
            return

        effective_leverage = leverage
        try:
            if hasattr(self._bingx_client, "get_max_leverage"):
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

    def _confirm_margin(
        self,
        sl_price: Optional[float],
        tp1_price: Optional[float],
    ) -> Optional[str]:
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

    async def open_order(self, signal: dict, risk) -> None:
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

        # ── 1. Extraer y validar side ─────────────────────────────────────
        # v32 FIX: guardia dura — abortar si side no es long/buy/short/sell.
        raw_side = str(
            signal.get("side")
            or signal.get("action")
            or signal.get("signal")
            or ""
        ).strip().lower()

        logger.info(
            "[%s] _do_open_order: raw_side=%r (campos: side=%r action=%r signal=%r)",
            self.symbol, raw_side,
            signal.get("side"), signal.get("action"), signal.get("signal"),
        )

        if raw_side not in _VALID_SIDES:
            logger.error(
                "[%s] 🚫 _do_open_order: side=%r no reconocido "
                "(válidos: long/buy/short/sell) — abortando orden.",
                self.symbol, raw_side,
            )
            return

        is_long  = raw_side in _VALID_LONG_SIDES
        side_str = "long" if is_long else "short"

        logger.info(
            "[%s] _do_open_order: side_str=%s",
            self.symbol, side_str,
        )

        sl_price  = float(signal.get("sl")  or 0) or None
        tp1_price = float(signal.get("tp1") or 0) or None
        tp2_ref   = float(signal.get("tp2") or 0) or None
        tp3_ref   = float(signal.get("tp3") or 0) or None

        # v28: ATR de la señal — fuente primaria para trailing SL
        signal_atr = float(signal.get("atr") or 0.0)

        # v33: margen FIJO — nunca dinámico, nunca ajustado por Kelly
        usdc_per_trade = _FIXED_USDC_PER_TRADE
        leverage       = int(getattr(risk, "leverage", self.leverage) or self.leverage)

        logger.info(
            "[%s] _do_open_order: margen fijo=%.2f USDT lev=%dx (Kelly desactivado)",
            self.symbol, usdc_per_trade, leverage,
        )

        # ── 2. GUARDIA DURA PRE-REBASE: SL y TP1 obligatorios en CUALQUIER modo ──
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

        # ── 4. Rebase automático de niveles al precio de mercado (v24) ───
        rebase_err = _rebase_signal_to_market(signal, ref_price, symbol=self.symbol)
        if rebase_err:
            logger.warning("[%s] open_order cancelado: %s", self.symbol, rebase_err)
            return

        sl_price  = float(signal.get("sl")  or 0) or None
        tp1_price = float(signal.get("tp1") or 0) or None

        # ── 2b. GUARDIA DURA POST-REBASE ──────────────────────────────────
        guard_err_post = self._confirm_margin(sl_price, tp1_price)
        if guard_err_post:
            logger.error(
                "[%s] 🚫 Post-rebase: %s — abortando.",
                self.symbol, guard_err_post,
            )
            return

        # ── 5. Calcular qty ───────────────────────────────────────────────
        # v33: qty SIEMPRE basado en margen fijo × leverage — sin Kelly
        notional = usdc_per_trade * leverage
        raw_qty  = notional / ref_price
        qty = round(raw_qty, 4)
        dec = 4
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
            self.tp2            = tp2_ref
            self.tp3            = tp3_ref
            self._open_qty      = qty
            self._open_notional = usdc_per_trade
            self._open_leverage = leverage
            self._protection_ok = True
            self._tp1_be_done   = False
            self.atr                   = signal_atr
            self.trailing_sl_activated = False
            self._trailing_peak        = None
            save_position(
                self.symbol,
                side=side_str,
                entry=ref_price,
                sl=sl_price,
                tp1=tp1_price,
                tp2=tp2_ref,
                tp3=tp3_ref,
                qty=qty,
                usdc_amount=usdc_per_trade,
                leverage=leverage,
                be_done=False,
            )
            logger.info(
                "[%s] [DRY-RUN] Posición simulada: %s @ %.4f (qty=%.6f) | "
                "SL=%.4f | TP1=%.4f (único TP activo) | ATR=%.6f",
                self.symbol, side_str.upper(), ref_price, qty,
                sl_price or 0, tp1_price or 0, signal_atr,
            )
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
        # v33: se usa SIEMPRE place_market + _place_tpsl por separado.
        # place_market_with_tpsl desactivado — comportamiento inconsistente en BingX.
        if self._bingx_client is None:
            await self._get_ccxt()

        await self._set_leverage(leverage)

        filled_price: Optional[float] = None

        try:
            result = await asyncio.to_thread(
                self._bingx_client.place_market,
                is_long,
                qty,
            )
            if result and result.get("code") in (0, "0", None):
                filled_price = float(
                    (result.get("data") or [{}])[0].get("price")
                    or (result.get("data") or [{}])[0].get("avgPrice")
                    or ref_price
                )
                logger.info("[%s] place_market OK: filled_price=%.4f", self.symbol, filled_price)
            else:
                err = (result or {}).get("msg", "sin respuesta")
                logger.error("[%s] open_order: place_market rechazado: %s — abortando.", self.symbol, err)
                return
        except Exception as e:
            logger.error("[%s] open_order: place_market error: %s — abortando.", self.symbol, e)
            return

        # Confirmar fill con precio real
        for attempt in range(_FILL_RETRIES):
            try:
                confirmed = await self.get_price()
                if confirmed > 0:
                    filled_price = confirmed
                    break
            except Exception:
                pass
            await asyncio.sleep(_FILL_DELAY)

        # v34 fix Bug 1: garantizar que filled_price nunca es None/0 antes
        # de pasarlo a _adjust_levels_to_fill — usar ref_price como fallback.
        if not filled_price or filled_price <= 0:
            logger.warning(
                "[%s] filled_price inválido (%.4f) — usando ref_price=%.4f como fallback.",
                self.symbol, filled_price or 0, ref_price,
            )
            filled_price = ref_price

        # Colocar SL + TP1 por separado
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
            logger.error(
                "[%s] 🚨 SL NO colocado — cerrando posición "
                "para evitar exposición sin stop loss.",
                self.symbol,
            )
            try:
                await self.close_position(reason="NO_SL")
                logger.warning("[%s] Posición cerrada preventivamente por falta de SL.", self.symbol)
            except Exception as close_exc:
                logger.critical(
                    "[%s] ❌❌ FALLO CRÍTICO: no se pudo colocar SL NI cerrar la posición: %s",
                    self.symbol, close_exc,
                )
            try:
                from bot.telegram_bot import send_message
                await send_message(
                    f"🚨 *ALERTA CRÍTICA* `{self.symbol}`\n"
                    f"SL no pudo colocarse.\n"
                    f"Posición cerrada preventivamente."
                )
            except Exception:
                pass
            return

        # ── 8. Ajustar niveles al fill real ───────────────────────────────
        sl_adj, tp1_adj = _adjust_levels_to_fill(signal, filled_price, ref_price)
        if sl_adj:
            sl_price  = sl_adj
        if tp1_adj:
            tp1_price = tp1_adj

        # ── 9. Actualizar estado ──────────────────────────────────────────
        self.position       = side_str
        self.entry_price    = filled_price
        self.sl             = sl_price
        self.tp1            = tp1_price
        self.tp2            = tp2_ref
        self.tp3            = tp3_ref
        self._open_qty      = qty
        self._open_notional = usdc_per_trade
        self._open_leverage = leverage
        self._tp1_be_done   = False
        self._last_price    = filled_price
        self.atr                   = signal_atr
        self.trailing_sl_activated = False
        self._trailing_peak        = None

        save_position(
            self.symbol,
            side=side_str,
            entry=filled_price,
            sl=sl_price,
            tp1=tp1_price,
            tp2=tp2_ref,
            tp3=tp3_ref,
            qty=qty,
            usdc_amount=usdc_per_trade,
            leverage=leverage,
            be_done=False,
        )

        logger.info(
            "[%s] ✅ Posición abierta: %s @ %.4f | SL=%.4f | TP1=%.4f (único TP) "
            "| qty=%.6f | ATR=%.6f",
            self.symbol, side_str.upper(), filled_price,
            sl_price or 0, tp1_price or 0, qty, signal_atr,
        )

        # ── 9b. v22: registrar margen en pretrade_risk ────────────────────
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

        # ── 10. v23: NO se colocan TP2/TP3 — único TP es tp1 ─────────────

    # ── _place_tpsl ───────────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> bool:
        if self.dry_run:
            logger.info(
                "[%s] [DRY-RUN] _place_tpsl: SL=%.4f TP=%.4f qty=%.6f",
                self.symbol, sl_price or 0, tp_price or 0, qty,
            )
            return True

        if self._bingx_client is None:
            logger.warning("[%s] _place_tpsl: BingXClient no inicializado — skip.", self.symbol)
            return sl_price is None or sl_price <= 0

        sl_placed = True

        if sl_price and sl_price > 0:
            sl_placed = False
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
        if self._bingx_client is None:
            return []
        try:
            orders = await asyncio.to_thread(self._bingx_client.get_open_orders)
            return list(orders)
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e)
            return []

    async def _get_open_trigger_orders_raw(self) -> list[dict]:
        if self._bingx_client is None:
            return []
        try:
            all_orders = await asyncio.to_thread(self._bingx_client.get_open_orders)
            normalized = []
            for o in all_orders:
                otype = str(o.get("type") or o.get("orderType") or "").upper()
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
                    result = await asyncio.to_thread(
                        self._bingx_client.place_market,
                        not is_long,
                        qty,
                        True,
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

        from bot.state import clear_position as _clear
        self.position              = None
        self.entry_price           = None
        self.sl                    = None
        self.tp1                   = None
        self.tp2                   = None
        self.tp3                   = None
        self._open_qty             = 0.0
        self._open_notional        = 0.0
        self._protection_ok        = False
        self._tp1_be_done          = False
        self.atr                   = 0.0
        self.trailing_sl_activated = False
        self._trailing_peak        = None
        _clear(self.symbol)
