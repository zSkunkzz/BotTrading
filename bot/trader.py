#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada pública para main.py.

FIX KELLY (#2 2026-06-03):
  open_order ahora aplica kelly_multiplier() al usdc_per_trade base.
  Si Kelly no tiene historial suficiente (<30 trades) usa mult=1.0 sin cambio.
  El size efectivo = usdc_per_trade * kelly_mult, clampeado entre
  KELLY_MIN_MULT y KELLY_MAX_MULT de kelly_sizer.py.

FIX FREEZE (2026-06-03):
  CAUSA RAÍZ del freeze «no veo nada más / TradingLoop iniciado y silencio»:
  FuturesTrader.__init__ llamaba HLClient(symbol) directamente, que a su vez
  llamaba _HLCore.get(), que ejecuta _warm_cache() con 3 llamadas HTTP
  bloqueantes (requests). Con 7+ traders, el primer trader bloqueaba el hilo
  principal ~2-5s; si había latencia o 429, time.sleep() en los reintentos
  congelaba el event loop entero — ningún trader llegaba a _iteration().

  Fixes aplicados:
    1. __init__: _hl_client = None. El SDK jamás se crea aquí.
    2. _get_ccxt(): crea _hl_client vía HLClient.create() (async) la primera
       vez que se llama. TradingLoop.run() invoca _get_ccxt() desde _init()
       dentro del event loop, por lo que es seguro awaitar.
    3. _set_leverage: asyncio.wait_for con timeout=15s.
    4. Todos los métodos que usan _hl_client verifican que no sea None.

FIX DEADLOCK (2026-06-03 anterior):
  Todas las llamadas al SDK síncrono se envuelven en asyncio.to_thread().

FIX get_ohlcv None (2026-06-03):
  Hyperliquid candleSnapshot devuelve null (Python None) cuando startTime
  es demasiado antiguo o el request falla silenciosamente.
  Fixes:
    - Reducir startTime: n = BARS_NEEDED (sin el +20 extra).
    - Guard: si raw is None, retry 1 vez con ventana aún más corta.
    - Log del status HTTP y body truncado cuando no es lista.

FIX _get_positions NoneType (2026-06-03):
  Cuando _master_addr está vacío (init incompleto) o HL devuelve error JSON,
  data es None o un dict sin 'assetPositions'. Fix:
    - Guard early-return si _master_addr vacío.
    - if data is None: log + return [].
    - try/except TypeError alrededor de data.get().

FIX NameError aiohttp (2026-06-03):
  _fetch_candles usaba aiohttp.ClientTimeout pero el import estaba solo
  dentro de get_ohlcv (scope distinto). Movido al nivel de módulo.

FIX get_price NoneType (2026-06-03):
  Si HL devuelve null, un error HTTP, o un body no-dict (e.g. string de error),
  data.get(self.coin) lanzaba 'NoneType object has no attribute get'.
  Fix: guard isinstance(data, dict) antes de llamar .get().
  Si data no es dict → raise ValueError con el body truncado para diagnóstico.

FIX _ensure_tpsl spam (2026-06-03):
  En Hyperliquid, los SL/TP colocados con place_sl/place_tp son TRIGGER ORDERS
  y viven en el endpoint openTriggerOrders, NO en openOrders. Por eso
  _ensure_tpsl los veía siempre como «faltantes» y los recolocaba en bucle.
  Fix: añadido _get_open_trigger_orders_raw() que llama al endpoint correcto.

FIX OHLCV semáforo (2026-06-03):
  Con 10 traders × 3 timeframes = 30 fetch simultáneos a HL → NoneType spam.
  Añadido _OHLCV_SEMAPHORE global (asyncio.Semaphore) que limita los fetch
  de candleSnapshot a max OHLCV_MAX_CONCURRENCY peticiones en paralelo.
  El semáforo se inicializa lazy en get_ohlcv() la primera vez que se llama
  (dentro del event loop), evitando el error de "attached to a different loop".
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import time
from typing import Callable, Optional

import aiohttp

from bot.core.hl_client import HLClient, _norm_coin
from bot.core.trading_loop import TradingLoop
from bot.state import save_position

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

_OHLCV_BARS = int(os.getenv("BARS_NEEDED", "100"))
_OHLCV_MAX_CONCURRENCY = int(os.getenv("OHLCV_MAX_CONCURRENCY", "5"))

_TF_MINUTES = {
    "1m":  1,
    "3m":  3,
    "5m":  5,
    "15m": 15,
    "30m": 30,
    "1h":  60,
    "2h":  120,
    "4h":  240,
    "8h":  480,
    "1d":  1440,
}

_FILL_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))
_FILL_DELAY   = float(os.getenv("POST_FILL_CONFIRM_DELAY", "2.0"))

_MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "3.0")) / 100.0

# Semáforo OHLCV — inicializado lazy dentro del event loop la primera vez
# que get_ohlcv() se llama. No se crea a nivel de módulo para evitar el error
# "got Future attached to a different loop" en Python <3.10.
_OHLCV_SEMAPHORE: Optional[asyncio.Semaphore] = None
_OHLCV_SEM_LOCK = asyncio.Lock.__new__(asyncio.Lock)  # placeholder, se crea lazy


def _get_ohlcv_semaphore() -> asyncio.Semaphore:
    """Devuelve el semáforo OHLCV global, creándolo la primera vez."""
    global _OHLCV_SEMAPHORE
    if _OHLCV_SEMAPHORE is None:
        _OHLCV_SEMAPHORE = asyncio.Semaphore(_OHLCV_MAX_CONCURRENCY)
        logger.info(
            "[OHLCVSemaphore] Inicializado: max_concurrency=%d (env OHLCV_MAX_CONCURRENCY)",
            _OHLCV_MAX_CONCURRENCY,
        )
    return _OHLCV_SEMAPHORE


def _check_price_staleness(
    signal: dict,
    ref_price: float,
    is_long: bool,
) -> Optional[str]:
    entry_signal = float(signal.get("entry") or 0)
    if entry_signal <= 0:
        return None

    drift = (ref_price - entry_signal) / entry_signal
    abs_drift = abs(drift)
    threshold = _MAX_ENTRY_DRIFT_PCT

    if abs_drift > threshold * 2:
        return (
            f"⚠️ Precio actual ({ref_price:.4f}) se alejó {drift*100:+.2f}% del entry del signal "
            f"({entry_signal:.4f}) — supera el límite absoluto de ±{threshold*200:.1f}% — entrada cancelada"
        )

    if abs_drift <= threshold:
        return None

    if is_long:
        if drift > 0:
            return (
                f"⏫ [LONG] Precio actual ({ref_price:.4f}) subió {drift*100:+.2f}% sobre entry del signal "
                f"({entry_signal:.4f}) — entrada demasiado cara, cancelada "
                f"(límite: +{threshold*100:.1f}%)"
            )
        else:
            return (
                f"⏪ [LONG] Precio actual ({ref_price:.4f}) cayó {drift*100:+.2f}% bajo entry del signal "
                f"({entry_signal:.4f}) — setup roto (precio en caída), cancelado "
                f"(límite: -{threshold*100:.1f}%)"
            )
    else:
        if drift < 0:
            return (
                f"⏪ [SHORT] Precio actual ({ref_price:.4f}) bajó {drift*100:+.2f}% bajo entry del signal "
                f"({entry_signal:.4f}) — entrada demasiado barata/cara para short, cancelada "
                f"(límite: -{threshold*100:.1f}%)"
            )
        else:
            return (
                f"⏫ [SHORT] Precio actual ({ref_price:.4f}) subió {drift*100:+.2f}% sobre entry del signal "
                f"({entry_signal:.4f}) — setup roto (precio en subida), cancelado "
                f"(límite: +{threshold*100:.1f}%)"
            )


def _adjust_levels_to_fill(
    signal: dict,
    filled_price: float,
    ref_price: float,
) -> tuple[float, float, float]:
    sl_px  = float(signal.get("sl")  or 0)
    tp1_px = float(signal.get("tp1") or 0)
    tp2_px = float(signal.get("tp2") or 0)

    base = float(signal.get("entry") or 0)
    if base <= 0:
        base = ref_price

    if abs(filled_price - base) / base < 0.0005:
        return sl_px, tp1_px, tp2_px

    def _rescale(level: float) -> float:
        if level <= 0:
            return level
        pct = (level - base) / base
        return filled_price * (1.0 + pct)

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
    """
    Orquestador principal de un par de trading en Hyperliquid.
    """

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

        self.position:        Optional[str]   = None
        self.entry_price:     Optional[float] = None
        self.sl:              Optional[float] = None
        self.tp1:             Optional[float] = None
        self.tp2:             Optional[float] = None
        self.tp3:             Optional[float] = None
        self.tp2_hit:         bool            = False
        self._open_notional:  float           = 0.0
        self._open_leverage:  int             = leverage
        self._open_qty:       float           = 0.0
        self._protection_ok:  bool            = False
        self._tp1_be_done:    bool            = False
        self._last_price:     float           = 0.0

        self._api_key    = api_key or ""
        self._api_secret = api_secret or ""

        # FIX FREEZE: NO crear HLClient aquí (bloquea el event loop).
        self._hl_client: Optional[HLClient] = None
        self._master_addr: str = ""
        self._agent_mode:  bool = False

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)
        self._ccxt_exchange  = None

    # ── Interfaz pública requerida por main.py ──────────────────

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

    # ── _get_ccxt: crea HLClient la primera vez (async-safe) ──────────

    async def _get_ccxt(self) -> None:
        if self._hl_client is not None:
            return
        try:
            logger.info("[%s] _get_ccxt: inicializando HLClient...", self.symbol)
            self._hl_client = await HLClient.create(self.symbol)
            self._master_addr = self._hl_client._account_addr
            self._agent_mode  = self._hl_client._agent_mode
            logger.info(
                "[%s] _get_ccxt: HLClient listo | addr=%s | agente=%s",
                self.symbol,
                self._master_addr[:10] + "..." if self._master_addr else "N/A",
                self._agent_mode,
            )
        except Exception as e:
            logger.error("[%s] _get_ccxt: error inicializando HLClient: %s", self.symbol, e)
            raise

    # ── Acceso seguro a _hl_client ────────────────────────────────

    def _require_hl(self) -> Optional[HLClient]:
        if self._hl_client is None:
            logger.error(
                "[%s] _hl_client no inicializado. ¿Se llamó _get_ccxt() antes?",
                self.symbol,
            )
            return None
        return self._hl_client

    # ── Métodos que TradingLoop llama sobre el objeto trader ────────

    async def get_price(self) -> float:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                text = await resp.text()

        try:
            data = _json.loads(text)
        except Exception:
            raise ValueError(
                f"[{self.symbol}] allMids respuesta no-JSON: {text[:200]}"
            )

        if not isinstance(data, dict):
            raise ValueError(
                f"[{self.symbol}] allMids devolvió tipo inesperado "
                f"({type(data).__name__}): {text[:200]}"
            )

        price = data.get(self.coin)
        if price is None:
            raise ValueError(f"[{self.symbol}] Precio no encontrado en allMids")
        return float(price)

    async def _fetch_candles(self, session, timeframe: str, n_bars: int):
        interval = _TF_MINUTES.get(timeframe, 15)
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - (n_bars * interval * 60 * 1000)

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin":      self.coin,
                "interval":  timeframe,
                "startTime": start_ms,
                "endTime":   end_ms,
            },
        }
        async with session.post(
            f"{_API_URL}/info",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            text = await resp.text()
            try:
                return _json.loads(text)
            except Exception:
                logger.warning(
                    "[%s] get_ohlcv(%s): respuesta no-JSON (status=%s): %s",
                    self.symbol, timeframe, resp.status, text[:200],
                )
                return None

    async def get_ohlcv(self, timeframe: str) -> list:
        n = _OHLCV_BARS
        sem = _get_ohlcv_semaphore()

        try:
            async with sem:
                async with aiohttp.ClientSession() as session:
                    raw = await self._fetch_candles(session, timeframe, n)

                    if raw is None or not isinstance(raw, list):
                        logger.warning(
                            "[%s] get_ohlcv(%s) respuesta inesperada (tipo=%s val=%r) — "
                            "reintentando con ventana reducida...",
                            self.symbol, timeframe, type(raw).__name__, raw,
                        )
                        raw = await self._fetch_candles(session, timeframe, n // 2)

                    if raw is None or not isinstance(raw, list):
                        logger.warning(
                            "[%s] get_ohlcv(%s) sigue sin ser lista tras retry (tipo=%s) — "
                            "devolviendo lista vacía.",
                            self.symbol, timeframe, type(raw).__name__,
                        )
                        return []

        except Exception as e:
            logger.warning("[%s] get_ohlcv(%s) error: %s", self.symbol, e)
            return []

        bars = []
        for candle in raw:
            try:
                bars.append([
                    int(candle["t"]),
                    float(candle["o"]),
                    float(candle["h"]),
                    float(candle["l"]),
                    float(candle["c"]),
                    float(candle["v"]),
                ])
            except (KeyError, TypeError, ValueError):
                continue

        logger.debug(
            "[%s] get_ohlcv(%s): %d velas descargadas",
            self.symbol, timeframe, len(bars),
        )
        return bars

    def get_ohlcv_fn(self) -> Callable:
        async def _fn(tf: str) -> list:
            return await self.get_ohlcv(tf)
        return _fn

    async def _get_positions(self) -> list[dict]:
        if not self._master_addr:
            logger.debug("[%s] _get_positions: _master_addr vacío — skip.", self.symbol)
            return []

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{_API_URL}/info",
                    json={"type": "clearinghouseState", "user": self._master_addr},
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = _json.loads(await resp.text())
        except Exception as e:
            logger.warning("[%s] _get_positions error: %s", self.symbol, e)
            return []

        if data is None:
            logger.warning("[%s] _get_positions: respuesta null de HL API.", self.symbol)
            return []

        try:
            asset_positions = data.get("assetPositions", [])
        except (TypeError, AttributeError) as e:
            logger.warning("[%s] _get_positions: respuesta inesperada (tipo=%s): %s",
                           self.symbol, type(data).__name__, e)
            return []

        result = []
        for p in asset_positions:
            try:
                pos = p.get("position", {})
                if pos.get("coin", "").upper() != self.coin.upper():
                    continue
                szi = float(pos.get("szi", 0) or 0)
                if abs(szi) == 0:
                    continue
                result.append({
                    "side":    "long" if szi > 0 else "short",
                    "size":    abs(szi),
                    "entryPx": float(pos.get("entryPx") or 0),
                    "coin":    pos.get("coin", ""),
                })
            except (TypeError, AttributeError, ValueError) as e:
                logger.debug("[%s] _get_positions: error parseando posición: %s", self.symbol, e)
                continue

        return result

    async def _get_open_orders_raw(self) -> list[dict]:
        """Órdenes normales (limit/market). NO contiene SL/TP trigger orders."""
        if not self._master_addr:
            return []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{_API_URL}/info",
                    json={"type": "openOrders", "user": self._master_addr},
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = _json.loads(await resp.text())
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e)
            return []

        if not isinstance(data, list):
            logger.warning("[%s] _get_open_orders_raw respuesta inesperada: %s", self.symbol, type(data))
            return []

        return data

    async def _get_open_trigger_orders_raw(self) -> list[dict]:
        """
        FIX _ensure_tpsl spam: En Hyperliquid, place_sl / place_tp crean
        TRIGGER ORDERS que viven en el endpoint 'frontendOpenOrders', no en
        'openOrders'. Este método los obtiene para que _ensure_tpsl los detecte
        correctamente y no los recoloque en bucle.

        Endpoint: POST /info {"type": "frontendOpenOrders", "user": addr}
        La respuesta incluye orderType.trigger.tpsl = "sl" | "tp".
        """
        if not self._master_addr:
            return []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{_API_URL}/info",
                    json={"type": "frontendOpenOrders", "user": self._master_addr},
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = _json.loads(await resp.text())
        except Exception as e:
            logger.warning("[%s] _get_open_trigger_orders_raw error: %s", self.symbol, e)
            return []

        if not isinstance(data, list):
            logger.debug(
                "[%s] _get_open_trigger_orders_raw respuesta inesperada tipo=%s",
                self.symbol, type(data).__name__,
            )
            return []

        return data

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> None:
        hl = self._require_hl()
        if hl is None:
            return

        if self.dry_run:
            logger.info(
                "[%s] DRY_RUN: _place_tpsl sl=%.4f tp=%.4f omitido.",
                self.symbol, sl_price or 0, tp_price or 0,
            )
            return

        if sl_price and sl_price > 0:
            try:
                result = await asyncio.to_thread(
                    hl.place_sl,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=sl_price,
                    entry_px=self.entry_price or sl_price,
                )
                logger.info("[%s] _place_tpsl SL=%.4f: %s", self.symbol, sl_price, result)
            except Exception as e:
                logger.error("[%s] _place_tpsl SL error: %s", self.symbol, e)

        if tp_price and tp_price > 0:
            try:
                result = await asyncio.to_thread(
                    hl.place_tp,
                    is_buy=not is_long,
                    sz=qty,
                    trigger_px=tp_price,
                    entry_px=self.entry_price or tp_price,
                )
                logger.info("[%s] _place_tpsl TP=%.4f: %s", self.symbol, tp_price, result)
            except Exception as e:
                logger.error("[%s] _place_tpsl TP error: %s", self.symbol, e)

    def _round_qty(self, qty: float) -> float:
        hl = self._require_hl()
        if hl is None:
            return qty
        return hl.round_sz(qty)

    async def _set_leverage(self, leverage: int) -> None:
        hl = self._require_hl()
        if hl is None:
            return

        if self.dry_run:
            logger.info("[%s] DRY_RUN: _set_leverage(%d) omitido.", self.symbol, leverage)
            return
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    hl._exchange.update_leverage,
                    leverage, self.coin, False,
                ),
                timeout=15.0,
            )
            logger.info("[%s] Leverage configurado a %dx: %s", self.symbol, leverage, result)
        except asyncio.TimeoutError:
            logger.warning("[%s] _set_leverage timeout (15s) — continuando sin confirmar leverage.", self.symbol)
        except Exception as e:
            logger.warning("[%s] No se pudo configurar leverage: %s", self.symbol, e)

    async def _info_post(self, payload: dict) -> dict:
        hl = self._require_hl()
        if hl is None:
            return {}

        def _sync_call() -> dict:
            return hl._info._session.post(
                f"{_API_URL}/info", json=payload
            ).json()

        return await asyncio.to_thread(_sync_call)

    # ── open_order: entrada al mercado + SL + TP ──────────────────

    async def open_order(self, signal: dict, risk) -> None:
        hl = self._require_hl()
        if hl is None:
            logger.error("[%s] open_order: _hl_client no inicializado, abortando.", self.symbol)
            return

        if self.position is not None:
            logger.info("[%s] open_order ignorado — ya hay posición abierta (%s).", self.symbol, self.position)
            return

        action = signal.get("action", "").upper()
        side   = signal.get("side", "").lower()

        is_long = (action == "BUY" or side == "long")
        is_buy  = is_long

        usdc_base = float(getattr(risk, "usdc_per_trade", 20.0))

        # FIX KELLY (#2): aplicar kelly_multiplier al size base
        # kelly_mult=1.0 si no hay historial suficiente (<KELLY_MIN_TRADES)
        try:
            from bot.kelly_sizer import kelly_multiplier
            entry_mode = signal.get("entry_mode") or "NORMAL"
            rr_val     = float(signal.get("rr") or 1.0)
            kelly_mult = kelly_multiplier(entry_mode, rr_val)
            usdc_per_trade = usdc_base * kelly_mult
            if kelly_mult != 1.0:
                logger.info(
                    "[%s] Kelly sizing: base=%.2f USDC × %.3f (mode=%s, RR=%.2f) → %.2f USDC",
                    self.symbol, usdc_base, kelly_mult, entry_mode, rr_val, usdc_per_trade,
                )
        except Exception as e:
            logger.warning("[%s] Kelly sizer error (%s) — usando base sin ajuste", self.symbol, e)
            usdc_per_trade = usdc_base

        notional = usdc_per_trade * self.leverage

        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio — abortando. %s", self.symbol, e)
            return

        if ref_price <= 0:
            logger.error("[%s] open_order: precio inválido (%s) — abortando.", self.symbol, ref_price)
            return

        stale_reason = _check_price_staleness(signal, ref_price, is_long)
        if stale_reason:
            logger.warning("[%s] open_order: ENTRADA CANCELADA — %s", self.symbol, stale_reason)
            return

        qty = notional / ref_price
        qty = hl.round_sz(qty)

        if qty <= 0:
            logger.error("[%s] open_order: qty calculada = 0 (notional=%.2f ref_price=%.4f) — abortando.",
                         self.symbol, notional, ref_price)
            return

        logger.info(
            "[%s] open_order: %s | qty=%.6f | ref_price=%.4f | notional=%.2f USDC | lev=%dx | "
            "entry_signal=%.4f | sl_signal=%.4f | tp1_signal=%.4f | drift=%.2f%%",
            self.symbol, "LONG" if is_long else "SHORT",
            qty, ref_price, notional, self.leverage,
            float(signal.get("entry") or 0),
            float(signal.get("sl") or 0),
            float(signal.get("tp1") or 0),
            (ref_price - float(signal.get("entry") or ref_price)) / float(signal.get("entry") or ref_price) * 100,
        )

        if self.dry_run:
            sl_px, tp1_px, tp2_px = _adjust_levels_to_fill(signal, ref_price, ref_price)
            tp3_px = float(signal.get("tp3") or 0)

            logger.info("[%s] DRY_RUN: open_order simulado — sin orden real.", self.symbol)
            self.position    = "long" if is_long else "short"
            self.entry_price = ref_price
            self.sl          = sl_px  if sl_px  > 0 else None
            self.tp1         = tp1_px if tp1_px > 0 else None
            self.tp2         = tp2_px if tp2_px > 0 else None
            self.tp3         = tp3_px if tp3_px > 0 else None
            self._open_notional = notional
            self._open_leverage = self.leverage
            self._open_qty      = qty
            self._protection_ok = (sl_px > 0)
            return

        # ── Orden de mercado ──────────────────────────────────────
        try:
            result = await asyncio.to_thread(
                hl.place_market,
                is_buy,
                qty,
                False,
                ref_price,
            )
            logger.info("[%s] Orden de mercado enviada: %s", self.symbol, result)
        except Exception as e:
            logger.error("[%s] open_order: error al enviar orden de mercado: %s", self.symbol, e)
            return

        status = (result or {}).get("status", "")
        if status not in ("ok", ""):
            logger.error("[%s] open_order: orden rechazada por exchange: %s", self.symbol, result)
            return

        # ── Esperar fill y obtener precio real de entrada ─────────────────
        filled_price = ref_price
        for attempt in range(_FILL_RETRIES):
            await asyncio.sleep(_FILL_DELAY)
            try:
                positions = await self._get_positions()
                if positions:
                    filled_price = positions[0].get("entryPx", ref_price)
                    logger.info(
                        "[%s] Fill confirmado (intento %d/%d): entryPx=%.4f",
                        self.symbol, attempt + 1, _FILL_RETRIES, filled_price,
                    )
                    break
            except Exception as e:
                logger.warning("[%s] open_order: error confirmando fill: %s", self.symbol, e)
        else:
            logger.warning("[%s] open_order: fill no confirmado tras %d intentos — usando ref_price=%.4f",
                           self.symbol, _FILL_RETRIES, ref_price)

        # ── Re-escalar SL/TP al precio real de fill ────────────────────
        sl_px, tp1_px, tp2_px = _adjust_levels_to_fill(signal, filled_price, ref_price)

        tp3_raw = float(signal.get("tp3") or 0)
        tp3_px = 0.0
        if tp3_raw > 0:
            base = float(signal.get("entry") or ref_price)
            if base > 0 and abs(filled_price - base) / base >= 0.0005:
                pct = (tp3_raw - base) / base
                tp3_px = filled_price * (1.0 + pct)
            else:
                tp3_px = tp3_raw

        # ── Actualizar estado interno ──────────────────────────────
        self.position    = "long" if is_long else "short"
        self.entry_price = filled_price
        self.sl          = sl_px  if sl_px  > 0 else None
        self.tp1         = tp1_px if tp1_px > 0 else None
        self.tp2         = tp2_px if tp2_px > 0 else None
        self.tp3         = tp3_px if tp3_px > 0 else None
        self._open_notional = notional
        self._open_leverage = self.leverage
        self._open_qty      = qty
        self._protection_ok = False
        self._tp1_be_done   = False

        # ── Colocar SL ────────────────────────────────────────────
        if sl_px and sl_px > 0:
            try:
                sl_result = await asyncio.to_thread(
                    hl.place_sl,
                    not is_buy,
                    qty,
                    sl_px,
                    filled_price,
                )
                logger.info("[%s] SL colocado en %.4f: %s", self.symbol, sl_px, sl_result)
                self._protection_ok = True
            except Exception as e:
                logger.error("[%s] open_order: error colocando SL: %s", self.symbol, e)

        # ── Colocar TP1 ──────────────────────────────────────────
        if tp1_px and tp1_px > 0:
            try:
                tp_result = await asyncio.to_thread(
                    hl.place_tp,
                    not is_buy,
                    qty,
                    tp1_px,
                    None,
                    filled_price,
                )
                logger.info("[%s] TP1 colocado en %.4f: %s", self.symbol, tp1_px, tp_result)
            except Exception as e:
                logger.error("[%s] open_order: error colocando TP1: %s", self.symbol, e)

        # ── Persistir estado ────────────────────────────────────
        try:
            save_position(self.symbol, {
                "side":        self.position,
                "entry":       self.entry_price,
                "sl":          self.sl,
                "tp1":         self.tp1,
                "tp2":         self.tp2,
                "tp3":         self.tp3,
                "tp2_hit":     self.tp2_hit,
                "usdc_amount": usdc_per_trade,
                "leverage":    self.leverage,
                "qty":         self._open_qty,
            })
        except Exception as e:
            logger.warning("[%s] open_order: no se pudo persistir estado: %s", self.symbol, e)

        logger.info(
            "[%s] ✅ Posición abierta: %s @ %.4f | SL=%.4f | TP1=%.4f | Kelly=%.2fx",
            self.symbol,
            self.position.upper(),
            self.entry_price,
            self.sl or 0,
            self.tp1 or 0,
            kelly_mult if 'kelly_mult' in dir() else 1.0,
        )


__all__ = ["FuturesTrader"]
