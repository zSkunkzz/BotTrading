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
    - Guard: si raw is None, retry 1 vez con ventana más corta.
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

FIX OHLCV semáforo → ohlcv_cache (2026-06-05):
  Con 10 traders × 3 timeframes = 30 fetch simultáneos a HL → NoneType spam.
  ANTES: _OHLCV_SEMAPHORE global duplicado en este módulo, sin stale fallback.
  AHORA: get_ohlcv() delega completamente en ohlcv_cache.get() (singleton de
  bot/ohlcv_cache.py) que ya tiene semáforo, backoff exponencial, stale
  fallback y LRU eviction. Se eliminan _OHLCV_SEMAPHORE y _get_ohlcv_semaphore()
  de este módulo.

FIX allMids NoneType — retry + caché último precio (2026-06-04):
  Cuando HL devuelve null en allMids (cold-start o saturación puntual),
  get_price() ahora:
    1. Reintenta 1 vez tras 0.4s si data no es dict.
    2. Si sigue fallando, devuelve self._last_price (último precio válido
       cacheado) en lugar de propagar la excepción — el tick se procesa
       con el precio anterior y el WARNING queda silenciado.
    3. Si _last_price == 0 (primer arranque y falla) → propaga excepción.
    4. Cada llamada exitosa actualiza self._last_price.

FIX get_ohlcv 3 bugs (2026-06-05):
  Bug 1 — except arg faltante: logger.warning tenía self.symbol y e pero
    faltaba timeframe como segundo %s → el timeframe se perdía y el error
    real quedaba sin contexto. Corregido el format string.
  Bug 2 — retry sin backoff: el segundo intento (n//2) se lanzaba
    inmediatamente con la misma sesión abierta. Si HL está saturado el
    segundo intento falla igual. Ahora espera 1s con asyncio.sleep antes
    del retry y abre una nueva sesión aiohttp.
  Bug 3 — raw scope inseguro: raw se asignaba dentro del bloque
    `async with sem` pero se usaba fuera. Si una excepción interrumpía
    el bloque antes de asignar raw, el código posterior lanzaba NameError.
    Ahora raw se inicializa a None antes del bloque y el bloque de parseo
    queda dentro del try/except con early-return [] si raw no es lista.

FIX kelly_mult scope + place_tp firma (2026-06-05):
  Bug 1 — kelly_mult scope: el log final usaba dir() para comprobar si
    kelly_mult estaba definida, pero dir() no incluye variables locales.
    Resultado: siempre mostraba Kelly=1.00x aunque Kelly ajustara el size.
    Fix: inicializar kelly_mult = 1.0 antes del bloque try/except de Kelly.
  Bug 2 — place_tp firma incorrecta: se llamaba con 5 args posicionales
    (not is_buy, qty, tp1_px, None, filled_price) pero la firma real es
    place_tp(is_buy, sz, trigger_px, entry_px) — 4 args. El None extra
    causaba TypeError en runtime. Fix: eliminar el None intermedio.

FIX get_ohlcv backoff exponencial (2026-06-05):
  Sustituye el retry único con 1 espera fija por un loop de hasta
  OHLCV_FETCH_RETRIES (default 3) intentos con backoff exponencial
  2^i + jitter ±_OHLCV_FETCH_JITTER_S (default 0.3s) y nueva sesión
  aiohttp en cada intento. OHLCV_MAX_CONCURRENCY default 5 → 3.

FIX get_price backoff (2026-06-05):
  Ampliado de 1 retry a PRICE_FETCH_RETRIES (default 3) con esperas
  crecientes (0.4s, 0.8s, 1.6s) antes de caer al stale cache.
"""
from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import random
import time
from typing import Callable, Optional

import aiohttp

from bot.core.hl_client import HLClient, _norm_coin
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

_OHLCV_BARS          = int(os.getenv("BARS_NEEDED",           "100"))
# OHLCV_MAX_CONCURRENCY ahora vive en ohlcv_cache.py (env HL_OHLCV_CONCURRENCY).
# Se mantiene esta var sólo para no romper env vars existentes que la expongan;
# ohlcv_cache ignora este nombre y usa HL_OHLCV_CONCURRENCY.
_OHLCV_MAX_CONCURRENCY = int(os.getenv("OHLCV_MAX_CONCURRENCY", "3"))

_PRICE_FETCH_RETRIES = int(os.getenv("PRICE_FETCH_RETRIES",   "3"))

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
        self._last_price:     float           = 0.0  # caché del último precio válido

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
            logger.info("[%s] Inicializando HLClient (async)…", self.symbol)
            self._hl_client = await HLClient.create(
                self._api_key, self._api_secret, self.coin
            )
            self._master_addr = getattr(self._hl_client, "master_address", "") or ""
            self._agent_mode  = getattr(self._hl_client, "agent_mode",     False)
            logger.info(
                "[%s] HLClient listo — addr=%s agent=%s",
                self.symbol, self._master_addr[:10] + "…" if self._master_addr else "N/A",
                self._agent_mode,
            )
        except Exception as e:
            logger.error("[%s] _get_ccxt error: %s", self.symbol, e)
            raise

    # ── OHLCV: delega en ohlcv_cache singleton ────────────────────────

    async def get_ohlcv(self, timeframe: str, n: Optional[int] = None) -> list:
        """
        Devuelve barras OHLCV para self.coin/timeframe.

        Delega completamente en ohlcv_cache.get() (bot/ohlcv_cache.py) que
        gestiona semáforo de concurrencia, backoff exponencial con jitter y
        stale fallback. No se duplica ninguna lógica de retry aquí.
        """
        bars_needed = n or _OHLCV_BARS

        async def _fetch(tf: str) -> list:
            return await self._fetch_candles(tf, bars_needed)

        return await ohlcv_cache.get(self.coin, timeframe, _fetch)

    async def _fetch_candles(self, timeframe: str, n: int) -> list:
        """
        Llamada HTTP directa a HL candleSnapshot — sin retry ni semáforo
        (ohlcv_cache se encarga de ambos).
        """
        tf_min = _TF_MINUTES.get(timeframe, 15)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - n * tf_min * 60 * 1000

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin":       self.coin,
                "interval":   timeframe,
                "startTime":  start_ms,
                "endTime":    end_ms,
            },
        }

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{_API_URL}/info", json=payload
            ) as resp:
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
                    int(c["t"]),
                    float(c["o"]),
                    float(c["h"]),
                    float(c["l"]),
                    float(c["c"]),
                    float(c["v"]),
                ])
            except (KeyError, TypeError, ValueError):
                continue

        return result
