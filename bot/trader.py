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

FIX get_ohlcv None (2026-06-03 / 2026-06-05 v2):
  Hyperliquid candleSnapshot devuelve null (Python None) cuando startTime
  es demasiado antiguo o el request falla silenciosamente.
  Fixes:
    - Reducir startTime: n = BARS_NEEDED (sin el +20 extra).
    - Guard: si raw is None, retry exponencial (hasta 3 intentos con
      ventana reducida: 100% → 66% → 33%).
    - Log del status HTTP y body truncado cuando no es lista.
    - CORRECCIÓN BUG: faltaba 'timeframe' como argumento en logger del except.
    - WARNING de retry → DEBUG (es comportamiento normal intermitente).

FIX get_ohlcv WARNING spam (2026-06-05 v3):
  Coins sin datos OHLCV (ZEC y otras poco líquidas) emitían un WARNING
  en CADA ciclo de scan (~cada 10s), creando spam masivo en los logs.
  Fix:
    - _OHLCV_NO_DATA_COINS: set global que registra coins que fallaron
      los 3 intentos de get_ohlcv().
    - Primera vez que un coin entra en el set → WARNING (aviso único).
    - Fallos subsiguientes del mismo coin → DEBUG (sin spam).
    - El set se resetea cada _OHLCV_NO_DATA_RESET_INTERVAL segundos
      (default 1800s = 30 min) para reintentar coins que puedan haber
      ganado liquidez.

FIX _fetch_candles endTime vela abierta (2026-06-05 v4):
  CAUSA RAÍZ de candleSnapshot devolviendo [] para coins con datos:
  endTime = now() caía dentro de la vela actual (aún no cerrada).
  Hyperliquid devuelve [] silenciosamente cuando endTime está en una
  vela abierta. Fix: endTime retrocede 1 intervalo completo para que
  siempre apunte a la última vela cerrada.

FIX _get_positions NoneType (2026-06-03):
  Cuando _master_addr está vacío (init incompleto) o HL devuelve error JSON,
  data es None o un dict sin 'assetPositions'. Fix:
    - Guard early-return si _master_addr vacío.
    - if data is None: log + return [].
    - try/except TypeError alrededor de data.get().

FIX NameError aiohttp (2026-06-03):
  _fetch_candles usaba aiohttp.ClientTimeout pero el import estaba solo
  dentro de get_ohlcv (scope distinto). Movido al nivel de módulo.

FIX get_price NoneType (2026-06-03 / 2026-06-05 v2):
  Si HL devuelve null, un error HTTP, o un body no-dict (e.g. string de error),
  data.get(self.coin) lanzaba 'NoneType object has no attribute get'.
  Fix: guard isinstance(data, dict) antes de llamar .get().
  Si data no es dict → raise ValueError con el body truncado para diagnóstico.
  v2: WARNING → DEBUG cuando hay caché válida; WARNING solo en cold-start sin precio.

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

FIX allMids NoneType — retry + caché último precio (2026-06-04 / 2026-06-05 v2):
  Cuando HL devuelve null en allMids (cold-start o saturación puntual),
  get_price() ahora:
    1. Reintenta 1 vez tras 0.4s si data no es dict.
    2. Si sigue fallando, devuelve self._last_price (último precio válido
       cacheado) en lugar de propagar la excepción — el tick se procesa
       con el precio anterior.
    3. Si _last_price == 0 (primer arranque y falla) → propaga excepción.
    4. Cada llamada exitosa actualiza self._last_price.
    5. v2: los logs de uso de caché son DEBUG, no WARNING, para evitar
       spam en operación normal con pequeñas interrupciones de red.

FIX semáforo global HL + jitter anti-thundering-herd (2026-06-05 v5):
  CAUSA RAÍZ de los null en clearinghouseState y candleSnapshot:
  Todos los traders hacían sus llamadas a /info en paralelo sin ningún
  límite global — el semáforo previo solo cubría get_ohlcv(), dejando
  _get_positions, _get_open_orders_raw, _get_open_trigger_orders_raw
  e _info_post completamente sin restricción.
  Con N traders × M endpoints simultáneos, HL devuelve null en vez de 429.
  Fixes:
    1. _HL_SEMAPHORE: semáforo GLOBAL que cubre TODAS las llamadas a /info.
       Límite configurable via HL_CONCURRENCY (default 4).
       get_ohlcv() pasa a usar este semáforo global en vez del antiguo
       _OHLCV_SEMAPHORE (OHLCV_MAX_CONCURRENCY queda como alias retrocompat).
    2. Jitter en TradingLoop._init(): cada trader espera un retardo
       aleatorio de 0–HL_JITTER_MAX_S (default 3s) antes de empezar
       su primer ciclo. Evita que todos los loops arranquen en t=0
       y hagan poll simultáneo en el mismo segundo.
    3. Los WARNING de respuesta null en _get_positions pasan a DEBUG
       cuando el semáforo global está activo (son esperables bajo carga).

FIX duplicate nonce _set_leverage (2026-06-05 v6):
  CAUSA RAÍZ: _set_leverage llamaba hl._exchange.update_leverage()
  directamente via asyncio.to_thread(), sin pasar por _exchange_call()
  ni adquirir _EXCHANGE_LOCK. Con 7 traders terminando _get_ccxt() en
  el mismo milisegundo, todos llamaban update_leverage simultáneamente
  → colisión de nonce garantizada → HL rechaza con 'duplicate nonce'.
  Fix: usar hl.update_leverage(leverage) que internamente envuelve la
  llamada con _exchange_call() → _EXCHANGE_LOCK + _NONCE_MIN_DELAY_MS.

FIX _set_leverage auto-capping interno (2026-06-05 v7):
  CAUSA RAÍZ: En rotaciones de PairScanner, BitgetBot arranca traders
  nuevos (AAVE, INJ, TAO, DOGE, GRASS) con leverage=15x porque el
  snapshot de maxLeverage no incluía esos coins aún. HL rechaza con
  'Invalid leverage value' porque su maxLeverage real es inferior a 15x.
  Fix: _set_leverage consulta hl.get_max_leverage(self.coin) antes de
  llamar a update_leverage y cappa el valor automáticamente. Si falla
  la consulta, usa el valor solicitado como fallback. Actualiza
  self.leverage con el valor efectivo para que open_order use el real.
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
from bot.state import save_position

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

_OHLCV_BARS = int(os.getenv("BARS_NEEDED", "100"))

# ── Semáforo GLOBAL para TODAS las llamadas a /info ─────────────────────────
# Cubre: get_ohlcv, _get_positions, _get_open_orders_raw,
#        _get_open_trigger_orders_raw, _info_post y get_price.
# Configurable con HL_CONCURRENCY (default 4).
# OHLCV_MAX_CONCURRENCY se mantiene como alias retrocompatible: si se define,
# sobreescribe HL_CONCURRENCY para no romper configuraciones existentes.
_HL_CONCURRENCY = int(
    os.getenv("OHLCV_MAX_CONCURRENCY",  # alias retrocompat
    os.getenv("HL_CONCURRENCY", "4"))
)
_OHLCV_MAX_CONCURRENCY = _HL_CONCURRENCY  # alias para código legado

# Jitter de arranque: cada trader espera entre 0 y HL_JITTER_MAX_S segundos
# antes de su primera iteración para evitar thundering herd en t=0.
_HL_JITTER_MAX_S = float(os.getenv("HL_JITTER_MAX_S", "3.0"))

# Esperas (segundos) entre reintentos OHLCV: intento 1→2 y 2→3.
_OHLCV_RETRY_DELAYS_RAW = os.getenv("OHLCV_RETRY_DELAYS", "0.5,1.5")
try:
    _OHLCV_RETRY_DELAYS = [float(x) for x in _OHLCV_RETRY_DELAYS_RAW.split(",") if x.strip()]
except Exception:
    _OHLCV_RETRY_DELAYS = [0.5, 1.5]

# ── Supresión de spam WARNING para coins sin datos OHLCV ────────────────────
_OHLCV_NO_DATA_COINS: set[str] = set()
_OHLCV_NO_DATA_RESET_INTERVAL = float(os.getenv("OHLCV_NO_DATA_RESET_INTERVAL", "1800"))
_OHLCV_NO_DATA_LAST_RESET: float = time.monotonic()

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

# Semáforo global — inicializado lazy dentro del event loop.
_HL_SEMAPHORE: Optional[asyncio.Semaphore] = None

# Alias retrocompat — apunta al mismo objeto que _HL_SEMAPHORE.
_OHLCV_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _get_hl_semaphore() -> asyncio.Semaphore:
    """Devuelve el semáforo global HL, creándolo lazy la primera vez."""
    global _HL_SEMAPHORE, _OHLCV_SEMAPHORE
    if _HL_SEMAPHORE is None:
        _HL_SEMAPHORE = asyncio.Semaphore(_HL_CONCURRENCY)
        _OHLCV_SEMAPHORE = _HL_SEMAPHORE  # mismo objeto
        logger.info(
            "[HLSemaphore] Inicializado: max_concurrency=%d "
            "(cubre get_ohlcv + _get_positions + get_price + orders)",
            _HL_CONCURRENCY,
        )
    return _HL_SEMAPHORE


# Alias retrocompat para código que llamara a _get_ohlcv_semaphore()
_get_ohlcv_semaphore = _get_hl_semaphore


def _ohlcv_no_data_log(coin: str, timeframe: str, n_attempts: int) -> None:
    global _OHLCV_NO_DATA_COINS, _OHLCV_NO_DATA_LAST_RESET
    now = time.monotonic()
    if now - _OHLCV_NO_DATA_LAST_RESET > _OHLCV_NO_DATA_RESET_INTERVAL:
        if _OHLCV_NO_DATA_COINS:
            logger.info(
                "[OHLCVCache] Reset de coins sin datos (interval=%.0fs): %s",
                _OHLCV_NO_DATA_RESET_INTERVAL,
                ", ".join(sorted(_OHLCV_NO_DATA_COINS)),
            )
        _OHLCV_NO_DATA_COINS = set()
        _OHLCV_NO_DATA_LAST_RESET = now

    key = f"{coin}:{timeframe}"
    if key not in _OHLCV_NO_DATA_COINS:
        _OHLCV_NO_DATA_COINS.add(key)
        logger.warning(
            "[%s] get_ohlcv(%s) sin datos tras %d intentos — "
            "coin posiblemente sin liquidez en HL. "
            "Próximos fallos silenciados (DEBUG) durante %.0f min.",
            coin, timeframe, n_attempts,
            _OHLCV_NO_DATA_RESET_INTERVAL / 60,
        )
    else:
        logger.debug(
            "[%s] get_ohlcv(%s) sin datos (reintento suprimido — coin sin liquidez).",
            coin, timeframe,
        )


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

        self._hl_client: Optional[HLClient] = None
        self._master_addr: str = ""
        self._agent_mode:  bool = False

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)
        self._ccxt_exchange  = None

    # ── Interfaz pública requerida por main.py ──────────────────

    async def run(self, risk, *, global_risk=None) -> None:
        # Jitter anti-thundering-herd: cada trader arranca en un momento
        # ligeramente distinto para evitar que todos hagan poll a HL
        # en el mismo instante (causa principal de respuestas null).
        if _HL_JITTER_MAX_S > 0:
            jitter = random.uniform(0, _HL_JITTER_MAX_S)
            logger.debug(
                "[%s] Jitter de arranque: %.2fs (max=%.1fs)",
                self.symbol, jitter, _HL_JITTER_MAX_S,
            )
            await asyncio.sleep(jitter)

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

    async def _fetch_all_mids(self, session: aiohttp.ClientSession) -> Optional[dict]:
        """Llama al endpoint allMids y devuelve el dict o None si falla."""
        try:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                text = await resp.text()

            try:
                data = _json.loads(text)
            except _json.JSONDecodeError:
                logger.debug(
                    "[%s] allMids: respuesta no-JSON (status=%s): %s",
                    self.symbol, resp.status, text[:200],
                )
                return None

            if isinstance(data, dict):
                return data

            logger.debug(
                "[%s] allMids: tipo inesperado (%s): %s",
                self.symbol, type(data).__name__, text[:120],
            )
            return None
        except asyncio.TimeoutError:
            logger.debug("[%s] allMids: timeout", self.symbol)
            return None
        except Exception as e:
            logger.debug("[%s] allMids error: %s", self.symbol, e)
            return None

    async def get_price(self) -> float:
        """
        Obtiene el precio mid de self.coin vía allMids.
        Usa el semáforo global para no saturar HL.
        """
        sem = _get_hl_semaphore()
        async with sem:
            async with aiohttp.ClientSession() as session:
                data = await self._fetch_all_mids(session)

                if data is None:
                    await asyncio.sleep(0.4)
                    data = await self._fetch_all_mids(session)

        if data is None:
            if self._last_price > 0:
                logger.debug(
                    "[%s] allMids no disponible — usando último precio en caché: %.4f",
                    self.symbol, self._last_price,
                )
                return self._last_price
            logger.warning(
                "[%s] allMids devolvió tipo inesperado (NoneType): null — sin caché disponible",
                self.symbol,
            )
            raise ValueError(
                f"[{self.symbol}] allMids no disponible y sin precio en caché"
            )

        price = data.get(self.coin)
        if price is None:
            if self._last_price > 0:
                logger.debug(
                    "[%s] Precio no encontrado en allMids — usando caché: %.4f",
                    self.symbol, self._last_price,
                )
                return self._last_price
            raise ValueError(f"[{self.symbol}] Precio no encontrado en allMids")

        result = float(price)
        self._last_price = result
        return result

    async def _fetch_candles(self, session: aiohttp.ClientSession, timeframe: str, n_bars: int) -> Optional[list]:
        interval = _TF_MINUTES.get(timeframe, 15)
        interval_ms = interval * 60 * 1000
        end_ms   = int(time.time() * 1000) - interval_ms
        start_ms = end_ms - (n_bars * interval_ms)

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin":      self.coin,
                "interval":  timeframe,
                "startTime": start_ms,
                "endTime":   end_ms,
            },
        }
        try:
            async with session.post(
                f"{_API_URL}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                text = await resp.text()
                try:
                    return _json.loads(text)
                except _json.JSONDecodeError:
                    logger.debug(
                        "[%s] get_ohlcv(%s): respuesta no-JSON (status=%s): %s",
                        self.symbol, timeframe, resp.status, text[:200],
                    )
                    return None
        except asyncio.TimeoutError:
            logger.debug("[%s] get_ohlcv(%s): timeout en _fetch_candles", self.symbol, timeframe)
            return None
        except Exception as e:
            logger.debug("[%s] get_ohlcv(%s): error en _fetch_candles: %s", self.symbol, timeframe, e)
            return None

    async def get_ohlcv(self, timeframe: str) -> list:
        """
        Descarga velas OHLCV con reintentos exponenciales.
        Usa el semáforo global _HL_SEMAPHORE para no saturar HL.
        """
        n_bars_sequence = [
            _OHLCV_BARS,
            max(20, _OHLCV_BARS * 2 // 3),
            max(10, _OHLCV_BARS // 3),
        ]
        sem = _get_hl_semaphore()

        raw = None
        try:
            async with sem:
                async with aiohttp.ClientSession() as session:
                    for attempt, n_bars in enumerate(n_bars_sequence):
                        raw = await self._fetch_candles(session, timeframe, n_bars)

                        if isinstance(raw, list) and len(raw) > 0:
                            break

                        if attempt < len(n_bars_sequence) - 1:
                            delay = _OHLCV_RETRY_DELAYS[attempt] if attempt < len(_OHLCV_RETRY_DELAYS) else 1.0
                            logger.debug(
                                "[%s] get_ohlcv(%s) intento %d/%d: respuesta no válida "
                                "(tipo=%s) — reintentando con %d barras en %.1fs...",
                                self.symbol, timeframe,
                                attempt + 1, len(n_bars_sequence),
                                type(raw).__name__, n_bars_sequence[attempt + 1], delay,
                            )
                            await asyncio.sleep(delay)

        except Exception as e:
            logger.warning("[%s] get_ohlcv(%s) excepción inesperada: %s", self.symbol, timeframe, e)
            return []

        if raw is None or not isinstance(raw, list) or len(raw) == 0:
            _ohlcv_no_data_log(self.coin, timeframe, len(n_bars_sequence))
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

        sem = _get_hl_semaphore()
        try:
            async with sem:
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
            # Con semáforo global activo esto ya no debería ocurrir;
            # si ocurre es un problema real de HL, no de concurrencia nuestra.
            logger.debug("[%s] _get_positions: respuesta null de HL API.", self.symbol)
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
        sem = _get_hl_semaphore()
        try:
            async with sem:
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
        TRIGGER ORDERS que viven en el endpoint 'frontendOpenOrders'.
        """
        if not self._master_addr:
            return []
        sem = _get_hl_semaphore()
        try:
            async with sem:
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
        """
        Configura el leverage en HL con auto-capping interno (v7).

        Antes de llamar a update_leverage, consulta el maxLeverage real
        del coin en el caché de HLClient. Si el leverage solicitado lo
        supera, lo cappa automáticamente y actualiza self.leverage con
        el valor efectivo. Esto garantiza que la llamada nunca falle con
        'Invalid leverage value', incluso cuando BitgetBot arranca traders
        nuevos en rotaciones sin snapshot previo para ese coin.
        """
        hl = self._require_hl()
        if hl is None:
            return

        if self.dry_run:
            logger.info("[%s] DRY_RUN: _set_leverage(%d) omitido.", self.symbol, leverage)
            return

        # ── Auto-capping: consultar maxLeverage real del coin ────────────
        effective_leverage = leverage
        try:
            max_lev = await asyncio.to_thread(hl.get_max_leverage, self.coin)
            if max_lev and max_lev > 0 and leverage > max_lev:
                logger.info(
                    "[%s] ⚙️  Leverage capado internamente: %dx → %dx (max=%dx, fuente=HLClient cache)",
                    self.symbol, leverage, max_lev, max_lev,
                )
                effective_leverage = max_lev
        except Exception as e:
            logger.debug(
                "[%s] _set_leverage: no se pudo consultar maxLeverage (%s) — usando %dx sin capping",
                self.symbol, e, leverage,
            )

        try:
            # FIX duplicate nonce (v6): usar hl.update_leverage() que envuelve
            # la llamada con _exchange_call() → _EXCHANGE_LOCK + _NONCE_MIN_DELAY_MS.
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    hl.update_leverage,
                    effective_leverage,
                ),
                timeout=15.0,
            )
            logger.info("[%s] Leverage configurado a %dx: %s", self.symbol, effective_leverage, result)

            # Actualizar self.leverage con el valor efectivo real para que
            # open_order calcule el notional correcto.
            if effective_leverage != self.leverage:
                self.leverage = effective_leverage
                self._open_leverage = effective_leverage

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

        sem = _get_hl_semaphore()
        async with sem:
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
