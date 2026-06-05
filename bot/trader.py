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
    2. _get_ccxt(): crea _hl_client vía HLClient.create(symbol) (async) la
       primera vez que se llama. TradingLoop.run() invoca _get_ccxt() desde
       _init() dentro del event loop, por lo que es seguro awaitar.
       Las credenciales NO se pasan — _HLCore las lee de las variables de
       entorno (HL_API_PRIVATE_KEY, HL_API_WALLET_ADDRESS) directamente.
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
  (dentro del event loop), evitando el error de \"attached to a different loop\".

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
  Fix: usar hl.set_leverage(leverage) que internamente envuelve la
  llamada con _exchange_call() → _EXCHANGE_LOCK + _NONCE_MIN_DELAY_MS.

FIX _set_leverage auto-capping interno (2026-06-05 v7):
  CAUSA RAÍZ: En rotaciones de PairScanner, BitgetBot arranca traders
  nuevos (AAVE, INJ, TAO, DOGE, GRASS) con leverage=15x porque el
  snapshot de maxLeverage no incluía esos coins aún. HL rechaza con
  'Invalid leverage value' porque su maxLeverage real es inferior a 15x.
  Fix: _set_leverage consulta hl.get_max_leverage() antes de llamar a
  set_leverage y cappa el valor automáticamente. Si falla la consulta,
  usa el valor solicitado como fallback. Actualiza self.leverage con el
  valor efectivo para que open_order use el real.

FIX start() TradingLoop kwargs (2026-06-05 v8):
  CAUSA RAÍZ: start() instanciaba TradingLoop(trader=self, symbol=...,
  signal_fn=...) pero TradingLoop.__init__ solo acepta symbol:str.
  Además llamaba self._loop.run() sin argumentos, cuando run() requiere
  (trader, risk, *, global_risk=None).
  Fix:
    1. TradingLoop se instancia solo con symbol: TradingLoop(self.symbol).
    2. run() recibe trader=self y un objeto risk mínimo con usdc_per_trade.
    3. signal_fn ya no se pasa a TradingLoop — DecisionEngine lo gestiona
       internamente a través de signal_engine.

FIX HLClient.create() kwargs + get_max_leverage() signature (2026-06-05 v9):
  CAUSA RAÍZ 1: _get_ccxt() llamaba HLClient.create(master_addr=...,
  private_key=..., agent_key=..., agent_addr=...) pero la firma real es
  HLClient.create(symbol: str). Los kwargs no existen — _HLCore lee las
  credenciales directamente de las variables de entorno. Esto causaba
  TypeError en cada trader al intentar inicializar el SDK.
  Fix: llamar simplemente HLClient.create(self.symbol).

  CAUSA RAÍZ 2: _set_leverage() llamaba hl.get_max_leverage(self.symbol)
  pero get_max_leverage() no acepta argumentos — usa self.coin interno.
  El TypeError quedaba silenciado por el except: pass y el leverage nunca
  se capaba, causando 'Invalid leverage value' para AAVE, INJ, TAO, DOGE,
  GRASS y cualquier coin con maxLev < 15x en rotaciones de PairScanner.
  Fix: llamar hl.get_max_leverage() sin argumentos.

FIX _get_positions list vs dict + warning agent_mode=False (2026-06-05 v10):
  CAUSA RAÍZ: HLClient.get_positions() ya devuelve una list[dict] filtrada
  por coin (no un dict con clave 'assetPositions'). El código anterior en
  _get_positions() hacía data.get(\"assetPositions\", []) sobre esa lista,
  lo que lanzaba 'list' object has no attribute 'get' en CADA ciclo de
  todos los traders, paralizando el bot al 100% con error: 4 en decision_engine.
  Esto ocurre especialmente cuando agent_mode=False (wallet directa sin
  agente autorizado), donde el SDK puede comportarse de forma distinta.
  Fixes:
    1. _get_positions(): simplifica la lógica — hl.get_positions() ya
       devuelve list directamente. Si por compatibilidad futura recibe
       un dict, extrae assetPositions. Cualquier otro tipo → WARNING + [].
    2. _get_ccxt(): loguea WARNING visible si agent_mode=False para alertar
       que el bot opera sin agente Hyperliquid activo — facilita diagnóstico
       de expiración o revocación del agente wallet.

FIX _set_leverage AttributeError update_leverage (2026-06-05 v11):
  CAUSA RAÍZ: _set_leverage llamaba hl.update_leverage() pero HLClient
  expone el método como set_leverage(). El AttributeError causaba que
  todos los traders fallaran en init, resultando en agent_mode=False
  y analyze_pair error: 4 en decision_engine — bot completamente inoperativo.
  Fix: renombrar la llamada a hl.set_leverage(effective_leverage).

FIX _master_addr y _agent_mode nunca populados (2026-06-05 v12):
  CAUSA RAÍZ: self._master_addr = master_addr y self._agent_mode = bool(agent_key)
  en __init__ siempre quedaban "" / False porque main.py NO pasa esos kwargs —
  _HLCore ya lee las credenciales de las env vars directamente.
  Consecuencias:
    - _get_positions() siempre devolvía [] (guard 'if not self._master_addr').
    - TradingLoop logueaba siempre 'master=N/A | agent_mode=False'.
    - balance_svc.init_hl() recibía master_addr="" y nunca se inicializaba.
  Fix:
    _get_ccxt(), tras crear HLClient con éxito, sincroniza:
      self._master_addr = hl._account_addr   (wallet principal de _HLCore)
      self._agent_addr  = hl._agent_addr
      self._agent_mode  = hl._agent_mode
    Así el resto del código (trading_loop, _get_positions, balance_svc)
    recibe los valores reales leídos de las env vars.
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
    """Inicializa el semáforo global la primera vez (debe llamarse desde el event loop)."""
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


def _check_price_staleness(signal: dict, ref_price: float, is_long: bool) -> str | None:
    """
    Comprueba si el precio actual ha derivado demasiado respecto al
    precio de señal. Devuelve un string describiendo el problema si
    la entrada debe cancelarse, None si todo está bien.
    """
    entry_signal = float(signal.get("entry") or 0)
    if entry_signal <= 0 or ref_price <= 0:
        return None  # sin info suficiente para rechazar

    drift = (ref_price - entry_signal) / entry_signal
    if is_long and drift < -_MAX_ENTRY_DRIFT_PCT:
        return (
            f"precio cayó {drift*100:.2f}% vs señal "
            f"(ref={ref_price:.4f} entry_signal={entry_signal:.4f} "
            f"threshold={-_MAX_ENTRY_DRIFT_PCT*100:.1f}%)"
        )
    if not is_long and drift > _MAX_ENTRY_DRIFT_PCT:
        return (
            f"precio subió {drift*100:.2f}% vs señal "
            f"(ref={ref_price:.4f} entry_signal={entry_signal:.4f} "
            f"threshold=+{_MAX_ENTRY_DRIFT_PCT*100:.1f}%)"
        )
    return None


def _adjust_levels_to_fill(
    signal: dict,
    filled_price: float,
    ref_price: float,
) -> tuple[float, float, float]:
    """
    Re-escala SL, TP1, TP2 al precio real de fill.

    Si el fill es muy cercano al precio de señal (drift < 0.05%),
    usa los valores de señal sin ajuste para evitar ruido numérico.
    Si el drift es mayor, escala proporcionalmente cada nivel.
    """
    base_px = float(signal.get("entry") or ref_price)
    sl_raw  = float(signal.get("sl")  or 0)
    tp1_raw = float(signal.get("tp1") or 0)
    tp2_raw = float(signal.get("tp2") or 0)

    if base_px <= 0 or filled_price <= 0:
        return sl_raw, tp1_raw, tp2_raw

    drift = abs(filled_price - base_px) / base_px
    if drift < 0.0005:
        return sl_raw, tp1_raw, tp2_raw

    def scale(level: float) -> float:
        if level <= 0:
            return 0.0
        pct = (level - base_px) / base_px
        return filled_price * (1.0 + pct)

    return scale(sl_raw), scale(tp1_raw), scale(tp2_raw)


try:
    from bot.config import BARS_NEEDED
except ImportError:
    BARS_NEEDED = _OHLCV_BARS


class _RiskProxy:
    """
    Objeto mínimo compatible con la interfaz que TradingLoop.run() espera
    de 'risk': solo necesita el atributo usdc_per_trade.

    Se crea en FuturesTrader.start() para no acoplar trader.py a la
    clase RiskManager concreta de main.py.
    """
    __slots__ = ("usdc_per_trade",)

    def __init__(self, usdc_per_trade: float) -> None:
        self.usdc_per_trade = usdc_per_trade


class FuturesTrader:
    """
    Trader asíncrono para un único par en Hyperliquid.

    Responsabilidades:
      - Gestionar el ciclo de vida del TradingLoop para un símbolo.
      - Envolver todas las llamadas al SDK de HL en asyncio.to_thread().
      - Mantener el estado de la posición abierta (entry, sl, tp1/2/3).
      - Implementar open_order, _ensure_tpsl, close_position, etc.
    """

    def __init__(
        self,
        symbol:         str,
        leverage:       int,
        usdc_per_trade: float,
        signal_fn:      Callable,
        dry_run:        bool = False,
        master_addr:    str  = "",
        private_key:    str  = "",
        agent_key:      str  = "",
        agent_addr:     str  = "",
    ) -> None:
        self.symbol         = symbol
        self.coin           = symbol          # alias para compatibilidad con trading_loop._init()
        self.leverage       = leverage
        self.usdc_per_trade = usdc_per_trade
        self.signal_fn      = signal_fn
        self.dry_run        = dry_run

        # Estos valores se mantienen como fallback si main.py los pasa explícitamente,
        # pero en la práctica siempre quedan "" / False porque _HLCore lee las
        # credenciales directamente de las env vars.
        # _get_ccxt() los sobreescribe con los valores reales del HLClient tras crearlo.
        self._master_addr = master_addr
        self._private_key = private_key
        self._agent_key   = agent_key
        self._agent_addr  = agent_addr
        self._agent_mode  = bool(agent_key)   # se sobreescribe en _get_ccxt()

        # Estado de posición
        self.position:    Optional[str]   = None
        self.entry_price: float           = 0.0
        self.sl:          Optional[float] = None
        self.tp1:         Optional[float] = None
        self.tp2:         Optional[float] = None
        self.tp3:         Optional[float] = None
        self.tp2_hit:     bool            = False
        self._open_notional: float        = 0.0
        self._open_leverage: int          = leverage
        self._open_qty:      float        = 0.0
        self._protection_ok: bool         = False
        self._tp1_be_done:   bool         = False
        self._last_price:    float        = 0.0

        # SDK — se crea lazy en _get_ccxt() para no bloquear __init__.
        self._hl_client: Optional[HLClient] = None

        # TradingLoop — se crea en start() después de que el event loop
        # esté activo, para que asyncio.Queue() se cree en el loop correcto.
        self._loop: Optional[TradingLoop] = None

    # ─────────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ─────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Arranca el TradingLoop en background.

        TradingLoop.__init__ solo acepta symbol:str.
        TradingLoop.run() acepta (trader, risk, *, global_risk=None).
        """
        self._loop = TradingLoop(self.symbol)
        risk = _RiskProxy(self.usdc_per_trade)
        await self._loop.run(self, risk)

    async def stop(self) -> None:
        """Para el TradingLoop limpiamente."""
        if self._loop:
            await self._loop.stop()
            self._loop = None
            logger.info("[%s] Trader parado limpiamente.", self.symbol)

    def cancel(self) -> None:
        """Cancela la task del TradingLoop (para parada forzada)."""
        if self._loop:
            self._loop.cancel()

    def get_ohlcv_fn(self):
        """Devuelve un callable async para obtener OHLCV de este símbolo."""
        return self.get_ohlcv

    # ─────────────────────────────────────────────────────────────────────────
    # Inicialización lazy del SDK
    # ─────────────────────────────────────────────────────────────────────────

    async def _get_ccxt(self) -> Optional[HLClient]:
        """
        Inicializa HLClient la primera vez que se llama.
        Siempre se invoca desde dentro del event loop (a través de _init()),
        por lo que es seguro usar await / asyncio.to_thread aquí.

        IMPORTANTE: HLClient.create() solo acepta symbol:str.
        Las credenciales (HL_API_PRIVATE_KEY, HL_API_WALLET_ADDRESS) las
        lee _HLCore directamente de las variables de entorno — NO se pasan
        aquí como argumentos.

        FIX v12: tras crear HLClient, sincroniza self._master_addr,
        self._agent_addr y self._agent_mode desde los atributos reales
        del HLClient (que a su vez vienen de _HLCore y las env vars).
        Esto corrige que _get_positions() siempre devolviera [] por el
        guard 'if not self._master_addr' y que TradingLoop logueara
        siempre 'master=N/A | agent_mode=False'.
        """
        if self._hl_client is not None:
            return self._hl_client

        logger.info("[%s] _get_ccxt: inicializando HLClient...", self.symbol)
        try:
            self._hl_client = await HLClient.create(self.symbol)

            # ── FIX v12: sincronizar credenciales desde HLClient ──────────
            # _HLCore lee HL_API_PRIVATE_KEY / HL_API_WALLET_ADDRESS de las
            # env vars. Los parámetros del constructor (master_addr, agent_key)
            # son legacy y siempre llegan vacíos desde main.py actual.
            # Aquí se sobreescriben con los valores reales para que el resto
            # del código (trading_loop, _get_positions, balance_svc) funcione.
            self._master_addr = self._hl_client._account_addr
            self._agent_addr  = self._hl_client._agent_addr
            self._agent_mode  = self._hl_client._agent_mode

            agent_active = self._agent_mode
            logger.info(
                "[%s] _get_ccxt: HLClient listo | addr=%s | agente=%s",
                self.symbol,
                self._master_addr[:12] + "..." if self._master_addr else "N/A",
                agent_active,
            )
            if not agent_active:
                logger.warning(
                    "[%s] ⚠️  AGENTE INACTIVO — operando con master wallet directamente. "
                    "Verificar que el agente esté autorizado en app.hyperliquid.xyz "
                    "(clave puede haber expirado o sido revocada).",
                    self.symbol,
                )
        except Exception as e:
            logger.error("[%s] _get_ccxt: error inicializando HLClient: %s", self.symbol, e, exc_info=True)
            raise

        return self._hl_client

    def _require_hl(self) -> Optional[HLClient]:
        """Devuelve _hl_client o None si no está inicializado aún."""
        return self._hl_client

    # ─────────────────────────────────────────────────────────────────────────
    # Métodos de acceso al exchange
    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch_candles(
        self,
        coin:      str,
        timeframe: str,
        n:         int,
    ) -> list[dict]:
        """
        Descarga velas OHLCV de Hyperliquid usando la API REST de candleSnapshot.

        Calcula endTime retrocediendo 1 intervalo desde ahora para asegurarse
        de que siempre apunta a la última vela cerrada (HL devuelve [] si
        endTime cae dentro de la vela abierta actual).
        """
        tf_min = _TF_MINUTES.get(timeframe)
        if tf_min is None:
            raise ValueError(f"Timeframe desconocido: {timeframe!r}")

        interval_ms = tf_min * 60 * 1000
        # endTime = inicio de la vela abierta actual − 1ms (= cierre de la última vela cerrada)
        end_ms   = int(time.time() * 1000) - interval_ms
        start_ms = end_ms - n * interval_ms

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin":      _norm_coin(coin),
                "interval":  timeframe,
                "startTime": start_ms,
                "endTime":   end_ms,
            },
        }

        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{_API_URL}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                status = resp.status
                raw = await resp.json(content_type=None)

        if raw is None:
            raise ValueError(
                f"candleSnapshot devolvió null (status={status}) "
                f"coin={coin} tf={timeframe}"
            )
        if not isinstance(raw, list):
            body_preview = str(raw)[:200]
            raise ValueError(
                f"candleSnapshot respuesta inesperada (status={status}, "
                f"tipo={type(raw).__name__}): {body_preview}"
            )
        return raw

    async def get_ohlcv(
        self,
        timeframe: str = "15m",
        n:         int = 100,
    ) -> Optional[list[dict]]:
        """
        Devuelve las últimas n velas OHLCV o None si no hay datos.

        Lógica de reintentos:
          - Hasta 3 intentos con ventana progresivamente más pequeña
            (100% → 66% → 33% de n).
          - Esperas configurables entre reintentos (_OHLCV_RETRY_DELAYS).
          - Si los 3 intentos fallan, registra el coin en _OHLCV_NO_DATA_COINS
            (WARNING la primera vez, DEBUG las siguientes) y devuelve None.
        """
        global _OHLCV_NO_DATA_LAST_RESET, _OHLCV_NO_DATA_COINS

        # ── Reset periódico del set de coins sin datos ────────────────────
        now = time.monotonic()
        if now - _OHLCV_NO_DATA_LAST_RESET > _OHLCV_NO_DATA_RESET_INTERVAL:
            if _OHLCV_NO_DATA_COINS:
                logger.debug(
                    "[get_ohlcv] Reseteando lista de coins sin datos OHLCV "
                    "(%d coins) — reintentando.",
                    len(_OHLCV_NO_DATA_COINS),
                )
            _OHLCV_NO_DATA_COINS = set()
            _OHLCV_NO_DATA_LAST_RESET = now

        coin = _norm_coin(self.symbol)
        sem  = _get_hl_semaphore()

        multipliers = [1.0, 0.66, 0.33]
        last_err: Optional[Exception] = None

        for attempt, mult in enumerate(multipliers, start=1):
            n_req = max(1, int(n * mult))
            try:
                async with sem:
                    candles = await self._fetch_candles(coin, timeframe, n_req)
                if candles:
                    # Éxito — eliminar del set de coins problemáticos si estaba ahí
                    _OHLCV_NO_DATA_COINS.discard(coin)
                    return candles
                # Lista vacía (HL devolvió [])
                last_err = ValueError(f"lista vacía (attempt={attempt} n_req={n_req})")
                logger.debug(
                    "[%s] get_ohlcv(%s) lista vacía (attempt=%d/%d, n=%d) — reintentando...",
                    self.symbol, timeframe, attempt, len(multipliers), n_req,
                )
            except Exception as exc:
                last_err = exc
                logger.debug(
                    "[%s] get_ohlcv(%s) attempt %d/%d falló: %s",
                    self.symbol, timeframe, attempt, len(multipliers), exc,
                )

            if attempt < len(multipliers):
                delay_idx = attempt - 1
                delay = _OHLCV_RETRY_DELAYS[delay_idx] if delay_idx < len(_OHLCV_RETRY_DELAYS) else 1.0
                await asyncio.sleep(delay)

        # ── Todos los intentos fallaron ───────────────────────────────────
        first_time = coin not in _OHLCV_NO_DATA_COINS
        _OHLCV_NO_DATA_COINS.add(coin)

        log_fn = logger.warning if first_time else logger.debug
        log_fn(
            "[%s] get_ohlcv(%s) sin datos tras %d intentos — "
            "coin posiblemente sin liquidez en HL. "
            "Próximos fallos silenciados (DEBUG) durante %g min.",
            self.symbol, timeframe, len(multipliers),
            _OHLCV_NO_DATA_RESET_INTERVAL / 60,
        )
        return None

    async def get_ohlcv_exc(
        self,
        timeframe: str = "15m",
        n:         int = 100,
    ) -> list[dict]:
        """
        Versión de get_ohlcv que lanza excepción en vez de devolver None.
        Útil para callers que necesitan distinguir \"sin datos\" de \"error\".
        """
        try:
            result = await self.get_ohlcv(timeframe=timeframe, n=n)
        except Exception as e:
            logger.warning("[%s] get_ohlcv(%s) excepción inesperada: %s", self.symbol, timeframe, e, exc_info=True)
            return []
        if result is None:
            return []
        return result

    async def _get_positions(self) -> list[dict]:
        """
        Obtiene posiciones abiertas del exchange.
        Devuelve lista de dicts con 'coin', 'side', 'entryPx', 'szi', etc.

        HLClient.get_positions() ya devuelve una list[dict] filtrada por coin
        (no un dict — no llamar .get() sobre ella). Si por compatibilidad futura
        la respuesta fuera un dict, se extrae 'assetPositions'. Cualquier otro
        tipo inesperado → WARNING + [].

        NOTA: el guard 'if not self._master_addr' protege contra llamadas antes
        de que _get_ccxt() haya completado. Tras _get_ccxt(), _master_addr
        siempre contiene la dirección real leída de HL_API_WALLET_ADDRESS.
        """
        if not self._master_addr:
            return []

        hl = self._require_hl()
        if hl is None:
            return []

        sem = _get_hl_semaphore()
        try:
            async with sem:
                raw = await asyncio.to_thread(hl.get_positions)
        except Exception as e:
            logger.warning("[%s] _get_positions error: %s", self.symbol, e, exc_info=True)
            return []

        # Normalizar la respuesta — HL puede devolver list o dict según contexto
        if isinstance(raw, list):
            positions_raw = raw
        elif isinstance(raw, dict):
            positions_raw = raw.get("assetPositions", [])
        elif raw is None:
            logger.debug("[%s] _get_positions: HL devolvió null.", self.symbol)
            return []
        else:
            logger.warning(
                "[%s] _get_positions: respuesta inesperada (tipo=%s): %s",
                self.symbol, type(raw).__name__, str(raw)[:200],
            )
            return []

        result = []
        for item in positions_raw:
            try:
                # HLClient.get_positions() ya filtra por coin y szi != 0,
                # pero si llega el wrapper completo, extraemos el inner dict.
                pos = item.get("position", item) if isinstance(item, dict) else {}
                coin = pos.get("coin", "")
                szi  = float(pos.get("szi", 0))
                if szi == 0:
                    continue
                result.append({
                    "coin":    coin,
                    "side":    "long" if szi > 0 else "short",
                    "szi":     abs(szi),
                    "entryPx": float(pos.get("entryPx") or 0),
                })
            except (TypeError, AttributeError, ValueError) as e:
                logger.debug("[%s] _get_positions: error parseando posición: %s", self.symbol, e)
                continue
        return result

    async def _get_open_orders_raw(self) -> list[dict]:
        """Devuelve lista cruda de órdenes abiertas del exchange."""
        if not self._master_addr or not self._hl_client:
            return []
        sem = _get_hl_semaphore()
        try:
            async with sem:
                data = await asyncio.to_thread(self._hl_client.get_open_orders)
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e, exc_info=True)
            return []
        if not isinstance(data, list):
            logger.warning("[%s] _get_open_orders_raw respuesta inesperada: %s", self.symbol, type(data))
            return []
        return data

    async def _get_open_trigger_orders_raw(self) -> list[dict]:
        """Devuelve lista cruda de trigger orders abiertas (SL/TP en HL)."""
        if not self._master_addr or not self._hl_client:
            return []
        sem = _get_hl_semaphore()
        try:
            async with sem:
                data = await asyncio.to_thread(self._hl_client.get_open_trigger_orders)
        except Exception as e:
            logger.warning("[%s] _get_open_trigger_orders_raw error: %s", self.symbol, e, exc_info=True)
            return []
        if not isinstance(data, list):
            logger.debug("[%s] _get_open_trigger_orders_raw respuesta inesperada: %s",
                         self.symbol, type(data))
            return []
        return data

    async def _place_tpsl(
        self,
        is_long:   bool,
        qty:       float,
        sl_price:  Optional[float],
        tp_price:  Optional[float],
        ref_price: float,
    ) -> None:
        """Coloca SL y/o TP en el exchange."""
        hl = self._require_hl()
        if hl is None:
            return

        if sl_price and sl_price > 0:
            try:
                await asyncio.to_thread(
                    hl.place_sl,
                    not is_long,
                    qty,
                    sl_price,
                    ref_price,
                )
            except Exception as e:
                logger.error("[%s] _place_tpsl SL error: %s", self.symbol, e, exc_info=True)

        if tp_price and tp_price > 0:
            try:
                await asyncio.to_thread(
                    hl.place_tp,
                    not is_long,
                    qty,
                    tp_price,
                    None,
                    ref_price,
                )
            except Exception as e:
                logger.error("[%s] _place_tpsl TP error: %s", self.symbol, e, exc_info=True)

    def _round_qty(self, qty: float) -> float:
        """Redondea qty según las reglas de tamaño del exchange."""
        hl = self._require_hl()
        if hl is None:
            return qty
        return hl.round_sz(qty)

    async def _set_leverage(self, leverage: int) -> None:
        """
        Configura el apalancamiento en el exchange.

        Auto-capping interno: consulta hl.get_max_leverage() (sin args —
        usa self.coin internamente) antes de llamar a set_leverage y
        cappa automáticamente si el leverage solicitado supera el máximo
        permitido. Actualiza self.leverage con el valor efectivo para que
        open_order use el real.
        """
        hl = self._require_hl()
        if hl is None:
            return

        effective_leverage = leverage
        try:
            max_lev = await asyncio.to_thread(hl.get_max_leverage)
            if max_lev and max_lev < leverage:
                logger.info(
                    "[%s] _set_leverage: auto-capping %dx → %dx (maxLeverage=%d en HL)",
                    self.symbol, leverage, max_lev, max_lev,
                )
                effective_leverage = max_lev
        except Exception as e:
            logger.warning(
                "[%s] _set_leverage: no se pudo obtener maxLeverage (%s) — "
                "usando leverage solicitado %dx como fallback",
                self.symbol, e, leverage,
            )

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(hl.set_leverage, effective_leverage),  # FIX v11: era update_leverage
                timeout=15.0,
            )
            logger.info(
                "[%s] Leverage configurado a %dx: %s",
                self.symbol, effective_leverage, result,
            )
            self.leverage = effective_leverage  # actualizar estado interno
        except asyncio.TimeoutError:
            logger.warning("[%s] _set_leverage timeout (15s) — continuando sin confirmar leverage.", self.symbol)
        except Exception as e:
            logger.warning("[%s] No se pudo configurar leverage: %s", self.symbol, e, exc_info=True)

    async def _info_post(self, payload: dict) -> Optional[dict]:
        """POST genérico al endpoint /info de Hyperliquid."""
        sem = _get_hl_semaphore()
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with sem:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(
                        f"{_API_URL}/info",
                        json=payload,
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        return await resp.json(content_type=None)
        except Exception:
            return None

    async def get_price(self) -> float:
        """
        Obtiene el precio actual del activo.

        Con retry + caché:
          1. Intento normal.
          2. Si falla, reintento tras 0.4s.
          3. Si sigue fallando, devuelve _last_price (si > 0).
          4. Si _last_price == 0 (cold-start), propaga la excepción.

        Los logs de uso de caché son DEBUG (no spam en operación normal).
        """
        coin = _norm_coin(self.symbol)
        sem  = _get_hl_semaphore()

        for attempt in range(2):
            try:
                async with sem:
                    data = await self._info_post({"type": "allMids"})

                if not isinstance(data, dict):
                    raise ValueError(f"allMids respuesta inesperada: {str(data)[:100]}")

                raw = data.get(coin)
                if raw is None:
                    raise ValueError(f"coin {coin!r} no encontrado en allMids")

                price = float(raw)
                self._last_price = price
                return price

            except Exception as exc:
                if attempt == 0:
                    logger.debug(
                        "[%s] get_price intento 1 falló (%s) — reintentando en 0.4s...",
                        self.symbol, exc,
                    )
                    await asyncio.sleep(0.4)
                else:
                    if self._last_price > 0:
                        logger.debug(
                            "[%s] get_price falló ambos intentos (%s) — usando caché %.4f",
                            self.symbol, exc, self._last_price,
                        )
                        return self._last_price
                    raise

        # Nunca se llega aquí, pero satisface el type checker
        raise RuntimeError("get_price: bucle terminó sin devolver precio")

    # ─────────────────────────────────────────────────────────────────────────
    # Gestión de posiciones
    # ─────────────────────────────────────────────────────────────────────────

    async def open_order(self, signal: dict, risk) -> None:
        """
        Abre una posición de mercado según la señal recibida.

        Aplica Kelly sizing al usdc_per_trade base. Si Kelly no tiene
        historial suficiente (<30 trades), kelly_mult=1.0 (sin cambio).
        """
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

        usdc_base  = float(getattr(risk, "usdc_per_trade", 20.0))
        kelly_mult = 1.0  # default; se sobrescribe en el try si Kelly tiene historial suficiente

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
            logger.warning("[%s] Kelly sizer error (%s) — usando base sin ajuste", self.symbol, e, exc_info=True)
            usdc_per_trade = usdc_base

        notional = usdc_per_trade * self.leverage

        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio — abortando. %s", self.symbol, e, exc_info=True)
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
            logger.error("[%s] open_order: error al enviar orden de mercado: %s", self.symbol, e, exc_info=True)
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
                logger.warning("[%s] open_order: error confirmando fill: %s", self.symbol, e, exc_info=True)
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
                logger.error("[%s] open_order: error colocando SL: %s", self.symbol, e, exc_info=True)

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
                logger.error("[%s] open_order: error colocando TP1: %s", self.symbol, e, exc_info=True)

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
            logger.warning("[%s] open_order: no se pudo persistir estado: %s", self.symbol, e, exc_info=True)

        logger.info(
            "[%s] ✅ Posición abierta: %s @ %.4f | SL=%.4f | TP1=%.4f | Kelly=%.2fx",
            self.symbol,
            self.position.upper(),
            self.entry_price,
            self.sl or 0,
            self.tp1 or 0,
            kelly_mult,
        )


__all__ = ["FuturesTrader"]
