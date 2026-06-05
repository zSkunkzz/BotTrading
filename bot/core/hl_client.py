"""
hl_client.py — Cliente Hyperliquid basado en el SDK oficial.

FIX CRÍTICO FREEZE (2026-06-03):
  CAUSA RAÍZ DEL FREEZE «no veo nada más / TradingLoop iniciado y silencio»:
  _HLCore.get() se llamaba desde FuturesTrader.__init__ (código SÍNCRONO),
  que ejecuta _warm_cache() con 3 llamadas HTTP bloqueantes via requests:
    - self.info.meta()
    - self.info.all_mids()
    - self.info.meta_and_asset_ctxs()

  Con 7+ traders arrancando en paralelo:
    - El primer trader crea el singleton y bloquea el hilo ~2-5s.
    - Si hay latencia o 429, _build_*_with_retry() usa time.sleep() directo,
      congelando el event loop entero — NINGÚN trader llega a _iteration().
    - Resultado: «TradingLoop iniciado» aparece (ese log está en _init() ANTES
      de que _iteration() corra) pero nada más jamás se imprime.

  FIXES aplicados:
    1. _HLCore.get_async(): classmethod async que crea el singleton dentro
       de asyncio.to_thread(), sin bloquear el event loop.
    2. HLClient.create(symbol): classmethod async que usa get_async().
    3. FuturesTrader.__init__: _hl_client = None (no crea el SDK aquí).
       La creación real ocurre en _get_ccxt() que es async y se llama desde
       _init() en TradingLoop.run() — ya dentro del event loop.
    4. _set_leverage: asyncio.wait_for timeout=15s para no colgarse.
    5. _init_lock: asyncio.Lock para serializar creaciones concurrentes y
       evitar que 7 traders lancen 7 _warm_cache() simultáneos.

FIX CRÍTICO (2026-06-01):
  Causa raíz del error «Invalid TPSL price»:
  - round_px usaba math.floor.
  - pxDecimals se infería incorrectamente.
  - place_sl/place_tp no validaban lógica entry vs trigger.

FIX float_to_wire rounding (2026-06-02):
  place_sl / place_tp redondean sz a szDecimals antes de pasarlo al SDK.

FIX duplicate nonce (2026-06-05):
  CAUSA RAÍZ de 'Invalid nonce: duplicate nonce XXXXXXXXXX':
  El SDK genera el nonce con time.time_ns() // 1_000_000 (precisión ms).
  Cuando dos llamadas de escritura (update_leverage, order, cancel...)
  se despachan desde asyncio.to_thread() con <1ms de diferencia, el
  OS puede asignarles el mismo timestamp — colisión garantizada.
  Fix:
    - _EXCHANGE_LOCK: threading.Lock global.
    - Todas las llamadas de escritura adquieren el lock ANTES de llamar al SDK.
    - El lock es threading.Lock (no asyncio.Lock) porque las llamadas
      ocurren dentro de asyncio.to_thread() — en hilos del OS.
    - Tras el lock se añade un sleep mínimo de _NONCE_MIN_DELAY_MS (50ms).

FIX 429 en update_leverage (2026-06-05 v7):
  _NONCE_MIN_DELAY_MS: 2ms → 50ms. _exchange_call con retry backoff en 429.

FIX get_positions / get_balance_usdc (2026-06-05):
  isinstance check para manejar dict y list en user_state().

FIX 429 en _info calls + get_ohlcv endTime vela abierta (2026-06-05 v14):
  _INFO_SEMAPHORE threading (concurrencia=3). end_ts retrocede 2 intervalos.

FIX caché compartido user_state (2026-06-05 v15):
  _get_user_state_cached() con TTL=5s compartido entre todos los traders.

FIX round_px tick alignment + triggerPx float (2026-06-05 v16):
  math.floor(price/tick+0.5)*tick. trigger_px como float.

FIX SyntaxError _warm_cache (2026-06-05 v17):
  Completar línea truncada en _warm_cache.

FIX get_positions usa caché compartida (2026-06-05 v20):
  get_positions() usa _get_user_state_cached() en vez de _info_call().

FIX double-checked locking en _get_user_state_cached (2026-06-05 v21):
  CAUSA RAÍZ de 429 persistentes con caché ya implementado:
  El caché vacío + 20 traders simultáneos → todos leen _user_state_cache=None
  ANTES de que ninguno actualice → 20 user_state() concurrentes → 429.

  El lock anterior solo protegía la LECTURA del caché. El fetch ocurría
  FUERA del lock → ventana de carrera abierta para los 20 threads.

  Fix: patrón double-checked locking:
    1. Lectura rápida sin lock (fast path — caso normal, caché caliente)
    2. Si miss → adquirir lock → re-verificar caché (otro thread pudo
       haber rellenado el caché mientras esperábamos el lock)
    3. Solo si sigue siendo miss → hacer el fetch real a HL
    4. Actualizar caché dentro del lock
  Resultado: con 20 traders, solo 1 thread hace el fetch real.
  Los otros 19 esperan el lock y al adquirirlo encuentran caché fresco.

FIX _build_exchange/info_with_retry reintentos insuficientes (2026-06-05 v22):
  CAUSA RAÍZ: BTC fue el primer trader en construir el singleton justo cuando
  HL estaba throttleando a nivel de Exchange.__init__() (spotMeta interna).
  Con solo 3 reintentos (~14s de ventana total) los intentos se agotaban antes
  de que HL dejara de devolver 429. El resto de traders reutilizaban el
  singleton ya construido, así que solo BTC (el primero) fallaba.

  Fix:
    - _build_exchange_with_retry: retries 3 → 8, delay inicial 2s → 5s.
    - _build_info_with_retry:     retries 3 → 8, delay inicial 2s → 5s.
    - Ventana total máxima con backoff x2 cap 30s: ~5+10+20+30+30+30+30 = 155s.
    - Suficiente para sobrevivir cualquier pico de 429 al arrancar Railway.
    - Configurable vía HL_SDK_INIT_RETRIES y HL_SDK_INIT_DELAY_S si se necesita
      ajustar sin tocar código.

Autenticación soportada:
  Opción A (recomendada): API Wallet
    HL_API_PRIVATE_KEY     — private key del agente aprobado en app.hyperliquid.xyz
    HL_API_WALLET_ADDRESS  — dirección del wallet PRINCIPAL (el que tiene fondos)

  Opción B: Private key directa
    HL_PRIVATE_KEY         — private key del wallet principal
    HL_ACCOUNT_ADDR        — dirección pública (opcional, se deriva automáticamente)

Opcionales:
  HL_TESTNET             — «true» para testnet
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import threading
import time
from typing import Optional

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

logger = logging.getLogger("HLClient")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_BASE_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

_MARKET_SLIPPAGE = 0.03
_TP_LIMIT_BUFFER = 0.001

# Delay mínimo entre llamadas de escritura consecutivas al Exchange.
_NONCE_MIN_DELAY_MS = float(os.getenv("HL_NONCE_MIN_DELAY_MS", "50")) / 1000.0

# Reintentos automáticos en _exchange_call cuando HL responde 429.
_EXCHANGE_RETRIES     = int(float(os.getenv("HL_EXCHANGE_RETRIES", "3")))
_EXCHANGE_RETRY_DELAY = float(os.getenv("HL_EXCHANGE_RETRY_DELAY_S", "2.0"))

# Reintentos para la inicialización del SDK (Exchange.__init__ / Info.__init__).
# FIX v22: subidos de 3 → 8 y delay inicial de 2s → 5s para sobrevivir picos
# de 429 en el primer arranque del singleton (ventana total ~155s con backoff).
_SDK_INIT_RETRIES = int(float(os.getenv("HL_SDK_INIT_RETRIES", "8")))
_SDK_INIT_DELAY_S = float(os.getenv("HL_SDK_INIT_DELAY_S", "5.0"))

# Lock global de escritura al Exchange (threading porque va en to_thread).
_EXCHANGE_LOCK = threading.Lock()

# ── Semáforo threading para llamadas de LECTURA a _info (SDK síncrono) ──────────
_HL_INFO_CONCURRENCY = int(os.getenv("HL_INFO_CONCURRENCY", "3"))
_INFO_SEMAPHORE = threading.Semaphore(_HL_INFO_CONCURRENCY)

# ── Caché compartido de user_state (singleton por cuenta) ───────────────────
# FIX v21: TTL 5s. El fetch está protegido por double-checked locking para
# garantizar que solo 1 thread haga el request real aunque 20 lleguen juntos.
_USER_STATE_CACHE_TTL = float(os.getenv("HL_USER_STATE_CACHE_TTL_S", "5.0"))
_USER_STATE_LOCK      = threading.Lock()
_user_state_cache: dict | list | None = None
_user_state_cache_ts: float = 0.0

POST_FILL_CONFIRM_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "6"))
POST_FILL_CONFIRM_DELAY   = float(os.getenv("POST_FILL_CONFIRM_DELAY", "3.0"))


def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    for suffix in ("USDTUSDT", "USDT"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def _px_decimals_from_tick(tick: float) -> int:
    if tick <= 0:
        return 4
    dec = max(0, min(6, round(-math.log10(tick))))
    return dec


def _round_sz(sz: float, sz_decimals: int) -> float:
    factor = 10 ** sz_decimals
    return math.floor(sz * factor) / factor


def _is_429(exc: Exception) -> bool:
    """Devuelve True si la excepción indica rate-limit (429) de HL."""
    err = str(exc)
    if "429" in err:
        return True
    if exc.args and isinstance(exc.args[0], int) and exc.args[0] == 429:
        return True
    return False


def _exchange_call(fn, *args, **kwargs):
    """
    Ejecuta una llamada de escritura al Exchange SDK con lock global.
    Garantiza timestamps distintos y retry automático en 429.
    """
    last_exc: Exception | None = None
    delay = _EXCHANGE_RETRY_DELAY

    for attempt in range(max(1, _EXCHANGE_RETRIES)):
        try:
            with _EXCHANGE_LOCK:
                result = fn(*args, **kwargs)
                if _NONCE_MIN_DELAY_MS > 0:
                    time.sleep(_NONCE_MIN_DELAY_MS)
                return result
        except Exception as exc:
            last_exc = exc
            if _is_429(exc) and attempt < _EXCHANGE_RETRIES - 1:
                logger.warning(
                    "[ExchangeCall] 429 rate-limit (intento %d/%d) — reintentando en %.1fs: %s",
                    attempt + 1, _EXCHANGE_RETRIES, delay, exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
            else:
                raise

    if last_exc is not None:
        raise last_exc


def _info_call(fn, *args, **kwargs):
    """
    Ejecuta una llamada de LECTURA al SDK Info con semáforo threading global.
    Incluye retry automático con backoff en caso de 429.
    """
    last_exc: Exception | None = None
    delay = _EXCHANGE_RETRY_DELAY

    for attempt in range(max(1, _EXCHANGE_RETRIES)):
        try:
            with _INFO_SEMAPHORE:
                return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if _is_429(exc) and attempt < _EXCHANGE_RETRIES - 1:
                logger.warning(
                    "[InfoCall] 429 rate-limit (intento %d/%d) — reintentando en %.1fs",
                    attempt + 1, _EXCHANGE_RETRIES, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
            else:
                raise

    if last_exc is not None:
        raise last_exc


def _get_user_state_cached(info_obj, account_addr: str):
    """
    FIX v21: double-checked locking para caché singleton de user_state.

    Patrón:
      1. Fast path: leer caché sin lock (caso normal — caché caliente).
         Si está fresco, retornar inmediatamente sin contención.
      2. Si miss: adquirir lock → re-verificar caché dentro del lock.
         (Otro thread puede haber rellenado el caché mientras esperábamos.)
      3. Solo si sigue siendo miss dentro del lock: hacer fetch real a HL.
      4. Escribir resultado en caché dentro del lock antes de liberarlo.

    Con 20 traders llegando simultáneamente a caché vacío:
      - Thread 1 adquiere el lock, ve miss, hace fetch, escribe caché.
      - Threads 2-20 esperan el lock. Al adquirirlo, ven caché fresco → retornan.
      - Resultado: 1 sola request real a clearinghouseState por ciclo de 5s.
    """
    global _user_state_cache, _user_state_cache_ts

    # ── FAST PATH: sin lock ──────────────────────────────────────────────────
    # Lectura no atómica pero segura en CPython (GIL) para el caso caliente.
    now = time.monotonic()
    cached = _user_state_cache
    cached_ts = _user_state_cache_ts
    if cached is not None and (now - cached_ts) < _USER_STATE_CACHE_TTL:
        return cached

    # ── SLOW PATH: adquirir lock y re-verificar ──────────────────────────────
    last_exc: Exception | None = None
    delay = _EXCHANGE_RETRY_DELAY

    for attempt in range(max(1, _EXCHANGE_RETRIES)):
        try:
            with _USER_STATE_LOCK:
                # Double-check dentro del lock: otro thread puede haber
                # actualizado el caché mientras esperábamos el lock.
                now2 = time.monotonic()
                if _user_state_cache is not None and (now2 - _user_state_cache_ts) < _USER_STATE_CACHE_TTL:
                    return _user_state_cache

                # Fetch real — solo 1 thread llega aquí por ventana TTL.
                with _INFO_SEMAPHORE:
                    result = info_obj.user_state(account_addr)

                _user_state_cache    = result
                _user_state_cache_ts = time.monotonic()
                return result

        except Exception as exc:
            last_exc = exc
            if _is_429(exc) and attempt < _EXCHANGE_RETRIES - 1:
                jitter  = random.uniform(0.0, 0.5)
                sleep_s = min(delay + jitter, 30.0)
                logger.warning(
                    "[UserStateCache] 429 rate-limit (intento %d/%d) — reintentando en %.1fs",
                    attempt + 1, _EXCHANGE_RETRIES, sleep_s,
                )
                time.sleep(sleep_s)
                delay = min(delay * 2, 30.0)
            else:
                raise

    if last_exc is not None:
        raise last_exc


# ─────────────────────────────────────────────────────────────────
# _HLCore: singleton que contiene el Exchange + Info compartidos
# ─────────────────────────────────────────────────────────────────

class _HLCore:
    """
    Singleton que mantiene UNA instancia de Exchange + Info.
    Pre-carga szDecimals, pxDecimals y maxLeverage al arrancar.
    """

    _instance: "_HLCore | None" = None
    _init_lock: "asyncio.Lock | None" = None

    def __init__(self) -> None:
        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError("HL_API_WALLET_ADDRESS es obligatoria en modo agente.")
            wallet             = Account.from_key(api_pk)
            self.account_addr  = api_wallet
            self.agent_addr    = wallet.address
            self.agent_mode    = True
            exchange_wallet    = wallet
            exchange_kwargs    = {"account_address": api_wallet}
        else:
            pk = os.getenv("HL_PRIVATE_KEY", "").strip()
            if not pk:
                raise ValueError("Sin clave configurada. Define HL_API_PRIVATE_KEY o HL_PRIVATE_KEY.")
            wallet             = Account.from_key(pk)
            addr               = os.getenv("HL_ACCOUNT_ADDR", "").strip() or wallet.address
            self.account_addr  = addr
            self.agent_addr    = ""
            self.agent_mode    = False
            exchange_wallet    = wallet
            exchange_kwargs    = {}
            logger.warning(
                "⚠️  [HLCore] AGENTE INACTIVO — operando con master wallet directamente. "
                "Verificar que HL_API_PRIVATE_KEY esté configurada y el agente autorizado en Hyperliquid."
            )

        self.exchange = self._build_exchange_with_retry(exchange_wallet, exchange_kwargs)
        self.info     = self._build_info_with_retry()

        self._sz_decimals_cache:  dict[str, int]   = {}
        self._px_decimals_cache:  dict[str, int]   = {}
        self._max_leverage_cache: dict[str, int]   = {}
        self._tick_size_cache:    dict[str, float] = {}

        self._warm_cache()

        logger.info(
            "[HLCore] SDK Exchange+Info inicializados | addr=%s | agente=%s | "
            "coins cacheados: sz=%d px=%d lev=%d",
            self.account_addr[:10] + "...",
            self.agent_mode,
            len(self._sz_decimals_cache),
            len(self._px_decimals_cache),
            len(self._max_leverage_cache),
        )

    def _warm_cache(self) -> None:
        try:
            meta     = self.info.meta()
            universe = meta.get("universe", [])
        except Exception as exc:
            logger.warning("[HLCore] _warm_cache meta() falló: %s", exc)
            universe = []

        try:
            _, ctxs = self.info.meta_and_asset_ctxs()
        except Exception as exc:
            logger.warning("[HLCore] _warm_cache meta_and_asset_ctxs() falló: %s", exc)
            ctxs = []

        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            if not name:
                continue
            self._sz_decimals_cache[name]  = asset.get("szDecimals", 4)
            self._max_leverage_cache[name] = asset.get("maxLeverage", 20)

            ctx = ctxs[i] if i < len(ctxs) else {}
            raw_tick = ctx.get("minTick") if isinstance(ctx, dict) else None
            if raw_tick is not None:
                try:
                    tick = float(raw_tick)
                    self._tick_size_cache[name]   = tick
                    self._px_decimals_cache[name] = _px_decimals_from_tick(tick)
                except Exception as exc:
                    logger.warning("[HLCore] _warm_cache tick parse falló para %s: %s", name, exc)

    def _build_exchange_with_retry(self, wallet, kwargs: dict) -> Exchange:
        """
        FIX v22: reintentos 3 → 8, delay inicial 2s → 5s.
        Ventana total ~155s con backoff x2 cap 30s — sobrevive picos de 429
        al construir el singleton por primera vez en Railway.
        Configurable con HL_SDK_INIT_RETRIES y HL_SDK_INIT_DELAY_S.
        """
        retries = _SDK_INIT_RETRIES
        delay   = _SDK_INIT_DELAY_S
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return Exchange(wallet, _BASE_URL, **kwargs)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "[HLCore] Exchange init intento %d/%d falló: %s — reintentando en %.1fs",
                    attempt + 1, retries, exc, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
        raise RuntimeError(f"No se pudo inicializar Exchange tras {retries} intentos") from last_exc

    def _build_info_with_retry(self) -> Info:
        """
        FIX v22: reintentos 3 → 8, delay inicial 2s → 5s.
        Configurable con HL_SDK_INIT_RETRIES y HL_SDK_INIT_DELAY_S.
        """
        retries = _SDK_INIT_RETRIES
        delay   = _SDK_INIT_DELAY_S
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return Info(_BASE_URL, skip_ws=True)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "[HLCore] Info init intento %d/%d falló: %s — reintentando en %.1fs",
                    attempt + 1, retries, exc, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
        raise RuntimeError(f"No se pudo inicializar Info tras {retries} intentos") from last_exc

    @classmethod
    async def get_async(cls) -> "_HLCore":
        """Crea o devuelve el singleton dentro de asyncio.to_thread() para no bloquear el event loop."""
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        async with cls._init_lock:
            if cls._instance is None:
                cls._instance = await asyncio.to_thread(cls)
        return cls._instance

    @classmethod
    def get(cls) -> "_HLCore":
        """Versión síncrona (solo para uso en contextos ya threadeados)."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance


# ─────────────────────────────────────────────────────────────────
# HLClient: wrapper por símbolo sobre _HLCore
# ─────────────────────────────────────────────────────────────────

class HLClient:
    """
    Cliente Hyperliquid para un símbolo concreto.
    Usa el singleton _HLCore para Exchange + Info compartidos.
    """

    def __init__(self, core: _HLCore, symbol: str) -> None:
        self._core     = core
        self._symbol   = symbol
        self._coin     = _norm_coin(symbol)
        self._info     = core.info
        self._exchange = core.exchange
        self._account  = core.account_addr

    @classmethod
    async def create(cls, symbol: str) -> "HLClient":
        core = await _HLCore.get_async()
        return cls(core, symbol)

    # ── Propiedades de mercado ────────────────────────────────────────────────

    def get_sz_decimals(self) -> int:
        return self._core._sz_decimals_cache.get(self._coin, 4)

    def get_px_decimals(self) -> int:
        return self._core._px_decimals_cache.get(self._coin, 4)

    def get_tick_size(self) -> float:
        return self._core._tick_size_cache.get(self._coin, 0.0)

    def get_max_leverage(self) -> int:
        return self._core._max_leverage_cache.get(self._coin, 20)

    # ── Redondeo ──────────────────────────────────────────────────────────────

    def round_px(self, price: float) -> float:
        """
        FIX v16 Bug 1: redondea precio al múltiplo exacto del tick_size.
        """
        tick = self.get_tick_size()
        if tick > 0:
            return math.floor(price / tick + 0.5) * tick
        px_dec = self.get_px_decimals()
        factor = 10 ** px_dec
        return math.floor(price * factor + 0.5) / factor

    def _round_qty(self, qty: float) -> float:
        sz_dec = self.get_sz_decimals()
        return _round_sz(qty, sz_dec)

    # ── Estado de cuenta ──────────────────────────────────────────────────────

    def get_user_state(self) -> dict | list:
        return _get_user_state_cached(self._info, self._account)

    def get_positions(self) -> list[dict]:
        """
        FIX v20+v21: usa caché compartida con double-checked locking.
        20 traders → 1 request real a clearinghouseState por TTL.
        """
        state = _get_user_state_cached(self._info, self._account)
        if isinstance(state, dict):
            asset_positions = state.get("assetPositions", [])
        elif isinstance(state, list):
            logger.warning(
                "[HLClient] get_positions: user_state devolvió list (agente inactivo?). "
                "Devolviendo lista vacía."
            )
            return []
        else:
            logger.warning(
                "[HLClient] get_positions: respuesta inesperada (tipo=%s). Devolviendo [].",
                type(state).__name__,
            )
            return []
        return [p["position"] for p in asset_positions if "position" in p]

    def get_balance_usdc(self) -> float:
        state = _get_user_state_cached(self._info, self._account)
        if isinstance(state, dict):
            return float(state.get("marginSummary", {}).get("accountValue", 0.0))
        logger.warning("[HLClient] get_balance_usdc: user_state inesperado (tipo=%s) → 0.0", type(state).__name__)
        return 0.0

    def get_open_orders(self) -> list[dict]:
        return _info_call(self._info.open_orders, self._account) or []

    def get_frontend_open_orders(self) -> list[dict]:
        try:
            return _info_call(self._info.frontend_open_orders, self._account) or []
        except Exception as exc:
            logger.warning("[HLClient] get_frontend_open_orders falló: %s", exc)
            return []

    def get_ohlcv(self, interval: str = "15m", limit: int = 100) -> list:
        interval_ms = {
            "1m": 60_000, "3m": 180_000, "5m": 300_000,
            "15m": 900_000, "30m": 1_800_000,
            "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
            "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
        }
        ms = interval_ms.get(interval, 900_000)
        # FIX v14 Bug 2: retroceder 2 intervalos para apuntar a la última vela CERRADA.
        end_ts   = int(time.time() * 1000) - 2 * ms
        start_ts = end_ts - limit * ms
        try:
            candles = _info_call(
                self._info.candles_snapshot,
                self._coin, interval, start_ts, end_ts,
            )
            return candles or []
        except Exception as exc:
            logger.warning("[HLClient] get_ohlcv(%s, %s) falló: %s", self._coin, interval, exc)
            return []

    # ── Órdenes de escritura ──────────────────────────────────────────────────

    def place_limit(self, is_buy: bool, sz: float, px: float, reduce_only: bool = False) -> dict:
        sz = self._round_qty(sz)
        px = self.round_px(px)
        order_type = {"limit": {"tif": "Gtc"}}
        return _exchange_call(
            self._exchange.order,
            self._coin, is_buy, sz, px, order_type,
            reduce_only=reduce_only,
        )

    def place_market(self, is_buy: bool, sz: float, reduce_only: bool = False) -> dict:
        sz   = self._round_qty(sz)
        mids = _info_call(self._info.all_mids)
        mid  = float(mids.get(self._coin, 0))
        px   = mid * (1 + _MARKET_SLIPPAGE) if is_buy else mid * (1 - _MARKET_SLIPPAGE)
        px   = self.round_px(px)
        order_type = {"limit": {"tif": "Ioc"}}
        return _exchange_call(
            self._exchange.order,
            self._coin, is_buy, sz, px, order_type,
            reduce_only=reduce_only,
        )

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        tp_price: float,
        limit_price: Optional[float],
        entry_price: Optional[float] = None,
    ) -> dict:
        sz         = self._round_qty(sz)
        tp_price   = float(tp_price)
        trigger_px = self.round_px(tp_price)

        if limit_price is not None:
            lim_px = self.round_px(float(limit_price))
        else:
            buf    = _TP_LIMIT_BUFFER
            lim_px = self.round_px(trigger_px * (1 - buf) if is_buy else trigger_px * (1 + buf))

        order_type = {
            "trigger": {
                "triggerPx": trigger_px,
                "isMarket":  False,
                "tpsl":      "tp",
            }
        }
        return _exchange_call(
            self._exchange.order,
            self._coin, is_buy, sz, lim_px, order_type,
            reduce_only=True,
        )

    def place_sl(
        self,
        is_buy: bool,
        sz: float,
        sl_price: float,
        entry_price: Optional[float] = None,
    ) -> dict:
        sz         = self._round_qty(sz)
        sl_price   = float(sl_price)
        trigger_px = self.round_px(sl_price)

        order_type = {
            "trigger": {
                "triggerPx": trigger_px,
                "isMarket":  True,
                "tpsl":      "sl",
            }
        }
        return _exchange_call(
            self._exchange.order,
            self._coin, is_buy, sz, trigger_px, order_type,
            reduce_only=True,
        )

    def place_bulk(self, orders: list[dict]) -> dict:
        return _exchange_call(self._exchange.bulk_orders, orders)

    def cancel_order(self, oid: int) -> dict:
        return _exchange_call(self._exchange.cancel, self._coin, oid)

    def cancel_all_open_tpsl(self) -> None:
        orders = self.get_frontend_open_orders()
        coin_orders = [o for o in orders if str(o.get("coin", "")).upper() == self._coin]
        for o in coin_orders:
            oid = o.get("oid")
            if oid:
                try:
                    _exchange_call(self._exchange.cancel, self._coin, oid)
                except Exception as exc:
                    logger.warning("[HLClient] cancel_all_open_tpsl: error cancelando oid=%s: %s", oid, exc)

    def update_leverage(self, leverage: int, is_cross: bool = False) -> dict:
        return _exchange_call(
            self._exchange.update_leverage,
            leverage, self._coin, is_cross,
        )

    def confirm_fill(self, order_result: dict, timeout: float = POST_FILL_CONFIRM_RETRIES * POST_FILL_CONFIRM_DELAY) -> bool:
        """Espera a que una orden de mercado/límite se ejecute comprobando fills."""
        oid = None
        try:
            statuses = order_result.get("response", {}).get("data", {}).get("statuses", [])
            for s in statuses:
                if "resting" in s:
                    oid = s["resting"].get("oid")
                elif "filled" in s:
                    return True
        except Exception:
            pass

        if oid is None:
            return True  # Si no hay oid pendiente asumimos fill

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                orders = self.get_open_orders()
                oids = {o.get("oid") for o in orders}
                if oid not in oids:
                    return True
            except Exception:
                pass
            time.sleep(POST_FILL_CONFIRM_DELAY)

        logger.warning("[HLClient] confirm_fill: oid=%s no se llenó en %.1fs", oid, timeout)
        return False


# ── Alias público ──────────────────────────────────────────────────────────────
norm_coin = _norm_coin
