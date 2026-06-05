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
    - _EXCHANGE_LOCK: threading.Lock global (compartido entre todos los
      HLClient que usan el mismo singleton Exchange).
    - Todas las llamadas de escritura (place_limit, place_market, place_tp,
      place_sl, place_bulk, cancel_order, cancel_all_open_tpsl y
      update_leverage en trader.py) adquieren el lock ANTES de llamar al SDK.
    - El lock es threading.Lock (no asyncio.Lock) porque las llamadas
      ocurren dentro de asyncio.to_thread() — en hilos del OS, no en el
      event loop.
    - Tras el lock se añade un sleep mínimo de _NONCE_MIN_DELAY_MS ms
      (default 50ms) para garantizar timestamps distintos incluso si el
      scheduler del OS reutiliza el hilo inmediatamente.

FIX 429 en update_leverage (2026-06-05 v7):
  CAUSA RAÍZ: con 7 traders serializados por _EXCHANGE_LOCK pero solo 2ms
  de delay entre llamadas, HL recibe ~7 update_leverage en ráfaga rápida
  al arranque → responde 429 (rate limit de CloudFront/nginx).
  Fixes:
    1. _NONCE_MIN_DELAY_MS: default 2ms → 50ms (configurable con
       HL_NONCE_MIN_DELAY_MS). Con 7 traders = ~350ms total en arranque.
    2. _exchange_call: retry automático con backoff exponencial cuando
       HL responde 429. Hasta HL_EXCHANGE_RETRIES intentos (default 3)
       con espera inicial de HL_EXCHANGE_RETRY_DELAY_S (default 2s),
       duplicando en cada intento hasta máx 30s.

FIX get_positions / get_balance_usdc (2026-06-05):
  CAUSA RAÍZ de '_get_positions: respuesta inesperada (tipo=list)' y
  parálisis total con error:4 en decision_engine:
  Cuando agent_mode=False (agente expirado/revocado), user_state() del SDK
  puede devolver directamente una list en vez del dict {assetPositions:[...]}. 
  get_positions() llamaba .get() sobre la list → AttributeError inmediato.
  Fixes:
    1. get_positions(): isinstance check — maneja dict y list correctamente.
    2. get_balance_usdc(): mismo isinstance guard defensivo.
    3. Warning visible en startup cuando agent_mode=False para detectar
       inmediatamente agente expirado/no configurado.

FIX 429 en _info calls + get_ohlcv endTime vela abierta (2026-06-05 v14):
  BUG 1 — 429 en _get_positions (clearinghouseState):
    CAUSA RAÍZ: get_positions() llama self._info.user_state() que usa el
    SDK síncrono requests. Al ejecutarse via asyncio.to_thread(), el
    semáforo _HL_SEMAPHORE de trader.py (aiohttp) NO lo cubre.
    Con 10 traders llamando get_positions() simultáneamente en su
    primer ciclo → 10 requests a clearinghouseState en ráfaga → 429.
    FIX: _INFO_SEMAPHORE = threading.Semaphore(HL_INFO_CONCURRENCY, default 3)
    Todas las llamadas a self._info.* que van a /info adquieren este
    semáforo. Al ser threading.Semaphore funciona dentro de
    asyncio.to_thread() (hilos OS, no event loop).

  BUG 2 — get_ohlcv sin datos para BTC, HYPE y otros coins principales:
    CAUSA RAÍZ: end_ts = int(time.time() * 1000) apunta al instante actual,
    dentro de la vela abierta (aún no cerrada). HL devuelve [] silenciosamente
    cuando endTime está en una vela abierta.
    FIX: end_ts retrocede 2 intervalos para garantizar que siempre apunta
    a la última vela CERRADA (2 intervalos absorben latencia y skew de reloj).

FIX caché compartido user_state (2026-06-05 v15):
  CAUSA RAÍZ persistente de 429 en clearinghouseState:
  Aunque _INFO_SEMAPHORE limita la concurrencia a 3 simultáneos, con 10
  traders cada uno llamando get_user_state() en su ciclo de ~10s, se
  generan ~10 llamadas/10s = 1 req/s a clearinghouseState POR CUENTA.
  La respuesta es idéntica para todos (misma cuenta), así que hacer N
  llamadas es redundante y agota el rate-limit.
  FIX: caché singleton en _HLCore (_user_state_cache) compartido entre
  TODOS los HLClient. TTL configurable con HL_USER_STATE_CACHE_TTL_S
  (default 3s). get_user_state() devuelve el caché si es reciente,
  evitando la ráfaga de requests idénticos a clearinghouseState.
  Lock threading (_USER_STATE_LOCK) para acceso thread-safe desde
  asyncio.to_thread().

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

# Lock global de escritura al Exchange (threading porque va en to_thread).
_EXCHANGE_LOCK = threading.Lock()

# ── Semáforo threading para llamadas de LECTURA a _info (SDK síncrono) ──────────
# Limita la concurrencia de user_state / open_orders / candles_snapshot
# que usan requests internamente y NO pasan por el semáforo aiohttp de trader.py.
# threading.Semaphore porque se adquiere dentro de asyncio.to_thread() (hilos OS).
_HL_INFO_CONCURRENCY = int(os.getenv("HL_INFO_CONCURRENCY", "3"))
_INFO_SEMAPHORE = threading.Semaphore(_HL_INFO_CONCURRENCY)

# ── Caché compartido de user_state (singleton por cuenta) ───────────────────
# TTL configurable con HL_USER_STATE_CACHE_TTL_S (default 3s).
# Lock threading para acceso thread-safe desde asyncio.to_thread().
_USER_STATE_CACHE_TTL = float(os.getenv("HL_USER_STATE_CACHE_TTL_S", "3.0"))
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

    FIX v14: protege user_state(), open_orders(), candles_snapshot() y
    cualquier otra llamada a self._info.* contra concurrencia excesiva.
    Sin este semáforo, 10 traders llamando get_positions() al mismo tiempo
    lanzan 10 requests síncronos a /info en ráfaga => HL responde 429.

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
    FIX v15: caché singleton compartido de user_state para toda la cuenta.

    Todos los HLClient de todos los traders comparten el mismo caché porque
    la respuesta de clearinghouseState es idéntica para todos (misma cuenta).
    Hacer N llamadas simultáneas es redundante y satura el rate-limit de HL.

    TTL configurable con HL_USER_STATE_CACHE_TTL_S (default 3s).
    Lock threading para acceso seguro desde asyncio.to_thread().
    Incluye retry con backoff exponencial + jitter en caso de 429.
    """
    global _user_state_cache, _user_state_cache_ts

    now = time.monotonic()
    with _USER_STATE_LOCK:
        if _user_state_cache is not None and (now - _user_state_cache_ts) < _USER_STATE_CACHE_TTL:
            return _user_state_cache

    # Caché expirado o vacío — hacer la request fuera del lock para no bloquear
    last_exc: Exception | None = None
    delay = _EXCHANGE_RETRY_DELAY

    for attempt in range(max(1, _EXCHANGE_RETRIES)):
        try:
            with _INFO_SEMAPHORE:
                result = info_obj.user_state(account_addr)
            with _USER_STATE_LOCK:
                _user_state_cache    = result
                _user_state_cache_ts = time.monotonic()
            return result
        except Exception as exc:
            last_exc = exc
            if _is_429(exc) and attempt < _EXCHANGE_RETRIES - 1:
                jitter   = random.uniform(0.0, 0.5)
                sleep_s  = min(delay + jitter, 30.0)
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
                except (ValueError, TypeError):
                    self._px_decimals_cache[name] = 4
            else:
                self._px_decimals_cache[name] = 4

        logger.info("[HLCore] Caché pre-cargado: %d coins (szDecimals + pxDecimals + maxLeverage listos)", len(universe))

    @classmethod
    async def get_async(cls) -> "_HLCore":
        if cls._instance is not None:
            return cls._instance
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()
        async with cls._init_lock:
            if cls._instance is None:
                cls._instance = await asyncio.to_thread(cls)
        return cls._instance

    def _build_exchange_with_retry(self, wallet, kwargs: dict, retries: int = 3) -> Exchange:
        for attempt in range(retries):
            try:
                return Exchange(wallet, _BASE_URL, **kwargs)
            except Exception as exc:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning("[HLCore] Exchange init falló (intento %d/%d): %s — reintentando en %ds", attempt + 1, retries, exc, wait)
                    time.sleep(wait)
                else:
                    raise

    def _build_info_with_retry(self, retries: int = 3) -> Info:
        for attempt in range(retries):
            try:
                return Info(_BASE_URL, skip_ws=True)
            except Exception as exc:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning("[HLCore] Info init falló (intento %d/%d): %s — reintentando en %ds", attempt + 1, retries, exc, wait)
                    time.sleep(wait)
                else:
                    raise

    def get_sz_decimals(self, coin: str) -> int:
        return self._sz_decimals_cache.get(coin, 4)

    def get_px_decimals(self, coin: str) -> int:
        return self._px_decimals_cache.get(coin, 4)

    def get_max_leverage(self, coin: str) -> int:
        return self._max_leverage_cache.get(coin, 20)

    def get_tick_size(self, coin: str) -> float:
        return self._tick_size_cache.get(coin, 0.0)


# ─────────────────────────────────────────────────────────────────
# HLClient: cliente por símbolo
# ─────────────────────────────────────────────────────────────────

class HLClient:
    """
    Cliente por símbolo (coin) que delega al singleton _HLCore.
    Uso: client = await HLClient.create("BTC")
    """

    def __init__(self, symbol: str, core: "_HLCore | None" = None):
        self.symbol = symbol
        self.coin   = _norm_coin(symbol)
        if core is None:
            if _HLCore._instance is None:
                raise RuntimeError(
                    f"[HLClient] {symbol}: _HLCore no inicializado. "
                    "Usar HLClient.create(symbol) (async) en lugar de HLClient(symbol)."
                )
            core = _HLCore._instance
        self._exchange     = core.exchange
        self._info         = core.info
        self._account_addr = core.account_addr
        self._agent_addr   = core.agent_addr
        self._agent_mode   = core.agent_mode
        self._core         = core

    @classmethod
    async def create(cls, symbol: str) -> "HLClient":
        core = await _HLCore.get_async()
        return cls(symbol, core=core)

    # ── METADATOS ────────────────────────────────────────────────

    def _get_meta_asset(self) -> dict:
        try:
            meta = _info_call(self._info.meta)
            for asset in meta.get("universe", []):
                if asset.get("name") == self.coin:
                    return asset
        except Exception:
            pass
        return {}

    def get_sz_decimals(self) -> int:
        cached = self._core.get_sz_decimals(self.coin)
        if cached != 4:
            return cached
        return self._get_meta_asset().get("szDecimals", 4)

    def get_px_decimals(self) -> int:
        return self._core.get_px_decimals(self.coin)

    def get_tick_size(self) -> float:
        return self._core.get_tick_size(self.coin)

    def get_max_leverage(self) -> int:
        cached = self._core.get_max_leverage(self.coin)
        if cached != 20:
            return cached
        return self._get_meta_asset().get("maxLeverage", 20)

    def round_px(self, price: float) -> float:
        dec = self.get_px_decimals()
        factor = 10 ** dec
        return round(price * factor) / factor

    def round_sz(self, size: float) -> float:
        return _round_sz(size, self.get_sz_decimals())

    # ── OHLCV ────────────────────────────────────────────────

    def get_ohlcv(self, timeframe: str = "15m", limit: int = 100) -> list[dict]:
        """
        Obtiene velas OHLCV via SDK.

        FIX v14: end_ts retrocede 2 intervalos para garantizar que nunca
        apunta a la vela abierta actual. HL devuelve [] silenciosamente
        cuando endTime cae dentro de una vela no cerrada.
        También usa _info_call() para pasar por el semáforo threading.
        """
        tf_map = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15,
            "30m": 30, "1h": 60, "2h": 120, "4h": 240,
            "6h": 360, "8h": 480, "12h": 720, "1d": 1440,
        }
        interval    = tf_map.get(timeframe, 15)
        interval_ms = interval * 60 * 1000
        # Retroceder 2 intervalos: garantiza que endTime apunta a la ultima
        # vela CERRADA incluso con latencia de red o skew de reloj.
        end_ts   = int(time.time() * 1000) - 2 * interval_ms
        start_ts = end_ts - limit * interval_ms

        candles = _info_call(self._info.candles_snapshot, self.coin, interval, start_ts, end_ts)
        result  = []
        for c in (candles or []):
            try:
                result.append({
                    "timestamp": int(c["T"]),
                    "open":   float(c["o"]),
                    "high":   float(c["h"]),
                    "low":    float(c["l"]),
                    "close":  float(c["c"]),
                    "volume": float(c["v"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return result

    # ── PRECIO ───────────────────────────────────────────────

    def get_price(self) -> float:
        try:
            mids = _info_call(self._info.all_mids)
            val  = mids.get(self.coin)
            if val is not None:
                return float(val)
        except Exception:
            pass
        candles = self.get_ohlcv("1m", 1)
        if candles:
            return candles[-1]["close"]
        return 0.0

    # ── LEVERAGE ─────────────────────────────────────────────

    def set_leverage(self, leverage: int, is_cross: bool = True) -> dict:
        return _exchange_call(
            self._exchange.update_leverage,
            leverage,
            self.coin,
            is_cross,
        )

    # ── CONSULTAS INFO (solo lectura, protegidas por _INFO_SEMAPHORE) ────────

    def get_user_state(self) -> dict:
        """
        FIX v15: usa caché singleton compartido entre todos los traders.
        Evita N llamadas simultáneas a clearinghouseState (una por trader)
        que saturaban el rate-limit de Hyperliquid con 429.
        TTL configurable con HL_USER_STATE_CACHE_TTL_S (default 3s).
        """
        return _get_user_state_cached(self._info, self._account_addr)

    def get_open_orders(self) -> list:
        return _info_call(self._info.open_orders, self._account_addr)

    def get_positions(self) -> list:
        state = self.get_user_state()
        if isinstance(state, dict):
            asset_positions = state.get("assetPositions", [])
        elif isinstance(state, list):
            asset_positions = state
        else:
            logger.warning(
                "[%s] get_positions: respuesta inesperada (tipo=%s): %r",
                self.coin, type(state).__name__, state,
            )
            return []
        return [
            p for p in asset_positions
            if p.get("position", {}).get("coin") == self.coin
               and float(p.get("position", {}).get("szi", 0)) != 0
        ]

    def get_balance_usdc(self) -> float:
        state = self.get_user_state()
        if not isinstance(state, dict):
            logger.warning(
                "[%s] get_balance_usdc: respuesta inesperada (tipo=%s)",
                self.coin, type(state).__name__,
            )
            return 0.0
        return float(state.get("crossMarginSummary", {}).get("accountValue", 0.0))

    def cancel_order(self, order_id: int) -> dict:
        return _exchange_call(self._exchange.cancel, self.coin, order_id)

    def cancel_all_open_tpsl(self) -> list[dict]:
        orders  = self.get_open_orders()
        results = []
        for o in orders:
            if o.get("coin") != self.coin:
                continue
            ot = o.get("orderType", "")
            is_tpsl = False
            if isinstance(ot, dict):
                trigger  = ot.get("trigger", {})
                tpsl_val = trigger.get("tpsl", "")
                is_tpsl  = tpsl_val in ("tp", "sl")
            elif isinstance(ot, str):
                is_tpsl = any(
                    kw in ot
                    for kw in ("Trigger", "Stop", "Take Profit", "trigger", "stop", "tp", "sl")
                )
            if is_tpsl:
                oid = o.get("oid")
                if oid:
                    r = _exchange_call(self._exchange.cancel, self.coin, oid)
                    results.append(r)
                    logger.info(
                        "[%s] Trigger order cancelada: oid=%s type=%s",
                        self.coin, oid, ot,
                    )
        return results

    # ── ÓRDENES ─────────────────────────────────────────────

    def place_market(self, side: str, size: float) -> dict:
        """
        Ejecuta una orden de mercado.
        side: 'buy' | 'sell'
        size: tamaño en unidades del activo (ya redondeado a szDecimals).
        """
        is_buy  = side.lower() == "buy"
        px      = self.get_price()
        if px <= 0:
            raise ValueError(f"[{self.coin}] place_market: precio inválido ({px})")
        slippage = _MARKET_SLIPPAGE
        if is_buy:
            limit_px = self.round_px(px * (1 + slippage))
        else:
            limit_px = self.round_px(px * (1 - slippage))
        sz = self.round_sz(size)
        if sz <= 0:
            raise ValueError(f"[{self.coin}] place_market: size redondeado a 0 (raw={size})")
        return _exchange_call(
            self._exchange.order,
            self.coin,
            is_buy,
            sz,
            limit_px,
            {"limit": {"tif": "Ioc"}},
        )

    def place_limit(self, side: str, size: float, price: float, reduce_only: bool = False) -> dict:
        is_buy   = side.lower() == "buy"
        limit_px = self.round_px(price)
        sz       = self.round_sz(size)
        if sz <= 0:
            raise ValueError(f"[{self.coin}] place_limit: size redondeado a 0 (raw={size})")
        return _exchange_call(
            self._exchange.order,
            self.coin,
            is_buy,
            sz,
            limit_px,
            {"limit": {"tif": "Gtc"}},
            reduce_only=reduce_only,
        )

    def place_tp(self, side: str, size: float, entry_price: float, tp_price: float) -> dict:
        """
        Coloca una orden Take-Profit trigger.
        side: side de la POSICIÓN abierta ('buy' → TP es sell, 'sell' → TP es buy).
        """
        is_buy_pos = side.lower() == "buy"
        is_buy_tp  = not is_buy_pos
        sz         = _round_sz(size, self.get_sz_decimals())
        if sz <= 0:
            raise ValueError(f"[{self.coin}] place_tp: size redondeado a 0 (raw={size})")
        trigger_px = self.round_px(tp_price)
        if is_buy_tp:
            limit_px = self.round_px(trigger_px * (1 + _TP_LIMIT_BUFFER))
        else:
            limit_px = self.round_px(trigger_px * (1 - _TP_LIMIT_BUFFER))
        if is_buy_pos and trigger_px <= entry_price:
            raise ValueError(f"[{self.coin}] place_tp LONG: tp_price({trigger_px}) debe ser > entry({entry_price})")
        if not is_buy_pos and trigger_px >= entry_price:
            raise ValueError(f"[{self.coin}] place_tp SHORT: tp_price({trigger_px}) debe ser < entry({entry_price})")
        return _exchange_call(
            self._exchange.order,
            self.coin,
            is_buy_tp,
            sz,
            limit_px,
            {"trigger": {"triggerPx": str(trigger_px), "isMarket": False, "tpsl": "tp"}},
            reduce_only=True,
        )

    def place_sl(self, side: str, size: float, entry_price: float, sl_price: float) -> dict:
        """
        Coloca una orden Stop-Loss trigger.
        side: side de la POSICIÓN abierta ('buy' → SL es sell, 'sell' → SL es buy).
        """
        is_buy_pos = side.lower() == "buy"
        is_buy_sl  = not is_buy_pos
        sz         = _round_sz(size, self.get_sz_decimals())
        if sz <= 0:
            raise ValueError(f"[{self.coin}] place_sl: size redondeado a 0 (raw={size})")
        trigger_px = self.round_px(sl_price)
        if is_buy_sl:
            limit_px = self.round_px(trigger_px * (1 + _MARKET_SLIPPAGE))
        else:
            limit_px = self.round_px(trigger_px * (1 - _MARKET_SLIPPAGE))
        if is_buy_pos and trigger_px >= entry_price:
            raise ValueError(f"[{self.coin}] place_sl LONG: sl_price({trigger_px}) debe ser < entry({entry_price})")
        if not is_buy_pos and trigger_px <= entry_price:
            raise ValueError(f"[{self.coin}] place_sl SHORT: sl_price({trigger_px}) debe ser > entry({entry_price})")
        return _exchange_call(
            self._exchange.order,
            self.coin,
            is_buy_sl,
            sz,
            limit_px,
            {"trigger": {"triggerPx": str(trigger_px), "isMarket": True, "tpsl": "sl"}},
            reduce_only=True,
        )

    def place_bulk(self, orders: list[dict]) -> dict:
        """
        Envía múltiples órdenes en un solo request (bulk order).
        """
        return _exchange_call(self._exchange.bulk_orders, orders)
