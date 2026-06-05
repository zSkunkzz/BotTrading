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

FIX round_px tick alignment + triggerPx float (2026-06-05 v16):
  BUG 1 — round_px usaba round() con pxDecimals.
    HL exige múltiplos EXACTOS del tick_size (e.g. SOL tick=0.001).
    round() podía generar 0.0013 → 'Price must be divisible by tick size'.
    Fix: math.floor(price / tick + 0.5) * tick cuando tick > 0.

  BUG 2 — place_tp / place_sl pasaban triggerPx como str(trigger_px).
    El SDK float_to_wire() espera float →
    ValueError: 'Unknown format code f for object of type str'.
    Fix: trigger_px ya es float tras round_px(); pasar directamente.
    También se fuerza float() sobre entry_price, tp_price, sl_price
    para tolerar valores que lleguen como string desde state.json.

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
                    self._tick_size_cache[name]   = ti