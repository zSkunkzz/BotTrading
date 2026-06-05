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
# Garantiza timestamps distintos (nonce = time_ns // 1_000_000) y reduce
# el riesgo de 429 por rate-limit de HL en arranques con muchos traders.
# Configurable con HL_NONCE_MIN_DELAY_MS (default 50ms).
_NONCE_MIN_DELAY_MS = float(os.getenv("HL_NONCE_MIN_DELAY_MS", "50")) / 1000.0

# Reintentos automáticos en _exchange_call cuando HL responde 429.
# Configurable con HL_EXCHANGE_RETRIES (default 3) y
# HL_EXCHANGE_RETRY_DELAY_S (espera inicial en segundos, default 2.0).
_EXCHANGE_RETRIES     = int(float(os.getenv("HL_EXCHANGE_RETRIES", "3")))
_EXCHANGE_RETRY_DELAY = float(os.getenv("HL_EXCHANGE_RETRY_DELAY_S", "2.0"))

# Lock global de escritura al Exchange.
# threading.Lock (no asyncio.Lock) porque las llamadas ocurren en hilos
# via asyncio.to_thread(), no en el event loop directamente.
_EXCHANGE_LOCK = threading.Lock()

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
    # El SDK lanza una tupla/list con el status como primer elemento.
    # También puede venir como string "429" en el mensaje.
    if "429" in err:
        return True
    # Formato tuple del SDK: (status_code, ...) — primer arg int
    if exc.args and isinstance(exc.args[0], int) and exc.args[0] == 429:
        return True
    return False


def _exchange_call(fn, *args, **kwargs):
    """
    Ejecuta una llamada de escritura al Exchange SDK con lock global.

    Garantiza:
      1. Solo una llamada activa al SDK a la vez (_EXCHANGE_LOCK).
      2. Timestamps distintos entre llamadas consecutivas (_NONCE_MIN_DELAY_MS).
      3. Retry automático con backoff exponencial si HL responde 429.
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

    # Solo llega aquí si _EXCHANGE_RETRIES == 0 (misconfiguración)
    if last_exc is not None:
        raise last_exc


# ─────────────────────────────────────────────────────────────────
# _HLCore: singleton que contiene el Exchange + Info compartidos
# ─────────────────────────────────────────────────────────────────

class _HLCore:
    """
    Singleton que mantiene UNA instancia de Exchange + Info.
    Pre-carga szDecimals, pxDecimals y maxLeverage al arrancar.

    IMPORTANTE: la creación debe hacerse SIEMPRE mediante get_async()
    desde código async (dentro del event loop). Nunca llamar get()
    directamente desde __init__ de clases que se instancian antes de
    que asyncio.run() esté activo.
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
        """
        Pre-carga szDecimals, pxDecimals, tick_size y maxLeverage.
        NOTA: este método es síncrono y bloqueante — debe llamarse SIEMPRE
        desde asyncio.to_thread() (via get_async()), nunca directamente
        desde código async o desde __init__ de clases raíz.
        """
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
        """
        Obtiene (o crea) el singleton de forma async-safe.
        La creación real ocurre en asyncio.to_thread() para no bloquear
        el event loop con las llamadas HTTP síncronas de _warm_cache().
        """
        if cls._instance is not None:
            return cls._instance

        # Crear el lock si aún no existe (primera llamada)
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()

        async with cls._init_lock:
            # Double-check dentro del lock
            if cls._instance is None:
                cls._instance = await asyncio.to_thread(cls)
        return cls._instance

    # ── Builders con retry ────────────────────────────────────────

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

    # ── Acceso a caché ────────────────────────────────────────────

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

    Uso:
        client = await HLClient.create("BTC")
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

    # ── METADATOS ──────────────────────────────────────────

    def _get_meta_asset(self) -> dict:
        try:
            meta = self._info.meta()
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

    # ── OHLCV ──────────────────────────────────────────────

    def get_ohlcv(self, timeframe: str = "15m", limit: int = 100) -> list[dict]:
        tf_map = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15,
            "30m": 30, "1h": 60, "2h": 120, "4h": 240,
            "6h": 360, "8h": 480, "12h": 720, "1d": 1440,
        }
        interval = tf_map.get(timeframe, 15)
        end_ts   = int(time.time() * 1000)
        start_ts = end_ts - limit * interval * 60 * 1000

        candles = self._info.candles_snapshot(self.coin, interval, start_ts, end_ts)
        result  = []
        for c in candles:
            try:
                result.append({
                    "timestamp": int(c["T"]),
                    "open":  float(c["o"]),
                    "high":  float(c["h"]),
                    "low":   float(c["l"]),
                    "close": float(c["c"]),
                    "volume": float(c["v"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return result

    # ── PRECIO ─────────────────────────────────────────────

    def get_price(self) -> float:
        try:
            mids = self._info.all_mids()
            val  = mids.get(self.coin)
            if val is not None:
                return float(val)
        except Exception:
            pass
        candles = self.get_ohlcv("1m", 1)
        if candles:
            return candles[-1]["close"]
        return 0.0

    # ── LEVERAGE ───────────────────────────────────────────

    def set_leverage(self, leverage: int, is_cross: bool = True) -> dict:
        return _exchange_call(
            self._exchange.update_leverage,
            leverage,
            self.coin,
            is_cross,
        )

    # ── CONSULTAS INFO (solo lectura, sin lock) ───────────────────

    def get_user_state(self) -> dict:
        return self._info.user_state(self._account_addr)

    def get_open_orders(self) -> list:
        return self._info.open_orders(self._account_addr)

    def get_positions(self) -> list:
        state = self.get_user_state()
        # Hyperliquid puede devolver dict (normal) o list (sin agente / edge case).
        # Cuando agent_mode=False el SDK puede retornar directamente la lista
        # de posiciones sin el wrapper {assetPositions:[...]}.
        if isinstance(state, dict):
            asset_positions = state.get("assetPositions", [])
        elif isinstance(state, list):
            # La API devolvió directamente la lista de posiciones
            asset_positions = state
        else:
            logger.warning(
                "[%s] _get_positions: respuesta inesperada (tipo=%s): %r",
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

    # ── ÓRDENES ────────────────────────────────────────────

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
        order = {"limit_px": limit_px, "is_buy": is_buy, "sz": sz, "reduce_only": False}
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
        is_buy_tp  = not is_buy_pos   # la orden TP cierra la posición
        sz         = _round_sz(size, self.get_sz_decimals())
        if sz <= 0:
            raise ValueError(f"[{self.coin}] place_tp: size redondeado a 0 (raw={size})")
        trigger_px = self.round_px(tp_price)
        # Precio límite ligeramente más favorable para garantizar fill
        if is_buy_tp:
            limit_px = self.round_px(trigger_px * (1 + _TP_LIMIT_BUFFER))
        else:
            limit_px = self.round_px(trigger_px * (1 - _TP_LIMIT_BUFFER))
        # Validación lógica: TP debe estar al otro lado de entry
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
        is_buy_sl  = not is_buy_pos   # la orden SL cierra la posición
        sz         = _round_sz(size, self.get_sz_decimals())
        if sz <= 0:
            raise ValueError(f"[{self.coin}] place_sl: size redondeado a 0 (raw={size})")
        trigger_px = self.round_px(sl_price)
        # Precio límite con slippage para garantizar fill en stop
        if is_buy_sl:
            limit_px = self.round_px(trigger_px * (1 + _MARKET_SLIPPAGE))
        else:
            limit_px = self.round_px(trigger_px * (1 - _MARKET_SLIPPAGE))
        # Validación lógica: SL debe estar al mismo lado (contrario del TP)
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
        Cada elemento de orders es un dict con keys:
          coin, is_buy, sz, limit_px, order_type, reduce_only (opcional).
        """
        return _exchange_call(self._exchange.bulk_orders, orders)
"""
<parameter name="_tool_input_summary">Update bot/core/hl_client.py with 3 fixes: (1) get_positions handles list and dict responses, (2) get_balance_usdc defensive isinstance check, (3) warning on startup when agent_mode=False. SHA: 392057118a68a4278c6ac42ef61e76161675376b