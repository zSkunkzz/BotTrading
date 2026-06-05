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
            logger.warning("[HLCore] No se pudo obtener meta para caché: %s", exc)
            return

        mid_prices: dict[str, float] = {}
        try:
            mids = self.info.all_mids()
            mid_prices = {k: float(v) for k, v in mids.items()}
        except Exception as exc:
            logger.debug("[HLCore] all_mids no disponible: %s", exc)

        perp_ctx: dict[str, dict] = {}
        try:
            ctxs = self.info.meta_and_asset_ctxs()
            if isinstance(ctxs, (list, tuple)) and len(ctxs) == 2:
                ctx_list = ctxs[1]
                for i, asset in enumerate(universe):
                    coin = asset.get("name", "")
                    if coin and i < len(ctx_list):
                        perp_ctx[coin] = ctx_list[i] or {}
        except Exception as exc:
            logger.debug("[HLCore] meta_and_asset_ctxs no disponible: %s", exc)

        for asset in universe:
            coin = asset.get("name", "")
            if not coin:
                continue

            self._sz_decimals_cache[coin] = int(asset.get("szDecimals", 4))

            raw_lev = asset.get("maxLeverage") or asset.get("leverage", {}).get("max") or 20
            self._max_leverage_cache[coin] = int(raw_lev)

            if "maxDecimals" in asset:
                px_dec = int(asset["maxDecimals"])
                self._px_decimals_cache[coin] = px_dec
                self._tick_size_cache[coin]   = 10 ** (-px_dec)
                continue

            ctx = perp_ctx.get(coin, {})
            mark_px_str = str(ctx.get("markPx") or ctx.get("mark_px") or "")
            if mark_px_str and mark_px_str not in ("None", ""):
                try:
                    float(mark_px_str)
                    if "." in mark_px_str:
                        dec_part = mark_px_str.rstrip("0").split(".")[1]
                        px_dec   = len(dec_part) if dec_part else 0
                    else:
                        px_dec = 0
                    mid = mid_prices.get(coin, 0.0)
                    if mid < 100 and px_dec < 2:
                        px_dec = 2
                    if mid < 10 and px_dec < 3:
                        px_dec = 3
                    self._px_decimals_cache[coin] = px_dec
                    self._tick_size_cache[coin]   = 10 ** (-px_dec)
                    continue
                except Exception:
                    pass

            mid = mid_prices.get(coin, 0.0)
            if mid >= 10_000:
                px_dec = 1
            elif mid >= 1_000:
                px_dec = 2
            elif mid >= 100:
                px_dec = 3
            elif mid >= 10:
                px_dec = 4
            elif mid >= 1:
                px_dec = 4
            else:
                px_dec = 5

            self._px_decimals_cache[coin] = px_dec
            self._tick_size_cache[coin]   = 10 ** (-px_dec)

        logger.info(
            "[HLCore] Caché pre-cargado: %d coins (szDecimals + pxDecimals + maxLeverage listos)",
            len(universe),
        )

    @classmethod
    def get(cls) -> "_HLCore":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    async def get_async(cls) -> "_HLCore":
        if cls._init_lock is None:
            cls._init_lock = asyncio.Lock()

        if cls._instance is not None:
            return cls._instance

        async with cls._init_lock:
            if cls._instance is not None:
                return cls._instance
            logger.info("[HLCore] Inicializando SDK en hilo separado (asyncio.to_thread)...")
            cls._instance = await asyncio.to_thread(cls._create_sync)
            return cls._instance

    @classmethod
    def _create_sync(cls) -> "_HLCore":
        return cls()

    @staticmethod
    def _build_exchange_with_retry(wallet, kwargs: dict, retries: int = 6) -> Exchange:
        delay = 2.0
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return Exchange(wallet=wallet, base_url=_BASE_URL, **kwargs)
            except Exception as exc:
                err = str(exc)
                if "429" in err or "ClientError" in type(exc).__name__:
                    logger.warning(
                        "[HLCore] Exchange init 429 (intento %d/%d) — reintentando en %.1fs",
                        attempt + 1, retries, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    last_exc = exc
                else:
                    raise
        raise RuntimeError(f"[HLCore] No se pudo inicializar Exchange tras {retries} intentos") from last_exc

    @staticmethod
    def _build_info_with_retry(retries: int = 6) -> Info:
        delay = 2.0
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return Info(base_url=_BASE_URL, skip_ws=True)
            except Exception as exc:
                err = str(exc)
                if "429" in err or "ClientError" in type(exc).__name__:
                    logger.warning(
                        "[HLCore] Info init 429 (intento %d/%d) — reintentando en %.1fs",
                        attempt + 1, retries, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    last_exc = exc
                else:
                    raise
        raise RuntimeError(f"[HLCore] No se pudo inicializar Info tras {retries} intentos") from last_exc


# ─────────────────────────────────────────────────────────────────
# HLClient: un cliente ligero por symbol, comparte _HLCore
# ─────────────────────────────────────────────────────────────────

class HLClient:
    """
    Cliente ligero por symbol. Comparte Exchange + Info via _HLCore singleton.
    Todas las llamadas de escritura pasan por _exchange_call() para garantizar
    nonces únicos (lock global + delay mínimo entre llamadas).
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
        except Exception as exc:
            logger.warning("[%s] No se pudo obtener meta: %s", self.coin, exc)
        return {}

    def get_sz_decimals(self) -> int:
        cache = self._core._sz_decimals_cache
        if self.coin in cache:
            return cache[self.coin]
        asset = self._get_meta_asset()
        dec = int(asset.get("szDecimals", 4))
        cache[self.coin] = dec
        return dec

    def get_px_decimals(self) -> int:
        cache = self._core._px_decimals_cache
        if self.coin in cache:
            return cache[self.coin]
        dec = self._infer_px_decimals_from_l2()
        cache[self.coin] = dec
        return dec

    def _infer_px_decimals_from_l2(self) -> int:
        try:
            l2   = self._info.l2_snapshot(self.coin)
            bids = l2.get("levels", [[], []])[0]
            asks = l2.get("levels", [[], []])[1]
            all_px = [float(p["px"]) for p in (bids[:5] + asks[:5]) if "px" in p]
            if len(all_px) >= 2:
                all_px_sorted = sorted(set(all_px))
                diffs = [
                    abs(all_px_sorted[i+1] - all_px_sorted[i])
                    for i in range(len(all_px_sorted) - 1)
                    if all_px_sorted[i+1] - all_px_sorted[i] > 1e-9
                ]
                if diffs:
                    tick = min(diffs)
                    dec  = _px_decimals_from_tick(tick)
                    self._core._tick_size_cache[self.coin] = tick
                    logger.debug("[%s] pxDecimals=%d (tick=%.8f desde L2)", self.coin, dec, tick)
                    return dec
            if all_px:
                px_str = str(bids[0]["px"]) if bids else str(asks[0]["px"])
                if "." in px_str:
                    return max(1, len(px_str.rstrip("0").split(".")[1]))
        except Exception as exc:
            logger.warning("[%s] No se pudo inferir pxDecimals desde L2: %s", self.coin, exc)
        return 4

    def get_tick_size(self) -> float:
        cache = self._core._tick_size_cache
        if self.coin in cache:
            return cache[self.coin]
        dec  = self.get_px_decimals()
        tick = 10 ** (-dec)
        cache[self.coin] = tick
        return tick

    def get_max_leverage(self) -> int:
        cache = self._core._max_leverage_cache
        if self.coin in cache:
            return cache[self.coin]
        asset = self._get_meta_asset()
        lev = int(
            asset.get("maxLeverage")
            or asset.get("leverage", {}).get("max")
            or 20
        )
        cache[self.coin] = lev
        return lev

    def round_px(self, price: float) -> float:
        dec = self.get_px_decimals()
        return round(price, dec)

    def round_sz(self, sz: float) -> float:
        dec = self.get_sz_decimals()
        return _round_sz(sz, dec)

    def _adjust_sl_px(self, trigger_px: float, entry_px: Optional[float], is_long: bool) -> float:
        dec  = self.get_px_decimals()
        tick = self.get_tick_size()
        px   = round(trigger_px, dec)

        if entry_px and entry_px > 0:
            if is_long:
                while px >= entry_px:
                    px = round(px - tick, dec)
            else:
                while px <= entry_px:
                    px = round(px + tick, dec)

        return px

    def _adjust_tp_px(self, trigger_px: float, entry_px: Optional[float], is_long: bool) -> float:
        dec  = self.get_px_decimals()
        tick = self.get_tick_size()
        px   = round(trigger_px, dec)

        if entry_px and entry_px > 0:
            if is_long:
                while px <= entry_px:
                    px = round(px + tick, dec)
            else:
                while px >= entry_px:
                    px = round(px - tick, dec)

        return px

    # ── ÓRDENES BÁSICAS ───────────────────────────────────────

    def place_limit(
        self,
        is_buy: bool,
        sz: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> dict:
        price = self.round_px(price)
        sz    = self.round_sz(sz)
        return _exchange_call(
            self._exchange.order,
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=price,
            order_type={"limit": {"tif": tif}},
            reduce_only=reduce_only,
        )

    def place_market(
        self,
        is_buy: bool,
        sz: float,
        reduce_only: bool = False,
        ref_price: Optional[float] = None,
    ) -> dict:
        sz = self.round_sz(sz)
        if ref_price is None or ref_price <= 0:
            try:
                l2 = self._info.l2_snapshot(self.coin)
                best_ask = float(l2["levels"][1][0]["px"])
                best_bid = float(l2["levels"][0][0]["px"])
                ref_price = (best_ask + best_bid) / 2
            except Exception:
                ref_price = 0.0

        if ref_price and ref_price > 0:
            if is_buy:
                slippage_px = self.round_px(ref_price * (1 + _MARKET_SLIPPAGE))
            else:
                slippage_px = self.round_px(ref_price * (1 - _MARKET_SLIPPAGE))
        else:
            slippage_px = 999_999_999.0 if is_buy else 0.000001

        return _exchange_call(
            self._exchange.order,
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=slippage_px,
            order_type={"limit": {"tif": "Ioc"}},
            reduce_only=reduce_only,
        )

    # ── TRIGGER ORDERS — TP / SL ────────────────────────────

    def place_tp(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        limit_px:  Optional[float] = None,
        entry_px:  Optional[float] = None,
    ) -> dict:
        sz         = self.round_sz(sz)
        is_long    = not is_buy
        trigger_px = self._adjust_tp_px(trigger_px, entry_px, is_long)
        is_market  = limit_px is None

        if is_market:
            effective_limit_px = trigger_px
        else:
            if not is_buy:
                effective_limit_px = self.round_px(trigger_px * (1 - _TP_LIMIT_BUFFER))
            else:
                effective_limit_px = self.round_px(trigger_px * (1 + _TP_LIMIT_BUFFER))

        logger.debug(
            "[%s] place_tp: is_buy=%s sz=%.6f trigger=%.6f limit=%.6f entry=%s",
            self.coin, is_buy, sz, trigger_px, effective_limit_px,
            f"{entry_px:.6f}" if entry_px else "N/A",
        )

        return _exchange_call(
            self._exchange.order,
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=effective_limit_px,
            order_type={
                "trigger": {
                    "triggerPx": trigger_px,
                    "isMarket":  is_market,
                    "tpsl":      "tp",
                }
            },
            reduce_only=True,
        )

    def place_sl(
        self,
        is_buy: bool,
        sz: float,
        trigger_px: float,
        entry_px:   Optional[float] = None,
    ) -> dict:
        sz         = self.round_sz(sz)
        is_long    = not is_buy
        trigger_px = self._adjust_sl_px(trigger_px, entry_px, is_long)

        logger.debug(
            "[%s] place_sl: is_buy=%s sz=%.6f trigger=%.6f entry=%s",
            self.coin, is_buy, sz, trigger_px,
            f"{entry_px:.6f}" if entry_px else "N/A",
        )

        return _exchange_call(
            self._exchange.order,
            name=self.coin,
            is_buy=is_buy,
            sz=sz,
            limit_px=trigger_px,
            order_type={
                "trigger": {
                    "triggerPx": trigger_px,
                    "isMarket":  True,
                    "tpsl":      "sl",
                }
            },
            reduce_only=True,
        )

    def place_bulk(self, orders: list[dict]) -> dict:
        cleaned = []
        for o in orders:
            o  = dict(o)
            if "sz" in o:
                o["sz"] = self.round_sz(float(o["sz"]))
            ot = o.get("order_type", {})
            if isinstance(ot, dict) and "trigger" in ot:
                trig = dict(ot["trigger"])
                if "triggerPx" in trig:
                    trig["triggerPx"] = self.round_px(float(trig["triggerPx"]))
                ot = dict(ot)
                ot["trigger"] = trig
                o["order_type"] = ot
            if "limit_px" in o and o["limit_px"] is not None:
                o["limit_px"] = self.round_px(float(o["limit_px"]))
            cleaned.append(o)
        return _exchange_call(self._exchange.bulk_orders, cleaned)

    def update_leverage(self, leverage: int, is_cross: bool = False) -> dict:
        """
        Configura el leverage con lock global para evitar duplicate nonce.
        Incluye retry automático si HL responde 429.
        Llamar siempre desde asyncio.to_thread() en trader.py.
        """
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
        return [
            p for p in state.get("assetPositions", [])
            if p.get("position", {}).get("coin") == self.coin
               and float(p.get("position", {}).get("szi", 0)) != 0
        ]

    def get_balance_usdc(self) -> float:
        state = self.get_user_state()
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
