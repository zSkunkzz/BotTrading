"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Sizing en Hyperliquid:
  margen    = USDC_PER_TRADE          ← lo que arriesgas, siempre fijo
  notional  = USDC_PER_TRADE × lev   ← tamaño real de la posición
  qty       = notional / entry_price
  El leverage_efectivo = min(LEVERAGE_env, signal.suggested_lev, maxLeverage_del_exchange)

BUG #2 FIX: race condition en _open_lock
BUG #7 FIX: signal_flip_guard integrado en _try_open_position
BUG #8 FIX: cierre de emergencia reduce-only en sl_hit cuando _protection_ok=False
BUG #9 FIX: _ensure_tpsl usa reduce_only=True como fuente de verdad
OHLCV FIX: decide() recibe ohlcv_fn=self.get_ohlcv
TRAILING SL FIX: trailing_sl.py integrado en _manage_open_position
ROORDERS FIX: al restaurar posición, cancelar ro_orders acumuladas si > 2

FIX cancel_order:
  _exchange.cancel_order(oid, coin) no existe en el SDK de Hyperliquid.
  El método real es _exchange.cancel(coin, oid_int).

FIX pretrade_risk restart:
  Al reiniciarse el bot (Railway/cualquier plataforma), el singleton
  pretrade_risk._open_margin se reseteaba a 0 aunque hubiera una posición
  abierta en el exchange.

FIX bulk sz rounding (2026-06-02):
  _place_tpsl construía los dicts de bulk_orders con qty crudo (sin redondear).
  Fix: rounded_qty = client.round_sz(qty) antes de construir los dicts.

FIX DecisionEngine import (2026-06-02 v3):
  El módulo real es bot/decision_engine.py, NO bot/core/decision_engine.py.
  Corregido: from bot.decision_engine import DecisionEngine as _DecisionEngine

FIX KS L3 falso positivo tras restart (2026-06-02):
  Al restaurar posición, _protection_ok=False pero el watchdog disparaba
  on_state_mismatch cada 30s antes de que _manage_open_position verificara
  la protección (cada 120s). Fix: llamar _ensure_tpsl_on_exchange() en _init()
  envuelto con mark_tpsl_retrying()/clear_tpsl_retrying() para inhibir el
  watchdog durante la verificación y reposición.

FIX DecisionEngine/pretrade_risk interface (2026-06-02):
  1. on_position_closed en decision_engine.py llamaba
     `await self._risk.register_close()` pero ese método es síncrono.
     Resultado: TypeError: object NoneType can't be used in 'await' expression.
     Fix aplicado en decision_engine.py (llamada directa sin await).
  2. on_order_confirmed en _execute_signal no tenía try/except — si fallaba
     propagaba sin capturar y dejaba el risk ledger sin confirmar.
     Fix: envuelto en try/except con log.warning.
  3. _last_tpsl_verify_at = 0.0 antes de _verify_protection_on_restore()
     era redundante y exponía race condition si _verify fallaba:
     el watchdog veía 0.0 y disparaba on_state_mismatch inmediatamente.
     Fix: eliminada la asignación explícita (ya vale 0.0 en __init__).

FIX _get_signal → strategy.decide() (2026-06-02):
  _get_signal llamaba ai_decide() directamente, devolviendo solo
  {"action","confidence","reasoning"} sin sl/tp1/tp2.
  _execute_signal recíbia sl=None, tp1=None → nunca colocaba SL/TP
  → _protection_ok=False → KillSwitch disparaba KS L3 en 30s.
  Además todo el pipeline técnico (signal_engine, enriched_filter,
  F&G, funding, OI) era ignorado completamente.
  Fix: _get_signal ahora llama strategy.decide() con la firma correcta
  y extrae sl/tp1/tp2/entry_mode/rr del SignalResult devuelto.

FIX _execute_signal risk.usdc_per_trade (2026-06-02):
  _execute_signal usaba `risk.usdc_per_trade` como fallback de margin, pero
  `risk` es RiskManager (no tiene ese atributo) → AttributeError silencioso.
  Fix: usar float(os.getenv("USDC_PER_TRADE", "20")) como fallback.

FIX MANUAL_CLOSE cooldown (2026-06-02):
  Al detectar CLOSED_EXTERNALLY (posición en estado local pero ausente en
  exchange), se registra un cooldown de COOLDOWN_MANUAL_CLOSE segundos
  (por defecto 600s = 10 min) para evitar reentrar inmediatamente en el
  mismo símbolo tras un cierre manual. El cooldown se comprueba al inicio
  de _try_open_position y bloquea nuevas entradas hasta que expire.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
import json as _json
from typing import Optional

import aiohttp
from eth_account import Account

from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close, notify_tp_partial
from bot.state import save_position, load_position, clear_position, mark_tp2_hit
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.execution.execution_engine import execution_engine
from bot.ohlcv_cache import ohlcv_cache
from bot.signal_engine import signal_flip_guard
from bot.trailing_sl import compute_trailing_sl, is_trailing_sl_hit
from bot.signal_cooldown import signal_cooldown

# ── DecisionEngine (opcional) ────────────────────────────────────────────
try:
    from bot.decision_engine import DecisionEngine as _DecisionEngine  # FIX: era bot.core.decision_engine
    _DE_AVAILABLE = True
except ImportError:
    _DecisionEngine = None
    _DE_AVAILABLE = False

logger = logging.getLogger("Trader")

LOOP_SLEEP        = float(os.getenv("LOOP_SLEEP", "10"))
TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))
OHLCV_TF          = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT       = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS    = int(os.getenv("OHLCV_MIN_BARS", "55"))

_POS_CHECK_INTERVAL_S   = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))
_TPSL_VERIFY_INTERVAL_S = int(os.getenv("TPSL_VERIFY_INTERVAL_S", "120"))
_SL_SW_MARGIN           = float(os.getenv("SL_SW_MARGIN", "0.001"))

_POST_FILL_CONFIRM_DELAY_S = float(os.getenv("POST_FILL_CONFIRM_DELAY_S", "3.0"))
_POST_FILL_CONFIRM_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "6"))

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

_SET_LEVERAGE_TIMEOUT = float(os.getenv("SET_LEVERAGE_TIMEOUT", "20"))

_FALLBACK_ATR_SL_MULT  = float(os.getenv("FALLBACK_ATR_SL_MULT",  "1.8"))
_FALLBACK_ATR_TP1_MULT = float(os.getenv("FALLBACK_ATR_TP1_MULT", "3.5"))
_FALLBACK_ATR_TP2_MULT = float(os.getenv("FALLBACK_ATR_TP2_MULT", "5.0"))

_MAX_EXPECTED_RO_ORDERS = int(os.getenv("MAX_EXPECTED_RO_ORDERS", "2"))

_USDC_PER_TRADE = float(os.getenv("USDC_PER_TRADE", "20"))

# ── Rate limiter global para /info ─────────────────────────────────────────
GL_REST_LOCK    = asyncio.Lock()
_HL_LAST_CALL    = 0.0
_HL_MIN_INTERVAL = 0.6

async def _hl_throttle():
    global _HL_LAST_CALL
    async with GL_REST_LOCK:
        now = time.monotonic()
        wait = _HL_MIN_INTERVAL - (now - _HL_LAST_CALL)
        if wait > 0:
            await asyncio.sleep(wait)
        _HL_LAST_CALL = time.monotonic()


# ── Helpers ────────────────────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def _hl_side_to_str(raw_side: str) -> Optional[str]:
    if not raw_side:
        return None
    if raw_side == "B":
        return "long"
    if raw_side == "A":
        return "short"
    s = raw_side.lower()
    if s in ("long", "buy"):
        return "long"
    if s in ("short", "sell"):
        return "short"
    raise ValueError(f"Side desconocido de HL: {raw_side!r}")


def _nonzero(v) -> Optional[float]:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _compute_fallback_tpsl(
    side: str,
    entry: float,
    atr: float,
) -> tuple[float, float, float]:
    is_long   = side == "long"
    risk_dist = atr * _FALLBACK_ATR_SL_MULT
    if is_long:
        sl  = entry - risk_dist
        tp1 = entry + risk_dist * _FALLBACK_ATR_TP1_MULT
        tp2 = entry + risk_dist * _FALLBACK_ATR_TP2_MULT
    else:
        sl  = entry + risk_dist
        tp1 = entry - risk_dist * _FALLBACK_ATR_TP1_MULT
        tp2 = entry - risk_dist * _FALLBACK_ATR_TP2_MULT
    return round(sl, 6), round(tp1, 6), round(tp2, 6)


def _is_reduce_only_order(o: dict) -> bool:
    if o.get("reduceOnly") is True:
        return True
    if o.get("reduce_only") is True:
        return True
    ot = o.get("orderType", "")
    if isinstance(ot, dict):
        tpsl = ot.get("trigger", {}).get("tpsl", "")
        if tpsl in ("sl", "tp"):
            return True
        tpsl2 = ot.get("limit", {}).get("tpsl", "")
        if tpsl2 in ("sl", "tp"):
            return True
    return False


def _hl_cancel_order(exchange, coin: str, oid) -> None:
    exchange.cancel(coin, int(oid))


# ────────────────────────────────────────────────────────────────────────────────────
class FuturesTrader:
    def __init__(self, api_key, api_secret, symbol,
                 leverage, margin_mode, dry_run,
                 passphrase=None):
        self.symbol      = symbol
        self.coin        = _norm_coin(symbol)
        self.leverage    = leverage
        self.margin_mode = os.getenv("MARGIN_MODE", margin_mode or "isolated").lower()
        self.dry_run     = dry_run

        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            if not api_wallet:
                raise ValueError(f"[{symbol}] HL_API_WALLET_ADDRESS es OBLIGATORIA en modo agente.")
            self._private_key  = api_pk
            self._agent_mode   = True
            agent_acct         = Account.from_key(api_pk)
            self._agent_addr   = agent_acct.address
            self._master_addr  = api_wallet
            self._account_addr = self._master_addr
            from bot.core.hl_client import HLClient
            self._hl_client = HLClient(symbol)
            logger.info(
                "[%s] Auth: modo agente | master=%s | agente=%s",
                symbol, self._master_addr[:10] + "...", self._agent_addr[:10] + "...",
            )
        else:
            pk = os.getenv("HL_PRIVATE_KEY", api_secret or "").strip()
            if not pk:
                raise ValueError(f"[{symbol}] No hay ninguna clave configurada.")
            self._private_key   = pk
            self._account_addr  = os.getenv("HL_ACCOUNT_ADDR", "").strip()
            self._agent_mode    = False
            self._agent_addr    = ""
            if not self._account_addr:
                acct = Account.from_key(pk)
                self._account_addr = acct.address
            self._master_addr  = self._account_addr
            from bot.core.hl_client import HLClient
            self._hl_client = HLClient(symbol)
            logger.info(
                "[%s] Auth: modo directo | addr=%s",
                symbol, self._account_addr[:10] + "..." if self._account_addr else "N/A",
            )

        self.position       = None
        self.entry_price    = None
        self.sl             = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit        = False
        self._tp1_hit       = False
        self.trade_count    = 0
        self.win_count      = 0
        self.total_pnl      = 0.0
        self._open_notional = 0.0
        self._open_leverage = 1
        self._open_margin   = 0.0
        self._open_qty      = 0.0
        self._open_entry_mode = ""
        self._protection_ok = False
        self._last_pos_check_at:   float = 0.0
        self._last_tpsl_verify_at: float = 0.0
        self._ccxt_exchange = None
        self._global_risk   = None

        self._trailing_sl_activated: bool  = False
        self._trail_peak:            float = 0.0
        self._open_lock: asyncio.Lock = asyncio.Lock()
        self._stopped_event: asyncio.Event = asyncio.Event()

        # DecisionEngine: firma real __init__(pretrade_risk, signal_engine, usdc_per_trade, leverage)
        if _DE_AVAILABLE:
            self._decision_engine = _DecisionEngine(
                pretrade_risk=pretrade_risk,
                usdc_per_trade=_USDC_PER_TRADE,
                leverage=leverage,
            )
        else:
            self._decision_engine = None

    # ── Max leverage efectivo del exchange ────────────────────────────────────

    def _exchange_max_lev(self) -> int:
        try:
            return self._hl_client.get_max_leverage()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener maxLeverage — usando 20: %s", self.symbol, e)
            return 20

    # ── Qty rounding ─────────────────────────────────────────────────────

    def _round_qty(self, qty: float) -> float:
        try:
            sz_dec = self._hl_client.get_sz_decimals()
        except Exception:
            sz_dec = 4
        factor = 10 ** sz_dec
        return math.floor(qty * factor) / factor

    # ── ccxt ─────────────────────────────────────────────────────────────────────────────

    async def _get_ccxt(self):
        if self._ccxt_exchange is None:
            import ccxt.async_support as ccxt
            if self._agent_mode:
                self._ccxt_exchange = ccxt.hyperliquid({
                    "walletAddress":   self._master_addr,
                    "privateKey":      self._private_key,
                    "enableRateLimit": True,
                    "options": {"agentAddress": self._agent_addr},
                })
            else:
                self._ccxt_exchange = ccxt.hyperliquid({
                    "walletAddress":   self._master_addr,
                    "privateKey":      self._private_key,
                    "enableRateLimit": True,
                })
        return self._ccxt_exchange

    async def _close_ccxt(self):
        if self._ccxt_exchange is not None:
            try:
                await self._ccxt_exchange.close()
            except Exception:
                pass
            self._ccxt_exchange = None

    # ── HTTP helpers ─────────────────────────────────────────────────────────────────

    async def _info_post(self, payload: dict) -> dict:
        await _hl_throttle()
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/info", json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 429:
                    logger.warning("[%s] 429 en /info, esperando 5s...", self.symbol)
                    await asyncio.sleep(5.0)
                    await _hl_throttle()
                    async with aiohttp.ClientSession() as s2:
                        async with s2.post(
                            f"{_API_URL}/info", json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r2:
                            return _json.loads(await r2.text())
                return _json.loads(await r.text())

    # ── Init / cleanup ───────────────────────────────────────────────────────────────────

    async def _cleanup_excess_ro_orders(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            raw_orders = await loop.run_in_executor(None, self._hl_client.get_open_orders)
            coin_orders = [o for o in (raw_orders or []) if o.get("coin") == self.coin]
            ro_orders = [o for o in coin_orders if _is_reduce_only_order(o)]

            if len(ro_orders) <= _MAX_EXPECTED_RO_ORDERS:
                return

            logger.warning(
                "[%s] ROORDERS FIX: %d órdenes reduce_only acumuladas (≤%d esperadas) — "
                "cancelando y recolocando SL/TP limpios.",
                self.symbol, len(ro_orders), _MAX_EXPECTED_RO_ORDERS,
            )
            cancelled = await self._cancel_all_orders_reduce_only(coin_orders)
            logger.info("[%s] ROORDERS FIX: %d órdenes canceladas.", self.symbol, cancelled)
            self._last_tpsl_verify_at = 0.0
            self._protection_ok = False
        except Exception as e:
            logger.warning("[%s] _cleanup_excess_ro_orders error: %s", self.symbol, e)

    def _restore_position_fields(self, saved: dict) -> None:
        self.position         = saved["side"]
        self.entry_price      = saved["entry"]
        self.sl               = saved.get("sl")
        self.tp1              = saved.get("tp1")
        self.tp2              = saved.get("tp2")
        self.tp3              = saved.get("tp3")
        self.tp2_hit          = saved.get("tp2_hit", False)
        self._tp1_hit         = saved.get("tp1_hit", False)
        self._open_notional   = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
        self._open_leverage   = saved.get("leverage", self.leverage)

        saved_margin = saved.get("margin_usdc")
        if saved_margin and saved_margin > 0:
            self._open_margin = saved_margin
        elif self._open_leverage > 0:
            self._open_margin = self._open_notional / self._open_leverage
        else:
            self._open_margin = self._open_notional

        saved_qty = saved.get("qty")
        if saved_qty and float(saved_qty) > 0:
            self._open_qty = float(saved_qty)
        elif self._open_notional > 0 and self.entry_price and self.entry_price > 0:
            self._open_qty = self._open_notional / self.entry_price

        self._open_entry_mode           = saved.get("entry_mode", "")
        self._protection_ok             = False
        self._trailing_sl_activated     = saved.get("trailing_sl_activated", False)
        self._trail_peak                = saved.get("trail_peak", 0.0)

    async def _verify_protection_on_restore(self) -> None:
        """
        Llamado desde _init() tras restaurar una posición guardada.
        Verifica si ya existen SL/TP en el exchange y, si no, los repone.
        Usa mark_tpsl_retrying() para que el watchdog del KS ignore este
        símbolo durante la verificación y evite falsos state_mismatch.
        """
        if not self.position or not self.sl or not self.tp1:
            return

        kill_switch.mark_tpsl_retrying(self.symbol)
        try:
            await self._ensure_tpsl_on_exchange(
                qty=self._open_qty,
                sl=self.sl,
                tp1=self.tp1,
                pos_side=self.position,
            )
            self._last_tpsl_verify_at = time.monotonic()
            if self._protection_ok:
                logger.info(
                    "[%s] Protección SL/TP verificada/repuesta tras restart — _protection_ok=True",
                    self.symbol,
                )
            else:
                logger.warning(
                    "[%s] No se pudo verificar/reponer SL/TP tras restart",
                    self.symbol,
                )
        except Exception as e:
            logger.warning("[%s] _verify_protection_on_restore error: %s", self.symbol, e)
        finally:
            kill_switch.clear_tpsl_retrying(self.symbol)

    async def _init(self, usdc_per_trade: float):
        await self._get_ccxt()
        saved = load_position(self.symbol)
        if saved:
            exchange_pos = await self._get_positions()
            if exchange_pos is not None and len(exchange_pos) > 0:
                self._restore_position_fields(saved)
                logger.info("[%s] Posicion restaurada: %s @ %s",
                            self.symbol, self.position, self.entry_price)

                if self._open_margin > 0:
                    existing = pretrade_risk._open_margin_by_symbol.get(self.symbol, 0.0)
                    if existing == 0.0:
                        pretrade_risk.confirm_order(self.symbol, self._open_margin)
                        logger.info(
                            "[%s] pretrade_risk reconstruido: margin=%.2f USDC",
                            self.symbol, self._open_margin,
                        )

                await self._cleanup_excess_ro_orders()
                await self._verify_protection_on_restore()

            elif exchange_pos is not None and len(exchange_pos) == 0:
                logger.warning(
                    "[%s] Posicion guardada localmente pero NO existe en exchange — limpiando.",
                    self.symbol,
                )
                clear_position(self.symbol)
            else:
                logger.warning(
                    "[%s] No se pudo verificar posicion en exchange — restaurando sin proteccion OK.",
                    self.symbol,
                )
                self._restore_position_fields(saved)

        if not balance_svc.is_ready():
            balance_svc.init_hl(self._master_addr, self._info_post)

        exchange_max = self._exchange_max_lev()
        effective    = min(self.leverage, exchange_max)
        if effective < self.leverage:
            logger.warning(
                "[%s] LEVERAGE config=%dx > maxLeverage exchange=%dx — se usara %dx.",
                self.symbol, self.leverage, exchange_max, effective,
            )
        else:
            logger.info(
                "[%s] Leverage efectivo: %dx (config=%dx, exchange_max=%dx)",
                self.symbol, effective, self.leverage, exchange_max,
            )

        await self._set_leverage(effective)
        logger.info(
            "[%s] Trader iniciado | coin=%s | master=%s | agent_mode=%s",
            self.symbol, self.coin,
            self._master_addr[:10] + "..." if self._master_addr else "N/A",
            self._agent_mode,
        )

    async def cleanup(self):
        await self._close_ccxt()
        self._stopped_event.set()
        logger.info("[%s] Trader cleanup completado.", self.symbol)

    # ── Precio y OHLCV ───────────────────────────────────────────────────────────────────

    async def get_price(self) -> float:
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.is_price_fresh(self.coin):
                price = ws_feed.get_price(self.coin)
                if price and price > 0:
                    return price
        except Exception:
            pass
        data = await self._info_post({"type": "allMids"})
        mid = data.get(self.coin)
        if mid:
            return float(mid)
        raise ValueError(f"No se pudo obtener precio para {self.coin}")

    async def get_ohlcv(self, tf: str = OHLCV_TF) -> list:
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.has_data(self.coin, tf=tf, min_candles=OHLCV_MIN_BARS):
                df = ws_feed.get_ohlcv(self.coin, tf)
                if not df.empty and len(df) >= OHLCV_MIN_BARS:
                    df_reset = df.reset_index()
                    return [
                        [int(row["ts"].timestamp() * 1000),
                         float(row["open"]), float(row["high"]),
                         float(row["low"]),  float(row["close"]), float(row["volume"])]
                        for _, row in df_reset.iterrows()
                    ]
        except Exception as e:
            logger.debug("[%s] get_ohlcv WS error: %s", self.symbol, e)

        async def _fetch_rest(timeframe: str) -> list:
            tf_ms = {"15m": 15*60*1000, "1h": 60*60*1000, "4h": 4*60*60*1000}.get(timeframe, 15*60*1000)
            now   = int(time.time() * 1000)
            start = now - OHLCV_LIMIT * tf_ms
            data  = await self._info_post({
                "type": "candleSnapshot",
                "req":  {"coin": self.coin, "interval": timeframe, "startTime": start, "endTime": now},
            })
            if not isinstance(data, list) or not data:
                return []
            return [
                [int(c["t"]), float(c["o"]), float(c["h"]),
                 float(c["l"]), float(c["c"]), float(c["v"])]
                for c in data
            ]

        try:
            return await ohlcv_cache.get(self.coin, tf, _fetch_rest)
        except Exception as e:
            logger.error("[%s] get_ohlcv cache error: %s", self.symbol, e)
            return []

    async def get_balance(self) -> float | None:
        return await balance_svc.get()

    # ── Leverage ───────────────────────────────────────────────────────────────────────────────

    async def _set_leverage(self, leverage: int) -> None:
        if self.dry_run:
            return
        is_cross = self.margin_mode != "isolated"
        loop = asyncio.get_event_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: self._hl_client._exchange.update_leverage(
                    int(leverage), self.coin, is_cross
                ),
            ),
            timeout=_SET_LEVERAGE_TIMEOUT,
        )
        if isinstance(result, dict) and result.get("status") == "err":
            raise RuntimeError(
                f"Exchange rechazo leverage {leverage}x: {result.get('response', result)}"
            )
        logger.info("[%s] Leverage %dx establecido en exchange", self.symbol, leverage)

    # ── Ordenes ──────────────────────────────────────────────────────────────────────────────────────────────

    async def _get_order_status(self, order_id) -> dict:
        try:
            return await self._info_post({
                "type": "orderStatus",
                "user": self._account_addr,
                "oid":  int(order_id),
            })
        except Exception as e:
            logger.debug("[%s] _get_order_status error: %s", self.symbol, e)
            return {}

    async def _place_order(self, side: str, qty: float, reduce_only: bool = False,
                           sl: float | None = None, tp: float | None = None) -> dict:
        try:
            arrival_price = await self.get_price()
        except Exception:
            arrival_price = 0.0

        ask = bid = None
        try:
            from bot.ws_feed import ws_feed
            ob = ws_feed.get_orderbook_metrics(self.coin)
            if ob:
                ask = ob.get("ask")
                bid = ob.get("bid")
        except Exception:
            pass

        trade_side = "close" if reduce_only else "open"
        r = await execution_engine.execute(
            trader=self, side=side, qty=qty, arrival_price=arrival_price,
            ask=ask, bid=bid, trade_side=trade_side, reduce_only=reduce_only,
            sl=sl, tp=tp,
        )
        if r.get("status") == "ok":
            balance_svc.invalidate()
            await kill_switch.on_order_result(rejected=False)
        else:
            await kill_switch.on_order_result(rejected=True)
        return r

    # ── Posiciones ──────────────────────────────────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        try:
            data = await self._info_post({"type": "clearinghouseState", "user": self._account_addr})
            if not data or not isinstance(data, dict):
                return None
            positions = []
            for p in data.get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") != self.coin:
                    continue
                try:
                    szi = float(pos.get("szi", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if abs(szi) > 0:
                    positions.append(pos)
            return positions
        except Exception as e:
            logger.error("[%s] _get_positions error: %s", self.symbol, e)
            return None

    async def _confirm_position_with_retry(self) -> list | None:
        for attempt in range(_POST_FILL_CONFIRM_RETRIES):
            delay = _POST_FILL_CONFIRM_DELAY_S
            if attempt > 0:
                logger.debug(
                    "[%s] Post-fill confirm: intento %d/%d (%.1fs)...",
                    self.symbol, attempt + 1, _POST_FILL_CONFIRM_RETRIES, delay,
                )
            await asyncio.sleep(delay)
            positions = await self._get_positions()
            if positions is None:
                return None
            if len(positions) > 0:
                return positions
        logger.warning(
            "[%s] Post-fill confirm: posicion no visible tras %d intentos.",
            self.symbol, _POST_FILL_CONFIRM_RETRIES,
        )
        return []

    # ── _place_tpsl ───────────────────────────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        qty: float,
        sl: float | None,
        tp: float | None,
        entry_px: float | None = None,
    ) -> None:
        if not sl and not tp:
            return

        is_long      = self.position == "long"
        close_is_buy = not is_long
        client       = self._hl_client
        loop         = asyncio.get_event_loop()

        ep = entry_px if (entry_px and entry_px > 0) else self.entry_price

        # FIX bulk sz rounding
        rounded_qty = client.round_sz(qty)

        if sl and tp:
            bulk_ok = False
            try:
                sl_px = client._adjust_sl_px(sl, ep, is_long)
                tp_px = client._adjust_tp_px(tp, ep, is_long)
                tick      = client.get_tick_size()
                tp_lim_px = client.round_px(tp_px - tick if is_long else tp_px + tick)
                orders = [
                    {
                        "coin":        client.coin,
                        "is_buy":      close_is_buy,
                        "sz":          rounded_qty,
                        "limit_px":    sl_px,
                        "order_type":  {"trigger": {"triggerPx": sl_px, "isMarket": True,  "tpsl": "sl"}},
                        "reduce_only": True,
                    },
                    {
                        "coin":        client.coin,
                        "is_buy":      close_is_buy,
                        "sz":          rounded_qty,
                        "limit_px":    tp_lim_px,
                        "order_type":  {"trigger": {"triggerPx": tp_px, "isMarket": False, "tpsl": "tp"}},
                        "reduce_only": True,
                    },
                ]
                result = await loop.run_in_executor(
                    None, lambda: client._exchange.bulk_orders(orders)
                )
                statuses = (
                    result.get("response", {}).get("data", {}).get("statuses", [])
                    if isinstance(result, dict) else []
                )
                errors = [s for s in statuses if "error" in s]
                if errors:
                    raise RuntimeError(f"bulk_orders rechazado: {errors}")
                logger.info(
                    "[%s] SL=%.5f + TP=%.5f colocados en exchange (bulk)",
                    self.symbol, sl_px, tp_px,
                )
                bulk_ok = True
            except Exception as e:
                logger.warning("[%s] bulk SL+TP fallo (%s) — colocando individualmente", self.symbol, e)

            if bulk_ok:
                return

            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_sl(is_buy=close_is_buy, sz=qty, trigger_px=sl, entry_px=ep),
                )
                logger.info("[%s] SL=%.5f colocado (fallback)", self.symbol, sl)
            except Exception as e:
                logger.error("[%s] No se pudo colocar SL (fallback): %s", self.symbol, e)
                raise

            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_tp(is_buy=close_is_buy, sz=qty, trigger_px=tp, entry_px=ep),
                )
                logger.info("[%s] TP=%.5f colocado (fallback)", self.symbol, tp)
            except Exception as e:
                logger.error("[%s] No se pudo colocar TP (fallback): %s", self.symbol, e)
                raise
            return

        if sl:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_sl(is_buy=close_is_buy, sz=qty, trigger_px=sl, entry_px=ep),
                )
                logger.info("[%s] SL=%.5f colocado", self.symbol, sl)
            except Exception as e:
                logger.error("[%s] No se pudo colocar SL: %s", self.symbol, e)
                raise

        if tp:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_tp(is_buy=close_is_buy, sz=qty, trigger_px=tp, entry_px=ep),
                )
                logger.info("[%s] TP=%.5f colocado", self.symbol, tp)
            except Exception as e:
                logger.error("[%s] No se pudo colocar TP: %s", self.symbol, e)
                raise

    # ── _cancel_all_orders_reduce_only ────────────────────────────────────────────────────

    async def _cancel_all_orders_reduce_only(self, coin_orders: list) -> int:
        ro_orders = [o for o in coin_orders if _is_reduce_only_order(o)]
        if not ro_orders:
            return 0

        loop = asyncio.get_event_loop()
        cancelled = 0
        for o in ro_orders:
            oid = o.get("oid") or o.get("id") or o.get("orderId")
            if not oid:
                continue
            try:
                await loop.run_in_executor(
                    None,
                    lambda _oid=oid: _hl_cancel_order(self._hl_client._exchange, self.coin, _oid),
                )
                cancelled += 1
            except Exception as e:
                logger.warning("[%s] No se pudo cancelar orden oid=%s: %s", self.symbol, oid, e)
        logger.info(
            "[%s] _cancel_all_orders_reduce_only: %d/%d canceladas",
            self.symbol, cancelled, len(ro_orders),
        )
        return cancelled

    # ── _ensure_tpsl_on_exchange ─────────────────────────────────────────────────────────────────────

    async def _ensure_tpsl_on_exchange(
        self,
        qty: float,
        sl: float,
        tp1: float,
        pos_side: str,
    ) -> None:
        safe_qty = self._open_qty if self._open_qty > 0 else qty
        loop = asyncio.get_event_loop()

        try:
            raw_orders = await loop.run_in_executor(None, self._hl_client.get_open_orders)
        except Exception as e:
            logger.error("[%s] _ensure_tpsl: no se pudo consultar ordenes: %s", self.symbol, e)
            return

        coin_orders = [o for o in (raw_orders or []) if o.get("coin") == self.coin]
        ro_orders   = [o for o in coin_orders if _is_reduce_only_order(o)]
        has_sl = len(ro_orders) >= 1
        has_tp = len(ro_orders) >= 2

        logger.info(
            "[%s] _ensure_tpsl — SL_ok=%s TP_ok=%s | ro=%d | qty=%.4f",
            self.symbol, has_sl, has_tp, len(ro_orders), safe_qty,
        )

        if has_sl and has_tp:
            self._protection_ok = True
            return

        if has_sl and not has_tp:
            logger.warning("[%s] Falta TP — colocando TP=%.5f", self.symbol, tp1)
            try:
                await self._place_tpsl(qty=safe_qty, sl=None, tp=tp1)
                self._protection_ok = True
            except Exception as e:
                logger.error("[%s] No se pudo reponer TP: %s", self.symbol, e)
            return

        logger.warning(
            "[%s] Faltan SL y TP — recolocando bulk (SL=%.5f TP=%.5f qty=%.4f)",
            self.symbol, sl, tp1, safe_qty,
        )
        if coin_orders:
            try:
                cancelled = await self._cancel_all_orders_reduce_only(coin_orders)
                if cancelled > 0:
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning("[%s] Error cancelando previas: %s", self.symbol, e)

        try:
            await self._place_tpsl(qty=safe_qty, sl=sl, tp=tp1)
            self._protection_ok = True
        except Exception as e:
            logger.error("[%s] _ensure_tpsl: fallo al colocar SL/TP: %s", self.symbol, e)

    # ── Helpers de cierre ──────────────────────────────────────────────────────────────────────────────

    def _clear_position_state(self) -> None:
        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        self._tp1_hit = False
        self._open_qty = 0.0
        self._trailing_sl_activated = False
        self._trail_peak = 0.0
        signal_flip_guard.reset(self.symbol)

    async def _on_position_closed(self, pnl_pct: float, reason: str = "") -> None:
        if self._global_risk is not None:
            try:
                await self._global_risk.register_close(pnl_pct)
            except Exception as e:
                logger.warning("[%s] GlobalRisk.register_close error: %s", self.symbol, e)

        try:
            pretrade_risk.register_close_safe(self.symbol, self._open_margin)
        except Exception as e:
            logger.warning("[%s] PreTradeRisk.register_close error: %s", self.symbol, e)

        balance_svc.invalidate(reason=f"posicion cerrada {self.symbol} ({reason})")

        if self._decision_engine is not None:
            try:
                await self._decision_engine.on_position_closed(
                    symbol=self.symbol,
                    margin=self._open_margin,
                    reason=reason or "UNKNOWN",
                    entry_mode=self._open_entry_mode,
                )
            except Exception as e:
                logger.warning("[%s] DecisionEngine.on_position_closed error: %s", self.symbol, e)

    # ── _manage_open_position ──────────────────────────────────────────────────────────────────────

    async def _manage_open_position(self, price: float, risk) -> None:
        is_long = self.position == "long"

        # ── SL hit check ───────────────────────────────────────────────────────────────────────
        if self.sl and self.sl > 0:
            sl_triggered = (is_long and price <= self.sl) or (not is_long and price >= self.sl)
            if sl_triggered:
                logger.info("[%s] SL alcanzado (precio=%.5f SL=%.5f)", self.symbol, price, self.sl)
                if not self._protection_ok:
                    close_side = "sell" if is_long else "buy"
                    try:
                        await self._place_order(close_side, self._open_qty, reduce_only=True)
                        logger.info("[%s] Cierre emergencia reduce-only enviado.", self.symbol)
                    except Exception as e:
                        logger.error("[%s] Cierre emergencia fallo: %s", self.symbol, e)
                pnl_pct = ((price - self.entry_price) / self.entry_price) * (1 if is_long else -1)
                try:
                    await notify_close(self.symbol, self.position, self.entry_price, price,
                                       pnl_pct * 100, reason="SL")
                except Exception:
                    pass
                signal_cooldown.mark_closed(self.symbol, reason="SL", entry_mode=self._open_entry_mode)
                await self._on_position_closed(pnl_pct, reason="SL")
                self._clear_position_state()
                clear_position(self.symbol)
                return

        # ── TP1 hit check ─────────────────────────────────────────────────────────────────────
        if not self._tp1_hit and self.tp1 and self.tp1 > 0:
            tp1_triggered = (is_long and price >= self.tp1) or (not is_long and price <= self.tp1)
            if tp1_triggered:
                logger.info("[%s] TP1 alcanzado (precio=%.5f TP1=%.5f)", self.symbol, price, self.tp1)
                self._tp1_hit = True
                try:
                    await notify_tp_partial(self.symbol, self.position, self.entry_price, price, level=1)
                except Exception:
                    pass

                if not self._trailing_sl_activated:
                    self._trailing_sl_activated = True
                    self._trail_peak = price
                    logger.info(
                        "[%s] Trailing SL activado tras TP1 | peak=%.5f",
                        self.symbol, self._trail_peak,
                    )

                save_position(self.symbol, {
                    "side":  self.position, "entry": self.entry_price,
                    "sl":    self.sl,       "tp1":   self.tp1,
                    "tp2":   self.tp2,      "tp3":   self.tp3,
                    "tp1_hit": True,        "tp2_hit": self.tp2_hit,
                    "usdc_amount": self._open_notional, "leverage": self._open_leverage,
                    "margin_usdc": self._open_margin,   "qty":      self._open_qty,
                    "entry_mode": self._open_entry_mode,
                    "trailing_sl_activated": self._trailing_sl_activated,
                    "trail_peak": self._trail_peak,
                })

        # ── Trailing SL ───────────────────────────────────────────────────────────────────────────────
        if self._trailing_sl_activated and self.sl and self.sl > 0:
            new_sl, new_peak = compute_trailing_sl(
                is_long=is_long, current_price=price,
                peak_price=self._trail_peak, current_sl=self.sl,
            )

            if is_trailing_sl_hit(is_long=is_long, current_price=price, trailing_sl=new_sl):
                logger.info("[%s] Trailing SL hit | precio=%.5f sl=%.5f", self.symbol, price, new_sl)
                close_side = "sell" if is_long else "buy"
                try:
                    await self._place_order(close_side, self._open_qty, reduce_only=True)
                except Exception as e:
                    logger.error("[%s] Cierre trailing SL fallo: %s", self.symbol, e)
                pnl_pct = ((price - self.entry_price) / self.entry_price) * (1 if is_long else -1)
                try:
                    await notify_close(self.symbol, self.position, self.entry_price, price,
                                       pnl_pct * 100, reason="TRAILING_SL")
                except Exception:
                    pass
                signal_cooldown.mark_closed(self.symbol, reason="SL", entry_mode=self._open_entry_mode)
                await self._on_position_closed(pnl_pct, reason="TRAILING_SL")
                self._clear_position_state()
                clear_position(self.symbol)
                return

            sl_improved = (is_long and new_sl > self.sl) or (not is_long and new_sl < self.sl)
            if sl_improved:
                logger.info(
                    "[%s] Trailing SL: %.5f → %.5f (peak=%.5f)",
                    self.symbol, self.sl, new_sl, new_peak,
                )
                try:
                    loop = asyncio.get_event_loop()
                    raw_orders = await loop.run_in_executor(None, self._hl_client.get_open_orders)
                    coin_orders = [o for o in (raw_orders or []) if o.get("coin") == self.coin]
                    sl_orders = [
                        o for o in coin_orders
                        if _is_reduce_only_order(o) and (
                            isinstance(o.get("orderType"), dict) and
                            o["orderType"].get("trigger", {}).get("tpsl") == "sl"
                        )
                    ] or [o for o in coin_orders if _is_reduce_only_order(o)]

                    for o in sl_orders:
                        oid = o.get("oid") or o.get("id") or o.get("orderId")
                        if oid:
                            try:
                                await loop.run_in_executor(
                                    None,
                                    lambda _oid=oid: _hl_cancel_order(
                                        self._hl_client._exchange, self.coin, _oid
                                    ),
                                )
                            except Exception as ce:
                                logger.warning("[%s] Error cancelando SL oid=%s: %s", self.symbol, oid, ce)

                    await asyncio.sleep(0.3)
                    await self._place_tpsl(qty=self._open_qty, sl=new_sl, tp=None)
                    self.sl = new_sl
                    self._trail_peak = new_peak
                    save_position(self.symbol, {
                        "side":  self.position, "entry": self.entry_price,
                        "sl":    self.sl,       "tp1":   self.tp1,
                        "tp2":   self.tp2,      "tp3":   self.tp3,
                        "tp1_hit": self._tp1_hit, "tp2_hit": self.tp2_hit,
                        "usdc_amount": self._open_notional, "leverage": self._open_leverage,
                        "margin_usdc": self._open_margin,   "qty":      self._open_qty,
                        "entry_mode": self._open_entry_mode,
                        "trailing_sl_activated": self._trailing_sl_activated,
                        "trail_peak": self._trail_peak,
                    })
                except Exception as e:
                    logger.error("[%s] Error actualizando trailing SL: %s", self.symbol, e)

        # ── TP2 hit check ─────────────────────────────────────────────────────────────────────
        if self._tp1_hit and not self.tp2_hit and self.tp2 and self.tp2 > 0:
            tp2_triggered = (is_long and price >= self.tp2) or (not is_long and price <= self.tp2)
            if tp2_triggered:
                logger.info("[%s] TP2 alcanzado (precio=%.5f)", self.symbol, price)
                self.tp2_hit = True
                mark_tp2_hit(self.symbol)
                partial_qty = self._round_qty(self._open_qty * TP2_PARTIAL_RATIO)
                if partial_qty > 0:
                    close_side = "sell" if is_long else "buy"
                    try:
                        await self._place_order(close_side, partial_qty, reduce_only=True)
                    except Exception as e:
                        logger.error("[%s] Cierre parcial TP2 fallo: %s", self.symbol, e)
                try:
                    await notify_tp_partial(self.symbol, self.position, self.entry_price, price, level=2)
                except Exception:
                    pass

        # ── Verificacion periodica SL/TP ─────────────────────────────────────────────────────
        now = time.monotonic()
        if (
            not self._trailing_sl_activated
            and self.sl and self.tp1
            and now - self._last_tpsl_verify_at >= _TPSL_VERIFY_INTERVAL_S
        ):
            self._last_tpsl_verify_at = now
            try:
                await self._ensure_tpsl_on_exchange(
                    qty=self._open_qty, sl=self.sl, tp1=self.tp1, pos_side=self.position,
                )
            except Exception as e:
                logger.error("[%s] _ensure_tpsl error: %s", self.symbol, e)

    # ── _try_open_position ──────────────────────────────────────────────────────────────────────

    async def _try_open_position(self, price: float, risk, global_risk) -> None:
        # ── BLOQUEO MANUAL_CLOSE ───────────────────────────────────────────────────
        if signal_cooldown.is_manual_close_cooldown(self.symbol):
            rem = signal_cooldown.remaining(self.symbol)
            logger.debug(
                "[%s] MANUAL_CLOSE cooldown activo — bloqueando entrada (%.0fs restantes)",
                self.symbol, rem,
            )
            return

        if self._decision_engine is not None:
            try:
                signal = await self._get_signal(price)
                if signal:
                    approved, reason, enriched = await self._decision_engine.evaluate(
                        symbol=self.symbol,
                        signal=signal,
                        price=price,
                    )
                    if approved and enriched:
                        await self._execute_signal(enriched, price, risk)
                    elif not approved:
                        logger.debug("[%s] DecisionEngine rechazó señal: %s", self.symbol, reason)
            except Exception as e:
                logger.error("[%s] DecisionEngine.evaluate error: %s", self.symbol, e, exc_info=True)
        else:
            logger.debug("[%s] DecisionEngine no disponible — sin entradas automáticas.", self.symbol)

    async def _get_signal(self, price: float) -> dict | None:
        """
        Obtiene señal completa (con sl/tp1/tp2) usando strategy.decide().

        FIX 2026-06-02:
          Antes se llamaba ai_decide() directamente, que solo devuelve
          {"action","confidence","reasoning"} sin sl/tp1/tp2.
          _execute_signal recíbia sl=None, tp1=None → nunca colocaba SL/TP
          → _protection_ok=False → KillSwitch disparaba KS L3 en 30s.
          Además todo el pipeline técnico (signal_engine, enriched_filter,
          F&G, funding, OI) era ignorado completamente.

          Fix: llamar strategy.decide() con la firma correcta:
            decide(exch, symbol, ai_decide_fn, has_open_position, ohlcv_fn)
          y extraer sl/tp1/tp2/entry_mode/rr del SignalResult devuelto.
        """
        try:
            ohlcv = await self.get_ohlcv()
            if not ohlcv or len(ohlcv) < OHLCV_MIN_BARS:
                return None

            exch = await self._get_ccxt()

            result = await decide(
                exch,
                self.symbol,
                ai_decide,
                has_open_position=False,
                current_pnl=None,
                ohlcv_fn=self.get_ohlcv,
            )

            action = result.get("action", "HOLD")
            if action not in ("BUY", "SELL"):
                logger.debug(
                    "[%s] strategy.decide → %s | %s",
                    self.symbol, action, result.get("reason", ""),
                )
                return None

            sig = result.get("signal")
            if sig is None:
                logger.warning("[%s] strategy.decide devolvió action=%s pero signal=None", self.symbol, action)
                return None

            side = "long" if action == "BUY" else "short"

            logger.info(
                "[%s] ✅ Señal: %s | sl=%.5f tp1=%.5f tp2=%s | score=%s | %s",
                self.symbol, side,
                sig.sl or 0, sig.tp1 or 0,
                f"{sig.tp2:.5f}" if sig.tp2 else "N/A",
                getattr(sig, "score", "?"),
                result.get("reason", ""),
            )

            return {
                "side":       side,
                "sl":         sig.sl,
                "tp1":        sig.tp1,
                "tp2":        sig.tp2,
                "entry_mode": getattr(sig, "entry_mode", "NORMAL"),
                "rr":         getattr(sig, "rr", 2.0),
                "_leverage":  getattr(sig, "suggested_lev", self.leverage),
                "confidence": result.get("ai_confidence", getattr(sig, "score", 5)),
                "reasoning":  result.get("reason", ""),
            }

        except Exception as e:
            logger.debug("[%s] _get_signal error: %s", self.symbol, e)
            return None

    async def _execute_signal(self, enriched: dict, price: float, risk) -> None:
        """Abre posición basada en señal enriquecida por DecisionEngine."""
        side = enriched.get("side")
        margin = enriched.get("_margin", _USDC_PER_TRADE)
        leverage = enriched.get("_leverage", self.leverage)
        sl = enriched.get("sl")
        tp1 = enriched.get("tp1")
        tp2 = enriched.get("tp2")
        entry_mode = enriched.get("entry_mode", "NORMAL")

        if not side:
            return

        notional = margin * leverage
        qty = self._round_qty(notional / price)
        if qty <= 0:
            logger.warning("[%s] qty calculada = 0, skip.", self.symbol)
            return

        async with self._open_lock:
            if self.position is not None:
                return

            logger.info(
                "[%s] Abriendo %s | qty=%.4f | margin=%.2f | lev=%dx | sl=%s | tp1=%s",
                self.symbol, side, qty, margin, leverage, sl, tp1,
            )
            r = await self._place_order(side, qty, sl=sl, tp=tp1)
            if r.get("status") != "ok":
                logger.warning("[%s] Orden rechazada: %s", self.symbol, r)
                return

            positions = await self._confirm_position_with_retry()
            if not positions:
                logger.warning("[%s] Posición no confirmada tras fill.", self.symbol)
                return

            self.position       = side
            self.entry_price    = price
            self.sl             = sl
            self.tp1            = tp1
            self.tp2            = tp2
            self._open_qty      = qty
            self._open_margin   = margin
            self._open_notional = notional
            self._open_leverage = leverage
            self._open_entry_mode = entry_mode
            self._protection_ok = False

            if sl and tp1:
                try:
                    await self._place_tpsl(qty=qty, sl=sl, tp=tp1, entry_px=price)
                    self._protection_ok = True
                except Exception as e:
                    logger.error("[%s] Error colocando SL/TP: %s", self.symbol, e)

            save_position(self.symbol, {
                "side": side, "entry": price,
                "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": None,
                "tp1_hit": False, "tp2_hit": False,
                "usdc_amount": notional, "leverage": leverage,
                "margin_usdc": margin, "qty": qty,
                "entry_mode": entry_mode,
                "trailing_sl_activated": False, "trail_peak": 0.0,
            })

            try:
                await self._decision_engine.on_order_confirmed(symbol=self.symbol, margin=margin)
            except Exception as e:
                logger.warning("[%s] DecisionEngine.on_order_confirmed error: %s", self.symbol, e)

            try:
                await notify_open(self.symbol, side, price, qty, sl=sl, tp=tp1)
            except Exception:
                pass

    # ── Loop principal ────────────────────────────────────────────────────────────────────────────────────────

    async def run(self, risk, *, global_risk=None):
        self._global_risk = global_risk
        await self._init(risk.usdc_per_trade)
        async def _iteration_loop():
            while True:
                try:
                    await self._iteration(risk, global_risk)
                except asyncio.CancelledError:
                    logger.info("[%s] Trader cancelado.", self.symbol)
                    raise
                except Exception as e:
                    logger.error("[%s] Error en iteracion: %s", self.symbol, e, exc_info=True)
                await asyncio.sleep(LOOP_SLEEP)
        await _iteration_loop()

    async def _iteration(self, risk, global_risk):
        if kill_switch.is_halted(self.symbol):
            logger.debug("[%s] Kill switch activo — skip.", self.symbol)
            return

        try:
            price = await self.get_price()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
            return

        now = time.monotonic()
        did_check_exchange = False
        exchange_positions = None
        if now - self._last_pos_check_at >= _POS_CHECK_INTERVAL_S:
            self._last_pos_check_at = now
            did_check_exchange = True
            exchange_positions = await self._get_positions()

        if self.position is not None:
            if did_check_exchange and exchange_positions is not None:
                if len(exchange_positions) == 0:
                    logger.warning("[%s] Posicion local pero NO en exchange — limpiando.", self.symbol)
                    pnl_pct = 0.0
                    if self.entry_price and self.entry_price > 0:
                        pnl_pct = ((price - self.entry_price) / self.entry_price) * (
                            1 if self.position == "long" else -1
                        )
                    try:
                        await notify_close(
                            self.symbol, self.position, self.entry_price, price,
                            pnl_pct * 100, reason="CLOSED_EXTERNALLY",
                        )
                    except Exception:
                        pass
                    # FIX MANUAL_CLOSE: registrar cooldown para evitar reentrar
                    # inmediatamente tras un cierre manual o externo
                    signal_cooldown.mark_manual_close(self.symbol)
                    await self._on_position_closed(pnl_pct, reason="CLOSED_EXTERNALLY")
                    self._clear_position_state()
                    clear_position(self.symbol)
                    return

            await self._manage_open_position(price, risk)
        else:
            await self._try_open_position(price, risk, global_risk)
