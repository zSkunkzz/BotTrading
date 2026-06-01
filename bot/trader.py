"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Sizing en Hyperliquid:
  margen    = USDC_PER_TRADE          ← lo que arriesgas, siempre fijo
  notional  = USDC_PER_TRADE × lev   ← tamaño real de la posición
  qty       = notional / entry_price
  El leverage_efectivo = min(LEVERAGE_env, signal.suggested_lev, maxLeverage_del_exchange)

BUG #2 FIX: race condition en _open_lock
  - _opening_position eliminado como guard primario (era race-prone)
  - El check 'if self.position is not None' esta DENTRO del async with _open_lock
  - La verificacion en exchange tambien ocurre dentro del lock
  - asyncio.Lock es la unica fuente de verdad para evitar apertura doble

BUG #7 FIX: signal_flip_guard integrado en _try_open_position
  - Importa signal_flip_guard de bot.signal_engine
  - Si la senal invierte direccion en < SIGNAL_FLIP_COOLDOWN_S, se descarta

BUG #8 FIX: cierre de emergencia reduce-only en sl_hit cuando _protection_ok=False
  - Si el SL del exchange no fue confirmado (_protection_ok=False) y el precio
    cruza el SL local, se envia orden de mercado reduce-only antes de limpiar estado.

OHLCV FIX: decide() recibe ohlcv_fn=self.get_ohlcv para usar WS→caché→REST
  - Evita 3 REST hits extra por ciclo (antes analyze_pair llamaba exch.fetch_ohlcv directamente)
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
from bot.signal_engine import signal_flip_guard  # BUG #7 FIX

# ── DecisionEngine (opcional) ──────────────────────────────────────────────────
try:
    from bot.core.decision_engine import DecisionEngine as _DecisionEngine
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

# ── Rate limiter global para /info ───────────────────────────────────────────
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


# ── Helpers ─────────────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def _hl_side_to_str(raw_side: str) -> Optional[str]:
    """
    Convierte el campo 'side' de Hyperliquid a 'long'/'short'.
    Devuelve None si raw_side esta vacio (posicion residual / sin side todavia).
    Lanza ValueError solo si el valor es no-vacio pero desconocido.
    """
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


# ─────────────────────────────────────────────────────────────────────────────────
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

        # BUG #2 FIX: _opening_position ELIMINADO como guard primario.
        # asyncio.Lock es la unica fuente de verdad.
        # _open_lock: solo un coroutine puede estar en la fase de apertura.
        self._open_lock: asyncio.Lock = asyncio.Lock()

        # BUG #4 FIX: evento que se setea en cleanup() para que
        # pair_scanner pueda esperar a que el trader termine limpiamente.
        self._stopped_event: asyncio.Event = asyncio.Event()

        if _DE_AVAILABLE:
            self._decision_engine = _DecisionEngine(symbol)
        else:
            self._decision_engine = None

    # ── Max leverage efectivo del exchange ──────────────────────────────────────

    def _exchange_max_lev(self) -> int:
        try:
            return self._hl_client.get_max_leverage()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener maxLeverage — usando 20: %s", self.symbol, e)
            return 20

    # ── Qty rounding ────────────────────────────────────────────────────

    def _round_qty(self, qty: float) -> float:
        try:
            sz_dec = self._hl_client.get_sz_decimals()
        except Exception:
            sz_dec = 4
        factor = 10 ** sz_dec
        return math.floor(qty * factor) / factor

    # ── ccxt ──────────────────────────────────────────────────────────────────────

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

    # ── HTTP helpers ─────────────────────────────────────────────────────────

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

    # ── Init / cleanup ─────────────────────────────────────────────────────────────

    async def _init(self, usdc_per_trade: float):
        await self._get_ccxt()
        saved = load_position(self.symbol)
        if saved:
            exchange_pos = await self._get_positions()
            if exchange_pos is not None and len(exchange_pos) > 0:
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
                self._open_entry_mode = saved.get("entry_mode", "")
                self._protection_ok   = False
                self._last_tpsl_verify_at = 0.0
                logger.info("[%s] Posicion restaurada: %s @ %s — verificando SL/TP en exchange...",
                            self.symbol, self.position, self.entry_price)
            elif exchange_pos is not None and len(exchange_pos) == 0:
                logger.warning(
                    "[%s] Posicion guardada localmente pero NO existe en exchange — limpiando estado.",
                    self.symbol,
                )
                clear_position(self.symbol)
            else:
                logger.warning(
                    "[%s] No se pudo verificar posicion en exchange al arrancar — restaurando sin proteccion OK.",
                    self.symbol,
                )
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
                self._open_entry_mode = saved.get("entry_mode", "")
                self._protection_ok   = False

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
        """BUG #4 FIX: setea _stopped_event para que pair_scanner pueda esperar."""
        await self._close_ccxt()
        self._stopped_event.set()
        logger.info("[%s] Trader cleanup completado.", self.symbol)

    # ── Precio y OHLCV ─────────────────────────────────────────────────────────────

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

    # ── Leverage ───────────────────────────────────────────────────────────────────

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

    # ── Ordenes ─────────────────────────────────────────────────────────────────────────

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

    # ── Posiciones ────────────────────────────────────────────────────────────────────

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
                    logger.debug(
                        "[%s] _get_positions: szi no numerico (%r) — ignorando posicion residual.",
                        self.symbol, pos.get("szi"),
                    )
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
                    "[%s] Post-fill confirm: intento %d/%d (esperando %.1fs)...",
                    self.symbol, attempt + 1, _POST_FILL_CONFIRM_RETRIES, delay,
                )
            else:
                logger.debug(
                    "[%s] Post-fill confirm: esperando %.1fs para propagacion...",
                    self.symbol, delay,
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

    # ── _place_tpsl ────────────────────────────────────────────────────────────────────

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
                        "sz":          qty,
                        "limit_px":    sl_px,
                        "order_type":  {"trigger": {"triggerPx": sl_px, "isMarket": True,  "tpsl": "sl"}},
                        "reduce_only": True,
                    },
                    {
                        "coin":        client.coin,
                        "is_buy":      close_is_buy,
                        "sz":          qty,
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
                logger.warning(
                    "[%s] bulk SL+TP fallo (%s) — colocando individualmente",
                    self.symbol, e,
                )

            if bulk_ok:
                return

            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_sl(
                        is_buy=close_is_buy, sz=qty,
                        trigger_px=sl, entry_px=ep,
                    ),
                )
                logger.info("[%s] SL=%.5f colocado en exchange (fallback)", self.symbol, sl)
            except Exception as e:
                logger.error("[%s] No se pudo colocar SL (fallback): %s", self.symbol, e)
                raise

            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_tp(
                        is_buy=close_is_buy, sz=qty,
                        trigger_px=tp, entry_px=ep,
                    ),
                )
                logger.info("[%s] TP=%.5f colocado en exchange (fallback)", self.symbol, tp)
            except Exception as e:
                logger.error("[%s] No se pudo colocar TP (fallback): %s", self.symbol, e)
                raise

            return

        if sl:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_sl(
                        is_buy=close_is_buy, sz=qty,
                        trigger_px=sl, entry_px=ep,
                    ),
                )
                logger.info("[%s] SL=%.5f colocado en exchange", self.symbol, sl)
            except Exception as e:
                logger.error("[%s] No se pudo colocar SL: %s", self.symbol, e)
                raise

        if tp:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: client.place_tp(
                        is_buy=close_is_buy, sz=qty,
                        trigger_px=tp, entry_px=ep,
                    ),
                )
                logger.info("[%s] TP=%.5f colocado en exchange", self.symbol, tp)
            except Exception as e:
                logger.error("[%s] No se pudo colocar TP: %s", self.symbol, e)
                raise

    # ── _ensure_tpsl_on_exchange ────────────────────────────────────────────────────────────

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
            logger.error("[%s] _ensure_tpsl: no se pudo consultar ordenes abiertas: %s", self.symbol, e)
            return

        coin_orders = [o for o in (raw_orders or []) if o.get("coin") == self.coin]

        def _tpsl_type(o: dict) -> str | None:
            ot = o.get("orderType", "")
            if isinstance(ot, dict):
                tpsl = ot.get("trigger", {}).get("tpsl", "")
                if tpsl in ("sl", "tp"):
                    return tpsl
                tpsl2 = ot.get("limit", {}).get("tpsl", "")
                if tpsl2 in ("sl", "tp"):
                    return tpsl2
                return None
            if isinstance(ot, str):
                ot_l = ot.lower()
                if "stop" in ot_l or ot_l == "sl":
                    return "sl"
                if "take profit" in ot_l or "take_profit" in ot_l or ot_l == "tp":
                    return "tp"
            return None

        has_sl = any(_tpsl_type(o) == "sl" for o in coin_orders)
        has_tp = any(_tpsl_type(o) == "tp" for o in coin_orders)

        logger.info(
            "[%s] _ensure_tpsl — SL_ok=%s TP_ok=%s | ordenes_coin=%d | qty=%.4f",
            self.symbol, has_sl, has_tp, len(coin_orders), safe_qty,
        )

        if has_sl and has_tp:
            self._protection_ok = True
            return

        if has_sl and not has_tp:
            logger.warning("[%s] Falta TP en exchange — colocando TP=%.5f (SL intacto)", self.symbol, tp1)
            try:
                await self._place_tpsl(qty=safe_qty, sl=None, tp=tp1)
                self._protection_ok = True
            except Exception as e:
                logger.error("[%s] No se pudo reponer TP: %s", self.symbol, e)
            return

        if not has_sl and has_tp:
            logger.warning("[%s] Falta SL en exchange — colocando SL=%.5f (TP intacto)", self.symbol, sl)
            try:
                await self._place_tpsl(qty=safe_qty, sl=sl, tp=None)
                self._protection_ok = True
            except Exception as e:
                logger.error("[%s] No se pudo reponer SL: %s", self.symbol, e)
            return

        logger.warning(
            "[%s] Faltan SL y TP — cancelando y recolocando bulk (SL=%.5f TP=%.5f qty=%.4f)",
            self.symbol, sl, tp1, safe_qty,
        )
        if coin_orders:
            try:
                await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning("[%s] _ensure_tpsl: error cancelando ordenes previas: %s", self.symbol, e)

        try:
            await self._place_tpsl(qty=safe_qty, sl=sl, tp=tp1)
            self._protection_ok = True
        except Exception as e:
            logger.error("[%s] _ensure_tpsl: fallo al colocar SL/TP: %s", self.symbol, e)

    # ── Helpers de cierre ────────────────────────────────────────────────────────────────

    def _clear_position_state(self) -> None:
        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        self._tp1_hit = False
        self._open_qty = 0.0
        # BUG #7 FIX: resetear el flip guard al cerrar posicion
        signal_flip_guard.reset(self.symbol)

    async def _on_position_closed(self, pnl_pct: float, reason: str = "") -> None:
        if self._global_risk is not None:
            try:
                await self._global_risk.register_close(pnl_pct)
            except Exception as e:
                logger.warning("[%s] GlobalRisk.register_close error: %s", self.symbol, e)

        try:
            pretrade_risk.register_close_safe(self.symbol, self._open_margin)  # BUG #5 FIX
        except Exception as e:
            logger.warning("[%s] PreTradeRisk.register_close error: %s", self.symbol, e)

        # BUG #6 FIX: invalidar balance tras cierre para que el siguiente
        # pretrade check use el balance real post-trade, no el cacheado.
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

    # ── Loop principal ─────────────────────────────────────────────────────────────────────

    async def run(self, risk, *, global_risk=None):
        self._global_risk = global_risk
        await self._init(risk.usdc_per_trade)
        while True:
            try:
                await self._iteration(risk, global_risk)
            except asyncio.CancelledError:
                logger.info("[%s] Trader cancelado.", self.symbol)
                raise
            except Exception as e:
                logger.error("[%s] Error en iteracion: %s", self.symbol, e, exc_info=True)
            await asyncio.sleep(LOOP_SLEEP)

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
            exchange_positions      = await self._get_positions()
            self._last_pos_check_at = now
            did_check_exchange      = True

        if did_check_exchange:
            if exchange_positions is None:
                logger.warning("[%s] No se pudo verificar posicion — manteniendo estado local.", self.symbol)
            elif exchange_positions:
                ep = exchange_positions[0]
                if self.position is None:
                    raw_side = ep.get("side", "")
                    try:
                        parsed_side = _hl_side_to_str(raw_side)
                    except ValueError:
                        logger.warning("[%s] Side inesperado del exchange: %r — ignorando.", self.symbol, raw_side)
                        parsed_side = None
                    if parsed_side:
                        self.position    = parsed_side
                        self.entry_price = float(ep.get("entryPx") or 0)
                        logger.info("[%s] Posicion detectada: %s @ %s",
                                    self.symbol, self.position, self.entry_price)
                    else:
                        logger.debug("[%s] Posicion con side vacio ignorada.", self.symbol)
            else:
                if self.position is not None:
                    logger.info("[%s] Posicion cerrada externamente.", self.symbol)
                    # BUG #6 FIX: invalidar balance al detectar SL externo
                    balance_svc.invalidate_on_sl_detected(self.symbol)
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
                    except Exception as e:
                        logger.warning("[%s] No se pudieron cancelar triggers huerfanos: %s", self.symbol, e)
                    await self._on_position_closed(pnl_pct=0.0, reason="EXTERNAL")
                    self._clear_position_state()
                    clear_position(self.symbol)

        if self.position is not None:
            await self._manage_open_position(price, risk)
            return

        # BUG #2 FIX: El lock es la UNICA fuente de verdad.
        # No hay _opening_position flag. Si el lock esta ocupado,
        # otro coroutine esta en medio de una apertura -> skip.
        if self._open_lock.locked():
            logger.debug("[%s] _open_lock ocupado — apertura en curso, skip.", self.symbol)
            return

        async with self._open_lock:
            # BUG #2 FIX: re-check de position DENTRO del lock.
            # Esto elimina la race condition entre el check exterior y
            # el momento en que el lock se adquiere.
            if self.position is not None:
                await self._manage_open_position(price, risk)
                return

            live_positions = await self._get_positions()
            if live_positions is None:
                logger.warning(
                    "[%s] No se pudo verificar posicion en exchange — "
                    "abortando apertura por seguridad.",
                    self.symbol,
                )
                return
            if len(live_positions) > 0:
                ep = live_positions[0]
                raw_side = ep.get("side", "")
                try:
                    parsed_side = _hl_side_to_str(raw_side)
                except ValueError:
                    parsed_side = None
                if parsed_side:
                    self.position    = parsed_side
                    self.entry_price = float(ep.get("entryPx") or 0)
                    self._last_pos_check_at = time.monotonic()
                    logger.warning(
                        "[%s] Posicion ya abierta en exchange (%s @ %.5f) — "
                        "abortando apertura duplicada.",
                        self.symbol, parsed_side, self.entry_price,
                    )
                    await self._manage_open_position(price, risk)
                else:
                    logger.debug("[%s] Posicion con side vacio ignorada.", self.symbol)
                return

            await self._try_open_position(price, risk, global_risk)

    async def _try_open_position(self, price: float, risk, global_risk) -> None:
        """
        BUG #2 FIX: este metodo solo se llama desde dentro de async with _open_lock.
        No necesita ningun guard adicional (_opening_position eliminado).

        BUG #7 FIX: signal_flip_guard filtra inversiones de direccion rapidas.

        OHLCV FIX: pasa self.get_ohlcv como ohlcv_fn a decide() para que
        analyze_pair use la ruta WS→caché→REST en lugar de exch.fetch_ohlcv.
        """
        if global_risk:
            allowed, reason = await global_risk.can_open()
            if not allowed:
                logger.debug("[%s] GlobalRisk: %s", self.symbol, reason)
                return

        balance = await self.get_balance()
        if balance is not None and balance < risk.usdc_per_trade:
            logger.warning("[%s] Balance insuficiente (%.2f < %.2f USDC).",
                           self.symbol, balance, risk.usdc_per_trade)
            return

        usdc_per_trade = risk.usdc_per_trade
        try:
            ok, pt_reason = await pretrade_risk.check(
                symbol=self.symbol,
                side="buy",
                margin=usdc_per_trade,
            )
            if not ok:
                logger.debug("[%s] pretrade_risk bloqueo la entrada: %s", self.symbol, pt_reason)
                return
        except Exception as e:
            logger.warning("[%s] pretrade_risk.check error: %s", self.symbol, e)

        try:
            exch = await self._get_ccxt()
            decision = await decide(
                exch=exch,
                symbol=self.symbol,
                ai_decide_fn=ai_decide,
                has_open_position=False,
                current_pnl=None,
                ohlcv_fn=self.get_ohlcv,  # OHLCV FIX: usa WS→caché→REST del trader
            )
        except Exception as e:
            logger.error("[%s] decide() error: %s", self.symbol, e)
            return

        action = decision.get("action", "HOLD")
        signal = decision.get("signal")
        if action not in ("BUY", "SELL"):
            return

        # BUG #7 FIX: verificar flip-flop antes de procesar la senal
        if not signal_flip_guard.allow(self.symbol, signal):
            return  # senal bloqueada por cooldown anti flip-flop

        if signal:
            entry      = signal.entry or price
            sl         = _nonzero(signal.sl)
            tp1        = _nonzero(signal.tp1)
            tp2        = _nonzero(signal.tp2)
            tp3        = _nonzero(getattr(signal, "tp3", None))
            lev        = int(signal.suggested_lev or self.leverage)
            atr        = getattr(signal, "atr", None) or (entry * 0.005)
            entry_mode = getattr(signal, "entry_mode", "") or ""
        else:
            entry = price
            sl = tp1 = tp2 = tp3 = None
            lev = self.leverage
            atr = entry * 0.005
            entry_mode = ""

        exchange_max_lev = self._exchange_max_lev()
        lev = min(lev, self.leverage, exchange_max_lev)
        if lev < 1:
            lev = 1

        if not self.dry_run:
            try:
                await self._set_leverage(lev)
            except Exception as e:
                logger.error(
                    "[%s] No se pudo establecer leverage %dx — abortando entrada: %s",
                    self.symbol, lev, e,
                )
                return

        margin_usdc     = usdc_per_trade
        notional_target = margin_usdc * lev
        qty             = self._round_qty(notional_target / entry)
        notional        = qty * entry

        logger.info(
            "[%s] Sizing | margen=%.2f USDC | lev=%dx | notional=%.2f USDC | entry=%.4f | qty=%s",
            self.symbol, margin_usdc, lev, notional, entry, qty,
        )

        if qty <= 0:
            logger.warning("[%s] qty <= 0 tras redondeo, skip.", self.symbol)
            return

        pos_side       = "long" if action == "BUY" else "short"
        trade_side_str = "buy"  if action == "BUY" else "sell"

        if sl is None or tp1 is None:
            fb_sl, fb_tp1, fb_tp2 = _compute_fallback_tpsl(pos_side, entry, float(atr))
            if sl is None:
                sl = fb_sl
                logger.warning("[%s] signal.sl vacio — fallback ATR: SL=%.5f", self.symbol, sl)
            if tp1 is None:
                tp1 = fb_tp1
                logger.warning("[%s] signal.tp1 vacio — fallback ATR: TP1=%.5f", self.symbol, tp1)
            if tp2 is None:
                tp2 = fb_tp2

        assert sl  and sl  > 0, f"[{self.symbol}] SL invalido: {sl}"
        assert tp1 and tp1 > 0, f"[{self.symbol}] TP1 invalido: {tp1}"

        # BUG #2 FIX: NO se usa _opening_position flag aqui.
        # El lock ya garantiza exclusion mutua. El try/finally es solo
        # para cleanup en caso de excepcion, no como guard de concurrencia.
        try:
            order_result = await self._place_order(trade_side_str, qty, sl=sl, tp=tp1)
            if order_result.get("status") != "ok":
                logger.error("[%s] Orden rechazada: %s", self.symbol, order_result)
                return

            positions = await self._confirm_position_with_retry()
            if positions is None or len(positions) == 0:
                logger.warning("[%s] Orden ejecutada pero posicion no confirmada.", self.symbol)
                return

            pos_data   = positions[0]
            fill_entry = float(pos_data.get("entryPx") or entry)
            confirmed_qty = qty

            if fill_entry != entry and entry > 0:
                fill_ratio = fill_entry / entry
                sl_recalc  = round(sl  * fill_ratio, 6)
                tp1_recalc = round(tp1 * fill_ratio, 6)
                tp2_recalc = round(tp2 * fill_ratio, 6) if tp2 else tp2
                tp3_recalc = round(tp3 * fill_ratio, 6) if tp3 else tp3
                logger.info(
                    "[%s] Fill slippage: signal_entry=%.5f fill=%.5f (ratio=%.6f) "
                    "SL: %.5f->%.5f | TP1: %.5f->%.5f",
                    self.symbol, entry, fill_entry, fill_ratio,
                    sl, sl_recalc, tp1, tp1_recalc,
                )
                sl  = sl_recalc
                tp1 = tp1_recalc
                tp2 = tp2_recalc
                tp3 = tp3_recalc

            self.position         = pos_side
            self.entry_price      = fill_entry
            self.sl               = sl
            self.tp1              = tp1
            self.tp2              = tp2
            self.tp3              = tp3
            self.tp2_hit          = False
            self._tp1_hit         = False
            self._open_margin     = margin_usdc
            self._open_notional   = confirmed_qty * fill_entry
            self._open_leverage   = lev
            self._open_qty        = confirmed_qty
            self._open_entry_mode = entry_mode
            self._last_pos_check_at = time.monotonic()

            save_position(self.symbol, {
                "side":        pos_side,
                "entry":       fill_entry,
                "sl":          sl,
                "tp1":         tp1,
                "tp2":         tp2,
                "tp3":         tp3,
                "tp2_hit":     False,
                "tp1_hit":     False,
                "leverage":    lev,
                "usdc_amount": self._open_notional,
                "margin_usdc": margin_usdc,
                "qty":         confirmed_qty,
                "entry_mode":  entry_mode,
            })

            pretrade_risk.confirm_order(self.symbol, margin_usdc)

            try:
                await self._place_tpsl(qty=confirmed_qty, sl=sl, tp=tp1, entry_px=fill_entry)
                self._protection_ok = True
            except Exception as e:
                logger.error(
                    "[%s] _place_tpsl fallo al abrir — _ensure_tpsl lo repondra en <120s: %s",
                    self.symbol, e,
                )
                self._protection_ok = False

            await notify_open(
                symbol=self.symbol,
                side=pos_side,
                price=fill_entry,
                sl=sl,
                tp1=tp1,
                tp2=tp2,
                usdt=margin_usdc,
                leverage=lev,
            )

            logger.info(
                "[%s] Posicion abierta | %s @ %.5f | margen=%.2f USDC | "
                "notional=%.2f USDC | qty=%s | lev=%dx | SL=%.5f | TP1=%.5f",
                self.symbol, pos_side, fill_entry, margin_usdc,
                self._open_notional, confirmed_qty, lev, sl, tp1,
            )

        except Exception as e:
            logger.error("[%s] _try_open_position excepcion: %s", self.symbol, e, exc_info=True)
            # BUG #5 FIX: liberar margen si la apertura fallo a mitad
            pretrade_risk.register_close_safe(self.symbol)
            raise

    async def _manage_open_position(self, price: float, risk) -> None:
        if not self.position or not self.entry_price:
            return

        is_long  = self.position == "long"
        entry    = self.entry_price
        sl       = self.sl
        tp1      = self.tp1
        tp2      = self.tp2
        tp3      = self.tp3

        now = time.monotonic()
        if (
            not self._protection_ok
            or now - self._last_tpsl_verify_at >= _TPSL_VERIFY_INTERVAL_S
        ):
            self._last_tpsl_verify_at = now
            if sl and tp1 and self._open_qty > 0:
                await self._ensure_tpsl_on_exchange(
                    qty=self._open_qty, sl=sl, tp1=tp1, pos_side=self.position
                )
            elif sl and tp1:
                positions = await self._get_positions()
                if positions:
                    pos_data = positions[0]
                    exchange_qty = abs(float(pos_data.get("szi", 0)))
                    if exchange_qty > 0:
                        self._open_qty = exchange_qty
                        await self._ensure_tpsl_on_exchange(
                            qty=exchange_qty, sl=sl, tp1=tp1, pos_side=self.position
                        )

        if sl:
            sl_hit = (is_long and price <= sl) or (not is_long and price >= sl)
            if sl_hit:
                logger.info("[%s] SL alcanzado @ %.5f", self.symbol, price)
                # BUG #8 FIX: si el SL del exchange no fue confirmado y hay qty abierta,
                # enviar cierre de emergencia reduce-only antes de limpiar estado.
                if not self._protection_ok and self._open_qty > 0:
                    logger.warning(
                        "[%s] SL local sin _protection_ok — enviando cierre de emergencia reduce-only",
                        self.symbol,
                    )
                    close_side = "sell" if is_long else "buy"
                    try:
                        await self._place_order(close_side, self._open_qty, reduce_only=True)
                    except Exception as e:
                        logger.error("[%s] Cierre de emergencia SL fallido: %s", self.symbol, e)
                pnl_pct = ((price - entry) / entry * 100) if is_long else ((entry - price) / entry * 100)
                self.trade_count += 1
                self.total_pnl   += pnl_pct
                await self._on_position_closed(pnl_pct=pnl_pct, reason="SL")
                self._clear_position_state()
                clear_position(self.symbol)
                await notify_close(self.symbol, "SL", price, pnl_pct)
                return

        if tp1 and not self._tp1_hit:
            tp1_hit = (is_long and price >= tp1) or (not is_long and price <= tp1)
            if tp1_hit:
                logger.info("[%s] TP1 alcanzado @ %.5f — cierre parcial", self.symbol, price)
                positions = await self._get_positions()
                remaining_qty = 0.0
                if positions:
                    pos_data    = positions[0]
                    current_qty = abs(float(pos_data.get("szi", 0)))
                    partial_qty = self._round_qty(current_qty * TP2_PARTIAL_RATIO)
                    if partial_qty > 0:
                        close_side = "sell" if is_long else "buy"
                        await self._place_order(close_side, partial_qty, reduce_only=True)
                    remaining_qty = max(0.0, current_qty - partial_qty)

                self._tp1_hit = True
                self.tp1      = None
                self.sl = entry * (1 + _SL_SW_MARGIN) if is_long else entry * (1 - _SL_SW_MARGIN)
                if remaining_qty > 0:
                    self._open_qty = remaining_qty

                save_position(self.symbol, {
                    "side":        self.position,
                    "entry":       entry,
                    "sl":          self.sl,
                    "tp1":         None,
                    "tp2":         tp2,
                    "tp3":         tp3,
                    "tp2_hit":     False,
                    "tp1_hit":     True,
                    "leverage":    self._open_leverage,
                    "usdc_amount": self._open_notional,
                    "margin_usdc": self._open_margin,
                    "qty":         self._open_qty,
                    "entry_mode":  self._open_entry_mode,
                })
                self._protection_ok = False
                await notify_tp_partial(self.symbol, self.position, price, tp_level=1)
                return

        if tp2 and not self.tp2_hit:
            tp2_hit = (is_long and price >= tp2) or (not is_long and price <= tp2)
            if tp2_hit:
                logger.info("[%s] TP2 alcanzado @ %.5f — cierre parcial", self.symbol, price)
                positions = await self._get_positions()
                if positions:
                    pos_data    = positions[0]
                    current_qty = abs(float(pos_data.get("szi", 0)))
                    partial_qty = self._round_qty(current_qty * TP2_PARTIAL_RATIO)
                    if partial_qty > 0:
                        close_side = "sell" if is_long else "buy"
                        await self._place_order(close_side, partial_qty, reduce_only=True)
                mark_tp2_hit(self.symbol)
                self.tp2_hit = True
                save_position(self.symbol, {
                    "side":        self.position,
                    "entry":       entry,
                    "sl":          self.sl,
                    "tp1":         self.tp1,
                    "tp2":         tp2,
                    "tp3":         tp3,
                    "tp2_hit":     True,
                    "tp1_hit":     self._tp1_hit,
                    "leverage":    self._open_leverage,
                    "usdc_amount": self._open_notional,
                    "margin_usdc": self._open_margin,
                    "qty":         self._open_qty,
                    "entry_mode":  self._open_entry_mode,
                })
                await notify_tp_partial(self.symbol, self.position, price, tp_level=2)
                return

        if tp3:
            tp3_hit = (is_long and price >= tp3) or (not is_long and price <= tp3)
            if tp3_hit:
                logger.info("[%s] TP3 alcanzado @ %.5f — cierre total", self.symbol, price)
                pnl_pct = ((price - entry) / entry * 100) if is_long else ((entry - price) / entry * 100)
                self.trade_count += 1
                self.win_count   += 1
                self.total_pnl   += pnl_pct
                positions = await self._get_positions()
                if positions:
                    pos_data    = positions[0]
                    current_qty = abs(float(pos_data.get("szi", 0)))
                    if current_qty > 0:
                        close_side = "sell" if is_long else "buy"
                        await self._place_order(close_side, current_qty, reduce_only=True)
                await self._on_position_closed(pnl_pct=pnl_pct, reason="TP3")
                self._clear_position_state()
                clear_position(self.symbol)
                await notify_close(self.symbol, "TP3", price, pnl_pct)
                return
