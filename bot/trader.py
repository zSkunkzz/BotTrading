"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Cómo funciona el sizing en Hyperliquid (DIFERENTE a un CEX):
  - En HL mandas directamente el tamaño en moneda base (ej. 0.003 BTC)
  - USDC_PER_TRADE = margen real que quieres arriesgar (colateral)
  - El leverage define el notional real: notional = USDC_PER_TRADE × leverage
  - qty = (USDC_PER_TRADE × leverage) / entry_price
  - Margen reservado en HL = notional / leverage = USDC_PER_TRADE  ✓

Autenticación soportada:
  Opción A (recomendada): API Key de agente
    - HL_API_PRIVATE_KEY     : private key del wallet AGENTE generado en app.hyperliquid.xyz
    - HL_API_WALLET_ADDRESS  : dirección del wallet PRINCIPAL (el que tiene fondos y aprobó el agente)

  Opción B: Private key directa
    - HL_PRIVATE_KEY         : private key del wallet principal
    - HL_ACCOUNT_ADDR        : dirección pública (opcional, se deriva automáticamente)

Opcionales:
  HL_TESTNET        — "true" para usar testnet de Hyperliquid
  LOOP_SLEEP        — segundos entre iteraciones del loop (default 10)
  OHLCV_TF          — timeframe OHLCV (default 15m)
  OHLCV_LIMIT       — número de velas a cargar (default 200)
  OHLCV_MIN_BARS    — mínimo de velas requeridas (default 55)
  TP2_PARTIAL_RATIO — ratio de cierre parcial en TP2 (default 0.5)
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

# Multiplicadores ATR para fallback SL/TP cuando signal los trae vacíos
_FALLBACK_ATR_SL_MULT  = float(os.getenv("FALLBACK_ATR_SL_MULT",  "1.8"))
_FALLBACK_ATR_TP1_MULT = float(os.getenv("FALLBACK_ATR_TP1_MULT", "3.5"))
_FALLBACK_ATR_TP2_MULT = float(os.getenv("FALLBACK_ATR_TP2_MULT", "5.0"))

# ── Rate limiter global para /info ─────────────────────────────────────────────
_HL_REST_LOCK    = asyncio.Lock()
_HL_LAST_CALL    = 0.0
_HL_MIN_INTERVAL = 0.6

async def _hl_throttle():
    global _HL_LAST_CALL
    async with _HL_REST_LOCK:
        now = time.monotonic()
        wait = _HL_MIN_INTERVAL - (now - _HL_LAST_CALL)
        if wait > 0:
            await asyncio.sleep(wait)
        _HL_LAST_CALL = time.monotonic()


# ── Helpers ───────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def _hl_side_to_str(raw_side: str) -> str:
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
    """Devuelve float si v es un número > 0, None en caso contrario."""
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
    is_long = side == "long"
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
                raise ValueError(
                    f"[{symbol}] HL_API_WALLET_ADDRESS es OBLIGATORIA en modo agente."
                )
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
                raise ValueError(
                    f"[{symbol}] No hay ninguna clave configurada."
                )
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
        self.trade_count    = 0
        self.win_count      = 0
        self.total_pnl      = 0.0
        self._open_notional = 0.0
        self._open_leverage = 1
        self._protection_ok = False
        self._last_pos_check_at:   float = 0.0
        self._last_tpsl_verify_at: float = 0.0
        self._ccxt_exchange = None
        self._global_risk = None

    # ── Qty rounding respetando szDecimals ────────────────────────────

    def _round_qty(self, qty: float) -> float:
        try:
            sz_dec = self._hl_client.get_sz_decimals()
        except Exception:
            sz_dec = 4
        factor = 10 ** sz_dec
        return math.floor(qty * factor) / factor

    # ── ccxt ──────────────────────────────────────────────────────────

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

    # ── HTTP helpers ──────────────────────────────────────────────────

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

    # ── Init / cleanup ────────────────────────────────────────────────

    async def _init(self, usdc_per_trade: float):
        await self._get_ccxt()
        saved = load_position(self.symbol)
        if saved:
            exchange_pos = await self._get_positions()
            if exchange_pos is not None and len(exchange_pos) > 0:
                self.position       = saved["side"]
                self.entry_price    = saved["entry"]
                self.sl             = saved.get("sl")
                self.tp1            = saved.get("tp1")
                self.tp2            = saved.get("tp2")
                self.tp3            = saved.get("tp3")
                self.tp2_hit        = saved.get("tp2_hit", False)
                self._open_notional = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
                self._open_leverage = saved.get("leverage", self.leverage)
                self._protection_ok = True
                logger.info("[%s] Posicion restaurada: %s @ %s", self.symbol, self.position, self.entry_price)
            elif exchange_pos is not None and len(exchange_pos) == 0:
                logger.warning(
                    "[%s] Posición guardada localmente pero NO existe en exchange — limpiando estado.",
                    self.symbol,
                )
                clear_position(self.symbol)
            else:
                logger.warning(
                    "[%s] No se pudo verificar posición en exchange al arrancar — restaurando sin protección OK.",
                    self.symbol,
                )
                self.position       = saved["side"]
                self.entry_price    = saved["entry"]
                self.sl             = saved.get("sl")
                self.tp1            = saved.get("tp1")
                self.tp2            = saved.get("tp2")
                self.tp3            = saved.get("tp3")
                self.tp2_hit        = saved.get("tp2_hit", False)
                self._open_notional = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
                self._open_leverage = saved.get("leverage", self.leverage)
                self._protection_ok = False

        if not balance_svc.is_ready():
            balance_svc.init_hl(self._master_addr, self._info_post)

        try:
            await asyncio.wait_for(
                self._set_leverage(self.leverage),
                timeout=_SET_LEVERAGE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("[%s] _set_leverage timeout — continuando", self.symbol)
        except Exception as e:
            logger.warning("[%s] _set_leverage error (no crítico): %s", self.symbol, e)

        logger.info(
            "[%s] Trader iniciado | coin=%s | master=%s | agent_mode=%s",
            self.symbol, self.coin,
            self._master_addr[:10] + "..." if self._master_addr else "N/A",
            self._agent_mode,
        )

    async def cleanup(self):
        await self._close_ccxt()

    # ── Precio y OHLCV ────────────────────────────────────────────────

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

    # ── Leverage ──────────────────────────────────────────────────────

    async def _set_leverage(self, leverage: int):
        if self.dry_run:
            return
        is_cross = self.margin_mode != "isolated"
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self._hl_client._exchange.update_leverage(
                int(leverage), self.coin, is_cross
            ),
        )
        if isinstance(result, dict) and result.get("status") == "ok":
            logger.debug("[%s] Leverage %sx OK", self.symbol, leverage)

    # ── Órdenes ───────────────────────────────────────────────────────

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

    # ── Posiciones ────────────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        try:
            data = await self._info_post({"type": "clearinghouseState", "user": self._account_addr})
            if not data or not isinstance(data, dict):
                return None
            positions = []
            for p in data.get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") == self.coin:
                    szi = float(pos.get("szi", 0))
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
                    "[%s] Post-fill confirm: esperando %.1fs para propagación...",
                    self.symbol, delay,
                )
            await asyncio.sleep(delay)
            positions = await self._get_positions()
            if positions is None:
                return None
            if len(positions) > 0:
                return positions

        logger.warning(
            "[%s] Post-fill confirm: posición no visible tras %d intentos.",
            self.symbol, _POST_FILL_CONFIRM_RETRIES,
        )
        return []

    # ── Recolocar SL/TP si el bulk falló ──────────────────────────────

    async def _ensure_tpsl_on_exchange(
        self,
        qty: float,
        sl: float,
        tp1: float,
        pos_side: str,
    ) -> None:
        MAX_TRIES = 3
        for attempt in range(1, MAX_TRIES + 1):
            try:
                loop = asyncio.get_event_loop()
                open_triggers = await loop.run_in_executor(
                    None, self._hl_client.get_open_orders
                )
                has_sl = any(
                    o.get("orderType", "").lower() == "stop market"
                    or (o.get("tpsl", "") == "sl")
                    for o in (open_triggers or [])
                )
                has_tp = any(
                    o.get("orderType", "").lower() in ("take profit market", "take profit limit")
                    or (o.get("tpsl", "") == "tp")
                    for o in (open_triggers or [])
                )

                if has_sl and has_tp:
                    logger.info("[%s] ✅ SL y TP confirmados en exchange.", self.symbol)
                    return

                logger.warning(
                    "[%s] ⚠️ Intento %d/%d — SL=%s TP=%s NO confirmados. Recolocando...",
                    self.symbol, attempt, MAX_TRIES, has_sl, has_tp,
                )

                is_long      = pos_side == "long"
                close_is_buy = not is_long
                client       = self._hl_client
                orders       = []

                if not has_tp:
                    tp_px  = client.round_px(tp1)
                    buf    = 0.005
                    tp_lim = client.round_px(tp_px * (1 - buf) if is_long else tp_px * (1 + buf))
                    orders.append({
                        "coin": client.coin, "is_buy": close_is_buy, "sz": qty,
                        "limit_px": tp_lim,
                        "order_type": {"trigger": {"triggerPx": tp_px, "isMarket": False, "tpsl": "tp"}},
                        "reduce_only": True,
                    })

                if not has_sl:
                    sl_px = client.round_px(sl)
                    orders.append({
                        "coin": client.coin, "is_buy": close_is_buy, "sz": qty,
                        "limit_px": sl_px,
                        "order_type": {"trigger": {"triggerPx": sl_px, "isMarket": True, "tpsl": "sl"}},
                        "reduce_only": True,
                    })

                if orders:
                    await loop.run_in_executor(None, lambda: client.place_bulk(orders))
                    await asyncio.sleep(2.0)

            except Exception as e:
                logger.warning(
                    "[%s] _ensure_tpsl_on_exchange intento %d error: %s",
                    self.symbol, attempt, e,
                )
                await asyncio.sleep(2.0)

        logger.error(
            "[%s] ❌ No se pudieron confirmar SL/TP en exchange tras %d intentos. "
            "POSICIÓN SIN PROTECCIÓN — revisar manualmente.",
            self.symbol, MAX_TRIES,
        )

    # ── Helpers de cierre de posición ─────────────────────────────────

    def _clear_position_state(self) -> None:
        """Limpia el estado interno de posición abierta."""
        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False

    async def _on_position_closed(self, pnl_pct: float) -> None:
        if self._global_risk is not None:
            try:
                await self._global_risk.register_close(pnl_pct)
                logger.debug(
                    "[%s] GlobalRisk.register_close(%.2f%%) — OK",
                    self.symbol, pnl_pct,
                )
            except Exception as e:
                logger.warning("[%s] GlobalRisk.register_close error: %s", self.symbol, e)

        try:
            pretrade_risk.register_close(self.symbol, self._open_notional)
        except Exception as e:
            logger.warning("[%s] PreTradeRisk.register_close error: %s", self.symbol, e)

    # ── Loop principal ────────────────────────────────────────────────

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
                logger.error("[%s] Error en iteración: %s", self.symbol, e, exc_info=True)
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
                logger.warning("[%s] No se pudo verificar posición — manteniendo estado local.", self.symbol)
            elif exchange_positions:
                ep = exchange_positions[0]
                if self.position is None:
                    raw_side = ep.get("side", "")
                    try:
                        parsed_side = _hl_side_to_str(raw_side)
                    except ValueError:
                        logger.warning("[%s] Side desconocido: %r", self.symbol, raw_side)
                        parsed_side = None
                    if parsed_side:
                        self.position    = parsed_side
                        self.entry_price = float(ep.get("entryPx") or 0)
                        logger.info("[%s] Posición detectada: %s @ %s",
                                    self.symbol, self.position, self.entry_price)
            else:
                if self.position is not None:
                    logger.info("[%s] Posición cerrada externamente.", self.symbol)
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
                    except Exception as e:
                        logger.warning("[%s] No se pudieron cancelar triggers huérfanos: %s", self.symbol, e)
                    await self._on_position_closed(pnl_pct=0.0)
                    self._clear_position_state()
                    clear_position(self.symbol)

        if self.position is not None:
            await self._manage_open_position(price, risk)
            return

        # ── Pre-checks para abrir nueva posición ──────────────────────
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

        try:
            ok, pt_reason = await pretrade_risk.check(
                symbol=self.symbol,
                side="buy",
                notional=risk.usdc_per_trade,
                price=price,
                balance=balance,
                sl=None,
                ask=None,
                bid=None,
            )
            if not ok:
                logger.debug("[%s] pretrade_risk bloqueó la entrada: %s", self.symbol, pt_reason)
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
            )
        except Exception as e:
            logger.error("[%s] decide() error: %s", self.symbol, e)
            return

        action = decision.get("action", "HOLD")
        signal = decision.get("signal")
        if action not in ("BUY", "SELL"):
            return

        # ── Extraer parámetros de la señal ────────────────────────────
        if signal:
            entry = signal.entry or price
            sl    = _nonzero(signal.sl)
            tp1   = _nonzero(signal.tp1)
            tp2   = _nonzero(signal.tp2)
            tp3   = _nonzero(getattr(signal, "tp3", None))
            lev   = int(signal.suggested_lev or self.leverage)
            atr   = getattr(signal, "atr", None) or (entry * 0.005)
        else:
            entry = price
            sl = tp1 = tp2 = tp3 = None
            lev = self.leverage
            atr = entry * 0.005

        # Nunca superar el leverage configurado por el usuario
        lev = min(lev, self.leverage)

        # ── FIX #1: aplicar leverage SIEMPRE antes de abrir posición ──
        # (antes solo se aplicaba si lev != self.leverage, dejando casos
        #  en que HL tenía el leverage por defecto y no el configurado)
        if not self.dry_run:
            try:
                await asyncio.wait_for(
                    self._set_leverage(lev),
                    timeout=_SET_LEVERAGE_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning("[%s] _set_leverage timeout pre-orden — continuando", self.symbol)
            except Exception as e:
                logger.warning("[%s] _set_leverage pre-orden error: %s", self.symbol, e)

        # ── FIX #2: SIZING CORRECTO — notional incluye el leverage ────
        # En HL el margen reservado = notional / leverage.
        # Para arriesgar exactamente USDC_PER_TRADE de margen:
        #   notional  = usdc_per_trade × leverage
        #   qty       = notional / entry_price
        # Ejemplo: 20 USDC × 5× lev = 100 USDC notional → ~3.33 ZEC a $30
        usdc_per_trade = risk.usdc_per_trade
        notional_target = usdc_per_trade * lev
        qty     = self._round_qty(notional_target / entry)
        notional = qty * entry

        logger.info(
            "[%s] 📐 Sizing HL | margen=%.2f USDC | lev=%dx | "
            "notional=%.2f USDC | entry=%.4f | qty=%s",
            self.symbol, usdc_per_trade, lev, notional, entry, qty,
        )

        if qty <= 0:
            logger.warning("[%s] qty <= 0 tras redondeo, skip.", self.symbol)
            return

        pos_side = "long" if action == "BUY" else "short"
        trade_side_str = "buy" if action == "BUY" else "sell"

        # ── GARANTIZAR SL y TP — SIEMPRE ──────────────────────────────
        if sl is None or tp1 is None:
            fb_sl, fb_tp1, fb_tp2 = _compute_fallback_tpsl(pos_side, entry, float(atr))
            if sl is None:
                sl = fb_sl
                logger.warning("[%s] ⚠️ signal.sl vacío — fallback ATR: SL=%.5f", self.symbol, sl)
            if tp1 is None:
                tp1 = fb_tp1
                logger.warning("[%s] ⚠️ signal.tp1 vacío — fallback ATR: TP1=%.5f", self.symbol, tp1)
            if tp2 is None:
                tp2 = fb_tp2

        assert sl  and sl  > 0, f"[{self.symbol}] SL inválido: {sl}"
        assert tp1 and tp1 > 0, f"[{self.symbol}] TP1 inválido: {tp1}"

        # ── Check pretrade final con side y sl conocidos ──────────────
        ask = bid = None
        try:
            from bot.ws_feed import ws_feed
            ob = ws_feed.get_orderbook_metrics(self.coin)
            if ob:
                ask = ob.get("ask")
                bid = ob.get("bid")
        except Exception:
            pass

        try:
            ok, pt_reason = await pretrade_risk.check(
                symbol=self.symbol,
                side=trade_side_str,
                notional=notional,
                price=entry,
                balance=balance,
                sl=sl,
                ask=ask,
                bid=bid,
            )
            if not ok:
                logger.info("[%s] pretrade_risk bloqueó tras señal: %s", self.symbol, pt_reason)
                return
        except Exception as e:
            logger.warning("[%s] pretrade_risk.check (post-señal) error: %s", self.symbol, e)

        logger.info(
            "[%s] 📈 Abriendo %s · qty=%s · entry=~%.4f · sl=%.4f · tp1=%.4f | %s",
            self.symbol, action, qty, entry, sl, tp1,
            decision.get("reason", ""),
        )

        if self.dry_run:
            result = {"status": "ok", "_fill_price": entry}
            fill_price = entry
            confirmed_positions = [{"szi": qty}]
        else:
            result = await self._place_order(trade_side_str, qty, sl=sl, tp=tp1)
            if result.get("status") != "ok":
                logger.error("[%s] ❌ Orden rechazada: %s", self.symbol, result)
                return

            fill_price = entry
            try:
                fill_price = float(
                    result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                    .get("filled", {}).get("avgPx") or entry
                )
            except Exception:
                fill_price = entry

            confirmed_positions = await self._confirm_position_with_retry()
            if confirmed_positions is not None and len(confirmed_positions) == 0:
                logger.error(
                    "[%s] ❌ status=ok pero posición NO visible tras %d intentos.",
                    self.symbol, _POST_FILL_CONFIRM_RETRIES,
                )
                return
            if confirmed_positions is None:
                logger.warning(
                    "[%s] ⚠️ No se pudo confirmar posición (red) — registrando sin protección OK.",
                    self.symbol,
                )

        # Ajustar SL/TP al fill real (slippage)
        if fill_price and fill_price != entry:
            delta = fill_price - entry
            sl  = round(sl  + delta, 6)
            tp1 = round(tp1 + delta, 6)
            if tp2:
                tp2 = round(tp2 + delta, 6)
            if tp3:
                tp3 = round(tp3 + delta, 6)
            notional = qty * fill_price

        self.position       = pos_side
        self.entry_price    = fill_price
        self.sl             = sl
        self.tp1            = tp1
        self.tp2            = tp2
        self.tp3            = tp3
        self.tp2_hit        = False
        self._open_notional = notional
        self._open_leverage = lev
        self._protection_ok = (
            confirmed_positions is not None and len(confirmed_positions) > 0
        ) if not self.dry_run else True
        self.trade_count   += 1

        save_position(self.symbol, {
            "side":        self.position,
            "entry":       self.entry_price,
            "sl":          self.sl,
            "tp1":         self.tp1,
            "tp2":         self.tp2,
            "tp3":         self.tp3,
            "tp2_hit":     self.tp2_hit,
            "usdc_amount": notional,
            "leverage":    lev,
        })

        if global_risk:
            await global_risk.register_open()

        pretrade_risk.confirm_order(self.symbol)

        await notify_open(
            symbol=self.symbol,
            side=self.position,
            price=self.entry_price,
            leverage=lev,
            usdt=notional,
            sl=self.sl,
            tp1=self.tp1,
            tp2=self.tp2,
        )

        if not self.dry_run and confirmed_positions:
            live_qty = abs(float(confirmed_positions[0].get("szi", qty)))
            real_qty = live_qty if live_qty > 0 else qty
            asyncio.ensure_future(
                self._ensure_tpsl_on_exchange(real_qty, sl, tp1, self.position)
            )

    async def _manage_open_position(self, price: float, risk):
        if self.position is None or self.entry_price is None:
            return

        is_long = self.position == "long"

        # ── TP2 parcial ───────────────────────────────────────────────
        if self.tp2 and not self.tp2_hit:
            tp2_triggered = (is_long and price >= self.tp2) or (not is_long and price <= self.tp2)
            if tp2_triggered:
                self.tp2_hit = True
                mark_tp2_hit(self.symbol)
                partial_qty = self._round_qty(
                    (self._open_notional / self.entry_price) * TP2_PARTIAL_RATIO
                )
                if partial_qty > 0 and not self.dry_run:
                    close_side = "sell" if is_long else "buy"
                    try:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
                    except Exception as e:
                        logger.warning("[%s] TP2: no se pudieron cancelar triggers: %s", self.symbol, e)

                    r = await self._place_order(close_side, partial_qty, reduce_only=True)
                    if r.get("status") == "ok":
                        logger.info("[%s] TP2 parcial ejecutado (%.1f%%)", self.symbol, TP2_PARTIAL_RATIO * 100)
                        await notify_tp_partial(
                            symbol=self.symbol,
                            side=self.position,
                            price=price,
                            tp_level=2,
                            ratio=TP2_PARTIAL_RATIO,
                        )
                        remaining_notional = self._open_notional * (1 - TP2_PARTIAL_RATIO)
                        self._open_notional = remaining_notional
                        remaining_qty = self._round_qty(remaining_notional / self.entry_price)
                        if remaining_qty > 0 and (self.tp3 or self.sl):
                            try:
                                await self._place_tpsl(remaining_qty, self.sl, self.tp3)
                            except Exception as e:
                                logger.warning("[%s] No se pudo re-colocar TP/SL tras parcial: %s", self.symbol, e)

        # ── Trailing stop ─────────────────────────────────────────────
        if risk.trailing_sl and self.sl is not None:
            activation_px = self.entry_price * (
                1 + risk.trailing_activation_pct / 100 if is_long
                else 1 - risk.trailing_activation_pct / 100
            )
            activated = (is_long and price >= activation_px) or (not is_long and price <= activation_px)
            if activated:
                callback = risk.trailing_callback_pct / 100
                new_sl = price * (1 - callback if is_long else 1 + callback)
                sl_moved = (
                    (is_long and new_sl > self.sl) or
                    (not is_long and new_sl < self.sl)
                )
                if sl_moved:
                    old_sl = self.sl
                    self.sl = new_sl
                    logger.debug("[%s] Trailing SL → %.4f (era %.4f)", self.symbol, self.sl, old_sl)
                    try:
                        qty_positions = await self._get_positions()
                        if qty_positions:
                            live_qty = abs(float(qty_positions[0].get("szi", 0)))
                            if live_qty > 0:
                                await self._place_tpsl(live_qty, self.sl, None)
                    except Exception as e:
                        logger.warning("[%s] No se pudo actualizar trailing SL: %s", self.symbol, e)

        # ── Evaluar SL / TP ───────────────────────────────────────────
        sl_hit  = self.sl  and ((is_long and price <= self.sl)  or (not is_long and price >= self.sl))
        tp3_hit = self.tp3 and ((is_long and price >= self.tp3) or (not is_long and price <= self.tp3))
        tp1_hit = self.tp1 and not self.tp2 and ((is_long and price >= self.tp1) or (not is_long and price <= self.tp1))

        close_reason = "SL" if sl_hit else ("TP3" if tp3_hit else ("TP1" if tp1_hit else None))
        if not close_reason:
            return

        positions = await self._get_positions()
        if positions is None:
            logger.warning("[%s] Cierre por %s: error de red — reintentando.", self.symbol, close_reason)
            return
        if not positions:
            logger.warning("[%s] Cierre por %s: ya cerrada en exchange.", self.symbol, close_reason)
            await self._on_position_closed(pnl_pct=0.0)
            self._clear_position_state()
            clear_position(self.symbol)
            return

        qty = abs(float(positions[0].get("szi", 0)))
        if qty <= 0:
            logger.error("[%s] Cierre por %s: qty=0.", self.symbol, close_reason)
            return

        close_side = "sell" if is_long else "buy"
        fill_price = price

        if not self.dry_run:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._hl_client.cancel_all_open_tpsl)
            except Exception as e:
                logger.warning("[%s] No se pudieron cancelar triggers antes del cierre: %s", self.symbol, e)

            result = await self._place_order(close_side, qty, reduce_only=True)
            if result.get("status") == "ok":
                try:
                    fill_price = float(
                        result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                        .get("filled", {}).get("avgPx", price)
                    )
                except Exception:
                    pass

        pnl_pct = ((fill_price - self.entry_price) / self.entry_price) * (1 if is_long else -1) * 100
        pnl_usd = (pnl_pct / 100) * self._open_notional
        if pnl_usd > 0:
            self.win_count += 1
        self.total_pnl += pnl_usd
        logger.info("[%s] 🔒 Cerrado por %s · fill=%.4f · PnL=%.2f USDC (%.2f%%)",
                    self.symbol, close_reason, fill_price, pnl_usd, pnl_pct)

        entry_copy = self.entry_price
        pos_copy   = self.position

        await self._on_position_closed(pnl_pct)
        self._clear_position_state()
        clear_position(self.symbol)

        await notify_close(
            symbol=self.symbol,
            side=pos_copy,
            exit_p=fill_price,
            pnl=pnl_pct,
            entry=entry_copy,
            reason=close_reason,
        )

    async def _place_tpsl(self, qty: float, sl: float | None, tp: float | None) -> None:
        if not sl and not tp:
            return
        is_long = self.position == "long"
        orders = []
        if sl:
            orders.append(("sl", not is_long, qty, sl))
        if tp:
            orders.append(("tp", not is_long, qty, tp))
        for order_type, side_is_buy, q, px in orders:
            try:
                loop = asyncio.get_event_loop()
                if order_type == "sl":
                    await loop.run_in_executor(
                        None, lambda: self._hl_client.place_sl(is_buy=side_is_buy, sz=q, trigger_px=px)
                    )
                else:
                    await loop.run_in_executor(
                        None, lambda: self._hl_client.place_tp(is_buy=side_is_buy, sz=q, trigger_px=px)
                    )
            except Exception as e:
                logger.warning("[%s] No se pudo colocar %s: %s", self.symbol, order_type, e)
