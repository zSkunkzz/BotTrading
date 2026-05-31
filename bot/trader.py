"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Autenticación soportada:
  Opción A (recomendada): API Key de agente
    - HL_API_PRIVATE_KEY     : private key del wallet AGENTE generado en app.hyperliquid.xyz
    - HL_API_WALLET_ADDRESS  : dirección del wallet PRINCIPAL (el que tiene fondos y aprobó el agente)
    Las órdenes se firman con la clave del agente. El action_hash NO usa vault_address
    (debe ser None) — el campo vaultAddress del payload JSON también va a None.
    El endpoint /info usa master_addr para consultar posiciones y balance.

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
import os
import time
import json as _json
from decimal import Decimal
from typing import Optional

import aiohttp
from eth_account import Account
from eth_utils import to_hex

# SDK oficial de Hyperliquid — garantiza el mismo signing que el exchange
from hyperliquid.utils.signing import sign_l1_action, float_to_wire

from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close, notify_tp_partial
from bot.state import save_position, load_position, clear_position, mark_tp2_hit
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.execution_engine import execution_engine
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

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

# ── Rate limiter global para /info ──────────────────────────────────────────
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


# ── Nonce único global ───────────────────────────────────────────────────────
_NONCE_LOCK = asyncio.Lock()
_NONCE_LAST = 0

async def _unique_nonce() -> int:
    global _NONCE_LAST
    async with _NONCE_LOCK:
        n = int(time.time() * 1000)
        if n <= _NONCE_LAST:
            n = _NONCE_LAST + 1
        _NONCE_LAST = n
        return n


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm_coin(symbol: str) -> str:
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


# Alias para compatibilidad con el resto del código que usaba _float_to_wire
_float_to_wire = float_to_wire


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
                    f"[{symbol}] HL_API_WALLET_ADDRESS es OBLIGATORIA en modo agente. "
                    "Debe ser la dirección del wallet PRINCIPAL (el que tiene fondos) "
                    "que aprobó al agente en app.hyperliquid.xyz → Settings → API."
                )
            self._private_key  = api_pk
            self._agent_mode   = True
            agent_acct         = Account.from_key(api_pk)
            self._agent_addr   = agent_acct.address
            self._master_addr  = api_wallet
            # En modo agente:
            #   - _account_addr  → master (para consultas /info: balance, posiciones)
            #   - _vault_address → None   (el agente firma SIN vault_address)
            self._account_addr  = self._master_addr
            self._vault_address = None
            logger.info(
                "[%s] Auth: modo agente | master=%s | agente=%s",
                symbol, self._master_addr[:10] + "...", self._agent_addr[:10] + "...",
            )
        else:
            pk = os.getenv("HL_PRIVATE_KEY", api_secret or "").strip()
            if not pk:
                raise ValueError(
                    f"[{symbol}] No hay ninguna clave configurada. "
                    "Configura HL_API_PRIVATE_KEY (modo agente) o HL_PRIVATE_KEY (modo directo)."
                )
            self._private_key   = pk
            self._account_addr  = os.getenv("HL_ACCOUNT_ADDR", "").strip()
            self._agent_mode    = False
            self._agent_addr    = ""
            self._vault_address = None
            if not self._account_addr:
                acct = Account.from_key(pk)
                self._account_addr = acct.address
                logger.debug("[%s] HL_ACCOUNT_ADDR derivada: %s", symbol, self._account_addr)
            self._master_addr  = self._account_addr
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

    # ── ccxt ────────────────────────────────────────────────────────────

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

    # ── Coin index ──────────────────────────────────────────────────────

    _coin_index_cache: dict[str, int] = {}

    async def _get_coin_index(self) -> int:
        if self.coin in self._coin_index_cache:
            return self._coin_index_cache[self.coin]
        data = await self._info_post({"type": "meta"})
        for i, uni in enumerate(data.get("universe", [])):
            self._coin_index_cache[uni.get("name", "")] = i
        return self._coin_index_cache.get(self.coin, 0)

    # ── HTTP helpers ────────────────────────────────────────────────────

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

    async def _exchange_post(self, action: dict) -> dict:
        if not self._private_key:
            raise ValueError("No hay clave configurada (HL_API_PRIVATE_KEY o HL_PRIVATE_KEY)")

        nonce = await _unique_nonce()

        # Usar SDK oficial de Hyperliquid para signing — garantiza
        # el mismo msgpack + EIP-712 phantom-agent que el exchange valida.
        # vault_address=None en modo agente normal.
        wallet    = Account.from_key(self._private_key)
        signature = sign_l1_action(
            wallet,
            action,
            self._vault_address,   # None en modo agente
            nonce,
            not _USE_TESTNET,      # is_mainnet
        )

        payload: dict = {
            "action":       action,
            "nonce":        nonce,
            "signature":    {"r": to_hex(signature["r"]), "s": to_hex(signature["s"]), "v": signature["v"]},
            "vaultAddress": self._vault_address,
        }

        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/exchange", json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                text = await r.text()
                try:
                    result = _json.loads(text)
                except Exception:
                    result = {"status": "error", "response": text}

        if result.get("status") != "ok":
            err_str = str(result.get("response", ""))
            if "does not exist" in err_str:
                logger.error(
                    "[%s] HL rechazó con 'does not exist'.\n"
                    "  agente=%s | vault=%s | respuesta HL: %s\n"
                    "  → Verifica que el agente %s esté aprobado en\n"
                    "    app.hyperliquid.xyz → Settings → API para la master=%s",
                    self.symbol,
                    self._agent_addr  or "N/A",
                    self._vault_address or "None",
                    err_str,
                    self._agent_addr  or "N/A",
                    self._master_addr or "NO CONFIGURADA",
                )
        return result

    # ── Init / cleanup ───────────────────────────────────────────────────

    async def _init(self, usdc_per_trade: float):
        await self._get_ccxt()
        saved = load_position(self.symbol)
        if saved:
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

        if not balance_svc.is_ready():
            balance_svc.init_hl(self._master_addr, self._info_post)

        await self._set_leverage(self.leverage)
        logger.info(
            "[%s] Trader iniciado | coin=%s | master=%s | agent_mode=%s | agente=%s",
            self.symbol, self.coin,
            self._master_addr[:10] + "..." if self._master_addr else "N/A",
            self._agent_mode,
            self._agent_addr[:10] + "..." if self._agent_addr else "N/A",
        )

    async def cleanup(self):
        await self._close_ccxt()

    # ── Precio y OHLCV ───────────────────────────────────────────────────

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

    # ── Leverage ─────────────────────────────────────────────────────────

    async def _set_leverage(self, leverage: int, side: str = "cross"):
        if self.dry_run:
            return
        coin_idx = await self._get_coin_index()
        is_cross = (self.margin_mode != "isolated")
        action   = {"type": "updateLeverage", "asset": coin_idx,
                    "isCross": is_cross, "leverage": leverage}
        r = await self._exchange_post(action)
        if r.get("status") == "ok":
            logger.debug("[%s] Leverage %sx OK (cross=%s)", self.symbol, leverage, is_cross)
        else:
            logger.debug("[%s] set_leverage respuesta: %s", self.symbol, r)

    # ── Órdenes ──────────────────────────────────────────────────────────

    async def _place_order_raw(
        self,
        side:        str,
        qty:         float,
        order_type:  str = "market",
        price:       float | None = None,
        reduce_only: bool = False,
        sl:          float | None = None,
        tp:          float | None = None,
        trade_side:  str = "open",
        pos_side:    str | None = None,
    ) -> dict:
        if self.dry_run:
            logger.info("[%s] DRY RUN: side=%s qty=%s price=%s sl=%s tp=%s",
                        self.symbol, side, qty, price, sl, tp)
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {"oid": "dry"}}]}}}

        coin_idx = await self._get_coin_index()
        is_buy   = side in ("buy", "long")

        if order_type == "limit" and price:
            order_type_obj = {"limit": {"tif": "Gtc"}}
            limit_px       = float_to_wire(price)
        else:
            current_price  = await self.get_price()
            slippage       = 0.05
            raw_px         = current_price * (1 + slippage) if is_buy else current_price * (1 - slippage)
            slippage_px    = round(float(f"{raw_px:.5g}"), 6)
            limit_px       = float_to_wire(slippage_px)
            order_type_obj = {"limit": {"tif": "Ioc"}}

        order: dict = {
            "a": coin_idx, "b": is_buy,
            "p": limit_px, "s": float_to_wire(qty),
            "r": reduce_only, "t": order_type_obj,
        }

        tpsl_parts = []
        if tp:
            tpsl_parts.append({
                "a": coin_idx, "b": not is_buy,
                "p": float_to_wire(tp), "s": float_to_wire(qty), "r": True,
                "t": {"trigger": {"triggerPx": float_to_wire(tp), "isMarket": True, "tpsl": "tp"}},
            })
        if sl:
            tpsl_parts.append({
                "a": coin_idx, "b": not is_buy,
                "p": float_to_wire(sl), "s": float_to_wire(qty), "r": True,
                "t": {"trigger": {"triggerPx": float_to_wire(sl), "isMarket": True, "tpsl": "sl"}},
            })

        if tpsl_parts:
            action = {"type": "order", "orders": [order] + tpsl_parts, "grouping": "positionTpsl"}
        else:
            action = {"type": "order", "orders": [order], "grouping": "na"}

        try:
            result = await self._exchange_post(action)
            if result.get("status") != "ok":
                logger.error("[%s] _place_order_raw FAILED: %s", self.symbol, result)
            else:
                logger.debug("[%s] _place_order_raw OK: side=%s qty=%s", self.symbol, side, qty)
            return result
        except Exception as e:
            logger.error("[%s] _place_order_raw exception: %s", self.symbol, e)
            return {"status": "error", "response": str(e)}

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

    async def _cancel_order(self, order_id) -> dict:
        if self.dry_run:
            return {"status": "ok"}
        try:
            coin_idx = await self._get_coin_index()
            action   = {"type": "cancel", "cancels": [{"a": coin_idx, "o": int(order_id)}]}
            return await self._exchange_post(action)
        except Exception as e:
            logger.debug("[%s] _cancel_order error: %s", self.symbol, e)
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

    # ── Posiciones ───────────────────────────────────────────────────────

    async def _get_positions(self) -> list:
        try:
            data = await self._info_post({"type": "clearinghouseState", "user": self._account_addr})
            if not data or not isinstance(data, dict):
                logger.warning("[%s] _get_positions: respuesta vacía o inválida", self.symbol)
                return []
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
            return []

    # ── Loop principal ───────────────────────────────────────────────────

    async def run(self, risk, *, global_risk=None):
        """Loop principal del trader para un símbolo."""
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
        """Una iteración del loop de trading."""
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
        exchange_positions = []
        if now - self._last_pos_check_at >= _POS_CHECK_INTERVAL_S:
            exchange_positions     = await self._get_positions()
            self._last_pos_check_at = now
            did_check_exchange     = True

        if did_check_exchange:
            if exchange_positions:
                ep = exchange_positions[0]
                if self.position is None:
                    self.position    = ep["side"]
                    self.entry_price = ep["entryPx"]
                    logger.info("[%s] Posición detectada en exchange: %s @ %s",
                                self.symbol, self.position, self.entry_price)
            else:
                if self.position is not None:
                    logger.info("[%s] Posición cerrada externamente.", self.symbol)
                    self.position    = None
                    self.entry_price = None
                    self.sl = self.tp1 = self.tp2 = self.tp3 = None
                    self.tp2_hit = False
                    clear_position(self.symbol)

        if self.position is not None:
            await self._manage_open_position(price, risk)
            return

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
            if not await pretrade_risk.check(self.symbol, risk, balance or 0.0):
                logger.debug("[%s] pretrade_risk bloqueó la entrada.", self.symbol)
                return
        except Exception as e:
            logger.debug("[%s] pretrade_risk error (ignorando): %s", self.symbol, e)

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

        if signal:
            entry = signal.entry or price
            sl    = signal.sl
            tp1   = signal.tp1
            tp2   = signal.tp2
            tp3   = getattr(signal, "tp3", None)
            lev   = signal.suggested_lev or self.leverage
        else:
            entry = price
            sl = tp1 = tp2 = tp3 = None
            lev = self.leverage

        lev = min(int(lev), self.leverage)
        if lev != self.leverage:
            await self._set_leverage(lev)

        notional = risk.usdc_per_trade * lev
        qty      = round(notional / entry, 6)
        if qty <= 0:
            logger.warning("[%s] Cantidad calculada <= 0, skip.", self.symbol)
            return

        side = "buy" if action == "BUY" else "sell"
        logger.info(
            "[%s] 📈 Abriendo %s · qty=%s · entry=~%s · sl=%s · tp1=%s | %s",
            self.symbol, action, qty, round(entry, 4),
            round(sl, 4) if sl else "N/A",
            round(tp1, 4) if tp1 else "N/A",
            decision.get("reason", ""),
        )

        result = {"status": "ok"} if self.dry_run else await self._place_order(side, qty, sl=sl, tp=tp1)

        if result.get("status") == "ok":
            self.position       = "long" if action == "BUY" else "short"
            self.entry_price    = entry
            self.sl             = sl
            self.tp1            = tp1
            self.tp2            = tp2
            self.tp3            = tp3
            self.tp2_hit        = False
            self._open_notional = notional
            self._open_leverage = lev
            self._protection_ok = False
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

            await notify_open(
                symbol=self.symbol,
                side=self.position,
                entry=self.entry_price,
                sl=self.sl,
                tp1=self.tp1,
                tp2=self.tp2,
                size_usdc=notional,
                leverage=lev,
                signal_block=decision.get("signal_block", ""),
                ai_used=decision.get("ai_used", False),
                ai_confidence=decision.get("ai_confidence", 0),
            )

    async def _manage_open_position(self, price: float, risk):
        """Gestiona TP parciales, trailing stop y cierre de posición."""
        if self.position is None or self.entry_price is None:
            return

        is_long = self.position == "long"
        pnl_pct = ((price - self.entry_price) / self.entry_price) * (1 if is_long else -1) * 100

        # TP2 parcial
        if self.tp2 and not self.tp2_hit:
            tp2_triggered = (is_long and price >= self.tp2) or (not is_long and price <= self.tp2)
            if tp2_triggered:
                self.tp2_hit = True
                mark_tp2_hit(self.symbol)
                partial_qty = round((self._open_notional / self.entry_price) * TP2_PARTIAL_RATIO, 6)
                if partial_qty > 0 and not self.dry_run:
                    close_side = "sell" if is_long else "buy"
                    r = await self._place_order(close_side, partial_qty, reduce_only=True)
                    if r.get("status") == "ok":
                        logger.info("[%s] TP2 parcial ejecutado (%.1f%%)", self.symbol, TP2_PARTIAL_RATIO * 100)
                        await notify_tp_partial(self.symbol, self.position, price, self.tp2, partial_qty)

        # Trailing stop
        if risk.trailing_sl and self.sl is not None:
            activation_px = self.entry_price * (
                1 + risk.trailing_activation_pct / 100 if is_long
                else 1 - risk.trailing_activation_pct / 100
            )
            activated = (is_long and price >= activation_px) or (not is_long and price <= activation_px)
            if activated:
                callback = risk.trailing_callback_pct / 100
                new_sl = price * (1 - callback if is_long else 1 + callback)
                if is_long and new_sl > self.sl:
                    self.sl = new_sl
                    logger.debug("[%s] Trailing SL → %.4f", self.symbol, self.sl)
                elif not is_long and new_sl < self.sl:
                    self.sl = new_sl
                    logger.debug("[%s] Trailing SL → %.4f", self.symbol, self.sl)

        # Evaluar SL / TP
        sl_hit  = self.sl  and ((is_long and price <= self.sl)  or (not is_long and price >= self.sl))
        tp3_hit = self.tp3 and ((is_long and price >= self.tp3) or (not is_long and price <= self.tp3))
        tp1_hit = self.tp1 and not self.tp2 and ((is_long and price >= self.tp1) or (not is_long and price <= self.tp1))

        close_reason = "SL" if sl_hit else ("TP3" if tp3_hit else ("TP1" if tp1_hit else None))
        if not close_reason:
            return

        positions = await self._get_positions()
        if not positions:
            logger.warning("[%s] Cierre por %s: posición no encontrada en exchange (ya cerrada?).",
                           self.symbol, close_reason)
            self.position = self.entry_price = self.sl = None
            self.tp1 = self.tp2 = self.tp3 = None
            self.tp2_hit = False
            clear_position(self.symbol)
            return

        qty = abs(float(positions[0].get("szi", 0)))
        if qty <= 0:
            logger.error("[%s] Cierre por %s: qty=0, no se envía orden.", self.symbol, close_reason)
            return

        close_side = "sell" if is_long else "buy"
        if not self.dry_run:
            await self._place_order(close_side, qty, reduce_only=True)

        pnl_usd = (pnl_pct / 100) * self._open_notional
        if pnl_usd > 0:
            self.win_count += 1
        self.total_pnl += pnl_usd
        logger.info("[%s] 🔒 Cerrado por %s · PnL=%.2f USDC (%.2f%%)",
                    self.symbol, close_reason, pnl_usd, pnl_pct)

        entry_copy = self.entry_price
        pos_copy   = self.position
        self.position = self.entry_price = self.sl = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

        await notify_close(
            symbol=self.symbol,
            side=pos_copy,
            entry=entry_copy,
            exit_price=price,
            pnl_usd=pnl_usd,
            reason=close_reason,
        )
