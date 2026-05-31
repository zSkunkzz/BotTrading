"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Autenticación soportada:
  Opción A (recomendada): API Key de agente
    - HL_API_PRIVATE_KEY     : private key del wallet AGENTE generado en app.hyperliquid.xyz
    - HL_API_WALLET_ADDRESS  : dirección del wallet PRINCIPAL (el que tiene fondos y aprobó el agente)
    Las órdenes se firman con la clave del agente. El action_hash INCLUYE master_addr
    como vault_address (igual que SDK oficial). Sin esto la firma es inválida.

  Opción B: Private key directa
    - HL_PRIVATE_KEY         : private key del wallet principal
    - HL_ACCOUNT_ADDR        : dirección pública (opcional, se deriva automáticamente)

Opcionales:
  HL_TESTNET       — "true" para usar testnet de Hyperliquid
  LOOP_SLEEP       — segundos entre iteraciones del loop (default 10)
  OHLCV_TF         — timeframe OHLCV (default 15m)
  OHLCV_LIMIT      — número de velas a cargar (default 200)
  OHLCV_MIN_BARS   — mínimo de velas requeridas (default 55)
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
import eth_account
import msgpack
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak, to_hex

from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close
from bot.state import save_position, load_position, clear_position, mark_tp2_hit
from bot.telegram_bot import notify_tp_partial
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.execution_engine import execution_engine
from bot.ohlcv_cache import ohlcv_cache

logger = logging.getLogger("Trader")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))
OHLCV_TF         = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT      = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS   = int(os.getenv("OHLCV_MIN_BARS", "55"))

_POS_CHECK_INTERVAL_S   = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))
_TPSL_VERIFY_INTERVAL_S = int(os.getenv("TPSL_VERIFY_INTERVAL_S", "120"))
_SL_SW_MARGIN           = float(os.getenv("SL_SW_MARGIN", "0.001"))

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL     = "https://api.hyperliquid-testnet.xyz" if _USE_TESTNET else "https://api.hyperliquid.xyz"

# ── Rate limiter global para /info (compartido entre todos los traders) ──
_HL_REST_LOCK    = asyncio.Lock()
_HL_LAST_CALL    = 0.0
_HL_MIN_INTERVAL = 0.6

async def _hl_throttle():
    """Espera el mínimo intervalo entre llamadas REST a Hyperliquid para evitar 429."""
    global _HL_LAST_CALL
    async with _HL_REST_LOCK:
        now = time.monotonic()
        wait = _HL_MIN_INTERVAL - (now - _HL_LAST_CALL)
        if wait > 0:
            await asyncio.sleep(wait)
        _HL_LAST_CALL = time.monotonic()


# ── Nonce único global — evita colisiones entre traders concurrentes ──
_NONCE_LOCK = asyncio.Lock()
_NONCE_LAST = 0

async def _unique_nonce() -> int:
    """Devuelve un nonce en milisegundos garantizando unicidad global."""
    global _NONCE_LAST
    async with _NONCE_LOCK:
        n = int(time.time() * 1000)
        if n <= _NONCE_LAST:
            n = _NONCE_LAST + 1
        _NONCE_LAST = n
        return n


# ── Helpers de signing — idénticos a hyperliquid-python-sdk/signing.py v0.23.0 ──

def _float_to_wire(x: float) -> str:
    """Convierte float a string sin notación científica, como exige Hyperliquid."""
    rounded = f"{x:.8f}"
    if abs(float(rounded) - x) >= 1e-12:
        raise ValueError(f"_float_to_wire rounding error: {x}")
    if rounded == "-0":
        rounded = "0"
    normalized = Decimal(rounded).normalize()
    return f"{normalized:f}"


def _address_to_bytes(address: str) -> bytes:
    return bytes.fromhex(address[2:] if address.startswith("0x") else address)


def _action_hash(action: dict, vault_address: Optional[str], nonce: int,
                 expires_after: Optional[int] = None) -> bytes:
    """
    Hash canónico de una acción L1.
    - msgpack.packb SIN opciones adicionales (igual que SDK oficial).
    """
    data = msgpack.packb(action)   # ← sin use_bin_type, idéntico al SDK oficial
    data += nonce.to_bytes(8, "big")
    if vault_address is None:
        data += b"\x00"
    else:
        data += b"\x01"
        data += _address_to_bytes(vault_address)
    if expires_after is not None:
        data += b"\x00"
        data += expires_after.to_bytes(8, "big")
    return keccak(data)


def _phantom_agent(hash_bytes: bytes, is_mainnet: bool) -> dict:
    return {"source": "a" if is_mainnet else "b", "connectionId": hash_bytes}


def _l1_payload(phantom_agent: dict) -> dict:
    return {
        "domain": {
            "chainId": 1337,
            "name": "Exchange",
            "verifyingContract": "0x0000000000000000000000000000000000000000",
            "version": "1",
        },
        "types": {
            "Agent": [
                {"name": "source",       "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
            "EIP712Domain": [
                {"name": "name",              "type": "string"},
                {"name": "version",           "type": "string"},
                {"name": "chainId",           "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
        },
        "primaryType": "Agent",
        "message": phantom_agent,
    }


def _sign_l1_action(private_key: str, action: dict, vault_address: Optional[str],
                    nonce: int, is_mainnet: bool,
                    expires_after: Optional[int] = None) -> dict:
    """
    Firma una acción L1 igual que sign_l1_action() del SDK oficial.
    """
    wallet     = Account.from_key(private_key)
    h          = _action_hash(action, vault_address, nonce, expires_after)
    agent      = _phantom_agent(h, is_mainnet)
    data       = _l1_payload(agent)
    structured = encode_typed_data(full_message=data)
    signed     = wallet.sign_message(structured)
    return {"r": to_hex(signed["r"]), "s": to_hex(signed["s"]), "v": signed["v"]}


def _norm_coin(symbol: str) -> str:
    """BTCUSDT / BTC/USDT:USDT → BTC"""
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


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
            self._private_key  = api_pk
            self._agent_mode   = True
            agent_acct         = Account.from_key(api_pk)
            self._agent_addr   = agent_acct.address
            self._master_addr  = api_wallet if api_wallet else self._agent_addr
            self._account_addr = self._master_addr
            if not api_wallet:
                logger.warning(
                    "[%s] HL_API_WALLET_ADDRESS no configurada en modo agente. "
                    "Usando dirección del agente como master. "
                    "Si el agente no es el wallet principal, las órdenes fallarán.",
                    symbol
                )
            logger.info("[%s] Auth: API key agente → agente=%s master=%s",
                        symbol, self._agent_addr[:10] + "...", self._master_addr[:10] + "...")
        else:
            pk = os.getenv("HL_PRIVATE_KEY", api_secret or "").strip()
            self._private_key  = pk
            self._account_addr = os.getenv("HL_ACCOUNT_ADDR", "").strip()
            self._agent_mode   = False
            self._agent_addr   = ""
            self._master_addr  = self._account_addr
            if pk and not self._account_addr:
                acct = Account.from_key(pk)
                self._account_addr = acct.address
                self._master_addr  = self._account_addr
            logger.info("[%s] Auth: private key directa → addr=%s", symbol,
                        self._account_addr[:10] + "..." if self._account_addr else "N/A")

        self.position     = None
        self.entry_price  = None
        self.sl           = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit      = False
        self.trade_count  = 0
        self.win_count    = 0
        self.total_pnl    = 0.0
        self._open_notional = 0.0
        self._open_leverage = 1
        self._protection_ok = False
        self._last_pos_check_at:   float = 0.0
        self._last_tpsl_verify_at: float = 0.0

        self._ccxt_exchange = None

    # ── ccxt session management ──────────────────────────────────────────

    async def _get_ccxt(self):
        if self._ccxt_exchange is None:
            import ccxt.async_support as ccxt
            self._ccxt_exchange = ccxt.hyperliquid({
                "walletAddress": self._master_addr,
                "privateKey":    self._private_key,
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
            name = uni.get("name", "")
            self._coin_index_cache[name] = i
        return self._coin_index_cache.get(self.coin, 0)

    # ── HTTP helpers ────────────────────────────────────────────────────

    async def _info_post(self, payload: dict) -> dict:
        """POST a /info con throttle para evitar 429."""
        await _hl_throttle()
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 429:
                    logger.warning("[%s] 429 en /info, esperando 5s...", self.symbol)
                    await asyncio.sleep(5.0)
                    await _hl_throttle()
                    async with aiohttp.ClientSession() as s2:
                        async with s2.post(
                            f"{_API_URL}/info",
                            json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r2:
                            return _json.loads(await r2.text())
                return _json.loads(await r.text())

    async def _exchange_post(self, action: dict) -> dict:
        """
        POST autenticado a /exchange con firma EIP-712 L1.
        """
        if not self._private_key:
            raise ValueError("No hay clave configurada (HL_API_PRIVATE_KEY o HL_PRIVATE_KEY)")

        nonce      = await _unique_nonce()
        is_mainnet = not _USE_TESTNET
        vault_address = self._master_addr if self._agent_mode else None

        signature = _sign_l1_action(
            private_key=self._private_key,
            action=action,
            vault_address=vault_address,
            nonce=nonce,
            is_mainnet=is_mainnet,
        )

        payload: dict = {
            "action":    action,
            "nonce":     nonce,
            "signature": signature,
        }

        if vault_address is not None:
            payload["vaultAddress"] = vault_address

        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/exchange",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                text = await r.text()
                try:
                    return _json.loads(text)
                except Exception:
                    return {"status": "error", "response": text}

    # ── Init ────────────────────────────────────────────────────────────

    async def _init(self, usdc_per_trade: float):
        await self._get_ccxt()

        saved = load_position(self.symbol)
        if saved:
            self.position    = saved["side"]
            self.entry_price = saved["entry"]
            self.sl          = saved.get("sl")
            self.tp1         = saved.get("tp1")
            self.tp2         = saved.get("tp2")
            self.tp3         = saved.get("tp3")
            self.tp2_hit     = saved.get("tp2_hit", False)
            self._open_notional = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
            self._open_leverage = saved.get("leverage", self.leverage)
            self._protection_ok = True
            logger.info("[%s] Posicion restaurada: %s @ %s", self.symbol, self.position, self.entry_price)

        if not balance_svc.is_ready():
            balance_svc.init_hl(self._master_addr, self._info_post)

        await self._set_leverage(self.leverage)
        logger.info("[%s] Trader Hyperliquid iniciado | coin=%s | addr=%s | agent_mode=%s",
                    self.symbol, self.coin, self._account_addr[:10] + "...", self._agent_mode)

    async def cleanup(self):
        await self._close_ccxt()

    # ── Precio y OHLCV ──────────────────────────────────────────────────

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
            if not isinstance(data, list) or len(data) == 0:
                logger.debug("[%s] get_ohlcv REST: respuesta inválida (%s)", self.symbol, type(data).__name__)
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

    # ── Leverage ────────────────────────────────────────────────────────

    async def _set_leverage(self, leverage: int, side: str = "cross"):
        if self.dry_run:
            return
        coin_idx = await self._get_coin_index()
        is_cross = (self.margin_mode != "isolated")
        action   = {
            "type":     "updateLeverage",
            "asset":    coin_idx,
            "isCross":  is_cross,
            "leverage": leverage,
        }
        r = await self._exchange_post(action)
        if r.get("status") == "ok":
            logger.debug("[%s] Leverage %sx OK (cross=%s)", self.symbol, leverage, is_cross)
        else:
            logger.debug("[%s] set_leverage respuesta: %s", self.symbol, r)

    # ── Órdenes ─────────────────────────────────────────────────────────

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
            limit_px       = _float_to_wire(price)
        else:
            current_price  = await self.get_price()
            slippage_px    = current_price * 1.05 if is_buy else current_price * 0.95
            limit_px       = _float_to_wire(slippage_px)
            order_type_obj = {"limit": {"tif": "Ioc"}}

        order: dict = {
            "a": coin_idx,
            "b": is_buy,
            "p": limit_px,
            "s": _float_to_wire(qty),
            "r": reduce_only,
            "t": order_type_obj,
        }

        if sl or tp:
            tpsl_parts = []
            if tp:
                tpsl_parts.append({
                    "a": coin_idx,
                    "b": not is_buy,
                    "p": _float_to_wire(tp),
                    "s": _float_to_wire(qty),
                    "r": True,
                    "t": {"trigger": {"triggerPx": _float_to_wire(tp), "isMarket": True, "tpsl": "tp"}},
                })
            if sl:
                tpsl_parts.append({
                    "a": coin_idx,
                    "b": not is_buy,
                    "p": _float_to_wire(sl),
                    "s": _float_to_wire(qty),
                    "r": True,
                    "t": {"trigger": {"triggerPx": _float_to_wire(sl), "isMarket": True, "tpsl": "sl"}},
                })
            if tpsl_parts:
                action = {
                    "type":     "order",
                    "orders":   [order] + tpsl_parts,
                    "grouping": "positionTpsl",
                }
            else:
                action = {"type": "order", "orders": [order], "grouping": "na"}
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

    async def _get_order_status(self, order_id: int) -> dict:
        try:
            return await self._info_post({"type": "orderStatus", "user": self._account_addr, "oid": order_id})
        except Exception as e:
            logger.debug("[%s] _get_order_status error: %s", self.symbol, e)
            return {}

    async def _cancel_order(self, order_id: int) -> dict:
        if self.dry_run:
            return {"status": "ok"}
        try:
            coin_idx = await self._get_coin_index()
            action   = {"type": "cancel", "cancels": [{"a": coin_idx, "o": order_id}]}
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

    # ── Posiciones ──────────────────────────────────────────────────────

    async def _get_positions(self) -> list:
        try:
            data = await self._info_post({"type": "clearinghouseState", "user": self._account_addr})
            # FIX: _info_post puede devolver None si hay timeout o error de red
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
