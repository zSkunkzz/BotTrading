"""
trader.py — Motor de trading para Hyperliquid perpetuos.

Autenticación soportada:
  Opción A (recomendada): API Key de agente
    - HL_API_PRIVATE_KEY     : private key del wallet AGENTE generado en app.hyperliquid.xyz
    - HL_API_WALLET_ADDRESS  : dirección del wallet PRINCIPAL (el que tiene fondos y aprobó el agente)
    Las órdenes se firman con la clave del agente. NO se usa vaultAddress en el payload
    (eso es solo para vaults/subaccounts). El agente debe estar aprobado en app.hyperliquid.xyz.

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


# ── Helpers de signing (alineados con hyperliquid-python-sdk/signing.py) ──────

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
    Hash canónico de una acción L1 según el SDK oficial:
      msgpack(action) + nonce(8 bytes BE) + vault_flag [+ vault_bytes] [+ expires_flag + expires(8 bytes BE)]
    """
    data = msgpack.packb(action, use_bin_type=True)
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
    """Estructura EIP-712 que usa el SDK para firmar acciones L1 (órdenes, cancel, leverage, etc.)."""
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
    Firma una acción L1 (order, cancel, updateLeverage, etc.) exactamente como
    lo hace el SDK oficial de Hyperliquid.
    """
    h         = _action_hash(action, vault_address, nonce, expires_after)
    agent     = _phantom_agent(h, is_mainnet)
    data      = _l1_payload(agent)
    structured = encode_typed_data(full_message=data)
    signed    = Account.sign_message(structured, private_key=private_key)
    return {"r": to_hex(signed.r), "s": to_hex(signed.s), "v": signed.v}


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

        # ── Autenticación según doc oficial HL ──────────────────────────
        #
        # Opción A — API key de agente:
        #   HL_API_PRIVATE_KEY     = private key del wallet AGENTE
        #   HL_API_WALLET_ADDRESS  = dirección del wallet PRINCIPAL (el que tiene fondos)
        #
        #   Flujo correcto (doc oficial):
        #   - Firmar con la clave del AGENTE
        #   - NO enviar vaultAddress en el payload (eso es solo para vaults/subaccounts)
        #   - El agente debe estar aprobado en app.hyperliquid.xyz Settings > API
        #   - Las consultas /info (balance, posiciones) se hacen con la dirección PRINCIPAL
        #
        # Opción B — private key directa del wallet principal:
        #   HL_PRIVATE_KEY    = private key del wallet principal
        #   HL_ACCOUNT_ADDR   = dirección pública (opcional, se deriva)

        api_pk     = os.getenv("HL_API_PRIVATE_KEY", "").strip()
        api_wallet = os.getenv("HL_API_WALLET_ADDRESS", "").strip()

        if api_pk:
            # Opción A: firmar con clave de agente, sin vaultAddress en el payload
            self._private_key       = api_pk
            self._agent_mode        = True
            # Dirección del agente (derivada de su clave) — usada para firmar
            agent_acct = Account.from_key(api_pk)
            self._agent_addr        = agent_acct.address
            # Dirección principal — usada para consultas /info (balance, posiciones)
            self._master_addr       = api_wallet if api_wallet else self._agent_addr
            # _account_addr apunta al master para que clearinghouseState y balance lean bien
            self._account_addr      = self._master_addr
            logger.info("[%s] Auth: API key agente → agente=%s master=%s",
                        symbol, self._agent_addr[:10] + "...", self._master_addr[:10] + "...")
        else:
            # Opción B: private key directa
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

        # Estado de posición
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

        self.exchange = None

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
        """POST a /info (sin autenticación)."""
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{_API_URL}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                return _json.loads(await r.text())

    async def _exchange_post(self, action: dict) -> dict:
        """
        POST autenticado a /exchange con firma EIP-712 L1 (SDK oficial).

        Según la doc oficial de Hyperliquid:
        - Las API keys de agente firman directamente SIN vaultAddress.
          vaultAddress solo se usa para vaults/subaccounts reales.
        - El hash de la acción no incluye vault_address en modo agente.
        """
        if not self._private_key:
            raise ValueError("No hay clave configurada (HL_API_PRIVATE_KEY o HL_PRIVATE_KEY)")

        nonce      = int(time.time() * 1000)
        is_mainnet = not _USE_TESTNET

        # Modo agente: no hay vault_address en el hash ni en el payload
        # Modo directo (Opción B): tampoco, a menos que se opere sobre un vault
        vault_address = None

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
        # NO añadir vaultAddress — eso solo es para vaults/subaccounts reales
        # Ref: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint

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
        import ccxt.async_support as ccxt
        # ccxt usa la clave de firma (agente o principal) y la dirección para consultas
        self.exchange = ccxt.hyperliquid({
            "walletAddress": self._master_addr,
            "privateKey":    self._private_key,
        })

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
        logger.info("[%s] Trader Hyperliquid iniciado | coin=%s | addr=%s | agent=%s",
                    self.symbol, self.coin, self._account_addr[:10] + "...", self._agent_mode)

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

        try:
            tf_ms = {"15m": 15*60*1000, "1h": 60*60*1000, "4h": 4*60*60*1000}.get(tf, 15*60*1000)
            now   = int(time.time() * 1000)
            start = now - OHLCV_LIMIT * tf_ms
            data  = await self._info_post({
                "type": "candleSnapshot",
                "req":  {"coin": self.coin, "interval": tf, "startTime": start, "endTime": now},
            })
            return [
                [int(c["t"]), float(c["o"]), float(c["h"]),
                 float(c["l"]), float(c["c"]), float(c["v"])]
                for c in data
            ]
        except Exception as e:
            logger.error("[%s] get_ohlcv REST error: %s", self.symbol, e)
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
            logger.warning("[%s] set_leverage error: %s", self.symbol, r)

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

        # TP/SL: se envían como órdenes trigger separadas según la doc oficial
        if sl or tp:
            tpsl_parts = []
            if tp:
                tpsl_parts.append({
                    "a": coin_idx,
                    "b": not is_buy,  # cierre en dirección contraria
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
            positions = []
            for p in data.get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") == self.coin:
                    szi = float(pos.get("szi", 0))
                    if abs(szi) > 0:
                        positions.append({
                            "coin":          pos["coin"],
                            "size":          abs(szi),
                            "side":          "long" if szi > 0 else "short",
                            "entryPx":       float(pos.get("entryPx") or 0),
                            "unrealizedPnl": float(pos.get("unrealizedPnl") or 0),
                        })
            return positions
        except Exception as e:
            logger.debug("[%s] _get_positions error: %s", self.symbol, e)
            return []

    async def _check_external_close(self) -> bool:
        if self.dry_run:
            return False
        try:
            positions = await self._get_positions()
            if positions:
                self._last_pos_check_at = time.time()
                return False

            closed_side  = self.position
            closed_entry = self.entry_price
            exit_price   = await self.get_price()

            pnl = 0.0
            if closed_entry and exit_price:
                if closed_side == "long":
                    pnl = (exit_price - closed_entry) / closed_entry * 100
                else:
                    pnl = (closed_entry - exit_price) / closed_entry * 100

            logger.warning(
                "[%s] 🔔 Posicion cerrada externamente (TPSL) | side=%s entry=%.4f exit=~%.4f pnl=~%+.2f%%",
                self.symbol, closed_side, closed_entry or 0, exit_price, pnl,
            )

            pretrade_risk.register_close(self.symbol, self._open_notional)
            self._open_notional = 0.0
            self._open_leverage = 1
            self.position       = None
            self.entry_price    = None
            self.sl = self.tp1 = self.tp2 = self.tp3 = None
            self.tp2_hit        = False
            self._protection_ok = False
            self._last_pos_check_at   = time.time()
            self._last_tpsl_verify_at = 0.0
            clear_position(self.symbol)

            if pnl >= 0:
                self.win_count += 1
            self.trade_count += 1
            self.total_pnl   += pnl
            await kill_switch.on_trade_result(pnl)
            await notify_close(self.symbol, closed_side, exit_price, pnl,
                               reason="TPSL_SERVIDOR", dry_run=self.dry_run)
            return True
        except Exception as e:
            logger.error("[%s] _check_external_close error: %s", self.symbol, e)
            return False

    # ── Qty ─────────────────────────────────────────────────────────────

    async def _get_min_qty(self) -> float:
        try:
            data = await self._info_post({"type": "meta"})
            for uni in data.get("universe", []):
                if uni.get("name") == self.coin:
                    decimals = int(uni.get("szDecimals", 3))
                    return 10 ** (-decimals)
        except Exception:
            pass
        return 0.001

    async def _calc_qty(self, usdc_amount: float, price: float, leverage: int) -> float:
        effective_lev = leverage or self.leverage
        raw_qty  = (usdc_amount * effective_lev) / price
        min_qty  = await self._get_min_qty()
        qty      = max(min_qty, round(raw_qty / min_qty) * min_qty)
        decimals = len(str(min_qty).rstrip("0").split(".")[-1]) if "." in str(min_qty) else 0
        return round(qty, decimals)

    # ── Abrir posiciones ─────────────────────────────────────────────────

    async def open_long(self, usdc_amount, sl=None, tp1=None, tp2=None, tp3=None, leverage=None):
        if kill_switch.is_halted(self.symbol):
            return
        price   = await self.get_price()
        lev     = leverage or self.leverage
        qty     = await self._calc_qty(usdc_amount, price, lev)
        balance = await self.get_balance() or 0.0
        ok, reason = await pretrade_risk.check(
            symbol=self.symbol, side="buy", notional=usdc_amount,
            price=price, balance=balance, sl=sl,
        )
        if not ok:
            logger.warning("[%s] open_long bloqueado: %s", self.symbol, reason)
            return
        await self._set_leverage(lev)
        r = await self._place_order("buy", qty, reduce_only=False, sl=sl, tp=tp3)
        if r.get("status") == "ok":
            self.position       = "long"
            self.entry_price    = price
            self.sl   = sl
            self.tp1  = tp1
            self.tp2  = tp2
            self.tp3  = tp3
            self.tp2_hit        = False
            self._open_notional = usdc_amount
            self._open_leverage = lev
            self._protection_ok = bool(sl or tp3)
            self._last_pos_check_at = time.time()
            save_position(self.symbol, "long", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                          usdc_amount=usdc_amount, leverage=lev)
            logger.warning("[%s] LONG @ %.4f lev=%sx sl=%s tp=%s", self.symbol, price, lev, sl, tp3)
            await notify_open(self.symbol, "long", price, lev, sl=sl,
                              tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run)
        else:
            logger.error("[%s] open_long FAILED: %s", self.symbol, r)

    async def open_short(self, usdc_amount, sl=None, tp1=None, tp2=None, tp3=None, leverage=None):
        if kill_switch.is_halted(self.symbol):
            return
        price   = await self.get_price()
        lev     = leverage or self.leverage
        qty     = await self._calc_qty(usdc_amount, price, lev)
        balance = await self.get_balance() or 0.0
        ok, reason = await pretrade_risk.check(
            symbol=self.symbol, side="sell", notional=usdc_amount,
            price=price, balance=balance, sl=sl,
        )
        if not ok:
            logger.warning("[%s] open_short bloqueado: %s", self.symbol, reason)
            return
        await self._set_leverage(lev)
        r = await self._place_order("sell", qty, reduce_only=False, sl=sl, tp=tp3)
        if r.get("status") == "ok":
            self.position       = "short"
            self.entry_price    = price
            self.sl   = sl
            self.tp1  = tp1
            self.tp2  = tp2
            self.tp3  = tp3
            self.tp2_hit        = False
            self._open_notional = usdc_amount
            self._open_leverage = lev
            self._protection_ok = bool(sl or tp3)
            self._last_pos_check_at = time.time()
            save_position(self.symbol, "short", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                          usdc_amount=usdc_amount, leverage=lev)
            logger.warning("[%s] SHORT @ %.4f lev=%sx sl=%s tp=%s", self.symbol, price, lev, sl, tp3)
            await notify_open(self.symbol, "short", price, lev, sl=sl,
                              tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run)
        else:
            logger.error("[%s] open_short FAILED: %s", self.symbol, r)

    async def close_position(self, reason: str = ""):
        if not self.position:
            return
        side = "sell" if self.position == "long" else "buy"
        qty  = 0.0
        try:
            positions = await self._get_positions()
            if positions:
                qty = float(positions[0].get("size", 0))
        except Exception:
            pass

        exit_price = await self.get_price()
        pnl = 0.0
        if self.entry_price and exit_price:
            if self.position == "long":
                pnl = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                pnl = (self.entry_price - exit_price) / self.entry_price * 100

        closed_side = self.position
        if qty > 0:
            r = await self._place_order(side, qty, reduce_only=True)
            if r.get("status") != "ok":
                logger.error("[%s] close_position FAILED: %s", self.symbol, r)
                await self._check_external_close()
                return

        pretrade_risk.register_close(self.symbol, self._open_notional)
        self._open_notional = 0.0
        self._open_leverage = 1
        self.position       = None
        self.entry_price    = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit        = False
        self._protection_ok = False
        self._last_tpsl_verify_at = 0.0
        self._last_pos_check_at   = time.time()
        clear_position(self.symbol)

        if pnl >= 0:
            self.win_count += 1
        self.trade_count += 1
        self.total_pnl   += pnl
        await kill_switch.on_trade_result(pnl)
        logger.warning("[%s] %s cerrado | razon=%s | pnl=%+.2f%%", self.symbol, closed_side, reason, pnl)
        await notify_close(self.symbol, closed_side, exit_price, pnl,
                           reason=reason, dry_run=self.dry_run)

    async def partial_close(self, ratio: float = 0.5):
        if not self.position:
            return
        side = "sell" if self.position == "long" else "buy"
        qty  = 0.0
        try:
            positions = await self._get_positions()
            if positions:
                total   = float(positions[0].get("size", 0))
                min_qty = await self._get_min_qty()
                qty     = max(min_qty, round((total * ratio) / min_qty) * min_qty)
        except Exception as e:
            logger.warning("[%s] partial_close: %s", self.symbol, e)
            return
        if not qty:
            return
        r = await self._place_order(side, qty, reduce_only=True)
        if r.get("status") == "ok":
            freed = self._open_notional * ratio
            pretrade_risk.register_close(self.symbol, freed)
            self._open_notional = max(0.0, self._open_notional - freed)
            mark_tp2_hit(self.symbol)
            self.tp2_hit = True
            exit_price   = await self.get_price()
            await notify_tp_partial(self.symbol, self.position, exit_price,
                                    ratio=ratio, dry_run=self.dry_run)
            logger.info("[%s] Cierre parcial %s%%", self.symbol, int(ratio * 100))
        else:
            logger.warning("[%s] partial_close FAILED: %s", self.symbol, r)

    # ── Loop principal ──────────────────────────────────────────────────

    async def run(self, risk, global_risk=None):
        usdc_per_trade = risk.usdc_per_trade
        await self._init(usdc_per_trade)

        async def _ai_decide_fn(symbol, bars, position, entry_price, leverage, context_override=None):
            return await ai_decide(
                symbol=symbol, bars=bars, position=position,
                entry_price=entry_price, leverage=leverage,
                context_override=context_override,
            )

        while True:
            try:
                if kill_switch.is_hard_killed():
                    logger.critical("[%s] KillSwitch HARD -- bot detenido", self.symbol)
                    return
                if kill_switch.is_halted(self.symbol):
                    await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))
                    continue

                price = await self.get_price()
                bars  = await self.get_ohlcv()
                if len(bars) < OHLCV_MIN_BARS:
                    await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))
                    continue

                if self.position:
                    if self.sl:
                        sl_long  = self.sl * (1 - _SL_SW_MARGIN)
                        sl_short = self.sl * (1 + _SL_SW_MARGIN)
                        if (self.position == "long"  and price <= sl_long) or \
                           (self.position == "short" and price >= sl_short):
                            logger.warning("[%s] 🛑 SL_SOFTWARE @ %.4f", self.symbol, price)
                            await self.close_position(reason="SL_SOFTWARE")
                            continue

                    if self.tp3:
                        if (self.position == "long"  and price >= self.tp3) or \
                           (self.position == "short" and price <= self.tp3):
                            logger.info("[%s] 🎯 TP3_SOFTWARE @ %.4f", self.symbol, price)
                            await self.close_position(reason="TP3_SOFTWARE")
                            continue

                    if (time.time() - self._last_pos_check_at) >= _POS_CHECK_INTERVAL_S:
                        was_closed = await self._check_external_close()
                        if was_closed:
                            continue

                    if self.tp2 and not self.tp2_hit:
                        if (self.position == "long"  and price >= self.tp2) or \
                           (self.position == "short" and price <= self.tp2):
                            await self.partial_close(ratio=TP2_PARTIAL_RATIO)

                    result = await decide(self.exchange, self.symbol, _ai_decide_fn, has_open_position=True)
                    action = result.get("action", "HOLD")
                    if action in ("CLOSE_LONG", "CLOSE_SHORT"):
                        await self.close_position(reason="strategy")

                else:
                    result = await decide(self.exchange, self.symbol, _ai_decide_fn, has_open_position=False)
                    action = result.get("action", "HOLD")
                    signal = result.get("signal")
                    usdc   = risk.usdc_per_trade

                    if signal:
                        lev     = signal.suggested_lev if signal.suggested_lev else self.leverage
                        sl      = signal.sl
                        tp1     = signal.tp1
                        tp2     = signal.tp2
                        atr_val = signal.atr
                        tp3     = round(signal.entry + 3.0 * atr_val, 6) if atr_val and signal.entry else None
                        if signal.signal == "SHORT" and signal.entry and atr_val:
                            tp3 = round(signal.entry - 3.0 * atr_val, 6)
                    else:
                        lev = self.leverage
                        sl  = tp1 = tp2 = tp3 = None

                    if action == "BUY":
                        await self.open_long(usdc, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)
                    elif action == "SELL":
                        await self.open_short(usdc, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)

            except asyncio.CancelledError:
                logger.info("[%s] Loop cancelado", self.symbol)
                break
            except Exception as e:
                logger.error("[%s] Loop error: %s", self.symbol, e, exc_info=True)
            await asyncio.sleep(int(os.getenv("LOOP_SLEEP", "10")))

        if self.exchange:
            try:
                await self.exchange.close()
            except Exception:
                pass
            self.exchange = None
