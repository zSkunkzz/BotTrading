import asyncio
import base64
import logging
import os
import hmac
import hashlib
import time
import json as _json
import aiohttp
import ccxt.async_support as ccxt
from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open, notify_close
from bot.state import (
    save_position, load_position, clear_position, mark_tp2_hit
)

logger = logging.getLogger("Trader")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))
BITGET_BASE = "https://api.bitget.com"


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol       = symbol
        self.leverage     = leverage
        self.margin_mode  = margin_mode or "isolated"
        self.dry_run      = dry_run
        self.position     = None
        self.entry_price  = None
        self.sl           = None
        self.tp1          = None
        self.tp2          = None
        self.tp3          = None
        self.tp2_hit      = False
        self.usdt_amount  = None
        self.trade_count  = 0
        self.win_count    = 0
        self.total_pnl    = 0.0
        self._api_key     = api_key
        self._api_secret  = api_secret
        self._passphrase  = passphrase
        # "v3" | "v2" | "ua" — se autodetecta al primer intento de orden
        # "ua" = Unified Account: v3 con posSide (hedge) o sin posSide (one-way)
        self._api_version = None
        # None = no detectado, "hedge" | "one_way" = modo detectado en UA
        self._ua_pos_mode = None
        self.exchange = ccxt.bitget({
            "apiKey":   api_key,
            "secret":   api_secret,
            "password": passphrase,
            "options":  {
                "defaultType":    "swap",
                "defaultSubType": "linear",
            },
        })

    # ─────────────────────────────────────────────────────────────
    # FIRMA HTTP DIRECTA
    # ─────────────────────────────────────────────────────────────

    def _sign(self, ts: str, method: str, path_with_qs: str, body: str = "") -> str:
        msg = ts + method.upper() + path_with_qs + body
        return base64.b64encode(
            hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _headers(self, method: str, path_with_qs: str, body: str = "") -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY":        self._api_key,
            "ACCESS-SIGN":       self._sign(ts, method, path_with_qs, body),
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }

    async def _http_get(self, path_with_qs: str) -> dict:
        url = BITGET_BASE + path_with_qs
        headers = self._headers("GET", path_with_qs)
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                text = await r.text()
                try:
                    return _json.loads(text)
                except Exception:
                    logger.warning(f"_http_get non-JSON ({r.status}): {text[:200]}")
                    return {"code": "ERR", "msg": text[:200]}

    async def _http_post(self, path: str, payload: dict) -> dict:
        body = _json.dumps(payload)
        headers = self._headers("POST", path, body)
        async with aiohttp.ClientSession() as s:
            async with s.post(BITGET_BASE + path, headers=headers, data=body,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                text = await r.text()
                try:
                    return _json.loads(text)
                except Exception:
                    logger.warning(f"_http_post non-JSON ({r.status}): {text[:200]}")
                    return {"code": "ERR", "msg": text[:200]}

    # ─────────────────────────────────────────────────────────────
    # BALANCE
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_usdt_available(data) -> float:
        if not data:
            return 0.0
        if isinstance(data, dict):
            data = [data]
        for item in data:
            coin = str(item.get("coin") or "").upper()
            if coin == "USDT":
                return float(
                    item.get("available") or item.get("availableAmount") or
                    item.get("crossMaxAvailable") or 0
                )
            for asset in (item.get("assets") or []):
                if str(asset.get("coin") or "").upper() == "USDT":
                    return float(
                        asset.get("available") or asset.get("availableAmount") or
                        asset.get("crossMaxAvailable") or 0
                    )
        return 0.0

    async def _get_balance_direct(self) -> float:
        for path in ["/api/v3/account/assets", "/api/v3/account/assets?coin=USDT"]:
            try:
                r = await self._http_get(path)
                if r.get("code") == "00000":
                    free = self._extract_usdt_available(r.get("data"))
                    if free > 0:
                        logger.info(f"[{self.symbol}] ✅ Balance USDT (v3/assets): {free}")
                        return free
            except Exception as e:
                logger.warning(f"[{self.symbol}] balance {path}: {e}")
        try:
            r = await self._http_get("/api/v2/unified/account/assets?coin=USDT")
            if r.get("code") == "00000":
                free = self._extract_usdt_available(r.get("data"))
                if free > 0:
                    logger.info(f"[{self.symbol}] ✅ Balance USDT (unified/assets): {free}")
                    return free
        except Exception as e:
            logger.warning(f"[{self.symbol}] balance unified/assets: {e}")
        logger.warning(f"[{self.symbol}] ⚠️ Balance = 0 — todos los endpoints fallaron.")
        return 0.0

    # ─────────────────────────────────────────────────────────────
    # ÓRDENES — con fallback automático v3 → v2 Mix → UA
    #
    # Flujo de autodetección (solo en el primer trade por par):
    #   1. Intentar v3 con holdSide (hedge mode estándar).
    #   2. Si 25236 → intentar v2 Mix con tradeSide (classic hedge).
    #   3. Si 40085 o 25236 en v2 → cuenta en Unified Account:
    #      3a. Intentar v3 con posSide (UA hedge mode).
    #      3b. Si 25236 → intentar v3 sin posSide ni holdSide (UA one-way).
    #   4. Una vez detectado, usar siempre esa versión directamente.
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _bitget_symbol(ccxt_symbol: str) -> str:
        return ccxt_symbol.split("/")[0] + ccxt_symbol.split("/")[1].split(":")[0]

    def _build_payload_v3(self, sym: str, side: str, trade_side: str,
                          qty: float, hold_side: str) -> dict:
        """Payload para /api/v3/trade/place-order (holdSide obligatorio — hedge)."""
        payload = {
            "symbol":     sym,
            "category":   "USDT-FUTURES",
            "marginMode": self.margin_mode,
            "marginCoin": "USDT",
            "qty":        str(qty),
            "side":       side,
            "orderType":  "market",
            "holdSide":   hold_side,
        }
        if trade_side == "close":
            payload["reduceOnly"] = "YES"
        return payload

    def _build_payload_ua_hedge(self, sym: str, side: str, trade_side: str,
                                qty: float, hold_side: str) -> dict:
        """
        Payload para Unified Account en modo HEDGE.
        Usa posSide (long/short) en lugar de holdSide.
        - Abrir long:   side=buy,  posSide=long
        - Abrir short:  side=sell, posSide=short
        - Cerrar long:  side=sell, posSide=long  + reduceOnly=YES
        - Cerrar short: side=buy,  posSide=short + reduceOnly=YES
        """
        pos_side = hold_side  # "long" o "short"
        payload = {
            "symbol":     sym,
            "category":   "USDT-FUTURES",
            "marginMode": self.margin_mode,
            "marginCoin": "USDT",
            "qty":        str(qty),
            "side":       side,
            "orderType":  "market",
            "posSide":    pos_side,
        }
        if trade_side == "close":
            payload["reduceOnly"] = "YES"
        return payload

    def _build_payload_ua_oneway(self, sym: str, side: str, trade_side: str,
                                 qty: float) -> dict:
        """
        Payload para Unified Account en modo ONE-WAY.
        Sin holdSide ni posSide. Cierre con reduceOnly=YES.
        """
        payload = {
            "symbol":     sym,
            "category":   "USDT-FUTURES",
            "marginMode": self.margin_mode,
            "marginCoin": "USDT",
            "qty":        str(qty),
            "side":       side,
            "orderType":  "market",
        }
        if trade_side == "close":
            payload["reduceOnly"] = "YES"
        return payload

    def _build_payload_v2(self, sym: str, side: str, trade_side: str,
                          qty: float, hold_side: str) -> dict:
        """Payload para /api/v2/mix/order/place-order (tradeSide, sin holdSide)."""
        payload = {
            "symbol":      sym,
            "productType": "USDT-FUTURES",
            "marginMode":  self.margin_mode,
            "marginCoin":  "USDT",
            "size":        str(qty),
            "side":        side,
            "orderType":   "market",
            "tradeSide":   trade_side,
        }
        if trade_side == "close":
            payload["reduceOnly"] = "YES"
        return payload

    async def _place_order(self, side: str, trade_side: str, qty: float,
                           hold_side: str = None) -> dict:
        """
        Coloca una orden con fallback automático v3 → v2 Mix → UA hedge → UA one-way.

        Versiones:
        - v3        : holdSide (hedge) — la mayoría de contratos
        - v2        : tradeSide (classic mix API) — algunos contratos legacy
        - ua        : Unified Account
                      · ua_hedge  = v3 con posSide (contratos en hedge)
                      · ua_oneway = v3 sin holdSide/posSide (contratos en one-way)

        - side       : "buy" | "sell"
        - trade_side : "open" | "close"
        - qty        : cantidad en contratos
        - hold_side  : "long" | "short"
        """
        sym = self._bitget_symbol(self.symbol)

        # Si ya sabemos qué versión usa este par, ir directamente
        if self._api_version == "v3":
            return await self._place_order_v3(sym, side, trade_side, qty, hold_side)
        if self._api_version == "v2":
            return await self._place_order_v2(sym, side, trade_side, qty, hold_side)
        if self._api_version == "ua":
            return await self._place_order_ua(sym, side, trade_side, qty, hold_side)

        # ── Autodetección: intentar v3 primero ──────────────────
        path_v3    = "/api/v3/trade/place-order"
        payload_v3 = self._build_payload_v3(sym, side, trade_side, qty, hold_side)
        logger.info(f"[{self.symbol}] 📤 order [v3]: {payload_v3}")
        resp_v3 = await self._http_post(path_v3, payload_v3)
        logger.info(f"[{self.symbol}] 📥 response [v3]: {resp_v3}")

        if resp_v3.get("code") == "00000":
            self._api_version = "v3"
            logger.info(f"[{self.symbol}] 📌 API version fijada: v3")
            order_id = (resp_v3.get("data") or {}).get("orderId", "?")
            logger.info(
                f"[{self.symbol}] ✅ {side}/{trade_side} holdSide={hold_side} "
                f"qty={qty} orderId={order_id} [v3]"
            )
            return resp_v3

        if resp_v3.get("code") == "25236":
            # Contrato no soporta holdSide → probar v2 Mix
            logger.warning(
                f"[{self.symbol}] ⚠️ v3 devolvió 25236 — "
                f"contrato no soporta holdSide, intentando v2 Mix"
            )
            path_v2    = "/api/v2/mix/order/place-order"
            payload_v2 = self._build_payload_v2(sym, side, trade_side, qty, hold_side)
            logger.info(f"[{self.symbol}] 📤 retry [v2]: {payload_v2}")
            resp_v2 = await self._http_post(path_v2, payload_v2)
            logger.info(f"[{self.symbol}] 📥 response [v2]: {resp_v2}")

            if resp_v2.get("code") == "00000":
                self._api_version = "v2"
                logger.info(f"[{self.symbol}] 📌 API version fijada: v2 Mix")
                order_id = (resp_v2.get("data") or {}).get("orderId", "?")
                logger.info(
                    f"[{self.symbol}] ✅ {side}/{trade_side} tradeSide={trade_side} "
                    f"qty={qty} orderId={order_id} [v2]"
                )
                return resp_v2

            # 40085 = Unified Account explícito, 25236 = UA que no acepta tradeSide
            # En ambos casos intentar UA
            if resp_v2.get("code") in ("40085", "25236"):
                logger.warning(
                    f"[{self.symbol}] ⚠️ v2 devolvió {resp_v2.get('code')} — "
                    f"Unified Account detectado, intentando UA hedge (posSide)"
                )
                resp_ua = await self._place_order_ua(sym, side, trade_side, qty, hold_side)
                if resp_ua.get("code") == "00000":
                    self._api_version = "ua"
                    logger.info(f"[{self.symbol}] 📌 API version fijada: UA (modo={self._ua_pos_mode})")
                return resp_ua

            raise Exception(f"place-order {resp_v2.get('code')}: {resp_v2.get('msg')}")

        # Cualquier otro error de v3
        raise Exception(f"place-order {resp_v3.get('code')}: {resp_v3.get('msg')}")

    async def _place_order_v3(self, sym: str, side: str, trade_side: str,
                               qty: float, hold_side: str) -> dict:
        path = "/api/v3/trade/place-order"
        payload = self._build_payload_v3(sym, side, trade_side, qty, hold_side)
        logger.info(f"[{self.symbol}] 📤 order [v3]: {payload}")
        resp = await self._http_post(path, payload)
        logger.info(f"[{self.symbol}] 📥 response [v3]: {resp}")
        if resp.get("code") == "00000":
            order_id = (resp.get("data") or {}).get("orderId", "?")
            logger.info(
                f"[{self.symbol}] ✅ {side}/{trade_side} holdSide={hold_side} "
                f"qty={qty} orderId={order_id} [v3]"
            )
            return resp
        raise Exception(f"place-order {resp.get('code')}: {resp.get('msg')}")

    async def _place_order_ua(self, sym: str, side: str, trade_side: str,
                               qty: float, hold_side: str = None) -> dict:
        """
        Unified Account: intenta primero con posSide (hedge mode).
        Si devuelve 25236, reintenta sin posSide (one-way mode).
        Persiste el modo detectado en self._ua_pos_mode.
        """
        path = "/api/v3/trade/place-order"

        # Si ya sabemos el modo UA de este par, ir directo
        if self._ua_pos_mode == "hedge" and hold_side:
            payload = self._build_payload_ua_hedge(sym, side, trade_side, qty, hold_side)
            logger.info(f"[{self.symbol}] 📤 order [ua-hedge]: {payload}")
            resp = await self._http_post(path, payload)
            logger.info(f"[{self.symbol}] 📥 response [ua-hedge]: {resp}")
            if resp.get("code") == "00000":
                order_id = (resp.get("data") or {}).get("orderId", "?")
                logger.info(f"[{self.symbol}] ✅ {side}/{trade_side} posSide={hold_side} qty={qty} orderId={order_id} [ua-hedge]")
                return resp
            raise Exception(f"place-order UA-hedge {resp.get('code')}: {resp.get('msg')}")

        if self._ua_pos_mode == "one_way":
            payload = self._build_payload_ua_oneway(sym, side, trade_side, qty)
            logger.info(f"[{self.symbol}] 📤 order [ua-oneway]: {payload}")
            resp = await self._http_post(path, payload)
            logger.info(f"[{self.symbol}] 📥 response [ua-oneway]: {resp}")
            if resp.get("code") == "00000":
                order_id = (resp.get("data") or {}).get("orderId", "?")
                logger.info(f"[{self.symbol}] ✅ {side}/{trade_side} qty={qty} orderId={order_id} [ua-oneway]")
                return resp
            raise Exception(f"place-order UA-oneway {resp.get('code')}: {resp.get('msg')}")

        # ── Autodetección UA: probar hedge primero ───────────────
        if hold_side:
            payload_hedge = self._build_payload_ua_hedge(sym, side, trade_side, qty, hold_side)
            logger.info(f"[{self.symbol}] 📤 order [ua-hedge]: {payload_hedge}")
            resp_hedge = await self._http_post(path, payload_hedge)
            logger.info(f"[{self.symbol}] 📥 response [ua-hedge]: {resp_hedge}")

            if resp_hedge.get("code") == "00000":
                self._ua_pos_mode = "hedge"
                logger.info(f"[{self.symbol}] 📌 UA pos mode: hedge (posSide)")
                order_id = (resp_hedge.get("data") or {}).get("orderId", "?")
                logger.info(f"[{self.symbol}] ✅ {side}/{trade_side} posSide={hold_side} qty={qty} orderId={order_id} [ua-hedge]")
                return resp_hedge

            if resp_hedge.get("code") != "25236":
                raise Exception(f"place-order UA-hedge {resp_hedge.get('code')}: {resp_hedge.get('msg')}")

            logger.warning(
                f"[{self.symbol}] ⚠️ UA hedge 25236 — contrato en one-way, intentando sin posSide"
            )

        # Fallback UA one-way (sin posSide)
        payload_ow = self._build_payload_ua_oneway(sym, side, trade_side, qty)
        logger.info(f"[{self.symbol}] 📤 order [ua-oneway]: {payload_ow}")
        resp_ow = await self._http_post(path, payload_ow)
        logger.info(f"[{self.symbol}] 📥 response [ua-oneway]: {resp_ow}")

        if resp_ow.get("code") == "00000":
            self._ua_pos_mode = "one_way"
            logger.info(f"[{self.symbol}] 📌 UA pos mode: one_way (sin posSide)")
            order_id = (resp_ow.get("data") or {}).get("orderId", "?")
            logger.info(f"[{self.symbol}] ✅ {side}/{trade_side} qty={qty} orderId={order_id} [ua-oneway]")
            return resp_ow

        raise Exception(f"place-order UA {resp_ow.get('code')}: {resp_ow.get('msg')}")

    async def _place_order_v2(self, sym: str, side: str, trade_side: str,
                               qty: float, hold_side: str) -> dict:
        path = "/api/v2/mix/order/place-order"
        payload = self._build_payload_v2(sym, side, trade_side, qty, hold_side)
        logger.info(f"[{self.symbol}] 📤 order [v2]: {payload}")
        resp = await self._http_post(path, payload)
        logger.info(f"[{self.symbol}] 📥 response [v2]: {resp}")
        if resp.get("code") == "00000":
            order_id = (resp.get("data") or {}).get("orderId", "?")
            logger.info(
                f"[{self.symbol}] ✅ {side}/{trade_side} tradeSide={trade_side} "
                f"qty={qty} orderId={order_id} [v2]"
            )
            return resp
        raise Exception(f"place-order {resp.get('code')}: {resp.get('msg')}")

    # ─────────────────────────────────────────────────────────────
    # POSICIONES
    # ─────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list:
        sym = self._bitget_symbol(self.symbol)
        try:
            path = f"/api/v3/position/single-position?symbol={sym}&category=USDT-FUTURES&marginCoin=USDT"
            r = await self._http_get(path)
            if r.get("code") == "00000":
                data = r.get("data") or []
                if isinstance(data, dict):
                    data = [data]
                result = [p for p in data if float(p.get("total") or p.get("size", 0)) > 0]
                if result:
                    return result
        except Exception as e:
            logger.warning(f"[{self.symbol}] get_positions v3: {e}")
        # Fallback v2
        try:
            sym_v2 = sym + "_UMCBL"
            path_v2 = f"/api/v2/mix/position/single-position?symbol={sym_v2}&productType=USDT-FUTURES&marginCoin=USDT"
            r2 = await self._http_get(path_v2)
            if r2.get("code") == "00000":
                data2 = r2.get("data") or []
                if isinstance(data2, dict):
                    data2 = [data2]
                result2 = [p for p in data2 if float(p.get("total") or p.get("size", 0)) > 0]
                if result2:
                    return result2
        except Exception as e2:
            logger.warning(f"[{self.symbol}] get_positions v2: {e2}")
        try:
            positions = await self.exchange.fetch_positions([self.symbol])
            return [p for p in positions if float(p.get("contracts") or 0) > 0]
        except Exception as e3:
            logger.warning(f"[{self.symbol}] get_positions ccxt: {e3}")
        return []

    # ─────────────────────────────────────────────────────────────
    # INIT
    # ─────────────────────────────────────────────────────────────

    async def _init(self, usdt_amount: float):
        await self.exchange.load_markets()
        saved = load_position(self.symbol)
        if saved:
            self.position    = saved["position"]
            self.entry_price = saved["entry_price"]
            self.sl          = saved.get("sl")
            self.tp1         = saved.get("tp1")
            self.tp2         = saved.get("tp2")
            self.tp3         = saved.get("tp3")
            self.tp2_hit     = saved.get("tp2_hit", False)
            self.usdt_amount = saved.get("usdt_amount", usdt_amount)
            logger.warning(
                f"[{self.symbol}] ♻️  Estado recuperado: "
                f"{self.position} @ {self.entry_price} | "
                f"SL={self.sl} TP1={self.tp1} TP2={self.tp2} TP3={self.tp3}"
            )
        else:
            self.usdt_amount = usdt_amount
        mode = "🧪 DRY" if self.dry_run else "💰 REAL"
        logger.info(f"✅ [{self.symbol}] Listo | x{self.leverage} | {self.margin_mode.upper()} | {mode}")

    # ─────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────

    async def get_balance(self):
        if self.dry_run:
            return 1000.0
        return await self._get_balance_direct()

    async def get_price(self):
        t = await self.exchange.fetch_ticker(self.symbol)
        return float(t["last"])

    async def fetch_ohlcv(self, timeframe="15m", limit=100):
        return await self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=limit)

    # ─────────────────────────────────────────────────────────────
    # OPEN / CLOSE helpers
    # ─────────────────────────────────────────────────────────────

    async def _open_order(self, side: str, usdt_amount: float):
        price = await self.get_price()
        qty   = round((usdt_amount * self.leverage) / price, 4)
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] OPEN {side.upper()} {qty} @ {price}")
            return
        hold_side = "long" if side == "buy" else "short"
        await self._place_order(side, "open", qty, hold_side=hold_side)

    async def _close_order(self, pos_side: str, qty: float):
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] CLOSE {pos_side.upper()} {qty}")
            return
        close_side = "sell" if pos_side == "long" else "buy"
        await self._place_order(close_side, "close", qty, hold_side=pos_side)

    async def _partial_close_order(self, pos_side: str, ratio: float):
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] PARTIAL CLOSE {ratio*100:.0f}% {pos_side.upper()}")
            return
        try:
            positions = await self._get_positions()
            for p in positions:
                size = float(p.get("total") or p.get("contracts") or p.get("size", 0))
                if size > 0:
                    partial_qty = round(size * ratio, 4)
                    hold_side   = str(p.get("holdSide") or p.get("side") or "").lower()
                    ps          = "long" if hold_side in ("long", "buy") else "short"
                    await self._close_order(ps, partial_qty)
                    logger.info(f"[{self.symbol}] TP parcial {ratio*100:.0f}% ({partial_qty} contratos)")
                    break
        except Exception as e:
            logger.error(f"[{self.symbol}] Partial close error: {e}")

    # ─────────────────────────────────────────────────────────────
    # SINCRONIZACIÓN WEBHOOK
    # ─────────────────────────────────────────────────────────────

    async def _sync_closed_from_exchange(self, fill_price: float, reason: str):
        if not self.position:
            return
        entry = self.entry_price or fill_price
        pnl   = 0.0
        if fill_price and entry:
            pnl = ((fill_price - entry) / entry * 100 * self.leverage
                   if self.position == "long" else
                   (entry - fill_price) / entry * 100 * self.leverage)
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1
        logger.warning(f"[Webhook][{self.symbol}] Cerrado | {reason} | PnL: {pnl:+.2f}%")
        await notify_close(self.symbol, self.position, entry, fill_price, pnl, reason, self.dry_run)
        self.position = self.entry_price = self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

    # ─────────────────────────────────────────────────────────────
    # ABRIR POSICIÓN
    # ─────────────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None):
        await self._open_order("buy", usdt_amount)
        self.position    = "long"
        self.entry_price = await self.get_price()
        self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
        self.tp2_hit = False
        self.usdt_amount = usdt_amount
        self.trade_count += 1
        save_position(self.symbol, self.position, self.entry_price,
                      sl, tp1, tp2, tp3, usdt_amount, self.leverage)
        logger.warning(f"📈 [{self.symbol}] LONG @ {self.entry_price} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}")
        await notify_open(self.symbol, "long", self.entry_price, self.leverage, usdt_amount, self.dry_run)

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None):
        await self._open_order("sell", usdt_amount)
        self.position    = "short"
        self.entry_price = await self.get_price()
        self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
        self.tp2_hit = False
        self.usdt_amount = usdt_amount
        self.trade_count += 1
        save_position(self.symbol, self.position, self.entry_price,
                      sl, tp1, tp2, tp3, usdt_amount, self.leverage)
        logger.warning(f"📉 [{self.symbol}] SHORT @ {self.entry_price} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}")
        await notify_open(self.symbol, "short", self.entry_price, self.leverage, usdt_amount, self.dry_run)

    # ─────────────────────────────────────────────────────────────
    # SL / TP CHECK
    # ─────────────────────────────────────────────────────────────

    async def _check_and_handle_sl_tp(self, price: float, risk, global_risk) -> bool:
        if not self.position or not self.entry_price:
            return False
        is_long = self.position == "long"

        if self.sl:
            sl_hit = (price <= self.sl) if is_long else (price >= self.sl)
            if sl_hit:
                result = await self.close_position(f"SL @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if self.tp2 and not self.tp2_hit:
            tp2_hit = (price >= self.tp2) if is_long else (price <= self.tp2)
            if tp2_hit:
                logger.warning(f"[{self.symbol}] ✂️  TP2 parcial @ {price:.4f}")
                await self._partial_close_order(self.position, TP2_PARTIAL_RATIO)
                self.tp2_hit = True
                mark_tp2_hit(self.symbol)
                self.sl = self.entry_price
                try:
                    from bot.telegram_bot import notify_tp_partial
                    await notify_tp_partial(self.symbol, self.position, price, 2, TP2_PARTIAL_RATIO)
                except Exception:
                    pass
                return False

        if self.tp3:
            tp3_hit = (price >= self.tp3) if is_long else (price <= self.tp3)
            if tp3_hit:
                result = await self.close_position(f"TP3 @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if self.tp1 and not self.tp2:
            tp1_hit = (price >= self.tp1) if is_long else (price <= self.tp1)
            if tp1_hit:
                result = await self.close_position(f"TP1 @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if not self.sl and not self.tp1:
            pnl = ((price - self.entry_price) / self.entry_price * 100 * self.leverage
                   if is_long else
                   (self.entry_price - price) / self.entry_price * 100 * self.leverage)
            tp_pct = float(os.getenv("AI_TP_PCT",  "3.0"))
            sl_pct = float(os.getenv("AI_SL_PCT", "-1.5"))
            if pnl >= tp_pct or pnl <= sl_pct:
                tag = f"TP +{pnl:.2f}%" if pnl >= tp_pct else f"SL {pnl:.2f}%"
                result = await self.close_position(tag)
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))
                return True
        return False

    # ─────────────────────────────────────────────────────────────
    # CERRAR POSICIÓN
    # ─────────────────────────────────────────────────────────────

    async def close_position(self, reason=""):
        if not self.position:
            return {}
        price = await self.get_price()
        if not self.dry_run:
            try:
                positions = await self._get_positions()
                for p in positions:
                    size      = float(p.get("total") or p.get("contracts") or p.get("size", 0))
                    hold_side = str(p.get("holdSide") or p.get("side") or "").lower()
                    if size > 0:
                        ps = "long" if hold_side in ("long", "buy") else "short"
                        await self._close_order(ps, size)
                        break
            except Exception as e:
                logger.error(f"[{self.symbol}] Close error: {e}")

        pnl = ((price - self.entry_price) / self.entry_price * 100 * self.leverage
               if self.position == "long" else
               (self.entry_price - price) / self.entry_price * 100 * self.leverage)
        self.total_pnl += pnl
        if pnl > 0: self.win_count += 1
        wr = self.win_count / self.trade_count * 100 if self.trade_count else 0
        logger.warning(f"🔒 [{self.symbol}] {self.position.upper()} cerrado | {reason} | PnL: {pnl:+.2f}% | WR: {wr:.1f}%")
        await notify_close(self.symbol, self.position, self.entry_price, price, pnl, reason, self.dry_run)
        result = {"side": self.position, "entry": self.entry_price, "exit": price,
                  "pnl_pct": round(pnl, 2), "reason": reason}
        self.position = self.entry_price = self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)
        return result

    # ─────────────────────────────────────────────────────────────
    # LOOP PRINCIPAL
    # ─────────────────────────────────────────────────────────────

    async def run(self, risk, global_risk=None):
        await self._init(risk.usdt_per_trade)
        interval = int(os.getenv("LOOP_INTERVAL", "60"))
        usdt = risk.usdt_per_trade
        tf   = os.getenv("TIMEFRAME", "15m")

        while True:
            try:
                price = await self.get_price()

                if self.position:
                    closed = await self._check_and_handle_sl_tp(price, risk, global_risk)
                    if closed:
                        await asyncio.sleep(interval)
                        continue

                async def _ai_fn(sym, ctx):
                    bars = await self.fetch_ohlcv(tf, limit=100)
                    return (await ai_decide(
                        sym, bars, self.position, self.entry_price, self.leverage,
                        context_override=ctx,
                    ))["action"]

                decision = await decide(
                    exch=self.exchange, symbol=self.symbol,
                    ai_decide_fn=_ai_fn,
                    has_open_position=self.position is not None,
                    current_pnl=None,
                )

                action = decision["action"]
                sig    = decision["signal"]
                reason = decision["reason"]

                if action == "CLOSE" and self.position:
                    result = await self.close_position(reason[:60])
                    risk.on_trade_close(result.get("pnl_pct", 0))
                    if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))

                elif action == "BUY":
                    if self.position == "short":
                        result = await self.close_position("Regresión → LONG")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))
                    if self.position is None:
                        bal = await self.get_balance()
                        can_l, r1 = risk.can_open_trade(bal)
                        can_g, r2 = (True, "OK") if not global_risk else await global_risk.can_open()
                        if can_l and can_g:
                            await self.open_long(usdt,
                                sl=sig.sl if sig else None, tp1=sig.tp1 if sig else None,
                                tp2=sig.tp2 if sig else None, tp3=sig.tp3 if sig else None)
                            risk.on_trade_open(self.entry_price, "long")
                            if global_risk: await global_risk.register_open()
                        else:
                            logger.info(f"[{self.symbol}] ⛔ {r1 if not can_l else r2}")

                elif action == "SELL":
                    if self.position == "long":
                        result = await self.close_position("Regresión → SHORT")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk: await global_risk.register_close(result.get("pnl_pct", 0))
                    if self.position is None:
                        bal = await self.get_balance()
                        can_l, r1 = risk.can_open_trade(bal)
                        can_g, r2 = (True, "OK") if not global_risk else await global_risk.can_open()
                        if can_l and can_g:
                            await self.open_short(usdt,
                                sl=sig.sl if sig else None, tp1=sig.tp1 if sig else None,
                                tp2=sig.tp2 if sig else None, tp3=sig.tp3 if sig else None)
                            risk.on_trade_open(self.entry_price, "short")
                            if global_risk: await global_risk.register_open()
                        else:
                            logger.info(f"[{self.symbol}] ⛔ {r1 if not can_l else r2}")

            except ccxt.NetworkError as e:
                logger.error(f"[{self.symbol}] Red: {e}")
                await asyncio.sleep(60)
                continue
            except ccxt.ExchangeError as e:
                logger.error(f"[{self.symbol}] Exchange: {e}")
                await asyncio.sleep(30)
                continue
            except Exception as e:
                logger.exception(f"[{self.symbol}] Error: {e}")
                await asyncio.sleep(30)
                continue

            await asyncio.sleep(interval)

    async def close(self):
        try:
            await self.exchange.close()
        except Exception:
            pass
