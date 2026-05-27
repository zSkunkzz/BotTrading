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
        self.margin_mode  = margin_mode or "crossed"
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
        # ccxt con opciones Unified Account
        self.exchange = ccxt.bitget({
            "apiKey":   api_key,
            "secret":   api_secret,
            "password": passphrase,
            "options":  {
                "defaultType": "swap",
                "defaultSubType": "linear",
                "accountType": "unified",
            },
        })

    # ─────────────────────────────────────────────────────────────
    # FIRMA HTTP DIRECTA (Base64 — requerido por Bitget v2 Unified)
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
        # Intento 1 — v3 assets (Unified)
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

        # Intento 2 — mix accounts
        try:
            r = await self._http_get("/api/v2/mix/account/accounts?productType=USDT-FUTURES")
            if r.get("code") == "00000":
                for acc in (r.get("data") or []):
                    coin = str(acc.get("marginCoin") or acc.get("coin") or "").upper()
                    if coin == "USDT":
                        free = float(acc.get("available") or acc.get("crossMaxAvailable") or 0)
                        if free > 0:
                            logger.info(f"[{self.symbol}] ✅ Balance USDT (mix/accounts): {free}")
                            return free
        except Exception as e:
            logger.warning(f"[{self.symbol}] balance mix: {e}")

        # Intento 3 — ccxt unified
        try:
            data = await self.exchange.fetch_balance({"accountType": "unified"})
            free = float((data.get("USDT") or {}).get("free") or 0)
            if free > 0:
                logger.info(f"[{self.symbol}] ✅ Balance USDT (ccxt/unified): {free}")
                return free
        except Exception as e:
            logger.warning(f"[{self.symbol}] balance ccxt: {e}")

        logger.warning(f"[{self.symbol}] ⚠️ Balance = 0 — todos los endpoints fallaron.")
        return 0.0

    # ─────────────────────────────────────────────────────────────
    # ÓRDENES — ccxt con params Unified Account
    #
    # ccxt.bitget.create_order acepta params extra que se pasan
    # directamente al body de la request v2:
    #   productType  → "USDT-FUTURES"
    #   marginMode   → "crossed" | "isolated"
    #   marginCoin   → "USDT"
    #   tradeSide    → "open" | "close"
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _bitget_symbol(ccxt_symbol: str) -> str:
        """Convierte 'BTC/USDT:USDT' → 'BTCUSDT'"""
        return ccxt_symbol.split("/")[0] + ccxt_symbol.split("/")[1].split(":")[0]

    async def _place_order(self, side: str, trade_side: str, qty: float,
                           reduce_only: bool = False) -> dict:
        """
        Coloca una orden de mercado via ccxt con params Unified Account.
        Si ccxt falla, intenta HTTP directo como fallback.
        side       : 'buy' | 'sell'
        trade_side : 'open' | 'close'
        qty        : cantidad en contratos
        """
        params = {
            "productType": "USDT-FUTURES",
            "marginMode":  self.margin_mode,
            "marginCoin":  "USDT",
            "tradeSide":   trade_side,
        }
        if reduce_only:
            params["reduceOnly"] = True

        # --- Intento 1: ccxt ---
        try:
            resp = await self.exchange.create_order(
                symbol=self.symbol,
                type="market",
                side=side,
                amount=qty,
                params=params,
            )
            order_id = resp.get("id") or resp.get("info", {}).get("orderId", "?")
            logger.info(
                f"[{self.symbol}] 📤 Orden ccxt {side}/{trade_side} qty={qty} | orderId={order_id}"
            )
            return resp
        except Exception as e:
            logger.warning(f"[{self.symbol}] ccxt create_order falló: {e} — intentando HTTP directo")

        # --- Intento 2: HTTP directo ---
        path = "/api/v2/mix/order/place-order"
        payload = {
            "symbol":      self._bitget_symbol(self.symbol),
            "productType": "USDT-FUTURES",
            "marginMode":  self.margin_mode,
            "marginCoin":  "USDT",
            "size":        str(qty),
            "side":        side,
            "tradeSide":   trade_side,
            "orderType":   "market",
        }
        logger.info(f"[{self.symbol}] 📤 HTTP payload: {payload}")
        resp2 = await self._http_post(path, payload)
        logger.info(f"[{self.symbol}] 📥 HTTP response: {resp2}")
        if resp2.get("code") != "00000":
            raise Exception(
                f"place-order HTTP error {resp2.get('code')}: {resp2.get('msg')}"
            )
        logger.info(
            f"[{self.symbol}] 📤 Orden HTTP {side}/{trade_side} qty={qty} "
            f"| orderId={resp2.get('data', {}).get('orderId')}"
        )
        return resp2

    # ─────────────────────────────────────────────────────────────
    # POSICIONES — HTTP directo
    # ─────────────────────────────────────────────────────────────

    async def _get_positions(self) -> list:
        sym   = self._bitget_symbol(self.symbol)
        path  = f"/api/v2/mix/position/single-position?symbol={sym}&productType=USDT-FUTURES&marginCoin=USDT"
        try:
            r = await self._http_get(path)
            if r.get("code") == "00000":
                data = r.get("data") or []
                if isinstance(data, dict):
                    data = [data]
                return [p for p in data if float(p.get("total") or p.get("size", 0)) > 0]
        except Exception as e:
            logger.warning(f"[{self.symbol}] get_positions: {e}")

        # Fallback ccxt
        try:
            positions = await self.exchange.fetch_positions([self.symbol])
            return [p for p in positions if float(p.get("contracts") or 0) > 0]
        except Exception as e2:
            logger.warning(f"[{self.symbol}] get_positions ccxt: {e2}")

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
        logger.info(f"✅ [{self.symbol}] Listo | x{self.leverage} | {mode}")

    # ─────────────────────────────────────────────────────────────
    # HELPERS PRECIO / OHLCV
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
    # OPEN / CLOSE
    # ─────────────────────────────────────────────────────────────

    async def _open_order(self, side: str, usdt_amount: float):
        price = await self.get_price()
        qty   = round((usdt_amount * self.leverage) / price, 4)
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] {side.upper()} {qty} @ {price}")
            return {"price": price}
        await self._place_order(side, "open", qty)
        return {"price": price}

    async def _partial_close_order(self, side: str, ratio: float):
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] PARTIAL CLOSE {ratio*100:.0f}% {side.upper()}")
            return
        try:
            positions = await self._get_positions()
            for p in positions:
                size = float(p.get("total") or p.get("contracts") or p.get("size", 0))
                if size > 0:
                    partial_qty  = round(size * ratio, 4)
                    pos_side     = str(p.get("holdSide") or p.get("side") or "").lower()
                    close_side   = "sell" if pos_side in ("long", "buy") else "buy"
                    await self._place_order(close_side, "close", partial_qty)
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
                await self._partial_close_order("sell" if is_long else "buy", TP2_PARTIAL_RATIO)
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
                    size     = float(p.get("total") or p.get("contracts") or p.get("size", 0))
                    pos_side = str(p.get("holdSide") or p.get("side") or "").lower()
                    if size > 0:
                        close_side = "sell" if pos_side in ("long", "buy") else "buy"
                        await self._place_order(close_side, "close", size)
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
