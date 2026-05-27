import asyncio
import logging
import os
import hmac
import hashlib
import json
import time
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


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol       = symbol
        self.leverage     = leverage
        self.margin_mode  = margin_mode
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
        self.exchange = ccxt.bitget({
            "apiKey":   api_key,
            "secret":   api_secret,
            "password": passphrase,
            "options":  {
                "defaultType": "swap",
                "accountType": "unified",
            },
        })

    # ─────────────────────────────────────────────────────────────
    # BALANCE — Unified Account REST directa (sin ccxt fetch_balance)
    # La Unified Account de Bitget NO soporta /api/v2/mix/account/accounts
    # El endpoint correcto es /api/v2/unified/account/assets
    # ─────────────────────────────────────────────────────────────

    def _sign(self, ts: str, method: str, path: str, body: str = "") -> str:
        msg = ts + method.upper() + path + body
        return hmac.new(
            self._api_secret.encode(),
            msg.encode(),
            hashlib.sha256,
        ).hexdigest()

    async def _get_balance_direct(self) -> float:
        """
        Obtiene balance USDT disponible en Unified Account.
        Endpoint: GET /api/v2/unified/account/assets
        """
        path = "/api/v2/unified/account/assets"
        ts   = str(int(time.time() * 1000))
        # Sin query params para obtener todos los assets
        sig  = self._sign(ts, "GET", path)
        headers = {
            "ACCESS-KEY":        self._api_key,
            "ACCESS-SIGN":       sig,
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.bitget.com" + path,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()

            code = str(data.get("code", ""))
            if code != "00000":
                logger.warning(f"[{self.symbol}] Balance unified error: code={code} msg={data.get('msg')}")
                return 0.0

            assets = data.get("data") or []
            # data puede ser lista o dict
            if isinstance(assets, dict):
                assets = [assets]
            for asset in assets:
                coin = (asset.get("coin") or asset.get("coinName") or "").upper()
                if coin == "USDT":
                    free = float(
                        asset.get("available")
                        or asset.get("availableAmount")
                        or asset.get("available_amount")
                        or 0
                    )
                    logger.debug(f"[{self.symbol}] Balance USDT disponible: {free}")
                    return free

            logger.warning(f"[{self.symbol}] USDT no encontrado en assets: {assets}")
            return 0.0

        except Exception as e:
            logger.warning(f"[{self.symbol}] _get_balance_direct error: {e}")
            return 0.0

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
    # EXCHANGE HELPERS
    # ─────────────────────────────────────────────────────────────

    async def get_balance(self):
        if self.dry_run:
            return 1000.0
        bal = await self._get_balance_direct()
        if bal > 0:
            return bal
        logger.warning(f"[{self.symbol}] Balance = 0 — revisa permisos API Key (necesita 'Read' en Unified Account)")
        return 0.0

    async def get_price(self):
        t = await self.exchange.fetch_ticker(self.symbol)
        return float(t["last"])

    async def fetch_ohlcv(self, timeframe="15m", limit=100):
        return await self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=limit)

    async def _open_order(self, side, usdt_amount):
        price = await self.get_price()
        qty   = round((usdt_amount * self.leverage) / price, 4)
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] {side.upper()} {qty} @ {price}")
            return {"id": f"DRY_{side}", "price": price}
        return await self.exchange.create_order(
            self.symbol, "market", side, qty,
            params={
                "reduceOnly":  False,
                "marginMode":  self.margin_mode,
                "productType": "USDT-FUTURES",
            }
        )

    async def _partial_close_order(self, side, ratio: float):
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] PARTIAL CLOSE {ratio*100:.0f}% {side.upper()}")
            return
        try:
            positions = await self.exchange.fetch_positions(
                [self.symbol],
                params={"productType": "USDT-FUTURES"}
            )
            for p in positions:
                contracts = float(p.get("contracts") or p.get("size", 0))
                if contracts > 0:
                    partial_qty = round(contracts * ratio, 4)
                    close_side  = "sell" if p["side"] == "long" else "buy"
                    await self.exchange.create_order(
                        self.symbol, "market", close_side, partial_qty,
                        params={"reduceOnly": True, "productType": "USDT-FUTURES"}
                    )
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
        if fill_price and entry:
            if self.position == "long":
                pnl = (fill_price - entry) / entry * 100 * self.leverage
            else:
                pnl = (entry - fill_price) / entry * 100 * self.leverage
        else:
            pnl = 0.0
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1
        logger.warning(f"[Webhook][{self.symbol}] Cerrado | {reason} | PnL: {pnl:+.2f}%")
        await notify_close(self.symbol, self.position, entry, fill_price, pnl, reason, self.dry_run)
        self.position = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
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
                positions = await self.exchange.fetch_positions(
                    [self.symbol], params={"productType": "USDT-FUTURES"}
                )
                for p in positions:
                    contracts = float(p.get("contracts") or p.get("size", 0))
                    if contracts > 0:
                        side = "sell" if p["side"] == "long" else "buy"
                        await self.exchange.create_order(
                            self.symbol, "market", side, contracts,
                            params={"reduceOnly": True, "productType": "USDT-FUTURES"}
                        )
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
        self.position = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
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
        await self.exchange.close()
