import asyncio
import logging
import os
import ccxt.async_support as ccxt
from bot.ai_trader import ai_decide
from bot.telegram_bot import (
    notify_open, notify_close, notify_ai_decision, notify_risk_block
)

logger = logging.getLogger("Trader")


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol = symbol
        self.leverage = leverage
        self.margin_mode = margin_mode
        self.dry_run = dry_run
        self.position = None
        self.entry_price = None
        self.trade_count = 0
        self.win_count = 0
        self.total_pnl = 0.0
        self.exchange = ccxt.bitget({
            "apiKey": api_key,
            "secret": api_secret,
            "password": passphrase,
            "options": {"defaultType": "swap"},
        })

    async def _init(self):
        await self.exchange.load_markets()
        if not self.dry_run:
            try:
                await self.exchange.set_leverage(
                    self.leverage, self.symbol,
                    params={"marginMode": self.margin_mode}
                )
            except Exception as e:
                logger.warning(f"[{self.symbol}] Leverage: {e}")
        mode = "\U0001f9ea DRY" if self.dry_run else "\U0001f4b0 REAL"
        logger.info(f"\u2705 [{self.symbol}] Listo | x{self.leverage} | {mode}")

    async def get_balance(self):
        if self.dry_run:
            return 1000.0
        try:
            bal = await self.exchange.fetch_balance({"type": "swap"})
            return float(bal.get("USDT", {}).get("free", 0))
        except Exception as e:
            logger.error(f"[{self.symbol}] Balance: {e}")
            return 0

    async def get_price(self):
        t = await self.exchange.fetch_ticker(self.symbol)
        return float(t["last"])

    async def fetch_ohlcv(self, timeframe="15m", limit=100):
        # limit reducido de 200 a 100 — suficiente para todos los indicadores
        return await self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=limit)

    async def _open_order(self, side, usdt_amount):
        price = await self.get_price()
        qty = round((usdt_amount * self.leverage) / price, 4)
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] {side.upper()} {qty} @ {price}")
            return {"id": f"DRY_{side}", "price": price}
        return await self.exchange.create_order(
            self.symbol, "market", side, qty,
            params={"reduceOnly": False, "marginMode": self.margin_mode}
        )

    async def open_long(self, usdt_amount):
        await self._open_order("buy", usdt_amount)
        self.position = "long"
        self.entry_price = await self.get_price()
        self.trade_count += 1
        logger.warning(f"\U0001f4c8 [{self.symbol}] LONG abierto @ {self.entry_price}")
        await notify_open(self.symbol, "long", self.entry_price,
                          self.leverage, usdt_amount, self.dry_run)

    async def open_short(self, usdt_amount):
        await self._open_order("sell", usdt_amount)
        self.position = "short"
        self.entry_price = await self.get_price()
        self.trade_count += 1
        logger.warning(f"\U0001f4c9 [{self.symbol}] SHORT abierto @ {self.entry_price}")
        await notify_open(self.symbol, "short", self.entry_price,
                          self.leverage, usdt_amount, self.dry_run)

    async def close_position(self, reason=""):
        if not self.position:
            return {}
        price = await self.get_price()
        if not self.dry_run:
            try:
                positions = await self.exchange.fetch_positions([self.symbol])
                for p in positions:
                    contracts = float(p.get("contracts") or p.get("size", 0))
                    if contracts > 0:
                        side = "sell" if p["side"] == "long" else "buy"
                        await self.exchange.create_order(
                            self.symbol, "market", side, contracts,
                            params={"reduceOnly": True}
                        )
                        break
            except Exception as e:
                logger.error(f"[{self.symbol}] Close: {e}")
        if self.position == "long":
            pnl = (price - self.entry_price) / self.entry_price * 100 * self.leverage
        else:
            pnl = (self.entry_price - price) / self.entry_price * 100 * self.leverage
        self.total_pnl += pnl
        if pnl > 0:
            self.win_count += 1
        wr = self.win_count / self.trade_count * 100 if self.trade_count else 0
        logger.warning(
            f"\U0001f512 [{self.symbol}] {self.position.upper()} cerrado | {reason} | "
            f"PnL: {pnl:+.2f}% | WR: {wr:.1f}%"
        )
        await notify_close(self.symbol, self.position, self.entry_price,
                           price, pnl, reason, self.dry_run)
        result = {"side": self.position, "entry": self.entry_price,
                  "exit": price, "pnl_pct": round(pnl, 2), "reason": reason}
        self.position = None
        self.entry_price = None
        return result

    async def run(self, strategy, risk, global_risk=None):
        await self._init()
        interval = int(os.getenv("LOOP_INTERVAL", "60"))
        usdt = risk.usdt_per_trade
        tf = os.getenv("TIMEFRAME", "15m")
        while True:
            try:
                bars = await self.fetch_ohlcv(tf, limit=100)

                # ai_decide aplica la nueva lógica:
                # - Sin posición + HOLD técnico → 0 llamadas IA
                # - Con posición abierta → 0 llamadas IA (solo revisa PnL)
                # - Sin posición + señal clara → 1 llamada IA de confirmación
                decision = await ai_decide(
                    self.symbol, bars,
                    self.position, self.entry_price,
                    self.leverage
                )
                action     = decision["action"]
                confidence = decision.get("confidence", 5)
                reasoning  = decision.get("reasoning", "")

                # Solo notificar por Telegram si hay acción real (no HOLD)
                # Evita spam de notificaciones en cada ciclo
                if action != "HOLD":
                    await notify_ai_decision(self.symbol, action, confidence, reasoning)

                if action == "CLOSE" and self.position:
                    result = await self.close_position(f"{reasoning[:50]}")
                    risk.on_trade_close(result.get("pnl_pct", 0))
                    if global_risk:
                        await global_risk.register_close(result.get("pnl_pct", 0))

                elif action == "BUY":
                    if self.position == "short":
                        result = await self.close_position("Reversión → LONG")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk:
                            await global_risk.register_close(result.get("pnl_pct", 0))
                    if self.position is None:
                        bal = await self.get_balance()
                        can_l, r1 = risk.can_open_trade(bal)
                        can_g, r2 = (True, "OK") if not global_risk else await global_risk.can_open()
                        if can_l and can_g:
                            await self.open_long(usdt)
                            risk.on_trade_open(self.entry_price, "long")
                            if global_risk:
                                await global_risk.register_open()
                        else:
                            reason = r1 if not can_l else r2
                            logger.info(f"[{self.symbol}] ⛔ {reason}")
                            await notify_risk_block(self.symbol, reason)

                elif action == "SELL":
                    if self.position == "long":
                        result = await self.close_position("Reversión → SHORT")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk:
                            await global_risk.register_close(result.get("pnl_pct", 0))
                    if self.position is None:
                        bal = await self.get_balance()
                        can_l, r1 = risk.can_open_trade(bal)
                        can_g, r2 = (True, "OK") if not global_risk else await global_risk.can_open()
                        if can_l and can_g:
                            await self.open_short(usdt)
                            risk.on_trade_open(self.entry_price, "short")
                            if global_risk:
                                await global_risk.register_open()
                        else:
                            reason = r1 if not can_l else r2
                            logger.info(f"[{self.symbol}] ⛔ {reason}")
                            await notify_risk_block(self.symbol, reason)

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
