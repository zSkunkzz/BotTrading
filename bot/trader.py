import asyncio
import hashlib
import hmac
import json
import logging
import os
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
        self._trader_ref = self

    # ─────────────────────────────────────────────────────────────
    # BITGET UNIFIED — llamadas REST directas
    # ─────────────────────────────────────────────────────────────

    def _bitget_sign(self, timestamp: str, method: str, path: str, body: str) -> str:
        msg = timestamp + method.upper() + path + body
        return hmac.new(
            self._api_secret.encode("utf-8"),
            msg.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _bitget_get(self, path: str, params: dict) -> dict:
        """GET autenticado contra la API Bitget."""
        qs        = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        full_path = f"{path}?{qs}" if qs else path
        ts        = str(int(time.time() * 1000))
        signature = self._bitget_sign(ts, "GET", full_path, "")
        headers   = {
            "ACCESS-KEY":        self._api_key,
            "ACCESS-SIGN":       signature,
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }
        url = "https://api.bitget.com" + full_path
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                return await resp.json()

    async def _set_leverage_direct(self):
        """
        Intenta setear leverage via API directa.
        En Unified Account puede devolver code=40085 (no soportado) — es esperado,
        no es un error real: el leverage ya fue configurado manualmente en la cuenta.
        """
        base_symbol = self.symbol.split("/")[0] + "USDT"
        payload = {
            "symbol":      base_symbol,
            "productType": "USDT-FUTURES",
            "marginCoin":  "USDT",
            "leverage":    str(self.leverage),
        }
        body      = json.dumps(payload, separators=(",", ":"))
        path      = "/api/v2/mix/account/set-leverage"
        ts        = str(int(time.time() * 1000))
        signature = self._bitget_sign(ts, "POST", path, body)
        headers   = {
            "ACCESS-KEY":        self._api_key,
            "ACCESS-SIGN":       signature,
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }
        url = "https://api.bitget.com" + path
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=body) as resp:
                data = await resp.json()
                code = str(data.get("code", "0"))
                if code == "00000":
                    logger.debug(f"[{self.symbol}] Leverage x{self.leverage} OK")
                elif code == "40085":
                    # Unified Account no soporta este endpoint — es normal, no logear como warning
                    logger.debug(f"[{self.symbol}] Leverage skip (Unified Account mode)")
                else:
                    logger.warning(
                        f"[{self.symbol}] set_leverage code={code} msg={data.get('msg')}"
                    )

    async def _get_balance_direct(self) -> float:
        """
        Balance USDT via Unified Account API.
        Intento 1: /api/v2/unified/account/assets
        Intento 2: /api/v2/mix/account/accounts (fallback)
        """
        # Intento 1: Unified
        try:
            data = await self._bitget_get(
                "/api/v2/unified/account/assets",
                {"assetType": "USDT"}
            )
            if str(data.get("code")) == "00000":
                assets = data.get("data") or []
                if isinstance(assets, list):
                    for asset in assets:
                        if asset.get("coin", "").upper() == "USDT":
                            free = asset.get("available") or asset.get("availableAmount") or 0
                            return float(free)
                elif isinstance(assets, dict):
                    free = assets.get("available") or assets.get("availableAmount") or 0
                    return float(free)
        except Exception as e:
            logger.debug(f"[{self.symbol}] Balance unified error: {e}")

        # Intento 2: Mix/futures
        try:
            data = await self._bitget_get(
                "/api/v2/mix/account/accounts",
                {"productType": "USDT-FUTURES"}
            )
            if str(data.get("code")) == "00000":
                accounts = data.get("data") or []
                for acc in accounts:
                    if acc.get("marginCoin", "").upper() == "USDT":
                        free = acc.get("available") or acc.get("availableAmount") or 0
                        return float(free)
        except Exception as e:
            logger.debug(f"[{self.symbol}] Balance mix error: {e}")

        logger.warning(f"[{self.symbol}] No se pudo obtener balance — devolviendo 0")
        return 0.0

    # ─────────────────────────────────────────────────────────────
    # INIT + RECUPERACIÓN DE ESTADO
    # ─────────────────────────────────────────────────────────────

    async def _init(self, usdt_amount: float):
        await self.exchange.load_markets()
        if not self.dry_run:
            try:
                await self._set_leverage_direct()
            except Exception as e:
                logger.debug(f"[{self.symbol}] Leverage skip: {e}")

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
        try:
            return await self._get_balance_direct()
        except Exception as e:
            logger.error(f"[{self.symbol}] Balance error: {e}")
            return 0

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
                        params={
                            "reduceOnly":  True,
                            "productType": "USDT-FUTURES",
                        }
                    )
                    logger.info(
                        f"[{self.symbol}] TP parcial ejecutado: "
                        f"{ratio*100:.0f}% ({partial_qty} contratos)"
                    )
                    break
        except Exception as e:
            logger.error(f"[{self.symbol}] Partial close error: {e}")

    # ─────────────────────────────────────────────────────────────
    # SINCRONIZACIÓN DESDE WEBHOOK (cierre externo)
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
        logger.warning(
            f"[Webhook] [{self.symbol}] Posición cerrada | "
            f"{reason} | PnL: {pnl:+.2f}%"
        )
        await notify_close(
            self.symbol, self.position, entry,
            fill_price, pnl, reason, self.dry_run
        )
        self.position    = None
        self.entry_price = None
        self.sl          = None
        self.tp1         = None
        self.tp2         = None
        self.tp3         = None
        self.tp2_hit     = False
        clear_position(self.symbol)

    # ─────────────────────────────────────────────────────────────
    # ABRIR POSICIÓN
    # ─────────────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None):
        await self._open_order("buy", usdt_amount)
        self.position    = "long"
        self.entry_price = await self.get_price()
        self.sl          = sl
        self.tp1         = tp1
        self.tp2         = tp2
        self.tp3         = tp3
        self.tp2_hit     = False
        self.usdt_amount = usdt_amount
        self.trade_count += 1
        save_position(self.symbol, self.position, self.entry_price,
                      self.sl, self.tp1, self.tp2, self.tp3, usdt_amount, self.leverage)
        logger.warning(f"📈 [{self.symbol}] LONG @ {self.entry_price} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}")
        await notify_open(self.symbol, "long", self.entry_price,
                          self.leverage, usdt_amount, self.dry_run)

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None):
        await self._open_order("sell", usdt_amount)
        self.position    = "short"
        self.entry_price = await self.get_price()
        self.sl          = sl
        self.tp1         = tp1
        self.tp2         = tp2
        self.tp3         = tp3
        self.tp2_hit     = False
        self.usdt_amount = usdt_amount
        self.trade_count += 1
        save_position(self.symbol, self.position, self.entry_price,
                      self.sl, self.tp1, self.tp2, self.tp3, usdt_amount, self.leverage)
        logger.warning(f"📉 [{self.symbol}] SHORT @ {self.entry_price} | SL={sl} TP1={tp1} TP2={tp2} TP3={tp3}")
        await notify_open(self.symbol, "short", self.entry_price,
                          self.leverage, usdt_amount, self.dry_run)

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
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if self.tp2 and not self.tp2_hit:
            tp2_hit = (price >= self.tp2) if is_long else (price <= self.tp2)
            if tp2_hit:
                logger.warning(f"[{self.symbol}] ✂️  TP2 parcial @ {price:.4f}")
                await self._partial_close_order(
                    "sell" if is_long else "buy", TP2_PARTIAL_RATIO
                )
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
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if self.tp1 and not self.tp2:
            tp1_hit = (price >= self.tp1) if is_long else (price <= self.tp1)
            if tp1_hit:
                result = await self.close_position(f"TP1 @ {price:.4f}")
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        if not self.sl and not self.tp1:
            if is_long:
                pnl = (price - self.entry_price) / self.entry_price * 100 * self.leverage
            else:
                pnl = (self.entry_price - price) / self.entry_price * 100 * self.leverage
            tp_pct = float(os.getenv("AI_TP_PCT",  "3.0"))
            sl_pct = float(os.getenv("AI_SL_PCT", "-1.5"))
            if pnl >= tp_pct or pnl <= sl_pct:
                tag = f"TP +{pnl:.2f}%" if pnl >= tp_pct else f"SL {pnl:.2f}%"
                result = await self.close_position(tag)
                risk.on_trade_close(result.get("pnl_pct", 0))
                if global_risk:
                    await global_risk.register_close(result.get("pnl_pct", 0))
                return True

        return False

    # ─────────────────────────────────────────────────────────────
    # CERRAR POSICIÓN (completo)
    # ─────────────────────────────────────────────────────────────

    async def close_position(self, reason=""):
        if not self.position:
            return {}
        price = await self.get_price()
        if not self.dry_run:
            try:
                positions = await self.exchange.fetch_positions(
                    [self.symbol],
                    params={"productType": "USDT-FUTURES"}
                )
                for p in positions:
                    contracts = float(p.get("contracts") or p.get("size", 0))
                    if contracts > 0:
                        side = "sell" if p["side"] == "long" else "buy"
                        await self.exchange.create_order(
                            self.symbol, "market", side, contracts,
                            params={
                                "reduceOnly":  True,
                                "productType": "USDT-FUTURES",
                            }
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
            f"🔒 [{self.symbol}] {self.position.upper()} cerrado | {reason} | "
            f"PnL: {pnl:+.2f}% | WR: {wr:.1f}%"
        )
        await notify_close(self.symbol, self.position, self.entry_price,
                           price, pnl, reason, self.dry_run)
        result = {"side": self.position, "entry": self.entry_price,
                  "exit": price, "pnl_pct": round(pnl, 2), "reason": reason}
        self.position    = None
        self.entry_price = None
        self.sl          = None
        self.tp1         = None
        self.tp2         = None
        self.tp3         = None
        self.tp2_hit     = False
        clear_position(self.symbol)
        return result

    # ─────────────────────────────────────────────────────────────
    # LOOP PRINCIPAL
    # ─────────────────────────────────────────────────────────────

    async def run(self, risk, global_risk=None):
        await self._init(risk.usdt_per_trade)
        interval = int(os.getenv("LOOP_INTERVAL", "60"))
        usdt     = risk.usdt_per_trade
        tf       = os.getenv("TIMEFRAME", "15m")

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
                        sym, bars,
                        self.position, self.entry_price, self.leverage,
                        context_override=ctx,
                    ))["action"]

                decision = await decide(
                    exch              = self.exchange,
                    symbol            = self.symbol,
                    ai_decide_fn      = _ai_fn,
                    has_open_position = self.position is not None,
                    current_pnl       = None,
                )

                action = decision["action"]
                sig    = decision["signal"]
                reason = decision["reason"]

                if action == "CLOSE" and self.position:
                    result = await self.close_position(reason[:60])
                    risk.on_trade_close(result.get("pnl_pct", 0))
                    if global_risk:
                        await global_risk.register_close(result.get("pnl_pct", 0))

                elif action == "BUY":
                    if self.position == "short":
                        result = await self.close_position("Regresión → LONG")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk:
                            await global_risk.register_close(result.get("pnl_pct", 0))
                    if self.position is None:
                        bal = await self.get_balance()
                        can_l, r1 = risk.can_open_trade(bal)
                        can_g, r2 = (True, "OK") if not global_risk else await global_risk.can_open()
                        if can_l and can_g:
                            await self.open_long(
                                usdt,
                                sl  = sig.sl   if sig else None,
                                tp1 = sig.tp1  if sig else None,
                                tp2 = sig.tp2  if sig else None,
                                tp3 = sig.tp3  if sig else None,
                            )
                            risk.on_trade_open(self.entry_price, "long")
                            if global_risk:
                                await global_risk.register_open()
                        else:
                            block_reason = r1 if not can_l else r2
                            logger.info(f"[{self.symbol}] ⛔ {block_reason}")

                elif action == "SELL":
                    if self.position == "long":
                        result = await self.close_position("Regresión → SHORT")
                        risk.on_trade_close(result.get("pnl_pct", 0))
                        if global_risk:
                            await global_risk.register_close(result.get("pnl_pct", 0))
                    if self.position is None:
                        bal = await self.get_balance()
                        can_l, r1 = risk.can_open_trade(bal)
                        can_g, r2 = (True, "OK") if not global_risk else await global_risk.can_open()
                        if can_l and can_g:
                            await self.open_short(
                                usdt,
                                sl  = sig.sl   if sig else None,
                                tp1 = sig.tp1  if sig else None,
                                tp2 = sig.tp2  if sig else None,
                                tp3 = sig.tp3  if sig else None,
                            )
                            risk.on_trade_open(self.entry_price, "short")
                            if global_risk:
                                await global_risk.register_open()
                        else:
                            block_reason = r1 if not can_l else r2
                            logger.info(f"[{self.symbol}] ⛔ {block_reason}")

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
