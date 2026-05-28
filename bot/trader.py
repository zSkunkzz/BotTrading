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

# Cache de posMode por symbol: 'hedge' | 'one_way'
# Se detecta automáticamente al primer intento fallido.
_pos_mode_cache: dict = {}

# Cache del posMode real de la cuenta (se consulta una vez al arranque)
_account_pos_mode: str | None = None


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
    # DETECTAR POSMODE REAL DE LA CUENTA
    # ─────────────────────────────────────────────────────────────

    async def _fetch_account_pos_mode(self) -> str:
        """
        Consulta el posMode real configurado en la cuenta de Bitget.
        Retorna 'hedge' o 'one_way'.
        La API devuelve: 'hedge_mode' | 'one_way_mode'
        """
        global _account_pos_mode
        if _account_pos_mode is not None:
            return _account_pos_mode
        try:
            r = await self._http_get("/api/v3/position/account-mode?productType=USDT-FUTURES")
            code = r.get("code")
            if code == "00000":
                raw = str((r.get("data") or {}).get("posMode") or "").lower()
                # Bitget devuelve: "hedge_mode" | "one_way_mode"
                mode = "hedge" if "hedge" in raw else "one_way"
                logger.info(f"[{self.symbol}] 🔍 posMode cuenta: {raw} → usando '{mode}'")
                _account_pos_mode = mode
                # Precarga el cache para este símbolo
                _pos_mode_cache[self.symbol] = mode
                return mode
            else:
                logger.warning(f"[{self.symbol}] ⚠️ account-mode error {code}: {r.get('msg')} — asumiendo one_way")
        except Exception as e:
            logger.warning(f"[{self.symbol}] ⚠️ account-mode excepción: {e} — asumiendo one_way")
        _account_pos_mode = "one_way"
        _pos_mode_cache[self.symbol] = "one_way"
        return "one_way"

    # ─────────────────────────────────────────────────────────────
    # ÓRDENES — Usa posMode real de la cuenta
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _bitget_symbol(ccxt_symbol: str) -> str:
        return ccxt_symbol.split("/")[0] + ccxt_symbol.split("/")[1].split(":")[0]

    def _pos_mode(self) -> str:
        """Devuelve 'hedge' o 'one_way' según caché (default: one_way)."""
        return _pos_mode_cache.get(self.symbol, "one_way")

    async def _place_order(self, side: str, trade_side: str, qty: float) -> dict:
        """
        side       : "buy" | "sell"
        trade_side : "open" | "close"
        qty        : cantidad en contratos

        Detecta posMode real de la cuenta en el primer intento.
        Si aun así recibe 25236 hace un único fallback al modo contrario.
        """
        # Asegurar que tenemos el posMode real antes del primer order
        await self._fetch_account_pos_mode()

        sym  = self._bitget_symbol(self.symbol)
        path = "/api/v3/trade/place-order"
        mode = self._pos_mode()
        is_close = (trade_side == "close")

        def build_payload(hedge: bool) -> dict:
            p = {
                "symbol":     sym,
                "category":   "USDT-FUTURES",
                "marginMode": self.margin_mode,
                "marginCoin": "USDT",
                "qty":        str(qty),
                "side":       side,
                "orderType":  "market",
            }
            if hedge:
                p["tradeSide"] = trade_side        # "open" | "close"
            else:
                if is_close:
                    p["reduceOnly"] = "YES"        # one-way cierre
            return p

        # Intento 1: según modo real de la cuenta
        payload = build_payload(hedge=(mode == "hedge"))
        logger.info(f"[{self.symbol}] 📤 order [{mode}]: {payload}")
        resp = await self._http_post(path, payload)
        logger.info(f"[{self.symbol}] 📥 response: {resp}")

        # 25236 → modo incorrecto, cambiar y reintentar una vez
        if resp.get("code") == "25236":
            new_mode = "one_way" if mode == "hedge" else "hedge"
            logger.warning(
                f"[{self.symbol}] ⚠️ 25236 con mode={mode} → "
                f"cambiando a {new_mode} y reintentando"
            )
            _pos_mode_cache[self.symbol] = new_mode
            global _account_pos_mode
            _account_pos_mode = new_mode  # actualizar también el global
            payload = build_payload(hedge=(new_mode == "hedge"))
            logger.info(f"[{self.symbol}] 📤 retry [{new_mode}]: {payload}")
            resp = await self._http_post(path, payload)
            logger.info(f"[{self.symbol}] 📥 retry response: {resp}")

        if resp.get("code") == "00000":
            order_id = (resp.get("data") or {}).get("orderId", "?")
            effective_mode = _pos_mode_cache.get(self.symbol, "one_way")
            logger.info(
                f"[{self.symbol}] ✅ {side}/{trade_side} qty={qty} "
                f"mode={effective_mode} marginMode={self.margin_mode} orderId={order_id}"
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
        # Detectar posMode real de la cuenta al arranque
        await self._fetch_account_pos_mode()
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
        await self._place_order(side, "open", qty)

    async def _close_order(self, pos_side: str, qty: float):
        if self.dry_run:
            logger.warning(f"[DRY][{self.symbol}] CLOSE {pos_side.upper()} {qty}")
            return
        # hedge : side opuesto + tradeSide=close
        # one_way: side opuesto + reduceOnly=YES (gestionado en _place_order)
        close_side = "sell" if pos_side == "long" else "buy"
        await self._place_order(close_side, "close", qty)

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
