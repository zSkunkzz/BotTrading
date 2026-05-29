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
from bot.telegram_bot import notify_tp_partial
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.execution_engine import execution_engine

logger = logging.getLogger("Trader")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

OHLCV_TF        = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT     = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS  = int(os.getenv("OHLCV_MIN_BARS", "55"))

_MIN_QTY_FALLBACK = {
    "BTCUSDT":   0.001,
    "ETHUSDT":   0.01,
    "SOLUSDT":   0.1,
    "XRPUSDT":   1.0,
    "SUIUSDT":   1.0,
    "NEARUSDT":  0.1,
    "XLMUSDT":   1.0,
    "XAUUSDT":   0.01,
    "XAUTUSDT":  0.001,
    "XAGUSDT":   0.1,
    "HYPEUSDT":  0.1,
    "FILOUSDT":  0.1,
    "FILUSDT":   0.1,
    "SOXLUSDT":  0.1,
    "ZECUSDT":   0.01,
    "WLDUSDT":   0.1,
    "BEATUSDT":  1.0,
    "BZUSDT":    1.0,
    "TAOUSDT":   0.001,
    "ADAUSDT":   1.0,
    "DOGEUSDTUSDT": 1.0,
    "BCHUSDT":   0.01,
    "DOGEUSDT":  1.0,
    "ALOUSDT":   1.0,
}

_min_qty_cache = {}


class FuturesTrader:
    def __init__(self, api_key, api_secret, passphrase, symbol,
                 leverage, margin_mode, dry_run):
        self.symbol       = symbol
        self.leverage     = leverage
        self.margin_mode  = margin_mode or "isolated"
        self.dry_run      = dry_run
        self._api_key     = api_key
        self._api_secret  = api_secret
        self._passphrase  = passphrase
        self.position     = None
        self.entry_price  = None
        self.sl           = None
        self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit      = False
        self.trade_count  = 0
        self.win_count    = 0
        self.total_pnl    = 0.0
        self.exchange     = None
        self._api_version = None
        self._ua_pos_mode = None
        self._v2_pos_mode = None
        self._balance_ok  = False
        # ── Commit 1: protective orders server-side ──────────────────────────────
        self.sl_order_id    = None
        self.tp_order_id    = None
        self._protection_ok = False
        # ── Commit 2: notional abierto para pretrade_risk ─────────────────────
        self._open_notional = 0.0

    # ── HTTP helpers ──────────────────────────────────────────────────────────

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

    async def _http_get(self, path: str, params: dict | None = None) -> dict:
        qs = ""
        if params:
            qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        url = "https://api.bitget.com" + path + qs
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=self._headers("GET", path + qs),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                return await self._safe_json(r)

    async def _http_post(self, path: str, payload: dict) -> dict:
        body = _json.dumps(payload)
        url  = "https://api.bitget.com" + path
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                headers=self._headers("POST", path, body),
                data=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                return await self._safe_json(r)

    @staticmethod
    async def _safe_json(response) -> dict:
        text = await response.text()
        stripped = text.strip()
        if not stripped.startswith(("{" , "[")):
            raise ValueError(f"Respuesta no-JSON: {stripped[:200]}")
        try:
            data = _json.loads(stripped)
        except _json.JSONDecodeError as e:
            raise ValueError(f"JSON inválido: {e} — contenido: {stripped[:200]}")
        if not isinstance(data, dict):
            raise ValueError(f"Respuesta inesperada: {str(data)[:300]}")
        return data

    # ── Inicialización ────────────────────────────────────────────────────────

    async def _init(self, usdt_per_trade: float):
        self.exchange = ccxt.bitget({
            "apiKey":     self._api_key,
            "secret":     self._api_secret,
            "password":   self._passphrase,
            "options":    {"defaultType": "swap"},
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
            self._protection_ok = True
            logger.info(f"[{self.symbol}] 🔄 Posición restaurada: {self.position} @ {self.entry_price}")

        if not balance_svc.is_ready():
            logger.warning(f"[{self.symbol}] ⚠️ balance_svc no listo — init desde trader")
            balance_svc.init(self._api_key, self._api_secret, self._passphrase)

        self._api_version = "ua"
        self._ua_pos_mode = "single_hold"
        logger.info(f"[{self.symbol}] ✅ Forzado modo Unified Account (single_hold)")

    # ── Precio, OHLCV y balance ───────────────────────────────────────────────

    async def get_price(self) -> float:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.is_price_fresh(sym_clean):
                price = ws_feed.get_price(sym_clean)
                if price and price > 0:
                    return price
        except Exception:
            pass
        ticker = await self.exchange.fetch_ticker(self.symbol)
        return float(ticker["last"])

    async def get_ohlcv(self, tf: str = OHLCV_TF) -> list:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        try:
            from bot.ws_feed import ws_feed
            if ws_feed.has_data(sym_clean, tf=tf, min_candles=OHLCV_MIN_BARS):
                df = ws_feed.get_ohlcv(sym_clean, tf)
                if not df.empty and len(df) >= OHLCV_MIN_BARS:
                    df_reset = df.reset_index()
                    bars = [
                        [
                            int(row["ts"].timestamp() * 1000),
                            float(row["open"]),
                            float(row["high"]),
                            float(row["low"]),
                            float(row["close"]),
                            float(row["volume"]),
                        ]
                        for _, row in df_reset.iterrows()
                    ]
                    return bars
        except Exception as e:
            logger.debug(f"[{self.symbol}] get_ohlcv WS error: {e}")

        tf_ccxt = {"15m": "15m", "1h": "1h", "4h": "4h"}.get(tf, tf)
        bars = await self.exchange.fetch_ohlcv(self.symbol, tf_ccxt, limit=OHLCV_LIMIT)
        return bars

    async def get_balance(self) -> float | None:
        return await balance_svc.get()

    # ── Leverage (V3 UA /mix) ─────────────────────────────────────────────────

    async def set_leverage(self, leverage: int, side: str | None = None):
        """Ajusta el apalancamiento usando el endpoint V3 mix de Unified Account.
        Endpoint: POST /api/v3/mix/account/set-leverage
        """
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        payload = {
            "symbol":      sym_clean,
            "productType": "USDT-FUTURES",
            "marginCoin":  "USDT",
            "leverage":    str(leverage),
        }
        try:
            r = await self._http_post("/api/v3/mix/account/set-leverage", payload)
            if r.get("code") == "00000":
                logger.debug(f"[{self.symbol}] Leverage {leverage}x (V3 mix) OK")
            else:
                logger.warning(f"[{self.symbol}] set_leverage V3 error: {r}")
        except Exception as e:
            logger.warning(f"[{self.symbol}] set_leverage V3 exception: {e}")

    # ── Mínimos de qty ────────────────────────────────────────────────────────

    async def _get_min_qty(self) -> float:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        if sym_clean in _min_qty_cache:
            return _min_qty_cache[sym_clean]
        try:
            r = await self._http_get(
                "/api/v2/mix/market/contracts",
                {"symbol": sym_clean, "productType": "USDT-FUTURES"}
            )
            if r.get("code") == "00000":
                items = r.get("data") or []
                items = items if isinstance(items, list) else []
                if items:
                    min_qty = float(
                        items[0].get("minTradeNum") or
                        items[0].get("minOrderSize") or 0.001
                    )
                    _min_qty_cache[sym_clean] = min_qty
                    return min_qty
        except Exception as e:
            logger.debug(f"[{self.symbol}] _get_min_qty error: {e}")
        fallback = _MIN_QTY_FALLBACK.get(sym_clean, 0.001)
        _min_qty_cache[sym_clean] = fallback
        return fallback

    # ── Posiciones abiertas (V3 mix UA) ──────────────────────────────────────

    async def _get_positions(self) -> list | None:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        try:
            r = await self._http_get(
                "/api/v3/mix/position/single-position",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and float(p.get("total") or p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug(f"[{self.symbol}] positions V3 mix error: {e}")

        try:
            r = await self._http_get(
                "/api/v3/mix/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and p.get("symbol") == sym_clean
                    and float(p.get("total") or p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug(f"[{self.symbol}] all-positions V3 mix error: {e}")

        logger.warning(f"[{self.symbol}] ⚠️ _get_positions falló")
        return None

    # ── Protective orders server-side (COMMIT 1) ──────────────────────────────

    async def _place_pos_tpsl(self, sl: float | None = None, tp: float | None = None) -> dict:
        if not self.position:
            return {"code": "NO_POSITION", "msg": "No hay posición abierta"}
        if not sl and not tp:
            return {"code": "NO_TPSL", "msg": "Sin niveles de protección"}

        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        hold_side = "long" if self.position == "long" else "short"
        payload = {
            "symbol":      sym_clean,
            "productType": "USDT-FUTURES",
            "marginCoin":  "USDT",
            "holdSide":    hold_side,
        }
        if tp:
            payload["stopSurplusTriggerPrice"] = str(tp)
            payload["stopSurplusTriggerType"]  = "mark_price"
            payload["stopSurplusExecutePrice"] = "0"
        if sl:
            payload["stopLossTriggerPrice"]    = str(sl)
            payload["stopLossTriggerType"]     = "mark_price"
            payload["stopLossExecutePrice"]    = "0"

        if self.dry_run:
            logger.info(f"[{self.symbol}] 🟡 DRY RUN TPSL: sl={sl} tp={tp}")
            self.sl_order_id = "dry-sl"
            self.tp_order_id = "dry-tp"
            return {"code": "00000", "data": {"orderId": "dry-tpsl"}}

        try:
            r = await self._http_post("/api/v3/mix/order/place-tpsl", payload)
            if r.get("code") == "00000":
                data = r.get("data") or {}
                item = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else {})
                self.sl_order_id = item.get("stopLossClientOid") or item.get("orderId")
                self.tp_order_id = item.get("stopSurplusClientOid") or item.get("orderId")
                logger.info(f"[{self.symbol}] 🛡️ TPSL server-side OK — SL={self.sl_order_id} TP={self.tp_order_id}")
            else:
                logger.error(f"[{self.symbol}] ❌ TPSL server-side FAILED: {r}")
            return r
        except Exception as e:
            logger.error(f"[{self.symbol}] _place_pos_tpsl exception: {e}")
            return {"code": "ERROR", "msg": str(e)}

    async def reconcile_position(self) -> bool:
        try:
            positions = await self._get_positions()
            has_pos   = bool(positions)
            sl_covered = bool(self.sl_order_id) or (self.sl is None)
            tp_covered = bool(self.tp_order_id) or (self.tp3 is None)
            self._protection_ok = has_pos and sl_covered and tp_covered

            if not has_pos:
                logger.error(f"[{self.symbol}] ❌ Reconcile: posición no encontrada en exchange")
                await kill_switch.on_state_mismatch(self.symbol)
            elif not (sl_covered and tp_covered):
                logger.error(
                    f"[{self.symbol}] ❌ Reconcile: faltan órdenes TPSL "
                    f"(sl_ok={sl_covered} tp_ok={tp_covered})"
                )
            else:
                logger.info(f"[{self.symbol}] ✅ Reconcile OK")

            return self._protection_ok
        except Exception as e:
            self._protection_ok = False
            logger.error(f"[{self.symbol}] reconcile_position error: {e}")
            return False

    # ── Colocar / cerrar órdenes (V3 mix UA) ─────────────────────────────────

    async def _place_order_raw(
        self, side: str, qty: float,
        order_type: str = "market", price: float | None = None,
        trade_side: str = "open",
    ) -> dict:
        """
        Capa base de envío de órdenes al exchange.
        Usada por ExecutionEngine y por _place_order().
        No modifica estado interno: sólo envía y devuelve el resultado.
        Usa el endpoint V3 mix de Unified Account.

        trade_side: "open" para abrir posición, "close" para cerrarla.
        En modo single_hold de UA v3 este campo distingue apertura de cierre.
        """
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        # V3 mix UA order endpoint
        endpoint  = "/api/v3/mix/order/place-order"
        payload: dict = {
            "symbol":      sym_clean,
            "productType": "USDT-FUTURES",
            "marginMode":  self.margin_mode,
            "marginCoin":  "USDT",
            "size":        str(qty),
            "orderType":   order_type,
            "side":        side,
            "tradeSide":   trade_side,  # "open" o "close" — nunca "single_hold"
        }
        if order_type == "limit" and price is not None:
            payload["price"] = str(price)

        if self.dry_run:
            logger.info(f"[{self.symbol}] 🟡 DRY RUN RAW: {side} {order_type} qty={qty} price={price} tradeSide={trade_side}")
            return {"code": "00000", "data": {"orderId": "dry"}}

        try:
            return await self._http_post(endpoint, payload)
        except Exception as e:
            logger.error(f"[{self.symbol}] _place_order_raw exception: {e}")
            return {"code": "ERROR", "msg": str(e)}

    async def _get_order_status(self, order_id: str) -> dict:
        """Consulta el estado de una orden por su ID (V3 mix UA)."""
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        try:
            return await self._http_get(
                "/api/v3/mix/order/detail",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "orderId": order_id},
            )
        except Exception as e:
            logger.debug(f"[{self.symbol}] _get_order_status error: {e}")
            return {}

    async def _cancel_order(self, order_id: str) -> dict:
        """Cancela una orden por su ID (V3 mix UA)."""
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        if sym_clean.endswith("USDTUSDT"):
            sym_clean = sym_clean[:-4]
        try:
            return await self._http_post(
                "/api/v3/mix/order/cancel-order",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "orderId": order_id},
            )
        except Exception as e:
            logger.debug(f"[{self.symbol}] _cancel_order error: {e}")
            return {}

    async def _place_order(self, side: str, qty: float, trade_side: str = "open") -> dict:
        """
        Punto de entrada principal para abrir/cerrar posiciones.
        Delega en ExecutionEngine (limit→timeout→market) y mantiene
        todos los hooks de kill_switch y balance_svc.

        trade_side: "open" para abrir, "close" para cerrar.
        """
        # arrival price para el execution engine
        try:
            arrival_price = await self.get_price()
        except Exception:
            arrival_price = 0.0

        # ask/bid desde orderbook metrics
        ask = bid = None
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
            from bot.ws_feed import ws_feed
            ob = ws_feed.get_orderbook_metrics(sym_clean)
            if ob:
                ask = ob.get("ask")
                bid = ob.get("bid")
        except Exception:
            pass

        r = await execution_engine.execute(
            trader=self,
            side=side,
            qty=qty,
            arrival_price=arrival_price,
            ask=ask,
            bid=bid,
            trade_side=trade_side,
        )

        rejected = r.get("code") != "00000"
        await kill_switch.on_order_result(rejected=rejected)
        if not rejected:
            balance_svc.invalidate()
        else:
            logger.error(f"[{self.symbol}] Order failed: {r}")
        return r

    async def _calc_qty(self, usdt_amount: float, price: float, leverage: int) -> float:
        effective_lev = leverage or self.leverage
        raw_qty = (usdt_amount * effective_lev) / price
        min_qty = await self._get_min_qty()
        qty = max(min_qty, round(raw_qty / min_qty) * min_qty)
        decimals = len(str(min_qty).rstrip("0").split(".")[-1]) if "." in str(min_qty) else 0
        qty = round(qty, decimals)
        return qty

    # ── Abrir posiciones ──────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None, leverage=None):
        if kill_switch.is_halted(self.symbol):
            logger.warning(f"[{self.symbol}] 🛑 open_long bloqueado por KillSwitch L{kill_switch.level()}")
            return

        price  = await self.get_price()
        lev    = leverage or self.leverage
        qty    = await self._calc_qty(usdt_amount, price, lev)
        balance = await self.get_balance() or 0.0

        ok, reason = await pretrade_risk.check(
            symbol=self.symbol, side="buy", notional=usdt_amount,
            price=price, balance=balance, sl=sl,
        )
        if not ok:
            logger.warning(f"[{self.symbol}] 🚫 open_long bloqueado por PreTradeRisk: {reason}")
            return

        await self.set_leverage(lev, side="long")
        r = await self._place_order("buy", qty, trade_side="open")
        if r.get("code") == "00000":
            self.position    = "long"
            self.entry_price = price
            self.sl   = sl
            self.tp1  = tp1
            self.tp2  = tp2
            self.tp3  = tp3
            self.tp2_hit       = False
            self.sl_order_id   = None
            self.tp_order_id   = None
            self._protection_ok = False
            self._open_notional = usdt_amount
            save_position(self.symbol, "long", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(f"🟢 [{self.symbol}] LONG @ {price:.4f} lev={lev}x")
            await notify_open(self.symbol, "long", price, lev, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run)
            await self._place_pos_tpsl(sl=sl, tp=tp3)
            ok2 = await self.reconcile_position()
            if not ok2:
                logger.error(f"[{self.symbol}] ⚠️ Posición abierta pero sin protección confirmada")
        else:
            logger.error(f"[{self.symbol}] open_long FAILED: {r}")

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None, leverage=None):
        if kill_switch.is_halted(self.symbol):
            logger.warning(f"[{self.symbol}] 🛑 open_short bloqueado por KillSwitch L{kill_switch.level()}")
            return

        price  = await self.get_price()
        lev    = leverage or self.leverage
        qty    = await self._calc_qty(usdt_amount, price, lev)
        balance = await self.get_balance() or 0.0

        ok, reason = await pretrade_risk.check(
            symbol=self.symbol, side="sell", notional=usdt_amount,
            price=price, balance=balance, sl=sl,
        )
        if not ok:
            logger.warning(f"[{self.symbol}] 🚫 open_short bloqueado por PreTradeRisk: {reason}")
            return

        await self.set_leverage(lev, side="short")
        r = await self._place_order("sell", qty, trade_side="open")
        if r.get("code") == "00000":
            self.position    = "short"
            self.entry_price = price
            self.sl   = sl
            self.tp1  = tp1
            self.tp2  = tp2
            self.tp3  = tp3
            self.tp2_hit       = False
            self.sl_order_id   = None
            self.tp_order_id   = None
            self._protection_ok = False
            self._open_notional = usdt_amount
            save_position(self.symbol, "short", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(f"🔴 [{self.symbol}] SHORT @ {price:.4f} lev={lev}x")
            await notify_open(self.symbol, "short", price, lev, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run)
            await self._place_pos_tpsl(sl=sl, tp=tp3)
            ok2 = await self.reconcile_position()
            if not ok2:
                logger.error(f"[{self.symbol}] ⚠️ Posición abierta pero sin protección confirmada")
        else:
            logger.error(f"[{self.symbol}] open_short FAILED: {r}")

    async def close_position(self, reason: str = ""):
        if not self.position:
            return
        side = "sell" if self.position == "long" else "buy"
        qty  = None
        try:
            positions = await self._get_positions()
            if positions:
                qty = float(positions[0].get("total") or positions[0].get("size", 0))
        except Exception:
            pass

        if not qty or qty <= 0:
            qty = 0

        exit_price = await self.get_price()
        pnl = 0.0
        if self.entry_price and exit_price:
            if self.position == "long":
                pnl = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                pnl = (self.entry_price - exit_price) / self.entry_price * 100

        if qty > 0:
            r = await self._place_order(side, qty, trade_side="close")
            if r.get("code") != "00000":
                logger.error(f"[{self.symbol}] close_position FAILED: {r}")
                return

        old_pos = self.position
        pretrade_risk.register_close(self.symbol, self._open_notional)
        self._open_notional = 0.0

        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit      = False
        self.sl_order_id  = None
        self.tp_order_id  = None
        self._protection_ok = False
        clear_position(self.symbol)

        if pnl >= 0:
            self.win_count += 1
        self.trade_count += 1
        self.total_pnl   += pnl

        await kill_switch.on_trade_result(pnl)

        logger.warning(f"[{self.symbol}] 🟡 {old_pos.upper()} cerrado | razón={reason} | pnl={pnl:+.2f}%")
        await notify_close(self.symbol, old_pos, exit_price, pnl, reason=reason, dry_run=self.dry_run)

    async def partial_close(self, ratio: float = 0.5):
        if not self.position:
            return
        side = "sell" if self.position == "long" else "buy"
        qty  = None
        try:
            positions = await self._get_positions()
            if positions:
                total   = float(positions[0].get("total") or positions[0].get("size", 0))
                min_qty = await self._get_min_qty()
                qty     = max(min_qty, round((total * ratio) / min_qty) * min_qty)
        except Exception as e:
            logger.warning(f"[{self.symbol}] partial_close: {e}")
            return

        if not qty or qty <= 0:
            return

        r = await self._place_order(side, qty, trade_side="close")
        if r.get("code") == "00000":
            freed = self._open_notional * ratio
            pretrade_risk.register_close(self.symbol, freed)
            self._open_notional = max(0.0, self._open_notional - freed)

            mark_tp2_hit(self.symbol)
            self.tp2_hit = True
            exit_price   = await self.get_price()
            await notify_tp_partial(self.symbol, self.position, exit_price, ratio=ratio, dry_run=self.dry_run)
            logger.info(f"[{self.symbol}] ✂️ Cierre parcial {int(ratio*100)}%")
        else:
            logger.warning(f"[{self.symbol}] partial_close FAILED: {r}")

    # ── Loop principal ────────────────────────────────────────────────────────

    async def run(self, risk: "RiskManager", global_risk: "GlobalRisk" = None):
        from bot.risk import RiskManager
        usdt_per_trade = risk.usdt_per_trade
        await self._init(usdt_per_trade)

        while True:
            try:
                if kill_switch.is_hard_killed():
                    logger.critical(f"[{self.symbol}] 💥 L4 HARD KILL — cerrando posición si la hay")
                    if self.position:
                        await self.close_position(reason="KS-L4-HARD-KILL")
                    break

                price = await self.get_price()

                balance = await self.get_balance()
                if balance is None:
                    logger.warning(f"[{self.symbol}] ⏳ Balance no disponible, esperando 5s...")
                    await asyncio.sleep(5)
                    continue
                if balance <= 0:
                    logger.warning(f"[{self.symbol}] ⚠️ Balance {balance:.2f} USDT — insuficiente")
                    await asyncio.sleep(10)
                    continue

                if not self._balance_ok:
                    self._balance_ok = True
                    logger.info(f"[{self.symbol}] ✅ Balance confirmado: {balance:.2f} USDT")

                if self.position:
                    if not self._protection_ok:
                        logger.warning(
                            f"[{self.symbol}] ⚠️ Posición sin protección — reconciliando..."
                        )
                        await self.reconcile_position()
                        await asyncio.sleep(5)
                        continue

                    if not self.tp2_hit and self.tp2:
                        if (self.position == "long" and price >= self.tp2) or \
                           (self.position == "short" and price <= self.tp2):
                            await self.partial_close(ratio=TP2_PARTIAL_RATIO)

                    should_exit, exit_reason = risk.check_exit(price)

                    if not should_exit and self.sl and self.tp3:
                        hit_sl  = (self.position == "long"  and price <= self.sl)  or \
                                  (self.position == "short" and price >= self.sl)
                        hit_tp3 = (self.position == "long"  and price >= self.tp3) or \
                                  (self.position == "short" and price <= self.tp3)
                        if hit_sl:
                            should_exit = True
                            exit_reason = f"SL fijo {price:.4f}"
                        elif hit_tp3:
                            should_exit = True
                            exit_reason = f"TP3 fijo {price:.4f}"

                    if should_exit:
                        pnl_pct = 0.0
                        if self.entry_price:
                            if self.position == "long":
                                pnl_pct = (price - self.entry_price) / self.entry_price * 100
                            else:
                                pnl_pct = (self.entry_price - price) / self.entry_price * 100
                        if global_risk:
                            await global_risk.register_close(pnl_pct=pnl_pct)
                        await self.close_position(reason=exit_reason)
                        risk.on_trade_close(pnl_pct=pnl_pct)

                    await asyncio.sleep(2)
                    continue

                if kill_switch.is_halted(self.symbol):
                    logger.debug(f"[{self.symbol}] KS L{kill_switch.level()} — sin nuevas entradas")
                    await asyncio.sleep(5)
                    continue

                can_trade, reason = risk.can_open_trade(balance)
                if not can_trade:
                    logger.debug(f"[{self.symbol}] Risk bloquea: {reason}")
                    await asyncio.sleep(2)
                    continue

                if global_risk:
                    gr_ok, gr_reason = await global_risk.can_open()
                    if not gr_ok:
                        logger.debug(f"[{self.symbol}] GlobalRisk bloquea: {gr_reason}")
                        await asyncio.sleep(2)
                        continue

                bars = await self.get_ohlcv()
                if not bars or len(bars) < OHLCV_MIN_BARS:
                    await asyncio.sleep(2)
                    continue

                decision = await ai_decide(
                    symbol=self.symbol,
                    bars=bars,
                    position=self.position,
                    entry_price=self.entry_price,
                    leverage=self.leverage,
                )

                action = decision.get("action")
                if action in ("LONG", "SHORT", "BUY", "SELL"):
                    usdt_amount = min(usdt_per_trade, balance * 0.95)
                    lev  = decision.get("leverage", self.leverage)
                    sl   = decision.get("sl")
                    tp1  = decision.get("tp1")
                    tp2  = decision.get("tp2")
                    tp3  = decision.get("tp3")

                    if action in ("LONG", "BUY"):
                        await self.open_long(usdt_amount, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)
                        if self.position:
                            if global_risk:
                                await global_risk.register_open()
                            risk.on_trade_open(self.entry_price, "long")
                    else:
                        await self.open_short(usdt_amount, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)
                        if self.position:
                            if global_risk:
                                await global_risk.register_open()
                            risk.on_trade_open(self.entry_price, "short")

                elif action == "CLOSE" and self.position:
                    pnl_pct = 0.0
                    if self.entry_price:
                        if self.position == "long":
                            pnl_pct = (price - self.entry_price) / self.entry_price * 100
                        else:
                            pnl_pct = (self.entry_price - price) / self.entry_price * 100
                    if global_risk:
                        await global_risk.register_close(pnl_pct=pnl_pct)
                    await self.close_position(reason=decision.get("reasoning", "IA-CLOSE"))
                    risk.on_trade_close(pnl_pct=pnl_pct)

            except asyncio.CancelledError:
                logger.info(f"[{self.symbol}] Trader cancelado.")
                break
            except Exception as e:
                logger.error(f"[{self.symbol}] run() error: {e}")

            await asyncio.sleep(2)
