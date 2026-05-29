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

logger = logging.getLogger("Trader")

TP2_PARTIAL_RATIO = float(os.getenv("TP2_PARTIAL_RATIO", "0.5"))

OHLCV_TF        = os.getenv("OHLCV_TF", "15m")
OHLCV_LIMIT     = int(os.getenv("OHLCV_LIMIT", "200"))
OHLCV_MIN_BARS  = int(os.getenv("OHLCV_MIN_BARS", "55"))

_BALANCE_MAX_RETRIES = int(os.getenv("BALANCE_MAX_RETRIES", "5"))
_BALANCE_RETRY_SLEEP = float(os.getenv("BALANCE_RETRY_SLEEP", "3"))

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

_min_qty_cache: dict = {}


async def _safe_json(response) -> dict:
    text = await response.text()
    stripped = text.strip()
    if not stripped.startswith("{") and not stripped.startswith("["):
        raise ValueError(f"Respuesta no-JSON: {stripped[:200]}")
    try:
        data = _json.loads(stripped)
    except _json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido: {e} — contenido: {stripped[:200]}")
    if not isinstance(data, dict):
        raise ValueError(f"Respuesta inesperada (tipo {type(data).__name__}): {str(data)[:300]}")
    return data


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


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

    # ── HTTP HELPERS ──────────────────────────────────────────────────────────

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
                return await _safe_json(r)

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
                return await _safe_json(r)

    # ── INICIALIZACIÓN ────────────────────────────────────────────────────────

    async def _init(self, usdt_per_trade: float):
        # Registrar credenciales en el servicio singleton (idempotente)
        balance_svc.init(self._api_key, self._api_secret, self._passphrase)

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
            logger.info(f"[{self.symbol}] 🔄 Posición restaurada: {self.position} @ {self.entry_price}")
        await self._detect_account_type()

    async def _detect_account_type(self):
        # Probe mix/accounts (más fiable en UA para futuros)
        try:
            r = await self._http_get(
                "/api/v2/mix/account/accounts",
                {"productType": "USDT-FUTURES"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                try:
                    rp = await self._http_get(
                        "/api/v2/mix/position/all-position",
                        {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
                    )
                    if rp.get("code") == "00000":
                        items = rp.get("data") or []
                        self._ua_pos_mode = items[0].get("holdMode", "hedge") if items else "hedge"
                    else:
                        self._ua_pos_mode = "hedge"
                except Exception:
                    self._ua_pos_mode = "hedge"
                logger.info(
                    f"[{self.symbol}] ✅ Unified Account (UA) via mix/accounts. pos_mode={self._ua_pos_mode}"
                )
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] UA mix/accounts probe error: {e}")

        # Probe spot (fallback universal)
        try:
            r = await self._http_get(
                "/api/v2/spot/account/assets",
                {"coin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "ua"
                self._ua_pos_mode = "hedge"
                logger.info(f"[{self.symbol}] ✅ Cuenta detectada via spot/assets.")
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] spot probe error: {e}")

        # Probe Classic v2
        try:
            sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
            r = await self._http_get(
                "/api/v2/mix/account/account",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                self._api_version = "v2"
                d = r.get("data") or {}
                d = d if isinstance(d, dict) else {}
                self._v2_pos_mode = d.get("holdMode", "hedge")
                logger.info(f"[{self.symbol}] ✅ Classic Account (v2). pos_mode={self._v2_pos_mode}")
                return
        except Exception as e:
            logger.debug(f"[{self.symbol}] v2 probe error: {e}")

        logger.warning(f"[{self.symbol}] ⚠️ Tipo de cuenta no detectado, asumiendo UA.")
        self._api_version = "ua"
        self._ua_pos_mode = "hedge"

    # ── PRECIO Y OHLCV ────────────────────────────────────────────────────────

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
                    logger.debug(f"[{self.symbol}] OHLCV desde WS ({len(bars)} velas)")
                    return bars
        except Exception as e:
            logger.debug(f"[{self.symbol}] get_ohlcv WS error: {e}")

        tf_ccxt = {"15m": "15m", "1h": "1h", "4h": "4h"}.get(tf, tf)
        logger.debug(f"[{self.symbol}] OHLCV fallback REST ({tf_ccxt})")
        bars = await self.exchange.fetch_ohlcv(self.symbol, tf_ccxt, limit=OHLCV_LIMIT)
        return bars

    # ── BALANCE (delegado al singleton) ───────────────────────────────────────

    async def get_balance(self) -> float | None:
        """Todos los traders comparten el mismo fetch via balance_svc."""
        return await balance_svc.get()

    # ── LEVERAGE ──────────────────────────────────────────────────────────────

    async def set_leverage(self, leverage: int, side: str | None = None):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        endpoint = "/api/v2/mix/account/set-leverage"
        pos_mode = self._ua_pos_mode or self._v2_pos_mode or "hedge"
        sides = ["long", "short"] if pos_mode == "hedge" else [side or "long"]

        for hold_side in sides:
            try:
                payload = {
                    "symbol":      sym_clean,
                    "productType": "USDT-FUTURES",
                    "marginCoin":  "USDT",
                    "leverage":    str(leverage),
                    "holdSide":    hold_side,
                }
                r = await self._http_post(endpoint, payload)
                if r.get("code") == "00000":
                    logger.debug(f"[{self.symbol}] Leverage {leverage}x ({hold_side}) OK")
                else:
                    logger.warning(
                        f"[{self.symbol}] set_leverage {hold_side} "
                        f"code={r.get('code')} msg={r.get('msg')}"
                    )
            except Exception as e:
                logger.warning(f"[{self.symbol}] set_leverage error: {e}")

    # ── MÍNIMOS DE QTY ────────────────────────────────────────────────────────

    async def _get_min_qty(self) -> float:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
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

    # ── POSICIONES ABIERTAS ───────────────────────────────────────────────────

    async def _get_positions(self) -> list | None:
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")

        try:
            r = await self._http_get(
                "/api/v2/mix/position/single-position",
                {"symbol": sym_clean, "productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and float(p.get("total") or p.get("contracts") or
                              p.get("size", 0)) > 0
                ]
            else:
                logger.debug(
                    f"[{self.symbol}] positions: code={r.get('code')} msg={r.get('msg')}"
                )
        except Exception as e:
            logger.debug(f"[{self.symbol}] positions error: {e}")

        try:
            r = await self._http_get(
                "/api/v2/mix/position/all-position",
                {"productType": "USDT-FUTURES", "marginCoin": "USDT"}
            )
            if r.get("code") == "00000":
                data = r.get("data") or []
                data = data if isinstance(data, list) else []
                return [
                    p for p in data
                    if isinstance(p, dict)
                    and p.get("symbol") == sym_clean
                    and float(p.get("total") or p.get("contracts") or
                              p.get("size", 0)) > 0
                ]
        except Exception as e:
            logger.debug(f"[{self.symbol}] all-positions error: {e}")

        logger.warning(f"[{self.symbol}] ⚠️ _get_positions falló — estado local preservado")
        return None

    # ── COLOCAR / CERRAR ÓRDENES ──────────────────────────────────────────────

    async def _place_order(self, side: str, trade_side: str, qty: float):
        sym_clean = self.symbol.replace("/", "").replace(":USDT", "")
        endpoint = "/api/v2/mix/order/place-order"
        pos_mode = self._ua_pos_mode or self._v2_pos_mode or "hedge"

        def _build_payload(mode: str) -> dict:
            p = {
                "symbol":      sym_clean,
                "productType": "USDT-FUTURES",
                "marginMode":  self.margin_mode,
                "marginCoin":  "USDT",
                "qty":         str(qty),
                "orderType":   "market",
                "side":        side,
            }
            if mode == "hedge":
                p["tradeSide"] = trade_side
            return p

        if self.dry_run:
            logger.info(f"[{self.symbol}] 🟡 DRY RUN: {side}/{trade_side} qty={qty}")
            return {"code": "00000", "data": {"orderId": "dry"}}

        payload = _build_payload(pos_mode)
        try:
            r = await self._http_post(endpoint, payload)
            if r.get("code") == "00000":
                # Invalidar caché de balance tras ejecutar una orden
                balance_svc.invalidate()
                return r
            if pos_mode == "hedge" and r.get("code") in ("40786", "40787", "40788"):
                logger.warning(
                    f"[{self.symbol}] Hedge order failed ({r.get('code')}), retrying one-way"
                )
                r2 = await self._http_post(endpoint, _build_payload("one_way"))
                if r2.get("code") == "00000":
                    balance_svc.invalidate()
                    return r2
            logger.error(
                f"[{self.symbol}] Order failed: code={r.get('code')} msg={r.get('msg')}"
            )
            return r
        except Exception as e:
            logger.error(f"[{self.symbol}] _place_order exception: {e}")
            return {"code": "ERROR", "msg": str(e)}

    async def _calc_qty(self, usdt_amount: float, price: float, leverage: int) -> float:
        effective_lev = leverage or self.leverage
        raw_qty = (usdt_amount * effective_lev) / price
        min_qty = await self._get_min_qty()
        qty = max(min_qty, round(raw_qty / min_qty) * min_qty)
        decimals = len(str(min_qty).rstrip("0").split(".")[-1]) if "." in str(min_qty) else 0
        qty = round(qty, decimals)
        return qty

    # ── ABRIR POSICIONES ──────────────────────────────────────────────────────

    async def open_long(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                        leverage=None):
        price = await self.get_price()
        lev   = leverage or self.leverage
        qty   = await self._calc_qty(usdt_amount, price, lev)
        await self.set_leverage(lev, side="long")
        r = await self._place_order("buy", "open", qty)
        if r.get("code") == "00000":
            self.position    = "long"
            self.entry_price = price
            self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
            self.tp2_hit = False
            save_position(self.symbol, "long", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(
                f"🟢 [{self.symbol}] LONG abierto @ {price:.4f} | "
                f"lev={lev}x | sl={sl} tp1={tp1} tp2={tp2} tp3={tp3}"
            )
            await notify_open(
                self.symbol, "long", price, lev,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run
            )
        else:
            logger.error(
                f"[{self.symbol}] open_long FAILED: code={r.get('code')} msg={r.get('msg')}"
            )

    async def open_short(self, usdt_amount, sl=None, tp1=None, tp2=None, tp3=None,
                         leverage=None):
        price = await self.get_price()
        lev   = leverage or self.leverage
        qty   = await self._calc_qty(usdt_amount, price, lev)
        await self.set_leverage(lev, side="short")
        r = await self._place_order("sell", "open", qty)
        if r.get("code") == "00000":
            self.position    = "short"
            self.entry_price = price
            self.sl = sl; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3
            self.tp2_hit = False
            save_position(self.symbol, "short", price, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3)
            logger.warning(
                f"🔴 [{self.symbol}] SHORT abierto @ {price:.4f} | "
                f"lev={lev}x | sl={sl} tp1={tp1} tp2={tp2} tp3={tp3}"
            )
            await notify_open(
                self.symbol, "short", price, lev,
                sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, dry_run=self.dry_run
            )
        else:
            logger.error(
                f"[{self.symbol}] open_short FAILED: code={r.get('code')} msg={r.get('msg')}"
            )

    async def close_position(self, reason: str = ""):
        if not self.position:
            return
        side       = "sell" if self.position == "long" else "buy"
        trade_side = "close"
        qty = None
        try:
            positions = await self._get_positions()
            if positions:
                qty = float(
                    positions[0].get("total") or
                    positions[0].get("contracts") or
                    positions[0].get("size") or 0
                )
        except Exception:
            pass

        if not qty or qty <= 0:
            logger.warning(f"[{self.symbol}] close_position: qty no disponible, usando 0")
            qty = 0

        exit_price = await self.get_price()
        pnl = 0.0
        if self.entry_price and exit_price:
            if self.position == "long":
                pnl = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                pnl = (self.entry_price - exit_price) / self.entry_price * 100

        if qty > 0:
            r = await self._place_order(side, trade_side, qty)
            if r.get("code") != "00000":
                logger.error(
                    f"[{self.symbol}] close_position FAILED: code={r.get('code')} msg={r.get('msg')}"
                )
                return

        old_pos = self.position
        self.position    = None
        self.entry_price = None
        self.sl = self.tp1 = self.tp2 = self.tp3 = None
        self.tp2_hit = False
        clear_position(self.symbol)

        if pnl >= 0:
            self.win_count += 1
        self.trade_count += 1
        self.total_pnl   += pnl

        logger.warning(
            f"[{self.symbol}] 🟡 {old_pos.upper()} cerrado | razón={reason} | "
            f"pnl={pnl:+.2f}% | trades={self.trade_count} wins={self.win_count}"
        )
        await notify_close(
            self.symbol, old_pos, exit_price, pnl,
            reason=reason, dry_run=self.dry_run
        )

    async def partial_close(self, ratio: float = 0.5):
        if not self.position:
            return
        side       = "sell" if self.position == "long" else "buy"
        trade_side = "close"
        qty = None
        try:
            positions = await self._get_positions()
            if positions:
                total = float(
                    positions[0].get("total") or
                    positions[0].get("contracts") or
                    positions[0].get("size") or 0
                )
                min_qty = await self._get_min_qty()
                qty = max(min_qty, round((total * ratio) / min_qty) * min_qty)
        except Exception as e:
            logger.warning(f"[{self.symbol}] partial_close: {e}")
            return

        if not qty or qty <= 0:
            return

        r = await self._place_order(side, trade_side, qty)
        if r.get("code") == "00000":
            mark_tp2_hit(self.symbol)
            self.tp2_hit = True
            exit_price = await self.get_price()
            await notify_tp_partial(
                self.symbol, self.position, exit_price,
                ratio=ratio, dry_run=self.dry_run
            )
            logger.info(f"[{self.symbol}] ✂️ Cierre parcial {int(ratio*100)}% ejecutado")
        else:
            logger.warning(
                f"[{self.symbol}] partial_close FAILED: code={r.get('code')} msg={r.get('msg')}"
            )

    # ── LOOP PRINCIPAL ────────────────────────────────────────────────────────

    async def run(self, risk: "RiskManager", global_risk: "GlobalRisk" = None):
        from bot.risk import RiskManager
        usdt_per_trade = risk.usdt_per_trade
        await self._init(usdt_per_trade)

        # Espera inicial escalonada para que no todos hagan fetch a la vez
        await asyncio.sleep(0.5)

        while True:
            try:
                price = await self.get_price()

                # ── Balance via singleton ──────────────────────────────────────
                balance = await self.get_balance()

                if balance is None or balance <= 0:
                    if not self._balance_ok:
                        logger.warning(
                            f"[{self.symbol}] ⚠️ Balance={balance or 0:.2f} USDT — "
                            f"esperando {_BALANCE_RETRY_SLEEP * 2:.0f}s"
                        )
                    await asyncio.sleep(_BALANCE_RETRY_SLEEP * 2)
                    continue

                if not self._balance_ok:
                    self._balance_ok = True
                    logger.info(f"[{self.symbol}] ✅ Balance confirmado: {balance:.2f} USDT")

                # ── Gestión de posición abierta ────────────────────────────────
                if self.position:
                    if not self.tp2_hit and self.tp2:
                        if (self.position == "long"  and price >= self.tp2) or \
                           (self.position == "short" and price <= self.tp2):
                            await self.partial_close(ratio=TP2_PARTIAL_RATIO)

                    if self.sl and self.tp3:
                        hit_sl  = (self.position == "long"  and price <= self.sl) or \
                                  (self.position == "short" and price >= self.sl)
                        hit_tp3 = (self.position == "long"  and price >= self.tp3) or \
                                  (self.position == "short" and price <= self.tp3)
                        if hit_sl:
                            await self.close_position(reason="SL")
                            risk.on_trade_close(pnl_pct=-risk.sl_pct)
                        elif hit_tp3:
                            await self.close_position(reason="TP3")
                            risk.on_trade_close(pnl_pct=risk.tp_pct)

                    await asyncio.sleep(2)
                    continue

                # ── Sin posición: verificar risk ───────────────────────────────
                can_trade, reason = risk.can_open_trade(balance)
                if not can_trade:
                    logger.debug(f"[{self.symbol}] RiskManager bloqueó trade: {reason}")
                    await asyncio.sleep(2)
                    continue

                if global_risk and not global_risk.can_open_trade():
                    await asyncio.sleep(2)
                    continue

                bars = await self.get_ohlcv()

                if not bars or len(bars) < OHLCV_MIN_BARS:
                    logger.debug(
                        f"[{self.symbol}] Esperando candles WS "
                        f"({len(bars) if bars else 0}/{OHLCV_MIN_BARS})"
                    )
                    await asyncio.sleep(2)
                    continue

                decision = await ai_decide(
                    symbol=self.symbol,
                    bars=bars,
                    position=self.position,
                    entry_price=self.entry_price,
                    leverage=self.leverage,
                )

                if decision.get("action") in ("LONG", "SHORT", "BUY", "SELL"):
                    action = decision["action"]
                    safe_balance = balance if (balance is not None and balance > 0) else usdt_per_trade
                    usdt_amount  = min(usdt_per_trade, safe_balance * 0.95)

                    lev  = decision.get("leverage", self.leverage)
                    sl   = decision.get("sl")
                    tp1  = decision.get("tp1")
                    tp2  = decision.get("tp2")
                    tp3  = decision.get("tp3")

                    if global_risk:
                        global_risk.register_open_trade()

                    if action in ("LONG", "BUY"):
                        await self.open_long(usdt_amount, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)
                        if self.position:
                            risk.on_trade_open(self.entry_price, "long")
                    else:
                        await self.open_short(usdt_amount, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3, leverage=lev)
                        if self.position:
                            risk.on_trade_open(self.entry_price, "short")

                    if global_risk:
                        global_risk.register_close_trade()

                elif decision.get("action") == "CLOSE" and self.position:
                    await self.close_position(reason=decision.get("reasoning", "IA-CLOSE"))
                    risk.on_trade_close(pnl_pct=0.0)

            except asyncio.CancelledError:
                logger.info(f"[{self.symbol}] Trader cancelado.")
                break
            except Exception as e:
                logger.error(f"[{self.symbol}] run() error: {e}")

            await asyncio.sleep(2)
