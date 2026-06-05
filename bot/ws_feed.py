"""
bot/ws_feed.py — WebSocket feed de OKX para precio, OHLCV y Order Book L2.

v2 — OKX migration (2026-06-06):
  Sustituye el WS de Hyperliquid por el WS público de OKX v5:
    wss://ws.okx.com:8443/ws/v5/public

  Canales suscritos por símbolo activo:
    • tickers        → precio last/bid/ask en tiempo real
    • books5         → top-5 bid/ask (order book ligero)
    • candle15m      → velas 15m
    • candle1H       → velas 1h
    • candle4H       → velas 4h

  Instrumento OKX: {COIN}-USDT-SWAP  (perpetuo USDT-margined)

  API pública idéntica a la versión anterior (compatible con signal_engine):
    from bot.ws_feed import ws_feed
    price  = ws_feed.get_price("BTC")
    df15   = ws_feed.get_ohlcv("BTC", "15m")
    ob     = ws_feed.get_orderbook_metrics("BTC")
    closed = ws_feed.is_candle_closed("BTC", "15m")
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Dict, List, Optional

import aiohttp
import pandas as pd

log = logging.getLogger("WSFeed")

# OKX WS público (no requiere autenticación para datos de mercado)
WS_URL         = "wss://ws.okx.com:8443/ws/v5/public"
PING_INTERVAL  = 25          # OKX requiere ping cada 30s; usamos 25 para margen
OHLCV_LIMIT    = 200
RECONNECT_BASE = 2.0
RECONNECT_MAX  = 60.0

# Mapa TF interno → canal OKX
TF_MAP: dict[str, str] = {
    "15m": "candle15m",
    "1h":  "candle1H",
    "4h":  "candle4H",
}

_TF_SECS: dict[str, float] = {
    "1m":  60,   "3m":  180,  "5m":  300,
    "15m": 900,  "30m": 1800, "1h":  3600,
    "2h":  7200, "4h":  14400, "6h": 21600,
    "12h": 43200, "1d": 86400,
}

OB_DEPTH = 5  # books5 solo tiene 5 niveles


def _norm(symbol: str) -> str:
    """BTCUSDT / BTC/USDT / BTC-USDT-SWAP → BTC"""
    s = symbol.upper().replace("/", "-")
    if s.endswith("-SWAP"):
        s = s[:-5]
    for suffix in ("-USDT", "USDT", "-USDC", "USDC"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    return s.split("-")[0]


def _inst_id(coin: str) -> str:
    """BTC → BTC-USDT-SWAP"""
    return f"{coin}-USDT-SWAP"


class _OrderBookCache:
    __slots__ = ("bids", "asks", "ts")

    def __init__(self):
        self.bids: list = []
        self.asks: list = []
        self.ts:   float = 0.0

    def update(self, bids: list, asks: list):
        self.bids = sorted(
            [[float(p), float(s)] for p, s, *_ in bids  if float(s) > 0],
            key=lambda x: -x[0]
        )[:OB_DEPTH]
        self.asks = sorted(
            [[float(p), float(s)] for p, s, *_ in asks  if float(s) > 0],
            key=lambda x:  x[0]
        )[:OB_DEPTH]
        self.ts = time.monotonic()

    def metrics(self) -> Optional[dict]:
        if not self.bids or not self.asks:
            return None
        best_bid   = self.bids[0][0]
        best_ask   = self.asks[0][0]
        mid        = (best_bid + best_ask) / 2.0
        spread_pct = (best_ask - best_bid) / mid * 100 if mid > 0 else 0.0
        bid_vol    = sum(s for _, s in self.bids)
        ask_vol    = sum(s for _, s in self.asks)
        total      = bid_vol + ask_vol
        imbalance  = (bid_vol - ask_vol) / total if total > 0 else 0.0
        return {
            "bid":        best_bid,
            "ask":        best_ask,
            "mid":        mid,
            "spread_pct": round(spread_pct, 4),
            "imbalance":  round(imbalance, 4),
            "bid_vol":    round(bid_vol, 4),
            "ask_vol":    round(ask_vol, 4),
            "age":        round(time.monotonic() - self.ts, 2),
        }


class _SymbolCache:
    def __init__(self):
        self.price:             Optional[float] = None
        self.price_ts:          float = 0.0
        self.candles:           Dict[str, deque] = {tf: deque(maxlen=OHLCV_LIMIT) for tf in TF_MAP}
        self.candle_ts:         Dict[str, float] = {tf: 0.0 for tf in TF_MAP}
        self.candle_open_ts_ms: Dict[str, int]   = {tf: 0   for tf in TF_MAP}
        self.ob:                _OrderBookCache  = _OrderBookCache()

    def update_price(self, last: float):
        self.price    = last
        self.price_ts = time.monotonic()

    def update_candle(self, tf: str, candle: list):
        """candle = [ts_ms, o, h, l, c, vol, ...]"""
        if tf not in self.candles:
            return
        try:
            ts  = int(candle[0])
            row = [ts, float(candle[1]), float(candle[2]),
                   float(candle[3]), float(candle[4]), float(candle[5])]
        except (IndexError, ValueError):
            return
        dq = self.candles[tf]
        if dq and dq[-1][0] == ts:
            dq[-1] = row          # actualizar vela corriente
        else:
            dq.append(row)        # nueva vela
        self.candle_ts[tf]         = time.monotonic()
        self.candle_open_ts_ms[tf] = ts

    def get_ohlcv_df(self, tf: str) -> pd.DataFrame:
        dq = self.candles.get(tf)
        if not dq or len(dq) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(list(dq), columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.set_index("ts").astype(float)


class WSFeed:
    def __init__(self):
        self._cache:   Dict[str, _SymbolCache] = {}
        self._symbols: List[str] = []          # coins normalizados ("BTC", "ETH"...)
        self._running  = False
        self._task:    Optional[asyncio.Task] = None

    # ── API pública ──────────────────────────────────────────────────────────

    def _key(self, symbol: str) -> str:
        return _norm(symbol)

    def get_price(self, symbol: str) -> Optional[float]:
        c = self._cache.get(self._key(symbol))
        return c.price if c else None

    def get_ohlcv(self, symbol: str, tf: str) -> pd.DataFrame:
        c = self._cache.get(self._key(symbol))
        if not c:
            return pd.DataFrame()
        return c.get_ohlcv_df(tf)

    def get_orderbook_metrics(self, symbol: str) -> Optional[dict]:
        c = self._cache.get(self._key(symbol))
        if not c:
            return None
        return c.ob.metrics()

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        return None  # consultar por REST si se necesita

    def has_data(self, symbol: str, tf: str = "15m", min_candles: int = 55) -> bool:
        c = self._cache.get(self._key(symbol))
        if not c:
            return False
        return len(c.candles.get(tf, [])) >= min_candles

    def is_price_fresh(self, symbol: str, max_age: float = 10.0) -> bool:
        c = self._cache.get(self._key(symbol))
        if not c or c.price is None:
            return False
        return (time.monotonic() - c.price_ts) < max_age

    def has_orderbook(self, symbol: str, max_age: float = 5.0) -> bool:
        c = self._cache.get(self._key(symbol))
        if not c or not c.ob.bids:
            return False
        return (time.monotonic() - c.ob.ts) < max_age

    def is_candle_closed(
        self,
        symbol:    str,
        tf:        str = "15m",
        threshold: float = 0.80,
    ) -> bool:
        tf_secs = _TF_SECS.get(tf)
        if not tf_secs:
            return True
        c = self._cache.get(self._key(symbol))
        if not c:
            return True
        open_ms = c.candle_open_ts_ms.get(tf, 0)
        if not open_ms:
            return True
        elapsed  = (time.time() * 1000 - open_ms) / 1000.0
        progress = elapsed / tf_secs
        closed   = progress >= threshold
        if not closed:
            log.debug(
                "[%s] Vela %s al %.0f%% (≤%.0f%%) — esperando cierre.",
                symbol, tf, progress * 100, threshold * 100,
            )
        return closed

    def candle_progress(self, symbol: str, tf: str = "15m") -> float:
        tf_secs = _TF_SECS.get(tf)
        if not tf_secs:
            return 1.0
        c = self._cache.get(self._key(symbol))
        if not c:
            return 1.0
        open_ms = c.candle_open_ts_ms.get(tf, 0)
        if not open_ms:
            return 1.0
        return min((time.time() * 1000 - open_ms) / 1000.0 / tf_secs, 1.0)

    # ── Control ──────────────────────────────────────────────────────────────

    def start(self, symbols: List[str]):
        self._symbols = [_norm(s) for s in symbols]
        for sym in self._symbols:
            if sym not in self._cache:
                self._cache[sym] = _SymbolCache()
        self._running = True
        self._task    = asyncio.ensure_future(self._run_loop())
        log.info("[WSFeed] Iniciado para %d símbolos (OKX)", len(self._symbols))

    def update_symbols(self, symbols: List[str]):
        new = [_norm(s) for s in symbols if _norm(s) not in self._cache]
        if new:
            for sym in new:
                self._cache[sym] = _SymbolCache()
            self._symbols = list(set(self._symbols + new))
            log.info("[WSFeed] Añadidos %d símbolos nuevos", len(new))

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        log.info("[WSFeed] Detenido")

    # ── Loop de reconexión ────────────────────────────────────────────────────

    async def _run_loop(self):
        attempt = 0
        while self._running:
            try:
                await self._connect_and_listen()
                attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                delay = min(RECONNECT_BASE * (2 ** attempt), RECONNECT_MAX)
                log.warning("[WSFeed] Desconectado (%s), reconectando en %.0fs...", e, delay)
                attempt += 1
                await asyncio.sleep(delay)

    async def _connect_and_listen(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                WS_URL,
                heartbeat=PING_INTERVAL,
                receive_timeout=90,
            ) as ws:
                log.info("[WSFeed] Conectado a OKX WS")
                await self._subscribe(ws)
                ping_task = asyncio.ensure_future(self._ping_loop(ws))
                try:
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle_message(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws):
        """
        OKX WS v5 — formato de suscripción:
        {"op": "subscribe", "args": [{"channel": "...", "instId": "..."}]}
        """
        args = []
        for coin in self._symbols:
            inst = _inst_id(coin)
            args.append({"channel": "tickers",  "instId": inst})
            args.append({"channel": "books5",    "instId": inst})
            for okx_channel in TF_MAP.values():
                args.append({"channel": okx_channel, "instId": inst})

        # OKX acepta hasta 240 args por mensaje; dividir en chunks si hace falta
        chunk_size = 100
        for i in range(0, len(args), chunk_size):
            await ws.send_str(json.dumps({"op": "subscribe", "args": args[i:i+chunk_size]}))

        log.debug("[WSFeed] Suscripciones enviadas: %d canales para %d símbolos",
                  len(args), len(self._symbols))

    async def _ping_loop(self, ws):
        """OKX requiere enviar 'ping' (texto) cada ≤30s."""
        while self._running:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await ws.send_str("ping")
            except Exception:
                break

    def _handle_message(self, raw: str):
        # OKX responde 'pong' al ping
        if raw == "pong":
            return

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Confirmación de suscripción / errores
        event = msg.get("event")
        if event == "error":
            log.warning("[WSFeed] Error OKX: %s", msg.get("msg", msg))
            return
        if event in ("subscribe", "unsubscribe"):
            return

        channel = msg.get("arg", {}).get("channel", "")
        inst_id = msg.get("arg", {}).get("instId", "")
        data    = msg.get("data", [])
        if not data:
            return

        # Extraer coin clave ("BTC-USDT-SWAP" → "BTC")
        coin = inst_id.split("-")[0] if inst_id else ""
        if coin not in self._cache:
            return

        # ── Ticker → precio ─────────────────────────────────────────────────
        if channel == "tickers":
            d = data[0]
            try:
                price = float(d.get("last") or d.get("askPx") or 0)
                if price > 0:
                    self._cache[coin].update_price(price)
            except (TypeError, ValueError):
                pass

        # ── books5 → order book ──────────────────────────────────────────────
        elif channel == "books5":
            d = data[0]
            # OKX books5: {"bids": [[px, sz, ..], ..], "asks": [[px, sz, ..], ..]}
            bids_raw = d.get("bids", [])
            asks_raw = d.get("asks", [])
            if bids_raw and asks_raw:
                self._cache[coin].ob.update(bids_raw, asks_raw)

        # ── Candles (candle15m, candle1H, candle4H) ──────────────────────────
        elif channel in TF_MAP.values():
            # Mapear canal OKX → TF interno
            tf = next((k for k, v in TF_MAP.items() if v == channel), None)
            if tf is None:
                return
            # OKX candle data: [[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm], ...]
            # Procesar todas las velas recibidas (suelen ser 1-2)
            for raw_candle in data:
                # Solo guardar velas confirmadas (confirm=="1") o la corriente (=="0")
                candle = [
                    int(raw_candle[0]),   # ts ms
                    float(raw_candle[1]), # open
                    float(raw_candle[2]), # high
                    float(raw_candle[3]), # low
                    float(raw_candle[4]), # close
                    float(raw_candle[5]), # volume (contratos)
                ]
                self._cache[coin].update_candle(tf, candle)


# Instancia global
ws_feed = WSFeed()
