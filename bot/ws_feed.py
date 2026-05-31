"""
ws_feed.py — WebSocket feed de Hyperliquid para precio, OHLCV y Order Book L2.

Suscripciones por símbolo activo:
  • allMids       → precios mid en tiempo real (todos los pares)
  • l2Book        → mejor bid/ask + imbalance L2 en tiempo real
  • candle        → candles 15m / 1h / 4h por símbolo

Uso desde signal_engine.py (API idéntica a la versión Bitget):
    from bot.ws_feed import ws_feed
    price   = ws_feed.get_price("BTC")       # sin USDT
    df15    = ws_feed.get_ohlcv("BTC", "15m")
    ob      = ws_feed.get_orderbook_metrics("BTC")

Nota: Hyperliquid usa nombres cortos de coin ("BTC", "ETH", "SOL"), sin "USDT".
      El módulo acepta "BTCUSDT" y lo normaliza internamente.
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

WS_URL         = "wss://api.hyperliquid.xyz/ws"
PING_INTERVAL  = 20
OHLCV_LIMIT    = 200
RECONNECT_BASE = 2.0
RECONNECT_MAX  = 60.0

TF_MAP = {
    "15m": "15m",
    "1h":  "1h",
    "4h":  "4h",
}

OB_DEPTH = 20


def _norm(symbol: str) -> str:
    """BTCUSDT / BTC/USDT:USDT → BTC (Hyperliquid coin name)"""
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


class _OrderBookCache:
    __slots__ = ("bids", "asks", "ts")

    def __init__(self):
        self.bids: list = []
        self.asks: list = []
        self.ts:   float = 0.0

    def update(self, bids: list, asks: list):
        self.bids = sorted([[float(p), float(s)] for p, s in bids  if float(s) > 0], key=lambda x: -x[0])[:OB_DEPTH]
        self.asks = sorted([[float(p), float(s)] for p, s in asks  if float(s) > 0], key=lambda x:  x[0])[:OB_DEPTH]
        self.ts   = time.monotonic()

    def metrics(self) -> Optional[dict]:
        if not self.bids or not self.asks:
            return None
        best_bid   = self.bids[0][0]
        best_ask   = self.asks[0][0]
        mid        = (best_bid + best_ask) / 2.0
        spread_pct = (best_ask - best_bid) / mid * 100 if mid > 0 else 0.0
        bid_vol    = sum(s for _, s in self.bids[:5])
        ask_vol    = sum(s for _, s in self.asks[:5])
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
        self.price:     Optional[float] = None
        self.price_ts:  float = 0.0
        self.candles:   Dict[str, deque] = {tf: deque(maxlen=OHLCV_LIMIT) for tf in TF_MAP}
        self.candle_ts: Dict[str, float] = {tf: 0.0 for tf in TF_MAP}
        self.ob:        _OrderBookCache  = _OrderBookCache()

    def update_price(self, last: float):
        self.price    = last
        self.price_ts = time.monotonic()

    def update_candle(self, tf: str, candle: list):
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
            dq[-1] = row
        else:
            dq.append(row)
        self.candle_ts[tf] = time.monotonic()

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
        self._symbols: List[str] = []
        self._running  = False
        self._task:    Optional[asyncio.Task] = None

    # ── API pública (compatible con versión Bitget) ───────────────────────────

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
        return None  # se consulta por REST

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

    # ── Control ───────────────────────────────────────────────────────────────

    def start(self, symbols: List[str]):
        self._symbols = [_norm(s) for s in symbols]
        for sym in self._symbols:
            if sym not in self._cache:
                self._cache[sym] = _SymbolCache()
        self._running = True
        self._task    = asyncio.ensure_future(self._run_loop())
        log.info(f"[WSFeed] Iniciado para {len(self._symbols)} símbolos (Hyperliquid)")

    def update_symbols(self, symbols: List[str]):
        new = [_norm(s) for s in symbols if _norm(s) not in self._cache]
        if new:
            for sym in new:
                self._cache[sym] = _SymbolCache()
            self._symbols = list(set(self._symbols + new))
            log.info(f"[WSFeed] Añadidos {len(new)} símbolos nuevos")

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
                log.warning(f"[WSFeed] Desconectado ({e}), reconectando en {delay:.0f}s...")
                attempt += 1
                await asyncio.sleep(delay)

    async def _connect_and_listen(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL, heartbeat=PING_INTERVAL, receive_timeout=60) as ws:
                log.info("[WSFeed] Conectado a Hyperliquid WS")
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
        # allMids — precios de todos los pares en tiempo real
        await ws.send_str(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))

        # l2Book y candles por símbolo
        for sym in self._symbols:
            await ws.send_str(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": sym}
            }))
            for tf in TF_MAP.values():
                await ws.send_str(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "candle", "coin": sym, "interval": tf}
                }))
        log.debug(f"[WSFeed] Suscripciones enviadas para {len(self._symbols)} símbolos")

    async def _ping_loop(self, ws):
        while self._running:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await ws.send_str(json.dumps({"method": "ping"}))
            except Exception:
                break

    def _handle_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        channel = msg.get("channel", "")
        data    = msg.get("data")

        if channel == "allMids" and isinstance(data, dict):
            mids = data.get("mids", {})
            for coin, mid_str in mids.items():
                if coin in self._cache:
                    try:
                        self._cache[coin].update_price(float(mid_str))
                    except (ValueError, TypeError):
                        pass

        elif channel == "l2Book" and isinstance(data, dict):
            coin = data.get("coin", "")
            if coin in self._cache:
                levels = data.get("levels", [[], []])
                bids_raw = levels[0] if len(levels) > 0 else []
                asks_raw = levels[1] if len(levels) > 1 else []
                bids = [[e["px"], e["sz"]] for e in bids_raw if isinstance(e, dict)]
                asks = [[e["px"], e["sz"]] for e in asks_raw if isinstance(e, dict)]
                self._cache[coin].ob.update(bids, asks)

        elif channel == "candle" and isinstance(data, dict):
            coin = data.get("s", "")  # símbolo en candles
            tf   = data.get("i", "")  # intervalo: "15m", "1h", "4h"
            if coin in self._cache and tf in TF_MAP:
                # Hyperliquid candle: {t, o, h, l, c, v, ...}
                candle = [
                    data.get("t", 0),
                    data.get("o", 0),
                    data.get("h", 0),
                    data.get("l", 0),
                    data.get("c", 0),
                    data.get("v", 0),
                ]
                self._cache[coin].update_candle(tf, candle)


# Instancia global
ws_feed = WSFeed()
