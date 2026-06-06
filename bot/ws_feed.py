"""
bot/ws_feed.py — WebSocket feed de BingX para precio, OHLCV y Order Book L2.

v3 — BingX migration (2026-06-06):
  Sustituye el WS de OKX por el WS público de BingX Perpetuos USDT-M:
    wss://open-api-ws.bingx.com/market

  Diferencias clave BingX vs OKX:
  • URL: wss://open-api-ws.bingx.com/market  (no wss://ws.okx.com)
  • Suscripción: campo 'dataType' (no 'channel'+'instId')
      {"id":"<uuid>","dataType":"<{SYMBOL}@kline_15m>","reqType":"sub"}
  • Ping/Pong: BingX envía texto "Ping"; cliente debe responder "Pong".
      No enviar pings propios basados en tiempo como OKX — solo responder.
  • Ticker: dataType = "{SYMBOL}@ticker"
      Payload: {"c": last, "b": bid, "a": ask, ...}
  • Klines: dataType = "{SYMBOL}@kline_{interval}"
      Intervalo: 1m | 3m | 5m | 15m | 30m | 1h | 2h | 4h | 6h | 12h | 1d
      Payload: {"data": {"T": ts_close_ms, "o": open, "h": high, "l": low,
                          "c": close, "v": volume, "n": is_closed (bool/0/1)}}
  • Order Book: dataType = "{SYMBOL}@depth20" (5/10/20 niveles disponibles)
      Payload: {"bids": [[px, qty], ...], "asks": [[px, qty], ...]}
  • Símbolo BingX: "BTC-USDT" (no "BTC-USDT-SWAP")
  Ref: BingX WebSocket Market Data API docs.

  API pública idéntica a la versión anterior (compatible con signal_engine):
    from bot.ws_feed import ws_feed
    price  = ws_feed.get_price("BTC")
    df15   = ws_feed.get_ohlcv("BTC", "15m")
    ob     = ws_feed.get_orderbook_metrics("BTC")
    closed = ws_feed.is_candle_closed("BTC", "15m")
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
import uuid
from collections import deque
from typing import Dict, List, Optional

import aiohttp
import pandas as pd

log = logging.getLogger("WSFeed")

# ── BingX WS público perpetuos USDT-M ────────────────────────────────────────
# Ref: BingX WebSocket Market Data docs.
WS_URL        = "wss://open-api-ws.bingx.com/market"
OHLCV_LIMIT   = 200
RECONNECT_BASE = 2.0
RECONNECT_MAX  = 60.0

# Mapa TF interno → intervalo BingX kline
# Ref: BingX kline dataType — {SYMBOL}@kline_{interval}
# Intervalos válidos: 1m | 3m | 5m | 15m | 30m | 1h | 2h | 4h | 6h | 12h | 1d
TF_MAP: dict[str, str] = {
    "15m": "15m",
    "1h":  "1h",
    "4h":  "4h",
}

_TF_SECS: dict[str, float] = {
    "1m":  60,   "3m":  180,  "5m":  300,
    "15m": 900,  "30m": 1800, "1h":  3600,
    "2h":  7200, "4h":  14400, "6h": 21600,
    "12h": 43200, "1d": 86400,
}

OB_DEPTH = 20  # BingX soporta depth5/depth10/depth20; usamos depth20


def _norm(symbol: str) -> str:
    """BTCUSDT / BTC/USDT / BTC-USDT → BTC"""
    s = symbol.upper().replace("/", "-")
    if s.endswith("-SWAP"):
        s = s[:-5]
    for suffix in ("-USDT", "USDT", "-USDC", "USDC"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    return s.split("-")[0]


def _bx_symbol(coin: str) -> str:
    """
    BingX usa '{COIN}-USDT' como símbolo en los dataType WS.
    Ref: BingX WS docs — dataType format: '{symbol}@<channel>'.
    """
    return f"{coin}-USDT"


class _OrderBookCache:
    __slots__ = ("bids", "asks", "ts")

    def __init__(self):
        self.bids: list = []
        self.asks: list = []
        self.ts:   float = 0.0

    def update(self, bids: list, asks: list):
        self.bids = sorted(
            [[float(p), float(s)] for p, s in bids  if float(s) > 0],
            key=lambda x: -x[0]
        )[:OB_DEPTH]
        self.asks = sorted(
            [[float(p), float(s)] for p, s in asks  if float(s) > 0],
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

    def update_candle(self, tf: str, ts_ms: int, o: float, h: float,
                      l: float, c: float, v: float):
        if tf not in self.candles:
            return
        row = [ts_ms, o, h, l, c, v]
        dq = self.candles[tf]
        if dq and dq[-1][0] == ts_ms:
            dq[-1] = row          # actualizar vela corriente
        else:
            dq.append(row)        # nueva vela
        self.candle_ts[tf]         = time.monotonic()
        self.candle_open_ts_ms[tf] = ts_ms

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
        log.info("[WSFeed] Iniciado para %d símbolos (BingX)", len(self._symbols))

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
        """
        BingX WS: los mensajes pueden llegar comprimidos con gzip o como texto plano.
        Ref: BingX WS docs — "The data format returned from the server is compressed
        using gzip. The client needs to decompress it."
        """
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                WS_URL,
                receive_timeout=90,
            ) as ws:
                log.info("[WSFeed] Conectado a BingX WS")
                await self._subscribe(ws)
                async for msg in ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        # BingX envía datos comprimidos en gzip
                        try:
                            text = gzip.decompress(msg.data).decode("utf-8")
                        except Exception:
                            continue
                        self._handle_message(ws, text)
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        self._handle_message(ws, msg.data)
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        break

    async def _subscribe(self, ws):
        """
        BingX WS — formato de suscripción:
          {"id": "<uuid>", "reqType": "sub", "dataType": "<symbol>@<channel>"}

        Canales:
          - Ticker:     {SYMBOL}@ticker
          - Kline:      {SYMBOL}@kline_{interval}  (ej. BTC-USDT@kline_15m)
          - Order Book: {SYMBOL}@depth20

        Ref: BingX WebSocket Market Data API — Subscribe Request Format.
        """
        for coin in self._symbols:
            sym = _bx_symbol(coin)  # "BTC-USDT"

            # Ticker → precio en tiempo real
            await ws.send_str(json.dumps({
                "id":       str(uuid.uuid4()),
                "reqType":  "sub",
                "dataType": f"{sym}@ticker",
            }))

            # Order Book depth20
            await ws.send_str(json.dumps({
                "id":       str(uuid.uuid4()),
                "reqType":  "sub",
                "dataType": f"{sym}@depth20",
            }))

            # Klines por cada TF
            for interval in TF_MAP.values():  # "15m", "1h", "4h"
                await ws.send_str(json.dumps({
                    "id":       str(uuid.uuid4()),
                    "reqType":  "sub",
                    "dataType": f"{sym}@kline_{interval}",
                }))

        log.debug(
            "[WSFeed] Suscripciones BingX enviadas: %d símbolos × %d canales",
            len(self._symbols), 2 + len(TF_MAP),
        )

    def _handle_message(self, ws, raw: str):
        """
        BingX WS — protocolo Ping/Pong:
          - El servidor envía el texto 'Ping' periódicamente.
          - El cliente DEBE responder 'Pong' (texto), si no la conexión se cierra.
          - NO enviar pings propios; solo responder al servidor.
          Ref: BingX WS docs — Heartbeat / Ping-Pong.
        """
        if raw == "Ping":
            asyncio.ensure_future(ws.send_str("Pong"))
            return

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Confirmación de suscripción — ignorar
        if msg.get("code") is not None or msg.get("id") is not None:
            if msg.get("code") and str(msg.get("code")) != "0":
                log.warning("[WSFeed] Error suscripción BingX: %s", msg)
            return

        data_type = msg.get("dataType", "")
        data      = msg.get("data", {})
        if not data_type or not data:
            return

        # Extraer coin del dataType: "BTC-USDT@ticker" → "BTC"
        try:
            coin = data_type.split("@")[0].replace("-USDT", "")
        except Exception:
            return
        if coin not in self._cache:
            return

        # ── Ticker → precio ─────────────────────────────────────────────────
        # BingX ticker payload:
        #   {"c": last_price, "b": best_bid, "a": best_ask, "o": open_24h,
        #    "h": high_24h, "l": low_24h, "v": volume_24h, "t": timestamp_ms}
        # Ref: BingX WS ticker stream docs.
        if "@ticker" in data_type:
            try:
                last = float(data.get("c") or data.get("a") or 0)
                if last > 0:
                    self._cache[coin].update_price(last)
            except (TypeError, ValueError):
                pass

        # ── Order Book depth20 → spread / imbalance ─────────────────────────
        # BingX depth payload:
        #   {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}
        # Ref: BingX WS depth stream docs.
        elif "@depth" in data_type:
            bids_raw = data.get("bids", [])
            asks_raw = data.get("asks", [])
            if bids_raw and asks_raw:
                self._cache[coin].ob.update(bids_raw, asks_raw)

        # ── Klines → OHLCV ──────────────────────────────────────────────────
        # BingX kline payload:
        #   {"E": event_time, "s": symbol,
        #    "K": {"t": open_ts_ms, "T": close_ts_ms, "o": open, "h": high,
        #          "l": low, "c": close, "v": volume, "n": is_closed}}
        # Nota: en algunos pares BingX envía el payload directamente como
        # objeto plano sin clave 'K'. Manejamos ambos formatos.
        # Ref: BingX WS kline stream docs.
        elif "@kline_" in data_type:
            interval = data_type.split("@kline_")[-1]  # "15m", "1h", "4h"
            tf = next((k for k, v in TF_MAP.items() if v == interval), None)
            if tf is None:
                return
            try:
                kline = data.get("K") or data  # soportar ambos formatos
                ts_ms = int(kline.get("t") or kline.get("T") or 0)
                o = float(kline.get("o", 0))
                h = float(kline.get("h", 0))
                l = float(kline.get("l", 0))
                c = float(kline.get("c", 0))
                v = float(kline.get("v", 0))
                if ts_ms > 0 and c > 0:
                    self._cache[coin].update_candle(tf, ts_ms, o, h, l, c, v)
            except (TypeError, ValueError, KeyError):
                pass


# Instancia global
ws_feed = WSFeed()
