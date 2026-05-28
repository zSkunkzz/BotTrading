"""
ws_feed.py — WebSocket feed de Bitget para precio, OHLCV y Order Book L2.

Suscripciones por símbolo activo:
  • ticker         → precio last en tiempo real
  • candle15m      → candles 15m (últimas 200 velas en caché)
  • candle1H       → candles 1h
  • candle4H       → candles 4h
  • books          → mejor bid/ask + imbalance L2 en tiempo real

NOTA: funding-rate eliminado del WS (Bitget devuelve code 30016 con instId
sin sufijo _UMCBL). El funding rate se consulta por REST cuando se necesita.

Uso desde signal_engine.py:
    from bot.ws_feed import ws_feed
    price   = ws_feed.get_price("BTCUSDT")
    df15    = ws_feed.get_ohlcv("BTCUSDT", "15m")
    ob      = ws_feed.get_orderbook_metrics("BTCUSDT")
    # ob → {"bid": float, "ask": float, "spread_pct": float, "imbalance": float}
    # imbalance: +1.0 = presión compradora total, -1.0 = vendedora total

El módulo arranca con ws_feed.start(symbols) y se detiene con ws_feed.stop().
Se reconecta automáticamente con backoff exponencial.
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

# ── Constantes Bitget WS ──────────────────────────────────────────────────────
WS_URL        = "wss://ws.bitget.com/v2/ws/public"
PING_INTERVAL = 25
OHLCV_LIMIT   = 200
RECONNECT_BASE = 2.0
RECONNECT_MAX  = 60.0

TF_MAP = {
    "15m": "candle15m",
    "1h":  "candle1H",
    "4h":  "candle4H",
}

# Número de niveles de profundidad a cachear por side (bid/ask)
OB_DEPTH = 20


# ── Caché de Order Book ───────────────────────────────────────────────────────

class _OrderBookCache:
    """
    Caché del order book L2 para un símbolo.
    Guarda los mejores OB_DEPTH niveles de bids y asks.
    """
    __slots__ = ("bids", "asks", "ts")

    def __init__(self):
        self.bids: list = []
        self.asks: list = []
        self.ts:   float = 0.0

    def apply_snapshot(self, bids: list, asks: list):
        self.bids = sorted([[float(p), float(s)] for p, s in bids if float(s) > 0],
                           key=lambda x: -x[0])[:OB_DEPTH]
        self.asks = sorted([[float(p), float(s)] for p, s in asks if float(s) > 0],
                           key=lambda x:  x[0])[:OB_DEPTH]
        self.ts = time.monotonic()

    def apply_delta(self, bids: list, asks: list):
        """Aplica delta incremental al order book en memoria."""
        def _merge(levels: list, updates: list, descending: bool) -> list:
            d = {p: s for p, s in levels}
            for p_str, s_str in updates:
                p, s = float(p_str), float(s_str)
                if s == 0:
                    d.pop(p, None)
                else:
                    d[p] = s
            return sorted([[p, s] for p, s in d.items()],
                          key=lambda x: -x[0] if descending else x[0])[:OB_DEPTH]

        if bids:
            self.bids = _merge(self.bids, bids, descending=True)
        if asks:
            self.asks = _merge(self.asks, asks, descending=False)
        self.ts = time.monotonic()

    def metrics(self) -> Optional[dict]:
        """Retorna métricas de microestructura o None si no hay datos."""
        if not self.bids or not self.asks:
            return None
        best_bid = self.bids[0][0]
        best_ask = self.asks[0][0]
        mid      = (best_bid + best_ask) / 2.0
        spread_pct = (best_ask - best_bid) / mid * 100 if mid > 0 else 0.0

        bid_vol = sum(s for _, s in self.bids[:5])
        ask_vol = sum(s for _, s in self.asks[:5])
        total   = bid_vol + ask_vol
        imbalance = (bid_vol - ask_vol) / total if total > 0 else 0.0

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


# ── Caché por símbolo ─────────────────────────────────────────────────────────

class _SymbolCache:
    """Caché completa de un símbolo: precio, candles y OB."""

    def __init__(self):
        self.price:        Optional[float] = None
        self.price_ts:     float = 0.0
        self.candles:      Dict[str, deque] = {tf: deque(maxlen=OHLCV_LIMIT) for tf in TF_MAP}
        self.candle_ts:    Dict[str, float] = {tf: 0.0 for tf in TF_MAP}
        self.ob:           _OrderBookCache  = _OrderBookCache()

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


# ── Manager principal ─────────────────────────────────────────────────────────

class WSFeed:
    def __init__(self):
        self._cache:   Dict[str, _SymbolCache] = {}
        self._symbols: List[str] = []
        self._running  = False
        self._task:    Optional[asyncio.Task] = None

    # ── API pública ───────────────────────────────────────────────────────────

    def get_price(self, symbol: str) -> Optional[float]:
        c = self._cache.get(symbol)
        return c.price if c else None

    def get_ohlcv(self, symbol: str, tf: str) -> pd.DataFrame:
        c = self._cache.get(symbol)
        if not c:
            return pd.DataFrame()
        return c.get_ohlcv_df(tf)

    def get_orderbook_metrics(self, symbol: str) -> Optional[dict]:
        """
        Retorna métricas de microestructura del order book L2:
          bid, ask, mid, spread_pct, imbalance [-1..+1], bid_vol, ask_vol, age
        Retorna None si no hay datos de OB disponibles.
        """
        c = self._cache.get(symbol)
        if not c:
            return None
        return c.ob.metrics()

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        """
        Funding rate eliminado del WS (Bitget error 30016).
        Retorna siempre None — consultar por REST si se necesita.
        """
        return None

    def has_data(self, symbol: str, tf: str = "15m", min_candles: int = 55) -> bool:
        c = self._cache.get(symbol)
        if not c:
            return False
        return len(c.candles.get(tf, [])) >= min_candles

    def is_price_fresh(self, symbol: str, max_age: float = 10.0) -> bool:
        c = self._cache.get(symbol)
        if not c or c.price is None:
            return False
        return (time.monotonic() - c.price_ts) < max_age

    def has_orderbook(self, symbol: str, max_age: float = 5.0) -> bool:
        """True si el OB tiene datos recientes."""
        c = self._cache.get(symbol)
        if not c or not c.ob.bids:
            return False
        return (time.monotonic() - c.ob.ts) < max_age

    # ── Control del feed ─────────────────────────────────────────────────────

    def start(self, symbols: List[str]):
        self._symbols = list(symbols)
        for sym in self._symbols:
            if sym not in self._cache:
                self._cache[sym] = _SymbolCache()
        self._running = True
        self._task = asyncio.ensure_future(self._run_loop())
        log.info(f"[WSFeed] Iniciado para {len(self._symbols)} símbolos")

    def update_symbols(self, symbols: List[str]):
        new = [s for s in symbols if s not in self._cache]
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

    # ── Conexión y escucha ────────────────────────────────────────────────────

    async def _connect_and_listen(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                WS_URL,
                heartbeat=PING_INTERVAL,
                receive_timeout=60,
            ) as ws:
                log.info("[WSFeed] Conectado a Bitget WS")
                await self._subscribe(ws)

                ping_task = asyncio.ensure_future(self._ping_loop(ws))
                try:
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            log.error(f"[WSFeed] WS error: {ws.exception()}")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws):
        """Envía las suscripciones de ticker, candles y OB para todos los símbolos.
        
        FIX: funding-rate eliminado — Bitget devuelve error 30016 (Param error)
        con el formato instId=BTCUSDT. Requeriría BTCUSDT_UMCBL y no está
        disponible en el WS público v2. Se consulta por REST cuando se necesita.
        """
        args = []
        for sym in self._symbols:
            # Ticker — precio en tiempo real
            args.append({"instType": "USDT-FUTURES", "channel": "ticker", "instId": sym})
            # Candles — todas las timeframes
            for tf_key in TF_MAP.values():
                args.append({"instType": "USDT-FUTURES", "channel": tf_key, "instId": sym})
            # Order Book L2
            args.append({"instType": "USDT-FUTURES", "channel": "books", "instId": sym})
            # ELIMINADO: funding-rate (error 30016 en WS público Bitget v2)

        # Bitget acepta hasta 100 args por batch
        for i in range(0, len(args), 100):
            batch   = args[i:i + 100]
            payload = json.dumps({"op": "subscribe", "args": batch})
            await ws.send_str(payload)
            log.debug(f"[WSFeed] Suscrito batch {i//100 + 1} ({len(batch)} canales)")

    async def _ping_loop(self, ws):
        while self._running:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await ws.send_str("ping")
            except Exception:
                break

    # ── Procesado de mensajes ─────────────────────────────────────────────────

    def _handle_message(self, raw: str):
        if raw == "pong":
            return
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if msg.get("event") in ("subscribe", "error"):
            if msg.get("event") == "error":
                log.warning(f"[WSFeed] Suscripción error: {msg}")
            return

        action  = msg.get("action")
        arg     = msg.get("arg", {})
        channel = arg.get("channel", "")
        inst_id = arg.get("instId", "")
        data    = msg.get("data", [])

        if not data or inst_id not in self._cache:
            return

        cache = self._cache[inst_id]

        if channel == "ticker":
            self._handle_ticker(cache, data)

        elif channel in TF_MAP.values():
            tf = next((k for k, v in TF_MAP.items() if v == channel), None)
            if tf:
                for candle in data:
                    cache.update_candle(tf, candle)

        elif channel == "books":
            self._handle_orderbook(cache, action, data)

    def _handle_ticker(self, cache: _SymbolCache, data: list):
        try:
            item = data[0] if isinstance(data, list) else data
            last = float(item.get("last") or item.get("lastPr") or 0)
            if last > 0:
                cache.update_price(last)
        except (IndexError, KeyError, ValueError, TypeError):
            pass

    def _handle_orderbook(self, cache: _SymbolCache, action: str, data: list):
        """
        Procesa snapshot y deltas del canal books.
        Formato Bitget:
          data[0] = {"asks": [[precio, size], ...], "bids": [[precio, size], ...], "ts": ...}
        """
        try:
            item = data[0] if isinstance(data, list) else data
            bids = item.get("bids", [])
            asks = item.get("asks", [])
            if action == "snapshot":
                cache.ob.apply_snapshot(bids, asks)
            else:
                cache.ob.apply_delta(bids, asks)
        except (IndexError, KeyError, ValueError, TypeError) as e:
            log.debug(f"[WSFeed] OB parse error: {e}")


# ── Instancia global ──────────────────────────────────────────────────────────
ws_feed = WSFeed()
