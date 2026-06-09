"""ws_feed.py — WebSocket kline feed de BingX perpetual.

Suscribe todos los pares a streams de klines 15m, 1h y 4h.
Mantiene un buffer de velas en memoria que signals.py consume.

Uso:
    feed = KlineFeed(config.SYMBOLS)
    feed.start()
    candles_15m = feed.get("BTC-USDT", "15m")
    candles_1h  = feed.get("BTC-USDT", "1h")
    candles_4h  = feed.get("BTC-USDT", "4h")
"""
import gzip
import json
import logging
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import websocket

import config
import exchange

log = logging.getLogger("ws_feed")

WS_URL        = "wss://open-api-swap.bingx.com/swap-market"
BUFFER_SIZE   = 300
TIMEFRAMES    = ["15m", "1h", "4h"]
PING_INTERVAL = 20
PRELOAD_WORKERS = 8   # peticiones REST en paralelo

_READY_MIN = {"15m": 120, "1h": 215}
_PRELOAD   = {"15m": 120, "1h": 220, "4h": 70}


class KlineFeed:
    def __init__(self, symbols: list[str]):
        self._symbols = symbols
        self._lock    = threading.Lock()
        self._data: dict[str, dict[str, deque]] = {
            s: {tf: deque(maxlen=BUFFER_SIZE) for tf in TIMEFRAMES}
            for s in symbols
        }
        self._ws      = None
        self._running = False

    # ── API pública ──────────────────────────────────────────────────────

    def get(self, symbol: str, timeframe: str) -> list[dict]:
        with self._lock:
            return list(self._data.get(symbol, {}).get(timeframe, []))

    def has_tf(self, symbol: str, timeframe: str) -> bool:
        with self._lock:
            return len(self._data.get(symbol, {}).get(timeframe, [])) > 0

    def ready(self, symbol: str) -> bool:
        """True si tenemos velas suficientes en 15m y 1h."""
        with self._lock:
            return (
                len(self._data[symbol]["15m"]) >= _READY_MIN["15m"] and
                len(self._data[symbol]["1h"])  >= _READY_MIN["1h"]
            )

    def ready_count(self) -> int:
        """Cuántos pares tienen datos suficientes."""
        return sum(1 for s in self._symbols if self.ready(s))

    def start(self) -> None:
        log.info("Precargando velas REST para %d pares (paralelo, %d workers)...",
                 len(self._symbols), PRELOAD_WORKERS)
        self._preload_parallel()
        self._running = True
        t = threading.Thread(target=self._run_forever, daemon=True)
        t.start()
        log.info("WebSocket feed arrancado (15m + 1h + 4h)")

    def stop(self) -> None:
        self._running = False
        if self._ws:
            self._ws.close()

    # ── Precarga REST en paralelo ────────────────────────────────────────

    def _preload_one(self, symbol: str, tf: str, limit: int) -> None:
        try:
            candles = exchange.get_ohlcv(symbol, interval=tf, limit=limit)
            with self._lock:
                self._data[symbol][tf].extend(candles)
            log.debug("[%s %s] precargadas %d velas", symbol, tf, len(candles))
        except Exception as e:
            log.warning("[%s %s] error precarga: %s", symbol, tf, e)

    def _preload_parallel(self) -> None:
        tasks = [
            (symbol, tf, limit)
            for symbol in self._symbols
            for tf, limit in _PRELOAD.items()
        ]
        with ThreadPoolExecutor(max_workers=PRELOAD_WORKERS) as ex:
            futures = {ex.submit(self._preload_one, s, tf, lim): (s, tf)
                       for s, tf, lim in tasks}
            done = 0
            for f in as_completed(futures):
                done += 1
                if done % 20 == 0 or done == len(tasks):
                    log.info("Precarga: %d/%d completadas", done, len(tasks))

    # ── WebSocket ────────────────────────────────────────────────────────

    def _run_forever(self) -> None:
        while self._running:
            try:
                self._connect()
            except Exception as e:
                log.error("WebSocket error: %s — reconectando en 5s", e)
                time.sleep(5)

    def _connect(self) -> None:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = self._on_error,
            on_close   = self._on_close,
        )
        self._ws = ws
        ws.run_forever(ping_interval=PING_INTERVAL, ping_timeout=10)

    def _on_open(self, ws) -> None:
        total = len(self._symbols) * len(TIMEFRAMES)
        log.info("WebSocket conectado — suscribiendo %d streams", total)
        for symbol in self._symbols:
            for tf in TIMEFRAMES:
                ws.send(json.dumps({
                    "id":       str(uuid.uuid4()),
                    "reqType":  "sub",
                    "dataType": f"{symbol}@kline_{tf}",
                }))

    def _on_message(self, ws, raw) -> None:
        try:
            data_str = gzip.decompress(raw).decode("utf-8") if isinstance(raw, bytes) else raw

            if data_str == "Ping":
                ws.send("Pong")
                return

            msg = json.loads(data_str)
            if "dataType" not in msg:
                return

            parts = msg["dataType"].split("@kline_")
            if len(parts) != 2:
                return

            symbol, tf = parts[0], parts[1]
            if symbol not in self._data or tf not in TIMEFRAMES:
                return

            raw_data = msg.get("data")
            if not raw_data:
                return

            kline = raw_data[0] if isinstance(raw_data, list) else raw_data
            if not isinstance(kline, dict):
                return

            candle = {
                "open":   float(kline["o"]),
                "high":   float(kline["h"]),
                "low":    float(kline["l"]),
                "close":  float(kline["c"]),
                "volume": float(kline["v"]),
                "closed": kline.get("confirm", False),
            }

            with self._lock:
                buf = self._data[symbol][tf]
                if buf and not buf[-1].get("closed", True):
                    buf[-1] = candle
                else:
                    buf.append(candle)

        except Exception as e:
            log.warning("Error procesando mensaje WS: %s", e)

    def _on_error(self, ws, error) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        log.warning("WebSocket cerrado (code=%s) — reconectando...", code)
