"""ws_feed.py — WebSocket kline feed de BingX perpetual.

Suscribe todos los pares configurados a los streams de klines 15m y 1h.
Mantiene un buffer de velas en memoria que signals.py consume.

Uso:
    feed = KlineFeed(config.SYMBOLS)
    feed.start()                      # arranca el thread en background
    candles_15m = feed.get("BTC-USDT", "15m")
    candles_1h  = feed.get("BTC-USDT", "1h")
"""
import gzip
import json
import logging
import threading
import time
import uuid
from collections import deque

import websocket

import config
import exchange

log = logging.getLogger("ws_feed")

WS_URL      = "wss://open-api-swap.bingx.com/swap-market"
BUFFER_SIZE = 250   # velas por par/timeframe
TIMEFRAMES  = ["15m", "1h"]
PING_INTERVAL = 20  # segundos


class KlineFeed:
    def __init__(self, symbols: list[str]):
        self._symbols  = symbols
        self._lock     = threading.Lock()
        # _data[symbol][timeframe] = deque de velas
        self._data: dict[str, dict[str, deque]] = {
            s: {tf: deque(maxlen=BUFFER_SIZE) for tf in TIMEFRAMES}
            for s in symbols
        }
        self._ws      = None
        self._running = False

    # ── API pública ─────────────────────────────────────────────────────────────

    def get(self, symbol: str, timeframe: str) -> list[dict]:
        """Devuelve la lista de velas cerradas (excluye la vela viva)."""
        with self._lock:
            buf = self._data.get(symbol, {}).get(timeframe, deque())
            candles = list(buf)
        # excluimos la última (vela viva)
        return candles[:-1] if len(candles) > 1 else []

    def ready(self, symbol: str) -> bool:
        """True si tenemos suficientes velas para evaluar señales."""
        return (
            len(self._data[symbol]["15m"]) >= 120 and
            len(self._data[symbol]["1h"])  >= 215
        )

    def start(self) -> None:
        """Precarga velas REST y arranca el WebSocket en background."""
        log.info("Precargando velas REST para %d pares...", len(self._symbols))
        self._preload()
        self._running = True
        t = threading.Thread(target=self._run_forever, daemon=True)
        t.start()
        log.info("WebSocket feed arrancado")

    def stop(self) -> None:
        self._running = False
        if self._ws:
            self._ws.close()

    # ── Precarga REST ───────────────────────────────────────────────────────────

    def _preload(self) -> None:
        for symbol in self._symbols:
            for tf, limit in [("15m", 120), ("1h", 220)]:
                try:
                    candles = exchange.get_ohlcv(symbol, interval=tf, limit=limit)
                    with self._lock:
                        self._data[symbol][tf].extend(candles)
                    log.debug("[%s %s] precargadas %d velas", symbol, tf, len(candles))
                except Exception as e:
                    log.warning("[%s %s] error precarga: %s", symbol, tf, e)

    # ── WebSocket ───────────────────────────────────────────────────────────────

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
        # ping_interval interno de websocket-client
        ws.run_forever(ping_interval=PING_INTERVAL, ping_timeout=10)

    def _on_open(self, ws) -> None:
        log.info("WebSocket conectado — suscribiendo %d streams",
                 len(self._symbols) * len(TIMEFRAMES))
        for symbol in self._symbols:
            for tf in TIMEFRAMES:
                sub = {
                    "id":      str(uuid.uuid4()),
                    "reqType": "sub",
                    "dataType": f"{symbol}@kline_{tf}",
                }
                ws.send(json.dumps(sub))

    def _on_message(self, ws, raw) -> None:
        # BingX envía los mensajes comprimidos en gzip
        try:
            if isinstance(raw, bytes):
                data_str = gzip.decompress(raw).decode("utf-8")
            else:
                data_str = raw

            # Respuesta a ping del servidor
            if data_str == "Ping":
                ws.send("Pong")
                return

            msg = json.loads(data_str)

            # Ignorar confirmaciones de suscripción
            if "dataType" not in msg:
                return

            # Formato: "BTC-USDT@kline_15m"
            data_type = msg["dataType"]          # ej. "BTC-USDT@kline_15m"
            parts = data_type.split("@kline_")  # ["BTC-USDT", "15m"]
            if len(parts) != 2:
                return

            symbol, tf = parts[0], parts[1]
            if symbol not in self._data or tf not in TIMEFRAMES:
                return

            raw_data = msg.get("data")
            if not raw_data:
                return

            # BingX puede enviar data como dict o como lista de dicts.
            # Normalizamos siempre a dict tomando el primer elemento si es lista.
            if isinstance(raw_data, list):
                if len(raw_data) == 0:
                    return
                kline = raw_data[0]
            else:
                kline = raw_data

            if not isinstance(kline, dict):
                return

            candle = {
                "open":   float(kline["o"]),
                "high":   float(kline["h"]),
                "low":    float(kline["l"]),
                "close":  float(kline["c"]),
                "volume": float(kline["v"]),
                "closed": kline.get("confirm", False),  # True = vela cerrada
            }

            with self._lock:
                buf = self._data[symbol][tf]
                if buf and not buf[-1].get("closed", True):
                    # Actualizar la vela viva
                    buf[-1] = candle
                else:
                    # Nueva vela
                    buf.append(candle)

        except Exception as e:
            log.warning("Error procesando mensaje WS: %s", e)

    def _on_error(self, ws, error) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        log.warning("WebSocket cerrado (code=%s) — reconectando...", code)
