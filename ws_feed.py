"""ws_feed.py — WebSocket kline feed de Hyperliquid.

Suscribe todos los pares a streams de candles 15m, 1h y 4h.
Mantiene un buffer de velas en memoria que signals.py consume.

Cambios respecto a la implementación BingX:
  - URL: wss://api.hyperliquid.xyz/ws
  - Protocolo: JSON puro (no gzip como BingX)
  - Suscripción: {"method": "subscribe", "subscription": {"type": "candle", "coin": "BTC", "interval": "15m"}}
  - Mensajes: {"channel": "candle", "data": {t, T, o, h, l, c, v, n, s, i}}
  - Ping/Pong: Hyperliquid envía pings del servidor; websocket-client responde automáticamente.
  - Nombres de coin: token base sin '-USDT' ('BTC', 'ETH', ...)

El formato interno del buffer es idéntico al de la implementación BingX
para no romper nada en signals.py ni main.py.
"""
import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import websocket

import config
import exchange

log = logging.getLogger("ws_feed")

WS_URL        = "wss://api.hyperliquid.xyz/ws"
BUFFER_SIZE   = 300
TIMEFRAMES    = ["15m", "1h", "4h"]
PING_INTERVAL = 30
PRELOAD_WORKERS = 8

_READY_MIN = {"15m": 120, "1h": 215}
_PRELOAD   = {"15m": 120, "1h": 220, "4h": 70}

STALE_THRESHOLD = 4 * 60  # 4 minutos


def _hl_coin(symbol: str) -> str:
    """'BTC-USDT' → 'BTC'"""
    return symbol.split("-")[0]


class KlineFeed:
    def __init__(self, symbols: list[str]):
        self._symbols  = symbols
        self._lock     = threading.Lock()
        self._data: dict[str, dict[str, deque]] = {
            s: {tf: deque(maxlen=BUFFER_SIZE) for tf in TIMEFRAMES}
            for s in symbols
        }
        self._last_update: dict[str, dict[str, float]] = {
            s: {tf: 0.0 for tf in TIMEFRAMES}
            for s in symbols
        }
        # Mapa inverso: coin_hl -> symbol_bot
        self._coin_to_sym: dict[str, str] = {_hl_coin(s): s for s in symbols}
        self._ws      = None
        self._running = False

    # ── API pública ────────────────────────────────────────────────────────────

    def get(self, symbol: str, timeframe: str) -> list[dict]:
        with self._lock:
            return list(self._data.get(symbol, {}).get(timeframe, []))

    def has_tf(self, symbol: str, timeframe: str) -> bool:
        with self._lock:
            return len(self._data.get(symbol, {}).get(timeframe, [])) > 0

    def ready(self, symbol: str) -> bool:
        """True si tenemos velas suficientes en 15m y 1h Y los datos son frescos."""
        with self._lock:
            has_enough = (
                len(self._data[symbol]["15m"]) >= _READY_MIN["15m"] and
                len(self._data[symbol]["1h"])  >= _READY_MIN["1h"]
            )
            if not has_enough:
                return False
            now     = time.time()
            age_15m = now - self._last_update[symbol]["15m"]
            age_1h  = now - self._last_update[symbol]["1h"]
            if age_15m > STALE_THRESHOLD or age_1h > STALE_THRESHOLD:
                log.warning(
                    "[%s] Datos obsoletos: 15m=%.0fs 1h=%.0fs (umbral %ds)",
                    symbol, age_15m, age_1h, STALE_THRESHOLD,
                )
                return False
            return True

    def ready_count(self) -> int:
        return sum(1 for s in self._symbols if self.ready(s))

    def start(self) -> None:
        log.info("Precargando velas REST para %d pares (%d workers)...",
                 len(self._symbols), PRELOAD_WORKERS)
        self._preload_parallel()
        self._running = True
        t = threading.Thread(target=self._run_forever, daemon=True)
        t.start()
        log.info("WebSocket Hyperliquid arrancado (15m + 1h + 4h)")

    def stop(self) -> None:
        self._running = False
        if self._ws:
            self._ws.close()

    # ── Precarga REST en paralelo ─────────────────────────────────────────────────

    def _preload_one(self, symbol: str, tf: str, limit: int) -> None:
        try:
            candles = exchange.get_ohlcv(symbol, interval=tf, limit=limit)
            with self._lock:
                self._data[symbol][tf].extend(candles)
                self._last_update[symbol][tf] = time.time()
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

    # ── WebSocket ──────────────────────────────────────────────────────────────────────

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
        # Hyperliquid no requiere ping manual; el servidor gestiona keepalive
        ws.run_forever(ping_interval=PING_INTERVAL, ping_timeout=10)

    def _on_open(self, ws) -> None:
        total = len(self._symbols) * len(TIMEFRAMES)
        log.info("WS Hyperliquid conectado — suscribiendo %d streams", total)
        for symbol in self._symbols:
            coin = _hl_coin(symbol)
            for tf in TIMEFRAMES:
                # Protocolo Hyperliquid WS (doc oficial)
                ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {
                        "type":     "candle",
                        "coin":     coin,
                        "interval": tf,
                    },
                }))

    def _on_message(self, ws, raw) -> None:
        try:
            # Hyperliquid envía JSON puro, no gzip
            msg = json.loads(raw)

            # Solo procesar mensajes de canal 'candle'
            if msg.get("channel") != "candle":
                return

            kline = msg.get("data")
            if not isinstance(kline, dict):
                return

            # Hyperliquid candle fields: t, T, o, h, l, c, v, n, s, i
            coin = kline.get("s", "")  # symbol (e.g. 'BTC')
            tf   = kline.get("i", "")  # interval (e.g. '15m')

            symbol = self._coin_to_sym.get(coin)
            if symbol is None or tf not in TIMEFRAMES:
                return

            open_time = int(kline["t"])
            close_val = float(kline["c"])
            vol       = float(kline["v"])

            candle = {
                "ts":           open_time,
                "open_time":    open_time,
                "open":         float(kline["o"]),
                "high":         float(kline["h"]),
                "low":          float(kline["l"]),
                "close":        close_val,
                "volume":       vol,
                "quote_volume": vol * close_val,
                # Hyperliquid no tiene campo 'confirm';
                # una vela se considera cerrada cuando llega con T (close_time) < ahora
                "closed":       int(kline.get("T", 0)) < int(time.time() * 1000),
            }

            with self._lock:
                buf = self._data[symbol][tf]
                if buf and buf[-1]["ts"] == candle["ts"]:
                    buf[-1] = candle
                else:
                    # Nueva vela: cerrar la anterior y añadir
                    if buf:
                        buf[-1] = dict(buf[-1], closed=True)
                    buf.append(candle)
                self._last_update[symbol][tf] = time.time()

        except Exception as e:
            log.warning("Error procesando mensaje WS: %s", e)

    def _on_error(self, ws, error) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        log.warning("WebSocket cerrado (code=%s) — reconectando...", code)
