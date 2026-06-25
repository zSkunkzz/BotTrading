"""ws_feed.py — WebSocket kline feed de BingX perpetual.

Suscribe todos los pares a streams de klines 15m, 1h y 4h.
Mantiene un buffer de velas en memoria que signals.py consume.

FIX: se añade _last_update[symbol][tf] que registra el timestamp de la última
vela recibida. ready() devuelve False si los datos de 15m o 1h tienen más de
STALE_THRESHOLD segundos sin actualizarse, evitando que el bot evalúe señales
con datos obsoletos cuando el WebSocket se cae sin reconectar.

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
PRELOAD_WORKERS = 8

_READY_MIN = {"15m": 120, "1h": 215}
_PRELOAD   = {"15m": 120, "1h": 220, "4h": 70}

# FIX #4: STALE_THRESHOLD reducido de 10 min a 4 min.
# El loop principal corre cada 20s. Con 10 min el bot podía evaluar señales
# con datos de hasta 9 min de antigüedad tras una caída del WS, abriendo
# posiciones sobre precios que ya se habían movido significativamente.
# Con 4 min (≈2 velas de 15m sin confirmar) se detecta la caída antes
# de que los datos sean peligrosamente obsoletos.
STALE_THRESHOLD = 4 * 60  # 4 minutos


class KlineFeed:
    def __init__(self, symbols: list[str]):
        self._symbols = symbols
        self._lock    = threading.Lock()
        self._data: dict[str, dict[str, deque]] = {
            s: {tf: deque(maxlen=BUFFER_SIZE) for tf in TIMEFRAMES}
            for s in symbols
        }
        # Timestamp (time.time()) de la última actualización por símbolo y tf.
        # Se inicializa a 0. La precarga REST lo actualiza al terminar para que
        # los símbolos no aparezcan como obsoletos nada más arrancar.
        self._last_update: dict[str, dict[str, float]] = {
            s: {tf: 0.0 for tf in TIMEFRAMES}
            for s in symbols
        }
        self._ws      = None
        self._running = False

    # ── API pública ──────────────────────────────────────────────────

    def get(self, symbol: str, timeframe: str) -> list[dict]:
        with self._lock:
            return list(self._data.get(symbol, {}).get(timeframe, []))

    def has_tf(self, symbol: str, timeframe: str) -> bool:
        with self._lock:
            return len(self._data.get(symbol, {}).get(timeframe, [])) > 0

    def ready(self, symbol: str) -> bool:
        """True si tenemos velas suficientes en 15m y 1h Y los datos son frescos.

        FIX: comprueba que la última actualización de 15m y 1h fue hace menos
        de STALE_THRESHOLD segundos. Si el WebSocket lleva más tiempo sin enviar
        datos para este símbolo, devuelve False y el bot omite la evaluación.
        """
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
                    "[%s] Datos obsoletos: 15m=%.0fs 1h=%.0fs sin actualizar (umbral %ds)",
                    symbol, age_15m, age_1h, STALE_THRESHOLD,
                )
                return False

            return True

    def ready_count(self) -> int:
        """Cuántos pares tienen datos suficientes y frescos."""
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

    # ── Precarga REST en paralelo ───────────────────────────────────────────

    def _preload_one(self, symbol: str, tf: str, limit: int) -> None:
        try:
            candles = exchange.get_ohlcv(symbol, interval=tf, limit=limit)
            with self._lock:
                self._data[symbol][tf].extend(candles)
                # Marcar como actualizado para que ready() no lo descarte
                # inmediatamente tras la precarga REST.
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

    # ── WebSocket ────────────────────────────────────────────────────────────────────────

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
                "ts":     int(kline.get("T", kline.get("t", 0))),
                "open":   float(kline["o"]),
                "high":   float(kline["h"]),
                "low":    float(kline["l"]),
                "close":  float(kline["c"]),
                "volume": float(kline["v"]),
                "closed": bool(kline.get("confirm", False)),
            }

            with self._lock:
                buf = self._data[symbol][tf]
                if buf and buf[-1]["ts"] == candle["ts"]:
                    buf[-1] = candle
                else:
                    if not buf or buf[-1].get("closed", True):
                        buf.append(candle)
                    else:
                        buf[-1] = dict(buf[-1], closed=True)
                        buf.append(candle)

                # Actualizar timestamp de frescura en cada mensaje recibido
                self._last_update[symbol][tf] = time.time()

        except Exception as e:
            log.warning("Error procesando mensaje WS: %s", e)

    def _on_error(self, ws, error) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        log.warning("WebSocket cerrado (code=%s) — reconectando...", code)
