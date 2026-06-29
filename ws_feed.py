"""ws_feed.py — WebSocket kline feed de BingX perpetual.

Suscribe todos los pares a streams de klines 15m, 1h y 4h.
Mantiene un buffer de velas en memoria que signals.py consume.

FIX Bug3: Las velas precargadas por REST y las recibidas por WS ahora incluyen
siempre el campo 'open_time' (alias de 'ts') para que signals._daily_candle_context
pueda filtrar las velas del día actual correctamente. Sin este fix, el contexto
diario nunca funcionaba porque buscaba 'open_time' y las velas solo tenían 'ts'.

FIX Bug4: STALE check diferenciado por timeframe:
  - 15m: STALE_THRESHOLD_15M = 4 min (si el WS no manda nada en 4 min, hay problema)
  - 1h:  STALE_THRESHOLD_1H  = 6 min (WS reenvía vela 1h en curso cada ~5s con movimiento;
          en mercados quietos puede tardar más, 6 min da margen suficiente)
  - 4h:  SIN stale check (emite vela nueva cada 4 horas; cualquier umbral de minutos
          haría que ready() devuelva False permanentemente)

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

# FIX Bug4: umbrales de frescura diferenciados por timeframe.
# 4h NO tiene stale check: las velas de 4h solo se emiten cada 4 horas,
# cualquier umbral en minutos descartaría todos los símbolos permanentemente.
STALE_THRESHOLD_15M = 4 * 60   # 4 minutos
STALE_THRESHOLD_1H  = 6 * 60   # 6 minutos


class KlineFeed:
    def __init__(self, symbols: list[str]):
        self._symbols = symbols
        self._lock    = threading.Lock()
        self._data: dict[str, dict[str, deque]] = {
            s: {tf: deque(maxlen=BUFFER_SIZE) for tf in TIMEFRAMES}
            for s in symbols
        }
        # Timestamp (time.time()) de la última actualización por símbolo y tf.
        # Inicializado a 0; la precarga REST lo actualiza al terminar.
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

        FIX Bug4: stale check solo para 15m y 1h, con umbrales distintos.
        El timeframe 4h no tiene stale check (velas cada 4 horas).
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

            if age_15m > STALE_THRESHOLD_15M:
                log.warning(
                    "[%s] Datos 15m obsoletos: %.0fs sin actualizar (umbral %ds)",
                    symbol, age_15m, STALE_THRESHOLD_15M,
                )
                return False

            if age_1h > STALE_THRESHOLD_1H:
                log.warning(
                    "[%s] Datos 1h obsoletos: %.0fs sin actualizar (umbral %ds)",
                    symbol, age_1h, STALE_THRESHOLD_1H,
                )
                return False

            # 4h: sin stale check
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
            # FIX Bug3: añadir 'open_time' = 'ts' para que signals._daily_candle_context
            # filtre las velas del día actual. exchange.get_ohlcv() solo emite 'ts';
            # 'open_time' es el campo que buscaba signals y devolvía 0 siempre.
            for c in candles:
                if "open_time" not in c:
                    c["open_time"] = c.get("ts", 0)
            with self._lock:
                self._data[symbol][tf].extend(candles)
                # Marcar como actualizado para que ready() no descarte los datos
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

            ts = int(kline.get("T", kline.get("t", 0)))
            candle = {
                "ts":        ts,
                "open_time": ts,   # FIX Bug3: alias para signals._daily_candle_context
                "open":      float(kline["o"]),
                "high":      float(kline["h"]),
                "low":       float(kline["l"]),
                "close":     float(kline["c"]),
                "volume":    float(kline["v"]),
                "closed":    bool(kline.get("confirm", False)),
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
