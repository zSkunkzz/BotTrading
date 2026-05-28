"""
ws_feed.py — WebSocket feed de Bitget para precio y OHLCV en tiempo real.

Suscripciones por símbolo activo:
  • ticker         → precio last en tiempo real
  • candle15m      → candles 15m (últimas 200 velas en caché)
  • candle1H       → candles 1h
  • candle4H       → candles 4h

Uso desde signal_engine.py:
    from bot.ws_feed import ws_feed
    price  = ws_feed.get_price("BTCUSDT")
    df15   = ws_feed.get_ohlcv("BTCUSDT", "15m")

Uso desde trader.py (get_price):
    from bot.ws_feed import ws_feed
    price = ws_feed.get_price(sym_clean)  # fallback a REST si no disponible

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
PING_INTERVAL = 25        # segundos entre ping
OHLCV_LIMIT   = 200       # velas en caché por tf
RECONNECT_BASE = 2.0      # segundos base para backoff
RECONNECT_MAX  = 60.0     # techo backoff

TF_MAP = {
    "15m": "candle15m",
    "1h":  "candle1H",
    "4h":  "candle4H",
}


# ── Caché en memoria ──────────────────────────────────────────────────────────

class _SymbolCache:
    """Caché de precio y candles para un símbolo."""

    def __init__(self):
        self.price:     Optional[float] = None
        self.price_ts:  float = 0.0
        # tf → deque de [ts_ms, open, high, low, close, volume]
        self.candles:   Dict[str, deque] = {tf: deque(maxlen=OHLCV_LIMIT) for tf in TF_MAP}
        self.candle_ts: Dict[str, float] = {tf: 0.0 for tf in TF_MAP}

    def update_price(self, last: float):
        self.price    = last
        self.price_ts = time.monotonic()

    def update_candle(self, tf: str, candle: list):
        """
        candle: [ts_ms_str, open, high, low, close, volume, ...]
        Bitget envía la vela actual (puede estar incompleta) con el campo
        'confirm' = 0 (en curso) o 1 (cerrada).
        Reemplazamos la última vela si tiene el mismo ts, o añadimos si es nueva.
        """
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
            dq[-1] = row          # actualizar vela actual en curso
        else:
            dq.append(row)        # vela nueva

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
        """Retorna el último precio recibido por WS, o None si no disponible."""
        c = self._cache.get(symbol)
        return c.price if c else None

    def get_ohlcv(self, symbol: str, tf: str) -> pd.DataFrame:
        """
        Retorna DataFrame OHLCV desde caché WS.
        Mismo formato que ccxt.fetch_ohlcv → compatible con signal_engine._analyze_tf().
        Retorna DataFrame vacío si no hay datos suficientes.
        """
        c = self._cache.get(symbol)
        if not c:
            return pd.DataFrame()
        return c.get_ohlcv_df(tf)

    def has_data(self, symbol: str, tf: str = "15m", min_candles: int = 55) -> bool:
        """True si hay suficientes candles para el signal engine."""
        c = self._cache.get(symbol)
        if not c:
            return False
        return len(c.candles.get(tf, [])) >= min_candles

    def is_price_fresh(self, symbol: str, max_age: float = 10.0) -> bool:
        """True si el precio fue actualizado hace menos de max_age segundos."""
        c = self._cache.get(symbol)
        if not c or c.price is None:
            return False
        return (time.monotonic() - c.price_ts) < max_age

    # ── Control del feed ─────────────────────────────────────────────────────

    def start(self, symbols: List[str]):
        """Arranca el feed WS para la lista de símbolos (formato Bitget: BTCUSDT)."""
        self._symbols = list(symbols)
        for sym in self._symbols:
            if sym not in self._cache:
                self._cache[sym] = _SymbolCache()
        self._running = True
        self._task = asyncio.ensure_future(self._run_loop())
        log.info(f"[WSFeed] Iniciado para {len(self._symbols)} símbolos")

    def update_symbols(self, symbols: List[str]):
        """Añade nuevos símbolos al feed (los existentes siguen activos)."""
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
        """Envía las suscripciones de ticker y candles para todos los símbolos."""
        args = []
        for sym in self._symbols:
            # Ticker
            args.append({
                "instType": "USDT-FUTURES",
                "channel":  "ticker",
                "instId":   sym,
            })
            # Candles 15m / 1h / 4h
            for tf_key in TF_MAP.values():
                args.append({
                    "instType": "USDT-FUTURES",
                    "channel":  tf_key,
                    "instId":   sym,
                })

        # Bitget acepta hasta 100 args por mensaje de suscripción
        for i in range(0, len(args), 100):
            batch = args[i:i + 100]
            payload = json.dumps({"op": "subscribe", "args": batch})
            await ws.send_str(payload)
            log.debug(f"[WSFeed] Suscrito batch {i//100 + 1} ({len(batch)} canales)")

    async def _ping_loop(self, ws):
        """Mantiene la conexión viva enviando 'ping' cada PING_INTERVAL s."""
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

        # Confirmación de suscripción
        if msg.get("event") in ("subscribe", "error"):
            if msg.get("event") == "error":
                log.warning(f"[WSFeed] Suscripción error: {msg}")
            return

        action  = msg.get("action")           # "snapshot" o "update"
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
            # Invertir TF_MAP: "candle15m" → "15m"
            tf = next((k for k, v in TF_MAP.items() if v == channel), None)
            if tf:
                if action == "snapshot":
                    # Snapshot: lista de velas históricas (más antiguas primero)
                    for candle in data:
                        cache.update_candle(tf, candle)
                else:
                    # Update: vela actual (puede ser incompleta)
                    for candle in data:
                        cache.update_candle(tf, candle)

    def _handle_ticker(self, cache: _SymbolCache, data: list):
        try:
            item = data[0] if isinstance(data, list) else data
            last = float(item.get("last") or item.get("lastPr") or 0)
            if last > 0:
                cache.update_price(last)
        except (IndexError, KeyError, ValueError, TypeError):
            pass


# ── Instancia global ──────────────────────────────────────────────────────────
ws_feed = WSFeed()
