#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada público para main.py.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Callable, Optional

from bot.core.hl_client import HLClient, _norm_coin
from bot.core.trading_loop import TradingLoop

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

# Cuántas velas pedir por timeframe
_OHLCV_BARS = int(os.getenv("BARS_NEEDED", "100"))

# Mapeo timeframe → intervalo en minutos para candle_snapshot
_TF_MINUTES = {
    "1m":  1,
    "3m":  3,
    "5m":  5,
    "15m": 15,
    "30m": 30,
    "1h":  60,
    "2h":  120,
    "4h":  240,
    "8h":  480,
    "1d":  1440,
}


class FuturesTrader:
    """
    Orquestador principal de un par de trading en Hyperliquid.
    """

    def __init__(
        self,
        api_key: Optional[str],
        api_secret: str,
        passphrase: Optional[str],
        symbol: str,
        leverage: int = 5,
        margin_mode: str = "isolated",
        dry_run: bool = True,
    ) -> None:
        self.symbol      = symbol
        self.coin        = _norm_coin(symbol)
        self.leverage    = leverage
        self.margin_mode = margin_mode
        self.dry_run     = dry_run

        self.position:        Optional[str]   = None
        self.entry_price:     Optional[float] = None
        self.sl:              Optional[float] = None
        self.tp1:             Optional[float] = None
        self.tp2:             Optional[float] = None
        self.tp3:             Optional[float] = None
        self.tp2_hit:         bool            = False
        self._open_notional:  float           = 0.0
        self._open_leverage:  int             = leverage
        self._protection_ok:  bool            = False
        self._tp1_be_done:    bool            = False

        self._api_key    = api_key or ""
        self._api_secret = api_secret or ""

        self._hl_client = HLClient(symbol)

        self._master_addr = self._hl_client._account_addr
        self._agent_mode  = self._hl_client._agent_mode

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)
        self._ccxt_exchange  = None

    # ── Interfaz pública requerida por main.py ──────────────────────

    async def run(self, risk, *, global_risk=None) -> None:
        try:
            await self._trading_loop.run(self, risk, global_risk=global_risk)
        except asyncio.CancelledError:
            logger.info("[%s] FuturesTrader cancelado.", self.symbol)
        finally:
            self._stopped_event.set()

    async def cleanup(self) -> None:
        try:
            from bot.ai_trader import close_sessions
            await close_sessions()
        except Exception as e:
            logger.debug("[%s] cleanup ai_trader sessions: %s", self.symbol, e)
        self._stopped_event.set()

    # ── Métodos que TradingLoop llama sobre el objeto trader ────────

    async def _get_ccxt(self) -> None:
        pass

    async def get_price(self) -> float:
        import aiohttp
        import json as _json

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = _json.loads(await resp.text())

        price = data.get(self.coin)
        if price is None:
            raise ValueError(f"[{self.symbol}] Precio no encontrado en allMids")
        return float(price)

    async def get_ohlcv(self, timeframe: str) -> list:
        """
        Descarga velas OHLCV desde Hyperliquid /info candle_snapshot.
        Devuelve lista de [timestamp, open, high, low, close, volume] compatible
        con signal_engine._compute_indicators().

        Este método es el ohlcv_fn que se pasa a analyze_pair() — reemplaza
        la lista vacía [] que antes causaba que el bot nunca escaneara señales.
        """
        import aiohttp
        import json as _json
        import time as _time

        interval = _TF_MINUTES.get(timeframe, 15)
        # Pedir un poco más de barras para tener margen de indicadores
        n = _OHLCV_BARS + 20
        # end = ahora (ms), start = n velas atrás
        end_ms   = int(_time.time() * 1000)
        start_ms = end_ms - (n * interval * 60 * 1000)

        payload = {
            "type":      "candleSnapshot",
            "req": {
                "coin":       self.coin,
                "interval":   timeframe,
                "startTime":  start_ms,
                "endTime":    end_ms,
            },
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{_API_URL}/info",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    raw = _json.loads(await resp.text())
        except Exception as e:
            logger.warning("[%s] get_ohlcv(%s) error: %s", self.symbol, timeframe, e)
            return []

        if not isinstance(raw, list):
            logger.warning("[%s] get_ohlcv(%s) respuesta inesperada: %s", self.symbol, timeframe, raw)
            return []

        # Hyperliquid candle_snapshot devuelve lista de dicts:
        # {t, T, s, i, o, h, l, c, v, n}
        # Convertir a formato OHLCV estándar: [ts, open, high, low, close, volume]
        bars = []
        for candle in raw:
            try:
                bars.append([
                    int(candle["t"]),
                    float(candle["o"]),
                    float(candle["h"]),
                    float(candle["l"]),
                    float(candle["c"]),
                    float(candle["v"]),
                ])
            except (KeyError, TypeError, ValueError):
                continue

        logger.debug(
            "[%s] get_ohlcv(%s): %d velas descargadas",
            self.symbol, timeframe, len(bars),
        )
        return bars

    def get_ohlcv_fn(self) -> Callable:
        """
        Devuelve un callable async compatible con analyze_pair(ohlcv_fn=...).
        El signal_engine llama a ohlcv_fn(timeframe) para cada TF.
        """
        async def _fn(tf: str) -> list:
            return await self.get_ohlcv(tf)
        return _fn

    async def _get_positions(self) -> list[dict]:
        import aiohttp
        import json as _json

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "clearinghouseState", "user": self._master_addr},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = _json.loads(await resp.text())

        result = []
        for p in data.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("coin", "").upper() != self.coin.upper():
                continue
            try:
                szi = float(pos.get("szi", 0) or 0)
            except (TypeError, ValueError):
                continue
            if abs(szi) == 0:
                continue
            result.append({
                "side":    "long" if szi > 0 else "short",
                "size":    abs(szi),
                "entryPx": float(pos.get("entryPx") or 0),
                "coin":    pos.get("coin", ""),
            })
        return result

    async def _set_leverage(self, leverage: int) -> None:
        if self.dry_run:
            logger.info("[%s] DRY_RUN: _set_leverage(%d) omitido.", self.symbol, leverage)
            return
        try:
            result = self._hl_client._exchange.update_leverage(
                leverage, self.coin, is_cross=False
            )
            logger.info("[%s] Leverage configurado a %dx: %s", self.symbol, leverage, result)
        except Exception as e:
            logger.warning("[%s] No se pudo configurar leverage: %s", self.symbol, e)

    def _info_post(self, payload: dict) -> dict:
        return self._hl_client._info._session.post(
            f"{_API_URL}/info", json=payload
        ).json()


__all__ = ["FuturesTrader"]
