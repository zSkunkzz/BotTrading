#!/usr/bin/env python3
"""
market_regime.py — Filtro de régimen de mercado basado en BTC

Semáforo global:
  GREEN  → BTC en tendencia clara (ADX >= threshold, EMA alineadas)
  YELLOW → BTC en zona gris (ADX moderado o divergencia)
  RED    → BTC en chop / reversión → pausar todos los pares

Activar con REGIME_FILTER=true (default: true)
Umbral ADX BTC: REGIME_ADX_MIN (default: 18)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

log = logging.getLogger(__name__)

REGIME_FILTER      = os.getenv("REGIME_FILTER", "true").lower() != "false"
REGIME_ADX_MIN     = float(os.getenv("REGIME_ADX_MIN", "18"))
REGIME_CACHE_TTL   = int(os.getenv("REGIME_CACHE_TTL", "300"))  # 5 min


class MarketRegime:
    """
    Evalúa el régimen de BTC cada REGIME_CACHE_TTL segundos.
    Expone `is_tradeable() -> bool` para que decision_engine lo consulte.
    """

    def __init__(self) -> None:
        self._regime: str = "GREEN"     # GREEN / YELLOW / RED
        self._btc_adx: float = 0.0
        self._btc_trend: int = 0        # 1 long, -1 short, 0 neutral
        self._last_update: float = 0.0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_tradeable(self) -> bool:
        """True si el régimen permite abrir nuevas posiciones."""
        if not REGIME_FILTER:
            return True
        return self._regime in ("GREEN", "YELLOW")

    def regime(self) -> str:
        return self._regime

    def btc_adx(self) -> float:
        return self._btc_adx

    def btc_trend(self) -> int:
        return self._btc_trend

    def summary(self) -> str:
        icon = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(self._regime, "⚪")
        return f"{icon} Régimen BTC: {self._regime} · ADX {self._btc_adx:.1f} · Trend {self._btc_trend:+d}"

    async def refresh(self, exch=None) -> None:
        """Actualiza el régimen si el caché expiró."""
        now = time.monotonic()
        if now - self._last_update < REGIME_CACHE_TTL:
            return
        async with self._lock:
            if now - self._last_update < REGIME_CACHE_TTL:
                return
            await self._compute(exch)
            self._last_update = time.monotonic()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _compute(self, exch=None) -> None:
        try:
            import numpy as np
            import pandas as pd

            # Intentar desde ws_feed primero
            df1h: Optional[pd.DataFrame] = None
            try:
                from bot.ws_feed import ws_feed
                df1h = ws_feed.get_ohlcv("BTC", "1h")
                if df1h is not None and len(df1h) < 50:
                    df1h = None
            except Exception:
                pass

            if df1h is None or df1h.empty:
                # Fallback: REST HL
                from bot.signal_engine import _fetch_ohlcv_hl
                df1h = await _fetch_ohlcv_hl("BTC", "1h", 200)

            if df1h is None or df1h.empty or len(df1h) < 50:
                log.warning("[regime] Sin datos BTC 1h — régimen GREEN por defecto")
                return

            adx_val = self._calc_adx(df1h)
            trend   = self._calc_ema_trend(df1h)

            self._btc_adx   = adx_val
            self._btc_trend = trend

            if adx_val < REGIME_ADX_MIN:
                self._regime = "RED"
            elif adx_val < REGIME_ADX_MIN + 5:
                self._regime = "YELLOW"
            else:
                self._regime = "GREEN"

            log.info("[regime] %s", self.summary())

        except Exception as e:
            log.warning("[regime] Error calculando régimen BTC: %s", e)

    @staticmethod
    def _calc_adx(df, period: int = 14) -> float:
        try:
            import ta
            val = ta.trend.ADXIndicator(
                df["high"], df["low"], df["close"], window=period
            ).adx().iloc[-1]
            import numpy as np
            return round(float(val), 1) if not np.isnan(val) else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def _calc_ema_trend(df) -> int:
        try:
            import ta
            c   = df["close"]
            e9  = ta.trend.ema_indicator(c, window=9).iloc[-1]
            e21 = ta.trend.ema_indicator(c, window=21).iloc[-1]
            e50 = ta.trend.ema_indicator(c, window=50).iloc[-1]
            cl  = c.iloc[-1]
            if e9 > e21 > e50 and cl > e50:
                return 1
            if e9 < e21 < e50 and cl < e50:
                return -1
            return 0
        except Exception:
            return 0


# Singleton
market_regime = MarketRegime()
