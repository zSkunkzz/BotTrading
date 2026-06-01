#!/usr/bin/env python3
"""
market_regime.py — Detección de régimen de mercado (TRENDING / RANGING / VOLATILE)

MEJORAS v3:
  - MarketRegimeSingleton: clase con .refresh(exch) / .regime() / .btc_trend()
    compatible con la API que usa decision_engine.py.
  - verify_regime_gate(): función libre para uso directo con DataFrame.
  - detect_regime(): función libre (sin cambios).

Config Railway:
  MARKET_REGIME_GATE     → default true  (false = solo informativo, no bloquea)
  REGIME_FILTER          → alias de MARKET_REGIME_GATE (decision_engine usa esta)
  REGIME_ADX_TREND       → ADX mínimo para TRENDING (default 25)
  REGIME_ADX_RANGING     → ADX máximo para RANGING (default 20)
  REGIME_BB_WIDTH_FACTOR → factor ancho BB para VOLATILE (default 2.0)
  REGIME_BTC_SYMBOL      → símbolo BTC para btc_trend (default BTC/USDC:USDC)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import numpy as np
import pandas as pd

try:
    import ta as ta_lib
except ImportError:
    ta_lib = None

log = logging.getLogger(__name__)

MARKET_REGIME_GATE     = (
    os.getenv("MARKET_REGIME_GATE", "true").lower() != "false"
    or os.getenv("REGIME_FILTER",   "false").lower() == "true"
)
REGIME_ADX_TREND       = float(os.getenv("REGIME_ADX_TREND",       "25"))
REGIME_ADX_RANGING     = float(os.getenv("REGIME_ADX_RANGING",     "20"))
REGIME_BB_WIDTH_FACTOR = float(os.getenv("REGIME_BB_WIDTH_FACTOR", "2.0"))
REGIME_BTC_SYMBOL      = os.getenv("REGIME_BTC_SYMBOL", "BTC/USDC:USDC")

# Mapeo interno → string simplificado que usa decision_engine ("GREEN"/"YELLOW"/"RED")
_REGIME_TO_SIGNAL = {
    "TRENDING": "GREEN",
    "VOLATILE": "YELLOW",
    "RANGING":  "RED",
    "UNKNOWN":  "GREEN",   # fail-open: si no hay datos, no bloqueamos
}


def detect_regime(df: pd.DataFrame) -> str:
    """
    Detecta el régimen de mercado actual.

    Returns:
        'TRENDING' | 'RANGING' | 'VOLATILE' | 'UNKNOWN'
    """
    if ta_lib is None or df is None or df.empty or len(df) < 30:
        return "UNKNOWN"

    try:
        adx_ind = ta_lib.trend.ADXIndicator(
            df["high"], df["low"], df["close"], window=14
        )
        adx = float(adx_ind.adx().iloc[-1])

        bb = ta_lib.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        bbw    = float((bb.bollinger_hband().iloc[-1] - bb.bollinger_lband().iloc[-1])
                       / bb.bollinger_mavg().iloc[-1])
        bbw_ma = float(
            ((bb.bollinger_hband() - bb.bollinger_lband()) / bb.bollinger_mavg())
            .rolling(50).mean().iloc[-1]
        )

        if np.isnan(adx):
            return "UNKNOWN"

        if not np.isnan(bbw_ma) and bbw > bbw_ma * REGIME_BB_WIDTH_FACTOR:
            return "VOLATILE"

        if adx >= REGIME_ADX_TREND:
            return "TRENDING"

        if adx < REGIME_ADX_RANGING:
            return "RANGING"

        return "TRENDING"  # zona intermedia: tratar como tendencia débil

    except Exception as e:
        log.debug("[regime] detect_regime error: %s", e)
        return "UNKNOWN"


def verify_regime_gate(df: pd.DataFrame, symbol: str = "") -> tuple[bool, str]:
    """
    Gate bloqueante para uso directo con DataFrame.

    Returns:
        (allowed, reason)
    """
    if not MARKET_REGIME_GATE:
        return True, ""

    regime = detect_regime(df)

    if regime == "RANGING":
        reason = "market_regime=RANGING (ADX bajo) — entrada bloqueada"
        log.info("[regime] %s %s", symbol, reason)
        return False, reason

    log.debug("[regime] %s regime=%s → permitido", symbol, regime)
    return True, ""


class MarketRegimeSingleton:
    """
    Singleton con la API que consume decision_engine.py:

        await market_regime.refresh(exch=ccxt_exchange)
        regime = market_regime.regime()          # "GREEN" | "YELLOW" | "RED"
        btc    = market_regime.btc_trend()       # +1 | 0 | -1

    refresh() obtiene OHLCV 1h de BTC del exchange y actualiza la caché interna.
    regime() y btc_trend() son síncronos — usan la caché.
    """

    def __init__(self) -> None:
        self._last_regime: str = "UNKNOWN"        # TRENDING/RANGING/VOLATILE/UNKNOWN
        self._last_signal: str = "GREEN"          # GREEN/YELLOW/RED
        self._btc_trend:   int = 0                # +1 long, -1 short, 0 neutral
        self._df_btc: Optional[pd.DataFrame] = None

    async def refresh(self, exch=None, df: Optional[pd.DataFrame] = None) -> None:
        """
        Actualiza caché de régimen.

        Acepta:
          - exch: instancia ccxt/exchange con .fetch_ohlcv(symbol, tf, limit=N)
          - df:   DataFrame preformateado con columnas open/high/low/close/volume
                  (alternativa para tests o cuando ya se tiene el DataFrame)
        """
        try:
            if df is not None:
                self._df_btc = df
            elif exch is not None:
                raw = await exch.fetch_ohlcv(REGIME_BTC_SYMBOL, "1h", limit=200)
                self._df_btc = pd.DataFrame(
                    raw, columns=["ts", "open", "high", "low", "close", "volume"]
                )
            else:
                log.debug("[regime] refresh() sin exch ni df — usando caché anterior")
                return

            self._last_regime = detect_regime(self._df_btc)
            self._last_signal = _REGIME_TO_SIGNAL.get(self._last_regime, "GREEN")

            # btc_trend: EMA20 vs EMA50 en cierre
            if self._df_btc is not None and len(self._df_btc) >= 50:
                closes = self._df_btc["close"]
                ema20 = float(closes.ewm(span=20).mean().iloc[-1])
                ema50 = float(closes.ewm(span=50).mean().iloc[-1])
                if ema20 > ema50 * 1.002:
                    self._btc_trend = 1
                elif ema20 < ema50 * 0.998:
                    self._btc_trend = -1
                else:
                    self._btc_trend = 0
            else:
                self._btc_trend = 0

            log.debug(
                "[regime] BTC regime=%s signal=%s btc_trend=%+d",
                self._last_regime, self._last_signal, self._btc_trend,
            )

        except Exception as e:
            log.warning("[regime] refresh() error: %s — manteniendo caché anterior", e)

    def regime(self) -> str:
        """Devuelve señal simplificada: 'GREEN' | 'YELLOW' | 'RED'."""
        return self._last_signal

    def regime_raw(self) -> str:
        """Devuelve régimen crudo: 'TRENDING' | 'RANGING' | 'VOLATILE' | 'UNKNOWN'."""
        return self._last_regime

    def btc_trend(self) -> int:
        """Devuelve tendencia BTC: +1 (alcista), -1 (bajista), 0 (neutral)."""
        return self._btc_trend

    def is_ranging(self) -> bool:
        return self._last_regime == "RANGING"

    def is_gate_blocked(self) -> bool:
        """True si MARKET_REGIME_GATE activo y régimen bloquea entradas."""
        return MARKET_REGIME_GATE and self._last_regime == "RANGING"


# Singleton global — decision_engine importa este objeto
market_regime = MarketRegimeSingleton()
