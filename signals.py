"""signals.py — Sistema de señales v4.

Nuevas mejoras v4:
  A. Proto-bull / proto-bear detection
     El bot ya no espera a que las EMAs estén completamente ordenadas para
     detectar un cambio de régimen. Si la estructura de swings confirma bull
     (2 HH+HL) Y el precio está por encima de la EMA200_1h Y el ADX_1h >= 22,
     se declara régimen 'proto_bull' (análogo para 'proto_bear').
     Captura entradas al INICIO de tendencia, no cuando ya ha corrido varios %.

     proto_bull: estructura=bull, precio > EMA200_1h, ADX_1h >= 22
                 (las EMAs aún no están en orden price>20>50>200)
     proto_bear: estructura=bear, precio < EMA200_1h, ADX_1h >= 22

     El proto-régimen penaliza -4 puntos en scoring (menos convicción),
     y exige score mínimo +4 extra para compensar.

  B. Score mínimo dinámico por volatilidad (ATR)
     En días de alta volatilidad (ATR_1h > 2% del precio) el MIN_SCORE
     sube automáticamente +8 puntos. Reduce SLs en días difíciles.
     En días de muy baja volatilidad (ATR_1h < 0.5%) sube +4 (mercado
     adormecido → señales falsas frecuentes).

  C. Penalización contradicción mantenida en -12 (v3)

Mejoras estructurales v3 (heredadas):
  A. Estructura de precio 1h con swing highs/lows reales
  B. Hard-guard ADX en rango lateral
  C. Contexto de vela diaria
  D. Filtro de liquidez del par
  E. Penalización por alejamiento del open diario

Filtros heredados:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : EMAs 1h + estructura HH/HL (swings reales) + proto-bull/bear
  3. Macro 4h           : EMA50 en 4h
  4. EMA200 1h          : hard-guard de dirección
  5. EMA200 15m         : hard-guard en SHORTs
  6. ATR volátil        : hard-guard >3.5%
  7. ADX 15m            : hard-guard <20
  8. ADX 1h             : scoring + hard-guard SHORT
  9. RSI 15m            : scoring + hard-guard SHORT sobrevendido
  10. MACD 15m + 1h     : scoring
  11. Volumen 15m       : scoring
  12. Divergencia RSI   : scoring
  13. Sesgo horario     : scoring
  14. No-chase          : hard-guard rango vela
  15. Pullback EMA20    : hard-guard sobreextensión 15m
  16. Score mínimo      : LONGs >= MIN_SCORE (dinámico), SHORTs >= MIN_SCORE+8

REGLA FUNDAMENTAL:
  Bear/proto_bear → SOLO SHORT. Bull/proto_bull → SOLO LONG. Sin contra-tendencia.

Fixes aplicados:
  - Bug 1: Eliminado hard-guard duplicado (segundo if nunca alcanzable)
  - Bug 2: _find_swing_highs/_find_swing_lows usan > / < estrictos para evitar
           contar plateaus (velas con mismo high/low) como swings válidos
  - Bug 3: DAILY_CANDLE_GUARD no bloquea días muy fuertes en dirección del régimen;
           en su lugar aplica penalización -10 (igual que DAILY_CANDLE_PENALTY)

v4.1:
  - evaluate() devuelve 4 valores: (side, score, regime, metrics)
    donde metrics = {adx_1h, adx_15m, vol_ratio, atr_1h_pct, rsi, avg_vol, last_vol}
    Permite que main.py logee ADX y volumen en nivel INFO sin depender de DEBUG.
"""
from __future__ import annotations
import logging
import datetime
from datetime import timezone

import config

log = logging.getLogger("signals")

# ── Umbrales configurables ────────────────────────────────────────────────
EMA200_MIN_DIST     = 0.003
NO_CHASE_MULT       = 2.0
VOLUME_MULT         = 1.2
VOLUME_WEAK         = 0.8
MIN_SCORE           = config.MIN_SCORE

PULLBACK_EMA20_DIST = 0.015

REGIME_CONFIRM_BARS   = 3
SHORT_MIN_SCORE_EXTRA = 8
ATR_VOLATILE_PCT      = 0.035
ADX_15M_MIN           = 20

HIGH_BIAS_HOURS = {8, 9, 10, 13, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS  = {2, 3, 4, 5}

# ── Umbrales v2 ───────────────────────────────────────────────────────────
STRUCTURE_LOOKBACK    = 8
DAILY_CANDLE_BLOCK    = 0.015
DAILY_CANDLE_PENALTY  = 0.025
DAILY_CANDLE_GUARD    = 0.040
MIN_HOURLY_VOLUME     = 1_000_000

# ── Umbrales v3 ───────────────────────────────────────────────────────────
ADX_1H_STRUCTURE_MIN  = 25
SWING_CONFIRM_COUNT   = 2

# ── Umbrales v4 (nuevos) ──────────────────────────────────────────────────
PROTO_ADX_MIN         = 22
PROTO_SCORE_PENALTY   = 4
PROTO_MIN_SCORE_EXTRA = 4

ATR_HIGH_VOL_PCT      = 0.020
ATR_LOW_VOL_PCT       = 0.005
ATR_HIGH_VOL_BUMP     = 8
ATR_LOW_VOL_BUMP      = 4


# ── Indicadores ──────────────────────────────────────────────────────────

def _ema(closes: list[float], period: int) -> list[float]:
    if not closes:
        return []
    k = 2 / (period + 1)
    emas = [closes[0]]
    for c in closes[1:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas


def _rsi(closes: list[float], period: int = 14) -> list[float]:
    rsi = [50.0] * len(closes)
    if len(closes) < period + 1:
        return rsi
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    avg_gain = sum(d for d in deltas[:period] if d > 0) / period
    avg_loss = sum(-d for d in deltas[:period] if d < 0) / period
    for i in range(period, len(closes)):
        delta = deltas[i - 1]