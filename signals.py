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
  16. Score mínimo      : LONGs >= MIN_SCORE (dinámico), SHORTs >= MIN_SCORE+SHORT_EXTRA

REGLA FUNDAMENTAL:
  Bear/proto_bear → SOLO SHORT. Bull/proto_bull → SOLO LONG. Sin contra-tendencia.

Fixes aplicados:
  - Bug 1: Eliminado hard-guard duplicado (segundo if nunca alcanzable)
  - Bug 2: _find_swing_highs/_find_swing_lows usan > / < estrictos para evitar
           contar plateaus (velas con mismo high/low) como swings válidos
  - Bug 3: DAILY_CANDLE_GUARD no bloquea días muy fuertes en dirección del régimen;
           en su lugar aplica penalización -10 (igual que DAILY_CANDLE_PENALTY)
  - Bug 4: _liquidity_ok ahora usa 'quote_volume' (volumen en USDT) en lugar de
           'volume' (unidades base). Para BTC, 'volume' es ~0.1-5 por vela, muy
           por debajo del umbral de 1_000_000 USDT → todos los pares fallaban el
           filtro de liquidez. quote_volume = volume * close en USDT.
  - Bug 5: _daily_candle_context ahora usa 'open_time' para filtrar las velas del
           día actual. Antes usaba 'open_time' pero las velas REST no incluían ese
           campo → contexto diario siempre vacío. exchange.py (FIX D) garantiza
           que todas las velas (REST y WS) incluyan 'open_time'.

Pesos v5 (scorer rebalanceado):
  Checks de alta predictividad reciben mayor peso.
  Checks de bajo valor informativo reducidos.
  Nuevo máximo teórico: ~80 puntos.
  MIN_SCORE semana=70, SHORT_MIN_SCORE_EXTRA=6.

Logs:
  - skip rutinarios (range, hard-guard, sobreextendido) → DEBUG
    (no aparecen en producción con nivel INFO)
  - SCORE FINAL y SEÑAL confirmada → INFO siempre
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
SHORT_MIN_SCORE_EXTRA = 6
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

# ── Pesos del scorer (v5) ─────────────────────────────────────────────────
W_ADX_1H_30    = 15
W_ADX_1H_25    = 10
W_ADX_1H_20    =  6
W_MACD_1H      = 12
W_RSI_IDEAL    = 10
W_STRUCTURE    = 12
W_VELA         =  5
W_DIVERGENCIA  =  5
W_HORA_HIGH    =  3
W_MACD_15M     =  8
W_VOLUME_HIGH  =  8
W_VOLUME_LOW   = -4
W_HORA_LOW     = -4
W_RSI_SOBRE    = -8


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
        avg_gain = (avg_gain * (period - 1) + max(delta, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-delta, 0)) / period
        rs = avg_gain / avg_loss if avg_loss else float("inf")
        rsi[i] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + rs))
    return rsi


def _macd_histogram(closes: list[float], fast=12, slow=26, signal=9) -> list[float]:
    ema_fast  = _ema(closes, fast)
    ema_slow  = _ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line  = _ema(macd_line, signal)
    return [m - s for m, s in zip(macd_line, sig_line)]


def _atr(candles: list[dict], period: int = 14) -> float:
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _adx(candles: list[dict], period: int = 14) -> float:
    if len(candles) < period + 2:
        return 0.0
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(candles)):
        h, l   = candles[i]["high"],   candles[i]["low"]
        ph, pl = candles[i-1]["high"], candles[i-1]["low"]
        pc     = candles[i-1]["close"]
        up, down = h - ph, pl - l
        plus_dm.append(up   if up > down and up > 0   else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))

    def _smooth(lst):
        s = sum(lst[:period])
        out = [s]
        for v in lst[period:]:
            s = s - s / period + v
            out.append(s)
        return out

    atr_s = _smooth(tr_list)
    pdm_s = _smooth(plus_dm)
    mdm_s = _smooth(minus_dm)
    dx_vals = []
    for a, p, m in zip(atr_s, pdm_s, mdm_s):
        if a == 0:
            dx_vals.append(0.0)
            continue
        pdi = 100 * p / a
        mdi = 100 * m / a
        dx_vals.append(100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) else 0.0)
    if len(dx_vals) < period:
        return 0.0
    adx = sum(dx_vals[:period]) / period
    for dx in dx_vals[period:]:
        adx = (adx * (period - 1) + dx) / period
    return adx


def _rsi_divergence(closes: list[float], candles: list[dict], lookback: int = 10) -> str | None:
    if len(closes) < lookback + 14 or len(candles) < lookback + 1:
        return None
    rsi_series     = _rsi(closes, 14)
    recent_closes  = closes[-lookback:]
    recent_rsi     = rsi_series[-lookback:]
    recent_candles = candles[-lookback:]

    avg_vol  = sum(c["volume"] for c in recent_candles[:-1]) / max(1, len(recent_candles) - 1)
    last_vol = recent_candles[-1].get("volume", 0.0)
    if avg_vol > 0 and last_vol < avg_vol * 0.6:
        return None

    try:
        lo_val = min(recent_closes[:-1])
        lo_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == lo_val)
        if recent_closes[-1] < lo_val and recent_rsi[-1] > recent_rsi[lo_idx] + 2:
            return "bullish"
    except (ValueError, IndexError):
        pass

    try:
        hi_val = max(recent_closes[:-1])
        hi_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == hi_val)
        if recent_closes[-1] > hi_val and recent_rsi[-1] < recent_rsi[hi_idx] - 2:
            return "bearish"
    except (ValueError, IndexError):
        pass

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
    """Detecta régimen de mercado.

    v4: Devuelve 'proto_bull' o 'proto_bear' cuando la estructura de swings
    confirma la dirección pero las EMAs aún no están completamente ordenadas.
    """
    closes        = [c["close"] for c in candles_1h]
    closes_closed = closes[:-1]
    ema20  = _ema(closes_closed, 20)[-1]
    ema50  = _ema(closes_closed, 50)[-1]
    ema200 = _ema(closes_closed, 200)[-1]
    adx    = _adx(candles_1h[:-1], 14)
    price  = closes[-2] if len(closes) >= 2 else closes[-1]

    if price > ema20 > ema50 > ema200:
        return "bull", adx
    if price < ema20 < ema50 < ema200:
        return "bear", adx
    if price > ema200 and price > ema50 and ema20 > ema200:
        return "bull", adx
    if price < ema200 and price < ema50 and ema20 < ema200:
        return "bear", adx

    if adx >= PROTO_ADX_MIN:
        structure = _price_structure(candles_1h)
        if structure == "bull" and price > ema200:
            log.debug(
                "[regime] proto_bull detectado (structure=bull price=%.6f > EMA200=%.6f adx=%.1f)",
                price, ema200, adx,
            )
            return "proto_bull", adx
        if structure == "bear" and price < ema200:
            log.debug(
                "[regime] proto_bear detectado (structure=bear price=%.6f < EMA200=%.6f adx=%.1f)",
                price, ema200, adx,
            )
            return "proto_bear", adx

    return "range", adx


def _find_swing_highs(highs: list[float], wing: int = 2) -> list[int]:
    pivots = []
    for i in range(wing, len(highs) - wing):
        if all(highs[i] > highs[i - j] for j in range(1, wing + 1)) and \
           all(highs[i] > highs[i + j] for j in range(1, wing + 1)):
            pivots.append(i)
    return pivots


def _find_swing_lows(lows: list[float], wing: int = 2) -> list[int]:
    pivots = []
    for i in range(wing, len(lows) - wing):
        if all(lows[i] < lows[i - j] for j in range(1, wing + 1)) and \
           all(lows[i] < lows[i + j] for j in range(1, wing + 1)):
            pivots.append(i)
    return pivots


def _price_structure(candles_1h: list[dict], lookback: int = STRUCTURE_LOOKBACK) -> str:
    closed = candles_1h[:-1]
    if len(closed) < lookback + 4:
        return "range"

    recent = closed[-(lookback + 4):]
    highs  = [c["high"]  for c in recent]
    lows   = [c["low"]   for c in recent]

    swing_high_idxs = _find_swing_highs(highs, wing=2)
    swing_low_idxs  = _find_swing_lows(lows,  wing=2)

    if len(swing_high_idxs) < 2 or len(swing_low_idxs) < 2:
        return "range"

    swing_high_vals = [highs[i] for i in swing_high_idxs]
    swing_low_vals  = [lows[i]  for i in swing_low_idxs]

    hh_count = sum(
        1 for i in range(1, len(swing_high_vals))
        if swing_high_vals[i] > swing_high_vals[i - 1] * 1.001
    )
    hl_count = sum(
        1 for i in range(1, len(swing_low_vals))
        if swing_low_vals[i] > swing_low_vals[i - 1] * 1.001
    )
    lh_count = sum(
        1 for i in range(1, len(swing_high_vals))
        if swing_high_vals[i] < swing_high_vals[i - 1] * 0.999
    )
    ll_count = sum(
        1 for i in range(1, len(swing_low_vals))
        if swing_low_vals[i] < swing_low_vals[i - 1] * 0.999
    )

    n = SWING_CONFIRM_COUNT

    if hh_count >= n and hl_count >= n:
        return "bull"
    if lh_count >= n and ll_count >= n:
        return "bear"
    return "range"


def _daily_candle_context(candles_1h: list[dict]) -> tuple[float, float]:
    """Devuelve (open_diario, close_actual) usando velas del día UTC actual.

    FIX Bug 5: usa 'open_time' (garantizado por exchange.py FIX D y ws_feed.py
    FIX E) para filtrar las velas del día. Antes las velas REST no incluían
    'open_time' → today_candles siempre vacío → contexto diario siempre (0, 0)
    → penalización daily_candle_context inutilizada.
    """
    now_utc     = datetime.datetime.now(timezone.utc)
    midnight    = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_ts = midnight.timestamp() * 1000

    today_candles = [
        c for c in candles_1h[:-1]
        if c.get("open_time", 0) >= midnight_ts
    ]

    if not today_candles:
        if len(candles_1h) >= 2:
            c = candles_1h[-2]
            return c["open"], c["close"]
        return 0.0, 0.0

    open_daily  = today_candles[0]["open"]
    close_today = today_candles[-1]["close"]
    return open_daily, close_today


def _liquidity_ok(candles_1h: list[dict]) -> bool:
    """Verifica que el par tenga volumen USDT suficiente en 1h.

    FIX Bug 4: usaba 'volume' (unidades base: BTC, ETH, TRX…). Para BTC el
    volumen por vela es ~0.1-5 BTC, muy por debajo del umbral de 1_000_000 USDT
    → todos los pares fallaban el filtro incluso con alta liquidez real.
    Ahora usa 'quote_volume' (volumen en USDT = volume * close), que es el
    valor correcto para comparar contra MIN_HOURLY_VOLUME expresado en USDT.
    """
    recent = candles_1h[-25:-1]
    if not recent:
        return False
    avg_vol = sum(c.get("quote_volume", c.get("volume", 0.0) * c.get("close", 1.0))
                  for c in recent) / len(recent)
    ok = avg_vol >= MIN_HOURLY_VOLUME
    if not ok:
        log.debug(
            "[liquidity] skip — quote_volume_avg_1h=%.0f < umbral %d USDT",
            avg_vol, MIN_HOURLY_VOLUME,
        )
    return ok


def _dynamic_min_score_bump(candles_1h: list[dict], price: float) -> int:
    if price <= 0 or len(candles_1h) < 16:
        return 0
    atr_1h = _atr(candles_1h[:-1], period=14)
    if atr_1h <= 0:
        return 0
    atr_pct = atr_1h / price
    if atr_pct > ATR_HIGH_VOL_PCT:
        log.debug(
            "[vol] ATR_1h=%.4f%% > %.1f%% → min_score +%d (alta volatilidad)",
            atr_pct * 100, ATR_HIGH_VOL_PCT * 100, ATR_HIGH_VOL_BUMP,
        )
        return ATR_HIGH_VOL_BUMP
    if atr_pct < ATR_LOW_VOL_PCT:
        log.debug(
            "[vol] ATR_1h=%.4f%% < %.1f%% → min_score +%d (baja volatilidad)",
            atr_pct * 100, ATR_LOW_VOL_PCT * 100, ATR_LOW_VOL_BUMP,
        )
        return ATR_LOW_VOL_BUMP
    return 0
