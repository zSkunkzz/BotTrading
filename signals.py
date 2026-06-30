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

     El proto-régimen penaliza -4 puntos en scoring (menos convicción).
     PROTO_MIN_SCORE_EXTRA eliminado (era doble penalización).

  B. Score mínimo dinámico por volatilidad (ATR)
     En días de alta volatilidad (ATR_1h > 2% del precio) el MIN_SCORE
     sube automáticamente +5 puntos (era +8, reducido para no ser prohibitivo).
     En días de muy baja volatilidad (ATR_1h < 0.5%) sube +4 (mercado
     adormecido → señales falsas frecuentes).

  C. Penalización contradicción estructura: -8 (simétrica con W_STRUCTURE=+8)

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
  7. ADX 15m            : hard-guard <18 (era <20)
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
  - Bug 6: daily_move convertido de hard-guard a penalización (-6).
           Un pullback intradía de 1.5-2.5% en bull fuerte es una entrada
           potencial. El scorer decide si el setup sigue siendo válido.

Pesos v5.2 (scorer rebalanceado + fixes anti-sequía):
  - W_RSI_IDEAL   : 10 → 12
  - W_STRUCTURE   : 12 →  8 (menos bloqueante cuando swings no confirmados)
  - W_VOLUME_HIGH :  8 → 10
  - W_VELA        :  5 →  4 (devuelto parcialmente; confirmación de alineación)
  - W_HORA_HIGH   :  3 →  5
  - W_DIVERGENCIA :  5 →  8 (señal rara pero muy predictiva)
  - W_ADX_1H_20   :  6 →  3 (ADX 20-25 es señal débil)
  - ATR_HIGH_VOL_BUMP: 8 → 5 (mínimo 75 es alcanzable)
  - Penalización structure contraria: -12 → -8 (simétrico con W_STRUCTURE)
  Nuevo máximo teórico: ~82 puntos.
  MIN_SCORE semana=70, SHORT_MIN_SCORE_EXTRA=6.

Fixes v5.2:
  - PROTO_MIN_SCORE_EXTRA: 4 → 0 (doble penalización eliminada)
  - ADX_15M_MIN: 20 → 18 (hard-guard menos agresivo)
  - SWING_CONFIRM_COUNT: 2 → 1 (estructura detecta tendencias más tempranas)
  - W_VELA: 0 → 4 (confirmación de vela devuelta parcialmente)
  - RSI zona ideal: bull 45-65 → 40-68 / bear 35-55 → 32-58

Fixes v5.3:
  - MIN_HOURLY_VOLUME: 1_000_000 → 500_000 USDT
    Con posiciones de 20 USDT × 10x = 200 USDT, un volumen 1h de 500K
    es más que suficiente para entrar/salir sin deslizamiento.
    El umbral de 1M excluía pares líquidos como LINK, NEAR, 1KSHIB.

Structure lookback v5.1:
  - STRUCTURE_LOOKBACK: 8 → 12 (más ventana para confirmar HH+HL)
    Con 8 velas, tendencias de 6-7h perdían sus swings más antiguos
    y volvían a 'range' aunque la tendencia siguiera activa.

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
ADX_15M_MIN           = 18   # era 20 — hard-guard menos agresivo

HIGH_BIAS_HOURS = {8, 9, 10, 13, 14, 15, 16, 20, 21}
LOW_BIAS_HOURS  = {2, 3, 4, 5}

# ── Umbrales v2 ───────────────────────────────────────────────────────────
STRUCTURE_LOOKBACK    = 12
DAILY_CANDLE_BLOCK    = 0.015
DAILY_CANDLE_PENALTY  = 0.025
DAILY_CANDLE_GUARD    = 0.040
DAILY_CANDLE_BLOCK_PEN = 6
MIN_HOURLY_VOLUME     = 500_000   # era 1_000_000 — v5.3

# ── Umbrales v3 ───────────────────────────────────────────────────────────
ADX_1H_STRUCTURE_MIN  = 25
SWING_CONFIRM_COUNT   = 1   # era 2 — detecta tendencias más tempranas

# ── Umbrales v4 (nuevos) ──────────────────────────────────────────────────
PROTO_ADX_MIN         = 22
PROTO_SCORE_PENALTY   = 4
PROTO_MIN_SCORE_EXTRA = 0   # era 4 — doble penalización eliminada (ya penalizado en score)

ATR_HIGH_VOL_PCT      = 0.020
ATR_LOW_VOL_PCT       = 0.005
ATR_HIGH_VOL_BUMP     = 5
ATR_LOW_VOL_BUMP      = 4

# ── Pesos del scorer (v5.2) ───────────────────────────────────────────────
W_ADX_1H_30    = 15
W_ADX_1H_25    = 10
W_ADX_1H_20    =  3
W_MACD_1H      = 12
W_RSI_IDEAL    = 12
W_STRUCTURE    =  8
W_VELA         =  4   # era 0 — devuelto parcialmente como confirmación de alineación
W_DIVERGENCIA  =  8
W_HORA_HIGH    =  5
W_MACD_15M     =  8
W_VOLUME_HIGH  = 10
W_VOLUME_LOW   = -4
W_HORA_LOW     = -4
W_RSI_SOBRE    = -8
W_STRUCTURE_CONTRA = -8


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

    n = SWING_CONFIRM_COUNT  # era 2, ahora 1

    if hh_count >= n and hl_count >= n:
        return "bull"
    if lh_count >= n and ll_count >= n:
        return "bear"
    return "range"


def _daily_candle_context(candles_1h: list[dict]) -> tuple[float, float]:
    """Devuelve (open_diario, close_actual) usando velas del día UTC actual."""
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
    """Verifica que el par tenga volumen USDT suficiente en 1h."""
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


def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int = MIN_SCORE,
    symbol:      str = "???",
) -> tuple[str | None, int, str | None]:
    """Evalúa si hay señal de entrada. Devuelve (side, score, regime) o (None, score, None)."""

    if len(candles_15m) < 50 or len(candles_1h) < 220:
        log.debug("[%s] skip: candles insuficientes (15m=%d 1h=%d)", symbol, len(candles_15m), len(candles_1h))
        return None, 0, None

    if not _liquidity_ok(candles_1h):
        log.debug("[%s] skip: liquidez insuficiente (avg_vol_1h < %d)", symbol, MIN_HOURLY_VOLUME)
        return None, 0, None

    closed  = candles_15m[-2]
    c_open  = closed["open"]
    c_close = closed["close"]
    c_high  = closed["high"]
    c_low   = closed["low"]
    bullish_candle = c_close > c_open

    # ── Régimen de mercado 1h ────────────────────────────────────────────
    regime, adx_1h = _market_regime(candles_1h)

    is_proto = regime in ("proto_bull", "proto_bear")
    effective_regime = "bull" if regime in ("bull", "proto_bull") else (
        "bear" if regime in ("bear", "proto_bear") else "range"
    )

    if effective_regime == "range":
        log.debug("[%s] skip: régimen=range adx_1h=%.1f", symbol, adx_1h)
        return None, 0, None

    # ── Estructura de precio 1h ──────────────────────────────────────────
    structure = _price_structure(candles_1h)

    if structure == "range" and adx_1h < ADX_1H_STRUCTURE_MIN:
        log.debug(
            "[%s] skip: structure=range ADX_1h=%.1f < %d (hard-guard)",
            symbol, adx_1h, ADX_1H_STRUCTURE_MIN,
        )
        return None, 0, None

    # ── Macro 4h ─────────────────────────────────────────────────────────
    if candles_4h and len(candles_4h) >= 55:
        closes_4h = [c["close"] for c in candles_4h[:-1]]
        ema50_4h  = _ema(closes_4h, 50)[-1]
        price_4h  = closes_4h[-1]
        if effective_regime == "bull" and price_4h < ema50_4h:
            log.debug("[%s] skip: macro 4h bearish (precio=%.6f < EMA50_4h=%.6f)", symbol, price_4h, ema50_4h)
            return None, 0, None
        if effective_regime == "bear" and price_4h > ema50_4h:
            log.debug("[%s] skip: macro 4h bullish (precio=%.6f > EMA50_4h=%.6f)", symbol, price_4h, ema50_4h)
            return None, 0, None

    # ── Indicadores 15m ──────────────────────────────────────────────────
    closes_15m = [c["close"] for c in candles_15m]
    price      = closes_15m[-2]

    ema20_15m  = _ema(closes_15m[:-1], 20)[-1]
    ema200_15m = _ema(closes_15m[:-1], 200)[-1]
    rsi_series = _rsi(closes_15m[:-1], 14)
    rsi        = rsi_series[-1]
    macd_hist  = _macd_histogram(closes_15m[:-1])[-1]
    atr_15m    = _atr(candles_15m[:-1], 14)
    adx_15m    = _adx(candles_15m[:-1], 14)
    volumes    = [c["volume"] for c in candles_15m]
    avg_vol    = sum(volumes[-21:-1]) / 20
    last_vol   = volumes[-2]
    vol_ratio  = last_vol / avg_vol if avg_vol else 0.0

    closes_1h        = [c["close"] for c in candles_1h]
    ema200_1h        = _ema(closes_1h[:-1], 200)[-1]
    closes_1h_closed = closes_1h[:-1]
    macd_1h          = _macd_histogram(closes_1h_closed)[-1]
    atr_1h_val       = _atr(candles_1h[:-1], 14)
    atr_1h_pct       = atr_1h_val / price if price > 0 else 0.0

    log.debug(
        "[%s] régimen=%s structure=%s | ADX_1h=%.1f ADX_15m=%.1f | "
        "RSI=%.1f MACD_15m=%.5f MACD_1h=%.5f | "
        "vol_ratio=%.2f (last=%.0f avg=%.0f) | "
        "ATR_15m=%.4f%% ATR_1h=%.4f%% | "
        "precio=%.6f EMA200_1h=%.6f EMA20_15m=%.6f",
        symbol, regime, structure,
        adx_1h, adx_15m,
        rsi, macd_hist, macd_1h,
        vol_ratio, last_vol, avg_vol,
        (atr_15m / price * 100) if price > 0 else 0,
        atr_1h_pct * 100,
        price, ema200_1h, ema20_15m,
    )

    # ── Hard-guards ───────────────────────────────────────────────────────
    if price > 0 and atr_15m / price > ATR_VOLATILE_PCT:
        log.debug("[%s] skip: ATR_15m=%.4f%% > %.1f%% (demasiado volátil)", symbol, atr_15m / price * 100, ATR_VOLATILE_PCT * 100)
        return None, 0, None

    if adx_15m < ADX_15M_MIN:
        log.debug("[%s] skip: ADX_15m=%.1f < %d (lateral 15m)", symbol, adx_15m, ADX_15M_MIN)
        return None, 0, None

    if effective_regime == "bull" and price < ema200_1h * (1 - EMA200_MIN_DIST):
        log.debug("[%s] skip: bull pero precio=%.6f < EMA200_1h=%.6f", symbol, price, ema200_1h)
        return None, 0, None
    if effective_regime == "bear" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        log.debug("[%s] skip: bear pero precio=%.6f > EMA200_1h=%.6f", symbol, price, ema200_1h)
        return None, 0, None

    if effective_regime == "bear" and price < ema200_15m * (1 - EMA200_MIN_DIST):
        log.debug("[%s] skip: bear pero precio=%.6f < EMA200_15m=%.6f (sobreextendido)", symbol, price, ema200_15m)
        return None, 0, None

    candle_range = c_high - c_low
    if candle_range > 0 and atr_15m > 0:
        if candle_range > NO_CHASE_MULT * atr_15m:
            log.debug("[%s] skip: no-chase rango_vela=%.6f > %.1f*ATR=%.6f", symbol, candle_range, NO_CHASE_MULT, atr_15m)
            return None, 0, None

    if price > 0 and ema20_15m > 0:
        dist_ema20 = abs(price - ema20_15m) / price
        if dist_ema20 > PULLBACK_EMA20_DIST:
            if effective_regime == "bull" and price > ema20_15m:
                log.debug("[%s] skip: sobreextendido sobre EMA20_15m dist=%.2f%%", symbol, dist_ema20 * 100)
                return None, 0, None
            if effective_regime == "bear" and price < ema20_15m:
                log.debug("[%s] skip: sobreextendido bajo EMA20_15m dist=%.2f%%", symbol, dist_ema20 * 100)
                return None, 0, None

    # ── Score mínimo dinámico por volatilidad (v4) ───────────────────────
    vol_bump   = _dynamic_min_score_bump(candles_1h, price)
    proto_bump = PROTO_MIN_SCORE_EXTRA if is_proto else 0  # = 0 en v5.2
    min_required_base = min_score + vol_bump + proto_bump

    # Contexto vela diaria
    open_daily, close_today = _daily_candle_context(candles_1h)
    score = 0

    if open_daily > 0:
        daily_move = (close_today - open_daily) / open_daily
        abs_move   = abs(daily_move)

        # Penalización por pullback contra el régimen
        if effective_regime == "bull" and daily_move < -DAILY_CANDLE_BLOCK:
            score -= DAILY_CANDLE_BLOCK_PEN
            log.debug(
                "[%s] daily_move=%.2f%% < -%.1f%% en bull → -%d (score=%d)",
                symbol, daily_move * 100, DAILY_CANDLE_BLOCK * 100, DAILY_CANDLE_BLOCK_PEN, score,
            )
        elif effective_regime == "bear" and daily_move > DAILY_CANDLE_BLOCK:
            score -= DAILY_CANDLE_BLOCK_PEN
            log.debug(
                "[%s] daily_move=%.2f%% > +%.1f%% en bear → -%d (score=%d)",
                symbol, daily_move * 100, DAILY_CANDLE_BLOCK * 100, DAILY_CANDLE_BLOCK_PEN, score,
            )

        # Penalización adicional por movimiento diario muy grande
        if abs_move > DAILY_CANDLE_PENALTY:
            score -= 10
            log.debug(
                "[%s] daily abs_move=%.2f%% > %.1f%% → penalización -10 (score=%d)",
                symbol, abs_move * 100, DAILY_CANDLE_PENALTY * 100, score,
            )

    # ── Scoring ───────────────────────────────────────────────────────────

    if is_proto:
        score -= PROTO_SCORE_PENALTY
        log.debug("[%s] proto-régimen → -%d (score=%d)", symbol, PROTO_SCORE_PENALTY, score)

    # W_VELA = 4: confirmación de alineación de la vela con el régimen
    if effective_regime == "bull" and bullish_candle:
        score += W_VELA
        log.debug("[%s] vela alcista en bull → +%d (score=%d)", symbol, W_VELA, score)
    elif effective_regime == "bear" and not bullish_candle:
        score += W_VELA
        log.debug("[%s] vela bajista en bear → +%d (score=%d)", symbol, W_VELA, score)
    else:
        log.debug("[%s] vela contraria al régimen → +0 (score=%d)", symbol, score)

    if effective_regime == "bull":
        if 40 <= rsi <= 68:   # era 45-65
            score += W_RSI_IDEAL
            log.debug("[%s] RSI=%.1f en zona ideal bull → +%d (score=%d)", symbol, rsi, W_RSI_IDEAL, score)
        elif rsi > 70:
            score += W_RSI_SOBRE
            log.debug("[%s] RSI=%.1f sobrecomprado → %d (score=%d)", symbol, rsi, W_RSI_SOBRE, score)
            if rsi > 80:
                log.debug("[%s] skip: RSI=%.1f > 80 (sobrecomprado extremo)", symbol, rsi)
                return None, score, None
        else:
            log.debug("[%s] RSI=%.1f fuera de zona ideal → +0 (score=%d)", symbol, rsi, score)
    else:
        if 32 <= rsi <= 58:   # era 35-55
            score += W_RSI_IDEAL
            log.debug("[%s] RSI=%.1f en zona ideal bear → +%d (score=%d)", symbol, rsi, W_RSI_IDEAL, score)
        elif rsi < 30:
            log.debug("[%s] skip: RSI=%.1f < 30 (sobrevendido en bear)", symbol, rsi)
            return None, score, None
        else:
            log.debug("[%s] RSI=%.1f fuera de zona ideal → +0 (score=%d)", symbol, rsi, score)

    if adx_1h >= 30:
        score += W_ADX_1H_30
        log.debug("[%s] ADX_1h=%.1f >= 30 → +%d (score=%d)", symbol, adx_1h, W_ADX_1H_30, score)
    elif adx_1h >= 25:
        score += W_ADX_1H_25
        log.debug("[%s] ADX_1h=%.1f >= 25 → +%d (score=%d)", symbol, adx_1h, W_ADX_1H_25, score)
    elif adx_1h >= 20:
        score += W_ADX_1H_20
        log.debug("[%s] ADX_1h=%.1f >= 20 → +%d (score=%d)", symbol, adx_1h, W_ADX_1H_20, score)
    else:
        log.debug("[%s] ADX_1h=%.1f < 20 → +0 (score=%d)", symbol, adx_1h, score)

    if effective_regime == "bear" and adx_1h < 22:
        log.debug("[%s] skip: bear + ADX_1h=%.1f < 22 (hard-guard short)", symbol, adx_1h)
        return None, score, None

    if effective_regime == "bull" and macd_hist > 0:
        score += W_MACD_15M
        log.debug("[%s] MACD_15m=%.5f positivo en bull → +%d (score=%d)", symbol, macd_hist, W_MACD_15M, score)
    elif effective_regime == "bear" and macd_hist < 0:
        score += W_MACD_15M
        log.debug("[%s] MACD_15m=%.5f negativo en bear → +%d (score=%d)", symbol, macd_hist, W_MACD_15M, score)
    else:
        log.debug("[%s] MACD_15m=%.5f contrario al régimen → +0 (score=%d)", symbol, macd_hist, score)

    if effective_regime == "bull" and macd_1h > 0:
        score += W_MACD_1H
        log.debug("[%s] MACD_1h=%.5f positivo en bull → +%d (score=%d)", symbol, macd_1h, W_MACD_1H, score)
    elif effective_regime == "bear" and macd_1h < 0:
        score += W_MACD_1H
        log.debug("[%s] MACD_1h=%.5f negativo en bear → +%d (score=%d)", symbol, macd_1h, W_MACD_1H, score)
    else:
        log.debug("[%s] MACD_1h=%.5f contrario al régimen → +0 (score=%d)", symbol, macd_1h, score)

    if avg_vol > 0:
        if last_vol >= avg_vol * VOLUME_MULT:
            score += W_VOLUME_HIGH
            log.debug("[%s] vol_ratio=%.2f >= %.1f → +%d (score=%d)", symbol, vol_ratio, VOLUME_MULT, W_VOLUME_HIGH, score)
        elif last_vol < avg_vol * VOLUME_WEAK:
            score += W_VOLUME_LOW
            log.debug("[%s] vol_ratio=%.2f < %.1f → %d (score=%d)", symbol, vol_ratio, VOLUME_WEAK, W_VOLUME_LOW, score)
        else:
            log.debug("[%s] vol_ratio=%.2f normal → +0 (score=%d)", symbol, vol_ratio, score)

    hour = datetime.datetime.now(timezone.utc).hour
    if hour in HIGH_BIAS_HOURS:
        score += W_HORA_HIGH
        log.debug("[%s] hora=%d en HIGH_BIAS_HOURS → +%d (score=%d)", symbol, hour, W_HORA_HIGH, score)
    elif hour in LOW_BIAS_HOURS:
        score += W_HORA_LOW
        log.debug("[%s] hora=%d en LOW_BIAS_HOURS → %d (score=%d)", symbol, hour, W_HORA_LOW, score)

    div = _rsi_divergence(closes_15m[:-1], candles_15m[:-1])
    if effective_regime == "bull" and div == "bullish":
        score += W_DIVERGENCIA
        log.debug("[%s] divergencia RSI bullish → +%d (score=%d)", symbol, W_DIVERGENCIA, score)
    elif effective_regime == "bear" and div == "bearish":
        score += W_DIVERGENCIA
        log.debug("[%s] divergencia RSI bearish → +%d (score=%d)", symbol, W_DIVERGENCIA, score)

    if structure == effective_regime:
        score += W_STRUCTURE
        log.debug("[%s] structure=%s == régimen → +%d (score=%d)", symbol, structure, W_STRUCTURE, score)
    elif structure != "range" and structure != effective_regime:
        score += W_STRUCTURE_CONTRA
        log.debug(
            "[%s] structure=%s contradice régimen=%s → %d (score=%d)",
            symbol, structure, regime, W_STRUCTURE_CONTRA, score,
        )

    # ── Score mínimo ──────────────────────────────────────────────────────
    min_required = min_required_base + (SHORT_MIN_SCORE_EXTRA if effective_regime == "bear" else 0)
    log.info(
        "[%s] SCORE=%d min=%d | régimen=%s adx1h=%.1f adx15m=%.1f rsi=%.1f vol=%.2f",
        symbol, score, min_required, regime, adx_1h, adx_15m, rsi, vol_ratio,
    )

    if score < min_required:
        return None, score, None

    side = "long" if effective_regime == "bull" else "short"
    log.info(
        "✅ SEÑAL %s | score=%d (min=%d) | regime=%s structure=%s "
        "adx1h=%.1f adx15m=%.1f rsi=%.1f macd=%.5f vol=%.2f%s",
        side.upper(), score, min_required, regime, structure,
        adx_1h, adx_15m, rsi, macd_hist, vol_ratio,
        " [PROTO]" if is_proto else "",
    )
    return side, score, regime
