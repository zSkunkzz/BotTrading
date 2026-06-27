"""signals.py — Sistema de señales con estructura de precio real.

Mejoras estructurales v2:
  A. Estructura de precio 1h (HH/HL vs LH/LL)
     El régimen ya no se basa solo en EMAs. Se valida que el precio esté
     construyendo máximos/mínimos crecientes (bull) o decrecientes (bear)
     en las últimas 6 velas de 1h. Sin estructura confirmada = range.

  B. Contexto de vela diaria
     Se calcula la vela diaria sintética (desde medianoche UTC) con las
     velas de 1h disponibles. LONGs bloqueados si la vela diaria baja >1.5%.
     SHORTs bloqueados si la vela diaria sube >1.5%. Evita entrar contra
     el momentum del día.

  C. Filtro de liquidez del par
     Volumen medio de las últimas 24 velas de 1h < umbral mínimo = par
     demasiado ilíquido para trading sistemático. Elimina pares tipo PENGU
     con spreads y spikes aleatorios.

  D. Penalización por alejamiento del open diario
     Si el precio ya se ha movido >2.5% desde el open diario en la dirección
     del trade, el movimiento es sobreextensión y el score se penaliza -10.
     Si se ha movido >4% = hard-guard (no entrar, el move ya ocurrió).

Filtros heredados:
  1. Vela cerrada       : penúltima vela (ya cerrada)
  2. Régimen de mercado : EMAs 1h + estructura HH/HL
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
  16. Score mínimo      : LONGs ≥ MIN_SCORE, SHORTs ≥ MIN_SCORE+8

REGLA FUNDAMENTAL:
  Bear → SOLO SHORT. Bull → SOLO LONG. Sin contra-tendencia.
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

# ── Nuevos umbrales v2 ────────────────────────────────────────────────
# Estructura de precio: ventana de velas 1h para detectar HH/HL o LH/LL
STRUCTURE_LOOKBACK    = 6    # velas 1h
# Contexto diario: bloquear si vela diaria va >X% contra el trade
DAILY_CANDLE_BLOCK    = 0.015  # 1.5%
DAILY_CANDLE_PENALTY  = 0.025  # 2.5% → penalización -10
DAILY_CANDLE_GUARD    = 0.040  # 4.0% → hard-guard
# Liquidez mínima del par: volumen medio por vela 1h (en USDT notional aprox)
# Pares con volumen < este umbral tienen spreads y spikes impredecibles
MIN_HOURLY_VOLUME     = 500_000   # 500k USDT/hora → ~12M/día


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
    return "range", adx


def _price_structure(candles_1h: list[dict], lookback: int = STRUCTURE_LOOKBACK) -> str:
    """Detecta estructura de precio en las últimas N velas 1h cerradas.

    Devuelve 'bull' si hay HH+HL, 'bear' si hay LH+LL, 'range' si no hay estructura clara.
    Usa los highs y lows de las velas, no solo los closes.
    """
    closed = candles_1h[:-1]  # excluir vela en curso
    if len(closed) < lookback + 1:
        return "range"

    recent = closed[-(lookback + 1):]
    highs  = [c["high"]  for c in recent]
    lows   = [c["low"]   for c in recent]

    # Comparar primera mitad vs segunda mitad de la ventana
    mid = len(recent) // 2
    avg_high_prev = sum(highs[:mid]) / mid
    avg_high_curr = sum(highs[mid:]) / (len(highs) - mid)
    avg_low_prev  = sum(lows[:mid])  / mid
    avg_low_curr  = sum(lows[mid:])  / (len(lows) - mid)

    hh = avg_high_curr > avg_high_prev * 1.001   # máximos creciendo
    hl = avg_low_curr  > avg_low_prev  * 1.001   # mínimos creciendo
    lh = avg_high_curr < avg_high_prev * 0.999   # máximos cayendo
    ll = avg_low_curr  < avg_low_prev  * 0.999   # mínimos cayendo

    if hh and hl:
        return "bull"
    if lh and ll:
        return "bear"
    if hh and ll:
        return "range"   # expansión: volatilidad sin dirección
    if lh and hl:
        return "range"   # contracción: rango apretándose
    return "range"


def _daily_candle_context(candles_1h: list[dict]) -> tuple[float, float]:
    """Calcula la vela diaria sintética desde medianoche UTC con las velas 1h.

    Devuelve (open_price, move_pct) donde move_pct es positivo si sube, negativo si baja.
    Si no hay suficientes datos devuelve (0, 0).
    """
    now_utc    = datetime.datetime.now(timezone.utc)
    midnight   = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_ts = midnight.timestamp() * 1000  # ms

    # Filtrar velas 1h cerradas del día actual
    today_candles = [
        c for c in candles_1h[:-1]
        if c.get("ts", 0) >= midnight_ts
    ]

    if len(today_candles) < 2:
        return 0.0, 0.0

    open_price  = today_candles[0]["open"]
    close_price = today_candles[-1]["close"]

    if open_price <= 0:
        return 0.0, 0.0

    move_pct = (close_price - open_price) / open_price
    return open_price, move_pct


def _hourly_liquidity(candles_1h: list[dict], window: int = 24) -> float:
    """Calcula el volumen medio por vela 1h en las últimas N velas cerradas.

    Usa quote_volume si está disponible (USDT), si no usa volume * close como aproximación.
    """
    closed = candles_1h[:-1]
    recent = closed[-window:] if len(closed) >= window else closed
    if not recent:
        return 0.0

    vols = []
    for c in recent:
        qv = c.get("quote_volume") or c.get("quoteVolume") or 0.0
        if qv > 0:
            vols.append(float(qv))
        else:
            # fallback: volume * close
            vols.append(float(c.get("volume", 0)) * float(c.get("close", 0)))

    return sum(vols) / len(vols) if vols else 0.0


def _regime_confirmed(candles_1h: list[dict], n: int = REGIME_CONFIRM_BARS) -> tuple[str, float]:
    if len(candles_1h) < 200 + n + 1:
        return "range", 0.0

    closed_1h = candles_1h[:-1]
    regime_now, adx_now = _market_regime(closed_1h)

    regimes_prev = []
    for k in range(1, n + 1):
        window = closed_1h[:-k]
        if len(window) < 200:
            return "range", adx_now
        regime_hist, _ = _market_regime(window)
        regimes_prev.append(regime_hist)

    all_regimes = regimes_prev + [regime_now]

    if len(set(all_regimes)) == 1:
        log.debug("régimen confirmado ×%d: %s", n + 1, regime_now)
        return regime_now, adx_now

    log.info("⚠️  Régimen inestable %s — esperando confirmación", all_regimes)
    return "range", adx_now


def _volume_ok(candles: list[dict], window: int = 20) -> bool:
    if len(candles) < window + 2:
        return False
    recent   = candles[-(window + 2):-2]
    if not recent:
        return False
    avg_vol  = sum(c["volume"] for c in recent) / len(recent)
    last_vol = candles[-2]["volume"]
    return last_vol > avg_vol * VOLUME_MULT


def _volume_weak(candles: list[dict], window: int = 20) -> bool:
    if len(candles) < window + 2:
        return False
    recent   = candles[-(window + 2):-2]
    if not recent:
        return False
    avg_vol  = sum(c["volume"] for c in recent) / len(recent)
    last_vol = candles[-2]["volume"]
    return avg_vol > 0 and last_vol < avg_vol * VOLUME_WEAK


def _macro_pts(candles_4h: list[dict] | None, direction: str) -> int:
    if not candles_4h or len(candles_4h) < 52:
        return 0
    closes = [c["close"] for c in candles_4h]
    ema50  = _ema(closes[:-1], 50)[-1]
    price  = closes[-2] if len(closes) >= 2 else closes[-1]
    if direction == "long":
        return 15 if price > ema50 else -10
    return 15 if price < ema50 else -10


def evaluate(
    candles_15m: list[dict],
    candles_1h:  list[dict],
    candles_4h:  list[dict] | None = None,
    min_score:   int = MIN_SCORE,
) -> tuple[str | None, int, str | None]:
    """Evalúa señal con scoring estructural.

    Devuelve (signal, score, regime) o (None, score, None).
    """

    # ── 0. Datos mínimos ──────────────────────────────────────────────────
    if len(candles_15m) < 50 or len(candles_1h) < 210:
        return None, 0, None

    # ── 1. Vela cerrada ───────────────────────────────────────────────────
    candle     = candles_15m[-2]
    closes_15m = [c["close"] for c in candles_15m]

    # ── 2. Régimen de mercado (EMAs) ──────────────────────────────────────
    regime, adx_1h = _regime_confirmed(candles_1h)
    if regime == "range":
        return None, 0, regime

    # REGLA FUNDAMENTAL: régimen dicta dirección.
    direction = "short" if regime == "bear" else "long"

    # ── 2b. NUEVO: Estructura de precio 1h (HH/HL vs LH/LL) ───────────────
    structure = _price_structure(candles_1h)
    if structure != "range" and structure != regime:
        # El régimen dice bull pero la estructura dice bear (o viceversa)
        # → contradicción, el mercado está girando. No entrar.
        log.info(
            "[structure] Régimen %s contradice estructura de precio %s — señal descartada",
            regime, structure,
        )
        return None, 0, regime

    # ── 2c. NUEVO: Filtro de liquidez del par ──────────────────────────────
    hourly_vol = _hourly_liquidity(candles_1h)
    if hourly_vol > 0 and hourly_vol < MIN_HOURLY_VOLUME:
        log.info(
            "[liquidity] Par bloqueado: volumen medio 1h = %.0f USDT < mínimo %.0f — par ilíquido",
            hourly_vol, MIN_HOURLY_VOLUME,
        )
        return None, 0, regime

    # ── 3. Score base ─────────────────────────────────────────────────────
    score = 20

    # Bonus si estructura confirma régimen (+8)
    if structure == regime:
        score += 8
        log.debug("[structure] Confirmada: %s +8pts", structure)

    # ── 4. Macro 4h ───────────────────────────────────────────────────────
    score += _macro_pts(candles_4h, direction)

    # ── 4b. NUEVO: Contexto de vela diaria ────────────────────────────────
    daily_open, daily_move = _daily_candle_context(candles_1h)
    if daily_open > 0:
        # move positivo = precio subió hoy, negativo = bajó
        # Para LONG: malo si el día ya baja mucho (daily_move muy negativo)
        # Para SHORT: malo si el día ya sube mucho (daily_move muy positivo)
        move_against = -daily_move if direction == "long" else daily_move

        if move_against > DAILY_CANDLE_GUARD:
            log.info(
                "[daily] Hard-guard: vela diaria %+.2f%% contra el trade (%s) — el move ya ocurrió",
                move_against * 100, direction,
            )
            return None, score, regime

        if move_against > DAILY_CANDLE_PENALTY:
            score -= 10
            log.debug("[daily] Penalización -10: vela diaria %+.2f%% contra el trade", move_against * 100)
        elif move_against > DAILY_CANDLE_BLOCK:
            score -= 5
            log.debug("[daily] Penalización -5: vela diaria %+.2f%% contra el trade", move_against * 100)
        elif move_against < -DAILY_CANDLE_BLOCK:
            # El día ya va a favor del trade → momentum adicional
            score += 5
            log.debug("[daily] Bonus +5: vela diaria %+.2f%% a favor del trade", abs(move_against) * 100)

    # ── 5. EMA200 1h hard-guard ───────────────────────────────────────────
    closes_1h_closed = [c["close"] for c in candles_1h[:-1]]
    closes_1h_full   = [c["close"] for c in candles_1h]
    ema200_1h = _ema(closes_1h_closed, 200)[-1]
    price     = closes_15m[-2]
    if direction == "long"  and price < ema200_1h * (1 - EMA200_MIN_DIST):
        return None, score, regime
    if direction == "short" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        return None, score, regime

    # ── 5b. EMA200 15m hard-guard (solo SHORTs) ───────────────────────────
    if len(closes_15m) >= 202:
        ema200_15m = _ema(closes_15m[:-1], 200)[-1]
        if direction == "short" and price > ema200_15m * 1.002:
            return None, score, regime

    # ── 5c. ATR volátil ───────────────────────────────────────────────────
    atr_15m_raw = _atr(candles_15m[:-1])
    if price > 0 and atr_15m_raw / price > ATR_VOLATILE_PCT:
        log.info(
            "Hard-guard ATR volátil: atr_pct=%.2f%% — mercado en evento",
            atr_15m_raw / price * 100,
        )
        return None, score, regime

    # ── 6. ADX 15m ────────────────────────────────────────────────────────
    adx_15m = _adx(candles_15m[:-1], 14)
    if adx_15m < ADX_15M_MIN:
        return None, score, regime

    if   adx_15m > 35: score += 12
    elif adx_15m > 25: score += 6
    else:              score -= 6

    # ── 7. ADX 1h ─────────────────────────────────────────────────────────
    if   adx_1h > 25: score += 5
    elif adx_1h < 18: score -= 5

    if direction == "short" and adx_1h < 18:
        return None, score, regime

    # ── 8. Volumen 15m ─────────────────────────────────────────────────────
    vol_strong = _volume_ok(candles_15m)
    vol_weak   = _volume_weak(candles_15m)
    if   vol_strong: score += 10
    elif vol_weak:   score -= 5

    # ── 9. RSI 15m ────────────────────────────────────────────────────────
    rsi_vals = _rsi(closes_15m[:-1], 14)
    rsi_now  = rsi_vals[-1]
    rsi_prev = rsi_vals[-2] if len(rsi_vals) > 1 else rsi_now

    if direction == "short" and rsi_now < 38:
        return None, score, regime

    if direction == "long":
        rsi_cross = rsi_prev < 50 <= rsi_now
        rsi_ext   = rsi_now > 58
        rsi_dir   = rsi_now > rsi_prev and rsi_now > 45
        rsi_bad   = rsi_now < 45
    else:
        rsi_cross = rsi_prev > 50 >= rsi_now
        rsi_ext   = rsi_now < 42
        rsi_dir   = rsi_now < rsi_prev and rsi_now < 55
        rsi_bad   = rsi_now > 60

    if   rsi_cross: score += 15
    elif rsi_ext:   score += 8
    elif rsi_dir:   score += 4
    elif rsi_bad:   score -= 5

    # ── 10. MACD 15m + 1h ─────────────────────────────────────────────────
    hist_15m = _macd_histogram(closes_15m[:-1])
    h_now    = hist_15m[-1]
    if direction == "long":
        if   h_now > 0: score += 10
        elif h_now < 0: score -= 5
    else:
        if   h_now < 0: score += 10
        elif h_now > 0: score -= 5

    hist_1h = _macd_histogram(closes_1h_full[:-1])
    h1_now  = hist_1h[-1]
    if direction == "long":
        if   h1_now > 0: score += 10
        elif h1_now < 0: score -= 5
    else:
        if   h1_now < 0: score += 10
        elif h1_now > 0: score -= 5

    # ── 11. Divergencia RSI ───────────────────────────────────────────────
    div = _rsi_divergence(closes_15m[:-1], candles_15m[:-1])
    if (direction == "long"  and div == "bullish") or \
       (direction == "short" and div == "bearish"):
        score += 8

    # ── 12. Sesgo horario ─────────────────────────────────────────────────
    candle_ts = candle.get("ts")
    if candle_ts:
        hour = datetime.datetime.fromtimestamp(candle_ts / 1000, tz=timezone.utc).hour
    else:
        hour = datetime.datetime.now(timezone.utc).hour

    if   hour in HIGH_BIAS_HOURS: score += 8
    elif hour in LOW_BIAS_HOURS:  score -= 10

    # ── 13. No-chase ────────────────────────────────────────────────────────
    candle_rng = candle["high"] - candle["low"]
    if atr_15m_raw > 0 and candle_rng > NO_CHASE_MULT * atr_15m_raw:
        return None, score, regime

    # ── 14. Pullback EMA20_15m ──────────────────────────────────────────────
    pullback_dist = None
    if len(closes_15m) >= 22:
        ema20_15m     = _ema(closes_15m[:-1], 20)[-1]
        pullback_dist = (price - ema20_15m) / ema20_15m
        if direction == "long" and price > ema20_15m * (1 + PULLBACK_EMA20_DIST):
            return None, score, regime
        if direction == "short" and price < ema20_15m * (1 - PULLBACK_EMA20_DIST):
            return None, score, regime

    # ── 15. Score mínimo ───────────────────────────────────────────────────
    min_score_directional = min_score + SHORT_MIN_SCORE_EXTRA if direction == "short" else min_score
    if score < min_score_directional:
        log.debug(
            "Score insuficiente: %d < %d (%s)", score, min_score_directional, direction,
        )
        return None, score, regime

    log.info(
        "✅ SEÑAL %s | score=%d (min=%d, margin=%+d) | regime=%s structure=%s "
        "adx1h=%.1f adx15m=%.1f rsi=%.1f macd15=%+.4f macd1h=%+.4f "
        "daily_move=%+.2f%% vol_1h=%.0fk vol=%s div=%s pb=%s",
        direction.upper(), score, min_score_directional, score - min_score_directional,
        regime, structure,
        adx_1h, adx_15m, rsi_now, h_now, h1_now,
        daily_move * 100 if daily_open > 0 else 0.0,
        hourly_vol / 1000,
        "strong" if vol_strong else ("weak" if vol_weak else "normal"),
        div,
        f"{pullback_dist:+.3%}" if pullback_dist is not None else "n/a",
    )
    return direction, score, regime
