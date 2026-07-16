from __future__ import annotations

import logging
from datetime import timezone
import datetime

import config
import market_context

log = logging.getLogger("signals")

EMA200_MIN_DIST = 0.001
NO_CHASE_MULT = 2.0
VOLUME_MULT = 1.2
VOLUME_WEAK = 0.8
MIN_SCORE = config.MIN_SCORE

PULLBACK_EMA20_DIST_BASE = 0.015
PULLBACK_ATR_MULT = 1.5

SHORT_MIN_SCORE_EXTRA = 4
ATR_VOLATILE_PCT = 0.035
ADX_15M_MIN = 18

STRUCTURE_LOOKBACK = 12
MIN_HOURLY_VOLUME = 50_000

PROTO_ADX_MIN = 22

ATR_HIGH_VOL_PCT = 0.020
ATR_LOW_VOL_PCT = 0.005
ATR_HIGH_VOL_BUMP = 3
ATR_LOW_VOL_BUMP = 4

W_ADX_1H_30 = 15
W_ADX_1H_25 = 12
W_ADX_1H_20 = 11
W_ADX_1H_15 = 8
W_MACD_1H = 13
W_RSI_IDEAL = 16
W_MACD_15M = 10
W_VOLUME_HIGH = 14
W_STRUCTURE = 13
W_DIVERGENCIA = 10
W_VELA = 8

W_VOLUME_LOW = -5
W_RSI_SOBRE = -9
W_STRUCTURE_CONTRA = -7
W_MACRO_CONTRA = -8
W_BEAR_EMA200_15M = -5
W_BEAR_LOW_ADX = -4
W_MACD_15M_CONTRA = -2


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


def _macd_histogram(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> list[float]:
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line = _ema(macd_line, signal)
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
        h, l = candles[i]["high"], candles[i]["low"]
        ph, pl = candles[i - 1]["high"], candles[i - 1]["low"]
        pc = candles[i - 1]["close"]
        up, down = h - ph, pl - l
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))

    def _smooth(lst: list[float]) -> list[float]:
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

    rsi_series = _rsi(closes, 14)
    recent_closes = closes[-lookback:]
    recent_rsi = rsi_series[-lookback:]
    recent_candles = candles[-lookback:]

    avg_vol = sum(c["volume"] for c in recent_candles[:-1]) / max(1, len(recent_candles) - 1)
    last_vol = recent_candles[-1].get("volume", 0.0)
    if avg_vol > 0 and last_vol < avg_vol * 0.6:
        return None

    last_close = recent_closes[-1]

    try:
        lo_val = min(recent_closes[:-1])
        lo_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == lo_val)
        if (
            last_close < lo_val
            and lo_val > 0
            and abs(last_close - lo_val) / lo_val >= 0.005
            and recent_rsi[-1] > recent_rsi[lo_idx] + 2
        ):
            return "bullish"
    except (ValueError, IndexError):
        pass

    try:
        hi_val = max(recent_closes[:-1])
        hi_idx = max(i for i, v in enumerate(recent_closes[:-1]) if v == hi_val)
        if (
            last_close > hi_val
            and hi_val > 0
            and abs(last_close - hi_val) / hi_val >= 0.005
            and recent_rsi[-1] < recent_rsi[hi_idx] - 2
        ):
            return "bearish"
    except (ValueError, IndexError):
        pass

    return None


def _market_regime(candles_1h: list[dict]) -> tuple[str, float]:
    closes = [c["close"] for c in candles_1h]
    closes_closed = closes[:-1]
    ema50 = _ema(closes_closed, 50)[-1]
    ema200 = _ema(closes_closed, 200)[-1]
    adx = _adx(candles_1h[:-1], 14)
    price = closes[-2] if len(closes) >= 2 else closes[-1]

    if price > ema200 and ema50 > ema200:
        return "bull", adx
    if price < ema200 and ema50 < ema200:
        return "bear", adx
    if price > ema200 and adx >= PROTO_ADX_MIN:
        return "proto_bull", adx
    if price < ema200 and adx >= PROTO_ADX_MIN:
        return "proto_bear", adx
    return "range", adx


def _find_swing_highs(highs: list[float], wing: int = 2) -> list[int]:
    pivots = []
    for i in range(wing, len(highs) - wing):
        if all(highs[i] > highs[i - j] for j in range(1, wing + 1)) and all(
            highs[i] > highs[i + j] for j in range(1, wing + 1)
        ):
            pivots.append(i)
    return pivots


def _find_swing_lows(lows: list[float], wing: int = 2) -> list[int]:
    pivots = []
    for i in range(wing, len(lows) - wing):
        if all(lows[i] < lows[i - j] for j in range(1, wing + 1)) and all(
            lows[i] < lows[i + j] for j in range(1, wing + 1)
        ):
            pivots.append(i)
    return pivots


def _price_structure(candles_1h: list[dict], lookback: int = STRUCTURE_LOOKBACK) -> str:
    closed = candles_1h[:-1]
    if len(closed) < lookback + 4:
        return "range"

    recent = closed[-(lookback + 4):]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]

    swing_high_idxs = _find_swing_highs(highs, wing=2)
    swing_low_idxs = _find_swing_lows(lows, wing=2)

    if len(swing_high_idxs) < 2 or len(swing_low_idxs) < 2:
        return "range"

    swing_high_vals = [highs[i] for i in swing_high_idxs]
    swing_low_vals = [lows[i] for i in swing_low_idxs]

    hh_count = sum(1 for i in range(1, len(swing_high_vals)) if swing_high_vals[i] > swing_high_vals[i - 1] * 1.001)
    hl_count = sum(1 for i in range(1, len(swing_low_vals)) if swing_low_vals[i] > swing_low_vals[i - 1] * 1.001)
    lh_count = sum(1 for i in range(1, len(swing_high_vals)) if swing_high_vals[i] < swing_high_vals[i - 1] * 0.999)
    ll_count = sum(1 for i in range(1, len(swing_low_vals)) if swing_low_vals[i] < swing_low_vals[i - 1] * 0.999)

    n = 2
    if hh_count >= n and hl_count >= n:
        return "bull"
    if lh_count >= n and ll_count >= n:
        return "bear"
    return "range"


def _liquidity_ok(candles_1h: list[dict]) -> bool:
    recent = candles_1h[-25:-1]
    if not recent:
        return False

    avg_vol = sum(
        c.get("quote_volume", c.get("volume", 0.0) * c.get("close", 1.0))
        for c in recent
    ) / len(recent)

    ok = avg_vol >= MIN_HOURLY_VOLUME
    if not ok:
        log.debug("[liquidity] skip — quote_volume_avg_1h=%.0f < umbral %d USDT", avg_vol, MIN_HOURLY_VOLUME)
    return ok


def _dynamic_min_score_bump(candles_1h: list[dict], price: float) -> int:
    if price <= 0 or len(candles_1h) < 16:
        return 0
    atr_1h = _atr(candles_1h[:-1], period=14)
    if atr_1h <= 0:
        return 0
    atr_pct = atr_1h / price
    if atr_pct > ATR_HIGH_VOL_PCT:
        return ATR_HIGH_VOL_BUMP
    if atr_pct < ATR_LOW_VOL_PCT:
        return ATR_LOW_VOL_BUMP
    return 0


def _price_change_1h(candles_1h: list[dict]) -> float:
    closes = [c["close"] for c in candles_1h]
    if len(closes) < 3:
        return 0.0
    prev = closes[-3]
    curr = closes[-2]
    if prev <= 0:
        return 0.0
    return (curr - prev) / prev


def evaluate(
    candles_15m: list[dict],
    candles_1h: list[dict],
    candles_4h: list[dict] | None = None,
    min_score: int = MIN_SCORE,
    symbol: str = "???",
    coin: str | None = None,
) -> tuple[str | None, int, str | None]:
    if len(candles_15m) < 50 or len(candles_1h) < 220:
        log.debug("[%s] skip: candles insuficientes (15m=%d 1h=%d)", symbol, len(candles_15m), len(candles_1h))
        return None, 0, None

    if not _liquidity_ok(candles_1h):
        log.debug("[%s] skip: liquidez insuficiente", symbol)
        return None, 0, None

    closed = candles_15m[-2]
    c_open = closed["open"]
    c_close = closed["close"]
    c_high = closed["high"]
    c_low = closed["low"]
    bullish_candle = c_close > c_open

    regime, adx_1h = _market_regime(candles_1h)
    is_proto = regime in ("proto_bull", "proto_bear")
    effective_regime = "bull" if regime in ("bull", "proto_bull") else ("bear" if regime in ("bear", "proto_bear") else "range")

    if effective_regime == "range":
        return None, 0, None

    structure = _price_structure(candles_1h)

    closes_15m = [c["close"] for c in candles_15m]
    price = closes_15m[-2]

    ema20_15m = _ema(closes_15m[:-1], 20)[-1]
    ema200_15m = _ema(closes_15m[:-1], 200)[-1]
    rsi_series = _rsi(closes_15m[:-1], 14)
    rsi = rsi_series[-1]
    macd_hist = _macd_histogram(closes_15m[:-1])[-1]
    atr_15m = _atr(candles_15m[:-1], 14)
    adx_15m = _adx(candles_15m[:-1], 14)

    volumes = [c["volume"] for c in candles_15m]
    avg_vol = sum(volumes[-21:-1]) / 20
    last_vol = volumes[-2]
    vol_ratio = last_vol / avg_vol if avg_vol else 0.0

    closes_1h = [c["close"] for c in candles_1h]
    ema200_1h = _ema(closes_1h[:-1], 200)[-1]
    macd_1h = _macd_histogram(closes_1h[:-1])[-1]

    atr_1h_val = _atr(candles_1h[:-1], 14)
    atr_1h_pct = atr_1h_val / price if price > 0 else 0.0

    atr_15m_pct = atr_15m / price if price > 0 else PULLBACK_EMA20_DIST_BASE
    pullback_dist = max(PULLBACK_EMA20_DIST_BASE, atr_15m_pct * PULLBACK_ATR_MULT)

    if price > 0 and atr_15m / price > ATR_VOLATILE_PCT:
        return None, 0, None

    if adx_15m < ADX_15M_MIN:
        return None, 0, None

    if effective_regime == "bull" and price < ema200_1h * (1 - EMA200_MIN_DIST):
        return None, 0, None
    if effective_regime == "bear" and price > ema200_1h * (1 + EMA200_MIN_DIST):
        return None, 0, None

    candle_range = c_high - c_low
    if candle_range > 0 and atr_15m > 0 and candle_range > NO_CHASE_MULT * atr_15m:
        return None, 0, None

    if price > 0 and ema20_15m > 0:
        dist_ema20 = abs(price - ema20_15m) / price
        if dist_ema20 > pullback_dist:
            if effective_regime == "bull" and price > ema20_15m:
                return None, 0, None
            if effective_regime == "bear" and price < ema20_15m:
                return None, 0, None

    vol_bump = _dynamic_min_score_bump(candles_1h, price)
    min_required_base = min_score + vol_bump
    score = 0

    if candles_4h and len(candles_4h) >= 55:
        closes_4h = [c["close"] for c in candles_4h[:-1]]
        ema50_4h = _ema(closes_4h, 50)[-1]
        price_4h = closes_4h[-1]
        if effective_regime == "bull" and price_4h < ema50_4h:
            score += W_MACRO_CONTRA
        elif effective_regime == "bear" and price_4h > ema50_4h:
            score += W_MACRO_CONTRA

    if effective_regime == "bear" and price < ema200_15m * (1 - EMA200_MIN_DIST):
        score += W_BEAR_EMA200_15M

    if effective_regime == "bear" and adx_1h < 22:
        score += W_BEAR_LOW_ADX

    if effective_regime == "bull" and bullish_candle:
        score += W_VELA
    elif effective_regime == "bear" and not bullish_candle:
        score += W_VELA

    if effective_regime == "bull":
        if 40 <= rsi <= 68:
            score += W_RSI_IDEAL
        elif rsi > 70:
            score += W_RSI_SOBRE
            if rsi > 80:
                return None, score, None
    else:
        if 32 <= rsi <= 58:
            score += W_RSI_IDEAL
        elif rsi < 30:
            score += W_RSI_SOBRE

    if adx_1h >= 30:
        score += W_ADX_1H_30
    elif adx_1h >= 25:
        score += W_ADX_1H_25
    elif adx_1h >= 20:
        score += W_ADX_1H_20
    elif adx_1h >= 15:
        score += W_ADX_1H_15

    if effective_regime == "bull" and macd_hist > 0:
        score += W_MACD_15M
    elif effective_regime == "bear" and macd_hist < 0:
        score += W_MACD_15M
    else:
        score += W_MACD_15M_CONTRA

    if effective_regime == "bull" and macd_1h > 0:
        score += W_MACD_1H
    elif effective_regime == "bear" and macd_1h < 0:
        score += W_MACD_1H

    if avg_vol > 0:
        if last_vol >= avg_vol * VOLUME_MULT:
            score += W_VOLUME_HIGH
        elif last_vol < avg_vol * VOLUME_WEAK:
            score += W_VOLUME_LOW

    div = _rsi_divergence(closes_15m[:-1], candles_15m[:-1])
    if effective_regime == "bull" and div == "bullish":
        score += W_DIVERGENCIA
    elif effective_regime == "bear" and div == "bearish":
        score += W_DIVERGENCIA

    if structure == effective_regime:
        score += W_STRUCTURE
    elif structure != "range" and structure != effective_regime:
        score += W_STRUCTURE_CONTRA

    context_modifier = 0
    side_candidate = "long" if effective_regime == "bull" else "short"
    if coin is not None:
        price_chg_1h = _price_change_1h(candles_1h)
        context_modifier = market_context.score_context(coin, side_candidate, price_chg_1h)
        score += context_modifier

    min_required = min_required_base + (SHORT_MIN_SCORE_EXTRA if effective_regime == "bear" else 0)

    log.info(
        "[%s] SCORE=%d min=%d | régimen=%s adx1h=%.1f adx15m=%.1f rsi=%.1f vol=%.2f ctx=%+d",
        symbol,
        score,
        min_required,
        regime,
        adx_1h,
        adx_15m,
        rsi,
        vol_ratio,
        context_modifier,
    )

    if score < min_required:
        return None, score, None

    side = "long" if effective_regime == "bull" else "short"
    return side, score, regime
