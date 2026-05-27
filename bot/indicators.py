import math

def ema(closes, period):
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(closes[:period]) / period]
    for price in closes[period:]:
        result.append(price * k + result[-1] * (1 - k))
    return result

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal:
        return 0.0, 0.0, 0.0
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    min_len = min(len(ema_fast), len(ema_slow))
    macd_line = [ema_fast[-(min_len-i)] - ema_slow[-(min_len-i)] for i in range(min_len)]
    signal_line = ema(macd_line, signal)
    if not signal_line:
        return 0.0, 0.0, 0.0
    m = round(macd_line[-1], 6)
    s = round(signal_line[-1], 6)
    return m, s, round(m - s, 6)

def atr(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0
    atr_val = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return round(atr_val, 6)

def supertrend(highs, lows, closes, period=10, factor=3.0):
    atr_vals = []
    for i in range(1, len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        atr_vals.append(tr)
    if len(atr_vals) < period:
        return 1, closes[-1]
    smoothed = [sum(atr_vals[:period])/period]
    for v in atr_vals[period:]:
        smoothed.append((smoothed[-1]*(period-1)+v)/period)
    direction = 1
    st = closes[-1]
    for i in range(len(smoothed)):
        idx = i + period
        if idx >= len(closes):
            break
        mid = (highs[idx] + lows[idx]) / 2
        upper = mid + factor * smoothed[i]
        lower = mid - factor * smoothed[i]
        if closes[idx] > upper:
            direction = 1
            st = lower
        elif closes[idx] < lower:
            direction = -1
            st = upper
    return direction, round(st, 6)
