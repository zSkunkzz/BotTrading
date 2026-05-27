"""
Indicadores tecnicos — puro Python, sin pandas ni ta-lib
"""


def ema(prices: list, period: int) -> list:
    if len(prices) < period:
        return [prices[-1]]
    k = 2 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result


def rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas[-period:]]
    losses = [-d if d < 0 else 0 for d in deltas[-period:]]
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    if avg_l == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_g / avg_l)), 2)


def macd(closes: list, fast=12, slow=26, signal_p=9):
    if len(closes) < slow + signal_p:
        return 0, 0, 0
    ema_f = ema(closes, fast)
    ema_s = ema(closes, slow)
    diff = len(ema_f) - len(ema_s)
    macd_line = [f - s for f, s in zip(ema_f[diff:], ema_s)]
    if len(macd_line) < signal_p:
        return macd_line[-1], macd_line[-1], 0
    signal = ema(macd_line, signal_p)
    hist = macd_line[-1] - signal[-1]
    return round(macd_line[-1], 6), round(signal[-1], 6), round(hist, 6)


def atr(highs: list, lows: list, closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 0
    tr_list = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        )
        tr_list.append(tr)
    atr_val = sum(tr_list[:period]) / period
    for tr in tr_list[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return round(atr_val, 6)


def supertrend(highs: list, lows: list, closes: list, period: int = 10, factor: float = 3.0):
    """Retorna (direction, value): direction 1=bullish, -1=bearish"""
    if len(closes) < period + 2:
        return 1, closes[-1]

    atr_vals = []
    for i in range(period, len(closes)):
        atr_vals.append(atr(highs[i-period:i+1], lows[i-period:i+1], closes[i-period:i+1], period))

    upper_basic, lower_basic = [], []
    for i, a in enumerate(atr_vals):
        idx = i + period
        hl2 = (highs[idx] + lows[idx]) / 2
        upper_basic.append(hl2 + factor * a)
        lower_basic.append(hl2 - factor * a)

    upper_band = list(upper_basic)
    lower_band = list(lower_basic)

    for i in range(1, len(atr_vals)):
        upper_band[i] = upper_basic[i] if upper_basic[i] < upper_band[i-1] or closes[period+i-1] > upper_band[i-1] else upper_band[i-1]
        lower_band[i] = lower_basic[i] if lower_basic[i] > lower_band[i-1] or closes[period+i-1] < lower_band[i-1] else lower_band[i-1]

    direction = 1
    st_direction, st_value = [], []
    for i in range(len(atr_vals)):
        if i == 0:
            st_direction.append(1)
            st_value.append(lower_band[i])
            continue
        prev_dir = st_direction[-1]
        if prev_dir == 1:
            direction = -1 if closes[period+i] < lower_band[i] else 1
        else:
            direction = 1 if closes[period+i] > upper_band[i] else -1
        st_direction.append(direction)
        st_value.append(lower_band[i] if direction == 1 else upper_band[i])

    return st_direction[-1], round(st_value[-1], 4)
