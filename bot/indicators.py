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
    # Alinear por el final: ema_slow es siempre la más corta
    diff = len(ema_fast) - len(ema_slow)
    macd_line = [ema_fast[diff + i] - ema_slow[i] for i in range(len(ema_slow))]
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
    smoothed = [sum(atr_vals[:period]) / period]
    for v in atr_vals[period:]:
        smoothed.append((smoothed[-1] * (period - 1) + v) / period)

    direction = 1
    prev_upper = None
    prev_lower = None
    st = closes[-1]

    for i in range(len(smoothed)):
        idx = i + period
        if idx >= len(closes):
            break
        mid = (highs[idx] + lows[idx]) / 2
        basic_upper = mid + factor * smoothed[i]
        basic_lower = mid - factor * smoothed[i]

        # Sticky bands: la banda solo se ajusta si es más favorable que la anterior
        if prev_upper is None or basic_upper < prev_upper or closes[idx - 1] > prev_upper:
            upper = basic_upper
        else:
            upper = prev_upper

        if prev_lower is None or basic_lower > prev_lower or closes[idx - 1] < prev_lower:
            lower = basic_lower
        else:
            lower = prev_lower

        if direction == 1:
            if closes[idx] < lower:
                direction = -1
                st = upper
            else:
                st = lower
        else:
            if closes[idx] > upper:
                direction = 1
                st = lower
            else:
                st = upper

        prev_upper = upper
        prev_lower = lower

    return direction, round(st, 6)


def vwap(bars: list) -> float:
    """VWAP acumulado de las barras provistas.

    Usa el Typical Price (H+L+C)/3 ponderado por volumen.
    Seguro ante barras con v=0 o v=None (las omite sin lanzar excepción).
    Retorna 0.0 si no hay volumen acumulado válido.

    Uso habitual: pasar bars_15m completas para obtener VWAP intradía
    referenciado al inicio de la sesión de datos disponibles.
    """
    cum_vol = 0.0
    cum_tpv = 0.0
    for b in bars:
        try:
            h = float(b[2])
            l = float(b[3])
            c = float(b[4])
            v = float(b[5]) if b[5] is not None else 0.0
        except (TypeError, IndexError, ValueError):
            continue
        if v <= 0:
            continue
        tp = (h + l + c) / 3
        cum_tpv += tp * v
        cum_vol  += v
    return round(cum_tpv / cum_vol, 6) if cum_vol > 0 else 0.0
