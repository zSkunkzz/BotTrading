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


def vwap(bars: list, reset_daily: bool = True) -> float:
    """VWAP con reset diario a las 00:00 UTC (por defecto).

    Cuando reset_daily=True (default), solo acumula las barras del
    dia UTC actual (timestamp >= inicio_dia_utc). Esto produce un
    VWAP diario real, mucho mas significativo como nivel de precio
    que el VWAP acumulado de todas las barras del array.

    Cuando reset_daily=False, acumula todas las barras provistas
    (comportamiento anterior, util para tests o timeframes > 1h).

    Usa el Typical Price (H+L+C)/3 ponderado por volumen.
    Seguro ante barras con v=0 o v=None (las omite sin lanzar excepcion).
    Retorna 0.0 si no hay volumen acumulado valido.
    """
    import time as _time
    if reset_daily:
        # Inicio del dia UTC en milisegundos
        now_s = _time.time()
        day_start_ms = int((now_s - (now_s % 86400)) * 1000)
        bars_to_use = [b for b in bars if b is not None and len(b) > 0 and b[0] is not None and int(b[0]) >= day_start_ms]
        # Si el dia acaba de empezar y hay menos de 4 barras, fallback a ultimas 24h
        if len(bars_to_use) < 4:
            day_start_ms -= 86400 * 1000
            bars_to_use = [b for b in bars if b is not None and len(b) > 0 and b[0] is not None and int(b[0]) >= day_start_ms]
    else:
        bars_to_use = bars

    cum_vol = 0.0
    cum_tpv = 0.0
    for b in bars_to_use:
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


def rsi_divergence(bars: list, rsi_period: int = 14, lookback: int = 30) -> str:
    """Detecta divergencias RSI alcistas y bajistas.

    Compara los dos ultimos minimos de precio (divergencia alcista) o
    los dos ultimos maximos de precio (divergencia bajista) con los
    valores de RSI correspondientes.

    Retorna:
      'BULLISH'  — precio hace minimo mas bajo pero RSI hace minimo mas alto
      'BEARISH'  — precio hace maximo mas alto pero RSI hace maximo mas bajo
      'NONE'     — sin divergencia detectada

    Requiere al menos lookback+rsi_period+2 barras para ser fiable.
    """
    if len(bars) < lookback + rsi_period + 2:
        return "NONE"

    closes = [float(b[4]) for b in bars]
    lows   = [float(b[3]) for b in bars]
    highs  = [float(b[2]) for b in bars]

    # Calcula RSI para cada barra del lookback usando ventana deslizante
    rsi_vals = []
    for i in range(len(closes) - lookback, len(closes)):
        window = closes[max(0, i - rsi_period * 2): i + 1]
        rsi_vals.append(rsi(window, rsi_period))

    price_lows  = lows[-lookback:]
    price_highs = highs[-lookback:]

    # Busca los dos ultimos minimos locales de precio
    def find_local_mins(series, window=3):
        mins = []
        for i in range(window, len(series) - window):
            if series[i] == min(series[i - window: i + window + 1]):
                mins.append(i)
        return mins[-2:] if len(mins) >= 2 else []

    def find_local_maxs(series, window=3):
        maxs = []
        for i in range(window, len(series) - window):
            if series[i] == max(series[i - window: i + window + 1]):
                maxs.append(i)
        return maxs[-2:] if len(maxs) >= 2 else []

    # Divergencia alcista: precio min mas bajo, RSI min mas alto
    lmin_idx = find_local_mins(price_lows)
    if len(lmin_idx) == 2:
        i1, i2 = lmin_idx
        if price_lows[i2] < price_lows[i1] and rsi_vals[i2] > rsi_vals[i1]:
            return "BULLISH"

    # Divergencia bajista: precio max mas alto, RSI max mas bajo
    hmax_idx = find_local_maxs(price_highs)
    if len(hmax_idx) == 2:
        i1, i2 = hmax_idx
        if price_highs[i2] > price_highs[i1] and rsi_vals[i2] < rsi_vals[i1]:
            return "BEARISH"

    return "NONE"
