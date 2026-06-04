"""tests/test_indicators.py"""
import pytest
from bot.indicators import ema, rsi, macd, atr, supertrend, vwap


def _make_bars(closes, highs=None, lows=None, vols=None):
    n = len(closes)
    highs = highs or [c * 1.01 for c in closes]
    lows  = lows  or [c * 0.99 for c in closes]
    vols  = vols  or [1000.0] * n
    return [[i, closes[i], highs[i], lows[i], closes[i], vols[i]] for i in range(n)]


def _trending_up(n=100, start=100.0, step=1.0):
    return [start + i * step for i in range(n)]


def _trending_down(n=100, start=200.0, step=1.0):
    return [start - i * step for i in range(n)]


class TestEma:
    def test_empty_when_insufficient_data(self):
        assert ema([1, 2, 3], 10) == []

    def test_length_correct(self):
        result = ema(list(range(1, 31)), 10)
        assert len(result) == 21

    def test_trending_up_ema21_gt_ema50(self):
        closes = _trending_up(100)
        e21 = ema(closes, 21)
        e50 = ema(closes, 50)
        assert e21[-1] > e50[-1]

    def test_trending_down_ema21_lt_ema50(self):
        closes = _trending_down(100)
        e21 = ema(closes, 21)
        e50 = ema(closes, 50)
        assert e21[-1] < e50[-1]

    def test_constant_series_ema_equals_value(self):
        closes = [50.0] * 30
        result = ema(closes, 10)
        assert abs(result[-1] - 50.0) < 0.0001


class TestRsi:
    def test_returns_50_when_insufficient(self):
        assert rsi([1, 2, 3], 14) == 50.0

    def test_overbought_strong_uptrend(self):
        """Serie fuertemente alcista → RSI > 70."""
        closes = _trending_up(80, step=1.5)
        assert rsi(closes, 14) > 70

    def test_oversold_strong_downtrend(self):
        """Serie fuertemente bajista → RSI < 30."""
        closes = _trending_down(80, step=1.5)
        assert rsi(closes, 14) < 30

    def test_neutral_oscillating(self):
        """Serie sin tendencia clara con alternancia +1/-1 → RSI cerca de 50."""
        closes = [100.0]
        for i in range(79):
            closes.append(closes[-1] + (1.0 if i % 2 == 0 else -1.0))
        val = rsi(closes, 14)
        assert 30 < val < 70

    def test_all_gains_returns_100(self):
        closes = list(range(1, 20))
        assert rsi(closes, 14) == 100.0


class TestMacd:
    def test_returns_zeros_when_insufficient(self):
        assert macd([1, 2, 3], 12, 26, 9) == (0.0, 0.0, 0.0)

    def test_bullish_hist_in_uptrend(self):
        """Tendencia alcista sostenida → histograma positivo.

        Necesitamos suficientes velas para que la línea de señal (9 periodos
        del MACD line) esté por debajo del MACD line en una tendencia alcista.
        Con 120 velas y step=2.0 la diferencia ema_fast-ema_slow es claramente
        positiva y la señal sigue por debajo → hist > 0.
        """
        closes = _trending_up(120, step=2.0)
        _, _, hist = macd(closes, 12, 26, 9)
        assert hist > 0

    def test_bearish_hist_in_downtrend(self):
        """Tendencia bajista sostenida → histograma negativo."""
        closes = _trending_down(120, step=2.0)
        _, _, hist = macd(closes, 12, 26, 9)
        assert hist < 0


class TestAtr:
    def test_positive_value(self):
        closes = _trending_up(30)
        highs  = [c * 1.01 for c in closes]
        lows   = [c * 0.99 for c in closes]
        val = atr(highs, lows, closes, 14)
        assert val > 0

    def test_zero_range_returns_zero(self):
        closes = [50.0] * 20
        val = atr(closes, closes, closes, 14)
        assert val == 0.0

    def test_insufficient_data(self):
        closes = [1.0, 2.0, 3.0]
        val = atr(closes, closes, closes, 14)
        assert val >= 0


class TestSupertrend:
    def test_returns_tuple(self):
        closes = _trending_up(30)
        highs  = [c * 1.01 for c in closes]
        lows   = [c * 0.99 for c in closes]
        direction, val = supertrend(highs, lows, closes)
        assert direction in (1, -1)
        assert val > 0

    def test_bullish_in_strong_uptrend(self):
        """Tendencia alcista clara con ATR pequeño relativo al avance → Supertrend 1."""
        closes = _trending_up(60, step=2.0)
        highs  = [c + 0.5 for c in closes]
        lows   = [c - 0.5 for c in closes]
        direction, _ = supertrend(highs, lows, closes, 10, 1.5)
        assert direction == 1

    def test_bearish_in_strong_downtrend(self):
        """Tendencia bajista clara con ATR pequeño relativo al descenso → Supertrend -1."""
        closes = _trending_down(60, step=2.0)
        highs  = [c + 0.5 for c in closes]
        lows   = [c - 0.5 for c in closes]
        direction, _ = supertrend(highs, lows, closes, 10, 1.5)
        assert direction == -1


class TestVwap:
    def test_returns_float(self):
        bars = _make_bars(_trending_up(30))
        result = vwap(bars)
        assert isinstance(result, float)
        assert result > 0

    def test_constant_price_equals_price(self):
        bars = _make_bars([100.0] * 20)
        result = vwap(bars)
        assert abs(result - 100.0) < 0.01

    def test_zero_volume_returns_zero(self):
        bars = _make_bars([100.0] * 10, vols=[0.0] * 10)
        assert vwap(bars) == 0.0

    def test_none_volume_doesnt_crash(self):
        bars = [[i, 100.0, 101.0, 99.0, 100.0, None] for i in range(10)]
        assert vwap(bars) == 0.0

    def test_empty_bars_returns_zero(self):
        assert vwap([]) == 0.0
