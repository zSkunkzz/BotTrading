"""tests/test_signal_engine.py — suite para signal_engine v23.1 (momentum puro)."""
import pytest
from bot.signal_engine import (
    _clean_bars,
    _compute_indicators,
    _evaluate_signal,
    _rsi_last,
    _structure_sl,
    _verify_real_pullback,
    _adjust_tp_for_structure,
    MIN_SCORE,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_bars(n=50, close=100.0, high_mult=1.005, low_mult=0.995, vol=1000.0):
    """Genera n velas OHLCV sintéticas con close fijo."""
    return [
        [i, close * 0.999, close * high_mult, close * low_mult, close, vol]
        for i in range(n)
    ]


def _ind(ema21=105.0, ema50=100.0, close=105.0, atr=1.0, vwap=103.0,
         vol_ratio=1.5, ema_spread=0.05):
    """Indicadores sintéticos mínimos para los tests de _evaluate_signal."""
    return {
        "ema21":      ema21,
        "ema50":      ema50,
        "ema_spread": ema_spread,
        "ema_bull":   ema21 > ema50,
        "ema_bear":   ema21 < ema50,
        "atr":        atr,
        "vol_ratio":  vol_ratio,
        "vwap":       vwap,
        "close":      close,
        "high":       close * 1.005,
        "low":        close * 0.995,
    }


def _ind_short(ema21=95.0, ema50=100.0, close=95.0, atr=1.0, vwap=97.0,
               vol_ratio=1.5, ema_spread=0.05):
    return _ind(ema21=ema21, ema50=ema50, close=close, atr=atr, vwap=vwap,
                vol_ratio=vol_ratio, ema_spread=ema_spread)


# ─── TestCleanBars ────────────────────────────────────────────────────────────

class TestCleanBars:
    def test_removes_none_bars(self):
        bars = [[1, 1.0, 1.1, 0.9, 1.0, 100.0], None, [2, 1.0, 1.1, 0.9, 1.0, 100.0]]
        assert len(_clean_bars(bars)) == 2

    def test_removes_bars_with_none_fields(self):
        bars = [
            [1, 1.0, None, 0.9, 1.0, 100.0],
            [2, 1.0, 1.1,  0.9, 1.0, 100.0],
        ]
        result = _clean_bars(bars)
        assert len(result) == 1
        assert result[0][0] == 2

    def test_empty_input_returns_empty(self):
        assert _clean_bars([]) == []

    def test_none_input_returns_empty(self):
        assert _clean_bars(None) == []

    def test_clean_bars_unchanged(self):
        bars = [[i, 1.0, 1.1, 0.9, 1.0, 100.0] for i in range(5)]
        assert _clean_bars(bars) == bars


# ─── TestComputeIndicators ────────────────────────────────────────────────────

class TestComputeIndicators:
    def test_returns_empty_for_insufficient_bars(self):
        assert _compute_indicators(_make_bars(5)) == {}

    def test_returns_dict_with_expected_keys(self):
        ind = _compute_indicators(_make_bars(60))
        for key in ("ema21", "ema50", "ema_bull", "ema_bear", "atr", "vol_ratio", "vwap", "close"):
            assert key in ind, f"Clave faltante: {key}"

    def test_bull_market(self):
        # close creciente → EMA21 > EMA50 al final
        bars = [[i, 90+i*0.5, 91+i*0.5, 89+i*0.5, 90+i*0.5, 1000.0] for i in range(60)]
        ind = _compute_indicators(bars)
        assert ind.get("ema_bull") is True
        assert ind.get("ema_bear") is False

    def test_bear_market(self):
        bars = [[i, 120-i*0.5, 121-i*0.5, 119-i*0.5, 120-i*0.5, 1000.0] for i in range(60)]
        ind = _compute_indicators(bars)
        assert ind.get("ema_bear") is True
        assert ind.get("ema_bull") is False


# ─── TestRsiLast ─────────────────────────────────────────────────────────────

class TestRsiLast:
    def test_insufficient_data_returns_none(self):
        assert _rsi_last([100.0] * 5) is None

    def test_flat_prices_returns_value(self):
        closes = [100.0] * 30
        result = _rsi_last(closes)
        assert result is not None

    def test_strong_uptrend_high_rsi(self):
        closes = [100.0 + i for i in range(30)]
        result = _rsi_last(closes)
        assert result is not None
        assert result > 70

    def test_strong_downtrend_low_rsi(self):
        closes = [130.0 - i for i in range(30)]
        result = _rsi_last(closes)
        assert result is not None
        assert result < 30


# ─── TestVerifyRealPullback ────────────────────────────────────────────────────

class TestVerifyRealPullback:
    def test_insufficient_bars_returns_true(self):
        """Sin datos suficientes no debe bloquear."""
        assert _verify_real_pullback([], "LONG", 100.0) is True

    def test_long_price_was_above_level(self):
        # Las velas anteriores tienen close > level → pullback real
        bars = _make_bars(10, close=105.0)
        assert _verify_real_pullback(bars, "LONG", 100.0) is True

    def test_long_price_never_above_level_returns_false(self):
        # Precio siempre debajo del nivel → no es pullback real
        bars = _make_bars(10, close=95.0)
        assert _verify_real_pullback(bars, "LONG", 100.0) is False

    def test_short_price_was_below_level(self):
        bars = _make_bars(10, close=95.0)
        assert _verify_real_pullback(bars, "SHORT", 100.0) is True

    def test_short_price_never_below_level_returns_false(self):
        bars = _make_bars(10, close=105.0)
        assert _verify_real_pullback(bars, "SHORT", 100.0) is False


# ─── TestStructureSl ──────────────────────────────────────────────────────────

class TestStructureSl:
    def test_fallback_when_insufficient_bars(self):
        result = _structure_sl(_make_bars(2), "LONG", 100.0, 98.0)
        assert result == 98.0

    def test_long_sl_below_entry(self):
        bars = _make_bars(10, close=100.0, low_mult=0.99)
        sl = _structure_sl(bars, "LONG", 100.0, 97.0)
        assert sl < 100.0

    def test_short_sl_above_entry(self):
        bars = _make_bars(10, close=100.0, high_mult=1.01)
        sl = _structure_sl(bars, "SHORT", 100.0, 103.0)
        assert sl > 100.0

    def test_caps_sl_when_too_far(self):
        """Si la estructura está muy lejos del entry, usa fallback."""
        # lows muy bajos → SL quedaría > SL_STRUCTURE_MAX_DIST_PCT → fallback
        bars = _make_bars(10, close=100.0, low_mult=0.90)  # -10% → supera el cap
        sl = _structure_sl(bars, "LONG", 100.0, 99.0)
        assert sl == 99.0  # fallback


# ─── TestAdjustTpForStructure ─────────────────────────────────────────────────

class TestAdjustTpForStructure:
    def test_no_bars_returns_raw(self):
        tp, rr = _adjust_tp_for_structure([], "LONG", 100.0, 110.0, 97.0)
        assert tp == 110.0

    def test_long_no_obstacle_returns_raw(self):
        # No hay highs entre entry (100) y tp (110)
        bars = _make_bars(25, close=115.0, high_mult=1.16)  # highs > tp
        tp, rr = _adjust_tp_for_structure(bars, "LONG", 100.0, 110.0, 97.0)
        assert tp == 110.0

    def test_long_obstacle_clips_tp(self):
        # Ponemos un high exactamente en 106 (entre entry=100 y tp=110)
        bars = _make_bars(25, close=106.0, high_mult=1.0)  # highs ≈ 106
        bars[-1][2] = 106.0  # asegurar que el high esté ahí
        tp, rr = _adjust_tp_for_structure(bars, "LONG", 100.0, 110.0, 97.0)
        # TP debe recortarse por debajo del obstacle
        assert tp < 110.0

    def test_rr_calculation(self):
        tp, rr = _adjust_tp_for_structure([], "LONG", 100.0, 110.0, 95.0)
        assert abs(rr - 2.0) < 0.01  # riesgo=5, ganancia=10 → RR=2


# ─── TestEvaluateSignal ────────────────────────────────────────────────────────

class TestEvaluateSignal:
    """Tests de las 3 reglas binarias de la estrategia v23.1."""

    def _bars_1h_with_pullback(self, close=100.0, n=30, level=100.0):
        """Barras 1h donde el precio viene de ENCIMA de level (para LONG)."""
        bars = []
        for i in range(n):
            c = level * 1.02 if i < n - 3 else close  # precio estuvo arriba
            bars.append([i, c * 0.999, c * 1.001, c * 0.999, c, 1000.0])
        return bars

    def _bars_15m_bullish(self, close=100.0):
        """Barras 15m con última vela alcista y volumen alto."""
        bars = _make_bars(30, close=close)
        # Última vela: open < close (alcista)
        bars[-1] = [29, close * 0.99, close * 1.01, close * 0.99, close, 2000.0]
        return bars

    def _bars_15m_bearish(self, close=100.0):
        bars = _make_bars(30, close=close)
        bars[-1] = [29, close * 1.01, close * 1.01, close * 0.99, close * 0.995, 2000.0]
        return bars

    def test_sin_datos_4h_neutral(self):
        direction, reasons = _evaluate_signal({}, _ind(), _ind(), _make_bars(), _make_bars())
        assert direction == "NEUTRAL"

    def test_sin_datos_1h_neutral(self):
        ind_4h = _ind(ema21=105.0, ema50=100.0, ema_spread=0.05)
        direction, _ = _evaluate_signal(ind_4h, {}, _ind(), _make_bars(), _make_bars())
        assert direction == "NEUTRAL"

    def test_4h_en_rango_neutral(self):
        """spread < EMA_SPREAD_MIN → NEUTRAL"""
        ind_4h = _ind(ema21=100.01, ema50=100.00, ema_spread=0.0001)
        direction, _ = _evaluate_signal(ind_4h, _ind(), _ind(), _make_bars(), _make_bars())
        assert direction == "NEUTRAL"

    def test_sin_pullback_neutral(self):
        """Precio lejos de EMA/VWAP → R2 falla → NEUTRAL"""
        ind_4h = _ind(ema21=105.0, ema50=100.0, ema_spread=0.05)
        # close_1h muy lejos de ema21 y vwap
        ind_1h = _ind(ema21=100.0, close=85.0, vwap=100.0, atr=0.5)
        ind_15m = _ind()
        direction, _ = _evaluate_signal(ind_4h, ind_1h, ind_15m, _make_bars(), _make_bars())
        assert direction == "NEUTRAL"

    def test_sin_confirmacion_15m_neutral(self):
        """R1+R2 ok pero vela 15m doji → NEUTRAL"""
        ind_4h  = _ind(ema21=105.0, ema50=100.0, ema_spread=0.05)
        # Pullback justo al EMA21_1h
        ind_1h  = _ind(ema21=100.0, close=100.0, atr=1.0, vwap=99.0)
        ind_15m = _ind(close=100.0, vol_ratio=1.5)
        bars_1h = self._bars_1h_with_pullback(close=100.0, level=100.0)
        # Última vela 15m: doji (open == close)
        bars_15m = _make_bars(30, close=100.0)
        bars_15m[-1][1] = 100.0  # open = close → doji
        direction, _ = _evaluate_signal(ind_4h, ind_1h, ind_15m, bars_1h, bars_15m)
        assert direction == "NEUTRAL"
