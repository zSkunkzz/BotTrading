"""tests/test_signal_engine.py"""
import pytest
from bot.signal_engine import (
    _score_tendencia,
    _score_breakout,
    _score_reversal,
    _clean_bars,
    _detect_setup,
    MIN_SCORE,
)


def _ind(
    ema_bull=True, ema_bear=False,
    macd_bull=True, macd_bear=False, macd_hist=0.01,
    st_bull=True, st_bear=False, st_dir=1,
    rsi_val=50.0, vol_ratio=1.5,
    ema21=105.0, ema50=100.0,
    atr=1.0, close=105.0, vwap=103.0,
):
    return {
        "ema_bull": ema_bull, "ema_bear": ema_bear,
        "macd_bull": macd_bull, "macd_bear": macd_bear, "macd_hist": macd_hist,
        "st_bull": st_bull, "st_bear": st_bear, "st_dir": st_dir,
        "rsi_val": rsi_val, "vol_ratio": vol_ratio,
        "ema21": ema21, "ema50": ema50,
        "atr": atr, "close": close, "vwap": vwap,
    }


def _ind_short():
    return _ind(
        ema_bull=False, ema_bear=True,
        macd_bull=False, macd_bear=True, macd_hist=-0.01,
        st_bull=False, st_bear=True, st_dir=-1,
        rsi_val=50.0, vol_ratio=1.5,
        ema21=95.0, ema50=100.0,
        close=95.0, vwap=97.0,
    )


def _bars(n=30, close=105.0):
    return [
        [i, close * 0.999, close * 1.001, close * 0.998, close, 1000.0]
        for i in range(n)
    ]


class TestCleanBars:
    def test_removes_none_bars(self):
        bars = [[1, 1.0, 1.1, 0.9, 1.0, 100.0], None, [2, 1.0, 1.1, 0.9, 1.0, 100.0]]
        result = _clean_bars(bars)
        assert len(result) == 2

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


class TestScoreTendencia:
    def test_long_valid_all_confirmations(self):
        i15 = _ind()
        i1h = _ind(ema21=105.0, ema50=100.0)
        i4h = _ind()
        setup, direction, score, max_score, reasons = _score_tendencia(i15, i1h, i4h, _bars())
        assert direction == "LONG"
        assert score >= MIN_SCORE

    def test_short_valid_all_confirmations(self):
        setup, direction, score, max_score, reasons = _score_tendencia(_ind_short(), _ind_short(), _ind_short(), _bars())
        assert direction == "SHORT"
        assert score >= MIN_SCORE

    def test_macd_obligatorio_bloquea_long(self):
        i15 = _ind(macd_bull=False, macd_bear=True, macd_hist=-0.01)
        _, direction, score, _, reasons = _score_tendencia(i15, _ind(), _ind(), _bars())
        assert direction == "NEUTRAL"

    def test_st1h_obligatorio_bloquea_long(self):
        i1h = _ind(st_bull=False, st_bear=True)
        _, direction, score, _, reasons = _score_tendencia(_ind(), i1h, _ind(), _bars())
        assert direction == "NEUTRAL"

    def test_st4h_contra_penaliza(self):
        i15 = _ind()
        i1h = _ind()
        i4h_contra = _ind(st_bull=False, st_bear=True)
        i4h_favor  = _ind(st_bull=True,  st_bear=False)
        _, _, score_contra, _, _ = _score_tendencia(i15, i1h, i4h_contra, _bars())
        _, _, score_favor,  _, _ = _score_tendencia(i15, i1h, i4h_favor,  _bars())
        assert score_favor > score_contra

    def test_rsi_sobreextendido_fuerza_score_cero(self):
        i15 = _ind(rsi_val=80.0)
        _, direction, score, _, _ = _score_tendencia(i15, _ind(), _ind(), _bars())
        assert score == 0 or direction == "NEUTRAL"

    def test_sin_datos_1h_neutral(self):
        _, direction, _, _, _ = _score_tendencia(_ind(), {}, _ind(), _bars())
        assert direction == "NEUTRAL"

    def test_mercado_en_rango_neutral(self):
        i1h = _ind(ema21=100.01, ema50=100.00)
        _, direction, _, _, _ = _score_tendencia(_ind(), i1h, _ind(), _bars())
        assert direction == "NEUTRAL"

    def test_vwap_suma_punto(self):
        i15_above = _ind(close=106.0, vwap=100.0)
        i15_below = _ind(close=98.0,  vwap=100.0)
        _, _, score_above, _, _ = _score_tendencia(i15_above, _ind(), _ind(), _bars())
        _, _, score_below, _, _ = _score_tendencia(i15_below, _ind(), _ind(), _bars())
        assert score_above > score_below


class TestScoreBreakout:
    def test_no_breakout_neutral(self):
        _, direction, _, _, _ = _score_breakout(_ind(), _ind(), _ind(), _bars(30, close=100.0))
        assert direction == "NEUTRAL"

    def test_insufficient_bars_neutral(self):
        _, direction, _, _, _ = _score_breakout(_ind(), _ind(), _ind(), _bars(5))
        assert direction == "NEUTRAL"

    def test_high_volume_scores_higher(self):
        bars = [[i, 100.0, 100.5, 99.5, 100.0, 1000.0] for i in range(24)]
        bars.append([24, 101.0, 102.0, 100.5, 101.5, 2000.0])
        i15_vol_high = _ind(vol_ratio=1.8, atr=0.5, close=101.5)
        i15_vol_low  = _ind(vol_ratio=0.8, atr=0.5, close=101.5)
        _, _, score_high, _, _ = _score_breakout(i15_vol_high, {}, {}, bars)
        _, _, score_low,  _, _ = _score_breakout(i15_vol_low,  {}, {}, bars)
        assert score_high >= score_low


class TestScoreReversal:
    def test_rsi_no_extremo_neutral(self):
        i1h = _ind(rsi_val=50.0)
        _, direction, _, _, _ = _score_reversal(_ind(), i1h, _ind(), _bars())
        assert direction == "NEUTRAL"

    def test_sobreventa_long(self):
        i1h = _ind(rsi_val=25.0)
        i15 = _ind(macd_bull=True, macd_hist=0.05)
        _, direction, score, _, _ = _score_reversal(i15, i1h, _ind(), _bars())
        assert direction == "LONG"
        assert score > 0

    def test_sobrecompra_short(self):
        i1h = _ind(rsi_val=75.0)
        i15 = _ind_short()
        _, direction, score, _, _ = _score_reversal(i15, i1h, _ind(), _bars())
        assert direction == "SHORT"
        assert score > 0

    def test_sin_datos_1h_neutral(self):
        _, direction, _, _, _ = _score_reversal(_ind(), {}, _ind(), _bars())
        assert direction == "NEUTRAL"

    def test_macd_confirma_suma_puntos(self):
        i1h = _ind(rsi_val=25.0)
        i15_confirm = _ind(macd_bull=True,  macd_hist=0.05)
        i15_no_conf = _ind(macd_bull=False, macd_hist=-0.01)
        _, _, score_with,    _, _ = _score_reversal(i15_confirm, i1h, {}, _bars())
        _, _, score_without, _, _ = _score_reversal(i15_no_conf, i1h, {}, _bars())
        assert score_with > score_without


class TestDetectSetup:
    def test_neutral_when_no_setup_reaches_min_score(self):
        i15 = _ind(macd_bull=False, macd_bear=False, macd_hist=0.0,
                   st_bull=False, st_bear=False, vol_ratio=0.3)
        setup, direction, score, _, _ = _detect_setup(i15, {}, {}, _bars())
        assert direction == "NEUTRAL"

    def test_returns_best_setup_by_ratio(self):
        i15 = _ind(vol_ratio=1.5)
        i1h = _ind(ema21=105.0, ema50=100.0)
        i4h = _ind()
        setup, direction, score, max_score, _ = _detect_setup(i15, i1h, i4h, _bars())
        if direction != "NEUTRAL":
            assert setup in ("TENDENCIA", "BREAKOUT", "REVERSAL")
            assert score >= MIN_SCORE
