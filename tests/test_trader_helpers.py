"""tests/test_trader_helpers.py"""
import pytest
from bot.trader_helpers import _check_price_staleness, _adjust_levels_to_fill


class TestCheckPriceStaleness:
    def _signal(self, entry=100.0):
        return {"entry": entry, "sl": 98.0, "tp1": 103.0, "tp2": 106.0}

    def test_no_entry_returns_none(self):
        assert _check_price_staleness({"entry": 0}, 100.0, True) is None
        assert _check_price_staleness({}, 100.0, True) is None

    def test_within_threshold_long_returns_none(self):
        assert _check_price_staleness(self._signal(100.0), 101.0, True) is None

    def test_within_threshold_short_returns_none(self):
        assert _check_price_staleness(self._signal(100.0), 99.0, False) is None

    def test_long_price_too_high_returns_reason(self):
        result = _check_price_staleness(self._signal(100.0), 104.0, True)
        assert result is not None and isinstance(result, str)

    def test_short_price_too_low_returns_reason(self):
        result = _check_price_staleness(self._signal(100.0), 96.0, False)
        assert result is not None and isinstance(result, str)

    def test_absolute_limit_blocks_long(self):
        result = _check_price_staleness(self._signal(100.0), 110.0, True)
        assert result is not None

    def test_absolute_limit_blocks_short(self):
        result = _check_price_staleness(self._signal(100.0), 88.0, False)
        assert result is not None

    def test_long_price_dropped_returns_reason(self):
        result = _check_price_staleness(self._signal(100.0), 96.0, True)
        assert result is not None

    def test_short_price_rose_returns_reason(self):
        result = _check_price_staleness(self._signal(100.0), 104.0, False)
        assert result is not None


class TestAdjustLevelsToFill:
    def _signal(self, entry=100.0, sl=97.0, tp1=103.0, tp2=106.0):
        return {"entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2}

    def test_no_drift_returns_original_levels(self):
        sl, tp1, tp2 = _adjust_levels_to_fill(self._signal(), 100.0, 100.0)
        assert abs(sl - 97.0) < 0.001
        assert abs(tp1 - 103.0) < 0.001
        assert abs(tp2 - 106.0) < 0.001

    def test_fill_higher_rescales_up(self):
        sl, tp1, tp2 = _adjust_levels_to_fill(self._signal(), 102.0, 100.0)
        assert sl > 97.0 and tp1 > 103.0 and tp2 > 106.0

    def test_fill_lower_rescales_down(self):
        sl, tp1, tp2 = _adjust_levels_to_fill(self._signal(), 98.0, 100.0)
        assert sl < 97.0 and tp1 < 103.0 and tp2 < 106.0

    def test_proportions_maintained(self):
        sl, tp1, tp2 = _adjust_levels_to_fill(self._signal(), 102.0, 100.0)
        assert abs((sl  - 102.0) / 102.0 - (-0.03)) < 0.001
        assert abs((tp1 - 102.0) / 102.0 - ( 0.03)) < 0.001

    def test_zero_entry_doesnt_crash(self):
        sl, tp1, tp2 = _adjust_levels_to_fill({"entry": 0, "sl": 97.0, "tp1": 103.0, "tp2": 106.0}, 100.0, 100.0)
        assert sl > 0 or tp1 > 0

    def test_tiny_drift_returns_original(self):
        sl, tp1, tp2 = _adjust_levels_to_fill(self._signal(), 100.03, 100.0)
        assert abs(sl - 97.0) < 0.001
        assert abs(tp1 - 103.0) < 0.001
