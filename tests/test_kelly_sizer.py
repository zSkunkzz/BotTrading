"""tests/test_kelly_sizer.py"""
import pytest
import sys
from unittest.mock import patch, MagicMock


def _reload_kelly(env_overrides=None):
    import os
    original = {}
    for k, v in (env_overrides or {}).items():
        original[k] = os.environ.get(k)
        os.environ[k] = v
    if "bot.kelly_sizer" in sys.modules:
        del sys.modules["bot.kelly_sizer"]
    import bot.kelly_sizer as ks
    for k, orig_v in original.items():
        if orig_v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = orig_v
    return ks


def _mock_shadow(win_rate, trades):
    mock = MagicMock()
    mock.win_rate_by_mode.return_value = {
        "NORMAL": {"win_rate": win_rate, "trades": trades},
        "FAST":   {"win_rate": win_rate, "trades": trades},
        "STRONG": {"win_rate": win_rate, "trades": trades},
        "EARLY":  {"win_rate": win_rate, "trades": trades},
    }
    return mock


class TestKellyMultiplier:
    def test_returns_1_when_disabled(self):
        ks = _reload_kelly({"KELLY_ENABLED": "false"})
        assert ks.kelly_multiplier("NORMAL", 2.0) == 1.0

    def test_returns_1_when_insufficient_trades(self):
        ks = _reload_kelly({"KELLY_ENABLED": "true", "KELLY_MIN_TRADES": "30"})
        shadow_mock = _mock_shadow(win_rate=0.6, trades=10)
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock(shadow_mode=shadow_mock)}):
            result = ks.kelly_multiplier("NORMAL", 2.0)
        assert result == 1.0

    def test_positive_edge_returns_above_1(self):
        ks = _reload_kelly({"KELLY_ENABLED": "true", "KELLY_MIN_TRADES": "5"})
        shadow_mock = _mock_shadow(win_rate=0.65, trades=50)
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock(shadow_mode=shadow_mock)}):
            result = ks.kelly_multiplier("NORMAL", 2.0)
        assert result > 1.0

    def test_negative_edge_returns_min_mult(self):
        ks = _reload_kelly({"KELLY_ENABLED": "true", "KELLY_MIN_TRADES": "5", "KELLY_MIN_MULT": "0.5"})
        shadow_mock = _mock_shadow(win_rate=0.30, trades=50)
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock(shadow_mode=shadow_mock)}):
            result = ks.kelly_multiplier("NORMAL", 1.0)
        assert result == pytest.approx(0.5, abs=0.01)

    def test_clamp_max(self):
        ks = _reload_kelly({"KELLY_ENABLED": "true", "KELLY_MIN_TRADES": "5", "KELLY_MAX_MULT": "2.0"})
        shadow_mock = _mock_shadow(win_rate=0.99, trades=50)
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock(shadow_mode=shadow_mock)}):
            result = ks.kelly_multiplier("NORMAL", 5.0)
        assert result <= 2.0

    def test_rr_zero_returns_min_mult(self):
        ks = _reload_kelly({"KELLY_ENABLED": "true"})
        result = ks.kelly_multiplier("NORMAL", 0.0)
        assert result == ks.KELLY_MIN_MULT

    def test_shadow_exception_returns_1(self):
        ks = _reload_kelly({"KELLY_ENABLED": "true", "KELLY_MIN_TRADES": "5"})
        broken = MagicMock()
        broken.shadow_mode.win_rate_by_mode.side_effect = RuntimeError("boom")
        with patch.dict("sys.modules", {"bot.shadow_mode": broken}):
            result = ks.kelly_multiplier("NORMAL", 2.0)
        assert result == 1.0

    def test_parse_int_env_invalid_uses_default(self):
        ks = _reload_kelly({"KELLY_MIN_TRADES": "false"})
        assert ks.KELLY_MIN_TRADES == 30
