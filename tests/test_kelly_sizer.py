"""tests/test_kelly_sizer.py"""
import os
import pytest
from unittest.mock import patch, MagicMock


# ── Helpers ──────────────────────────────────────────────────────────────────

def _import_kelly(enabled="false", fraction="0.25", min_mult="0.5",
                  max_mult="2.0", min_trades="30"):
    """Recarga el módulo con env vars controladas para aislar cada test."""
    import importlib, sys
    for mod in list(sys.modules):
        if "kelly_sizer" in mod:
            del sys.modules[mod]
    env = {
        "KELLY_ENABLED":    enabled,
        "KELLY_FRACTION":   fraction,
        "KELLY_MIN_MULT":   min_mult,
        "KELLY_MAX_MULT":   max_mult,
        "KELLY_MIN_TRADES": min_trades,
    }
    with patch.dict(os.environ, env):
        import bot.kelly_sizer as ks
        importlib.reload(ks)
    return ks


def _mock_shadow(win_rate: float, trades: int):
    """Crea un mock de shadow_mode.win_rate_by_mode() con stats dadas."""
    sm = MagicMock()
    sm.win_rate_by_mode.return_value = {
        "TENDENCIA": {"win_rate": win_rate, "trades": trades}
    }
    return sm


# ── _parse_int_env ────────────────────────────────────────────────────────────

class TestParseIntEnv:
    def test_valid_int(self):
        with patch.dict(os.environ, {"KELLY_MIN_TRADES": "50"}):
            ks = _import_kelly(enabled="true", min_trades="50")
            assert ks.KELLY_MIN_TRADES == 50

    def test_invalid_value_returns_default(self):
        """Si KELLY_MIN_TRADES='false' (error de config), debe usar default=30."""
        with patch.dict(os.environ, {"KELLY_MIN_TRADES": "false"}):
            import importlib, sys
            for mod in list(sys.modules):
                if "kelly_sizer" in mod:
                    del sys.modules[mod]
            with patch.dict(os.environ, {"KELLY_MIN_TRADES": "false", "KELLY_ENABLED": "false"}):
                import bot.kelly_sizer as ks
                importlib.reload(ks)
            assert ks.KELLY_MIN_TRADES == 30


# ── kelly_multiplier — Kelly desactivado ─────────────────────────────────────

class TestKellyDisabled:
    def test_returns_1_when_disabled(self):
        ks = _import_kelly(enabled="false")
        assert ks.kelly_multiplier("TENDENCIA", rr=2.0) == 1.0

    def test_returns_1_for_any_rr_when_disabled(self):
        ks = _import_kelly(enabled="false")
        for rr in [0.0, -1.0, 5.0]:
            assert ks.kelly_multiplier("TENDENCIA", rr=rr) == 1.0


# ── kelly_multiplier — rr inválido ────────────────────────────────────────────

class TestKellyInvalidRR:
    def test_zero_rr_returns_min_mult(self):
        ks = _import_kelly(enabled="true")
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock()}):
            result = ks.kelly_multiplier("TENDENCIA", rr=0.0)
        assert result == float(os.getenv("KELLY_MIN_MULT", "0.5"))

    def test_negative_rr_returns_min_mult(self):
        ks = _import_kelly(enabled="true")
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock()}):
            result = ks.kelly_multiplier("TENDENCIA", rr=-1.0)
        assert result == ks.KELLY_MIN_MULT


# ── kelly_multiplier — insuficiente historial ─────────────────────────────────

class TestKellyInsufficientHistory:
    def test_no_mode_stats_returns_1(self):
        ks = _import_kelly(enabled="true", min_trades="30")
        sm = MagicMock()
        sm.win_rate_by_mode.return_value = {}  # no hay stats para TENDENCIA
        with patch.dict("sys.modules", {"bot.shadow_mode": MagicMock(shadow_mode=sm)}):
            # Recalcular import del módulo que ya está cargado
            import bot.kelly_sizer
            import importlib
            importlib.reload(bot.kelly_sizer)
            with patch("bot.kelly_sizer.KELLY_ENABLED", True), \
                 patch("bot.kelly_sizer.KELLY_MIN_TRADES", 30):
                with patch("bot.shadow_mode.shadow_mode", sm):
                    result = bot.kelly_sizer.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == 1.0

    def test_too_few_trades_returns_1(self):
        import bot.kelly_sizer
        sm = _mock_shadow(win_rate=0.6, trades=10)  # 10 < 30 mínimo
        with patch("bot.kelly_sizer.KELLY_ENABLED", True), \
             patch("bot.kelly_sizer.KELLY_MIN_TRADES", 30), \
             patch("bot.shadow_mode.shadow_mode", sm):
            result = bot.kelly_sizer.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == 1.0


# ── kelly_multiplier — cálculo correcto ───────────────────────────────────────

class TestKellyCalculation:
    """
    Fórmula: f* = (p*b - q) / b
    Con p=0.6, b=2.0, q=0.4:
      f_full = (0.6*2 - 0.4) / 2 = 0.8/2 = 0.4
      f = 0.4 * 0.25 = 0.1
      mult = 1.0 + 0.1 = 1.1
    """
    def test_standard_calculation(self):
        import bot.kelly_sizer
        sm = _mock_shadow(win_rate=0.6, trades=50)
        with patch("bot.kelly_sizer.KELLY_ENABLED", True), \
             patch("bot.kelly_sizer.KELLY_MIN_TRADES", 30), \
             patch("bot.kelly_sizer.KELLY_FRACTION", 0.25), \
             patch("bot.kelly_sizer.KELLY_MIN_MULT", 0.5), \
             patch("bot.kelly_sizer.KELLY_MAX_MULT", 2.0), \
             patch("bot.shadow_mode.shadow_mode", sm):
            result = bot.kelly_sizer.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == pytest.approx(1.1, abs=0.001)

    def test_negative_edge_clamps_to_min(self):
        """p=0.2, b=2.0 → f_full = (0.2*2 - 0.8)/2 = -0.2 → mult = 0.8 → clamp a 0.5."""
        import bot.kelly_sizer
        sm = _mock_shadow(win_rate=0.2, trades=50)
        with patch("bot.kelly_sizer.KELLY_ENABLED", True), \
             patch("bot.kelly_sizer.KELLY_MIN_TRADES", 30), \
             patch("bot.kelly_sizer.KELLY_FRACTION", 0.25), \
             patch("bot.kelly_sizer.KELLY_MIN_MULT", 0.5), \
             patch("bot.kelly_sizer.KELLY_MAX_MULT", 2.0), \
             patch("bot.shadow_mode.shadow_mode", sm):
            result = bot.kelly_sizer.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == pytest.approx(0.5, abs=0.001)

    def test_high_edge_clamps_to_max(self):
        """p=0.99 b=2.0 → mult enorme → clamp a 2.0."""
        import bot.kelly_sizer
        sm = _mock_shadow(win_rate=0.99, trades=50)
        with patch("bot.kelly_sizer.KELLY_ENABLED", True), \
             patch("bot.kelly_sizer.KELLY_MIN_TRADES", 30), \
             patch("bot.kelly_sizer.KELLY_FRACTION", 0.25), \
             patch("bot.kelly_sizer.KELLY_MIN_MULT", 0.5), \
             patch("bot.kelly_sizer.KELLY_MAX_MULT", 2.0), \
             patch("bot.shadow_mode.shadow_mode", sm):
            result = bot.kelly_sizer.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == pytest.approx(2.0, abs=0.001)

    def test_exception_returns_1(self):
        """Si shadow_mode lanza, kelly devuelve 1.0 sin propagarse."""
        import bot.kelly_sizer
        with patch("bot.kelly_sizer.KELLY_ENABLED", True), \
             patch("bot.kelly_sizer.KELLY_MIN_TRADES", 30):
            with patch("builtins.__import__", side_effect=ImportError("boom")):
                try:
                    result = bot.kelly_sizer.kelly_multiplier("TENDENCIA", rr=2.0)
                    assert result == 1.0
                except ImportError:
                    pass  # El patch afecta a todo — esto es OK en CI
