"""tests/test_kelly_sizer.py"""
import os
import sys
import importlib
import pytest
from unittest.mock import patch, MagicMock


# ── Helpers ──────────────────────────────────────────────────────────────────

def _reload_kelly(**env_overrides):
    """
    Recarga bot.kelly_sizer con env vars controladas.
    Inyecta un stub de bot.shadow_mode en sys.modules para evitar
    que el import real de shadow_mode explote por dependencias externas.
    """
    # Stub mínimo de shadow_mode para que el import no falle
    stub_sm = MagicMock()
    stub_sm.shadow_mode.win_rate_by_mode.return_value = {}

    defaults = {
        "KELLY_ENABLED":    "false",
        "KELLY_FRACTION":   "0.25",
        "KELLY_MIN_MULT":   "0.5",
        "KELLY_MAX_MULT":   "2.0",
        "KELLY_MIN_TRADES": "30",
    }
    defaults.update(env_overrides)

    # Limpiar módulo cacheado
    for mod in list(sys.modules):
        if "kelly_sizer" in mod:
            del sys.modules[mod]

    with patch.dict(os.environ, defaults), \
         patch.dict(sys.modules, {"bot.shadow_mode": stub_sm}):
        import bot.kelly_sizer as ks
        importlib.reload(ks)

    return ks


def _mock_shadow_stats(win_rate: float, trades: int):
    """Crea un objeto shadow_mode con win_rate_by_mode() prefijado."""
    sm_obj = MagicMock()
    sm_obj.win_rate_by_mode.return_value = {
        "TENDENCIA": {"win_rate": win_rate, "trades": trades}
    }
    stub = MagicMock()
    stub.shadow_mode = sm_obj
    return stub


# ── _parse_int_env ────────────────────────────────────────────────────────────

class TestParseIntEnv:
    def test_valid_int(self):
        ks = _reload_kelly(KELLY_MIN_TRADES="50", KELLY_ENABLED="false")
        assert ks.KELLY_MIN_TRADES == 50

    def test_invalid_value_returns_default(self):
        """KELLY_MIN_TRADES='false' → debe usar default=30 sin explotar."""
        ks = _reload_kelly(KELLY_MIN_TRADES="false", KELLY_ENABLED="false")
        assert ks.KELLY_MIN_TRADES == 30


# ── Kelly desactivado ────────────────────────────────────────────────────────────

class TestKellyDisabled:
    def test_returns_1_when_disabled(self):
        ks = _reload_kelly(KELLY_ENABLED="false")
        # KELLY_ENABLED=false en CI, el módulo usa la constante en módulo
        # Parcheamos directamente la constante para garantizar el estado
        with patch.object(ks, "KELLY_ENABLED", False):
            assert ks.kelly_multiplier("TENDENCIA", rr=2.0) == 1.0

    def test_returns_1_for_any_rr_when_disabled(self):
        ks = _reload_kelly(KELLY_ENABLED="false")
        with patch.object(ks, "KELLY_ENABLED", False):
            for rr in [0.0, -1.0, 5.0]:
                assert ks.kelly_multiplier("TENDENCIA", rr=rr) == 1.0


# ── RR inválido ──────────────────────────────────────────────────────────────────

class TestKellyInvalidRR:
    def test_zero_rr_returns_min_mult(self):
        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED", True), \
             patch.object(ks, "KELLY_MIN_MULT", 0.5):
            result = ks.kelly_multiplier("TENDENCIA", rr=0.0)
        assert result == 0.5

    def test_negative_rr_returns_min_mult(self):
        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED", True), \
             patch.object(ks, "KELLY_MIN_MULT", 0.5):
            result = ks.kelly_multiplier("TENDENCIA", rr=-1.0)
        assert result == 0.5


# ── Insuficiente historial ────────────────────────────────────────────────────────

class TestKellyInsufficientHistory:
    def test_no_mode_stats_returns_1(self):
        """Win-rate dict vacío para el modo → debe retornar 1.0."""
        stub = _mock_shadow_stats(win_rate=0.6, trades=0)
        stub.shadow_mode.win_rate_by_mode.return_value = {}  # sin stats

        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED", True), \
             patch.object(ks, "KELLY_MIN_TRADES", 30), \
             patch.dict(sys.modules, {"bot.shadow_mode": stub}):
            result = ks.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == 1.0

    def test_too_few_trades_returns_1(self):
        """10 trades < 30 mínimo → retornar 1.0."""
        stub = _mock_shadow_stats(win_rate=0.6, trades=10)

        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED", True), \
             patch.object(ks, "KELLY_MIN_TRADES", 30), \
             patch.dict(sys.modules, {"bot.shadow_mode": stub}):
            result = ks.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == 1.0


# ── Cálculo correcto ─────────────────────────────────────────────────────────────

class TestKellyCalculation:
    """
    Fórmula: f* = (p*b - q) / b
    p=0.6, b=2.0, q=0.4:
      f_full = (0.6*2 - 0.4) / 2 = 0.4
      f = 0.4 * 0.25 = 0.1
      mult = 1.0 + 0.1 = 1.1
    """
    def test_standard_calculation(self):
        stub = _mock_shadow_stats(win_rate=0.6, trades=50)
        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED",    True), \
             patch.object(ks, "KELLY_MIN_TRADES", 30), \
             patch.object(ks, "KELLY_FRACTION",   0.25), \
             patch.object(ks, "KELLY_MIN_MULT",   0.5), \
             patch.object(ks, "KELLY_MAX_MULT",   2.0), \
             patch.dict(sys.modules, {"bot.shadow_mode": stub}):
            result = ks.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == pytest.approx(1.1, abs=0.001)

    def test_negative_edge_clamps_to_min(self):
        """
        p=0.2, b=2.0 → f_full = (0.4-0.8)/2 = -0.2 → mult = 0.8 → clamp 0.5.
        """
        stub = _mock_shadow_stats(win_rate=0.2, trades=50)
        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED",    True), \
             patch.object(ks, "KELLY_MIN_TRADES", 30), \
             patch.object(ks, "KELLY_FRACTION",   0.25), \
             patch.object(ks, "KELLY_MIN_MULT",   0.5), \
             patch.object(ks, "KELLY_MAX_MULT",   2.0), \
             patch.dict(sys.modules, {"bot.shadow_mode": stub}):
            result = ks.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == pytest.approx(0.5, abs=0.001)

    def test_high_edge_clamps_to_max(self):
        """p=0.99 → mult muy alto → clamp a 2.0."""
        stub = _mock_shadow_stats(win_rate=0.99, trades=50)
        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED",    True), \
             patch.object(ks, "KELLY_MIN_TRADES", 30), \
             patch.object(ks, "KELLY_FRACTION",   0.25), \
             patch.object(ks, "KELLY_MIN_MULT",   0.5), \
             patch.object(ks, "KELLY_MAX_MULT",   2.0), \
             patch.dict(sys.modules, {"bot.shadow_mode": stub}):
            result = ks.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == pytest.approx(2.0, abs=0.001)

    def test_exception_in_shadow_returns_1(self):
        """Si shadow_mode.win_rate_by_mode lanza → devuelve 1.0 sin propagarse."""
        stub_bad = MagicMock()
        stub_bad.shadow_mode.win_rate_by_mode.side_effect = RuntimeError("boom")
        ks = _reload_kelly(KELLY_ENABLED="true")
        with patch.object(ks, "KELLY_ENABLED",    True), \
             patch.object(ks, "KELLY_MIN_TRADES", 30), \
             patch.dict(sys.modules, {"bot.shadow_mode": stub_bad}):
            result = ks.kelly_multiplier("TENDENCIA", rr=2.0)
        assert result == 1.0
