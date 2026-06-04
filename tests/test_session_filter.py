"""tests/test_session_filter.py"""
import os
import importlib
import sys
from datetime import datetime, timezone
from unittest.mock import patch
import pytest


def _reload_session(enabled="true", start="7", end="18", allow_reversal="true"):
    """Recarga session_filter con env vars controladas."""
    for mod in list(sys.modules):
        if "session_filter" in mod:
            del sys.modules[mod]
    with patch.dict(os.environ, {
        "SESSION_FILTER_ENABLED": enabled,
        "SESSION_START_UTC":      start,
        "SESSION_END_UTC":        end,
        "SESSION_ALLOW_REVERSAL": allow_reversal,
    }):
        import bot.session_filter as sf
        importlib.reload(sf)
    return sf


def _fake_dt(hour: int, minute: int = 0):
    """datetime UTC con hora fija para inyectar en is_trading_session."""
    return datetime(2026, 1, 15, hour, minute, tzinfo=timezone.utc)


# ── Filtro desactivado ────────────────────────────────────────────────────────

class TestFilterDisabled:
    def test_always_allowed_when_disabled(self):
        sf = _reload_session(enabled="false")
        # Hora de noche (fuera de sesión) — pero filtro off
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", False):
            allowed, reason = sf.is_trading_session("TENDENCIA")
        assert allowed is True
        assert reason == ""

    def test_check_session_returns_none_when_disabled(self):
        sf = _reload_session(enabled="false")
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", False):
            assert sf.check_session("BREAKOUT") is None


# ── REVERSAL siempre permitido ────────────────────────────────────────────────

class TestReversalAlwaysAllowed:
    def test_reversal_allowed_at_night(self):
        """REVERSAL debe pasar aunque sea las 03:00 UTC (fuera de sesión)."""
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(3, 0)
            allowed, reason = sf.is_trading_session("REVERSAL")
        assert allowed is True
        assert reason == ""

    def test_reversal_blocked_when_allow_reversal_false(self):
        """Si SESSION_ALLOW_REVERSAL=false, REVERSAL se bloquea fuera de sesión."""
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", False), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(3, 0)
            allowed, reason = sf.is_trading_session("REVERSAL")
        assert allowed is False
        assert reason != ""


# ── Dentro de sesión ─────────────────────────────────────────────────────────

class TestInsideSession:
    @pytest.mark.parametrize("hour,minute", [
        (7,  0),   # exactamente el inicio
        (9,  30),  # London mañana
        (13, 0),   # NY open
        (17, 59),  # último minuto
    ])
    def test_tendencia_allowed_in_session(self, hour, minute):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(hour, minute)
            allowed, reason = sf.is_trading_session("TENDENCIA")
        assert allowed is True, f"Debería estar en sesión a las {hour:02d}:{minute:02d}"

    def test_breakout_allowed_at_london_open(self):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(8, 0)
            allowed, _ = sf.is_trading_session("BREAKOUT")
        assert allowed is True


# ── Fuera de sesión ───────────────────────────────────────────────────────────

class TestOutsideSession:
    @pytest.mark.parametrize("hour", [0, 3, 6, 18, 22, 23])
    def test_tendencia_blocked_outside_session(self, hour):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(hour)
            allowed, reason = sf.is_trading_session("TENDENCIA")
        assert allowed is False, f"Debería estar bloqueado a las {hour:02d}:00"
        assert "sesión" in reason.lower() or "session" in reason.lower() or "UTC" in reason

    def test_reason_contains_current_time(self):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(3, 45)
            _, reason = sf.is_trading_session("BREAKOUT")
        assert "03:45" in reason

    def test_exactly_at_session_end_is_blocked(self):
        """SESSION_END_UTC=18 → las 18:00:00 ya está bloqueado (< end, no <=)."""
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(18, 0)
            allowed, _ = sf.is_trading_session("TENDENCIA")
        assert allowed is False


# ── check_session (wrapper) ───────────────────────────────────────────────────

class TestCheckSession:
    def test_returns_none_when_allowed(self):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(10, 0)
            assert sf.check_session("TENDENCIA") is None

    def test_returns_reason_when_blocked(self):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(2, 0)
            result = sf.check_session("BREAKOUT")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_no_setup_type_defaults_gracefully(self):
        sf = _reload_session()
        with patch("bot.session_filter.SESSION_FILTER_ENABLED", True), \
             patch("bot.session_filter.SESSION_START_UTC", 7), \
             patch("bot.session_filter.SESSION_END_UTC", 18), \
             patch("bot.session_filter.SESSION_ALLOW_REVERSAL", True), \
             patch("bot.session_filter.datetime") as mock_dt:
            mock_dt.now.return_value = _fake_dt(2, 0)
            result = sf.check_session(None)  # sin tipo de setup
        assert result is not None  # bloqueado fuera de sesión
