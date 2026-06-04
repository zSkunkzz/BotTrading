"""tests/test_trailing_sl.py"""
import pytest
from bot.trailing_sl import compute_trailing_sl, is_trailing_sl_hit


# ── compute_trailing_sl — LONG ────────────────────────────────────────────────

class TestComputeTrailingSlLong:
    PCT = 0.015  # 1.5%

    def test_new_high_moves_sl_up(self):
        """Precio sube: nuevo pico = 110, SL = 110 * 0.985 = 108.35."""
        new_sl, new_peak = compute_trailing_sl(
            is_long=True, current_price=110.0, peak_price=100.0,
            current_sl=90.0, trailing_pct=self.PCT,
        )
        assert new_peak == pytest.approx(110.0)
        assert new_sl == pytest.approx(110.0 * (1 - self.PCT), abs=0.001)

    def test_price_below_peak_keeps_sl(self):
        """Precio cae por debajo del pico: el SL NO retrocede."""
        new_sl, new_peak = compute_trailing_sl(
            is_long=True, current_price=105.0, peak_price=110.0,
            current_sl=108.35, trailing_pct=self.PCT,
        )
        assert new_peak == pytest.approx(110.0)  # pico no cambia
        assert new_sl == pytest.approx(108.35)    # SL no retrocede

    def test_sl_never_decreases(self):
        """Garantía: el SL solo avanza en dirección favorable (nunca baja)."""
        current_sl = 99.0
        new_sl, _ = compute_trailing_sl(
            is_long=True, current_price=98.0, peak_price=100.0,
            current_sl=current_sl, trailing_pct=self.PCT,
        )
        assert new_sl >= current_sl

    def test_peak_tracks_highest_price(self):
        """El pico siempre es el máximo histórico."""
        _, peak1 = compute_trailing_sl(
            is_long=True, current_price=120.0, peak_price=100.0,
            current_sl=90.0, trailing_pct=self.PCT,
        )
        _, peak2 = compute_trailing_sl(
            is_long=True, current_price=110.0, peak_price=peak1,
            current_sl=90.0, trailing_pct=self.PCT,
        )
        assert peak2 == pytest.approx(120.0)  # conserva el máximo

    def test_custom_trailing_pct(self):
        new_sl, _ = compute_trailing_sl(
            is_long=True, current_price=100.0, peak_price=100.0,
            current_sl=0.0, trailing_pct=0.02,  # 2%
        )
        assert new_sl == pytest.approx(100.0 * 0.98, abs=0.001)


# ── compute_trailing_sl — SHORT ───────────────────────────────────────────────

class TestComputeTrailingSlShort:
    PCT = 0.015

    def test_new_low_moves_sl_down(self):
        """Precio baja: nuevo pico-min = 90, SL = 90 * 1.015 = 91.35."""
        new_sl, new_peak = compute_trailing_sl(
            is_long=False, current_price=90.0, peak_price=100.0,
            current_sl=110.0, trailing_pct=self.PCT,
        )
        assert new_peak == pytest.approx(90.0)
        assert new_sl == pytest.approx(90.0 * (1 + self.PCT), abs=0.001)

    def test_price_above_peak_min_keeps_sl(self):
        """Precio sube sin alcanzar mínimo favorable: el SL NO retrocede."""
        new_sl, new_peak = compute_trailing_sl(
            is_long=False, current_price=95.0, peak_price=90.0,
            current_sl=91.35, trailing_pct=self.PCT,
        )
        assert new_peak == pytest.approx(90.0)  # pico no empeora
        assert new_sl == pytest.approx(91.35)    # SL no sube

    def test_sl_never_increases_for_short(self):
        """Para shorts: el SL solo baja, nunca sube."""
        current_sl = 101.0
        new_sl, _ = compute_trailing_sl(
            is_long=False, current_price=102.0, peak_price=90.0,
            current_sl=current_sl, trailing_pct=self.PCT,
        )
        assert new_sl <= current_sl

    def test_initial_peak_zero_uses_current_price(self):
        """Si peak_price=0 (sin historial), el pico inicial es el precio actual."""
        _, new_peak = compute_trailing_sl(
            is_long=False, current_price=95.0, peak_price=0,
            current_sl=200.0, trailing_pct=self.PCT,
        )
        assert new_peak == pytest.approx(95.0)


# ── is_trailing_sl_hit ────────────────────────────────────────────────────────

class TestIsTrailingSlHit:
    def test_long_hit_when_price_at_sl(self):
        assert is_trailing_sl_hit(is_long=True, current_price=98.0, trailing_sl=98.0)

    def test_long_hit_when_price_below_sl(self):
        assert is_trailing_sl_hit(is_long=True, current_price=97.5, trailing_sl=98.0)

    def test_long_not_hit_when_price_above_sl(self):
        assert not is_trailing_sl_hit(is_long=True, current_price=99.0, trailing_sl=98.0)

    def test_short_hit_when_price_at_sl(self):
        assert is_trailing_sl_hit(is_long=False, current_price=105.0, trailing_sl=105.0)

    def test_short_hit_when_price_above_sl(self):
        assert is_trailing_sl_hit(is_long=False, current_price=106.0, trailing_sl=105.0)

    def test_short_not_hit_when_price_below_sl(self):
        assert not is_trailing_sl_hit(is_long=False, current_price=104.0, trailing_sl=105.0)

    def test_long_exactly_at_sl_is_hit(self):
        """Borde: precio exactamente igual al SL → hit."""
        assert is_trailing_sl_hit(is_long=True, current_price=100.0, trailing_sl=100.0)

    def test_short_exactly_at_sl_is_hit(self):
        assert is_trailing_sl_hit(is_long=False, current_price=100.0, trailing_sl=100.0)


# ── Integración: compute + hit ────────────────────────────────────────────────

class TestTrailingIntegration:
    def test_long_trail_then_hit(self):
        """Simula 3 ticks subiendo y luego un tick que toca el trailing SL."""
        PCT = 0.015
        sl = 95.0
        peak = 100.0

        # Tick 1: precio sube a 105
        sl, peak = compute_trailing_sl(
            is_long=True, current_price=105.0, peak_price=peak,
            current_sl=sl, trailing_pct=PCT,
        )
        assert not is_trailing_sl_hit(is_long=True, current_price=105.0, trailing_sl=sl)

        # Tick 2: precio sube a 110
        sl, peak = compute_trailing_sl(
            is_long=True, current_price=110.0, peak_price=peak,
            current_sl=sl, trailing_pct=PCT,
        )
        expected_sl = 110.0 * (1 - PCT)
        assert sl == pytest.approx(expected_sl, abs=0.001)

        # Tick 3: precio cae y toca el trailing SL
        assert is_trailing_sl_hit(is_long=True, current_price=expected_sl - 0.01, trailing_sl=sl)

    def test_short_trail_then_hit(self):
        PCT = 0.015
        sl = 105.0
        peak = 100.0

        # Tick 1: precio baja a 95
        sl, peak = compute_trailing_sl(
            is_long=False, current_price=95.0, peak_price=peak,
            current_sl=sl, trailing_pct=PCT,
        )
        assert not is_trailing_sl_hit(is_long=False, current_price=95.0, trailing_sl=sl)

        # Tick 2: precio baja a 90
        sl, peak = compute_trailing_sl(
            is_long=False, current_price=90.0, peak_price=peak,
            current_sl=sl, trailing_pct=PCT,
        )
        expected_sl = 90.0 * (1 + PCT)
        assert sl == pytest.approx(expected_sl, abs=0.001)

        # Tick 3: precio sube y toca el trailing SL
        assert is_trailing_sl_hit(is_long=False, current_price=expected_sl + 0.01, trailing_sl=sl)
