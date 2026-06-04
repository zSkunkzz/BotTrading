"""tests/test_position_manager.py"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bot.position_manager import (
    PositionManager,
    _get_tpsl_type,
    _is_reduce_only,
    _resolve_is_long,
    _round_qty_safe,
    _calc_fallback_tp,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_trader(
    symbol="BTCUSDC",
    position="long",
    entry=100.0,
    sl=97.0,
    tp1=110.0,
    price=101.0,
    qty=0.01,
    dry_run=True,
    protection_ok=False,
    tp1_be_done=False,
):
    t = MagicMock()
    t.symbol        = symbol
    t.coin          = symbol
    t.position      = position
    t.entry_price   = entry
    t.sl            = sl
    t.tp1           = tp1
    t._last_price   = price
    t._open_qty     = qty
    t.dry_run       = dry_run
    t._protection_ok = protection_ok
    t._tp1_be_done  = tp1_be_done
    t._get_open_orders_raw          = AsyncMock(return_value=[])
    t._get_open_trigger_orders_raw  = AsyncMock(return_value=[])
    t._place_tpsl                   = AsyncMock()
    t._close_position               = AsyncMock()
    return t


def _sl_order(price=97.0):
    return {"orderType": {"trigger": {"tpsl": "sl"}}, "coin": "BTCUSDC", "limitPx": str(price)}


def _tp_order(price=110.0):
    return {"orderType": {"trigger": {"tpsl": "tp"}}, "coin": "BTCUSDC", "limitPx": str(price)}


# ── _get_tpsl_type ─────────────────────────────────────────────────────────────

class TestGetTpslType:
    def test_via_orderType_trigger(self):
        o = {"orderType": {"trigger": {"tpsl": "sl"}}}
        assert _get_tpsl_type(o) == "sl"

    def test_via_type_trigger(self):
        o = {"type": {"trigger": {"tpsl": "tp"}}}
        assert _get_tpsl_type(o) == "tp"

    def test_via_tpsl_field(self):
        o = {"tpsl": "sl"}
        assert _get_tpsl_type(o) == "sl"

    def test_no_tpsl_returns_none(self):
        assert _get_tpsl_type({}) is None

    def test_empty_trigger_returns_none(self):
        o = {"orderType": {"trigger": {}}}
        assert _get_tpsl_type(o) is None


# ── _is_reduce_only ────────────────────────────────────────────────────────────

class TestIsReduceOnly:
    def test_reduce_only_flag(self):
        assert _is_reduce_only({"reduceOnly": True})

    def test_sl_is_reduce_only(self):
        assert _is_reduce_only({"orderType": {"trigger": {"tpsl": "sl"}}})

    def test_tp_is_reduce_only(self):
        assert _is_reduce_only({"orderType": {"trigger": {"tpsl": "tp"}}})

    def test_normal_order_not_reduce_only(self):
        assert not _is_reduce_only({"orderType": {}})


# ── _resolve_is_long ──────────────────────────────────────────────────────────

class TestResolveIsLong:
    def test_str_long(self):
        assert _resolve_is_long("long") is True

    def test_str_LONG_uppercase(self):
        assert _resolve_is_long("LONG") is True

    def test_str_short(self):
        assert _resolve_is_long("short") is False

    def test_dict_long(self):
        assert _resolve_is_long({"side": "LONG"}) is True

    def test_dict_short(self):
        assert _resolve_is_long({"side": "SHORT"}) is False

    def test_dict_missing_side(self):
        assert _resolve_is_long({}) is False

    def test_none_returns_false(self):
        assert _resolve_is_long(None) is False


# ── _round_qty_safe ───────────────────────────────────────────────────────────

class TestRoundQtySafe:
    def test_uses_trader_round_qty(self):
        t = MagicMock()
        t._round_qty = MagicMock(return_value=0.0100)
        assert _round_qty_safe(t, 0.01234) == 0.0100

    def test_fallback_when_no_method(self):
        t = MagicMock(spec=[])  # sin atributos
        result = _round_qty_safe(t, 0.123456789)
        assert result == round(0.123456789, 4)

    def test_fallback_when_method_raises(self):
        t = MagicMock()
        t._round_qty = MagicMock(side_effect=ValueError("boom"))
        result = _round_qty_safe(t, 0.1234)
        assert result == round(0.1234, 4)


# ── _calc_fallback_tp ─────────────────────────────────────────────────────────

class TestCalcFallbackTp:
    def test_long_positive_rr(self):
        tp = _calc_fallback_tp(100.0, 97.0, True, 2.0)
        assert abs(tp - 106.0) < 0.001

    def test_short_positive_rr(self):
        tp = _calc_fallback_tp(100.0, 103.0, False, 2.0)
        assert abs(tp - 94.0) < 0.001

    def test_zero_entry_returns_none(self):
        assert _calc_fallback_tp(0, 97.0, True, 2.0) is None

    def test_zero_sl_returns_none(self):
        assert _calc_fallback_tp(100.0, 0, True, 2.0) is None

    def test_entry_equals_sl_returns_none(self):
        assert _calc_fallback_tp(100.0, 100.0, True, 2.0) is None


# ── _check_sl_software ────────────────────────────────────────────────────────

class TestCheckSlSoftware:
    @pytest.mark.asyncio
    async def test_no_sl_returns_false(self):
        t = _make_trader(sl=None)
        pm = PositionManager(t)
        assert await pm._check_sl_software() is False

    @pytest.mark.asyncio
    async def test_no_position_returns_false(self):
        t = _make_trader(position=None)
        pm = PositionManager(t)
        assert await pm._check_sl_software() is False

    @pytest.mark.asyncio
    async def test_price_above_sl_long_no_trigger(self):
        t = _make_trader(position="long", price=101.0, sl=97.0)
        pm = PositionManager(t)
        assert await pm._check_sl_software() is False

    @pytest.mark.asyncio
    async def test_price_below_sl_long_triggers_close(self):
        t = _make_trader(position="long", price=96.0, sl=97.0, protection_ok=False)
        pm = PositionManager(t)
        result = await pm._check_sl_software()
        assert result is True
        t._close_position.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_price_above_sl_short_triggers_close(self):
        t = _make_trader(position="short", price=104.0, sl=103.0, protection_ok=False)
        pm = PositionManager(t)
        result = await pm._check_sl_software()
        assert result is True
        t._close_position.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_protection_ok_skips_emergency_close(self):
        """Si _protection_ok=True el exchange ya tiene la orden — no cerrar."""
        t = _make_trader(position="long", price=96.0, sl=97.0, protection_ok=True)
        pm = PositionManager(t)
        result = await pm._check_sl_software()
        assert result is False
        t._close_position.assert_not_awaited()


# ── _check_break_even ─────────────────────────────────────────────────────────

class TestCheckBreakEven:
    @pytest.mark.asyncio
    async def test_be_already_done_skips(self):
        t = _make_trader(tp1_be_done=True)
        pm = PositionManager(t)
        await pm._check_break_even()
        # no cambia sl
        assert t.sl == 97.0

    @pytest.mark.asyncio
    async def test_price_not_at_trigger_no_be(self):
        # entry=100, tp1=110 → trigger al 40%: 104. Precio en 102 → sin BE
        t = _make_trader(entry=100.0, tp1=110.0, price=102.0, sl=97.0)
        pm = PositionManager(t)
        await pm._check_break_even()
        assert t._tp1_be_done is False

    @pytest.mark.asyncio
    async def test_price_at_trigger_activates_be_dry_run(self):
        # entry=100, tp1=110 → trigger al 40%: 104. Precio en 105 → BE
        t = _make_trader(entry=100.0, tp1=110.0, price=105.0, sl=97.0, dry_run=True)
        pm = PositionManager(t)
        await pm._check_break_even()
        assert t._tp1_be_done is True
        # En dry_run el sl se actualiza igualmente en memoria
        assert t.sl == pytest.approx(100.0, abs=0.001)

    @pytest.mark.asyncio
    async def test_sl_already_above_be_marks_done_no_move(self):
        # SL ya está por encima del BE (entrada) en long
        t = _make_trader(entry=100.0, tp1=110.0, price=105.0, sl=101.0)
        pm = PositionManager(t)
        await pm._check_break_even()
        assert t._tp1_be_done is True
        assert t.sl == 101.0  # sin cambio

    @pytest.mark.asyncio
    async def test_no_position_skips(self):
        t = _make_trader(price=105.0)
        t.position = None
        pm = PositionManager(t)
        await pm._check_break_even()  # no debe lanzar


# ── _ensure_tpsl ─────────────────────────────────────────────────────────────

class TestEnsureTpsl:
    @pytest.mark.asyncio
    async def test_sl_and_tp_present_sets_protection_ok(self):
        t = _make_trader()
        t._get_open_orders_raw         = AsyncMock(return_value=[_sl_order(), _tp_order()])
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[])
        pm = PositionManager(t)
        await pm._ensure_tpsl()
        assert t._protection_ok is True
        t._place_tpsl.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sl_in_trigger_orders_detected(self):
        t = _make_trader()
        t._get_open_orders_raw         = AsyncMock(return_value=[_tp_order()])
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[_sl_order()])
        pm = PositionManager(t)
        await pm._ensure_tpsl()
        assert t._protection_ok is True

    @pytest.mark.asyncio
    async def test_missing_sl_triggers_emergency(self):
        t = _make_trader(protection_ok=False)
        t._get_open_orders_raw         = AsyncMock(return_value=[_tp_order()])
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[])
        pm = PositionManager(t)
        await pm._ensure_tpsl()
        t._place_tpsl.assert_awaited()

    @pytest.mark.asyncio
    async def test_missing_tp_triggers_emergency(self):
        t = _make_trader(protection_ok=False)
        t._get_open_orders_raw         = AsyncMock(return_value=[_sl_order()])
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[])
        pm = PositionManager(t)
        await pm._ensure_tpsl()
        t._place_tpsl.assert_awaited()

    @pytest.mark.asyncio
    async def test_protection_ok_skips_emergency_when_missing(self):
        """Si _protection_ok=True y no se ven SL/TP, probablemente ejecutados — no spamear."""
        t = _make_trader(protection_ok=True)
        t._get_open_orders_raw         = AsyncMock(return_value=[])
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[])
        pm = PositionManager(t)
        await pm._ensure_tpsl()
        t._place_tpsl.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fallback_by_price_detects_sl(self):
        """Bug B: detecta SL por precio cuando el campo tpsl no viene."""
        t = _make_trader(sl=97.0)
        # Orden reduce_only sin campo tpsl, pero precio muy cercano al SL
        order = {"coin": "BTCUSDC", "reduceOnly": True, "limitPx": "97.01"}
        tp_order = _tp_order()
        t._get_open_orders_raw         = AsyncMock(return_value=[order, tp_order])
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[])
        pm = PositionManager(t)
        await pm._ensure_tpsl()
        # SL detectado por fallback → no emergencia
        t._place_tpsl.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_open_orders_exception_handled(self):
        t = _make_trader(protection_ok=False)
        t._get_open_orders_raw         = AsyncMock(side_effect=RuntimeError("network"))
        t._get_open_trigger_orders_raw = AsyncMock(return_value=[])
        pm = PositionManager(t)
        await pm._ensure_tpsl()  # no debe lanzar


# ── _place_emergency_sl_tp ─────────────────────────────────────────────────────

class TestPlaceEmergencySlTp:
    @pytest.mark.asyncio
    async def test_places_sl_and_tp(self):
        t = _make_trader(sl=97.0, tp1=110.0, qty=0.01)
        pm = PositionManager(t)
        await pm._place_emergency_sl_tp(place_sl=True, place_tp=True)
        assert t._place_tpsl.await_count == 2
        assert t._protection_ok is True

    @pytest.mark.asyncio
    async def test_zero_qty_skips(self):
        t = _make_trader(qty=0.0)
        pm = PositionManager(t)
        await pm._place_emergency_sl_tp(place_sl=True, place_tp=True)
        t._place_tpsl.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fallback_tp_calculated_when_missing(self):
        """Bug J: TP dinámico cuando tp1=None."""
        t = _make_trader(tp1=None, sl=97.0, entry=100.0, qty=0.01)
        t.tp1 = None
        t.tp  = None
        pm = PositionManager(t)
        await pm._place_emergency_sl_tp(place_sl=False, place_tp=True)
        # TP calculado dinámicamente: entry=100, sl=97 → risk=3, rr=1.5 → tp=104.5
        assert t.tp1 is not None
        assert t.tp1 > 100.0

    @pytest.mark.asyncio
    async def test_exception_retries(self):
        t = _make_trader(sl=97.0, tp1=110.0, qty=0.01)
        call_count = 0

        async def failing_place_tpsl(**kw):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("exchange error")

        t._place_tpsl = failing_place_tpsl
        pm = PositionManager(t)
        # Parchear asyncio.sleep para no esperar
        with patch("bot.position_manager.asyncio.sleep", new=AsyncMock()):
            await pm._place_emergency_sl_tp(place_sl=True, place_tp=False)
        # Debe haber reintentado EMERGENCY_TPSL_RETRIES veces
        from bot.position_manager import _EMERGENCY_TPSL_RETRIES
        assert call_count == _EMERGENCY_TPSL_RETRIES


# ── _emergency_close ─────────────────────────────────────────────────────────

class TestEmergencyClose:
    @pytest.mark.asyncio
    async def test_calls_close_position(self):
        t = _make_trader()
        pm = PositionManager(t)
        await pm._emergency_close(reason="TEST")
        t._close_position.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_close_fn_doesnt_crash(self):
        t = _make_trader()
        del t._close_position
        pm = PositionManager(t)
        await pm._emergency_close()  # no debe lanzar

    @pytest.mark.asyncio
    async def test_close_exception_logged_not_raised(self):
        t = _make_trader()
        t._close_position = AsyncMock(side_effect=RuntimeError("boom"))
        pm = PositionManager(t)
        await pm._emergency_close()  # no debe propagarse
