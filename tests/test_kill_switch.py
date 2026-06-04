"""tests/test_kill_switch.py"""
import asyncio
import json
import os
import tempfile
import time
import pytest
from unittest.mock import patch, AsyncMock


def _fresh_ks(state_path=None):
    """Devuelve una instancia limpia de KillSwitch sin estado de disco."""
    import importlib
    import sys
    # Forzar recarga para que no use el singleton global
    if "bot.kill_switch" in sys.modules:
        del sys.modules["bot.kill_switch"]
    path = state_path or "/tmp/ks_test_nonexistent_file_xyz.json"
    with patch.dict(os.environ, {"KILL_SWITCH_STATE_PATH": path}):
        import bot.kill_switch as ks_mod
        ks = ks_mod.KillSwitch()
    return ks


class TestKillSwitchState:
    def test_initial_level_is_zero(self):
        ks = _fresh_ks()
        assert ks.level() == 0

    def test_not_halted_initially(self):
        ks = _fresh_ks()
        assert ks.is_halted() is False

    def test_not_hard_killed_initially(self):
        ks = _fresh_ks()
        assert ks.is_hard_killed() is False


class TestActivate:
    @pytest.mark.asyncio
    async def test_activate_sets_level(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(1, "test")
        assert ks.level() == 1

    @pytest.mark.asyncio
    async def test_activate_does_not_downgrade(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(3, "high")
            await ks.activate(1, "low")
        assert ks.level() == 3

    @pytest.mark.asyncio
    async def test_l1_is_halted(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(1, "test")
        assert ks.is_halted() is True

    @pytest.mark.asyncio
    async def test_activate_symbol_halts_symbol(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate_symbol("BTCUSDC", "test")
        assert ks.is_halted("BTCUSDC") is True
        assert ks.is_halted("ETHUSDC") is False

    @pytest.mark.asyncio
    async def test_hard_kill_sets_flag(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.hard_kill("test")
        assert ks.is_hard_killed() is True
        assert ks.level() == 4


class TestManualReset:
    @pytest.mark.asyncio
    async def test_reset_clears_level(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(2, "test")
            await ks.manual_reset()
        assert ks.level() == 0

    @pytest.mark.asyncio
    async def test_reset_clears_halted_symbols(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate_symbol("BTCUSDC", "test")
            await ks.manual_reset()
        assert ks.is_halted("BTCUSDC") is False

    @pytest.mark.asyncio
    async def test_reset_ignores_key(self):
        """Manual reset no requiere clave — cualquier string funciona."""
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(1, "test")
            result = await ks.manual_reset("wrong_key")
        assert result is True
        assert ks.level() == 0


class TestOnTradeResult:
    @pytest.mark.asyncio
    async def test_losses_accumulate(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()):
            await ks.on_trade_result(-1.0)
            await ks.on_trade_result(-1.0)
        assert ks._daily_pnl == pytest.approx(-2.0)
        assert ks._consec_losses == 2

    @pytest.mark.asyncio
    async def test_win_resets_consec_losses(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()):
            await ks.on_trade_result(-1.0)
            await ks.on_trade_result(-1.0)
            await ks.on_trade_result(2.0)  # win
        assert ks._consec_losses == 0

    @pytest.mark.asyncio
    async def test_daily_loss_limit_activates_l3(self):
        ks = _fresh_ks()
        activated = []
        with patch("bot.kill_switch.KillSwitch._save_state"):
            original_activate = ks.activate
            async def capture_activate(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture_activate
            with patch("bot.kill_switch.KillSwitch._save_state"):
                await ks.on_trade_result(-9.0)  # supera el 8% por defecto
        assert 3 in activated

    @pytest.mark.asyncio
    async def test_consec_losses_activates_l2(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            original_activate = ks.activate
            activated = []
            async def capture(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture
            for _ in range(5):  # límite por defecto es 5
                await ks.on_trade_result(-0.5)
        assert 2 in activated


class TestOnOrderResult:
    @pytest.mark.asyncio
    async def test_reject_rate_activates_l2(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            activated = []
            original_activate = ks.activate
            async def capture(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture
            # 10 órdenes, todas rechazadas (rate = 100% > 50%)
            for _ in range(10):
                await ks.on_order_result(rejected=True)
        assert 2 in activated

    @pytest.mark.asyncio
    async def test_low_reject_rate_no_activation(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()) as mock_activate:
            for i in range(10):
                await ks.on_order_result(rejected=(i % 5 == 0))  # 20% rechazos
        mock_activate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fewer_than_10_orders_no_activation(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()) as mock_activate:
            for _ in range(9):  # menos de 10
                await ks.on_order_result(rejected=True)
        mock_activate.assert_not_awaited()


class TestOnSlippage:
    @pytest.mark.asyncio
    async def test_high_slippage_activates_l1(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            activated = []
            original_activate = ks.activate
            async def capture(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture
            for _ in range(5):
                await ks.on_slippage(100.0)  # 100bps > límite 80bps
        assert 1 in activated

    @pytest.mark.asyncio
    async def test_low_slippage_no_activation(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()) as mock_activate:
            for _ in range(5):
                await ks.on_slippage(30.0)  # 30bps < 80bps
        mock_activate.assert_not_awaited()


class TestOnApiReconnect:
    @pytest.mark.asyncio
    async def test_too_many_reconnects_activates_l2(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            activated = []
            original_activate = ks.activate
            async def capture(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture
            for _ in range(10):  # límite por defecto es 10
                await ks.on_api_reconnect()
        assert 2 in activated


class TestOnStateMismatch:
    @pytest.mark.asyncio
    async def test_mismatches_activate_l3(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            activated = []
            original_activate = ks.activate
            async def capture(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture
            for _ in range(3):
                await ks.on_state_mismatch("BTCUSDC")
        assert 3 in activated

    @pytest.mark.asyncio
    async def test_tpsl_retrying_symbol_ignored(self):
        """Bug M: símbolos en retry de TPSL no cuentan como mismatch."""
        ks = _fresh_ks()
        ks.mark_tpsl_retrying("BTCUSDC")
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()) as mock_activate:
            for _ in range(5):
                await ks.on_state_mismatch("BTCUSDC")
        mock_activate.assert_not_awaited()
        assert ks._state_mismatches == 0

    @pytest.mark.asyncio
    async def test_clear_tpsl_retrying_reenables(self):
        ks = _fresh_ks()
        ks.mark_tpsl_retrying("BTCUSDC")
        ks.clear_tpsl_retrying("BTCUSDC")
        with patch("bot.kill_switch.KillSwitch._save_state"):
            activated = []
            original_activate = ks.activate
            async def capture(level, trigger):
                activated.append(level)
                await original_activate(level, trigger)
            ks.activate = capture
            for _ in range(3):
                await ks.on_state_mismatch("BTCUSDC")
        assert 3 in activated


class TestResetDailyPnl:
    @pytest.mark.asyncio
    async def test_resets_counters(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"), \
             patch.object(ks, "activate", new=AsyncMock()):
            await ks.on_trade_result(-2.0)
            await ks.on_api_reconnect()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.reset_daily_pnl()
        assert ks._daily_pnl == 0.0
        assert ks._consec_losses == 0
        assert ks._api_reconnects == 0


class TestPersistence:
    def test_save_and_load_state(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            ks = _fresh_ks(state_path=path)
            ks._level = 2
            ks._trigger = "test_trigger"
            ks._halted_symbols = {"ETHUSDC"}
            ks._ks2_activated_epoch = time.time()
            ks._save_state()

            ks2 = _fresh_ks(state_path=path)
            assert ks2._level == 2
            assert ks2._trigger == "test_trigger"
            assert "ETHUSDC" in ks2._halted_symbols
        finally:
            os.unlink(path)

    def test_load_recalculates_monotonic_from_epoch(self):
        """FIX #3: el cooldown de L2 sobrevive a reinicios."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            epoch_1min_ago = time.time() - 60
            json.dump({
                "level": 2,
                "trigger": "test",
                "triggered_at": epoch_1min_ago,
                "consec_losses": 0,
                "daily_pnl": 0.0,
                "api_reconnects": 0,
                "state_mismatches": 0,
                "halted_symbols": [],
                "hard_killed": False,
                "ks2_activated_at_epoch": epoch_1min_ago,
            }, f)
            path = f.name
        try:
            ks = _fresh_ks(state_path=path)
            # El monotonic debe reflejar que L2 se activó hace ~60s
            elapsed = time.monotonic() - ks._ks2_activated_at
            assert 55 < elapsed < 70  # tolerancia para CI lento
        finally:
            os.unlink(path)

    def test_missing_state_file_no_crash(self):
        ks = _fresh_ks(state_path="/tmp/ks_definitely_not_exists_xyz123.json")
        assert ks.level() == 0


class TestMaybeAutoresetL2:
    @pytest.mark.asyncio
    async def test_autoreset_after_cooldown(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(2, "test")
        # Simular cooldown expirado poniendo activated_at muy atrás
        ks._ks2_activated_at = time.monotonic() - 9999
        with patch.dict(os.environ, {"KS_L2_COOLDOWN_SECONDS": "10"}), \
             patch("bot.kill_switch.KillSwitch._save_state"), \
             patch("bot.kill_switch._CFG", {**__import__('bot.kill_switch', fromlist=['_CFG']).__dict__.get('_CFG', {}), "l2_cooldown_seconds": 10}):
            # Forzar directamente la comprobación con cooldown=10s
            ks._ks2_activated_at = time.monotonic() - 9999
            # Parche manual de _CFG local
            import bot.kill_switch as ks_mod
            original_cfg = ks_mod._CFG.copy()
            ks_mod._CFG["l2_cooldown_seconds"] = 10
            with patch("bot.kill_switch.KillSwitch._save_state"):
                result = await ks._maybe_autoreset_l2()
            ks_mod._CFG.update(original_cfg)
        assert result is True
        assert ks.level() == 0

    @pytest.mark.asyncio
    async def test_no_autoreset_within_cooldown(self):
        ks = _fresh_ks()
        with patch("bot.kill_switch.KillSwitch._save_state"):
            await ks.activate(2, "test")
        # activated_at reciente
        ks._ks2_activated_at = time.monotonic()
        import bot.kill_switch as ks_mod
        original_cfg = ks_mod._CFG.copy()
        ks_mod._CFG["l2_cooldown_seconds"] = 3600
        result = await ks._maybe_autoreset_l2()
        ks_mod._CFG.update(original_cfg)
        assert result is False
        assert ks.level() == 2
