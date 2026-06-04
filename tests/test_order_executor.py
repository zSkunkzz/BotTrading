"""Tests para OrderExecutor — exchange siempre mockeado, cero I/O real."""
from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from bot.execution.order_executor import OrderExecutor


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_executor(dry_run: bool = False) -> tuple[OrderExecutor, MagicMock]:
    ex = MagicMock()
    ex.create_order.return_value = {"orderId": "123", "status": "Filled"}
    return OrderExecutor(ex, dry_run=dry_run), ex


# ─────────────────────────────────────────────────────────────────────────────
# open_order — modo live
# ─────────────────────────────────────────────────────────────────────────────

class TestOpenOrderLive:
    def test_market_order_when_no_price(self):
        executor, ex = _make_executor()
        executor.open_order("BTCUSDC", "buy", 0.01)
        ex.create_order.assert_called_once()
        _, order_type, *_ = ex.create_order.call_args.args
        assert order_type == "market"

    def test_limit_order_when_price_given(self):
        executor, ex = _make_executor()
        executor.open_order("BTCUSDC", "buy", 0.01, price=50_000.0)
        _, order_type, *_ = ex.create_order.call_args.args
        assert order_type == "limit"

    def test_returns_exchange_response(self):
        executor, ex = _make_executor()
        result = executor.open_order("BTCUSDC", "buy", 0.01)
        assert result == {"orderId": "123", "status": "Filled"}

    def test_passes_symbol_side_qty(self):
        executor, ex = _make_executor()
        executor.open_order("ETHUSDC", "sell", 1.5)
        args = ex.create_order.call_args.args
        assert args[0] == "ETHUSDC"
        assert args[2] == "sell"
        assert args[3] == 1.5

    def test_kwargs_forwarded_as_params(self):
        executor, ex = _make_executor()
        executor.open_order("BTCUSDC", "buy", 0.01, reduceOnly=True)
        params = ex.create_order.call_args.kwargs["params"]
        assert params.get("reduceOnly") is True


# ─────────────────────────────────────────────────────────────────────────────
# open_order — modo dry-run
# ─────────────────────────────────────────────────────────────────────────────

class TestOpenOrderDryRun:
    def test_returns_dry_sentinel(self):
        executor, ex = _make_executor(dry_run=True)
        result = executor.open_order("BTCUSDC", "buy", 0.01)
        assert result == {"orderId": "dry", "status": "Filled"}

    def test_exchange_never_called(self):
        executor, ex = _make_executor(dry_run=True)
        executor.open_order("BTCUSDC", "buy", 0.01, price=50_000.0)
        ex.create_order.assert_not_called()

    def test_dry_run_with_any_symbol(self):
        executor, ex = _make_executor(dry_run=True)
        result = executor.open_order("SOLUSDC", "sell", 10.0)
        assert result["orderId"] == "dry"
        ex.create_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# place_tpsl — modo live
# ─────────────────────────────────────────────────────────────────────────────

class TestPlaceTpslLive:
    def test_creates_two_orders(self):
        executor, ex = _make_executor()
        executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        assert ex.create_order.call_count == 2

    def test_first_call_is_tp_limit(self):
        executor, ex = _make_executor()
        executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        first_call_args = ex.create_order.call_args_list[0].args
        assert first_call_args[1] == "limit"
        assert first_call_args[4] == 55_000.0

    def test_second_call_is_sl_stop(self):
        executor, ex = _make_executor()
        executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        second_call_args = ex.create_order.call_args_list[1].args
        assert second_call_args[1] == "stop"
        assert second_call_args[4] == 45_000.0

    def test_tp_params_reduce_only(self):
        executor, ex = _make_executor()
        executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        tp_params = ex.create_order.call_args_list[0].kwargs["params"]
        assert tp_params["reduceOnly"] is True
        assert tp_params["tpTrigger"] == 55_000.0

    def test_sl_params_reduce_only(self):
        executor, ex = _make_executor()
        executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        sl_params = ex.create_order.call_args_list[1].kwargs["params"]
        assert sl_params["reduceOnly"] is True
        assert sl_params["slTrigger"] == 45_000.0

    def test_symbol_and_qty_forwarded(self):
        executor, ex = _make_executor()
        executor.place_tpsl("ETHUSDC", "sell", 2.0, tp=4_000.0, sl=3_000.0)
        for c in ex.create_order.call_args_list:
            assert c.args[0] == "ETHUSDC"
            assert c.args[3] == 2.0


# ─────────────────────────────────────────────────────────────────────────────
# place_tpsl — modo dry-run
# ─────────────────────────────────────────────────────────────────────────────

class TestPlaceTpslDryRun:
    def test_no_exchange_calls(self):
        executor, ex = _make_executor(dry_run=True)
        executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        ex.create_order.assert_not_called()

    def test_returns_none(self):
        executor, ex = _make_executor(dry_run=True)
        result = executor.place_tpsl("BTCUSDC", "sell", 0.01, tp=55_000.0, sl=45_000.0)
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# cancel_all
# ─────────────────────────────────────────────────────────────────────────────

class TestCancelAll:
    def test_calls_cancel_all_orders(self):
        executor, ex = _make_executor()
        executor.cancel_all("BTCUSDC")
        ex.cancel_all_orders.assert_called_once_with("BTCUSDC")

    def test_swallows_exception(self):
        executor, ex = _make_executor()
        ex.cancel_all_orders.side_effect = RuntimeError("exchange down")
        # No debe propagar la excepción
        executor.cancel_all("BTCUSDC")

    def test_swallows_connection_error(self):
        executor, ex = _make_executor()
        ex.cancel_all_orders.side_effect = ConnectionError("timeout")
        executor.cancel_all("ETHUSDC")  # no explota

    def test_symbol_forwarded_correctly(self):
        executor, ex = _make_executor()
        executor.cancel_all("SOLUSDC")
        ex.cancel_all_orders.assert_called_once_with("SOLUSDC")
