"""Tests para PositionSync — exchange siempre mockeado, cero I/O real."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from bot.execution.position_sync import PositionSync


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_pos(symbol: str, contracts: float) -> dict:
    return {"symbol": symbol, "contracts": contracts, "side": "long"}


def _make_sync(positions_by_symbol: list[dict] | None = None,
               all_positions: list[dict] | None = None) -> PositionSync:
    """Construye un PositionSync con exchange mockeado."""
    ex = MagicMock()

    if positions_by_symbol is not None:
        ex.fetch_positions.side_effect = lambda *args, **kwargs: (
            positions_by_symbol if args else (all_positions or [])
        )
    else:
        ex.fetch_positions.return_value = all_positions or []

    return PositionSync(ex)


# ─────────────────────────────────────────────────────────────────────────────
# get_open
# ─────────────────────────────────────────────────────────────────────────────

class TestGetOpen:
    def test_returns_position_with_nonzero_contracts(self):
        pos = _make_pos("BTCUSDC", 1.5)
        sync = _make_sync(positions_by_symbol=[pos])
        result = sync.get_open("BTCUSDC")
        assert result == pos

    def test_returns_none_when_contracts_zero(self):
        pos = _make_pos("BTCUSDC", 0.0)
        sync = _make_sync(positions_by_symbol=[pos])
        assert sync.get_open("BTCUSDC") is None

    def test_returns_none_when_list_empty(self):
        sync = _make_sync(positions_by_symbol=[])
        assert sync.get_open("BTCUSDC") is None

    def test_skips_zero_and_returns_nonzero(self):
        positions = [
            _make_pos("BTCUSDC", 0.0),
            _make_pos("BTCUSDC", 2.0),
        ]
        sync = _make_sync(positions_by_symbol=positions)
        result = sync.get_open("BTCUSDC")
        assert result["contracts"] == 2.0

    def test_returns_none_on_exchange_exception(self):
        ex = MagicMock()
        ex.fetch_positions.side_effect = RuntimeError("network error")
        sync = PositionSync(ex)
        assert sync.get_open("BTCUSDC") is None

    def test_contracts_as_string_zero(self):
        """contracts puede llegar como string desde algunos exchanges."""
        pos = {"symbol": "BTCUSDC", "contracts": "0", "side": "long"}
        sync = _make_sync(positions_by_symbol=[pos])
        assert sync.get_open("BTCUSDC") is None

    def test_contracts_as_string_nonzero(self):
        pos = {"symbol": "BTCUSDC", "contracts": "3.5", "side": "long"}
        sync = _make_sync(positions_by_symbol=[pos])
        assert sync.get_open("BTCUSDC") is not None

    def test_missing_contracts_key_treated_as_zero(self):
        pos = {"symbol": "BTCUSDC", "side": "long"}  # sin 'contracts'
        sync = _make_sync(positions_by_symbol=[pos])
        assert sync.get_open("BTCUSDC") is None


# ─────────────────────────────────────────────────────────────────────────────
# get_all_open
# ─────────────────────────────────────────────────────────────────────────────

class TestGetAllOpen:
    def test_returns_only_nonzero_positions(self):
        positions = [
            _make_pos("BTCUSDC", 1.0),
            _make_pos("ETHUSDC", 0.0),
            _make_pos("SOLUSDC", 0.5),
        ]
        ex = MagicMock()
        ex.fetch_positions.return_value = positions
        sync = PositionSync(ex)
        result = sync.get_all_open()
        symbols = [p["symbol"] for p in result]
        assert "BTCUSDC" in symbols
        assert "SOLUSDC" in symbols
        assert "ETHUSDC" not in symbols

    def test_returns_empty_list_when_all_zero(self):
        positions = [_make_pos("BTCUSDC", 0.0), _make_pos("ETHUSDC", 0.0)]
        ex = MagicMock()
        ex.fetch_positions.return_value = positions
        sync = PositionSync(ex)
        assert sync.get_all_open() == []

    def test_returns_empty_list_on_exchange_exception(self):
        ex = MagicMock()
        ex.fetch_positions.side_effect = ConnectionError("timeout")
        sync = PositionSync(ex)
        assert sync.get_all_open() == []

    def test_returns_empty_list_when_no_positions(self):
        ex = MagicMock()
        ex.fetch_positions.return_value = []
        sync = PositionSync(ex)
        assert sync.get_all_open() == []

    def test_count_matches_nonzero_positions(self):
        positions = [_make_pos(f"SYM{i}USDC", float(i)) for i in range(5)]
        # i=0 → contracts=0, i=1..4 → nonzero → 4 posiciones abiertas
        ex = MagicMock()
        ex.fetch_positions.return_value = positions
        sync = PositionSync(ex)
        assert len(sync.get_all_open()) == 4
