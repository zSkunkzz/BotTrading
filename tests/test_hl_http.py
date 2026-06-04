"""Tests para bot/infra/hl_http.py.

Estrategia: mockear httpx.Client.get con unittest.mock.patch para no
realizar peticiones reales. Fixtures mínimas al estilo del proyecto.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from bot.infra.hl_http import get_candles, get_ticker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

URL = "https://api.example.com"
SYMBOL = "BTCUSDC"


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Devuelve un MagicMock que simula un httpx.Response exitoso."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None  # no lanza nada
    return resp


def _error_response() -> MagicMock:
    """Simula un httpx.Response que lanza HTTPStatusError al llamar raise_for_status."""
    resp = MagicMock()
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )
    return resp


# ---------------------------------------------------------------------------
# get_candles
# ---------------------------------------------------------------------------


class TestGetCandles:
    def test_returns_list_on_success(self):
        bars = [{"open": "1", "close": "2"}, {"open": "3", "close": "4"}]
        resp = _mock_response({"result": {"list": bars}})

        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        result = get_candles(session, URL, SYMBOL, "15", 10)

        assert result == bars
        session.get.assert_called_once_with(
            URL,
            params={"symbol": SYMBOL, "interval": "15", "limit": 10},
            timeout=10,
        )

    def test_returns_empty_list_when_result_missing(self):
        resp = _mock_response({"retCode": 0})  # sin clave "result"
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        result = get_candles(session, URL, SYMBOL, "1", 5)

        assert result == []

    def test_returns_empty_list_when_list_missing(self):
        resp = _mock_response({"result": {"category": "linear"}})  # sin "list"
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        result = get_candles(session, URL, SYMBOL, "5", 20)

        assert result == []

    def test_returns_empty_list_on_http_error(self):
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = _error_response()

        result = get_candles(session, URL, SYMBOL, "60", 100)

        assert result == []

    def test_returns_empty_list_on_network_exception(self):
        session = MagicMock(spec=httpx.Client)
        session.get.side_effect = httpx.ConnectError("timeout")

        result = get_candles(session, URL, SYMBOL, "D", 200)

        assert result == []

    def test_passes_correct_params(self):
        resp = _mock_response({"result": {"list": []}})
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        get_candles(session, URL, "ETHUSDT", "240", 50)

        _, kwargs = session.get.call_args
        assert kwargs["params"]["symbol"] == "ETHUSDT"
        assert kwargs["params"]["interval"] == "240"
        assert kwargs["params"]["limit"] == 50
        assert kwargs["timeout"] == 10


# ---------------------------------------------------------------------------
# get_ticker
# ---------------------------------------------------------------------------


class TestGetTicker:
    def test_returns_dict_on_success(self):
        payload = {"symbol": SYMBOL, "lastPrice": "30000"}
        resp = _mock_response({"result": payload})
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        result = get_ticker(session, URL, SYMBOL)

        assert result == payload
        session.get.assert_called_once_with(
            URL,
            params={"symbol": SYMBOL},
            timeout=5,
        )

    def test_returns_empty_dict_when_result_missing(self):
        resp = _mock_response({"retCode": 0})
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        result = get_ticker(session, URL, SYMBOL)

        assert result == {}

    def test_returns_empty_dict_on_http_error(self):
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = _error_response()

        result = get_ticker(session, URL, SYMBOL)

        assert result == {}

    def test_returns_empty_dict_on_network_exception(self):
        session = MagicMock(spec=httpx.Client)
        session.get.side_effect = httpx.ConnectError("refused")

        result = get_ticker(session, URL, SYMBOL)

        assert result == {}

    def test_passes_correct_timeout(self):
        resp = _mock_response({"result": {}})
        session = MagicMock(spec=httpx.Client)
        session.get.return_value = resp

        get_ticker(session, URL, "SOLUSDT")

        _, kwargs = session.get.call_args
        assert kwargs["timeout"] == 5
        assert kwargs["params"]["symbol"] == "SOLUSDT"
