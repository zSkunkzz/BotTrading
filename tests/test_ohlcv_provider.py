"""Tests para bot/infra/ohlcv_provider.py.

Estrategia:
  - Mockear bot.infra.hl_http.get_candles con unittest.mock.patch.
  - asyncio.sleep mockeado para que los tests no tarden.
  - Fixtures mínimas: listas de dicts de barras o listas vacías.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from bot.infra.ohlcv_provider import OhlcvProvider, _DEFAULT_RETRIES

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_URL = "https://api.example.com"
SYMBOL = "BTCUSDC"
INTERVAL = "15"
LIMIT = 200

FAKE_BARS = [{"open": "1", "close": "2"}, {"open": "3", "close": "4"}]


def _make_provider(
    semaphore: asyncio.Semaphore | None = None,
) -> OhlcvProvider:
    session = MagicMock()
    return OhlcvProvider(session, BASE_URL, semaphore=semaphore)


# ---------------------------------------------------------------------------
# Construcción
# ---------------------------------------------------------------------------


class TestInit:
    def test_default_semaphore_created(self):
        provider = _make_provider()
        assert provider._sem._value == 4  # Semaphore(4) por defecto

    def test_custom_semaphore_used(self):
        sem = asyncio.Semaphore(1)
        provider = _make_provider(semaphore=sem)
        assert provider._sem is sem

    def test_url_stored(self):
        provider = _make_provider()
        assert provider._url == BASE_URL


# ---------------------------------------------------------------------------
# fetch — caso feliz
# ---------------------------------------------------------------------------


class TestFetchSuccess:
    @pytest.mark.asyncio
    async def test_returns_bars_on_first_attempt(self):
        provider = _make_provider()
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", return_value=FAKE_BARS) as mock_gc:
            result = await provider.fetch(SYMBOL, INTERVAL, LIMIT)

        assert result == FAKE_BARS
        mock_gc.assert_called_once_with(
            provider._session, BASE_URL, SYMBOL, INTERVAL, LIMIT
        )

    @pytest.mark.asyncio
    async def test_default_limit_is_200(self):
        provider = _make_provider()
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", return_value=FAKE_BARS) as mock_gc:
            await provider.fetch(SYMBOL, INTERVAL)

        _, _, _, _, limit = mock_gc.call_args.args
        assert limit == 200

    @pytest.mark.asyncio
    async def test_no_sleep_when_first_attempt_succeeds(self):
        provider = _make_provider()
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", return_value=FAKE_BARS), \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await provider.fetch(SYMBOL, INTERVAL)

        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# fetch — reintentos
# ---------------------------------------------------------------------------


class TestFetchRetries:
    @pytest.mark.asyncio
    async def test_retries_on_empty_then_succeeds(self):
        """Primer intento vacío, segundo con datos."""
        provider = _make_provider()
        side_effects = [[], FAKE_BARS]
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", side_effect=side_effects), \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await provider.fetch(SYMBOL, INTERVAL)

        assert result == FAKE_BARS
        mock_sleep.assert_called_once_with(0.5)  # 0.5 * (0+1)

    @pytest.mark.asyncio
    async def test_all_attempts_empty_returns_empty(self):
        """Todos los intentos devuelven lista vacía."""
        provider = _make_provider()
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", return_value=[]), \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock):
            result = await provider.fetch(SYMBOL, INTERVAL)

        assert result == []

    @pytest.mark.asyncio
    async def test_exact_retry_count(self):
        """get_candles se llama exactamente _DEFAULT_RETRIES veces."""
        provider = _make_provider()
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", return_value=[]) as mock_gc, \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock):
            await provider.fetch(SYMBOL, INTERVAL)

        assert mock_gc.call_count == _DEFAULT_RETRIES

    @pytest.mark.asyncio
    async def test_sleep_backoff_increases(self):
        """Sleep se llama con 0.5, 1.0, 1.5 en tres intentos fallidos."""
        provider = _make_provider()
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", return_value=[]), \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await provider.fetch(SYMBOL, INTERVAL)

        assert mock_sleep.call_count == _DEFAULT_RETRIES
        expected_delays = [call(0.5 * (i + 1)) for i in range(_DEFAULT_RETRIES)]
        assert mock_sleep.call_args_list == expected_delays

    @pytest.mark.asyncio
    async def test_succeeds_on_last_attempt(self):
        """Dos intentos vacíos, tercero con datos."""
        provider = _make_provider()
        side_effects = [[], [], FAKE_BARS]
        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", side_effect=side_effects), \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await provider.fetch(SYMBOL, INTERVAL)

        assert result == FAKE_BARS
        assert mock_sleep.call_count == 2  # sleep tras intento 1 y 2


# ---------------------------------------------------------------------------
# Semáforo
# ---------------------------------------------------------------------------


class TestSemaphore:
    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self):
        """Con Semaphore(1) dos fetches concurrentes se ejecutan en serie."""
        sem = asyncio.Semaphore(1)
        provider = _make_provider(semaphore=sem)
        order: list[str] = []

        # hl_http.get_candles is a *sync* function inside an async method.
        # To instrument concurrent ordering we replace it with an AsyncMock
        # whose side_effect appends to `order` and yields via sleep(0) so
        # the event loop can interleave the two gather'd coroutines.
        async def _slow(*_args, **_kwargs):
            order.append("start")
            await asyncio.sleep(0)  # yield — lets the other coroutine try to acquire sem
            order.append("end")
            return FAKE_BARS

        mock_gc = AsyncMock(side_effect=_slow)

        with patch("bot.infra.ohlcv_provider.hl_http.get_candles", mock_gc), \
             patch("bot.infra.ohlcv_provider.asyncio.sleep", new_callable=AsyncMock):
            await asyncio.gather(
                provider.fetch(SYMBOL, INTERVAL),
                provider.fetch(SYMBOL, INTERVAL),
            )

        # With sem=1 the order must be start, end, start, end (never start, start)
        assert order == ["start", "end", "start", "end"]
