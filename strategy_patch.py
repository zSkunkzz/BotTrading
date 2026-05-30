"""
strategy_patch.py
Optional: caching layer for the enricher so multiple signals
for the same symbol within 2 minutes reuse the same context.

Merge this into your Strategy class.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from data_enricher import MarketDataEnricher, EnrichedContext

CACHE_TTL_SECONDS = 120  # 2 minutes


class EnricherCache:
    def __init__(self, enricher: MarketDataEnricher):
        self.enricher = enricher
        self._cache: dict[str, tuple[datetime, EnrichedContext]] = {}

    async def get(self, symbol: str) -> EnrichedContext:
        """Returns cached context if fresh, otherwise fetches a new one."""
        now = datetime.now(timezone.utc)
        if symbol in self._cache:
            cached_at, ctx = self._cache[symbol]
            age = (now - cached_at).total_seconds()
            if age < CACHE_TTL_SECONDS:
                return ctx

        ctx = await self.enricher.fetch_all(symbol)
        self._cache[symbol] = (now, ctx)
        return ctx

    def invalidate(self, symbol: str):
        self._cache.pop(symbol, None)

    def clear(self):
        self._cache.clear()


# ---------------------------------------------------------------------------
# Usage example inside your Strategy / SignalHandler class:
# ---------------------------------------------------------------------------
#
# class Strategy:
#     def __init__(self, ...):
#         self.enricher = MarketDataEnricher(
#             bitget_api_key=..., bitget_api_secret=..., bitget_passphrase=...
#         )
#         self.enricher_cache = EnricherCache(self.enricher)
#
#     async def on_signal(self, signal: dict):
#         ctx = await self.enricher_cache.get(signal["symbol"])
#         decision = await ai_decide_enriched(signal, ctx_override=ctx, ...)
#         ...
