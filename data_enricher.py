"""
data_enricher.py
Fetches external context (Fear & Greed, OI delta, Funding Rate, News)
in parallel without blocking the main trading loop.
"""

import asyncio
import aiohttp
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# --- Config ---
CRYPTOPANIC_API_KEY = "YOUR_API_KEY_HERE"  # https://cryptopanic.com/developers/api
BITGET_BASE = "https://api.bitget.com"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
CRYPTOPANIC_URL = "https://cryptopanic.com/api/v1/posts/"


@dataclass
class EnrichedContext:
    symbol: str
    fear_greed_value: Optional[int] = None
    fear_greed_label: Optional[str] = None
    oi_delta_4h_pct: Optional[float] = None
    funding_rate: Optional[float] = None
    news: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    fetched_at: Optional[datetime] = None

    def to_prompt_block(self) -> str:
        """Returns a formatted string ready to inject into the AI prompt."""
        lines = ["\n--- External Context ---"]

        if self.fear_greed_value is not None:
            emoji = "😱" if self.fear_greed_value < 25 else "😰" if self.fear_greed_value < 45 else "😐" if self.fear_greed_value < 55 else "😊" if self.fear_greed_value < 75 else "🤑"
            lines.append(f"Fear & Greed: {self.fear_greed_value}/100 ({self.fear_greed_label}) {emoji}")

        if self.oi_delta_4h_pct is not None:
            direction = "↑ increasing" if self.oi_delta_4h_pct > 0 else "↓ decreasing"
            lines.append(f"Open Interest 4h delta: {self.oi_delta_4h_pct:+.2f}% ({direction})")

        if self.funding_rate is not None:
            side = "longs paying" if self.funding_rate > 0 else "shorts paying"
            lines.append(f"Funding rate: {self.funding_rate:+.4f}% ({side})")

        if self.news:
            lines.append("Recent news:")
            for item in self.news[:5]:
                sentiment_emoji = "📈" if item.get("kind") == "positive" else "📉" if item.get("kind") == "negative" else "📰"
                lines.append(f"  {sentiment_emoji} [{item.get('kind', 'neutral')}] {item.get('title', '')}")

        if self.errors:
            lines.append(f"[Enricher errors: {', '.join(self.errors)}]")

        lines.append("--- End External Context ---\n")
        return "\n".join(lines)


class MarketDataEnricher:
    def __init__(self, bitget_api_key: str = "", bitget_api_secret: str = "", bitget_passphrase: str = ""):
        self.bitget_api_key = bitget_api_key
        self.bitget_api_secret = bitget_api_secret
        self.bitget_passphrase = bitget_passphrase

    async def fetch_all(self, symbol: str) -> EnrichedContext:
        """Fetch all external data in parallel. Never raises."""
        ctx = EnrichedContext(symbol=symbol, fetched_at=datetime.now(timezone.utc))

        # Convert BTCUSDT → BTC for news queries
        base_currency = symbol.replace("USDT", "").replace("PERP", "")

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            results = await asyncio.gather(
                self._fetch_fear_greed(session),
                self._fetch_oi_delta(session, symbol),
                self._fetch_funding_rate(session, symbol),
                self._fetch_news(session, base_currency),
                return_exceptions=True
            )

        handlers = [
            ("fear_greed", results[0]),
            ("oi_delta", results[1]),
            ("funding_rate", results[2]),
            ("news", results[3]),
        ]

        for name, result in handlers:
            if isinstance(result, Exception):
                logger.warning(f"[Enricher] {name} failed: {result}")
                ctx.errors.append(name)
            elif result is not None:
                if name == "fear_greed" and isinstance(result, dict):
                    ctx.fear_greed_value = result.get("value")
                    ctx.fear_greed_label = result.get("label")
                elif name == "oi_delta":
                    ctx.oi_delta_4h_pct = result
                elif name == "funding_rate":
                    ctx.funding_rate = result
                elif name == "news":
                    ctx.news = result

        return ctx

    async def _fetch_fear_greed(self, session: aiohttp.ClientSession) -> Optional[dict]:
        async with session.get(FEAR_GREED_URL, params={"limit": 1}) as resp:
            data = await resp.json(content_type=None)
            entry = data["data"][0]
            return {"value": int(entry["value"]), "label": entry["value_classification"]}

    async def _fetch_oi_delta(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """Calculates OI delta between latest and previous 4h candle."""
        url = f"{BITGET_BASE}/api/v2/mix/market/open-interest-history"
        params = {"symbol": symbol, "productType": "USDT-FUTURES", "period": "4H", "limit": "2"}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            records = data.get("data", [])
            if len(records) >= 2:
                oi_current = float(records[0]["openInterestValue"])
                oi_prev = float(records[1]["openInterestValue"])
                if oi_prev != 0:
                    return round((oi_current - oi_prev) / oi_prev * 100, 2)
        return None

    async def _fetch_funding_rate(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        url = f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate"
        params = {"symbol": symbol, "productType": "USDT-FUTURES"}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            rate_str = data.get("data", [{}])[0].get("fundingRate", None)
            if rate_str is not None:
                return round(float(rate_str) * 100, 4)
        return None

    async def _fetch_news(self, session: aiohttp.ClientSession, currency: str) -> list:
        if not CRYPTOPANIC_API_KEY or CRYPTOPANIC_API_KEY == "YOUR_API_KEY_HERE":
            return []
        params = {
            "auth_token": CRYPTOPANIC_API_KEY,
            "currencies": currency,
            "filter": "hot",
            "public": "true",
            "kind": "news",
        }
        async with session.get(CRYPTOPANIC_URL, params=params) as resp:
            data = await resp.json()
            results = data.get("results", [])[:5]
            news_items = []
            for item in results:
                votes = item.get("votes", {})
                # Determine sentiment from votes
                positive = votes.get("positive", 0)
                negative = votes.get("negative", 0)
                kind = "positive" if positive > negative else "negative" if negative > positive else "neutral"
                news_items.append({"title": item.get("title", ""), "kind": kind})
            return news_items
