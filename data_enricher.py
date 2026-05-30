"""
data_enricher.py
Fetches external context (Fear & Greed, OI delta, Funding Rate, News)
in parallel without blocking the main trading loop.

News sources (100% free, no API key required):
  - Primary:  https://cryptocurrency.cv/api/news  (REST JSON, no auth, CORS enabled)
  - Fallback: https://cointelegraph.com/rss        (RSS feed, no auth)
"""

import asyncio
import aiohttp
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# --- Config (no API keys needed) ---
BITGET_BASE        = "https://api.bitget.com"
FEAR_GREED_URL     = "https://api.alternative.me/fng/"
NEWS_API_URL       = "https://cryptocurrency.cv/api/news"          # free, no auth
NEWS_SEARCH_URL    = "https://cryptocurrency.cv/api/search"        # free, no auth
CT_RSS_URL         = "https://cointelegraph.com/rss"               # fallback RSS


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
            emoji = "\U0001f631" if self.fear_greed_value < 25 else "\U0001f630" if self.fear_greed_value < 45 else "\U0001f610" if self.fear_greed_value < 55 else "\U0001f60a" if self.fear_greed_value < 75 else "\U0001f911"
            lines.append(f"Fear & Greed: {self.fear_greed_value}/100 ({self.fear_greed_label}) {emoji}")

        if self.oi_delta_4h_pct is not None:
            direction = "\u2191 increasing" if self.oi_delta_4h_pct > 0 else "\u2193 decreasing"
            lines.append(f"Open Interest 4h delta: {self.oi_delta_4h_pct:+.2f}% ({direction})")

        if self.funding_rate is not None:
            side = "longs paying" if self.funding_rate > 0 else "shorts paying"
            lines.append(f"Funding rate: {self.funding_rate:+.4f}% ({side})")

        if self.news:
            lines.append("Recent news:")
            for item in self.news[:5]:
                sentiment_emoji = "\U0001f4c8" if item.get("sentiment") == "positive" else "\U0001f4c9" if item.get("sentiment") == "negative" else "\U0001f4f0"
                lines.append(f"  {sentiment_emoji} [{item.get('sentiment', 'neutral')}] {item.get('title', '')}")

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

        # Convert BTCUSDT -> BTC for news queries
        base_currency = symbol.replace("USDT", "").replace("PERP", "")

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            results = await asyncio.gather(
                self._fetch_fear_greed(session),
                self._fetch_oi_delta(session, symbol),
                self._fetch_funding_rate(session, symbol),
                self._fetch_news(session, base_currency),
                return_exceptions=True
            )

        for name, result in zip(["fear_greed", "oi_delta", "funding_rate", "news"], results):
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

    # ------------------------------------------------------------------
    # Fear & Greed Index  (alternative.me — free, no auth)
    # ------------------------------------------------------------------
    async def _fetch_fear_greed(self, session: aiohttp.ClientSession) -> Optional[dict]:
        async with session.get(FEAR_GREED_URL, params={"limit": 1}) as resp:
            data = await resp.json(content_type=None)
            entry = data["data"][0]
            return {"value": int(entry["value"]), "label": entry["value_classification"]}

    # ------------------------------------------------------------------
    # Open Interest delta 4h  (Bitget API — your existing credentials)
    # ------------------------------------------------------------------
    async def _fetch_oi_delta(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        url = f"{BITGET_BASE}/api/v2/mix/market/open-interest-history"
        params = {"symbol": symbol, "productType": "USDT-FUTURES", "period": "4H", "limit": "2"}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            records = data.get("data", [])
            if len(records) >= 2:
                oi_current = float(records[0]["openInterestValue"])
                oi_prev    = float(records[1]["openInterestValue"])
                if oi_prev != 0:
                    return round((oi_current - oi_prev) / oi_prev * 100, 2)
        return None

    # ------------------------------------------------------------------
    # Funding Rate  (Bitget API — your existing credentials)
    # ------------------------------------------------------------------
    async def _fetch_funding_rate(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        url = f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate"
        params = {"symbol": symbol, "productType": "USDT-FUTURES"}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            rate_str = data.get("data", [{}])[0].get("fundingRate", None)
            if rate_str is not None:
                return round(float(rate_str) * 100, 4)
        return None

    # ------------------------------------------------------------------
    # News  — PRIMARY: cryptocurrency.cv  |  FALLBACK: CoinTelegraph RSS
    # Both are 100% free and require zero API keys.
    # ------------------------------------------------------------------
    async def _fetch_news(self, session: aiohttp.ClientSession, currency: str) -> list:
        # Try primary source first
        try:
            return await self._news_from_cryptocurrency_cv(session, currency)
        except Exception as e:
            logger.warning(f"[Enricher] cryptocurrency.cv failed ({e}), falling back to RSS")

        # Fallback: CoinTelegraph RSS (general crypto, not symbol-specific)
        try:
            return await self._news_from_rss(session)
        except Exception as e:
            logger.warning(f"[Enricher] CoinTelegraph RSS failed ({e})")
            return []

    async def _news_from_cryptocurrency_cv(self, session: aiohttp.ClientSession, currency: str) -> list:
        """
        cryptocurrency.cv free REST API.
        Docs: https://news-crypto.vercel.app/km/developers
        Tries the /search endpoint first (symbol-specific), falls back to /news (general).
        """
        # Symbol-specific search
        async with session.get(NEWS_SEARCH_URL, params={"q": currency, "limit": 5}) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                articles = data if isinstance(data, list) else data.get("articles", data.get("results", []))
                if articles:
                    return self._parse_cv_articles(articles[:5])

        # General latest news
        async with session.get(NEWS_API_URL, params={"limit": 5}) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            articles = data if isinstance(data, list) else data.get("articles", data.get("results", []))
            return self._parse_cv_articles(articles[:5])

    @staticmethod
    def _parse_cv_articles(articles: list) -> list:
        items = []
        for a in articles:
            title = a.get("title") or a.get("name") or ""
            # API may return sentiment directly, or we leave it neutral
            sentiment = a.get("sentiment") or a.get("type") or "neutral"
            # Normalise to positive / negative / neutral
            if sentiment not in ("positive", "negative"):
                sentiment = "neutral"
            items.append({"title": title, "sentiment": sentiment})
        return items

    async def _news_from_rss(self, session: aiohttp.ClientSession) -> list:
        """Parse CoinTelegraph public RSS feed. No auth, no key."""
        async with session.get(CT_RSS_URL) as resp:
            resp.raise_for_status()
            text = await resp.text()

        root = ET.fromstring(text)
        channel = root.find("channel")
        items = []
        for item in (channel.findall("item") if channel is not None else [])[:5]:
            title_el = item.find("title")
            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            items.append({"title": title, "sentiment": "neutral"})
        return items
