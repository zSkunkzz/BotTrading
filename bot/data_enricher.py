"""
data_enricher.py — Fetches external market context for AI enrichment.

Sources (all free, no API key required):
  - Fear & Greed Index  : api.alternative.me
  - Open Interest delta : Bitget REST API (existing credentials)
  - Funding rate        : Bitget REST API (existing credentials)
  - News sentiment      : RSS feeds (Messari + CoinDesk + Cointelegraph)

All fetches run concurrently. Any individual failure is caught and logged;
the rest of the context is still returned.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
import feedparser  # type: ignore

logger = logging.getLogger(__name__)

# ── RSS feeds (Opción 2 — Messari + CoinDesk + Cointelegraph) ────────────────
RSS_FEEDS = [
    "https://messari.io/rss/all-news",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
]

BEARISH_KEYWORDS = {
    "crash", "ban", "lawsuit", "hack", "exploit", "fine",
    "bankrupt", "lost", "seized", "fraud", "investigation",
    "plunge", "dump", "sell-off", "collapse", "fear", "panic",
}
BULLISH_KEYWORDS = {
    "etf", "approved", "adoption", "inflow", "record",
    "rally", "bullish", "upgrade", "partnership", "launch",
    "surge", "ath", "breakout", "accumulation", "institutional",
}

# ── Bitget REST base ─────────────────────────────────────────────────────────
BITGET_BASE = "https://api.bitget.com"
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"


# ── Data containers ──────────────────────────────────────────────────────────

@dataclass
class FearGreedData:
    value: int = 50
    label: str = "Neutral"
    timestamp: str = ""


@dataclass
class OIData:
    oi_usd: float = 0.0
    oi_delta_pct: float = 0.0   # % change vs ~4 candles ago
    funding_rate: float = 0.0   # current funding rate as %


@dataclass
class NewsItem:
    title: str
    sentiment: str   # bullish | bearish | neutral
    source: str
    published: str


@dataclass
class EnrichedContext:
    fear_greed: FearGreedData = field(default_factory=FearGreedData)
    oi: OIData = field(default_factory=OIData)
    news: list = field(default_factory=list)   # list[NewsItem]
    fetched_at: str = ""
    errors: list = field(default_factory=list)


# ── Individual fetchers ───────────────────────────────────────────────────────

async def _fetch_fear_greed(session: aiohttp.ClientSession) -> FearGreedData:
    try:
        async with session.get(
            FEAR_GREED_URL, timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            data = await r.json(content_type=None)
            item = data["data"][0]
            return FearGreedData(
                value=int(item["value"]),
                label=item["value_classification"],
                timestamp=item.get("timestamp", ""),
            )
    except Exception as exc:
        logger.warning("[enricher] fear_greed: %s", exc)
        return FearGreedData()


async def _fetch_oi(session: aiohttp.ClientSession, symbol: str) -> OIData:
    """
    Fetches current OI and computes delta using Bitget history endpoint.
    No auth required for public market data.
    """
    params = {"symbol": symbol, "productType": "USDT-FUTURES"}
    try:
        # Current OI
        async with session.get(
            f"{BITGET_BASE}/api/v2/mix/market/open-interest",
            params=params,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            oi_resp = await r.json()
        oi_list = oi_resp.get("data", {}).get("openInterestList", [])
        current_oi = float(oi_list[0]["size"]) if oi_list else 0.0

        # Historical OI (4H granularity, last 2 candles → delta)
        async with session.get(
            f"{BITGET_BASE}/api/v2/mix/market/history-open-interest",
            params={**params, "period": "4H", "limit": "2"},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            hist_resp = await r.json()
        hist = hist_resp.get("data", [])
        if len(hist) >= 2:
            prev_oi = float(hist[1].get("openInterestList", [{}])[0].get("size", current_oi))
            delta_pct = ((current_oi - prev_oi) / prev_oi * 100) if prev_oi else 0.0
        else:
            delta_pct = 0.0

        # Funding rate
        async with session.get(
            f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
            params=params,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            fr_resp = await r.json()
        fr_list = fr_resp.get("data", [])
        funding_rate = float(fr_list[0]["fundingRate"]) * 100 if fr_list else 0.0

        return OIData(
            oi_usd=current_oi,
            oi_delta_pct=round(delta_pct, 2),
            funding_rate=round(funding_rate, 4),
        )

    except Exception as exc:
        logger.warning("[enricher] OI/funding: %s", exc)
        return OIData()


async def _fetch_news(
    session: aiohttp.ClientSession, base_currency: str
) -> list:
    """
    Parses public RSS feeds, filters by currency ticker,
    and classifies sentiment via keyword matching.
    No API key required.
    """
    results: list = []
    currency_upper = base_currency.upper()

    for feed_url in RSS_FEEDS:
        if len(results) >= 5:
            break
        try:
            async with session.get(
                feed_url, timeout=aiohttp.ClientTimeout(total=7)
            ) as r:
                text = await r.text()

            feed = feedparser.parse(text)
            source_name = feed_url.split("/")[2].replace("www.", "")

            for entry in feed.entries[:20]:
                if len(results) >= 5:
                    break
                title: str = entry.get("title", "")
                # Only include entries that mention the traded asset
                if currency_upper not in title.upper() and currency_upper not in entry.get("summary", "").upper():
                    continue

                title_lower = title.lower()
                if any(kw in title_lower for kw in BEARISH_KEYWORDS):
                    sentiment = "bearish"
                elif any(kw in title_lower for kw in BULLISH_KEYWORDS):
                    sentiment = "bullish"
                else:
                    sentiment = "neutral"

                results.append(NewsItem(
                    title=title[:120],
                    sentiment=sentiment,
                    source=source_name,
                    published=entry.get("published", ""),
                ))

        except Exception as exc:
            logger.warning("[enricher] RSS %s: %s", feed_url, exc)

    return results


# ── Public API ────────────────────────────────────────────────────────────────

async def fetch_enriched_context(symbol: str) -> EnrichedContext:
    """
    Main entry point. Fetches all external data concurrently.

    Args:
        symbol: trading symbol, e.g. "BTCUSDT"

    Returns:
        EnrichedContext — always returns, never raises.
        Partial results are available even if some sources fail.
    """
    base_currency = re.sub(r"(USDT|PERP|USD|BUSD)$", "", symbol.upper())
    ctx = EnrichedContext(fetched_at=datetime.now(timezone.utc).isoformat())

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            _fetch_fear_greed(session),
            _fetch_oi(session, symbol),
            _fetch_news(session, base_currency),
            return_exceptions=True,
        )

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            ctx.errors.append(f"source_{i}: {result}")
        elif i == 0:
            ctx.fear_greed = result
        elif i == 1:
            ctx.oi = result
        elif i == 2:
            ctx.news = result

    return ctx


def format_context_for_prompt(ctx: EnrichedContext) -> str:
    """
    Serialises an EnrichedContext into a compact string
    ready to be injected into the AI system prompt.
    """
    lines: list = []

    # Fear & Greed
    fg = ctx.fear_greed
    if fg.value < 25:
        fg_emoji = "\U0001f631"   # 😱
    elif fg.value < 45:
        fg_emoji = "\U0001f628"   # 😨
    elif fg.value < 55:
        fg_emoji = "\U0001f610"   # 😐
    elif fg.value < 75:
        fg_emoji = "\U0001f60a"   # 😊
    else:
        fg_emoji = "\U0001f911"   # 🤑
    lines.append(f"Fear & Greed: {fg.value}/100 ({fg.label}) {fg_emoji}")

    # Open Interest
    oi = ctx.oi
    if oi.oi_delta_pct > 1:
        oi_trend = "\u2191 increasing"
    elif oi.oi_delta_pct < -1:
        oi_trend = "\u2193 decreasing"
    else:
        oi_trend = "\u2192 stable"
    lines.append(f"OI 4h delta: {oi.oi_delta_pct:+.2f}% ({oi_trend})")

    # Funding rate
    paying = "longs paying" if oi.funding_rate > 0 else "shorts paying"
    lines.append(f"Funding rate: {oi.funding_rate:+.4f}% ({paying})")

    # News
    if ctx.news:
        lines.append("Recent news:")
        for item in ctx.news:
            icon = "\U0001f4c8" if item.sentiment == "bullish" else "\U0001f4c9" if item.sentiment == "bearish" else "\U0001f4f0"
            lines.append(f"  {icon} [{item.sentiment}] {item.title}")
    else:
        lines.append("Recent news: unavailable")

    if ctx.errors:
        lines.append(f"[enricher errors: {', '.join(ctx.errors)}]")

    return "\n".join(lines)
