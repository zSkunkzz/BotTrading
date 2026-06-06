"""
data_enricher.py — Fetches external market context for AI enrichment.

Sources (all free, no API key required):
  - Fear & Greed Index  : api.alternative.me
  - Open Interest       : OKX REST /api/v5/public/open-interest  (primario)
                          Hyperliquid REST /info                  (fallback)
  - Funding rate        : OKX REST /api/v5/public/funding-rate   (primario)
                          Hyperliquid REST /info                  (fallback)
  - News sentiment      : RSS feeds (Messari + CoinDesk + Cointelegraph)

All fetches run concurrently. Any individual failure is caught and logged;
the rest of the context is still returned.

v2 — OKX como fuente primaria de funding+OI.
  Para coins que solo cotizan en OKX (altcoins), Hyperliquid devolvía 0.0
  porque no los tiene en su universo. Ahora OKX es la fuente primaria y
  Hyperliquid actúa como fallback.
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp

try:
    import feedparser  # type: ignore
    _FEEDPARSER_OK = True
except ImportError:
    feedparser = None  # type: ignore
    _FEEDPARSER_OK = False

logger = logging.getLogger(__name__)

# ── RSS feeds ─────────────────────────────────────────────────────────────────────────────────
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

# ── API endpoints ───────────────────────────────────────────────────────────────────────────
HL_API_URL       = "https://api.hyperliquid.xyz/info"
FEAR_GREED_URL   = "https://api.alternative.me/fng/?limit=1"
OKX_BASE_URL     = "https://www.okx.com"


def _norm_coin(symbol: str) -> str:
    """BTCUSDT / BTC/USDT:USDT → BTC"""
    s = symbol.replace("/", "").replace(":USDT", "").upper()
    if s.endswith("USDTUSDT"):
        s = s[:-4]
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def _norm_inst_id(symbol: str) -> str:
    """BTCUSDT / BTC / BTC-USDT-SWAP → BTC-USDT-SWAP (formato instId de OKX)."""
    coin = _norm_coin(symbol)
    return f"{coin}-USDT-SWAP"


# ── Data containers ───────────────────────────────────────────────────────────────────────────

@dataclass
class FearGreedData:
    value: int  = 50
    label: str  = "Neutral"
    timestamp: str = ""


@dataclass
class OIData:
    oi_usd: float       = 0.0
    oi_delta_pct: float = 0.0   # % change vs previous snapshot (estimated)
    funding_rate: float = 0.0   # current funding rate as % per 8h
    source: str         = ""    # "okx" | "hyperliquid" | ""
    liq_usd_1h: float   = 0.0   # USD liquidados (long+short) en la ultima hora


@dataclass
class NewsItem:
    title: str
    sentiment: str   # bullish | bearish | neutral
    source: str
    published: str


@dataclass
class EnrichedContext:
    fear_greed: FearGreedData = field(default_factory=FearGreedData)
    oi: OIData                = field(default_factory=OIData)
    news: list                = field(default_factory=list)   # list[NewsItem]
    fetched_at: str           = ""
    errors: list              = field(default_factory=list)


# ── Individual fetchers ─────────────────────────────────────────────────────────────────────

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


async def _fetch_oi_okx(session: aiohttp.ClientSession, symbol: str) -> "OIData | None":
    """
    Fuente primaria: OKX REST publica (no requiere API key).

    Endpoints:
      GET /api/v5/public/funding-rate?instId={instId}
        fundingRate   — tasa de funding actual (decimal por 8h, ej: 0.0001)
        nextFundingRate — tasa estimada del siguiente funding
      GET /api/v5/public/open-interest?instType=SWAP&instId={instId}
        oi          — OI en contratos
        oiCcy       — OI en coin
        oiUsd       — OI en USD (si disponible)

    Devuelve None si el instrumento no existe en OKX o hay error.
    """
    inst_id = _norm_inst_id(symbol)
    try:
        funding_resp, oi_resp = await asyncio.gather(
            session.get(
                f"{OKX_BASE_URL}/api/v5/public/funding-rate",
                params={"instId": inst_id},
                timeout=aiohttp.ClientTimeout(total=8),
            ),
            session.get(
                f"{OKX_BASE_URL}/api/v5/public/open-interest",
                params={"instType": "SWAP", "instId": inst_id},
                timeout=aiohttp.ClientTimeout(total=8),
            ),
        )

        import json as _json
        funding_body = _json.loads(await funding_resp.text())
        oi_body      = _json.loads(await oi_resp.text())

        # Funding
        if funding_body.get("code") != "0" or not funding_body.get("data"):
            logger.debug("[enricher] OKX funding no disponible para %s: %s",
                         inst_id, funding_body.get("msg"))
            return None

        fd = funding_body["data"][0]
        funding_rate_raw = float(fd.get("fundingRate", 0) or 0)
        # OKX devuelve la tasa por periodo de 8h en decimal (ej: 0.0001 = 0.01%)
        funding_rate_pct = funding_rate_raw * 100  # % por 8h

        # OI
        oi_usd = 0.0
        if oi_body.get("code") == "0" and oi_body.get("data"):
            od = oi_body["data"][0]
            oi_usd_raw = od.get("oiUsd") or od.get("oiCcy") or "0"
            try:
                oi_usd = float(oi_usd_raw)
            except (ValueError, TypeError):
                oi_usd = 0.0

        logger.debug(
            "[enricher] OKX funding=%+.4f%% OI=$%.0f para %s",
            funding_rate_pct, oi_usd, inst_id,
        )
        return OIData(
            oi_usd=round(oi_usd, 0),
            oi_delta_pct=0.0,   # OKX público no expone delta histórico de OI
            funding_rate=round(funding_rate_pct, 4),
            source="okx",
        )

    except Exception as exc:
        logger.warning("[enricher] OKX OI/funding error para %s: %s", symbol, exc)
        return None


async def _fetch_oi_hyperliquid(session: aiohttp.ClientSession, symbol: str) -> OIData:
    """
    Fuente fallback: Hyperliquid REST publica.

    Hyperliquid metaAndAssetCtxs response per asset:
      openInterest  — OI en coin units
      funding       — hourly funding rate (decimal, e.g. 0.0001)
      markPx        — mark price
      prevDayPx     — previous day price (proxy para OI delta)
    """
    coin = _norm_coin(symbol)
    try:
        async with session.post(
            HL_API_URL,
            json={"type": "metaAndAssetCtxs"},
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            raw_text = await r.text()
            try:
                data = __import__("json").loads(raw_text)
            except Exception:
                logger.warning("[enricher] HL metaAndAssetCtxs: JSON invalido raw=%s",
                               raw_text[:200])
                return OIData()

        if not isinstance(data, list) or len(data) < 2:
            logger.warning("[enricher] HL metaAndAssetCtxs: estructura inesperada")
            return OIData()

        universe   = data[0].get("universe", [])
        asset_ctxs = data[1]

        coin_idx = None
        for i, u in enumerate(universe):
            if u.get("name", "").upper() == coin:
                coin_idx = i
                break

        if coin_idx is None or coin_idx >= len(asset_ctxs):
            logger.debug("[enricher] HL: coin %s no encontrado en universo", coin)
            return OIData()

        ctx       = asset_ctxs[coin_idx]
        oi_coins  = float(ctx.get("openInterest", 0) or 0)
        mark_px   = float(ctx.get("markPx", 0) or 0)
        prev_px   = float(ctx.get("prevDayPx", 0) or 0)
        funding_h = float(ctx.get("funding", 0) or 0)

        oi_usd = oi_coins * mark_px

        delta_pct = 0.0
        if prev_px > 0 and mark_px > 0:
            delta_pct = (mark_px - prev_px) / prev_px * 100

        funding_rate_pct = funding_h * 8 * 100  # % per 8h

        return OIData(
            oi_usd=round(oi_usd, 0),
            oi_delta_pct=round(delta_pct, 2),
            funding_rate=round(funding_rate_pct, 4),
            source="hyperliquid",
        )

    except Exception as exc:
        logger.warning("[enricher] HL OI/funding: %s", exc)
        return OIData()


async def _fetch_oi(session: aiohttp.ClientSession, symbol: str) -> OIData:
    """
    Intenta OKX primero (fuente primaria, cubre todos los pares del bot).
    Si falla o el instId no existe en OKX, cae a Hyperliquid como fallback.
    """
    okx_result = await _fetch_oi_okx(session, symbol)
    if okx_result is not None:
        return okx_result

    logger.debug("[enricher] OKX fallo para %s — usando Hyperliquid como fallback", symbol)
    return await _fetch_oi_hyperliquid(session, symbol)


async def _fetch_liq_okx(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    Descarga liquidaciones recientes de OKX (ultima 1h) para el simbolo.
    Endpoint: GET /api/v5/public/liquidation-orders
      instType=SWAP, instId=<coin>-USDT-SWAP, state=filled, limit=100

    Devuelve el volumen total liquidado en USD (long+short) de la ultima hora.
    Devuelve 0.0 si falla o no hay datos.
    """
    import time as _time
    import json as _json
    inst_id = _norm_inst_id(symbol)
    since_ms = int((_time.time() - 3600) * 1000)
    total_usd = 0.0
    try:
        async with session.get(
            f"{OKX_BASE_URL}/api/v5/public/liquidation-orders",
            params={"instType": "SWAP", "instId": inst_id, "state": "filled", "limit": "100"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            body = _json.loads(await r.text())
        if body.get("code") != "0" or not body.get("data"):
            logger.debug("[enricher] OKX liquidations no data para %s: %s", inst_id, body.get("msg"))
            return 0.0
        for item in body["data"]:
            for detail in item.get("details", []):
                ts = int(detail.get("ts", 0) or 0)
                if ts < since_ms:
                    continue
                sz  = float(detail.get("sz", 0) or 0)
                bkx = float(detail.get("bkPx", 0) or 0)
                total_usd += sz * bkx
        logger.debug("[enricher] OKX liq_usd_1h=%.0f para %s", total_usd, inst_id)
    except Exception as exc:
        logger.warning("[enricher] OKX liquidations error para %s: %s", symbol, exc)
    return round(total_usd, 2)


async def _fetch_news(
    session: aiohttp.ClientSession, base_currency: str
) -> list:
    """
    Parses public RSS feeds, filters by currency ticker,
    and classifies sentiment via keyword matching.
    Requires feedparser; returns [] gracefully if not installed.
    """
    if not _FEEDPARSER_OK:
        logger.debug("[enricher] feedparser not installed, skipping news")
        return []

    results: list = []
    currency_upper = base_currency.upper()

    for feed_url in RSS_FEEDS:
        if len(results) >= 5:
            break
        try:
            async with session.get(
                feed_url, timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                raw_bytes = await r.read()   # feedparser handles encoding internally

            feed = feedparser.parse(raw_bytes)
            source_name = feed_url.split("/")[2].replace("www.", "")

            for entry in feed.entries[:20]:
                if len(results) >= 5:
                    break
                title: str = entry.get("title", "")
                summary: str = entry.get("summary", "")
                if (
                    currency_upper not in title.upper()
                    and currency_upper not in summary.upper()
                ):
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


# ── Public API ────────────────────────────────────────────────────────────────────────────────

async def fetch_enriched_context(symbol: str) -> EnrichedContext:
    """
    Main entry point. Fetches all external data concurrently.

    Args:
        symbol: trading symbol, e.g. "BTCUSDT" or "BTC"

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
            _fetch_liq_okx(session, symbol),
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
        elif i == 3:
            if isinstance(result, (int, float)):
                ctx.oi.liq_usd_1h = result

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
    src_tag = f" [{oi.source}]" if oi.source else ""
    if oi.oi_delta_pct > 1:
        oi_trend = "\u2191 increasing"
    elif oi.oi_delta_pct < -1:
        oi_trend = "\u2193 decreasing"
    else:
        oi_trend = "\u2192 stable"
    lines.append(f"OI day delta: {oi.oi_delta_pct:+.2f}% ({oi_trend}){src_tag}")

    # Funding rate
    paying = "longs paying" if oi.funding_rate > 0 else "shorts paying"
    lines.append(f"Funding rate (8h): {oi.funding_rate:+.4f}% ({paying}){src_tag}")

    # Liquidaciones 1h
    liq = ctx.oi.liq_usd_1h
    if liq > 0:
        liq_str = f"${liq/1e6:.2f}M" if liq >= 1_000_000 else f"${liq/1_000:.0f}K"
        lines.append(f"Liquidations (1h): {liq_str}")

    # News
    if ctx.news:
        lines.append("Recent news:")
        for item in ctx.news:
            icon = (
                "\U0001f4c8" if item.sentiment == "bullish"
                else "\U0001f4c9" if item.sentiment == "bearish"
                else "\U0001f4f0"
            )
            lines.append(f"  {icon} [{item.sentiment}] {item.title}")
    else:
        lines.append("Recent news: unavailable")

    if ctx.errors:
        lines.append(f"[enricher errors: {', '.join(ctx.errors)}]")

    return "\n".join(lines)
