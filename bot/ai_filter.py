"""
ai_filter.py — Filtro de noticias via IA con RSS gratuitos.

Fuentes RSS (100% gratuitas, sin API key, sin registro):
  - CoinTelegraph : https://cointelegraph.com/rss
  - CoinDesk      : https://coindesk.com/arc/outboundfeeds/rss/
  - Decrypt       : https://decrypt.co/feed

Flujo:
  1. Check cache  → si vigente, devuelve sin ninguna llamada externa.
  2. Fetch RSS de las 3 fuentes en paralelo.
  3. Filtra titulares que mencionen el simbolo o su nombre completo.
  4. Sin titulares relevantes → 0.0 SIN llamar a Groq.
  5. Con titulares → llama a Groq para evaluar el sentimiento.
  6. Cachea el resultado.

Variables de entorno:
  GROQ_API_KEY            — clave Groq
  GROQ_MODEL              — modelo (default: llama-3.1-8b-instant)
  AI_NEWS_HOURS_LOOKBACK  — horas hacia atras para buscar noticias (default: 6)
  AI_NEWS_CACHE_TTL_H     — horas de validez del cache por simbolo (default: 2)
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import aiohttp

from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AIFilter")

GROQ_API_URL    = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL      = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
_HOURS_LOOKBACK = float(os.getenv("AI_NEWS_HOURS_LOOKBACK", "6"))
_CACHE_TTL_S    = float(os.getenv("AI_NEWS_CACHE_TTL_H", "2")) * 3600
_SCORE_MIN      = -2.0
_SCORE_MAX      =  2.0

RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://coindesk.com/arc/outboundfeeds/rss/",
    "https://decrypt.co/feed",
]

# Mapa de simbolo -> palabras clave adicionales para el filtro
_SYMBOL_KEYWORDS: dict[str, list[str]] = {
    "BTC":  ["bitcoin"],
    "ETH":  ["ethereum", "ether"],
    "SOL":  ["solana"],
    "BNB":  ["binance"],
    "XRP":  ["ripple"],
    "ADA":  ["cardano"],
    "DOGE": ["dogecoin"],
    "AVAX": ["avalanche"],
    "DOT":  ["polkadot"],
    "MATIC": ["polygon"],
    "LINK": ["chainlink"],
    "UNI":  ["uniswap"],
    "ATOM": ["cosmos"],
    "LTC":  ["litecoin"],
    "NEAR": ["near protocol"],
    "ARB":  ["arbitrum"],
    "OP":   ["optimism"],
    "SUI":  ["sui network"],
    "APT":  ["aptos"],
    "INJ":  ["injective"],
}

# Cache en memoria: {symbol: (score_delta, timestamp_monotonic)}
_NEWS_CACHE: dict[str, tuple[float, float]] = {}


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _get_cached(symbol: str) -> float | None:
    entry = _NEWS_CACHE.get(symbol)
    if entry is None:
        return None
    delta, ts = entry
    if time.monotonic() - ts < _CACHE_TTL_S:
        return delta
    del _NEWS_CACHE[symbol]
    return None


def _set_cached(symbol: str, delta: float) -> None:
    _NEWS_CACHE[symbol] = (delta, time.monotonic())


# ---------------------------------------------------------------------------
# RSS fetch
# ---------------------------------------------------------------------------

def _matches_symbol(text: str, symbol: str) -> bool:
    """True si el texto menciona el simbolo o sus nombres conocidos."""
    text_lower = text.lower()
    if symbol.lower() in text_lower:
        return True
    for kw in _SYMBOL_KEYWORDS.get(symbol, []):
        if kw in text_lower:
            return True
    return False


async def _fetch_feed(session: aiohttp.ClientSession, url: str, symbol: str, cutoff: datetime) -> list[str]:
    """Descarga un feed RSS y devuelve titulares relevantes para el simbolo."""
    try:
        async with session.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return []
            text = await resp.text(errors="replace")

        headlines = []
        items = re.findall(r"<item>(.*?)</item>", text, re.DOTALL)
        for item in items:
            title_m = re.search(r"<title>(?:<\!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL)
            if not title_m:
                continue
            title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()
            if not title or not _matches_symbol(title, symbol):
                continue
            # Filtro de fecha
            date_m = re.search(r"<pubDate>(.*?)</pubDate>", item)
            if date_m:
                try:
                    pub_dt = parsedate_to_datetime(date_m.group(1).strip())
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue
                except Exception:
                    pass
            headlines.append(title)
        return headlines

    except Exception as e:
        logger.debug("[AIFilter] RSS error %s: %s", url, e)
        return []


async def _fetch_recent_headlines(symbol: str, hours: float) -> list[str]:
    """Busca titulares recientes sobre el simbolo en todos los feeds RSS en paralelo."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[_fetch_feed(session, url, symbol, cutoff) for url in RSS_FEEDS],
            return_exceptions=True,
        )
    headlines: list[str] = []
    for r in results:
        if isinstance(r, list):
            headlines.extend(r)
    # Deduplicar por texto exacto
    seen: set[str] = set()
    unique = [h for h in headlines if not (h in seen or seen.add(h))]
    logger.debug("[AIFilter] %s — %d titulares relevantes en ultimas %.0fh", symbol, len(unique), hours)
    return unique[:10]


# ---------------------------------------------------------------------------
# Groq analysis
# ---------------------------------------------------------------------------

def _clean_json(content: str) -> str:
    content = content.strip()
    if content.startswith("```"):
        lines = content.splitlines()
        lines = lines[1:] if lines[0].startswith("```") else lines
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    return content


async def _groq_analyze(symbol: str, headlines: list[str]) -> float:
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return 0.0
    try:
        if not await budget.can_call_groq():
            return 0.0
    except Exception:
        return 0.0

    headlines_text = "\n".join(f"- {h}" for h in headlines)
    prompt = (
        f"Analiza estos titulares recientes sobre {symbol} y "
        f"evalua su impacto probable en el precio a corto plazo (4-24h):\n\n"
        f"{headlines_text}\n\n"
        f"Responde UNICAMENTE con este JSON (sin markdown):\n"
        f'{{"score_delta": <-2.0 a 2.0>, "reason": "<1 frase breve>"}}\n\n'
        f"Escala: +2=muy alcista, +1=alcista, 0=neutro, -1=bajista, -2=muy bajista."
    )

    try:
        async with budget.groq_semaphore:
            await budget.register_groq_call()
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    GROQ_API_URL,
                    headers={
                        "Authorization": f"Bearer {groq_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": GROQ_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 60,
                        "temperature": 0.1,
                    },
                    timeout=aiohttp.ClientTimeout(total=12),
                )
                if resp.status != 200:
                    logger.warning("[AIFilter] Groq HTTP %d para %s", resp.status, symbol)
                    return 0.0
                data   = await resp.json()
                parsed = json.loads(_clean_json(data["choices"][0]["message"]["content"]))
                delta  = max(_SCORE_MIN, min(_SCORE_MAX, float(parsed.get("score_delta", 0.0))))
                logger.info(
                    "[AIFilter] %s — 📰 %d titulares → score_delta=%+.1f | %s",
                    symbol, len(headlines), delta, parsed.get("reason", "")[:120],
                )
                return delta
    except (RateLimitExhausted, asyncio.TimeoutError, json.JSONDecodeError, Exception) as e:
        logger.debug("[AIFilter] Groq error para %s: %s", symbol, e)
        return 0.0


# ---------------------------------------------------------------------------
# Punto de entrada publico
# ---------------------------------------------------------------------------

async def news_score_adjustment(symbol: str) -> float:
    """
    Devuelve un delta de score en [-2.0, +2.0] basado en noticias reales.

    Flujo:
      1. Cache   → devuelve sin llamadas si vigente.
      2. RSS     → busca titulares que mencionen el simbolo (gratis, paralelo).
      3. Sin titulares → 0.0 SIN llamar a Groq.
      4. Con titulares → llama a Groq para analizar sentimiento.
    """
    base = symbol.replace("USDT", "").replace("USDC", "").replace("-PERP", "").upper()

    cached = _get_cached(base)
    if cached is not None:
        logger.debug("[AIFilter] %s — cache hit, score_delta=%+.1f", base, cached)
        return cached

    headlines = await _fetch_recent_headlines(base, _HOURS_LOOKBACK)

    if not headlines:
        logger.debug("[AIFilter] %s — sin noticias, score_delta=0", base)
        _set_cached(base, 0.0)
        return 0.0

    delta = await _groq_analyze(base, headlines)
    _set_cached(base, delta)
    return delta


# ---------------------------------------------------------------------------
# LEGACY
# ---------------------------------------------------------------------------

async def ai_rank_pairs(pairs_data: list) -> list:
    logger.debug("[AIFilter] ai_rank_pairs() legacy — sin IA")
    return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]
