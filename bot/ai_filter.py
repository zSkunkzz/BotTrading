"""
ai_filter.py — Filtro de noticias via IA con RSS gratuitos.

Fuentes RSS (100% gratuitas, sin API key, sin registro):
  - CoinTelegraph : https://cointelegraph.com/rss
  - CoinDesk      : https://coindesk.com/arc/outboundfeeds/rss/
  - Decrypt       : https://decrypt.co/feed

Resolucion de nombres de coins:
  En lugar de una lista estatica hardcodeada, se consulta CoinGecko API
  (/search?query=SYMBOL) la primera vez que se ve un simbolo. El nombre
  completo se cachea en memoria 24h. Esto cubre los 230+ pares de Hyperliquid
  sin mantenimiento manual.

  Overrides estaticos para tickers ambiguos donde CoinGecko devuelve resultados
  incorrectos (ej: ENA -> busca "ethena" directamente).

Flujo:
  1. Check cache  → si vigente, devuelve sin ninguna llamada externa.
  2. Fetch RSS de las 3 fuentes en paralelo (una sola ClientSession).
  3. Filtra titulares con word-boundary para tickers cortos + nombre completo.
  4. Sin titulares relevantes → 0.0 SIN llamar a Groq.
  5. Con titulares → llama a Groq (misma sesion reutilizada).
  6. Cachea el resultado.

Variables de entorno:
  GROQ_API_KEY            — clave Groq
  GROQ_MODEL              — modelo (default: llama-3.1-8b-instant)
  AI_NEWS_HOURS_LOOKBACK  — horas hacia atras para buscar noticias (default: 6)
  AI_NEWS_CACHE_TTL_H     — horas de validez del cache por simbolo (default: 2)

CAMBIOS v22 (mejora cache de noticias):
  - El cache ahora distingue entre deltas negativos y positivos.
  - Si el delta cacheado es negativo y llega un delta fresco >= NEWS_POSITIVE_MIN
    (default +2.0), se actualiza el cache y se devuelve el valor positivo.
    Esto permite que noticias muy positivas refuercen la entrada incluso si
    había un delta negativo cacheado previamente.
  - NEWS_POSITIVE_MIN es configurable via env para ajustar la sensibilidad.
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
_BOUNDARY_THRESHOLD = 4  # Tickers de <= 4 chars usan word-boundary

# v22: umbral para que una noticia positiva fresca override un cache negativo
_NEWS_POSITIVE_MIN = float(os.getenv("NEWS_POSITIVE_MIN", "2.0"))

RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://coindesk.com/arc/outboundfeeds/rss/",
    "https://decrypt.co/feed",
]

# Overrides estaticos para tickers donde CoinGecko puede devolver el resultado
# equivocado o donde el nombre en los medios difiere del nombre oficial.
# Solo se usan si CoinGecko falla o devuelve un nombre demasiado generico.
_STATIC_OVERRIDES: dict[str, list[str]] = {
    "ENA":   ["ethena"],
    "OP":    ["optimism"],
    "TON":   ["toncoin", "the open network"],
    "HYPE":  ["hyperliquid"],
    "PUMP":  ["pump.fun"],
    "WLD":   ["worldcoin"],
    "ONDO":  ["ondo finance"],
    "LIT":   ["litentry"],
    "ZEC":   ["zcash"],
    "BNB":   ["binance"],
    "XRP":   ["ripple"],
    "ADA":   ["cardano"],
    "SOL":   ["solana"],
    "ETH":   ["ethereum"],
    "BTC":   ["bitcoin"],
    "DOT":   ["polkadot"],
    "UNI":   ["uniswap"],
    "LINK":  ["chainlink"],
    "ATOM":  ["cosmos"],
    "LTC":   ["litecoin"],
    "ARB":   ["arbitrum"],
    "SUI":   ["sui network"],
    "APT":   ["aptos"],
    "INJ":   ["injective"],
    "NEAR":  ["near protocol"],
    "AVAX":  ["avalanche"],
    "DOGE":  ["dogecoin"],
    "MATIC": ["polygon"],
}

# Cache de nombres: {symbol: ([keywords], timestamp_monotonic)}
# TTL de 24h — los nombres de coins no cambian
_NAME_CACHE: dict[str, tuple[list[str], float]] = {}
_NAME_CACHE_TTL_S = 86400.0

# Cache de score: {symbol: (score_delta, timestamp_monotonic)}
_NEWS_CACHE: dict[str, tuple[float, float]] = {}


# ---------------------------------------------------------------------------
# Score cache
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
# Resolucion de nombre via CoinGecko (con cache 24h)
# ---------------------------------------------------------------------------

async def _resolve_coin_name(
    session: aiohttp.ClientSession,
    symbol: str,
) -> list[str]:
    """
    Devuelve lista de keywords para el simbolo: [nombre_completo_lower, ...].
    Orden de prioridad:
      1. Cache en memoria (TTL 24h)
      2. Override estatico (_STATIC_OVERRIDES)
      3. CoinGecko /search API (gratis, sin key)
      4. Fallback: lista vacia (solo se usara el ticker con word-boundary)
    """
    # 1. Cache
    entry = _NAME_CACHE.get(symbol)
    if entry is not None:
        kws, ts = entry
        if time.monotonic() - ts < _NAME_CACHE_TTL_S:
            return kws

    # 2. Override estatico (rapido, sin red)
    if symbol in _STATIC_OVERRIDES:
        kws = _STATIC_OVERRIDES[symbol]
        _NAME_CACHE[symbol] = (kws, time.monotonic())
        return kws

    # 3. CoinGecko search
    try:
        url = f"https://api.coingecko.com/api/v3/search?query={symbol}"
        async with session.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                _NAME_CACHE[symbol] = ([], time.monotonic())
                return []
            data = await resp.json()

        coins = data.get("coins", [])
        # Buscar el resultado cuyo simbolo coincida exactamente (case-insensitive)
        full_name = ""
        for coin in coins[:10]:
            if coin.get("symbol", "").upper() == symbol.upper():
                full_name = coin.get("name", "").lower().strip()
                break

        kws = [full_name] if full_name and full_name != symbol.lower() else []
        _NAME_CACHE[symbol] = (kws, time.monotonic())
        if kws:
            logger.debug("[AIFilter] CoinGecko: %s → '%s'", symbol, kws[0])
        return kws

    except Exception as e:
        logger.debug("[AIFilter] CoinGecko error para %s: %s", symbol, e)
        _NAME_CACHE[symbol] = ([], time.monotonic())
        return []


# ---------------------------------------------------------------------------
# Symbol matching con word-boundary para tickers cortos
# ---------------------------------------------------------------------------

def _word_boundary_match(text_lower: str, term: str) -> bool:
    return bool(re.search(rf"\b{re.escape(term)}\b", text_lower))


def _matches_symbol(text: str, symbol: str, keywords: list[str]) -> bool:
    """
    True si el texto menciona el simbolo o alguna keyword.
    - Tickers de <= 4 chars: word-boundary obligatorio.
    - Tickers de >= 5 chars: substring simple.
    - Keywords: siempre word-boundary.
    """
    text_lower = text.lower()
    sym_lower  = symbol.lower()

    if len(symbol) <= _BOUNDARY_THRESHOLD:
        if _word_boundary_match(text_lower, sym_lower):
            return True
    else:
        if sym_lower in text_lower:
            return True

    for kw in keywords:
        if _word_boundary_match(text_lower, kw):
            return True

    return False


# ---------------------------------------------------------------------------
# RSS fetch — recibe session ya abierta
# ---------------------------------------------------------------------------

async def _fetch_feed(
    session: aiohttp.ClientSession,
    url: str,
    symbol: str,
    keywords: list[str],
    cutoff: datetime,
) -> list[str]:
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
        for item in re.findall(r"<item>(.*?)</item>", text, re.DOTALL):
            title_m = re.search(
                r"<title>(?:<\!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL
            )
            if not title_m:
                continue
            title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()
            if not title or not _matches_symbol(title, symbol, keywords):
                continue
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


async def _fetch_recent_headlines(
    session: aiohttp.ClientSession,
    symbol: str,
    hours: float,
) -> list[str]:
    cutoff   = datetime.now(timezone.utc) - timedelta(hours=hours)
    keywords = await _resolve_coin_name(session, symbol)

    results = await asyncio.gather(
        *[_fetch_feed(session, url, symbol, keywords, cutoff) for url in RSS_FEEDS],
        return_exceptions=True,
    )
    headlines: list[str] = []
    for r in results:
        if isinstance(r, list):
            headlines.extend(r)
    seen: set[str] = set()
    unique = [h for h in headlines if not (h in seen or seen.add(h))]  # type: ignore[func-returns-value]
    logger.debug(
        "[AIFilter] %s (keywords=%s) — %d titulares en ultimas %.0fh",
        symbol, keywords or ["-"], len(unique), hours,
    )
    return unique[:10]


# ---------------------------------------------------------------------------
# Groq analysis — recibe session ya abierta
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


async def _groq_analyze(
    session: aiohttp.ClientSession,
    symbol: str,
    headlines: list[str],
) -> float:
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
            async with session.post(
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
            ) as resp:
                if resp.status != 200:
                    logger.warning("[AIFilter] Groq HTTP %d para %s", resp.status, symbol)
                    return 0.0
                data = await resp.json()

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

    Crea UNA sola ClientSession para toda la operacion y la cierra correctamente.

    Flujo:
      1. Cache       → devuelve sin llamadas si vigente Y no hay override positivo.
      2. CoinGecko   → resuelve nombre completo del ticker (cache 24h).
      3. RSS (gratis)→ busca titulares con ticker + nombre.
      4. Sin titulares → 0.0 SIN llamar a Groq.
      5. Con titulares → llama a Groq para analizar sentimiento.

    v22 — override positivo:
      Si hay un delta cacheado NEGATIVO y el delta fresco >= NEWS_POSITIVE_MIN,
      el cache se actualiza con el valor positivo y se devuelve ese valor.
      Esto evita que una noticia negativa cacheada bloquee una entrada cuando
      llega una noticia muy alcista posterior dentro de la misma ventana.
    """
    base = symbol.replace("USDT", "").replace("USDC", "").replace("-PERP", "").upper()

    cached = _get_cached(base)

    # Si no hay cache o es positivo, comportamiento normal
    if cached is not None and cached >= 0:
        logger.debug("[AIFilter] %s — cache hit (positivo/neutro), score_delta=%+.1f", base, cached)
        return cached

    # Si hay cache negativo, aún así consultamos para ver si llegó algo muy positivo
    async with aiohttp.ClientSession() as session:
        headlines = await _fetch_recent_headlines(session, base, _HOURS_LOOKBACK)

        if not headlines:
            if cached is not None:
                # Mantener cache negativo — sin noticias nuevas
                logger.debug("[AIFilter] %s — cache negativo mantenido (%+.1f), sin titulares nuevos", base, cached)
                return cached
            _set_cached(base, 0.0)
            return 0.0

        delta = await _groq_analyze(session, base, headlines)

    # v22: si el delta fresco es fuertemente positivo, override el cache negativo
    if cached is not None and cached < 0 and delta >= _NEWS_POSITIVE_MIN:
        logger.info(
            "[AIFilter] %s — override positivo: cache=%+.1f fresco=%+.1f >= %.1f → actualizando",
            base, cached, delta, _NEWS_POSITIVE_MIN,
        )
        _set_cached(base, delta)
        return delta

    # Si el cache era negativo y el delta fresco no es suficientemente positivo,
    # usar el peor de los dos (más conservador)
    if cached is not None and cached < 0:
        combined = min(cached, delta)
        logger.debug(
            "[AIFilter] %s — cache negativo (%+.1f) + fresco (%+.1f) → usando min=%+.1f",
            base, cached, delta, combined,
        )
        return combined

    _set_cached(base, delta)
    return delta


# ---------------------------------------------------------------------------
# LEGACY
# ---------------------------------------------------------------------------

async def ai_rank_pairs(pairs_data: list) -> list:
    logger.debug("[AIFilter] ai_rank_pairs() legacy — sin IA")
    return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]
