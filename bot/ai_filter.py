"""
ai_filter.py — Filtro de noticias via IA con fuente RSS real.

Arquitectura correcta:
  1. Fetch RSS de CryptoPanic (gratuito, sin API key) para el símbolo.
  2. Si no hay titulares nuevos en las últimas N horas → devuelve 0.0 SIN llamar a Groq.
  3. Solo si hay titulares reales → los envía a Groq para que evalué el impacto.

Esto elimina el problema anterior donde Groq se consultaba cada ciclo de scan
para TODOS los pares, incluso cuando no había absolutamente nada que analizar.

Variables de entorno:
  GROQ_API_KEY            — clave Groq (si falta, devuelve 0.0 siempre)
  GROQ_MODEL              — modelo (default: llama-3.1-8b-instant)
  AI_NEWS_HOURS_LOOKBACK  — horas hacia atras para buscar noticias (default: 6)
  AI_NEWS_CACHE_TTL_H     — horas de validez del cache por simbolo (default: 2)
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import aiohttp

from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AIFilter")

GROQ_API_URL    = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL      = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
_HOURS_LOOKBACK = float(os.getenv("AI_NEWS_HOURS_LOOKBACK", "6"))
_CACHE_TTL_S    = float(os.getenv("AI_NEWS_CACHE_TTL_H", "2")) * 3600
_SCORE_MIN      = -2.0
_SCORE_MAX      =  2.0

# Cache en memoria: {symbol: (score_delta, timestamp_monotonic)}
_NEWS_CACHE: dict[str, tuple[float, float]] = {}


# ────────────────────────────────────────────────────────────────────
# Cache helpers
# ────────────────────────────────────────────────────────────────────

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


# ────────────────────────────────────────────────────────────────────
# RSS fetch — CryptoPanic (sin API key, endpoint publico)
# ────────────────────────────────────────────────────────────────────

async def _fetch_recent_headlines(symbol: str, hours: float) -> list[str]:
    """
    Obtiene titulares recientes de CryptoPanic RSS para el simbolo dado.
    Devuelve lista de strings con los titulares. Lista vacia = sin noticias.
    Nunca lanza excepciones.
    """
    url = f"https://cryptopanic.com/news/{symbol.lower()}/rss/"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    headlines: list[str] = []

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    logger.debug("[AIFilter] RSS %s HTTP %d", symbol, resp.status)
                    return []
                text = await resp.text()

        # Parse RSS manualmente (sin xml.etree para evitar dependencias extra)
        # Extraer <item> con <title> y <pubDate>
        import re
        items = re.findall(r"<item>(.*?)</item>", text, re.DOTALL)
        for item in items:
            title_m = re.search(r"<title>(?:<\!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL)
            date_m  = re.search(r"<pubDate>(.*?)</pubDate>", item)
            if not title_m:
                continue
            title = title_m.group(1).strip()
            if date_m:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(date_m.group(1).strip())
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue  # Noticia demasiado antigua
                except Exception:
                    pass  # Si no podemos parsear la fecha, incluimos el titular
            if title:
                headlines.append(title)

        logger.debug("[AIFilter] RSS %s: %d titulares en ultimas %.0fh", symbol, len(headlines), hours)
        return headlines[:10]  # Maximo 10 titulares para no saturar el prompt

    except asyncio.TimeoutError:
        logger.debug("[AIFilter] RSS timeout para %s", symbol)
        return []
    except Exception as e:
        logger.debug("[AIFilter] RSS error para %s: %s", symbol, e)
        return []


# ────────────────────────────────────────────────────────────────────
# Groq analysis
# ────────────────────────────────────────────────────────────────────

def _clean_json_response(content: str) -> str:
    content = content.strip()
    if content.startswith("```"):
        lines = content.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    return content


async def _groq_analyze(symbol: str, headlines: list[str]) -> float:
    """
    Llama a Groq con los titulares reales y devuelve el score_delta.
    Se asume que headlines no está vacía (el caller ya lo verificó).
    """
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return 0.0

    if not await budget.can_call_groq():
        logger.debug("[AIFilter] Budget Groq agotado para %s", symbol)
        return 0.0

    headlines_text = "\n".join(f"- {h}" for h in headlines)
    prompt = (
        f"Analiza estos titulares de noticias recientes sobre {symbol} "
        f"y evalua su impacto probable en el precio a corto plazo (4-24h):\n\n"
        f"{headlines_text}\n\n"
        f"Responde UNICAMENTE con este JSON:\n"
        f'{{"score_delta": <-2.0 a 2.0>, "reason": "<1 frase>"}}'  "\n\n"
        f"Escala:\n"
        f"  +2.0 = muy positivo/alcista\n"
        f"  +1.0 = moderadamente positivo\n"
        f"   0.0 = neutro o incierto\n"
        f"  -1.0 = moderadamente negativo\n"
        f"  -2.0 = muy negativo/bajista"
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

                if resp.status == 429:
                    logger.warning("[AIFilter] Groq 429 para %s", symbol)
                    return 0.0
                if resp.status != 200:
                    logger.warning("[AIFilter] Groq HTTP %d para %s", resp.status, symbol)
                    return 0.0

                data    = await resp.json()
                content = data["choices"][0]["message"]["content"]
                parsed  = json.loads(_clean_json_response(content))

                delta  = float(parsed.get("score_delta", 0.0))
                reason = parsed.get("reason", "")
                delta  = max(_SCORE_MIN, min(_SCORE_MAX, delta))

                logger.info(
                    "[AIFilter] %s — 📰 %d titulares → score_delta=%+.1f | %s",
                    symbol, len(headlines), delta, reason[:120]
                )
                return delta

    except (RateLimitExhausted, asyncio.TimeoutError, json.JSONDecodeError, Exception) as e:
        logger.debug("[AIFilter] Error Groq para %s: %s", symbol, e)
        return 0.0


# ────────────────────────────────────────────────────────────────────
# Punto de entrada público
# ────────────────────────────────────────────────────────────────────

async def news_score_adjustment(symbol: str) -> float:
    """
    Devuelve un delta de score en [-2.0, +2.0] basado en noticias REALES.

    Flujo:
      1. Check cache → si vigente, devuelve sin ninguna llamada externa.
      2. Fetch RSS de CryptoPanic → si no hay titulares recientes, devuelve 0.0
         SIN llamar a Groq (este es el caso mas comun).
      3. Solo si hay titulares → llama a Groq para analizar el sentimiento.
      4. Cachea el resultado (TTL: AI_NEWS_CACHE_TTL_H horas, default 2h).

    Args:
        symbol: simbolo base, ej. "BTC", "ETH", "SOL" o "BTCUSDT"

    Returns:
        float en [-2.0, +2.0]
    """
    base = symbol.replace("USDT", "").replace("USDC", "").replace("-PERP", "").upper()

    # 1. Cache
    cached = _get_cached(base)
    if cached is not None:
        logger.debug("[AIFilter] %s — cache hit, score_delta=%+.1f", base, cached)
        return cached

    # 2. RSS — fuente de verdad
    headlines = await _fetch_recent_headlines(base, _HOURS_LOOKBACK)

    if not headlines:
        # Sin noticias → 0.0, cacheado para no volver a pedir RSS en el proximo ciclo
        logger.debug("[AIFilter] %s — sin titulares recientes, score_delta=0", base)
        _set_cached(base, 0.0)
        return 0.0

    # 3. Hay noticias → analizar con Groq
    delta = await _groq_analyze(base, headlines)
    _set_cached(base, delta)
    return delta


# ---------------------------------------------------------------------------
# LEGACY
# ---------------------------------------------------------------------------

async def ai_rank_pairs(pairs_data: list) -> list:
    """
    DEPRECADO. Devuelve pares ordenados por score sin llamar a Groq.
    """
    logger.debug("[AIFilter] ai_rank_pairs() legacy — sin IA")
    return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]
