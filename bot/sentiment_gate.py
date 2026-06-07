#!/usr/bin/env python3
"""
bot/sentiment_gate.py — Filtro multi-factor de sentimiento macro.

Combina dos fuentes gratuitas/baratas en un score 0-100:

  1. Fear & Greed Index (api.alternative.me) — sin API key, sin rate limit.
     Refleja el sentimiento general del mercado crypto.
     Valor 0 = Extreme Fear | 100 = Extreme Greed.

  2. Groq macro sentiment — usa el mismo GROQ_API_KEY que ai_filter.py.
     Llama al modelo con los últimos headlines de CoinTelegraph/CoinDesk/Decrypt
     sin filtrar por símbolo (contexto macro global, no por coin).
     Solo se llama si Fear&Greed no es suficientemente extremo para decidir solo.

Score final y acción:
  score >= SENTIMENT_OPEN_MIN (default 35)  → ✅ permitir entrada
  score <  SENTIMENT_OPEN_MIN               → 🚫 bloquear entrada
  score >= SENTIMENT_SIZE_BOOST (default 65) → 💪 size normal
  score <  SENTIMENT_SIZE_BOOST              → 📉 reducir size al 50%

Cache:
  - Fear&Greed: TTL 30 minutos (el índice se actualiza cada ~24h pero la API
    permite polling frecuente sin problema).
  - Groq macro: TTL 2 horas (configurable con GROQ_MACRO_CACHE_TTL_H).

Variables de entorno Railway:
  SENTIMENT_GATE          → true/false (default: true)
  SENTIMENT_OPEN_MIN      → score mínimo para abrir (default: 35)
  SENTIMENT_SIZE_BOOST    → score para size completo (default: 65)
  GROQ_MACRO_CACHE_TTL_H  → TTL cache Groq macro en horas (default: 2)
  GROQ_API_KEY            → ya existe en el proyecto
  GROQ_MODEL              → ya existe en el proyecto
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Optional

import aiohttp

log = logging.getLogger("SentimentGate")

# ── Configuración ────────────────────────────────────────────────────────────
SENTIMENT_GATE       = os.getenv("SENTIMENT_GATE", "true").lower() != "false"
SENTIMENT_OPEN_MIN   = float(os.getenv("SENTIMENT_OPEN_MIN",   "35"))
SENTIMENT_SIZE_BOOST = float(os.getenv("SENTIMENT_SIZE_BOOST", "65"))

_FNG_CACHE_TTL_S     = 1800.0   # 30 min — el índice no cambia tan rápido
_GROQ_CACHE_TTL_S    = float(os.getenv("GROQ_MACRO_CACHE_TTL_H", "2")) * 3600
_GROQ_API_URL        = "https://api.groq.com/openai/v1/chat/completions"
_GROQ_MODEL          = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
_FNG_API_URL         = "https://api.alternative.me/fng/?limit=1&format=json"

# RSS feeds para contexto macro (mismos que ai_filter, sin filtrar por coin)
_RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://coindesk.com/arc/outboundfeeds/rss/",
]

# ── Cache interno ─────────────────────────────────────────────────────────────
_fng_cache:   tuple[float, float] | None = None   # (score_0_100, timestamp)
_groq_cache:  tuple[float, float] | None = None   # (delta_-2_2, timestamp)

# ── Semáforo para evitar llamadas Groq concurrentes ─────────────────────────
_groq_lock = asyncio.Lock()


# ── Fear & Greed ─────────────────────────────────────────────────────────────

async def _fetch_fear_greed(session: aiohttp.ClientSession) -> float:
    """Devuelve el Fear & Greed Index actual en [0, 100]. 0=Extreme Fear."""
    global _fng_cache

    # Cache
    if _fng_cache is not None:
        val, ts = _fng_cache
        if time.monotonic() - ts < _FNG_CACHE_TTL_S:
            log.debug("[sentiment] Fear&Greed cache hit: %.0f", val)
            return val

    try:
        async with session.get(
            _FNG_API_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                log.warning("[sentiment] Fear&Greed HTTP %d", resp.status)
                return 50.0  # neutral si falla
            data = await resp.json(content_type=None)

        val = float(data["data"][0]["value"])
        label = data["data"][0].get("value_classification", "")
        _fng_cache = (val, time.monotonic())
        log.info("[sentiment] Fear&Greed = %.0f (%s)", val, label)
        return val

    except Exception as e:
        log.warning("[sentiment] Fear&Greed error: %s — usando 50 (neutral)", e)
        return 50.0


# ── Groq macro sentiment ──────────────────────────────────────────────────────

async def _fetch_macro_headlines(session: aiohttp.ClientSession, limit: int = 8) -> list[str]:
    """Obtiene los últimos headlines macro de crypto (sin filtrar por coin)."""
    headlines: list[str] = []
    for url in _RSS_FEEDS:
        try:
            async with session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    continue
                text = await resp.text(errors="replace")
            for item in re.findall(r"<item>(.*?)</item>", text, re.DOTALL)[:limit]:
                title_m = re.search(
                    r"<title>(?:<\!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL
                )
                if title_m:
                    title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()
                    if title:
                        headlines.append(title)
        except Exception as e:
            log.debug("[sentiment] RSS error %s: %s", url, e)
    # Deduplicar y limitar
    seen: set[str] = set()
    unique = [h for h in headlines if not (h in seen or seen.add(h))]  # type: ignore
    return unique[:limit]


async def _groq_macro_sentiment(session: aiohttp.ClientSession) -> float:
    """
    Llama a Groq con headlines macro y devuelve un delta en [-2, +2].
    Usa cache de 2h para no gastar tokens en cada señal.
    """
    global _groq_cache

    # Cache
    if _groq_cache is not None:
        delta, ts = _groq_cache
        if time.monotonic() - ts < _GROQ_CACHE_TTL_S:
            log.debug("[sentiment] Groq macro cache hit: %+.1f", delta)
            return delta

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        log.debug("[sentiment] GROQ_API_KEY no configurada — skip Groq macro")
        return 0.0

    async with _groq_lock:
        # Doble-check del cache tras adquirir el lock (evita doble llamada concurrente)
        if _groq_cache is not None:
            delta, ts = _groq_cache
            if time.monotonic() - ts < _GROQ_CACHE_TTL_S:
                return delta

        headlines = await _fetch_macro_headlines(session)
        if not headlines:
            log.debug("[sentiment] Sin headlines macro — Groq skip")
            return 0.0

        headlines_text = "\n".join(f"- {h}" for h in headlines)
        prompt = (
            "Eres un analista de mercados crypto. Analiza estos titulares recientes "
            "y evalúa el sentimiento MACRO del mercado crypto en general "
            "(no de una coin específica) a corto plazo (próximas 12-24h):\n\n"
            f"{headlines_text}\n\n"
            "Responde ÚNICAMENTE con este JSON (sin markdown ni explicación extra):\n"
            '{"score_delta": <número entre -2.0 y 2.0>, "reason": "<1 frase breve>"}\n\n'
            "Escala: +2=mercado muy alcista/risk-on, +1=alcista, 0=neutro, "
            "-1=bajista, -2=mercado muy bajista/risk-off."
        )

        try:
            async with session.post(
                _GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       _GROQ_MODEL,
                    "messages":    [{"role": "user", "content": prompt}],
                    "max_tokens":  80,
                    "temperature": 0.1,
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    log.warning("[sentiment] Groq macro HTTP %d", resp.status)
                    _groq_cache = (0.0, time.monotonic())
                    return 0.0
                data = await resp.json()

            content = data["choices"][0]["message"]["content"].strip()
            # Limpiar posibles code fences
            if content.startswith("```"):
                content = "\n".join(
                    l for l in content.splitlines()
                    if not l.strip().startswith("```")
                ).strip()

            parsed = json.loads(content)
            delta  = max(-2.0, min(2.0, float(parsed.get("score_delta", 0.0))))
            reason = parsed.get("reason", "")[:140]
            log.info(
                "[sentiment] Groq macro → score_delta=%+.1f | %s",
                delta, reason,
            )
            _groq_cache = (delta, time.monotonic())
            return delta

        except Exception as e:
            log.warning("[sentiment] Groq macro error: %s", e)
            _groq_cache = (0.0, time.monotonic())
            return 0.0


# ── Score combinado ───────────────────────────────────────────────────────────

async def compute_sentiment_score() -> dict:
    """
    Calcula el score multi-factor de sentimiento [0, 100].

    Ponderación:
      60% Fear&Greed (fuente objetiva, actualizada diariamente)
      40% Groq macro sentiment (normalizado de [-2,+2] a [0,100])

    Returns dict con:
      {
        "score":     float,   # 0-100
        "fng":       float,   # Fear&Greed raw (0-100)
        "groq":      float,   # Groq delta (-2 a +2)
        "allowed":   bool,    # score >= SENTIMENT_OPEN_MIN
        "full_size": bool,    # score >= SENTIMENT_SIZE_BOOST
        "reason":    str,
      }
    """
    async with aiohttp.ClientSession() as session:
        fng_val, groq_delta = await asyncio.gather(
            _fetch_fear_greed(session),
            _groq_macro_sentiment(session),
        )

    # Normalizar Groq delta [-2,+2] → [0,100]
    groq_normalized = (groq_delta + 2.0) / 4.0 * 100.0  # -2→0, 0→50, +2→100

    score = 0.60 * fng_val + 0.40 * groq_normalized
    score = max(0.0, min(100.0, score))

    allowed   = score >= SENTIMENT_OPEN_MIN
    full_size = score >= SENTIMENT_SIZE_BOOST

    # Etiqueta legible para Fear&Greed
    if fng_val <= 20:    fng_label = "Extreme Fear"
    elif fng_val <= 40:  fng_label = "Fear"
    elif fng_val <= 60:  fng_label = "Neutral"
    elif fng_val <= 80:  fng_label = "Greed"
    else:                fng_label = "Extreme Greed"

    reason = (
        f"F&G={fng_val:.0f} ({fng_label}) | "
        f"Groq={groq_delta:+.1f} | "
        f"score={score:.0f}/100"
    )

    log.info(
        "[sentiment] %s → %s size=%s",
        reason,
        "✅ OPEN" if allowed else "🚫 BLOCK",
        "full" if full_size else "50%",
    )

    return {
        "score":     score,
        "fng":       fng_val,
        "groq":      groq_delta,
        "allowed":   allowed,
        "full_size": full_size,
        "reason":    reason,
    }


async def sentiment_gate_check() -> tuple[bool, str, bool]:
    """
    Punto de entrada principal para decision_engine.

    Returns:
        (allowed, reason, full_size)

        allowed   → True si el sentimiento permite abrir posición
        reason    → string para log/Telegram
        full_size → True si usar size completo, False si reducir al 50%
    """
    if not SENTIMENT_GATE:
        return True, "sentiment_gate=OFF", True

    try:
        result = await compute_sentiment_score()
        return result["allowed"], result["reason"], result["full_size"]
    except Exception as e:
        log.warning("[sentiment] sentiment_gate_check error: %s — fail-open", e)
        return True, f"sentiment error ({e}) — fail-open", True
