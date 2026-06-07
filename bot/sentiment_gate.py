#!/usr/bin/env python3
"""
bot/sentiment_gate.py — Filtro multi-factor de sentimiento macro.

Combina dos fuentes en un score 0-100:

  1. Fear & Greed Index (api.alternative.me) — sin API key, gratuito.
     Valor 0 = Extreme Fear | 100 = Extreme Greed.

  2. Groq macro sentiment — usa GROQ_API_KEY ya existente.
     SOLO se llama cuando F&G está en zona AMBIGUA [FNG_GROQ_LO, FNG_GROQ_HI].
     Si F&G es extremo (< 25 o > 75), decide solo sin gastar tokens.
     Cache global de 2h: se llama como máximo 12 veces al día.

Lógica de llamada a Groq:
  - F&G <= FNG_GROQ_LO (default 25)  → Extreme Fear  → decide solo (score bajo)
  - F&G >= FNG_GROQ_HI (default 75)  → Extreme Greed → decide solo (score alto)
  - 25 < F&G < 75 (zona ambigua)      → llama Groq para desempatar

Score final y acción:
  score >= SENTIMENT_OPEN_MIN (default 35)   → ✅ permitir entrada
  score <  SENTIMENT_OPEN_MIN               → 🚫 bloquear entrada
  score >= SENTIMENT_SIZE_BOOST (default 65) → 💪 size completo
  score <  SENTIMENT_SIZE_BOOST             → 📉 reducir size al 50%

Cache:
  - Fear&Greed: TTL 30 min (1 llamada HTTP ligera cada 30 min máximo)
  - Groq macro: TTL 2h    (solo cuando F&G es ambiguo → máx 12/día)

Variables de entorno Railway:
  SENTIMENT_GATE=true/false       (default: true)
  SENTIMENT_OPEN_MIN=35           score mínimo para abrir
  SENTIMENT_SIZE_BOOST=65         score para size completo
  FNG_GROQ_LO=25                  por debajo: F&G decide solo (extremo bajista)
  FNG_GROQ_HI=75                  por encima: F&G decide solo (extremo alcista)
  GROQ_MACRO_CACHE_TTL_H=2        TTL cache Groq en horas
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time

import aiohttp

log = logging.getLogger("SentimentGate")

# ── Configuración ──────────────────────────────────────────────────────
SENTIMENT_GATE       = os.getenv("SENTIMENT_GATE",    "true").lower() != "false"
SENTIMENT_OPEN_MIN   = float(os.getenv("SENTIMENT_OPEN_MIN",   "35"))
SENTIMENT_SIZE_BOOST = float(os.getenv("SENTIMENT_SIZE_BOOST", "65"))
_FNG_GROQ_LO         = float(os.getenv("FNG_GROQ_LO", "25"))  # por debajo: no llama Groq
_FNG_GROQ_HI         = float(os.getenv("FNG_GROQ_HI", "75"))  # por encima: no llama Groq
_FNG_CACHE_TTL_S     = 1800.0  # 30 min
_GROQ_CACHE_TTL_S    = float(os.getenv("GROQ_MACRO_CACHE_TTL_H", "2")) * 3600
_GROQ_API_URL        = "https://api.groq.com/openai/v1/chat/completions"
_GROQ_MODEL          = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
_FNG_API_URL         = "https://api.alternative.me/fng/?limit=1&format=json"
_RSS_FEEDS           = [
    "https://cointelegraph.com/rss",
    "https://coindesk.com/arc/outboundfeeds/rss/",
]

# ── Cache interno ─────────────────────────────────────────────────────────────
_fng_cache:  tuple[float, float] | None = None  # (valor_0_100, timestamp)
_groq_cache: tuple[float, float] | None = None  # (delta_-2_2, timestamp)
_groq_lock   = asyncio.Lock()


# ── Fear & Greed ─────────────────────────────────────────────────────────────

async def _fetch_fear_greed(session: aiohttp.ClientSession) -> float:
    global _fng_cache
    if _fng_cache is not None:
        val, ts = _fng_cache
        if time.monotonic() - ts < _FNG_CACHE_TTL_S:
            return val
    try:
        async with session.get(
            _FNG_API_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return 50.0
            data = await resp.json(content_type=None)
        val   = float(data["data"][0]["value"])
        label = data["data"][0].get("value_classification", "")
        _fng_cache = (val, time.monotonic())
        log.info("[sentiment] Fear&Greed = %.0f (%s)", val, label)
        return val
    except Exception as e:
        log.warning("[sentiment] Fear&Greed error: %s — usando 50", e)
        return 50.0


# ── Groq macro (solo si F&G ambiguo) ─────────────────────────────────────────

async def _fetch_macro_headlines(session: aiohttp.ClientSession) -> list[str]:
    headlines: list[str] = []
    for url in _RSS_FEEDS:
        try:
            async with session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    continue
                text = await resp.text(errors="replace")
            for item in re.findall(r"<item>(.*?)</item>", text, re.DOTALL)[:6]:
                m = re.search(r"<title>(?:<\!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL)
                if m:
                    title = re.sub(r"<[^>]+>", "", m.group(1)).strip()
                    if title:
                        headlines.append(title)
        except Exception:
            pass
    seen: set[str] = set()
    return [h for h in headlines if not (h in seen or seen.add(h))][:8]  # type: ignore


async def _groq_macro(session: aiohttp.ClientSession) -> float:
    """Llama Groq UNA vez y cachea 2h. Protegido por lock para evitar doble llamada."""
    global _groq_cache

    # Check cache antes del lock (fast path)
    if _groq_cache is not None:
        delta, ts = _groq_cache
        if time.monotonic() - ts < _GROQ_CACHE_TTL_S:
            log.debug("[sentiment] Groq macro cache hit: %+.1f", delta)
            return delta

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return 0.0

    async with _groq_lock:
        # Double-check tras lock
        if _groq_cache is not None:
            delta, ts = _groq_cache
            if time.monotonic() - ts < _GROQ_CACHE_TTL_S:
                return delta

        headlines = await _fetch_macro_headlines(session)
        if not headlines:
            _groq_cache = (0.0, time.monotonic())
            return 0.0

        hl_text = "\n".join(f"- {h}" for h in headlines)
        prompt = (
            "Analiza estos titulares crypto y devuelve el sentimiento MACRO del mercado "
            "a corto plazo (12-24h).\n\n"
            f"{hl_text}\n\n"
            "Responde SOLO con JSON sin markdown:\n"
            '{"score_delta": <-2.0 a 2.0>, "reason": "<1 frase>"}\n'
            "Escala: +2 muy alcista, 0 neutro, -2 muy bajista."
        )
        try:
            async with session.post(
                _GROQ_API_URL,
                headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                json={"model": _GROQ_MODEL, "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 60, "temperature": 0.1},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    _groq_cache = (0.0, time.monotonic())
                    return 0.0
                data = await resp.json()

            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = "\n".join(
                    l for l in content.splitlines() if not l.strip().startswith("```")
                ).strip()
            parsed = json.loads(content)
            delta  = max(-2.0, min(2.0, float(parsed.get("score_delta", 0.0))))
            log.info("[sentiment] Groq macro → %+.1f | %s", delta, parsed.get("reason", "")[:120])
            _groq_cache = (delta, time.monotonic())
            return delta
        except Exception as e:
            log.warning("[sentiment] Groq macro error: %s", e)
            _groq_cache = (0.0, time.monotonic())
            return 0.0


# ── Score combinado ───────────────────────────────────────────────────────────

async def sentiment_gate_check() -> tuple[bool, str, bool]:
    """
    Punto de entrada para decision_engine.

    Returns: (allowed, reason, full_size)
      allowed   → True si el sentimiento permite abrir
      reason    → string para log/Telegram
      full_size → True = size completo | False = reducir al 50%

    Llamadas a Groq:
      - F&G <= FNG_GROQ_LO (25): NUNCA llama Groq (Extreme Fear, decide solo)
      - F&G >= FNG_GROQ_HI (75): NUNCA llama Groq (Extreme Greed, decide solo)
      - 25 < F&G < 75: llama Groq 1 vez, cache 2h (máx ~12 llamadas/día)
    """
    if not SENTIMENT_GATE:
        return True, "sentiment_gate=OFF", True

    try:
        async with aiohttp.ClientSession() as session:
            fng = await _fetch_fear_greed(session)

            # Zona extrema: F&G decide solo, 0 tokens Groq
            if fng <= _FNG_GROQ_LO:
                # Extreme Fear → score basado solo en F&G
                score = fng  # ej: 15 → score=15 → casi siempre < OPEN_MIN → bloquea
                groq_delta = None
                log.info("[sentiment] F&G=%.0f ≤ %.0f (Extreme Fear) → sin Groq, score=%.0f",
                         fng, _FNG_GROQ_LO, score)
            elif fng >= _FNG_GROQ_HI:
                # Extreme Greed → score basado solo en F&G
                score = fng  # ej: 85 → score=85 → full size
                groq_delta = None
                log.info("[sentiment] F&G=%.0f ≥ %.0f (Extreme Greed) → sin Groq, score=%.0f",
                         fng, _FNG_GROQ_HI, score)
            else:
                # Zona ambigua: llamar Groq para desempatar (con cache 2h)
                groq_delta = await _groq_macro(session)
                groq_norm  = (groq_delta + 2.0) / 4.0 * 100.0  # [-2,+2] → [0,100]
                score = 0.60 * fng + 0.40 * groq_norm
                log.info("[sentiment] F&G=%.0f (ambiguo) + Groq=%+.1f → score=%.0f",
                         fng, groq_delta, score)

        score     = max(0.0, min(100.0, score))
        allowed   = score >= SENTIMENT_OPEN_MIN
        full_size = score >= SENTIMENT_SIZE_BOOST

        if fng <= 20:       fng_label = "Extreme Fear"
        elif fng <= 40:     fng_label = "Fear"
        elif fng <= 60:     fng_label = "Neutral"
        elif fng <= 80:     fng_label = "Greed"
        else:               fng_label = "Extreme Greed"

        groq_str = f" | Groq={groq_delta:+.1f}" if groq_delta is not None else ""
        reason   = f"F&G={fng:.0f} ({fng_label}){groq_str} | score={score:.0f}/100"

        log.info(
            "[sentiment] %s → %s size=%s",
            reason,
            "✅ OPEN" if allowed else "🚫 BLOCK",
            "full" if full_size else "50%",
        )
        return allowed, reason, full_size

    except Exception as e:
        log.warning("[sentiment] sentiment_gate_check error (fail-open): %s", e)
        return True, f"sentiment error ({e}) — fail-open", True
