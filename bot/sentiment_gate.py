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

Lógica de sizing (NUNCA bloquea — solo escala el size):
  El sentimiento ya NO bloquea ninguna entrada, ni LONG ni SHORT.
  En su lugar, el score 0-100 se traduce en un size_multiplier:

    score >= SENTIMENT_SIZE_BOOST (default 65) → size_multiplier = 1.00 (full)
    score >= SENTIMENT_SIZE_MID   (default 50) → size_multiplier = 0.75
    score >= SENTIMENT_OPEN_MIN   (default 35) → size_multiplier = 0.50
    score <  SENTIMENT_OPEN_MIN               → size_multiplier = 0.35 (mínimo)

  allowed siempre es True — el gate nunca veta una señal técnica.
  full_size=True solo cuando size_multiplier == 1.0.

Llamadas a Groq:
  - F&G <= FNG_GROQ_LO (25): NUNCA llama Groq
  - F&G >= FNG_GROQ_HI (75): NUNCA llama Groq
  - 25 < F&G < 75: llama Groq 1 vez, cache 2h (máx ~12 llamadas/día)

Variables de entorno Railway:
  SENTIMENT_GATE=true/false       (default: true)
  SENTIMENT_OPEN_MIN=35           umbral mínimo → size_multiplier 0.35
  SENTIMENT_SIZE_BOOST=65         umbral full size → size_multiplier 1.0
  FNG_GROQ_LO=25                  por debajo: Extreme Fear → score = F&G
  FNG_GROQ_HI=75                  por encima: Extreme Greed → score = F&G
  GROQ_MACRO_CACHE_TTL_H=2        TTL cache Groq en horas
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

# ── Configuración ──────────────────────────────────────────────────────
SENTIMENT_GATE       = os.getenv("SENTIMENT_GATE",    "true").lower() != "false"
SENTIMENT_OPEN_MIN   = float(os.getenv("SENTIMENT_OPEN_MIN",   "35"))
SENTIMENT_SIZE_BOOST = float(os.getenv("SENTIMENT_SIZE_BOOST", "65"))
_SENTIMENT_SIZE_MID  = float(os.getenv("SENTIMENT_SIZE_MID",   "50"))
_FNG_GROQ_LO         = float(os.getenv("FNG_GROQ_LO", "25"))  # por debajo: Extreme Fear
_FNG_GROQ_HI         = float(os.getenv("FNG_GROQ_HI", "75"))  # por encima: Extreme Greed
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


def _score_to_size_multiplier(score: float) -> float:
    """Convierte score 0-100 en size_multiplier. Nunca devuelve 0."""
    if score >= SENTIMENT_SIZE_BOOST:
        return 1.00
    elif score >= _SENTIMENT_SIZE_MID:
        return 0.75
    elif score >= SENTIMENT_OPEN_MIN:
        return 0.50
    else:
        return 0.35


# ── Score combinado ───────────────────────────────────────────────────────────

async def sentiment_gate_check(
    side: Optional[str] = None,
) -> tuple[bool, str, bool]:
    """
    Punto de entrada para decision_engine.

    IMPORTANTE: El gate NUNCA bloquea una entrada.
    Solo calcula el size_multiplier basado en el score de sentimiento.

    Args:
        side: "LONG", "SHORT", "long", "short" o None.
              Ya no afecta al resultado (lógica direccional eliminada).
              Se mantiene el parámetro por compatibilidad con decision_engine.

    Returns: (allowed, reason, full_size)
      allowed    → SIEMPRE True (el gate nunca veta)
      reason     → string para log/Telegram con score y size_multiplier
      full_size  → True = size completo (1.0x) | False = size reducido

    El size_multiplier real se expone en el campo 'reason' para trazabilidad.
    El decision_engine puede leer full_size para ajustar el sizing:
      full_size=True  → usar USDC_PER_TRADE al 100%
      full_size=False → reducir según el multiplier embebido en reason

    Llamadas a Groq:
      - F&G <= FNG_GROQ_LO (25): NUNCA llama Groq
      - F&G >= FNG_GROQ_HI (75): NUNCA llama Groq
      - 25 < F&G < 75: llama Groq 1 vez, cache 2h (máx ~12 llamadas/día)
    """
    if not SENTIMENT_GATE:
        return True, "sentiment_gate=OFF", True

    groq_delta: float | None = None

    try:
        async with aiohttp.ClientSession() as session:
            fng = await _fetch_fear_greed(session)

            # ── Zona extrema: score = F&G directamente, sin Groq ──────────
            if fng <= _FNG_GROQ_LO:
                score = fng
                log.info(
                    "[sentiment] F&G=%.0f ≤ %.0f (Extreme Fear) → sin Groq, score=%.0f",
                    fng, _FNG_GROQ_LO, score,
                )
            elif fng >= _FNG_GROQ_HI:
                score = fng
                log.info(
                    "[sentiment] F&G=%.0f ≥ %.0f (Extreme Greed) → sin Groq, score=%.0f",
                    fng, _FNG_GROQ_HI, score,
                )
            else:
                # ── Zona ambigua: llamar Groq para desempatar (cache 2h) ──
                groq_delta = await _groq_macro(session)
                groq_norm  = (groq_delta + 2.0) / 4.0 * 100.0  # [-2,+2] → [0,100]
                score = 0.60 * fng + 0.40 * groq_norm
                log.info(
                    "[sentiment] F&G=%.0f (ambiguo) + Groq=%+.1f → score=%.0f",
                    fng, groq_delta, score,
                )

        score = max(0.0, min(100.0, score))
        size_mult = _score_to_size_multiplier(score)
        full_size = size_mult >= 1.0

        if fng <= 20:       fng_label = "Extreme Fear"
        elif fng <= 40:     fng_label = "Fear"
        elif fng <= 60:     fng_label = "Neutral"
        elif fng <= 80:     fng_label = "Greed"
        else:               fng_label = "Extreme Greed"

        groq_str  = f" | Groq={groq_delta:+.1f}" if groq_delta is not None else ""
        reason    = (
            f"F&G={fng:.0f} ({fng_label}){groq_str} | score={score:.0f}/100"
            f" | size={size_mult:.0%}"
        )

        log.info(
            "[sentiment] %s → ✅ OPEN size=%s",
            reason,
            f"{size_mult:.0%}",
        )
        # allowed siempre True — el técnico manda, el sentimiento solo pondera el size
        return True, reason, full_size

    except Exception as e:
        log.warning("[sentiment] sentiment_gate_check error (fail-open): %s", e)
        return True, f"sentiment error ({e}) — fail-open", True
