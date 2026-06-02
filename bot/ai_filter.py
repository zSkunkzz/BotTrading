"""
ai_filter.py — Filtro de noticias via IA.

Única responsabilidad: consultar a Groq sobre noticias recientes de un símbolo
y devolver un score_delta en el rango [-2.0, +2.0] para sumar al score técnico
del pair_scanner.

Comportamiento:
  - score_delta > 0  : noticias positivas/alcistas → aumenta prioridad del par
  - score_delta < 0  : noticias negativas/bajistas  → reduce prioridad del par
  - score_delta = 0  : sin noticias relevantes, Groq no disponible, o cualquier fallo

Caché en memoria (2026-06-02):
  Los resultados se cachean por símbolo durante AI_NEWS_CACHE_TTL_H horas
  (por defecto 4h). Mientras el resultado está en caché, news_score_adjustment()
  devuelve el valor almacenado sin llamar a Groq. Solo se realiza una nueva
  llamada cuando el TTL ha expirado o el símbolo es nuevo.

  Esto evita el patrón anterior de llamar a Groq para N pares en cada ciclo
  de scan (cada 30 min), que consumía budget innecesariamente.

Flujo de ahorro de budget (2026-06-02):
  El prompt pide has_news: bool. Si Groq responde has_news=false, se devuelve
  0.0 inmediatamente sin consumir mas tokens en analisis. Solo se procesa
  score_delta cuando hay noticias reales que impacten el precio.

Variables de entorno:
  GROQ_API_KEY          — clave de API de Groq (si no esta definida, siempre devuelve 0.0)
  GROQ_MODEL            — modelo a usar (default: llama-3.1-8b-instant)
  AI_NEWS_CACHE_TTL_H   — horas de validez del cache por simbolo (default: 4)
"""

import asyncio
import json
import logging
import os
import time

import aiohttp

from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AIFilter")

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

_SCORE_MIN = -2.0
_SCORE_MAX =  2.0

# Cache en memoria: {symbol: (score_delta, timestamp_unix)}
_NEWS_CACHE: dict[str, tuple[float, float]] = {}
_CACHE_TTL_S: float = float(os.getenv("AI_NEWS_CACHE_TTL_H", "4")) * 3600


def _get_cached(symbol: str) -> float | None:
    """Devuelve el score_delta cacheado si está vigente, o None si expiró/no existe."""
    entry = _NEWS_CACHE.get(symbol)
    if entry is None:
        return None
    delta, ts = entry
    if time.monotonic() - ts < _CACHE_TTL_S:
        return delta
    # Expirado — eliminar
    del _NEWS_CACHE[symbol]
    return None


def _set_cached(symbol: str, delta: float) -> None:
    _NEWS_CACHE[symbol] = (delta, time.monotonic())


def cache_stats() -> dict:
    """Devuelve estadísticas del caché para debugging."""
    now = time.monotonic()
    valid = sum(1 for _, ts in _NEWS_CACHE.values() if now - ts < _CACHE_TTL_S)
    return {"total": len(_NEWS_CACHE), "valid": valid, "ttl_h": _CACHE_TTL_S / 3600}


def _clean_json_response(content: str) -> str:
    """Elimina markdown fences que Groq a veces añade."""
    content = content.strip()
    if content.startswith("```"):
        lines = content.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    return content


async def news_score_adjustment(symbol: str) -> float:
    """
    Consulta a Groq sobre noticias recientes del simbolo y devuelve un delta
    de score en [-2.0, +2.0].

    Caché en memoria con TTL configurable (AI_NEWS_CACHE_TTL_H, default 4h).
    Si el resultado está en caché y no ha expirado, se devuelve directamente
    sin llamar a Groq. Esto evita N llamadas por ciclo de scan.

    Protocolo de ahorro de budget:
      1. Groq responde con has_news: bool en el mismo JSON.
      2. Si has_news=false  -> devuelve 0.0 inmediatamente (min tokens usados).
      3. Si has_news=true   -> lee score_delta y reason del mismo JSON.

    Nunca lanza excepciones: fallo silencioso con fallback a 0.0.

    Args:
        symbol: simbolo de trading, ej. "BTC", "ETH", "SOL"

    Returns:
        float en [-2.0, +2.0] — delta a sumar al score tecnico
    """
    # Limpiar sufijo de par si viene con USDT/USDC
    base = symbol.replace("USDT", "").replace("USDC", "").replace("-PERP", "").upper()

    # ── Caché hit ────────────────────────────────────────────────────────────
    cached = _get_cached(base)
    if cached is not None:
        logger.debug("[AIFilter] %s — cache hit, score_delta=%+.1f", base, cached)
        return cached
    # ─────────────────────────────────────────────────────────────────────────

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return 0.0

    try:
        if not await budget.can_call_groq():
            logger.debug("[AIFilter] Budget Groq agotado para %s — score_delta=0", base)
            return 0.0
    except Exception:
        return 0.0

    prompt = (
        f"Eres un analista de criptomonedas. Evalua si existen noticias recientes "
        f"relevantes sobre {base} que puedan impactar su precio en las proximas 4-24h.\n\n"
        f"Considera: anuncios de proyecto, regulaciones, hacks, listings/delistings, "
        f"partnerships, cambios macro relevantes para crypto, o eventos que afecten "
        f"directamente a {base}.\n\n"
        f"Responde UNICAMENTE con este JSON:\n"
        f'{{"has_news": <true|false>, "score_delta": <-2.0 a 2.0, o 0 si has_news=false>, "reason": "<1 frase o vacio si has_news=false>"}}\n\n'
        f"Escala score_delta (solo si has_news=true):\n"
        f"  +2.0 = noticias muy positivas/alcistas\n"
        f"  +1.0 = noticias moderadamente positivas\n"
        f"  -1.0 = noticias moderadamente negativas\n"
        f"  -2.0 = noticias muy negativas/bajistas\n"
        f"Si no hay noticias relevantes: has_news=false, score_delta=0, reason vacio."
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
                        "max_tokens": 80,
                        "temperature": 0.1,
                    },
                    timeout=aiohttp.ClientTimeout(total=12),
                )

                if resp.status == 429:
                    logger.warning("[AIFilter] Groq 429 para %s — score_delta=0", base)
                    return 0.0

                if resp.status != 200:
                    logger.warning("[AIFilter] Groq HTTP %d para %s — score_delta=0", resp.status, base)
                    return 0.0

                data    = await resp.json()
                content = data["choices"][0]["message"]["content"]
                content = _clean_json_response(content)
                parsed  = json.loads(content)

                if not isinstance(parsed, dict):
                    logger.warning("[AIFilter] Respuesta inesperada de Groq para %s: %r", base, parsed)
                    _set_cached(base, 0.0)
                    return 0.0

                # Si no hay noticias relevantes -> cachear 0.0 y devolver
                has_news = parsed.get("has_news", False)
                if not has_news:
                    logger.debug("[AIFilter] %s — [sin noticias] score_delta=0 (cacheado %gh)", base, _CACHE_TTL_S / 3600)
                    _set_cached(base, 0.0)
                    return 0.0

                # Hay noticias -> leer y validar score_delta
                if "score_delta" not in parsed:
                    logger.warning("[AIFilter] has_news=true pero sin score_delta para %s", base)
                    _set_cached(base, 0.0)
                    return 0.0

                delta  = float(parsed["score_delta"])
                reason = parsed.get("reason", "")

                # Clampear al rango permitido
                delta = max(_SCORE_MIN, min(_SCORE_MAX, delta))

                logger.info(
                    "[AIFilter] %s — \U0001f4f0 NOTICIAS score_delta=%+.1f | %s (cacheado %gh)",
                    base, delta, reason[:120], _CACHE_TTL_S / 3600
                )

                _set_cached(base, delta)
                return delta

    except RateLimitExhausted:
        logger.debug("[AIFilter] RateLimitExhausted para %s — score_delta=0", base)
        return 0.0
    except json.JSONDecodeError as e:
        logger.warning("[AIFilter] JSON invalido de Groq para %s: %s", base, e)
        return 0.0
    except asyncio.TimeoutError:
        logger.warning("[AIFilter] Timeout Groq para %s — score_delta=0", base)
        return 0.0
    except Exception as e:
        logger.warning("[AIFilter] Error inesperado para %s: %s — score_delta=0", base, e)
        return 0.0


# ---------------------------------------------------------------------------
# LEGACY — mantenido por compatibilidad pero NO se llama desde ningun modulo activo
# ---------------------------------------------------------------------------

async def ai_rank_pairs(pairs_data: list) -> list:
    """
    DEPRECADO. Devuelve los pares ordenados por score sin llamar a Groq.
    Usar news_score_adjustment() por simbolo en pair_scanner.py en su lugar.
    """
    logger.debug("[AIFilter] ai_rank_pairs() llamado (legacy) — devolviendo orden por score sin IA")
    return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]
