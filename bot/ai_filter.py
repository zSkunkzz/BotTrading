"""
ai_filter.py — Filtro de noticias via IA.

Única responsabilidad: consultar a Groq sobre noticias recientes de un símbolo
y devolver un score_delta en el rango [-2.0, +2.0] para sumar al score técnico
del pair_scanner.

Comportamiento:
  - score_delta > 0: noticias positivas/alcistas → aumenta prioridad del par
  - score_delta < 0: noticias negativas/bajistas → reduce prioridad del par
  - score_delta = 0: sin noticias relevantes o Groq no disponible (fallback seguro)

Integración:
  Llamar a news_score_adjustment(symbol) desde pair_scanner.py al calcular
  el score final de cada par. El resultado se suma al score técnico antes
  de ordenar los pares candidatos.

Variables de entorno:
  GROQ_API_KEY   — clave de API de Groq (si no está definida, siempre devuelve 0.0)
  GROQ_MODEL     — modelo a usar (default: llama-3.1-8b-instant)
"""

import json
import logging
import os

import aiohttp

from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AIFilter")

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

_SCORE_MIN = -2.0
_SCORE_MAX =  2.0


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
    Consulta a Groq sobre noticias recientes del símbolo y devuelve un delta
    de score en [-2.0, +2.0].

    - Devuelve 0.0 si Groq no está disponible, el budget está agotado,
      la respuesta es inválida, o se produce cualquier error.
    - Nunca lanza excepciones: fallo silencioso con fallback a 0.0.

    Args:
        symbol: símbolo de trading, ej. "BTC", "ETH", "SOL"

    Returns:
        float en [-2.0, +2.0] — delta a sumar al score técnico
    """
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return 0.0

    try:
        if not await budget.can_call_groq():
            logger.debug("[AIFilter] Budget Groq agotado para %s — score_delta=0", symbol)
            return 0.0
    except Exception:
        return 0.0

    # Limpiar sufijo de par si viene con USDT/USDC
    base = symbol.replace("USDT", "").replace("USDC", "").replace("-PERP", "").upper()

    prompt = (
        f"Eres un analista de criptomonedas. Tu tarea es evaluar el impacto de noticias "
        f"recientes sobre {base} en su precio a corto plazo (próximas 4-24 horas).\n\n"
        f"Considera: anuncios de proyecto, regulaciones, hacks, listings/delistings, "
        f"partnerships, cambios macroeconómicos relevantes para crypto, o cualquier "
        f"noticia que pueda afectar el precio de {base}.\n\n"
        f"Responde ÚNICAMENTE con un JSON con esta estructura exacta:\n"
        f'{{"score_delta": <número entre -2.0 y 2.0>, "reason": "<resumen en 1 frase>"}}'\n\n"
        f"Escala del score_delta:\n"
        f"  +2.0 = noticias muy positivas/alcistas\n"
        f"  +1.0 = noticias moderadamente positivas\n"
        f"   0.0 = sin noticias relevantes o noticias neutras\n"
        f"  -1.0 = noticias moderadamente negativas\n"
        f"  -2.0 = noticias muy negativas/bajistas\n"
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
                        "max_tokens": 120,
                        "temperature": 0.1,
                    },
                    timeout=aiohttp.ClientTimeout(total=12),
                )

                if resp.status == 429:
                    logger.warning("[AIFilter] Groq 429 para %s — score_delta=0", symbol)
                    return 0.0

                if resp.status != 200:
                    logger.warning("[AIFilter] Groq HTTP %d para %s — score_delta=0", resp.status, symbol)
                    return 0.0

                data    = await resp.json()
                content = data["choices"][0]["message"]["content"]
                content = _clean_json_response(content)
                parsed  = json.loads(content)

                if not isinstance(parsed, dict) or "score_delta" not in parsed:
                    logger.warning("[AIFilter] Respuesta inesperada de Groq para %s: %r", symbol, parsed)
                    return 0.0

                delta  = float(parsed["score_delta"])
                reason = parsed.get("reason", "")

                # Clampear al rango permitido
                delta = max(_SCORE_MIN, min(_SCORE_MAX, delta))

                if abs(delta) >= 0.5:
                    logger.info(
                        "[AIFilter] %s — score_delta=%+.1f | %s",
                        symbol, delta, reason[:120]
                    )
                else:
                    logger.debug("[AIFilter] %s — score_delta=%+.1f (neutro)", symbol, delta)

                return delta

    except RateLimitExhausted:
        logger.debug("[AIFilter] RateLimitExhausted para %s — score_delta=0", symbol)
        return 0.0
    except json.JSONDecodeError as e:
        logger.warning("[AIFilter] JSON inválido de Groq para %s: %s", symbol, e)
        return 0.0
    except asyncio.TimeoutError:
        logger.warning("[AIFilter] Timeout Groq para %s — score_delta=0", symbol)
        return 0.0
    except Exception as e:
        logger.warning("[AIFilter] Error inesperado para %s: %s — score_delta=0", symbol, e)
        return 0.0


# ---------------------------------------------------------------------------
# LEGACY — mantenido por compatibilidad pero NO se llama desde ningún módulo activo
# ---------------------------------------------------------------------------

async def ai_rank_pairs(pairs_data: list) -> list:
    """
    DEPRECADO. Devuelve los pares ordenados por score sin llamar a Groq.
    Usar news_score_adjustment() por símbolo en pair_scanner.py en su lugar.
    """
    logger.debug("[AIFilter] ai_rank_pairs() llamado (legacy) — devolviendo orden por score sin IA")
    return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]
