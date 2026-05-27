import logging
import os
import json
import aiohttp
from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AIFilter")

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


def _clean_json_response(content: str) -> str:
    """Elimina markdown fences y texto extra que Groq a veces añade."""
    content = content.strip()
    # Quitar ```json ... ``` o ``` ... ```
    if content.startswith("```"):
        lines = content.splitlines()
        # Eliminar primera y última línea si son fences
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    return content


async def ai_rank_pairs(pairs_data: list) -> list:
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return [p["symbol"] for p in pairs_data]

    if not await budget.can_call_groq():
        logger.warning("AIFilter: budget Groq agotado — devolviendo orden por score")
        return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]

    summary = json.dumps([
        {
            "symbol": p["symbol"],
            "volume_M_usdt": p["volume_usdt"],
            "change_24h_pct": p["change_pct"],
            "score": p["score"],
        }
        for p in pairs_data
    ], indent=2)

    prompt = f"""Eres un experto en trading de futuros de criptomonedas.
Analiza estos pares y ordénalos de mejor a peor oportunidad.
Responde ÚNICAMENTE con un JSON array de símbolos.
Ejemplo: ["BTCUSDT", "SOLUSDT"]

DATOS:\n{summary}"""

    try:
        async with budget.groq_semaphore:
            await budget.register_groq_call()
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    GROQ_API_URL,
                    headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                    json={
                        "model": GROQ_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 300,
                        "temperature": 0.1,
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                if resp.status == 429:
                    logger.warning("AIFilter: Groq 429 — devolviendo orden por score")
                    return [p["symbol"] for p in sorted(pairs_data, key=lambda x: x.get("score", 0), reverse=True)]
                data    = await resp.json()
                content = data["choices"][0]["message"]["content"]
                content = _clean_json_response(content)
                ranked  = json.loads(content)
                # Validar que es una lista de strings
                if not isinstance(ranked, list) or not all(isinstance(s, str) for s in ranked):
                    raise ValueError(f"Formato inesperado: {ranked!r}")
                return ranked
    except Exception as e:
        logger.warning(f"AI filter falló ({e}) — devolviendo orden original")
        return [p["symbol"] for p in pairs_data]
