import logging
import os
import json
import asyncio
import aiohttp
from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AIFilter")

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


async def ai_rank_pairs(pairs_data: list) -> list:
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return [p["symbol"] for p in pairs_data]

    # Verificar budget antes de llamar
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
                data = await resp.json()
                content = data["choices"][0]["message"]["content"].strip()
                return json.loads(content)
    except Exception as e:
        logger.warning(f"AI filter falló ({e})")
        return [p["symbol"] for p in pairs_data]
