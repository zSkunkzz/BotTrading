import logging
import os
import re
import json
import time
import asyncio
import aiohttp
from bot.indicators import ema, rsi, macd, supertrend, atr
from bot.data_enricher import fetch_enriched_context, format_context_for_prompt
from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AITrader")

GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GEMINI_URL   = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

CACHE_TTL     = int(os.getenv("AI_CACHE_TTL",     "300"))
CACHE_TTL_BUY = int(os.getenv("AI_CACHE_TTL_BUY", "300"))
_ai_cache: dict = {}

ENRICHED_CACHE_TTL = int(os.getenv("ENRICHED_CACHE_TTL", "120"))
_enriched_cache: dict = {}

AI_MIN_SCORE = int(os.getenv("AI_MIN_SCORE", "7"))

# Bug N fix: sesiones de módulo reutilizables en lugar de crear una nueva por llamada.
# Lazy-init con _get_*_session() para evitar crear la sesión fuera de un event loop.
_gemini_session: aiohttp.ClientSession | None = None
_groq_session:   aiohttp.ClientSession | None = None


def _get_gemini_session() -> aiohttp.ClientSession:
    """Devuelve (creando si hace falta) la sesión persistente para Gemini."""
    global _gemini_session
    if _gemini_session is None or _gemini_session.closed:
        _gemini_session = aiohttp.ClientSession()
    return _gemini_session


def _get_groq_session() -> aiohttp.ClientSession:
    """Devuelve (creando si hace falta) la sesión persistente para Groq."""
    global _groq_session
    if _groq_session is None or _groq_session.closed:
        _groq_session = aiohttp.ClientSession()
    return _groq_session


async def close_sessions() -> None:
    """Cierra las sesiones HTTP de módulo. Llamar al shutdown del bot."""
    global _gemini_session, _groq_session
    for sess in (_gemini_session, _groq_session):
        if sess is not None and not sess.closed:
            await sess.close()
    _gemini_session = None
    _groq_session   = None


# ── Prompts ───────────────────────────────────────────────────────────────────

# Prompt principal: análisis técnico + gestión de posición abierta (BUY/SELL/HOLD/CLOSE).
# Bug I fix: la IA SIEMPRE se consulta, tanto en entrada como con posición abierta.
# Las reglas numéricas (RSI, funding, F&G, OI, vol_ratio) las aplica enriched_filter
# antes de llegar aquí, así que este prompt NO las duplica.
SYSTEM_PROMPT = """You are a professional crypto futures trader. Reply ONLY with a JSON object, no extra text.

You receive:
  1. Technical indicators across multiple timeframes
  2. Current open position details (side, entry, PnL) when applicable

Decision rules:
  - If no position: BUY for long setup, SELL for short setup, HOLD if unclear
  - If position open: CLOSE if pnl > +3% or pnl < -1.5%, else HOLD
  - If unsure → HOLD

JSON format (strict, no markdown):
{"action":"BUY","confidence":8,"reason":"short reason max 15 words"}
action must be one of: BUY SELL HOLD CLOSE"""

# Prompt reducido para análisis SOLO de noticias.
# Se usa cuando strategy.py llama con task="news_sentiment_only".
# Ya NO incluye reglas de RSI/funding/OI porque enriched_filter las aplica antes.
NEWS_SYSTEM_PROMPT = """You are a crypto news sentiment analyst. Reply ONLY with a JSON object, no extra text.

You receive a trading signal direction and recent news headlines.
Your ONLY job: decide if the news sentiment is strongly against the signal.

Rules:
  - HOLD only if news are STRONGLY and CLEARLY against the signal direction
  - If news are neutral, mixed, or slightly against → do NOT block (return BUY or SELL)
  - If news confirm or are unrelated → return BUY or SELL
  - When in doubt → return BUY or SELL (do not over-block)

JSON format (strict, no markdown):
{"action":"BUY","confidence":8,"reason":"short reason max 15 words"}
action must be one of: BUY SELL HOLD"""


def _price_bucket(price: float) -> int:
    if not price or price <= 0:
        return 0
    import math
    magnitude = 10 ** math.floor(math.log10(price) - 2)
    return int(round(price / magnitude))


def _parse_ai_json(raw: str) -> dict:
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()
    if not raw:
        raise ValueError("Empty AI response")
    start = raw.find("{")
    end   = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        action_m = re.search(r'"action"\s*:\s*"(BUY|SELL|HOLD|CLOSE)"', raw)
        conf_m   = re.search(r'"confidence"\s*:\s*(\d+)', raw)
        if action_m:
            action = action_m.group(1)
            conf   = int(conf_m.group(1)) if conf_m else 5
            logger.debug(f"JSON truncado recuperado: action={action} conf={conf}")
            return {"action": action, "confidence": conf, "reason": "truncated response"}
        raise ValueError(f"No JSON found: {raw[:120]!r}")
    return json.loads(raw[start:end + 1])


async def _get_enriched_context(symbol: str):
    # Bug J fix: usar time.time() (epoch) en lugar de time.monotonic().
    # time.monotonic() no es persistible entre sesiones; tras un restart de Railway
    # un timestamp monotonic antiguo puede ser > al actual → age negativo →
    # la condición age < TTL es siempre True → respuestas obsoletas servidas ∞.
    now = time.time()
    cached = _enriched_cache.get(symbol)
    if cached:
        ctx, ts = cached
        if (now - ts) < ENRICHED_CACHE_TTL:
            return ctx
    ctx = await fetch_enriched_context(symbol)
    _enriched_cache[symbol] = (ctx, now)
    if len(_enriched_cache) > 50:
        oldest = min(_enriched_cache, key=lambda k: _enriched_cache[k][1])
        del _enriched_cache[oldest]
    return ctx


def build_market_context(symbol, bars, position, entry_price, leverage,
                         enriched_str: str = ""):
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    vols   = [b[5] for b in bars]

    ema21            = ema(closes, 21)
    ema50            = ema(closes, 50)
    rsi14            = rsi(closes, 14)
    m_line, s_line, hist = macd(closes, 12, 26, 9)
    st_dir, _        = supertrend(highs, lows, closes, 10, 3.0)
    atr14            = atr(highs, lows, closes, 14)
    avg_vol          = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    vol_ratio        = round(vols[-1] / avg_vol, 2) if avg_vol > 0 else 1
    last_3 = [
        {"o": bars[i][1], "h": bars[i][2], "l": bars[i][3],
         "c": bars[i][4], "v": round(bars[i][5], 2)}
        for i in range(-3, 0)
    ]
    ctx = {
        "sym":  symbol,
        "px":   closes[-1],
        "ema":  "UP" if (ema21 and ema50 and ema21[-1] > ema50[-1]) else "DOWN",
        "rsi":  rsi14,
        "macd": "UP" if hist > 0 else "DOWN",
        "st":   "UP" if st_dir == 1 else "DOWN",
        "atr":  round(atr14, 4),
        "vr":   vol_ratio,
        "c3":   last_3,
        "pos":  position or "NONE",
        "ep":   entry_price,
        "lev":  leverage,
    }
    if position and entry_price:
        if position == "long":
            pnl = (closes[-1] - entry_price) / entry_price * 100 * leverage
        else:
            pnl = (entry_price - closes[-1]) / entry_price * 100 * leverage
        ctx["pnl"] = round(pnl, 2)

    if enriched_str:
        ctx["external"] = enriched_str

    return ctx


async def _call_gemini(context: dict, system_prompt: str = SYSTEM_PROMPT):
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        return None
    try:
        if not await budget.can_call_gemini():
            raise RateLimitExhausted("gemini")
        async with budget.gemini_semaphore:
            await budget.register_gemini_call()
            url      = GEMINI_URL.format(model=GEMINI_MODEL) + f"?key={key}"
            data_str = json.dumps(context, ensure_ascii=False, separators=(',', ':'))
            prompt   = f"{system_prompt}\nDATA:{data_str}"

            # Bug N fix: reutilizar sesión de módulo en lugar de crear una nueva
            session = _get_gemini_session()
            for attempt in range(1, 4):
                try:
                    resp = await session.post(
                        url,
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {
                                "temperature":     0.0,
                                "maxOutputTokens": 256,
                            },
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    )
                except aiohttp.ClientError:
                    # Sesión pudo cerrarse inesperadamente — recrear y reintentar
                    _gemini_session = None
                    session = _get_gemini_session()
                    if attempt < 3:
                        await asyncio.sleep(2 * attempt)
                        continue
                    return None

                if resp.status == 503:
                    logger.warning(f"Gemini 503 intento {attempt}/3")
                    if attempt < 3:
                        await asyncio.sleep(3 * attempt)
                        continue
                    return None
                if resp.status == 429:
                    logger.warning("Gemini 429 rate limit")
                    return None
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Gemini HTTP {resp.status}: {body[:200]}")
                    return None

                data  = await resp.json()
                cands = data.get("candidates", [])
                if not cands:
                    logger.warning("Gemini: sin candidates")
                    return None
                finish = cands[0].get("finishReason", "STOP")
                if finish not in ("STOP", "MAX_TOKENS"):
                    logger.warning(f"Gemini finishReason={finish}")
                    return None
                raw = cands[0]["content"]["parts"][0]["text"]
                try:
                    result = _parse_ai_json(raw)
                    logger.debug(f"Gemini OK: {raw[:80]}")
                    return result
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Gemini JSON error: {e} | raw={raw!r}")
                    return None
    except RateLimitExhausted as e:
        logger.warning(f"[Gemini] {e}")
        return None
    except Exception as e:
        logger.warning(f"Gemini error: {e}")
        return None


async def _call_groq(context: dict, system_prompt: str = SYSTEM_PROMPT):
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        return None
    try:
        if not await budget.can_call_groq():
            raise RateLimitExhausted("groq")
        async with budget.groq_semaphore:
            await budget.register_groq_call()
            # Bug N fix: reutilizar sesión de módulo en lugar de crear una nueva
            session = _get_groq_session()
            try:
                resp = await session.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={
                        "model": GROQ_MODEL,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user",   "content": json.dumps(context, separators=(',', ':'))},
                        ],
                        "max_tokens": 128,
                        "temperature": 0.0,
                        "response_format": {"type": "json_object"},
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                )
            except aiohttp.ClientError:
                # Sesión pudo cerrarse — recrear y reintentar una vez
                global _groq_session
                _groq_session = None
                session = _get_groq_session()
                resp = await session.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={
                        "model": GROQ_MODEL,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user",   "content": json.dumps(context, separators=(',', ':'))},
                        ],
                        "max_tokens": 128,
                        "temperature": 0.0,
                        "response_format": {"type": "json_object"},
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                )

            if resp.status == 429:
                logger.warning("Groq 429 rate limit")
                return None
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"Groq HTTP {resp.status}: {body[:200]}")
                return None

            data    = await resp.json()
            choices = data.get("choices", [])
            if not choices:
                logger.warning("Groq: sin choices")
                return None
            raw = choices[0].get("message", {}).get("content", "")
            try:
                result = _parse_ai_json(raw)
                logger.debug(f"Groq OK: {raw[:80]}")
                return result
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Groq JSON error: {e} | raw={raw!r}")
                return None
    except RateLimitExhausted as e:
        logger.warning(f"[Groq] {e}")
        return None
    except Exception as e:
        logger.warning(f"Groq error: {e}")
        return None


async def analyze(
    symbol: str,
    bars: list,
    position: str | None = None,
    entry_price: float | None = None,
    leverage: int = 1,
    task: str = "full",
) -> dict:
    """
    Punto de entrada principal del módulo.

    Bug I fix: la IA SIEMPRE se consulta, con o sin posición abierta.
    Antes, con posición abierta se hardcodeaba CLOSE/HOLD sin consultar la IA,
    haciendo que SYSTEM_PROMPT y toda la lógica de CLOSE por IA fueran código muerto.
    Ahora el flujo Gemini→Groq→fallback se ejecuta siempre.

    task:
      "full"               → análisis completo (indicadores + contexto enriquecido)
      "news_sentiment_only" → solo análisis de noticias (usa NEWS_SYSTEM_PROMPT)
    """
    # Bug J fix: usar time.time() (epoch) para el cache de IA.
    # time.monotonic() anterior causaba age negativo tras restarts de Railway.
    now        = time.time()
    cache_key  = (symbol, _price_bucket(bars[-1][4] if bars else 0), position, task)
    cached     = _ai_cache.get(cache_key)
    if cached:
        result, ts = cached
        age = now - ts
        ttl = CACHE_TTL_BUY if (result or {}).get("action") in ("BUY", "SELL") else CACHE_TTL
        if age < ttl:
            logger.debug(f"[{symbol}] AI cache hit (age={age:.0f}s)")
            return result

    # Selección de prompt según task
    if task == "news_sentiment_only":
        system_prompt = NEWS_SYSTEM_PROMPT
    else:
        system_prompt = SYSTEM_PROMPT

    # Contexto enriquecido (noticias, funding, F&G, etc.)
    enriched_str = ""
    if task != "news_sentiment_only":
        try:
            enriched_data = await _get_enriched_context(symbol)
            if enriched_data:
                enriched_str = format_context_for_prompt(enriched_data)
        except Exception as e:
            logger.debug(f"[{symbol}] enriched context error (non-fatal): {e}")

    context = build_market_context(
        symbol, bars, position, entry_price, leverage, enriched_str
    )

    # Bug I fix: consultar la IA siempre (Gemini primero, Groq como fallback).
    # Antes el código salía antes de llegar aquí cuando había posición abierta.
    result = await _call_gemini(context, system_prompt=system_prompt)
    if result is None:
        result = await _call_groq(context, system_prompt=system_prompt)

    if result is None:
        # Fallback conservador: HOLD si hay posición, HOLD si no hay señal clara
        result = {"action": "HOLD", "confidence": 0, "reason": "no AI response"}
        logger.warning(f"[{symbol}] AI: ambos modelos fallaron — fallback HOLD")

    # Validar score mínimo de confianza
    confidence = result.get("confidence", 0)
    action     = result.get("action", "HOLD")
    if action in ("BUY", "SELL") and confidence < AI_MIN_SCORE:
        logger.info(
            f"[{symbol}] AI: {action} con confidence={confidence} < "
            f"AI_MIN_SCORE={AI_MIN_SCORE} → downgrade a HOLD"
        )
        result = {**result, "action": "HOLD", "reason": f"confidence {confidence} < {AI_MIN_SCORE}"}

    _ai_cache[cache_key] = (result, now)
    # Limitar tamaño del cache
    if len(_ai_cache) > 200:
        oldest = min(_ai_cache, key=lambda k: _ai_cache[k][1])
        del _ai_cache[oldest]

    logger.info(f"[{symbol}] AI: {result.get('action')} conf={result.get('confidence')} — {result.get('reason', '')}")
    return result
