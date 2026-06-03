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

# TTL del caché de decisiones IA
CACHE_TTL     = int(os.getenv("AI_CACHE_TTL",     "300"))
CACHE_TTL_BUY = int(os.getenv("AI_CACHE_TTL_BUY", "300"))
_ai_cache: dict = {}

# Caché del EnrichedContext: {symbol: (EnrichedContext, timestamp)}
ENRICHED_CACHE_TTL = int(os.getenv("ENRICHED_CACHE_TTL", "120"))  # 2 min
_enriched_cache: dict = {}

# AI_MIN_SCORE: solo aplica en el path directo SIN context_override.
AI_MIN_SCORE = int(os.getenv("AI_MIN_SCORE", "7"))

SYSTEM_PROMPT = """You are a professional crypto futures trader. Reply ONLY with a JSON object, no extra text.

You receive:
  1. Technical indicators across multiple timeframes (score 0-10)
  2. External market context: Fear & Greed index, Open Interest delta, funding rate, recent news

Decision rules:
  - CLOSE if pnl > +3% or pnl < -1.5%
  - No BUY if rsi > 70 | No SELL if rsi < 30
  - vol_ratio > 1.5 confirms signal | vol_ratio < 0.7 weakens signal
  - Fear & Greed < 20 + BUY signal → reduce confidence by 1 or HOLD
  - Fear & Greed > 80 + BUY signal → euphoria risk, reduce confidence by 1
  - Funding rate > +0.05% → crowded longs, favor SELL
  - Funding rate < -0.05% → crowded shorts, favor BUY
  - OI delta > +5% with price falling → bearish pressure, favor SELL
  - OI delta > +5% with price rising → strong conviction, confirm signal
  - Bearish news + BUY signal → reduce confidence by 1
  - Bullish news + SELL signal → reduce confidence by 1
  - If unsure → HOLD

JSON format (strict, no markdown):
{"action":"BUY","confidence":8,"reason":"short reason max 15 words"}
action must be one of: BUY SELL HOLD CLOSE"""


def _price_bucket(price: float) -> int:
    """Rounds price to ~0.1% bucket to invalidate cache when market moves."""
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
    """Returns cached EnrichedContext or fetches a fresh one."""
    now = time.monotonic()
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


async def _call_gemini(context: dict):
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
            prompt   = f"{SYSTEM_PROMPT}\nDATA:{data_str}"

            for attempt in range(1, 4):
                async with aiohttp.ClientSession() as s:
                    resp = await s.post(
                        url,
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {
                                "temperature":     0.0,
                                "maxOutputTokens": 512,
                            },
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    )
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


async def _call_groq(context: dict):
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        return None
    try:
        if not await budget.can_call_groq():
            raise RateLimitExhausted("groq")
        async with budget.groq_semaphore:
            await budget.register_groq_call()
            async with aiohttp.ClientSession() as s:
                resp = await s.post(
                    GROQ_URL,
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={
                        "model": GROQ_MODEL,
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user",   "content": json.dumps(context, separators=(',', ':'))},
                        ],
                        "max_tokens": 150,
                        "temperature": 0.0,
                        "response_format": {"type": "json_object"},
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Groq HTTP {resp.status}: {body[:200]}")
                    return None
                data = await resp.json()
                raw  = data["choices"][0]["message"]["content"]
                try:
                    return _parse_ai_json(raw)
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Groq JSON error: {e} | raw={raw[:120]!r}")
                    return None
    except RateLimitExhausted as e:
        logger.warning(f"[Groq] {e}")
        return None
    except Exception as e:
        logger.warning(f"Groq error: {e}")
        return None


def _technical_signal(bars) -> dict:
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    vols   = [b[5] for b in bars]

    ema21      = ema(closes, 21)
    ema50      = ema(closes, 50)
    rsi14      = rsi(closes, 14)
    _, _, hist = macd(closes, 12, 26, 9)
    st_dir, _  = supertrend(highs, lows, closes, 10, 3.0)
    avg_vol    = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    vol_ratio  = vols[-1] / avg_vol if avg_vol > 0 else 1

    ema_bull  = ema21 and ema50 and ema21[-1] > ema50[-1]
    ema_bear  = ema21 and ema50 and ema21[-1] < ema50[-1]
    st_bull   = st_dir == 1
    st_bear   = st_dir == -1
    macd_bull = hist > 0
    macd_bear = hist < 0
    rsi_long  = rsi14 is not None and rsi14 < 65
    rsi_short = rsi14 is not None and rsi14 > 35
    vol_ok    = vol_ratio >= 0.8

    if ema_bull and st_bull and macd_bull and rsi_long  and vol_ok:
        return {"signal": "BUY",  "confidence": 7}
    if ema_bear and st_bear and macd_bear and rsi_short and vol_ok:
        return {"signal": "SELL", "confidence": 7}
    return {"signal": "HOLD", "confidence": 4}


def _pnl_check(position, entry_price, current_price, leverage) -> str | None:
    if not position or not entry_price:
        return None
    if position == "long":
        pnl = (current_price - entry_price) / entry_price * 100 * leverage
    else:
        pnl = (entry_price - current_price) / entry_price * 100 * leverage
    tp = float(os.getenv("AI_TP_PCT",  "3.0"))
    sl = float(os.getenv("AI_SL_PCT", "-1.5"))
    if pnl >= tp:  return f"TP +{pnl:.2f}%"
    if pnl <= sl:  return f"SL {pnl:.2f}%"
    return None


async def ai_decide(symbol, bars, position, entry_price, leverage,
                    context_override: dict | None = None):
    current_price = bars[-1][4] if bars else (entry_price or 0.0)

    if position:
        close_reason = _pnl_check(position, entry_price, current_price, leverage)
        if close_reason:
            logger.info(f"\U0001f4ca [{symbol}] CLOSE | {close_reason}")
            return {"action": "CLOSE", "confidence": 9, "reasoning": close_reason, "key_factors": ["pnl"]}
        return {"action": "HOLD", "confidence": 5, "reasoning": "PnL dentro de rango", "key_factors": []}

    price_bucket = _price_bucket(current_price)

    # --- Cooldown por símbolo -------------------------------------------
    if await budget.symbol_on_cooldown(symbol):
        logger.debug(f"[{symbol}] cooldown activo — HOLD sin IA")
        return {"action": "HOLD", "confidence": 4,
                "reasoning": "Cooldown activo", "key_factors": []}
    # ---------------------------------------------------------------------

    enriched     = await _get_enriched_context(symbol)
    enriched_str = format_context_for_prompt(enriched)

    # ── FAST PATH: sin noticias → ejecutar señal técnica directamente ─────
    # Si enriched.news está vacío no hay nada que la IA pueda aportar que
    # los indicadores no reflejen ya. Nos ahorramos 100% de las llamadas
    # a Gemini/Groq en condiciones de mercado sin catalizadores externos.
    has_news = bool(getattr(enriched, "news", None))
    if not has_news:
        if context_override:
            tech_signal = context_override.get("signal", "NEUTRAL")
            score       = context_override.get("score", 0)
            action      = "BUY" if tech_signal == "LONG" else ("SELL" if tech_signal == "SHORT" else "HOLD")
            logger.info(f"[{symbol}] \U0001f7e2 Sin noticias → señal técnica directa ({action}, score={score}/10)")
            return {"action": action, "confidence": score, "reasoning": "Sin noticias — señal técnica directa", "key_factors": ["technical"]}
        else:
            if not bars or len(bars) < 30:
                return {"action": "HOLD", "confidence": 3,
                        "reasoning": "Bars insuficientes", "key_factors": []}
            tech = _technical_signal(bars)
            logger.info(f"[{symbol}] \U0001f7e2 Sin noticias → señal técnica directa ({tech['signal']})")
            return {"action": tech["signal"], "confidence": tech["confidence"],
                    "reasoning": "Sin noticias — señal técnica directa", "key_factors": ["technical"]}
    # ─────────────────────────────────────────────────────────────────────

    # ── SLOW PATH: hay noticias → consultar IA ────────────────────────────
    if context_override:
        tech_signal     = context_override.get("signal", "NEUTRAL")
        fallback_action = "BUY" if tech_signal == "LONG" else "SELL"
        score           = context_override.get("score", 0)

        cache_key = f"{symbol}:{score}:{tech_signal}:{price_bucket}"
        now       = time.monotonic()
        if cache_key in _ai_cache:
            cached_result, cached_ts = _ai_cache[cache_key]
            age = int(now - cached_ts)
            if cached_result.get("action") in ("BUY", "SELL", "CLOSE") and age < CACHE_TTL:
                logger.info(f"[{symbol}] \U0001f504 IA cached ({age}s ago): {cached_result.get('action')}")
                return cached_result

        context = {**context_override, "external": enriched_str}
        logger.info(f"[{symbol}] \U0001f4f0 Noticias detectadas — IA consultada (score={score}/10)")

    else:
        if not bars or len(bars) < 30:
            logger.warning(f"[{symbol}] ai_decide: bars insuficientes ({len(bars) if bars else 0}), HOLD")
            return {"action": "HOLD", "confidence": 3,
                    "reasoning": "Bars insuficientes para señal técnica", "key_factors": []}

        tech = _technical_signal(bars)
        if tech["signal"] == "HOLD":
            return {"action": "HOLD", "confidence": tech["confidence"],
                    "reasoning": "Sin señal técnica", "key_factors": []}

        fallback_action = tech["signal"]
        cache_key       = f"{symbol}:tech:{tech['signal']}:{price_bucket}"
        now             = time.monotonic()

        if cache_key in _ai_cache:
            cached_result, cached_ts = _ai_cache[cache_key]
            age = int(now - cached_ts)
            if cached_result.get("action") in ("BUY", "SELL", "CLOSE") and age < CACHE_TTL_BUY:
                logger.info(f"[{symbol}] \U0001f504 IA cached técnico ({age}s ago): {cached_result.get('action')}")
                return cached_result

        context = build_market_context(symbol, bars, position, entry_price, leverage,
                                       enriched_str=enriched_str)
        logger.info(f"[{symbol}] \U0001f4f0 Noticias detectadas — Técnico {tech['signal']} → IA consultada")

    result = await _call_gemini(context)
    if not result:
        result = await _call_groq(context)
    if not result:
        logger.warning(f"[{symbol}] Sin IA → fallback técnico")
        result = {"action": fallback_action, "confidence": 7,
                  "reasoning": "Fallback técnico", "key_factors": []}

    # Registrar en cooldown DESPUÉS de la llamada real
    await budget.register_symbol_call(symbol)

    confidence = result.get("confidence", 5)
    min_conf   = int(os.getenv("AI_MIN_CONFIDENCE", "5"))
    if confidence < min_conf and result.get("action") in ("BUY", "SELL"):
        logger.info(f"[{symbol}] IA confidence {confidence} < {min_conf} → HOLD")
        result["action"] = "HOLD"

    action = result.get("action")
    if action in ("BUY", "SELL", "CLOSE"):
        _ai_cache[cache_key] = (result, now)
        if len(_ai_cache) > 200:
            oldest_key = min(_ai_cache, key=lambda k: _ai_cache[k][1])
            del _ai_cache[oldest_key]

    logger.info(f"\U0001f916 [{symbol}] {result['action']} ({confidence}/10) | {result.get('reasoning', result.get('reason', ''))}")
    return result
