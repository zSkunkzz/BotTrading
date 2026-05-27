import logging
import os
import re
import json
import asyncio
import aiohttp
from bot.indicators import ema, rsi, macd, supertrend, atr
from ai_rate_limiter import budget, RateLimitExhausted

logger = logging.getLogger("AITrader")

GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GEMINI_URL   = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

SYSTEM_PROMPT = """Trader experto futuros crypto. Confirma señal técnica recibida.
Reglas: CLOSE si pnl>+3% o pnl<-1.5% | No BUY si rsi>70 | No SELL si rsi<30
Vol_ratio>1.5 confirma, <0.7 debilita. Si duda → HOLD.
Responde SOLO con JSON válido, sin texto antes ni después, sin markdown:
{"action":"BUY"|"SELL"|"HOLD"|"CLOSE","confidence":1-10,"reason":"max 1 frase"}"""


def _parse_ai_json(raw: str) -> dict:
    """
    Extrae JSON de respuesta IA de forma robusta:
    1. Elimina markdown fences (```json ... ```)
    2. Busca el primer '{' y el último '}' para aislar el objeto JSON
       aunque la IA anteponga texto libre como 'Here is the JSON:'
    3. Lanza ValueError si no hay JSON válido
    """
    raw = raw.strip()
    # Paso 1: quitar fences de markdown
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()
    if not raw:
        raise ValueError("Respuesta IA vacía tras strip")
    # Paso 2: extraer contenido entre primer '{' y último '}'
    start = raw.find("{")
    end   = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"No se encontró JSON en la respuesta: {raw[:120]!r}")
    return json.loads(raw[start:end + 1])


def build_market_context(symbol, bars, position, entry_price, leverage):
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    vols   = [b[5] for b in bars]

    ema21      = ema(closes, 21)
    ema50      = ema(closes, 50)
    rsi14      = rsi(closes, 14)
    m_line, s_line, hist = macd(closes, 12, 26, 9)
    st_dir, _  = supertrend(highs, lows, closes, 10, 3.0)
    atr14      = atr(highs, lows, closes, 14)
    avg_vol    = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    vol_ratio  = round(vols[-1] / avg_vol, 2) if avg_vol > 0 else 1
    last_3     = [
        {"open": bars[i][1], "high": bars[i][2], "low": bars[i][3],
         "close": bars[i][4], "volume": round(bars[i][5], 2)}
        for i in range(-3, 0)
    ]
    ctx = {
        "symbol":   symbol,
        "price":    closes[-1],
        "ind": {
            "ema_trend":  "UP" if (ema21 and ema50 and ema21[-1] > ema50[-1]) else "DOWN",
            "rsi":        rsi14,
            "macd":       "UP" if hist > 0 else "DOWN",
            "supertrend": "UP" if st_dir == 1 else "DOWN",
            "atr":        round(atr14, 4),
            "vol_ratio":  vol_ratio,
        },
        "candles_3": last_3,
        "position":  position or "NONE",
        "entry":     entry_price,
        "leverage":  leverage,
    }
    if position and entry_price:
        if position == "long":
            pnl = (closes[-1] - entry_price) / entry_price * 100 * leverage
        else:
            pnl = (entry_price - closes[-1]) / entry_price * 100 * leverage
        ctx["pnl_pct"] = round(pnl, 2)
    return ctx


async def _call_gemini(context: dict):
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        return None

    max_retries = int(os.getenv("GEMINI_MAX_RETRIES", "2"))

    try:
        if not await budget.can_call_gemini():
            raise RateLimitExhausted("gemini")
        async with budget.gemini_semaphore:
            await budget.register_gemini_call()
            url    = GEMINI_URL.format(model=GEMINI_MODEL) + f"?key={key}"
            prompt = SYSTEM_PROMPT + "\nDATA:" + json.dumps(context, ensure_ascii=False, separators=(',', ':'))

            for attempt in range(1, max_retries + 2):
                async with aiohttp.ClientSession() as s:
                    resp = await s.post(
                        url,
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {
                                "temperature": 0.1,
                                "maxOutputTokens": 150,
                                "responseMimeType": "application/json",
                            },
                        },
                        timeout=aiohttp.ClientTimeout(total=15),
                    )

                    if resp.status == 503:
                        body = await resp.text()
                        logger.warning(f"Gemini HTTP 503 (intento {attempt}/{max_retries+1}): {body[:200]}")
                        if attempt <= max_retries:
                            await asyncio.sleep(3 * attempt)
                            continue
                        return None

                    if resp.status == 429:
                        logger.warning("Gemini HTTP 429 — rate limit")
                        await asyncio.sleep(5)
                        return None

                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"Gemini HTTP {resp.status}: {body[:300]}")
                        return None

                    data = await resp.json()
                    if "candidates" not in data or not data["candidates"]:
                        logger.warning("Gemini: respuesta sin candidates")
                        return None

                    finish_reason = data["candidates"][0].get("finishReason", "STOP")
                    if finish_reason not in ("STOP", "MAX_TOKENS"):
                        logger.warning(f"Gemini finishReason inesperado: {finish_reason}")
                        return None

                    raw = data["candidates"][0]["content"]["parts"][0]["text"]
                    try:
                        return _parse_ai_json(raw)
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"Gemini fall {e} | raw={raw[:120]!r}")
                        return None

    except RateLimitExhausted as e:
        logger.warning(f"[Gemini] {e}")
        return None
    except Exception as e:
        logger.warning(f"Gemini error inesperado: {e}")
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
                            {"role": "system",  "content": SYSTEM_PROMPT},
                            {"role": "user",    "content": json.dumps(context, ensure_ascii=False, separators=(',', ':'))},
                        ],
                        "max_tokens": 120,
                        "temperature": 0.1,
                        "response_format": {"type": "json_object"},
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Groq HTTP {resp.status}: {body[:200]}")
                    return None
                data = await resp.json()
                raw = data["choices"][0]["message"]["content"]
                try:
                    return _parse_ai_json(raw)
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Groq fall {e} | raw={raw[:120]!r}")
                    return None
    except RateLimitExhausted as e:
        logger.warning(f"[Groq] {e}")
        return None
    except Exception as e:
        logger.warning(f"Groq error inesperado: {e}")
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
    tp  = float(os.getenv("AI_TP_PCT",  "3.0"))
    sl  = float(os.getenv("AI_SL_PCT", "-1.5"))
    if pnl >= tp:  return f"TP +{pnl:.2f}% alcanzado"
    if pnl <= sl:  return f"SL {pnl:.2f}% activado"
    return None


async def ai_decide(symbol, bars, position, entry_price, leverage,
                    context_override: dict | None = None):
    current_price = bars[-1][4] if bars else (entry_price or 0)

    if position:
        close_reason = _pnl_check(position, entry_price, current_price, leverage)
        if close_reason:
            logger.info(f"📊 [{symbol}] CLOSE | {close_reason}")
            return {"action": "CLOSE", "confidence": 9,
                    "reasoning": close_reason, "key_factors": ["pnl_management"]}
        logger.debug(f"[{symbol}] Posición activa, PnL ok → HOLD")
        return {"action": "HOLD", "confidence": 5,
                "reasoning": "Posición abierta, PnL dentro de rango", "key_factors": []}

    if context_override:
        context = context_override
        tech_signal = context_override.get("signal", "NEUTRAL")
        fallback_action = "BUY" if tech_signal == "LONG" else "SELL"
        logger.info(f"[{symbol}] Context override (score={context.get('score')}/10) → consultando IA")
    else:
        tech = _technical_signal(bars)
        if tech["signal"] == "HOLD":
            logger.debug(f"[{symbol}] Técnico HOLD → sin IA")
            return {"action": "HOLD", "confidence": tech["confidence"],
                    "reasoning": "Sin señal técnica clara", "key_factors": []}
        context = build_market_context(symbol, bars, position, entry_price, leverage)
        fallback_action = tech["signal"]
        logger.info(f"[{symbol}] Señal técnica {tech['signal']} → consultando IA")

    result = await _call_gemini(context)
    if not result:
        result = await _call_groq(context)
    if not result:
        logger.warning(f"[{symbol}] Sin IA — usando señal técnica directa")
        result = {
            "action":     fallback_action,
            "confidence": 7,
            "reasoning":  "Fallback técnico (IA no disponible)",
            "key_factors": [],
        }

    confidence = result.get("confidence", 5)
    min_conf   = int(os.getenv("AI_MIN_CONFIDENCE", "6"))
    if confidence < min_conf and result.get("action") in ("BUY", "SELL"):
        logger.info(f"[{symbol}] IA {result['action']} confianza {confidence}<{min_conf} → HOLD")
        result["action"] = "HOLD"

    logger.info(f"🤖 [{symbol}] {result['action']} ({confidence}/10) | {result.get('reasoning', '')}")
    return result
