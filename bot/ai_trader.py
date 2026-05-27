import logging
import os
import json
import asyncio
import aiohttp
from bot.indicators import ema, rsi, macd, supertrend, atr

logger = logging.getLogger("AITrader")

GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"
GEMINI_URL   = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# Semáforo global: máx 10 llamadas simultáneas a Gemini
_gemini_semaphore = asyncio.Semaphore(10)


def build_market_context(symbol, bars, position, entry_price, leverage):
    closes = [b[4] for b in bars]
    highs  = [b[2] for b in bars]
    lows   = [b[3] for b in bars]
    vols   = [b[5] for b in bars]

    ema21  = ema(closes, 21)
    ema50  = ema(closes, 50)
    ema200 = ema(closes, 200) if len(closes) >= 200 else ema(closes, min(len(closes)//2, 100))
    rsi14  = rsi(closes, 14)
    m_line, s_line, hist = macd(closes, 12, 26, 9)
    st_dir, st_val = supertrend(highs, lows, closes, 10, 3.0)
    atr14  = atr(highs, lows, closes, 14)

    last_5 = [
        {"open": bars[i][1], "high": bars[i][2], "low": bars[i][3],
         "close": bars[i][4], "volume": round(bars[i][5], 2)}
        for i in range(-5, 0)
    ]
    avg_vol = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / len(vols)
    vol_ratio = round(vols[-1] / avg_vol, 2) if avg_vol > 0 else 1

    ctx = {
        "symbol": symbol,
        "current_price": closes[-1],
        "indicators": {
            "EMA_21": round(ema21[-1], 4) if ema21 else None,
            "EMA_50": round(ema50[-1], 4) if ema50 else None,
            "EMA_200": round(ema200[-1], 4) if ema200 else None,
            "EMA_trend": "BULLISH" if (ema21 and ema50 and ema21[-1] > ema50[-1]) else "BEARISH",
            "RSI_14": rsi14,
            "MACD_line": m_line,
            "MACD_signal": s_line,
            "MACD_histogram": hist,
            "MACD_trend": "BULLISH" if hist > 0 else "BEARISH",
            "Supertrend_direction": "BULLISH" if st_dir == 1 else "BEARISH",
            "Supertrend_value": st_val,
            "ATR_14": round(atr14, 4),
            "volume_ratio_vs_avg": vol_ratio,
        },
        "last_5_candles": last_5,
        "current_position": position or "NONE",
        "entry_price": entry_price,
        "leverage": leverage,
    }

    if position and entry_price:
        if position == "long":
            pnl = (closes[-1] - entry_price) / entry_price * 100 * leverage
        else:
            pnl = (entry_price - closes[-1]) / entry_price * 100 * leverage
        ctx["current_pnl_pct"] = round(pnl, 2)

    return ctx


SYSTEM_PROMPT = """Eres un trader experto en futuros de criptomonedas con 10 años de experiencia.
Tu objetivo es maximizar ganancias con gestión estricta del riesgo.

Recibes datos técnicos en tiempo real de un par de futuros perpetuos.
Debes decidir la acción más rentable y segura en este momento.

REGLAS ESTRICTAS:
- Solo opera cuando la señal sea CLARA. En caso de duda: HOLD
- No abras LONG si RSI > 70 salvo ruptura con volumen muy alto
- No abras SHORT si RSI < 30 salvo ruptura con volumen muy alto
- Si el Supertrend y las EMAs coinciden = señal fuerte
- Si hay contradicción entre indicadores = HOLD
- Si tienes posición abierta con PnL > +3%: considera CLOSE para asegurar
- Si tienes posición abierta con PnL < -1.5%: considera CLOSE para limitar pérdida
- El volumen_ratio > 1.5 confirma la señal, < 0.7 la debilita

RESPONDE SOLO CON JSON VÁLIDO:
{
  "action": "BUY" | "SELL" | "HOLD" | "CLOSE",
  "confidence": 1-10,
  "reasoning": "explicación breve en español (máx 2 frases)",
  "key_factors": ["factor1", "factor2"]
}"""


async def _call_gemini(context):
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        logger.warning("Gemini: GEMINI_API_KEY no configurada")
        return None
    try:
        async with _gemini_semaphore:
            url = GEMINI_URL.format(model=GEMINI_MODEL) + f"?key={key}"
            prompt = SYSTEM_PROMPT + "\n\nDATOS:\n" + json.dumps(context, ensure_ascii=False)
            async with aiohttp.ClientSession() as s:
                resp = await s.post(
                    url,
                    json={
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 300},
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                )
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Gemini HTTP {resp.status}: {body[:300]}")
                    if resp.status == 429:
                        await asyncio.sleep(2)
                    return None

                data = await resp.json()

                if "candidates" not in data:
                    logger.warning(f"Gemini sin candidates: {json.dumps(data)[:300]}")
                    return None

                candidates = data["candidates"]
                if not candidates:
                    logger.warning("Gemini: candidates vacío")
                    return None

                finish_reason = candidates[0].get("finishReason", "STOP")
                if finish_reason not in ("STOP", "MAX_TOKENS"):
                    logger.warning(f"Gemini finishReason={finish_reason}, descartando")
                    return None

                raw = candidates[0]["content"]["parts"][0]["text"]
                raw = raw.strip().strip("```json").strip("```").strip()
                return json.loads(raw)

            # Pequeña pausa al soltar el semáforo para no saturar el minuto siguiente
            await asyncio.sleep(0.1)

    except json.JSONDecodeError as e:
        logger.warning(f"Gemini JSON inválido: {e}")
        return None
    except Exception as e:
        logger.warning(f"Gemini falló: {e}")
        return None


async def _call_groq(context):
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        return None
    try:
        async with aiohttp.ClientSession() as s:
            resp = await s.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": GROQ_MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
                    ],
                    "max_tokens": 200,
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
            return json.loads(data["choices"][0]["message"]["content"])
    except Exception as e:
        logger.warning(f"Groq falló: {e}")
        return None


async def ai_decide(symbol, bars, position, entry_price, leverage):
    context = build_market_context(symbol, bars, position, entry_price, leverage)

    # 1. Gemini primero (con semáforo de concurrencia)
    result = await _call_gemini(context)
    # 2. Groq como fallback
    if not result:
        result = await _call_groq(context)
    # 3. Fallback técnico
    if not result:
        logger.warning(f"[{symbol}] Sin IA — fallback técnico")
        ind = context["indicators"]
        if ind["EMA_trend"] == "BULLISH" and ind["Supertrend_direction"] == "BULLISH" and ind["RSI_14"] < 65:
            action = "BUY"
        elif ind["EMA_trend"] == "BEARISH" and ind["Supertrend_direction"] == "BEARISH" and ind["RSI_14"] > 35:
            action = "SELL"
        else:
            action = "HOLD"
        result = {"action": action, "confidence": 5, "reasoning": "Fallback técnico", "key_factors": []}

    confidence = result.get("confidence", 5)
    min_conf = int(os.getenv("AI_MIN_CONFIDENCE", "6"))
    if confidence < min_conf and result["action"] in ("BUY", "SELL"):
        logger.info(f"[{symbol}] IA quiere {result['action']} pero confianza {confidence} < {min_conf} → HOLD")
        result["action"] = "HOLD"

    logger.info(
        f"🤖 [{symbol}] {result['action']} (confianza: {confidence}/10) | {result.get('reasoning', '')}"
    )
    return result
