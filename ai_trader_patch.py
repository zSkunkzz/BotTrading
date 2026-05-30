"""
ai_trader_patch.py
Shows the changes to apply to your existing ai_trader.py.
Replace / merge these functions into your current file.
"""

import asyncio
from data_enricher import MarketDataEnricher, EnrichedContext

# ---------------------------------------------------------------------------
# 1. Instantiate the enricher once (at module level or inside your AITrader __init__)
# ---------------------------------------------------------------------------
# enricher = MarketDataEnricher(
#     bitget_api_key=os.getenv("BITGET_API_KEY"),
#     bitget_api_secret=os.getenv("BITGET_SECRET"),
#     bitget_passphrase=os.getenv("BITGET_PASSPHRASE"),
# )


# ---------------------------------------------------------------------------
# 2. Extended system prompt — replace your existing SYSTEM_PROMPT
# ---------------------------------------------------------------------------
SYSTEM_PROMPT_ENRICHED = """
You are a professional crypto futures trader assistant.
You receive a technical signal AND external market context.
Your job: synthesize BOTH to decide if the trade should be taken, skipped, or sized down.

Rules:
- If Fear & Greed < 20 (Extreme Fear): AVOID new longs unless signal score >= 8/10.
- If Fear & Greed > 80 (Extreme Greed): AVOID new shorts unless signal score >= 8/10.
- If Open Interest 4h delta > +10% AND direction is LONG: CAUTION — late longs, reduce size.
- If Open Interest 4h delta < -10% AND direction is SHORT: CAUTION — late shorts, reduce size.
- If Funding rate > +0.05% AND direction is LONG: market is crowded long — prefer to SKIP.
- If Funding rate < -0.05% AND direction is SHORT: market is crowded short — prefer to SKIP.
- If a negative news headline is recent and matches the symbol: add bearish weight.
- If a positive news headline is recent and matches the symbol: add bullish weight.

Output format (JSON only, no markdown):
{
  "decision": "ENTER" | "SKIP" | "REDUCE_SIZE",
  "confidence": 1-10,
  "reasoning": "short explanation",
  "suggested_size_multiplier": 0.5 | 0.75 | 1.0
}
"""


# ---------------------------------------------------------------------------
# 3. Enriched context builder — replaces your existing build_market_context()
# ---------------------------------------------------------------------------
def build_market_context_enriched(signal: dict, ctx: EnrichedContext) -> str:
    """
    Builds the user message for the AI, merging technical signal + external context.
    """
    technical_block = f"""Symbol: {signal.get('symbol')} · Direction: {signal.get('direction')} · Score: {signal.get('score', '?')}/10
RSI: {signal.get('rsi', 'N/A')}
EMA alignment: {signal.get('ema_alignment', 'N/A')}
MACD: {signal.get('macd', 'N/A')}
Entry: {signal.get('entry', 'N/A')} · SL: {signal.get('sl', 'N/A')} · TP: {signal.get('tp', 'N/A')}
Timeframe: {signal.get('timeframe', 'N/A')}"""

    external_block = ctx.to_prompt_block()

    return technical_block + external_block + "\nBased on all of the above, provide your JSON decision."


# ---------------------------------------------------------------------------
# 4. Updated ai_decide() — replaces your existing version
# ---------------------------------------------------------------------------
async def ai_decide_enriched(signal: dict, enricher: MarketDataEnricher, ai_client, model: str) -> dict:
    """
    Drop-in replacement for ai_decide().
    Fetches external context, builds enriched prompt, calls AI.
    """
    symbol = signal.get("symbol", "BTCUSDT")

    # Fetch enriched context (parallel, non-blocking, never raises)
    ctx: EnrichedContext = await enricher.fetch_all(symbol)

    user_message = build_market_context_enriched(signal, ctx)

    # --- Gemini example ---
    # response = await ai_client.generate_content_async(
    #     contents=[{"role": "user", "parts": [{"text": user_message}]}],
    #     system_instruction=SYSTEM_PROMPT_ENRICHED,
    # )
    # raw = response.text

    # --- Groq / OpenAI example ---
    # response = await ai_client.chat.completions.create(
    #     model=model,
    #     messages=[
    #         {"role": "system", "content": SYSTEM_PROMPT_ENRICHED},
    #         {"role": "user", "content": user_message},
    #     ],
    #     response_format={"type": "json_object"},
    # )
    # raw = response.choices[0].message.content

    import json
    try:
        result = json.loads(raw)
    except Exception:
        result = {"decision": "SKIP", "confidence": 0, "reasoning": "AI parse error", "suggested_size_multiplier": 0.0}

    return result
