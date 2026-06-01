"""
enriched_filter.py — Filtro determinista sobre datos externos (Fear & Greed,
funding rate, OI delta) que reemplaza las reglas del SYSTEM_PROMPT de la IA.

Todo lo que hace la IA con números se puede hacer aquí sin latencia ni coste de API.
La IA queda reservada para el análisis de noticias (texto libre).

Variables de entorno:
  EF_FG_FEAR_THRESHOLD      : F&G por debajo del cual se considera miedo extremo (default 20)
  EF_FG_GREED_THRESHOLD     : F&G por encima del cual se considera euforia (default 80)
  EF_FUNDING_LONG_MAX       : funding máximo para entrar LONG (default 0.05 % por 8h)
  EF_FUNDING_SHORT_MIN      : funding mínimo para entrar SHORT (default -0.05 % por 8h)
  EF_OI_DELTA_STRONG        : delta OI a partir del cual se considera movimiento fuerte (default 5.0 %)
  EF_RSI_OVERBOUGHT         : RSI por encima del cual no se abren LONG (default 65)  ← bajado de 70
  EF_RSI_OVERSOLD           : RSI por debajo del cual no se abren SHORT (default 35)
  EF_VOL_RATIO_MIN          : vol_ratio mínimo para confirmar señal (default 0.7)
  EF_NEWS_BEARISH_MAX       : máximo de noticias bearish para permitir LONG (default 2)
  EF_NEWS_BULLISH_MAX       : máximo de noticias bullish para permitir SHORT (default 2)
  EF_NEWS_AI_THRESHOLD      : si hay >= N noticias relevantes, llamar IA para análisis (default 2)

Retorna:
  FilterResult:
    .allowed  (bool)  — True si la señal pasa el filtro
    .reason   (str)   — motivo en caso de rechazo, o descripción resumida si pasa
    .penalty  (int)   — puntos de penalización acumulados (0-3), útil para ajustar confidence
    .news_ai_needed (bool) — True si hay suficientes noticias para que valga la pena consultar IA
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bot.data_enricher import EnrichedContext

logger = logging.getLogger("EnrichedFilter")

# ── Thresholds ────────────────────────────────────────────────────────────────
FG_FEAR_THRESHOLD    = int(float(os.getenv("EF_FG_FEAR_THRESHOLD",    "20")))
FG_GREED_THRESHOLD   = int(float(os.getenv("EF_FG_GREED_THRESHOLD",   "80")))
FUNDING_LONG_MAX     = float(os.getenv("EF_FUNDING_LONG_MAX",  "0.05"))   # % per 8h
FUNDING_SHORT_MIN    = float(os.getenv("EF_FUNDING_SHORT_MIN", "-0.05"))  # % per 8h
OI_DELTA_STRONG      = float(os.getenv("EF_OI_DELTA_STRONG",   "5.0"))   # %
# FIX: RSI_OVERBOUGHT bajado de 70 a 65 — bloquea entradas LONG cuando el mercado
# ya está caliente. Alineado con la zona óptima del scoring (<=60).
RSI_OVERBOUGHT       = float(os.getenv("EF_RSI_OVERBOUGHT",    "65"))
RSI_OVERSOLD         = float(os.getenv("EF_RSI_OVERSOLD",      "35"))
VOL_RATIO_MIN        = float(os.getenv("EF_VOL_RATIO_MIN",     "0.7"))
NEWS_BEARISH_MAX     = int(float(os.getenv("EF_NEWS_BEARISH_MAX",  "2")))
NEWS_BULLISH_MAX     = int(float(os.getenv("EF_NEWS_BULLISH_MAX",  "2")))
NEWS_AI_THRESHOLD    = int(float(os.getenv("EF_NEWS_AI_THRESHOLD", "2")))


@dataclass
class FilterResult:
    allowed: bool
    reason: str
    penalty: int = 0          # 0-3: cuántos puntos restar al confidence final
    news_ai_needed: bool = False


def apply(
    signal: str,           # "LONG" | "SHORT"
    enriched: "EnrichedContext",
    rsi: float | None = None,
    vol_ratio: float | None = None,
    price_direction: str | None = None,  # "rising" | "falling" | None
) -> FilterResult:
    """
    Aplica el conjunto de reglas del SYSTEM_PROMPT de la IA de forma determinista.

    Args:
        signal        : dirección de la señal técnica ("LONG" o "SHORT")
        enriched      : contexto externo obtenido por data_enricher
        rsi           : RSI actual (opcional, para filtro RSI)
        vol_ratio     : relación volumen actual / media (opcional)
        price_direction: tendencia de precio en la vela actual (opcional, para filtro OI)

    Returns:
        FilterResult — ver docstring del módulo
    """
    is_long  = signal.upper() == "LONG"
    is_short = signal.upper() == "SHORT"

    fg           = enriched.fear_greed.value  if enriched.fear_greed else 50
    funding      = enriched.oi.funding_rate   if enriched.oi        else 0.0
    oi_delta     = enriched.oi.oi_delta_pct   if enriched.oi        else 0.0
    news_items   = enriched.news or []

    reasons_block: list[str] = []
    reasons_warn:  list[str] = []
    penalty = 0

    # ── 1. RSI ────────────────────────────────────────────────────────────────
    if rsi is not None:
        if is_long and rsi > RSI_OVERBOUGHT:
            reasons_block.append(f"RSI={rsi:.1f} > {RSI_OVERBOUGHT} — sobrecompra, no LONG")
        if is_short and rsi < RSI_OVERSOLD:
            reasons_block.append(f"RSI={rsi:.1f} < {RSI_OVERSOLD} — sobreventa, no SHORT")

    # ── 2. Volumen ────────────────────────────────────────────────────────────
    if vol_ratio is not None and vol_ratio < VOL_RATIO_MIN:
        penalty += 1
        reasons_warn.append(f"vol_ratio={vol_ratio:.2f} < {VOL_RATIO_MIN} — señal débil")

    # ── 3. Fear & Greed ───────────────────────────────────────────────────────
    if is_long:
        if fg < FG_FEAR_THRESHOLD:
            penalty += 1
            reasons_warn.append(f"F&G={fg} — miedo extremo, cautela en LONG")
        if fg > FG_GREED_THRESHOLD:
            penalty += 1
            reasons_warn.append(f"F&G={fg} — euforia ({fg}/100), riesgo de techo")
    if is_short:
        if fg > FG_GREED_THRESHOLD:
            # Euforia confirma SHORT (todos se han puesto largos → mean reversion)
            reasons_warn.append(f"F&G={fg} — euforia, confirma SHORT")
        if fg < FG_FEAR_THRESHOLD:
            penalty += 1
            reasons_warn.append(f"F&G={fg} — pánico, SHORT puede ser tarde")

    # ── 4. Funding rate ───────────────────────────────────────────────────────
    if is_long and funding > FUNDING_LONG_MAX:
        reasons_block.append(
            f"Funding={funding:+.4f}% — longs saturados (>{FUNDING_LONG_MAX}%)"
        )
    if is_short and funding < FUNDING_SHORT_MIN:
        reasons_block.append(
            f"Funding={funding:+.4f}% — shorts saturados (<{FUNDING_SHORT_MIN}%)"
        )

    # ── 5. OI delta + dirección precio ───────────────────────────────────────
    if abs(oi_delta) >= OI_DELTA_STRONG:
        if is_long and oi_delta > 0 and price_direction == "falling":
            reasons_block.append(
                f"OI_delta={oi_delta:+.1f}% con precio cayendo — presión bajista"
            )
        if is_long and oi_delta > 0 and price_direction == "rising":
            # Convicción alcista — no bloquear, al contrario
            reasons_warn.append(
                f"OI_delta={oi_delta:+.1f}% con precio subiendo — fuerte convicción alcista ✓"
            )
        if is_short and oi_delta < 0:
            reasons_warn.append(
                f"OI_delta={oi_delta:+.1f}% — liquidación de longs, confirma SHORT ✓"
            )

    # ── 6. Noticias (keyword, sin IA) ────────────────────────────────────────
    bearish_count = sum(1 for n in news_items if n.sentiment == "bearish")
    bullish_count = sum(1 for n in news_items if n.sentiment == "bullish")

    if is_long and bearish_count >= NEWS_BEARISH_MAX:
        penalty += 1
        reasons_warn.append(
            f"{bearish_count} noticias bearish — cautela en LONG"
        )
    if is_short and bullish_count >= NEWS_BULLISH_MAX:
        penalty += 1
        reasons_warn.append(
            f"{bullish_count} noticias bullish — cautela en SHORT"
        )

    # ── 7. Decidir si hay suficientes noticias relevantes para consultar IA ──
    relevant_news = [
        n for n in news_items
        if n.sentiment in ("bullish", "bearish")
    ]
    news_ai_needed = len(relevant_news) >= NEWS_AI_THRESHOLD

    # ── Resultado ─────────────────────────────────────────────────────────────
    if reasons_block:
        full_reason = " | ".join(reasons_block)
        if reasons_warn:
            full_reason += " [warn: " + " | ".join(reasons_warn) + "]"
        logger.info(
            "[EnrichedFilter] %s BLOQUEADO — %s",
            signal, full_reason,
        )
        return FilterResult(
            allowed=False,
            reason=full_reason,
            penalty=penalty,
            news_ai_needed=news_ai_needed,
        )

    summary = f"F&G={fg} | funding={funding:+.4f}% | OI_delta={oi_delta:+.1f}%"
    if reasons_warn:
        summary += " | " + " | ".join(reasons_warn)
    logger.debug("[EnrichedFilter] %s OK — %s", signal, summary)
    return FilterResult(
        allowed=True,
        reason=summary,
        penalty=min(penalty, 3),
        news_ai_needed=news_ai_needed,
    )
