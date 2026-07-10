"""market_context.py — Contexto de mercado via Hyperliquid (funding + OI).

Una sola llamada REST a metaAndAssetCtxs devuelve funding + OI + markPx
para todos los assets a la vez. Se cachea 60s para no añadir latencia
en ciclos de 20s (LOOP_SLEEP).

Modificador final: entre -15 y +15 puntos sobre el score técnico.
  +6  funding favorece la dirección del trade
  +7  OI confirma el movimiento (nuevas posiciones, no cierres)
  -6  funding va contra el trade (riesgo de squeeze)
  -7  OI cae mientras el precio se mueve (cierres, sin convicción)
  -5  precio sube + OI cae en long (short squeeze, movimiento débil)
"""
from __future__ import annotations
import logging
import time
import requests

log = logging.getLogger("market_context")

_HL_INFO_URL = "https://api.hyperliquid.xyz/info"
_CACHE_TTL   = 60  # segundos — 3 ciclos de LOOP_SLEEP=20s

# Caché global: { coin: {"funding": float, "oi_usd": float, "ts": float} }
_cache: dict[str, dict] = {}
_meta_cache: dict       = {"data": None, "ts": 0.0}


def _fetch_meta() -> tuple[list, list] | None:
    """Devuelve (universe, asset_ctxs) con caché TTL."""
    now = time.time()
    if _meta_cache["data"] and now - _meta_cache["ts"] < _CACHE_TTL:
        return _meta_cache["data"]
    try:
        r = requests.post(
            _HL_INFO_URL,
            json={"type": "metaAndAssetCtxs"},
            timeout=5,
        )
        r.raise_for_status()
        data = r.json()
        if len(data) < 2:
            return None
        result = (data[0].get("universe", []), data[1])
        _meta_cache["data"] = result
        _meta_cache["ts"]   = now
        return result
    except Exception as exc:
        log.warning("market_context._fetch_meta falló: %s", exc)
        return None


def _get_asset_ctx(coin: str) -> dict | None:
    """Devuelve el contexto de un coin (funding, OI, markPx) desde HL."""
    meta = _fetch_meta()
    if not meta:
        return None
    universe, asset_ctxs = meta
    for i, asset in enumerate(universe):
        if asset.get("name") == coin and i < len(asset_ctxs):
            return asset_ctxs[i]
    return None


def score_context(coin: str, side: str, price_change_1h: float) -> int:
    """
    Calcula el modificador de contexto de mercado para un trade.

    Args:
        coin:             nombre del coin en HL (ej. "BTC", "ETH")
        side:             "long" o "short"
        price_change_1h:  cambio % del precio en la última hora
                          (ej. -0.012 = -1.2%, +0.008 = +0.8%)

    Returns:
        Entero entre -15 y +15. 0 si los datos no están disponibles.
    """
    ctx = _get_asset_ctx(coin)
    if ctx is None:
        log.debug("[%s] market_context: sin datos → modificador=0", coin)
        return 0

    funding  = float(ctx.get("funding", 0))
    oi_raw   = float(ctx.get("openInterest", 0))
    mark_px  = float(ctx.get("markPx", 0))
    oi_usd   = oi_raw * mark_px  # HL devuelve OI en contratos, convertimos a USD

    # OI previo del caché para calcular cambio
    prev_oi  = _cache.get(coin, {}).get("oi_usd", oi_usd)
    _cache[coin] = {"funding": funding, "oi_usd": oi_usd, "ts": time.time()}

    modifier = 0

    # ── Funding rate ─────────────────────────────────────────────────────
    # Funding positivo = longs pagan a shorts → mercado cargado de longs
    # Funding negativo = shorts pagan a longs → mercado cargado de shorts
    if side == "short":
        if funding > 0.0001:    # longs overcrowded → short favorecido
            modifier += 6
            log.debug("[%s] funding=%.5f positivo → short +6", coin, funding)
        elif funding < -0.0001: # shorts overcrowded → rebote probable
            modifier -= 6
            log.debug("[%s] funding=%.5f negativo → short -6", coin, funding)
    else:  # long
        if funding < -0.0001:   # shorts overcrowded → long favorecido
            modifier += 6
            log.debug("[%s] funding=%.5f negativo → long +6", coin, funding)
        elif funding > 0.0001:  # longs overcrowded → corrección probable
            modifier -= 6
            log.debug("[%s] funding=%.5f positivo → long -6", coin, funding)

    # ── Open Interest ─────────────────────────────────────────────────────
    # OI sube mientras precio se mueve = nuevas posiciones (convicción)
    # OI baja mientras precio se mueve = cierres (sin convicción, rebote probable)
    if prev_oi > 0:
        oi_change = (oi_usd - prev_oi) / prev_oi

        if side == "short" and price_change_1h < -0.005:
            if oi_change > 0.01:    # precio cae + OI sube = shorts abriendo
                modifier += 7
                log.debug("[%s] precio baja + OI sube=%.2f%% → short +7", coin, oi_change * 100)
            elif oi_change < -0.01: # precio cae + OI cae = longs cerrando (sin combustible)
                modifier -= 7
                log.debug("[%s] precio baja + OI cae=%.2f%% → short -7", coin, oi_change * 100)

        elif side == "long" and price_change_1h > 0.005:
            if oi_change > 0.01:    # precio sube + OI sube = longs abriendo
                modifier += 7
                log.debug("[%s] precio sube + OI sube=%.2f%% → long +7", coin, oi_change * 100)
            elif oi_change < -0.01: # precio sube + OI cae = shorts cerrando (short squeeze, débil)
                modifier -= 5
                log.debug("[%s] precio sube + OI cae=%.2f%% → long -5", coin, oi_change * 100)

    final = max(-15, min(15, modifier))
    log.info(
        "[%s] context_modifier=%+d | side=%s funding=%.5f oi_usd=%.0f "
        "oi_change=%.2f%% price_1h=%.2f%%",
        coin, final, side, funding, oi_usd,
        ((oi_usd - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0,
        price_change_1h * 100,
    )
    return final
