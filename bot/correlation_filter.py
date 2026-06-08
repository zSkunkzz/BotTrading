#!/usr/bin/env python3
"""
bot/correlation_filter.py — Límite de correlación entre pares abiertos (mejora #4).

Evita acumular demasiada exposición en la misma dirección cuando ya hay varias
posiciones abiertas simultáneas en pares correlacionados (BTC, ETH, SOL, etc.).

Configuración (variables de entorno):
    CORR_MAX_SAME_DIRECTION   (int,   default 2)
        Número máximo de posiciones LONG (o SHORT) abiertas simultáneamente
        antes de bloquear nuevas entradas en esa dirección.
        Ejemplo: si CORR_MAX_SAME_DIRECTION=2 y ya hay 2 LONGs abiertos,
        no se permite abrir un tercer LONG hasta que cierre alguno.

    CORR_BLOCK_OPPOSITE       (bool,  default false)
        Si true, también bloquea entradas en dirección CONTRARIA cuando el
        número de posiciones en la dirección opuesta supera el mismo límite.
        Útil para evitar exposición cruzada long/short en el mismo momento.

    CORR_SAME_BASE_ONLY       (bool,  default false)
        Si true, solo cuenta pares que comparten base con el símbolo nuevo.
        Ejemplo: BTC-USDT y BTC-BUSD son "misma base" pero ETH-USDT no.
        Por defecto (false) cuenta TODOS los pares como correlacionados
        (comportamiento más conservador).

Uso:
    from bot.correlation_filter import check_correlation_gate

    # open_traders es el dict global { symbol: FuturesTrader }
    allowed, reason = check_correlation_gate("ETH", "long", open_traders)
    if not allowed:
        logger.warning("[ETH] Bloqueado por correlación: %s", reason)
        return
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

_MAX_SAME_DIR     = int(os.getenv("CORR_MAX_SAME_DIRECTION", "2"))
_BLOCK_OPPOSITE   = os.getenv("CORR_BLOCK_OPPOSITE",  "false").lower() in ("true", "1", "yes")
_SAME_BASE_ONLY   = os.getenv("CORR_SAME_BASE_ONLY",  "false").lower() in ("true", "1", "yes")


def _base_coin(symbol: str) -> str:
    """Extrae la moneda base: 'ETH', 'ETH/USDT:USDT', 'ETH-USDT' → 'ETH'."""
    s = symbol.upper()
    for sep in ("/", "-"):
        if sep in s:
            return s.split(sep)[0]
    return s


def check_correlation_gate(
    symbol: str,
    side: str,
    open_traders: dict,
) -> tuple[bool, str]:
    """
    Comprueba si abrir una nueva posición en `symbol` con dirección `side`
    supera el límite de correlación configurado.

    Parámetros:
        symbol        — símbolo de la nueva entrada (ej. "ETH")
        side          — "long" o "short"
        open_traders  — dict { symbol: FuturesTrader } con todos los traders activos

    Devuelve:
        (True, "")            → entrada permitida
        (False, motivo_str)   → entrada bloqueada, motivo para log/Telegram
    """
    if _MAX_SAME_DIR <= 0:
        return True, ""

    new_side   = side.lower()
    new_base   = _base_coin(symbol)
    same_count = 0
    opp_count  = 0

    for sym, trader in open_traders.items():
        if not hasattr(trader, "position") or trader.position is None:
            continue
        if sym == symbol:
            # El propio trader ya tiene posición — open_order lo filtra antes,
            # pero lo ignoramos aquí para no contar doble.
            continue

        if _SAME_BASE_ONLY and _base_coin(sym) != new_base:
            continue

        pos_side = str(trader.position).lower()
        if pos_side == new_side:
            same_count += 1
        else:
            opp_count += 1

    if same_count >= _MAX_SAME_DIR:
        msg = (
            f"correlación bloqueada — ya hay {same_count} posición(es) {new_side.upper()} "
            f"abiertas (límite={_MAX_SAME_DIR})"
        )
        log.info("[%s] 🚫 %s", symbol, msg)
        return False, msg

    if _BLOCK_OPPOSITE and opp_count >= _MAX_SAME_DIR:
        msg = (
            f"correlación bloqueada — exposición opuesta: {opp_count} posición(es) "
            f"{'SHORT' if new_side == 'long' else 'LONG'} abiertas (límite={_MAX_SAME_DIR})"
        )
        log.info("[%s] 🚫 %s", symbol, msg)
        return False, msg

    return True, ""


def get_open_exposure(open_traders: dict) -> dict:
    """
    Devuelve un resumen de la exposición actual para logs o notificaciones Telegram.

    Retorna:
        {
          "long":  ["BTC", "SOL"],
          "short": ["ETH"],
          "total": 3,
        }
    """
    longs  = []
    shorts = []
    for sym, trader in open_traders.items():
        if not hasattr(trader, "position") or trader.position is None:
            continue
        pos = str(trader.position).lower()
        if pos == "long":
            longs.append(sym)
        elif pos == "short":
            shorts.append(sym)
    return {"long": longs, "short": shorts, "total": len(longs) + len(shorts)}
