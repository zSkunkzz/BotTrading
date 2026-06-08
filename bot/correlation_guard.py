#!/usr/bin/env python3
"""
bot/correlation_guard.py — Control de correlación entre posiciones abiertas.

Dos niveles de protección:

  Nivel 1 — Correlación de par (nuevo):
    Evita abrir posición si el «grupo de correlación» al que pertenece el
    símbolo ya tiene demasiadas posiciones abiertas. Los grupos reflejan
    activos que históricamente mueven juntos (ej.: BTC + ETH + SOL = L1).

  Nivel 2 — Dirección global (original):
    Evita acumular demasiada exposición en la misma dirección (todos LONG
    o todos SHORT) o superar el límite de posiciones abiertas totales.

Variables de entorno:
  CORR_ENABLED        (default: true)
  CORR_MAX_GROUP      (default: 2)  — máx. posiciones del mismo grupo de correlación
  CORR_MAX_SAME_DIR   (default: 3)  — máx. posiciones en la misma dirección
  CORR_MAX_OPEN       (default: 5)  — máx. posiciones abiertas totales

Fix v2 — Global Position Registry:
  Problema raíz: trading_loop pasaba _build_open_positions_dict(trader) que
  contenía solo el trader ACTUAL. Los demás pares abiertos eran invisibles.
  Con 3 LONGs en BTC/ETH/SOL cada trader ve 0 peers — guardia nunca bloqueaba.

  Solución: _POSITION_REGISTRY es un dict global {symbol: {"direction": "LONG"}}
  que todos los traders mantienen. check_correlation() fusiona el registro
  con open_positions para tener el portfolio completo.

  API nueva:
    register_position(symbol, direction)  — llamar tras abrir posición
    unregister_position(symbol)           — llamar al cerrar
    get_portfolio_snapshot()              — dict completo para logs/Telegram

Uso desde strategy.decide():
  ok, reason = check_correlation(
      symbol="BTCUSDT",
      proposed_direction="LONG",
      open_positions={"ETHUSDT": {"direction": "LONG"}, ...}
  )
"""

import logging
import os
from typing import Dict, Tuple

log = logging.getLogger(__name__)

# ── configuración ───────────────────────────────────────────────────────────────────

_ENABLED        = os.getenv("CORR_ENABLED",       "true").lower() not in ("false", "0", "no")
_MAX_GROUP      = int(os.getenv("CORR_MAX_GROUP",      "2"))
_MAX_SAME_DIR   = int(os.getenv("CORR_MAX_SAME_DIR",   "3"))
_MAX_OPEN       = int(os.getenv("CORR_MAX_OPEN",       "5"))

# ── registro global de posiciones (Fix v2) ───────────────────────────────────
# {symbol_upper: {"direction": "LONG" | "SHORT"}}
# Thread-safety: asyncio es single-threaded en el bot; no se necesita lock.
_POSITION_REGISTRY: Dict[str, dict] = {}


def register_position(symbol: str, direction: str) -> None:
    """
    Registra una posición abierta en el portfolio global.
    Llamar desde trading_loop._iteration() justo después de confirmar
    que trader.position pasó de None a no-None.

    Args:
        symbol    — ej. "BTC", "ETH"
        direction — "LONG" o "SHORT"
    """
    key = symbol.upper()
    _POSITION_REGISTRY[key] = {"direction": direction.upper()}
    log.debug("[correlation_guard] REGISTRY +%s %s | total=%d",
              key, direction.upper(), len(_POSITION_REGISTRY))


def unregister_position(symbol: str) -> None:
    """
    Elimina una posición del portfolio global.
    Llamar desde trading_loop._iteration() cuando se detecta cierre
    (externo o por PositionManager).
    """
    key = symbol.upper()
    if key in _POSITION_REGISTRY:
        del _POSITION_REGISTRY[key]
        log.debug("[correlation_guard] REGISTRY -%s | total=%d",
                  key, len(_POSITION_REGISTRY))


def get_portfolio_snapshot() -> dict:
    """
    Devuelve una copia del portfolio completo para logs o notificaciones
    Telegram.

    Retorna:
        {
          "BTC": {"direction": "LONG"},
          "ETH": {"direction": "SHORT"},
          "total": 2,
          "long": ["BTC"],
          "short": ["ETH"],
        }
    """
    snapshot = dict(_POSITION_REGISTRY)
    snapshot["total"] = len(_POSITION_REGISTRY)
    snapshot["long"]  = [s for s, v in _POSITION_REGISTRY.items() if v.get("direction") == "LONG"]
    snapshot["short"] = [s for s, v in _POSITION_REGISTRY.items() if v.get("direction") == "SHORT"]
    return snapshot


# ── grupos de correlación estáticos ────────────────────────────────────────────
# Los símbolos pueden ir con o sin USDT/USDC en el nombre;
# la normalización se hace en _group_of().
#
# Fuente de correlación implícita (rolling 90d, crypto 2024-2026):
#   L1_MAJOR   ≈ 0.85-0.95  entre sí
#   L2_ETH     ≈ 0.80-0.90  entre sí
#   DEFI       ≈ 0.75-0.88  entre sí
#   AI_AGENTS  ≈ 0.70-0.85  entre sí
#
# Añadir / quitar pares editando los grupos o sobreescribiendo con la env var
# CORR_GROUPS_JSON (formato JSON: {"GRUPO": ["SYM1","SYM2",...]}).

_DEFAULT_GROUPS: Dict[str, list] = {
    "L1_MAJOR": ["BTC", "ETH", "SOL", "AVAX", "SUI", "APT"],
    "L2_ETH":   ["ARB", "OP",  "MATIC", "POL", "STRK", "SCROLL"],
    "DEFI":     ["UNI", "AAVE", "CRV",  "SNX", "GMX",  "PENDLE"],
    "AI_AGENTS":["FET", "AGIX", "RENDER","AKT", "IO",   "TAO"],
    "MEME":     ["DOGE","SHIB", "PEPE",  "BONK","WIF",  "FLOKI"],
    "BTC_LAYER2":["STX","ORDI", "SATS",  "RUNES"],
}


def _load_groups() -> Dict[str, list]:
    """Carga grupos desde env var CORR_GROUPS_JSON si existe, si no usa defaults."""
    raw = os.getenv("CORR_GROUPS_JSON", "")
    if raw:
        import json
        try:
            custom = json.loads(raw)
            log.info("[correlation_guard] Grupos cargados desde CORR_GROUPS_JSON: %s",
                     list(custom.keys()))
            return {k: [s.upper() for s in v] for k, v in custom.items()}
        except Exception as exc:  # noqa: BLE001
            log.warning("[correlation_guard] CORR_GROUPS_JSON inválido (%s), usando defaults.", exc)
    return {k: [s.upper() for s in v] for k, v in _DEFAULT_GROUPS.items()}


_GROUPS: Dict[str, list] = _load_groups()

# Mapa inverso: base_symbol -> nombre_de_grupo  (para O(1) lookup)
_SYMBOL_TO_GROUP: Dict[str, str] = {
    sym: group
    for group, symbols in _GROUPS.items()
    for sym in symbols
}


def _base(symbol: str) -> str:
    """Extrae la base de un símbolo normalizado. 'BTCUSDT' -> 'BTC'."""
    s = symbol.upper()
    for suffix in ("USDT", "USDC", "USD", "PERP", "-PERP", "_PERP"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    return s


def _group_of(symbol: str) -> str | None:
    """Retorna el nombre del grupo de correlación del símbolo, o None si no aplica."""
    return _SYMBOL_TO_GROUP.get(_base(symbol))


# ── función pública ───────────────────────────────────────────────────────────────────

def check_correlation(
    proposed_direction: str,
    open_positions: dict,
    symbol: str = "",
) -> Tuple[bool, str]:
    """
    Retorna:
      (True,  "")       → entrada permitida.
      (False, motivo)   → entrada bloqueada.

    Argumentos:
      proposed_direction  : "LONG" o "SHORT".
      open_positions      : dict { symbol: {"direction": "LONG" | "SHORT"} }
                            pasado por trading_loop (puede ser solo el trader actual).
      symbol              : símbolo que se quiere abrir (ej.: "BTC"). Opcional
                            pero necesario para el chequeo de grupo.

    Fix v2: fusiona open_positions con _POSITION_REGISTRY (portfolio global)
    para que la guardia vea TODOS los pares, no solo el trader actual.
    """
    if not _ENABLED:
        return True, ""

    direction = proposed_direction.upper()

    # ── Fix v2: fusionar registry global con open_positions local ───────────
    # open_positions puede estar desactualizado (solo el trader actual);
    # _POSITION_REGISTRY tiene el estado real de todos los traders.
    merged: Dict[str, dict] = {}
    merged.update(_POSITION_REGISTRY)        # estado global completo
    for sym, val in (open_positions or {}).items():  # sobreescribe con info local
        merged[sym.upper()] = val

    # Excluir el propio símbolo (si ya tiene posición, open_order() lo filtra antes)
    if symbol:
        merged.pop(symbol.upper(), None)
        merged.pop(_base(symbol), None)

    positions = list(merged.items())
    total     = len(positions)

    # ── Nivel 2a: límite total de posiciones ────────────────────────────────────
    if total >= _MAX_OPEN:
        msg = (
            f"🔒 límite de posiciones abiertas alcanzado ({total}/{_MAX_OPEN}). "
            "Cierra alguna posición antes de abrir otra."
        )
        log.info("[correlation_guard] %s", msg)
        return False, msg

    # ── Nivel 2b: límite por dirección ───────────────────────────────────────
    same_dir = sum(
        1 for _, p in positions
        if (p.get("direction") or "").upper() == direction
    )
    if same_dir >= _MAX_SAME_DIR:
        msg = (
            f"🔒 demasiadas posiciones {direction} ({same_dir}/{_MAX_SAME_DIR}). "
            "Diversifica antes de añadir otra en la misma dirección."
        )
        log.info("[correlation_guard] %s", msg)
        return False, msg

    # ── Nivel 1: límite por grupo de correlación ─────────────────────────────
    new_group = _group_of(symbol) if symbol else None
    if new_group is not None:
        group_peers = [
            sym for sym, p in positions
            if _group_of(sym) == new_group
            and (p.get("direction") or "").upper() == direction
        ]
        if len(group_peers) >= _MAX_GROUP:
            msg = (
                f"🔒 correlación alta: el grupo {new_group!r} ya tiene "
                f"{len(group_peers)}/{_MAX_GROUP} posiciones {direction} "
                f"({', '.join(group_peers)}). "
                f"Añadir {_base(symbol) if symbol else '?'} incrementaría el riesgo correlacionado."
            )
            log.info("[correlation_guard] %s", msg)
            return False, msg

    log.debug(
        "[correlation_guard] %s %s OK — total=%d same_dir=%d group=%s (registry=%d)",
        symbol or "?", direction, total, same_dir,
        new_group or "ninguno", len(_POSITION_REGISTRY),
    )
    return True, ""
