"""
market_snapshot.py — Parser determinista de tabla de mercados del exchange.

Convierte el texto pegado de la UI (Hyperliquid u otro exchange compatible)
en la misma estructura que produce PairScanner._last_scored, sin ninguna
llamada a Gemini, Groq ni ninguna otra IA.

Formato esperado por bloque (un mercado = 7 líneas):
  Línea 0: SYMBOL-QUOTE          ej. "BTC-USDC"
  Línea 1: MAX_LEVERAGE          ej. "40x"
  Línea 2: COLLATERAL_TYPE       ej. "xyz" | "hyna" | "km" | "flx" | "cash" | "vntl"
  Línea 3: (precio oracle — OPCIONAL, presente solo en algunos pares)
  Línea 4: MARK_PRICE            ej. "73.655" o "73,655"
  Línea 5: CHANGE_24H            ej. "-300 / -0,40%" o "+1.3 / +0.02%"
  Línea 6: FUNDING_RATE          ej. "0,0100%"
  Línea 7: VOLUME_24H            ej. "$1.030.611.468"
  Línea 8: OPEN_INTEREST         ej. "$2.204.983.435"

Pares inactivos (precio == "--") son ignorados automáticamente.

Uso básico:
    from bot.market_snapshot import parse_snapshot, snapshot_to_scanner_format

    raw_text = open("snapshot.txt").read()        # o pegar directamente
    markets  = parse_snapshot(raw_text)            # list[MarketRow]
    scored   = snapshot_to_scanner_format(markets) # misma estructura que PairScanner

Integración con PairScanner:
    scanner.inject_snapshot(raw_text)              # sobreescribe _last_scored
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("MarketSnapshot")

# ---------------------------------------------------------------------------
# Tipos de colateral conocidos (el campo identifica si hay línea extra)
# ---------------------------------------------------------------------------
# Colaterales que NO insertan línea de precio oracle (precio spot omitido)
_COLLATERAL_NO_ORACLE = {"xyz", "cash", "hyna", "km", "flx", "vntl", ""}
# Los demás (número de colateral o desconocido) podrían tener línea extra


@dataclass
class MarketRow:
    """Un mercado parseado de la tabla del exchange."""
    symbol: str                    # ej. "BTC"
    quote: str                     # ej. "USDC"
    max_leverage: int              # ej. 40
    collateral: str                # ej. "xyz"
    mark_price: float              # precio de mercado actual
    change_abs: float              # cambio absoluto 24h
    change_pct: float              # cambio % 24h (sin signo para filtros)
    funding_rate: float            # funding rate en % (ej. 0.0100)
    volume_24h: float              # volumen 24h en USD
    open_interest: float           # OI en USD
    active: bool = True            # False si precio es "--"
    raw: str = field(default="", repr=False)  # bloque original para debug


# ---------------------------------------------------------------------------
# Helpers de parseo
# ---------------------------------------------------------------------------

def _clean_number(s: str) -> str:
    """Normaliza separadores de miles y decimales al estilo float."""
    s = s.strip()
    # Eliminar símbolo $ y espacios
    s = s.replace("$", "").replace(" ", "")
    # Detectar si usa punto como separador de miles y coma como decimal
    # Ej: "1.030.611.468" → "1030611468", "73,655" → "73.655", "-0,40%" → "-0.40"
    s = s.replace("%", "")
    # Contar puntos y comas
    n_dots  = s.count(".")
    n_comms = s.count(",")
    if n_dots > 1:
        # Puntos son separadores de miles → eliminar
        s = s.replace(".", "")
        if n_comms == 1:
            s = s.replace(",", ".")
    elif n_comms > 1:
        # Comas son separadores de miles → eliminar
        s = s.replace(",", "")
    elif n_dots == 1 and n_comms == 1:
        # Ambos presentes: el que está al final es el decimal
        dot_pos  = s.rfind(".")
        comm_pos = s.rfind(",")
        if comm_pos > dot_pos:
            # Coma es decimal, punto es miles → "1.234,56" → "1234.56"
            s = s.replace(".", "").replace(",", ".")
        else:
            # Punto es decimal, coma es miles → "1,234.56" → "1234.56"
            s = s.replace(",", "")
    elif n_comms == 1 and n_dots == 0:
        # Solo coma → decimal europeo: "73,655" → "73.655"
        s = s.replace(",", ".")
    return s


def _parse_float(s: str, default: float = 0.0) -> float:
    try:
        return float(_clean_number(s))
    except (ValueError, TypeError):
        return default


def _parse_leverage(s: str) -> int:
    """'40x' → 40"""
    try:
        return int(re.sub(r"[^0-9]", "", s))
    except (ValueError, TypeError):
        return 1


def _parse_change_line(s: str) -> tuple[float, float]:
    """
    Parsea líneas como:
      "-300 / -0,40%"  → (-300.0, -0.40)
      "+1,3 / +0,02%" → (1.3, 0.02)
      "--"             → (0.0, 0.0)
    Devuelve (change_abs, change_pct) — change_pct puede ser negativo.
    """
    s = s.strip()
    if s == "--" or not s:
        return 0.0, 0.0
    parts = [p.strip() for p in s.split("/")]
    if len(parts) == 2:
        change_abs = _parse_float(parts[0])
        change_pct = _parse_float(parts[1].replace("%", ""))
        return change_abs, change_pct
    return 0.0, 0.0


# ---------------------------------------------------------------------------
# Constantes de detección de líneas
# ---------------------------------------------------------------------------

_COLLATERAL_TOKENS = {
    "xyz", "hyna", "km", "flx", "cash", "vntl",
}

def _is_collateral_line(s: str) -> bool:
    return s.strip().lower() in _COLLATERAL_TOKENS

def _is_leverage_line(s: str) -> bool:
    return bool(re.fullmatch(r"\d+x", s.strip(), re.IGNORECASE))

def _is_symbol_line(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]+-[A-Z]+", s.strip().upper()))

def _is_price_line(s: str) -> bool:
    """Precio numérico (puede ser '--' para inactivos)."""
    s = s.strip()
    if s == "--":
        return True
    clean = _clean_number(s)
    try:
        v = float(clean)
        return v > 0
    except (ValueError, TypeError):
        return False

def _is_change_line(s: str) -> bool:
    return "/" in s or s.strip() == "--"

def _is_funding_line(s: str) -> bool:
    s = s.strip()
    # Funding rate: número con %, puede ser negativo
    return bool(re.match(r"^-?\d+[,.]\d+%$", s)) or s == "0,0000%" or s == "0.0000%"

def _is_dollar_amount(s: str) -> bool:
    s = s.strip()
    return s.startswith("$") and s != "$"


# ---------------------------------------------------------------------------
# Parser principal
# ---------------------------------------------------------------------------

def parse_snapshot(text: str) -> list[MarketRow]:
    """
    Parsea el texto completo de la tabla de mercados y devuelve
    una lista de MarketRow (solo mercados activos por defecto).

    El algoritmo es un state-machine tolerante: detecta el tipo de
    cada línea por su formato, no por posición fija, para manejar
    la presencia/ausencia de la línea de precio oracle.
    """
    lines = [l.rstrip() for l in text.splitlines()]
    # Filtrar líneas completamente vacías
    lines = [l for l in lines if l.strip()]

    markets: list[MarketRow] = []
    i = 0
    n = len(lines)

    while i < n:
        # Buscar inicio de bloque: línea de símbolo
        if not _is_symbol_line(lines[i]):
            i += 1
            continue

        # Bloque encontrado — intentar parsear
        block_start = i
        raw_lines: list[str] = []

        # Línea 0: símbolo
        sym_line = lines[i].strip().upper()   # ej. "BTC-USDC"
        raw_lines.append(sym_line)
        i += 1
        if i >= n:
            break

        # Línea 1: apalancamiento
        if not _is_leverage_line(lines[i]):
            continue  # bloque malformado
        lev_line = lines[i].strip()
        raw_lines.append(lev_line)
        i += 1
        if i >= n:
            break

        # Línea 2: colateral
        if not _is_collateral_line(lines[i]):
            continue
        coll_line = lines[i].strip().lower()
        raw_lines.append(coll_line)
        i += 1
        if i >= n:
            break

        # Línea opcional: precio oracle (número numérico sin $, sin /)
        # Solo está presente en algunos mercados — lo detectamos y saltamos
        if _is_price_line(lines[i]) and not _is_change_line(lines[i]) and not _is_dollar_amount(lines[i]):
            # Podría ser el precio oracle O el mark_price — leemos el siguiente
            # para distinguir: si el siguiente también es precio → esta es oracle
            candidate_oracle = lines[i].strip()
            if i + 1 < n and _is_price_line(lines[i + 1]) and not _is_change_line(lines[i + 1]):
                # Hay dos precios consecutivos → primera es oracle, segunda es mark
                raw_lines.append(f"oracle:{candidate_oracle}")
                i += 1  # consumir oracle
            # Ahora i apunta al mark_price (sea oracle o no)

        # Mark price
        if i >= n:
            break
        mark_line = lines[i].strip()
        raw_lines.append(mark_line)
        i += 1
        if i >= n:
            break

        # Change 24h
        if not _is_change_line(lines[i]):
            continue
        change_line = lines[i].strip()
        raw_lines.append(change_line)
        i += 1
        if i >= n:
            break

        # Funding rate
        if not _is_funding_line(lines[i]):
            continue
        funding_line = lines[i].strip()
        raw_lines.append(funding_line)
        i += 1
        if i >= n:
            break

        # Volumen 24h
        if not _is_dollar_amount(lines[i]):
            continue
        vol_line = lines[i].strip()
        raw_lines.append(vol_line)
        i += 1
        if i >= n:
            break

        # Open interest
        if not _is_dollar_amount(lines[i]):
            continue
        oi_line = lines[i].strip()
        raw_lines.append(oi_line)
        i += 1

        # --- Construir MarketRow ---
        parts = sym_line.split("-", 1)
        symbol = parts[0]
        quote  = parts[1] if len(parts) > 1 else "USDC"

        active = mark_line != "--"

        if active:
            mark_price = _parse_float(mark_line)
        else:
            mark_price = 0.0

        change_abs, change_pct = _parse_change_line(change_line)
        funding    = _parse_float(funding_line.replace("%", ""))
        volume_24h = _parse_float(vol_line)
        oi         = _parse_float(oi_line)

        row = MarketRow(
            symbol=symbol,
            quote=quote,
            max_leverage=_parse_leverage(lev_line),
            collateral=coll_line,
            mark_price=mark_price,
            change_abs=change_abs,
            change_pct=change_pct,
            funding_rate=funding,
            volume_24h=volume_24h,
            open_interest=oi,
            active=active,
            raw="\n".join(raw_lines),
        )
        markets.append(row)
        logger.debug("[snapshot] parsed: %s  price=%.4f  vol=$%.0f  oi=$%.0f  active=%s",
                     sym_line, mark_price, volume_24h, oi, active)

    logger.info("[MarketSnapshot] Parseados %d mercados (%d activos)",
                len(markets), sum(1 for m in markets if m.active))
    return markets


# ---------------------------------------------------------------------------
# Conversión al formato de PairScanner._last_scored
# ---------------------------------------------------------------------------

def snapshot_to_scanner_format(
    markets: list[MarketRow],
    *,
    min_volume_usdt: float = 1_000_000,
    min_change_pct: float  = 0.0,
    top_n: int             = 50,
    exclude_quotes: set    = None,
    exclude_collateral: set = None,
) -> list[dict]:
    """
    Convierte lista de MarketRow en la estructura que usa PairScanner._last_scored.

    Filtros aplicados:
      - Solo mercados activos (price != "--")
      - Volumen >= min_volume_usdt
      - |change_pct| >= min_change_pct
      - Excluir quotes/colaterales no deseados

    Score = (vol_M * 0.6) + (|change_pct| * 0.4)  — mismo que PairScanner
    """
    if exclude_quotes is None:
        exclude_quotes = set()          # ej. {"USDE", "USDH", "USDT"}
    if exclude_collateral is None:
        exclude_collateral = set()      # ej. {"hyna", "km"}

    scored: list[dict] = []
    for m in markets:
        if not m.active:
            continue
        if m.volume_24h < min_volume_usdt:
            continue
        if abs(m.change_pct) < min_change_pct:
            continue
        if m.quote.upper() in {q.upper() for q in exclude_quotes}:
            continue
        if m.collateral.lower() in {c.lower() for c in exclude_collateral}:
            continue

        vol_m = m.volume_24h / 1_000_000
        score = vol_m * 0.6 + abs(m.change_pct) * 0.4

        scored.append({
            "symbol":        m.symbol,
            "quote":         m.quote,
            "collateral":    m.collateral,
            "max_leverage":  m.max_leverage,
            "volume_usdt":   round(vol_m, 2),
            "change_pct":    round(m.change_pct, 4),
            "last_price":    m.mark_price,
            "funding":       round(m.funding_rate, 5),
            "oi_usdt":       round(m.open_interest / 1_000_000, 2),
            "score":         round(score, 3),
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_n]


# ---------------------------------------------------------------------------
# CLI rápido para debug
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import json

    logging.basicConfig(level=logging.DEBUG)
    text = sys.stdin.read() if not sys.stdin.isatty() else ""
    if not text:
        print("Uso: echo '<texto>' | python -m bot.market_snapshot")
        sys.exit(1)

    rows   = parse_snapshot(text)
    scored = snapshot_to_scanner_format(rows)
    print(json.dumps(scored, indent=2, ensure_ascii=False))
