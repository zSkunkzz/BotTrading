"""trade_logger.py — Registra cada trade cerrado en trades.csv.

Columnas:
    date        : fecha y hora UTC del cierre
    symbol      : par
    side        : long | short
    entry       : precio de entrada
    exit        : precio de salida
    pnl_pct     : PnL en % sobre el margen (con leverage)
    pnl_usdt    : PnL en USDT
    score       : score de la señal (0-100)
    reason      : TP | SL | externo
    duration_min: duración del trade en minutos

Uso:
    import trade_logger
    trade_logger.log(symbol, side, entry, exit_price, pnl_pct, pnl_usdt, score, reason, open_ts)
"""
import csv
import os
import time
import logging
from datetime import datetime, timezone

log = logging.getLogger("trade_logger")

LOG_FILE = os.getenv("TRADES_CSV", "trades.csv")

HEADER = [
    "date", "symbol", "side", "entry", "exit",
    "pnl_pct", "pnl_usdt", "score", "reason", "duration_min",
]


def _ensure_header() -> None:
    if not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0:
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow(HEADER)


def record(
    symbol:       str,
    side:         str,
    entry:        float,
    exit_price:   float,
    pnl_pct:      float,
    pnl_usdt:     float,
    score:        int,
    reason:       str,
    open_ts:      float,   # time.time() del momento de apertura
) -> None:
    """Añade una fila al CSV. Thread-safe (append mode + flush)."""
    _ensure_header()
    duration = round((time.time() - open_ts) / 60, 1)
    row = [
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        symbol,
        side,
        round(entry,      6),
        round(exit_price, 6),
        round(pnl_pct,    2),
        round(pnl_usdt,   4),
        score,
        reason,
        duration,
    ]
    try:
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
            f.flush()
        log.debug("[%s] Trade logueado: %s %+.2f%%", symbol, reason, pnl_pct)
    except Exception as e:
        log.warning("Error escribiendo trades.csv: %s", e)
