"""trade_logger.py — Registra trades cerrados en Google Sheets (+ CSV local como fallback).

Configuración (.env):
    GSHEET_CREDS_JSON   : contenido del JSON de la service account (como string)
    GSHEET_ID           : ID de la hoja de Google Sheets
    GSHEET_TAB          : nombre de la pestaña (default: "trades")

Si GSHEET_CREDS_JSON o GSHEET_ID no están definidos, se usa solo CSV local.

Columnas:
    date, symbol, side, entry, exit, pnl_pct, pnl_usdt, score, reason, duration_min
"""
import csv
import json
import logging
import os
import time
from datetime import datetime, timezone

log = logging.getLogger("trade_logger")

LOG_FILE   = os.getenv("TRADES_CSV", "trades.csv")
HEADER     = [
    "date", "symbol", "side", "entry", "exit",
    "pnl_pct", "pnl_usdt", "score", "reason", "duration_min",
]

# ── Google Sheets (opcional) ───────────────────────────────────────────
_sheet = None

def _get_sheet():
    global _sheet
    if _sheet is not None:
        return _sheet

    creds_raw = os.getenv("GSHEET_CREDS_JSON")
    sheet_id  = os.getenv("GSHEET_ID")
    tab_name  = os.getenv("GSHEET_TAB", "trades")

    if not creds_raw or not sheet_id:
        return None

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_dict = json.loads(creds_raw)
        scopes     = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(sheet_id).worksheet(tab_name)

        # Añadir cabecera si la hoja está vacía
        if sheet.row_count == 0 or not sheet.row_values(1):
            sheet.append_row(HEADER)

        _sheet = sheet
        log.info("Google Sheets conectado: %s / %s", sheet_id[:8] + "...", tab_name)
        return _sheet

    except Exception as e:
        log.warning("No se pudo conectar a Google Sheets: %s", e)
        return None


# ── CSV local (fallback) ─────────────────────────────────────────────────
def _write_csv(row: list) -> None:
    write_header = not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(HEADER)
        w.writerow(row)
        f.flush()


# ── API pública ──────────────────────────────────────────────────────────
def record(
    symbol:     str,
    side:       str,
    entry:      float,
    exit_price: float,
    pnl_pct:    float,
    pnl_usdt:   float,
    score:      int,
    reason:     str,
    open_ts:    float,
) -> None:
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

    # 1º intento: Google Sheets
    sheet = _get_sheet()
    if sheet:
        try:
            sheet.append_row(row)
            log.debug("[%s] Trade → Google Sheets: %s %+.2f%%", symbol, reason, pnl_pct)
            return
        except Exception as e:
            log.warning("[%s] Error escribiendo en Sheets, usando CSV: %s", symbol, e)

    # Fallback: CSV local
    try:
        _write_csv(row)
        log.debug("[%s] Trade → CSV local: %s %+.2f%%", symbol, reason, pnl_pct)
    except Exception as e:
        log.warning("[%s] Error escribiendo trades.csv: %s", symbol, e)
