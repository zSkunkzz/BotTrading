"""trade_logger.py — Persiste trades cerrados en un canal Telegram privado.

Cada trade cerrado se envía como mensaje a TG_LOG_CHAT_ID (puede ser el mismo
chat o un canal/grupo separado sólo para logs).
Además guarda en CSV local como caché hasta el próximo reinicio.

Variables de entorno:
    TG_LOG_CHAT_ID  : chat_id del canal de logs (si no se define, usa TG_CHAT_ID)

Formato del mensaje:
    📌 BTC-USDT | LONG | TP ✅
    Entry:    67420.0000
    Exit:     68850.0000
    PnL:      +4.23% | +8.46 USDT
    Score:    78 | Dur: 187 min
    2026-06-09 09:14 UTC
"""
import csv
import logging
import os
import time
from datetime import datetime, timezone

import httpx

import config

log = logging.getLogger("trade_logger")

LOG_FILE = os.getenv("TRADES_CSV", "trades.csv")
HEADER   = ["date","symbol","side","entry","exit","pnl_pct","pnl_usdt","score","reason","duration_min"]

_API         = f"https://api.telegram.org/bot{config.TG_TOKEN}"
_LOG_CHAT_ID = os.getenv("TG_LOG_CHAT_ID") or config.TG_CHAT_ID

# caché en memoria (se pierde al reiniciar, pero Telegram tiene el historial completo)
_cache: list[dict] = []


def _tg_send(text: str) -> None:
    if not config.TG_TOKEN or not _LOG_CHAT_ID:
        return
    try:
        httpx.post(f"{_API}/sendMessage", json={
            "chat_id":    _LOG_CHAT_ID,
            "text":       text,
            "parse_mode": "HTML",
        }, timeout=5)
    except Exception as e:
        log.warning("trade_logger TG error: %s", e)


def _write_csv(row: list) -> None:
    write_header = not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0
    try:
        with open(LOG_FILE, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(HEADER)
            w.writerow(row)
            f.flush()
    except Exception as e:
        log.warning("CSV write error: %s", e)


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
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    row = [now_str, symbol, side,
           round(entry, 6), round(exit_price, 6),
           round(pnl_pct, 2), round(pnl_usdt, 4),
           score, reason, duration]

    # ── 1. Telegram log ──
    result_icon = "✅" if pnl_pct >= 0 else "❌"
    side_icon   = "🟢" if side == "long" else "🔴"
    msg = (
        f"📌 <b>{symbol}</b> | {side_icon} {side.upper()} | {reason} {result_icon}\n"
        f"Entry:    <code>{entry:.4f}</code>\n"
        f"Exit:     <code>{exit_price:.4f}</code>\n"
        f"PnL:      <code>{pnl_pct:+.2f}%</code> | <code>{pnl_usdt:+.4f} USDT</code>\n"
        f"Score:    <code>{score}</code> | Dur: <code>{duration} min</code>\n"
        f"<i>{now_str} UTC</i>"
    )
    _tg_send(msg)

    # ── 2. CSV local (caché) ──
    _write_csv(row)

    # ── 3. Caché en memoria ──
    _cache.append({
        "date": now_str, "symbol": symbol, "side": side,
        "entry": entry, "exit": exit_price,
        "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
        "score": score, "reason": reason, "duration": duration,
    })


def send_daily_summary() -> None:
    """Envía un resumen de todos los trades de la sesión actual.
    Llama a esta función desde un scheduler o manualmente.
    """
    if not _cache:
        _tg_send("📊 <b>Resumen del día</b>\nSin trades en esta sesión.")
        return

    total   = len(_cache)
    wins    = sum(1 for t in _cache if t["pnl_pct"] >= 0)
    losses  = total - wins
    total_pnl = sum(t["pnl_usdt"] for t in _cache)
    win_rate  = wins / total * 100

    lines = [f"📊 <b>Resumen de sesión</b> ({total} trades)\n"]
    lines.append(f"Win rate: <code>{win_rate:.0f}%</code> ({wins}W / {losses}L)")
    lines.append(f"PnL total: <code>{total_pnl:+.4f} USDT</code>\n")
    for t in _cache:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(f"{icon} {t['symbol']} {t['side'].upper()} {t['pnl_pct']:+.2f}%")

    _tg_send("\n".join(lines))
