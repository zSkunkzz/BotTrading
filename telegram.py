"""telegram.py — Notificaciones Telegram."""
import logging
import time
from datetime import datetime, timezone

import httpx

import config

log = logging.getLogger("telegram")

BASE_URL = f"https://api.telegram.org/bot{config.TG_TOKEN}"


def notify(text: str) -> None:
    """Envía un mensaje HTML al chat configurado."""
    try:
        httpx.post(
            f"{BASE_URL}/sendMessage",
            json={"chat_id": config.TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning("Telegram error: %s", e)


def notify_open(
    symbol: str,
    price:  float,
    side:   str,
    qty:    float,
    sl:     float,
    tp:     float,
    score:  int,
    tp_rr:  float,
) -> None:
    side_icon = "\U0001f7e2" if side == "long" else "\U0001f534"
    arrow     = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    sl_pct    = abs(price - sl) / price * 100
    tp_pct    = abs(tp - price) / price * 100
    notify(
        f"{side_icon} <b>NUEVA POSICIÓN</b> {arrow}\n"
        f"Par: <b>{symbol}</b>\n"
        f"Dirección: <b>{side.upper()}</b>\n"
        f"Entrada: <code>{price:.6f}</code>\n"
        f"SL: <code>{sl:.6f}</code> (-{sl_pct:.2f}%)\n"
        f"TP: <code>{tp:.6f}</code> (+{tp_pct:.2f}%)\n"
        f"Qty: <code>{qty}</code>\n"
        f"RR: <code>{tp_rr:.1f}</code> | Score: <code>{score}</code>"
    )


def notify_close(
    symbol:    str,
    side:      str,
    entry:     float,
    exit_p:    float,
    pnl_pct:   float,
    pnl_usdt:  float,
    reason:    str,
    open_ts:   float,
    daily_pnl: float = 0.0,
) -> None:
    duration_min = (time.time() - open_ts) / 60 if open_ts else 0
    if duration_min < 60:
        dur_str = f"{duration_min:.0f}m"
    else:
        dur_str = f"{duration_min/60:.1f}h"

    if reason == "TP":
        icon = "\u2705"
        label = "TAKE PROFIT"
    elif reason == "SL":
        icon = "\u274c"
        label = "STOP LOSS"
    else:
        icon = "\U0001f7e1"
        label = "CIERRE MANUAL"

    pnl_sign = "+" if pnl_usdt >= 0 else ""
    notify(
        f"{icon} <b>{label}</b>\n"
        f"Par: <b>{symbol}</b> {side.upper()}\n"
        f"Entrada: <code>{entry:.6f}</code>\n"
        f"Salida:  <code>{exit_p:.6f}</code>\n"
        f"PnL: <code>{pnl_sign}{pnl_pct:.2f}%</code> | "
        f"<code>{pnl_sign}{pnl_usdt:.2f} USDT</code>\n"
        f"Duración: <code>{dur_str}</code>\n"
        f"PnL hoy: <code>{daily_pnl:+.2f} USDT</code>"
    )
