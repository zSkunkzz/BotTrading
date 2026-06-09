"""telegram.py — Notificaciones Telegram. Un solo archivo, sin dependencias externas."""
import logging

import httpx

import config

log = logging.getLogger("telegram")

API = f"https://api.telegram.org/bot{config.TG_TOKEN}"


def _send(text: str) -> None:
    if not config.TG_TOKEN or not config.TG_CHAT_ID:
        return
    try:
        httpx.post(f"{API}/sendMessage", json={
            "chat_id":    config.TG_CHAT_ID,
            "text":       text,
            "parse_mode": "HTML",
        }, timeout=5)
    except Exception as e:
        log.warning("Telegram error: %s", e)


def notify_open(symbol: str, side: str, price: float, qty: float, sl: float, tp: float) -> None:
    emoji = "🟢" if side == "long" else "🔴"
    _send(
        f"{emoji} <b>OPEN {side.upper()}</b> {symbol}\n"
        f"Entry: <code>{price:.4f}</code>\n"
        f"Qty:   <code>{qty}</code>\n"
        f"SL:    <code>{sl:.4f}</code>\n"
        f"TP:    <code>{tp:.4f}</code>"
    )


def notify_close(symbol: str, side: str, entry: float, exit_p: float, pnl_pct: float, reason: str = "") -> None:
    emoji = "✅" if pnl_pct >= 0 else "❌"
    _send(
        f"{emoji} <b>CLOSE {side.upper()}</b> {symbol}\n"
        f"Entry:  <code>{entry:.4f}</code>\n"
        f"Exit:   <code>{exit_p:.4f}</code>\n"
        f"PnL:    <code>{pnl_pct:+.2f}%</code>\n"
        + (f"Reason: {reason}" if reason else "")
    )


def notify(text: str) -> None:
    """Mensaje libre."""
    _send(text)
