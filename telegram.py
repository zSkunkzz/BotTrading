"""telegram.py — Notificaciones Telegram."""
import logging
import time

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


def _fmt_qty(qty: float) -> str:
    """Formatea qty eliminando ceros superfluos."""
    if qty >= 1:
        return f"{qty:.4f}".rstrip("0").rstrip(".")
    return f"{qty:.8f}".rstrip("0").rstrip(".")


def notify_open(
    symbol: str,
    side: str,
    price: float,
    qty: float,
    sl: float,
    tp: float,
    score: int = 0,
    tp_rr: float = 0.0,
) -> None:
    emoji      = "\U0001f7e2" if side == "long" else "\U0001f534"
    sl_pct     = abs(price - sl) / price * 100
    tp_pct     = abs(tp - price) / price * 100
    margin     = round(qty * price / config.LEVERAGE, 2)
    rr_str     = f"{tp_rr:.1f}" if tp_rr else "?"
    score_str  = f"{score}" if score else "?"
    _send(
        f"{emoji} <b>ABIERTA \u2014 {symbol}</b>\n"
        "\u2500" * 20 + "\n"
        f"Direcci\u00f3n: <b>{side.upper()}</b> | Score: <b>{score_str}</b> | RR: <b>{rr_str}</b>\n"
        f"Entry:  <code>{price:.4f}</code>\n"
        f"SL:     <code>{sl:.4f}</code>  <i>(-{sl_pct:.2f}%)</i>\n"
        f"TP:     <code>{tp:.4f}</code>  <i>(+{tp_pct:.2f}%)</i>\n"
        f"Qty:    <code>{_fmt_qty(qty)}</code>\n"
        f"Margen: <code>{margin} USDT</code> @ {config.LEVERAGE}x"
    )


def notify_close(
    symbol: str,
    side: str,
    entry: float,
    exit_p: float,
    pnl_pct: float,
    pnl_usdt: float = 0.0,
    reason: str = "",
    open_ts: float = 0.0,
) -> None:
    emoji = "\u2705" if pnl_pct >= 0 else "\u274c"

    # Duraci\u00f3n
    if open_ts:
        dur_s = int(time.time() - open_ts)
        h, rem = divmod(dur_s, 3600)
        m, _   = divmod(rem, 60)
        dur_str = f"{h}h {m}m" if h else f"{m}m"
    else:
        dur_str = "?"

    # Limpiar reason (quitar emoji duplicado si viene de main.py)
    reason_clean = reason.replace(" \u2705", "").replace(" \u274c", "").strip()
    reason_icon  = "\u2705" if reason_clean == "TP" else ("\u274c" if reason_clean == "SL" else "")
    reason_str   = f"{reason_icon} {reason_clean}".strip()

    pnl_usdt_str = f"{pnl_usdt:+.4f} USDT" if pnl_usdt else ""

    _send(
        f"{emoji} <b>CERRADA \u2014 {symbol}</b>\n"
        + "\u2500" * 20 + "\n"
        + f"Direcci\u00f3n: <b>{side.upper()}</b> | Raz\u00f3n: <b>{reason_str}</b>\n"
        f"Entry:  <code>{entry:.4f}</code>\n"
        f"Exit:   <code>{exit_p:.4f}</code>\n"
        f"PnL:    <code>{pnl_pct:+.2f}%</code>"
        + (f"  <code>{pnl_usdt_str}</code>" if pnl_usdt_str else "") +
        f"\nDur:    <code>{dur_str}</code>"
    )


def notify(text: str) -> None:
    """Mensaje libre."""
    _send(text)
