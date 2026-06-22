"""telegram.py — Notificaciones Telegram."""
import logging
import time

import httpx

import config

log = logging.getLogger("telegram")

API = f"https://api.telegram.org/bot{config.TG_TOKEN}"

SEP = "─" * 20


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


def _fmt_price(price: float) -> str:
    """Formatea un precio con precisión adaptativa según su magnitud.

    - Precios >= 1000   : 2 decimales  (BTC, ETH...)
    - Precios >= 1      : 4 decimales  (SOL, BNB...)
    - Precios >= 0.01   : 6 decimales  (XRP, DOGE...)
    - Precios >= 0.0001 : 8 decimales  (SHIB, PEPE...)
    - Precios menores   : notación e   (tokens muy pequeños)
    """
    if price == 0:
        return "0"
    if price >= 1000:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.01:
        return f"{price:.6f}"
    if price >= 0.0001:
        return f"{price:.8f}"
    # Tokens extremadamente pequeños — notación científica limpia
    return f"{price:.2e}"


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
    emoji     = "🟢" if side == "long" else "🔴"
    sl_pct    = abs(price - sl) / price * 100 if price else 0
    tp_pct    = abs(tp - price) / price * 100 if price else 0
    margin    = round(qty * price / config.LEVERAGE, 2) if price else 0
    rr_str    = f"{tp_rr:.1f}" if tp_rr else "?"
    score_str = f"{score}" if score else "?"
    _send(
        f"{emoji} <b>ABIERTA — {symbol}</b>\n"
        f"{SEP}\n"
        f"Dirección: <b>{side.upper()}</b> | Score: <b>{score_str}</b> | RR: <b>{rr_str}</b>\n"
        f"Entry:  <code>{_fmt_price(price)}</code>\n"
        f"SL:     <code>{_fmt_price(sl)}</code>  <i>(-{sl_pct:.2f}%)</i>\n"
        f"TP:     <code>{_fmt_price(tp)}</code>  <i>(+{tp_pct:.2f}%)</i>\n"
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
    emoji = "✅" if pnl_pct >= 0 else "❌"

    if open_ts:
        dur_s = int(time.time() - open_ts)
        h, rem = divmod(dur_s, 3600)
        m, _   = divmod(rem, 60)
        dur_str = f"{h}h {m}m" if h else f"{m}m"
    else:
        dur_str = "?"

    reason_clean = reason.replace(" ✅", "").replace(" ❌", "").strip()
    reason_icon  = "✅" if reason_clean == "TP" else ("❌" if reason_clean == "SL" else "")
    reason_str   = f"{reason_icon} {reason_clean}".strip()
    pnl_usdt_str = f"{pnl_usdt:+.4f} USDT" if pnl_usdt else ""

    _send(
        f"{emoji} <b>CERRADA — {symbol}</b>\n"
        f"{SEP}\n"
        f"Dirección: <b>{side.upper()}</b> | Razón: <b>{reason_str}</b>\n"
        f"Entry:  <code>{_fmt_price(entry)}</code>\n"
        f"Exit:   <code>{_fmt_price(exit_p)}</code>\n"
        f"PnL:    <code>{pnl_pct:+.2f}%</code>"
        + (f"  <code>{pnl_usdt_str}</code>" if pnl_usdt_str else "") +
        f"\nDur:    <code>{dur_str}</code>"
    )


def notify(text: str) -> None:
    """Mensaje libre."""
    _send(text)
