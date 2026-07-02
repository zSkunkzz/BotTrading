"""telegram.py — Notificaciones Telegram.

fix #4: notify_open incluye el régimen de mercado (bull/bear/proto-bull/proto-bear)
fix #5: notify_close distingue trailing SL en verde de un stop loss real
fix #8: _calc_pnl_net aplica descuento estimado de comisiones taker Hyperliquid
fix #9 (v8): notify_close muestra AMBOS: % precio y % con leverage para claridad.
             notify_trailing añade entry, PnL latente y distancia al TP.
"""
import logging
import time
from datetime import datetime, timezone

import httpx

import config

log = logging.getLogger("telegram")

BASE_URL = f"https://api.telegram.org/bot{config.TG_TOKEN}"

TAKER_FEE_RATE = 0.0004  # 0.04% por lado × 2 lados = 0.08% total por trade


class _RedactTokenFilter(logging.Filter):
    def __init__(self, token: str) -> None:
        super().__init__()
        self._token = token

    def filter(self, record: logging.LogRecord) -> bool:
        if self._token:
            record.msg = str(record.msg).replace(self._token, "***TG_TOKEN***")
            record.args = tuple(
                str(a).replace(self._token, "***TG_TOKEN***") if isinstance(a, str) else a
                for a in (record.args or ())
            )
        return True


def _install_filter() -> None:
    if not config.TG_TOKEN:
        return
    f = _RedactTokenFilter(config.TG_TOKEN)
    for name in ("httpx", "httpcore", "telegram"):
        logging.getLogger(name).addFilter(f)


_install_filter()


def notify(text: str) -> None:
    try:
        httpx.post(
            f"{BASE_URL}/sendMessage",
            json={"chat_id": config.TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning("Telegram error: %s", e)


def notify_open(
    symbol:  str,
    price:   float,
    side:    str,
    qty:     float,
    sl:      float,
    tp:      float,
    score:   int,
    tp_rr:   float,
    regime:  str = "",
) -> None:
    side_icon = "\U0001f7e2" if side == "long" else "\U0001f534"
    arrow     = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    sl_pct    = abs(price - sl) / price * 100
    tp_pct    = abs(tp - price) / price * 100
    regime_line = f"R\u00e9gimen: <code>{regime}</code>\n" if regime else ""
    notify(
        f"{side_icon} <b>NUEVA POSICI\u00d3N</b> {arrow}\n"
        f"Par: <b>{symbol}</b>\n"
        f"Direcci\u00f3n: <b>{side.upper()}</b>\n"
        f"{regime_line}"
        f"Entrada: <code>{price:.6f}</code>\n"
        f"SL: <code>{sl:.6f}</code> (-{sl_pct:.2f}%)\n"
        f"TP: <code>{tp:.6f}</code> (+{tp_pct:.2f}%)\n"
        f"Qty: <code>{qty}</code>\n"
        f"RR: <code>{tp_rr:.1f}</code> | Score: <code>{score}</code>"
    )


def notify_trailing(
    symbol:        str,
    side:          str,
    entry:         float,
    current_price: float,
    new_sl:        float,
    tp:            float,
) -> None:
    """Notificación de trailing SL con contexto completo.

    Muestra:
    - Nuevo SL
    - PnL latente en % precio (sin leverage) y en % real (con leverage)
    - Distancia al TP en %
    """
    lev = getattr(config, "LEVERAGE", 5)

    if side == "long":
        move_pct   = (current_price - entry) / entry * 100
        tp_dist_pct = (tp - current_price) / current_price * 100 if tp else 0.0
        sl_dist_pct = (current_price - new_sl) / current_price * 100
    else:
        move_pct   = (entry - current_price) / entry * 100
        tp_dist_pct = (current_price - tp) / current_price * 100 if tp else 0.0
        sl_dist_pct = (new_sl - current_price) / current_price * 100

    pnl_lev_pct = move_pct * lev
    icon = "\U0001f53c" if side == "long" else "\U0001f53d"

    notify(
        f"{icon} <b>Trailing SL movido</b>\n"
        f"Par: <b>{symbol}</b> {side.upper()}\n"
        f"Entry: <code>{entry:.6f}</code> \u2192 Precio: <code>{current_price:.6f}</code>\n"
        f"Nuevo SL: <code>{new_sl:.6f}</code> ({sl_dist_pct:.2f}% del precio)\n"
        f"TP en: <code>{tp:.6f}</code> (+{tp_dist_pct:.2f}% restante)\n"
        f"PnL latente: <code>{move_pct:+.2f}% precio</code> | "
        f"<code>{pnl_lev_pct:+.2f}% ({lev}x)</code>"
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
    """fix #5: trailing SL en verde no se muestra como STOP LOSS.
    fix #9: muestra % precio (sin leverage) Y % real (con leverage) para claridad.
    """
    lev = getattr(config, "LEVERAGE", 5)
    # % movimiento precio puro (sin leverage) — lo que se mueve el activo
    if side == "long":
        price_move_pct = (exit_p - entry) / entry * 100
    else:
        price_move_pct = (entry - exit_p) / entry * 100

    duration_min = (time.time() - open_ts) / 60 if open_ts else 0
    if duration_min < 60:
        dur_str = f"{duration_min:.0f}m"
    else:
        dur_str = f"{duration_min/60:.1f}h"

    if reason == "TP":
        icon  = "\u2705"
        label = "TAKE PROFIT"
    elif reason == "SL":
        if pnl_usdt > 0:
            icon  = "\u2705"
            label = "TRAILING SL (verde)"
        else:
            icon  = "\u274c"
            label = "STOP LOSS"
    else:
        icon  = "\U0001f7e1"
        label = "CIERRE MANUAL"

    pnl_sign = "+" if pnl_usdt >= 0 else ""
    notify(
        f"{icon} <b>{label}</b>\n"
        f"Par: <b>{symbol}</b> {side.upper()}\n"
        f"Entrada: <code>{entry:.6f}</code>\n"
        f"Salida:  <code>{exit_p:.6f}</code>\n"
        f"Precio: <code>{price_move_pct:+.2f}%</code> | "
        f"Con {lev}x: <code>{pnl_sign}{pnl_pct:.2f}%</code> | "
        f"<code>{pnl_sign}{pnl_usdt:.2f} USDT</code>\n"
        f"Duraci\u00f3n: <code>{dur_str}</code>\n"
        f"PnL hoy: <code>{daily_pnl:+.2f} USDT</code>"
    )
