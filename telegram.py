"""telegram.py — Notificaciones Telegram.

fix #4: notify_open incluye el régimen de mercado.
fix #5: notify_close distingue trailing SL en verde de SL real.
fix #8: _calc_pnl_net aplica descuento de comisiones taker.
fix #9: notify_trailing_sl — nueva función dedicada con contexto completo
        (entry, PnL latente, distancia al TP) y debounce por par.
        notify_open ahora sí recibe y muestra el parámetro regime.
        notify_close muestra PnL separado: % precio y % con leverage para
        evitar confusión entre movimiento de precio y beneficio real.
"""
import logging
import time
from datetime import datetime, timezone

import httpx

import config

log = logging.getLogger("telegram")

BASE_URL = f"https://api.telegram.org/bot{config.TG_TOKEN}"

TAKER_FEE_RATE = 0.0004  # 0.04% por lado × 2 lados = 0.08% total

# Debounce trailing SL: mínimo N segundos entre mensajes del mismo par
TRAILING_DEBOUNCE_SECS = 300  # 5 minutos
_trailing_last_notify: dict[str, float] = {}


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
    """Notificación de apertura de posición.

    fix #9: regime ahora sí se muestra — antes _open_position no lo pasaba.
    Muestra SL/TP con % respecto al precio de entrada.
    """
    side_icon = "\U0001f7e2" if side == "long" else "\U0001f534"
    arrow     = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    sl_pct    = abs(price - sl) / price * 100
    tp_pct    = abs(tp - price) / price * 100
    regime_label = regime.upper() if regime else "—"
    notify(
        f"{side_icon} <b>NUEVA POSICIÓN</b> {arrow}\n"
        f"Par: <b>{symbol}</b> | Régimen: <code>{regime_label}</code>\n"
        f"Dirección: <b>{side.upper()}</b>\n"
        f"Entrada: <code>{price:.6f}</code>\n"
        f"SL: <code>{sl:.6f}</code>  (-{sl_pct:.2f}% precio / -{sl_pct * config.LEVERAGE:.1f}% cuenta)\n"
        f"TP: <code>{tp:.6f}</code>  (+{tp_pct:.2f}% precio / +{tp_pct * config.LEVERAGE:.1f}% cuenta)\n"
        f"Qty: <code>{qty}</code> | RR: <code>{tp_rr:.1f}</code> | Score: <code>{score}</code>"
    )


def notify_trailing_sl(
    symbol:        str,
    side:          str,
    old_sl:        float,
    new_sl:        float,
    current_price: float,
    entry:         float,
    tp:            float,
) -> bool:
    """Notificación de movimiento de trailing SL con contexto completo.

    fix #9: nueva función dedicada que sustituye al telegram.notify() inline
    en _update_trailing. Incluye:
      - PnL latente en % precio y % cuenta
      - Distancia restante al TP
      - Cuánto se movió el SL
      - Debounce: devuelve False y no envía si el último mensaje fue hace
        menos de TRAILING_DEBOUNCE_SECS para este par.

    Devuelve True si el mensaje fue enviado, False si fue suprimido por debounce.
    """
    now = time.time()
    last = _trailing_last_notify.get(symbol, 0.0)
    if now - last < TRAILING_DEBOUNCE_SECS:
        return False
    _trailing_last_notify[symbol] = now

    icon = "\U0001f53c" if side == "long" else "\U0001f53d"

    # PnL latente en % precio
    if side == "long":
        pnl_price_pct = (current_price - entry) / entry * 100
        dist_to_tp    = (tp - current_price) / current_price * 100
        sl_move       = new_sl - old_sl
    else:
        pnl_price_pct = (entry - current_price) / entry * 100
        dist_to_tp    = (current_price - tp) / current_price * 100
        sl_move       = old_sl - new_sl

    pnl_account_pct = pnl_price_pct * config.LEVERAGE
    pnl_sign        = "+" if pnl_price_pct >= 0 else ""

    notify(
        f"{icon} <b>Trailing SL</b> — {symbol} {side.upper()}\n"
        f"SL: <code>{old_sl:.6f}</code> → <code>{new_sl:.6f}</code>  "
        f"(+{sl_move:.6f})\n"
        f"Precio actual: <code>{current_price:.6f}</code>\n"
        f"PnL latente: <code>{pnl_sign}{pnl_price_pct:.2f}%</code> precio "
        f"| <code>{pnl_sign}{pnl_account_pct:.1f}%</code> cuenta\n"
        f"Distancia al TP: <code>{dist_to_tp:.2f}%</code>"
    )
    return True


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
    """Notificación de cierre de posición.

    fix #5: trailing SL en verde no se muestra como ❌ STOP LOSS.
    fix #9: muestra PnL separado — % movimiento precio y % cuenta con leverage.
            Evita confusión entre "-15% cuenta" y "-1.5% precio".
    """
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

    # pnl_pct viene calculado en main con leverage aplicado (movimiento × lev).
    # Calculamos también el % de movimiento puro del precio para contexto.
    if entry > 0 and exit_p > 0:
        if side == "long":
            price_move_pct = (exit_p - entry) / entry * 100
        else:
            price_move_pct = (entry - exit_p) / entry * 100
    else:
        price_move_pct = 0.0

    pnl_sign  = "+" if pnl_usdt >= 0 else ""
    move_sign = "+" if price_move_pct >= 0 else ""

    notify(
        f"{icon} <b>{label}</b>\n"
        f"Par: <b>{symbol}</b> {side.upper()}\n"
        f"Entrada: <code>{entry:.6f}</code>\n"
        f"Salida:  <code>{exit_p:.6f}</code>  "
        f"({move_sign}{price_move_pct:.2f}% precio)\n"
        f"PnL cuenta: <code>{pnl_sign}{pnl_pct:.2f}%</code> "
        f"| <code>{pnl_sign}{pnl_usdt:.2f} USDT</code>\n"
        f"Duración: <code>{dur_str}</code> "
        f"| PnL hoy: <code>{daily_pnl:+.2f} USDT</code>"
    )
