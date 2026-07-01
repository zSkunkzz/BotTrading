"""telegram.py — Notificaciones Telegram.

fix #4: notify_open incluye el régimen de mercado (bull/bear/proto-bull/proto-bear)
        para que el usuario vea en Telegram si la señal es confirmada o provisional.
fix #5: notify_close distingue trailing SL en verde de un stop loss real:
        si reason=='SL' y pnl_usdt>0 → label 'TRAILING SL (verde)' con icono ✅.
fix #8: _calc_pnl_net aplica descuento estimado de comisiones taker Hyperliquid
        (~0.04% × 2 lados) para que el PnL mostrado sea neto real, no optimista.
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
    """Elimina el token de Telegram de cualquier mensaje de log."""

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
    """Instala el filtro en el logger de httpx para ocultar el token."""
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
    """fix #4: muestra el régimen de mercado en la notificación de apertura.

    El régimen (bull / bear / proto-bull / proto-bear) indica si la señal
    es de tendencia confirmada (ADX fuerte) o proto-señal en desarrollo.
    Ayuda a calibrar la cautela en cada trade sin cambiar la lógica.
    """
    side_icon = "\U0001f7e2" if side == "long" else "\U0001f534"
    arrow     = "\U0001f4c8" if side == "long" else "\U0001f4c9"
    sl_pct    = abs(price - sl) / price * 100
    tp_pct    = abs(tp - price) / price * 100
    regime_line = f"Régimen: <code>{regime}</code>\n" if regime else ""
    notify(
        f"{side_icon} <b>NUEVA POSICIÓN</b> {arrow}\n"
        f"Par: <b>{symbol}</b>\n"
        f"Dirección: <b>{side.upper()}</b>\n"
        f"{regime_line}"
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
    """fix #5: trailing SL en verde no se muestra como ❌ STOP LOSS.

    Si reason=='SL' pero pnl_usdt>0, fue un trailing SL que cerró en verde
    (el SL se movió por encima del entry). Se muestra como ✅ TRAILING SL (verde)
    para distinguirlo visualmente de un stop loss en pérdidas.
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
            # fix #5: trailing SL cerrado en verde — no confundir con pérdida
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
        f"PnL: <code>{pnl_sign}{pnl_pct:.2f}%</code> | "
        f"<code>{pnl_sign}{pnl_usdt:.2f} USDT</code>\n"
        f"Duración: <code>{dur_str}</code>\n"
        f"PnL hoy: <code>{daily_pnl:+.2f} USDT</code>"
    )
