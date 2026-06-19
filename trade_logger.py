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
import threading
import time
from datetime import datetime, timezone

import httpx

import config
import telegram as _tg

log = logging.getLogger("trade_logger")

LOG_FILE  = os.getenv("TRADES_CSV", "trades.csv")
HEADER    = ["date","symbol","side","entry","exit","pnl_pct","pnl_usdt","score","reason","duration_min"]

_API         = f"https://api.telegram.org/bot{config.TG_TOKEN}"
_LOG_CHAT_ID = os.getenv("TG_LOG_CHAT_ID") or config.TG_CHAT_ID

_csv_lock: threading.Lock = threading.Lock()
_cache: list[dict] = []

# ── Daily drawdown tracking ───────────────────────────────────────────────────
_daily_loss_usdt: float = 0.0          # pérdida acumulada hoy (solo trades negativos)
_daily_loss_date: str   = ""           # fecha UTC del día actual (YYYY-MM-DD)
_daily_limit_hit: bool  = False        # True si se alcanzó el límite hoy
_daily_loss_lock: threading.Lock = threading.Lock()


def is_daily_limit_hit() -> bool:
    """Devuelve True si el bot debe pausarse por daily drawdown."""
    return _daily_limit_hit


def _reset_daily_if_needed() -> None:
    """Resetea el contador diario si cambió el día UTC."""
    global _daily_loss_usdt, _daily_loss_date, _daily_limit_hit
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _daily_loss_date:
        _daily_loss_date  = today
        _daily_loss_usdt  = 0.0
        _daily_limit_hit  = False
        log.info("Daily drawdown reseteado para %s", today)


def _check_daily_drawdown(pnl_usdt: float) -> None:
    """Acumula pérdida y activa el límite si se supera MAX_DAILY_LOSS_USDT."""
    global _daily_loss_usdt, _daily_limit_hit
    if pnl_usdt >= 0:
        return
    with _daily_loss_lock:
        _reset_daily_if_needed()
        _daily_loss_usdt += abs(pnl_usdt)
        if not _daily_limit_hit and _daily_loss_usdt >= config.MAX_DAILY_LOSS_USDT:
            _daily_limit_hit = True
            log.warning(
                "🚨 Daily drawdown límite alcanzado: -%.2f USDT (límite %.2f USDT) — "
                "bot pausado hasta mañana UTC",
                _daily_loss_usdt, config.MAX_DAILY_LOSS_USDT,
            )
            _tg.notify(
                f"🚨 <b>Daily drawdown límite alcanzado</b>\n"
                f"Pérdida acumulada hoy: <code>-{_daily_loss_usdt:.2f} USDT</code>\n"
                f"Límite: <code>{config.MAX_DAILY_LOSS_USDT:.2f} USDT</code>\n"
                f"⛔ No se abrirán nuevas posiciones hasta mañana (00:00 UTC)."
            )


# ── Win rate monitor ──────────────────────────────────────────────────────────
_winrate_alerted: bool = False   # evita spam: solo avisa una vez por racha mala


def _check_winrate() -> None:
    """Alerta si el win rate de los últimos N trades cae por debajo del umbral."""
    global _winrate_alerted
    lookback  = config.WINRATE_LOOKBACK
    threshold = config.WINRATE_ALERT_PCT

    if len(_cache) < lookback:
        return

    recent   = _cache[-lookback:]
    wins     = sum(1 for t in recent if t["pnl_pct"] >= 0)
    win_rate = wins / lookback * 100

    if win_rate < threshold and not _winrate_alerted:
        _winrate_alerted = True
        log.warning(
            "⚠️ Win rate bajo: %.0f%% en últimos %d trades (umbral %.0f%%)",
            win_rate, lookback, threshold,
        )
        _tg.notify(
            f"⚠️ <b>Win rate bajo</b>\n"
            f"Últimos {lookback} trades: <code>{win_rate:.0f}%</code> "
            f"({wins}W / {lookback - wins}L)\n"
            f"Umbral: <code>{threshold:.0f}%</code>\n"
            f"Revisa las condiciones de mercado."
        )
    elif win_rate >= threshold and _winrate_alerted:
        # Se recuperó — resetear alerta
        _winrate_alerted = False
        log.info("Win rate recuperado: %.0f%% en últimos %d trades", win_rate, lookback)


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
    with _csv_lock:
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
    _write_csv(row)

    _cache.append({
        "date": now_str, "symbol": symbol, "side": side,
        "entry": entry, "exit": exit_price,
        "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
        "score": score, "reason": reason, "duration": duration,
    })

    # ── Post-trade checks ──────────────────────────────────────────────────
    _check_daily_drawdown(pnl_usdt)
    _check_winrate()


def send_daily_summary() -> None:
    if not _cache:
        _tg_send("📊 <b>Resumen del día</b>\nSin trades en esta sesión.")
        return

    total     = len(_cache)
    wins      = sum(1 for t in _cache if t["pnl_pct"] >= 0)
    losses    = total - wins
    total_pnl = sum(t["pnl_usdt"] for t in _cache)
    win_rate  = wins / total * 100

    lines = [f"📊 <b>Resumen de sesión</b> ({total} trades)\n"]
    lines.append(f"Win rate: <code>{win_rate:.0f}%</code> ({wins}W / {losses}L)")
    lines.append(f"PnL total: <code>{total_pnl:+.4f} USDT</code>\n")
    for t in _cache:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(f"{icon} {t['symbol']} {t['side'].upper()} {t['pnl_pct']:+.2f}%")

    _tg_send("\n".join(lines))
    # Reset caché y contadores diarios tras el resumen de medianoche
    _cache.clear()
    with _daily_loss_lock:
        global _daily_loss_usdt, _daily_loss_date, _daily_limit_hit
        _daily_loss_usdt = 0.0
        _daily_loss_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _daily_limit_hit = False


def _daily_summary_scheduler() -> None:
    while True:
        now = datetime.now(timezone.utc)
        seconds_until_midnight = (
            (23 - now.hour) * 3600
            + (59 - now.minute) * 60
            + (60 - now.second)
        )
        time.sleep(seconds_until_midnight)
        try:
            send_daily_summary()
        except Exception as e:
            log.warning("Error en resumen diario: %s", e)
        time.sleep(5)


def start_scheduler() -> None:
    threading.Thread(
        target=_daily_summary_scheduler,
        daemon=True,
        name="daily-summary",
    ).start()
    log.info("Scheduler de resumen diario iniciado (00:00 UTC)")
