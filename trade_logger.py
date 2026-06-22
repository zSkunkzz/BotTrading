"""trade_logger.py — Persiste trades cerrados en un canal Telegram privado.

Cada trade cerrado se envía como mensaje a TG_LOG_CHAT_ID (puede ser el mismo
chat o un canal/grupo separado sólo para logs).
Además guarda en CSV local como caché hasta el próximo reinicio.

Al arrancar, restaura el daily_loss del día actual leyendo el CSV — así el
limite diario sobrevive reinicios y deploys de Railway.

Variables de entorno:
    TG_LOG_CHAT_ID  : chat_id del canal de logs (si no se define, usa TG_CHAT_ID)

FIX: _cache protegido con _cache_lock (threading.Lock) para evitar race conditions
     entre el thread del loop principal (record/append) y el thread de Telegram
     (_fetch_trade_history / _check_winrate).

NOTA: trade_logger.record() ya NO envía mensaje a Telegram — eso lo hace
     telegram.notify_close() en main.py para evitar mensajes duplicados.
     trade_logger solo persiste en CSV y en _cache para el resumen diario.
"""
import csv
import logging
import os
import threading
import time
from datetime import datetime, timezone

import config

log = logging.getLogger("trade_logger")

LOG_FILE  = os.getenv("TRADES_CSV", "trades.csv")
HEADER    = ["date","symbol","side","entry","exit","pnl_pct","pnl_usdt","score","reason","duration_min"]

_csv_lock:   threading.Lock = threading.Lock()
_cache_lock: threading.Lock = threading.Lock()
_cache: list[dict] = []

# ── Daily drawdown tracking ───────────────────────────────────────────────────
_daily_loss_usdt: float = 0.0
_daily_loss_date: str   = ""
_daily_limit_hit: bool  = False
_daily_loss_lock: threading.Lock = threading.Lock()


def is_daily_limit_hit() -> bool:
    return _daily_limit_hit


def _reset_daily_if_needed() -> None:
    global _daily_loss_usdt, _daily_loss_date, _daily_limit_hit
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _daily_loss_date:
        _daily_loss_date  = today
        _daily_loss_usdt  = 0.0
        _daily_limit_hit  = False
        log.info("Daily drawdown reseteado para %s", today)


def _restore_daily_loss_from_csv() -> None:
    """Lee el CSV al arrancar y recalcula la pérdida acumulada del día UTC actual."""
    global _daily_loss_usdt, _daily_loss_date, _daily_limit_hit

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _daily_loss_date = today

    if not os.path.exists(LOG_FILE):
        log.info("CSV no encontrado — daily loss arranca desde 0")
        return

    recovered_loss = 0.0
    recovered_trades = 0
    try:
        with open(LOG_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("date", "").startswith(today):
                    continue
                try:
                    pnl = float(row["pnl_usdt"])
                    if pnl < 0:
                        recovered_loss += abs(pnl)
                    recovered_trades += 1
                    with _cache_lock:
                        _cache.append({
                            "date":     row["date"],
                            "symbol":   row["symbol"],
                            "side":     row["side"],
                            "entry":    float(row["entry"]),
                            "exit":     float(row["exit"]),
                            "pnl_pct":  float(row["pnl_pct"]),
                            "pnl_usdt": pnl,
                            "score":    int(row.get("score", 0)),
                            "reason":   row["reason"],
                            "duration": float(row.get("duration_min", 0)),
                        })
                except (ValueError, KeyError):
                    continue

        _daily_loss_usdt = recovered_loss
        log.info(
            "Daily loss restaurado desde CSV: -%.2f USDT (%d trades hoy)",
            recovered_loss, recovered_trades,
        )

    except Exception as e:
        log.warning("Error restaurando daily loss desde CSV: %s — arrancando desde 0", e)
        _daily_loss_usdt = 0.0


def _check_daily_drawdown(pnl_usdt: float) -> None:
    global _daily_loss_usdt, _daily_limit_hit
    if pnl_usdt >= 0:
        return
    with _daily_loss_lock:
        _reset_daily_if_needed()
        _daily_loss_usdt += abs(pnl_usdt)
        if not _daily_limit_hit and _daily_loss_usdt >= config.MAX_DAILY_LOSS_USDT:
            _daily_limit_hit = True
            log.warning(
                "🚨 Daily drawdown límite alcanzado: -%.2f USDT (límite %.2f USDT)",
                _daily_loss_usdt, config.MAX_DAILY_LOSS_USDT,
            )


# ── Win rate monitor ──────────────────────────────────────────────────────────
_winrate_alerted: bool = False


def _check_winrate() -> None:
    global _winrate_alerted
    lookback  = config.WINRATE_LOOKBACK
    threshold = config.WINRATE_ALERT_PCT

    with _cache_lock:
        cache_snapshot = list(_cache)

    if len(cache_snapshot) < lookback:
        return

    recent   = cache_snapshot[-lookback:]
    wins     = sum(1 for t in recent if t["pnl_pct"] >= 0)
    win_rate = wins / lookback * 100

    if win_rate < threshold and not _winrate_alerted:
        _winrate_alerted = True
        log.warning(
            "⚠️ Win rate bajo: %.0f%% en últimos %d trades (umbral %.0f%%)",
            win_rate, lookback, threshold,
        )
        # Importar telegram aquí para evitar circular import
        try:
            import telegram as _tg
            _tg.notify(
                f"⚠️ <b>Win rate bajo</b>\n"
                f"Últimos {lookback} trades: <code>{win_rate:.0f}%</code> "
                f"({wins}W / {lookback - wins}L)\n"
                f"Umbral: <code>{threshold:.0f}%</code>\n"
                f"Revisa las condiciones de mercado."
            )
        except Exception:
            pass
    elif win_rate >= threshold and _winrate_alerted:
        _winrate_alerted = False
        log.info("Win rate recuperado: %.0f%% en últimos %d trades", win_rate, lookback)


def get_cache_snapshot() -> list[dict]:
    """Devuelve una copia thread-safe de _cache para lectura externa."""
    with _cache_lock:
        return list(_cache)


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
    """Persiste el trade en CSV y _cache. NO envía mensaje Telegram — eso lo hace main.py."""
    duration = round((time.time() - open_ts) / 60, 1)
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    row = [now_str, symbol, side,
           round(entry, 8), round(exit_price, 8),
           round(pnl_pct, 2), round(pnl_usdt, 4),
           score, reason, duration]

    _write_csv(row)

    with _cache_lock:
        _cache.append({
            "date": now_str, "symbol": symbol, "side": side,
            "entry": entry, "exit": exit_price,
            "pnl_pct": pnl_pct, "pnl_usdt": pnl_usdt,
            "score": score, "reason": reason, "duration": duration,
        })

    _check_daily_drawdown(pnl_usdt)
    _check_winrate()


def send_daily_summary() -> None:
    try:
        import telegram as _tg
    except Exception:
        return

    with _cache_lock:
        cache_snapshot = list(_cache)

    if not cache_snapshot:
        _tg.notify("📊 <b>Resumen del día</b>\nSin trades en esta sesión.")
        return

    total     = len(cache_snapshot)
    wins      = sum(1 for t in cache_snapshot if t["pnl_pct"] >= 0)
    losses    = total - wins
    total_pnl = sum(t["pnl_usdt"] for t in cache_snapshot)
    win_rate  = wins / total * 100

    lines = [f"📊 <b>Resumen de sesión</b> ({total} trades)\n"]
    lines.append(f"Win rate: <code>{win_rate:.0f}%</code> ({wins}W / {losses}L)")
    lines.append(f"PnL total: <code>{total_pnl:+.4f} USDT</code>\n")
    for t in cache_snapshot:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(f"{icon} {t['symbol']} {t['side'].upper()} {t['pnl_pct']:+.2f}%")

    _tg.notify("\n".join(lines))

    with _cache_lock:
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
    """Restaura el estado del día desde CSV y arranca el scheduler de resumen diario."""
    _restore_daily_loss_from_csv()
    threading.Thread(
        target=_daily_summary_scheduler,
        daemon=True,
        name="daily-summary",
    ).start()
    log.info("Scheduler de resumen diario iniciado (00:00 UTC)")
