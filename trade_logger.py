"""trade_logger.py — Persiste trades cerrados en CSV y restaura el estado al arrancar.

NOTA: El tracking de drawdown diario vive en bot_state.py (fuente única de verdad).
Este módulo solo persiste en CSV, mantiene _cache para el resumen diario
y llama a bot_state.record_trade() para actualizar el PnL.

FIX: _cache protegido con _cache_lock (threading.Lock) para evitar race conditions.
FIX: PnL neto real — ganancias y pérdidas se acumulan algebraicamente.
"""
import csv
import logging
import os
import threading
import time
from datetime import datetime, timezone

import bot_state
import config

log = logging.getLogger("trade_logger")

LOG_FILE = os.getenv("TRADES_CSV", "trades.csv")
HEADER   = ["date", "symbol", "side", "entry", "exit",
            "pnl_pct", "pnl_usdt", "score", "reason", "duration_min"]

_csv_lock:   threading.Lock = threading.Lock()
_cache_lock: threading.Lock = threading.Lock()
_cache: list[dict] = []

# ── Win rate monitor ─────────────────────────────────────────────────────────
_winrate_alerted: bool = False


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ── CSV helpers ──────────────────────────────────────────────────────────────

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


def _restore_from_csv() -> None:
    """Lee el CSV al arrancar, reconstruye _cache y restaura el PnL diario en bot_state."""
    today = _today_utc()
    if not os.path.exists(LOG_FILE):
        log.info("CSV no encontrado — arrancando desde 0")
        return

    trades_today: list[dict] = []
    try:
        with open(LOG_FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_date = row.get("date", "")
                # Solo trades del día UTC actual
                if not row_date.startswith(today):
                    continue
                try:
                    t = {
                        "date":     row_date,
                        "symbol":   row["symbol"],
                        "side":     row["side"],
                        "entry":    float(row["entry"]),
                        "exit":     float(row["exit"]),
                        "pnl_pct":  float(row["pnl_pct"]),
                        "pnl_usdt": float(row["pnl_usdt"]),
                        "score":    int(row.get("score", 0)),
                        "reason":   row["reason"],
                        "duration": float(row.get("duration_min", 0)),
                    }
                    trades_today.append(t)
                except (ValueError, KeyError):
                    continue

        with _cache_lock:
            _cache.extend(trades_today)

        # Restaurar PnL neto en bot_state (fuente única de verdad)
        bot_state.restore_from_csv(trades_today)

        pnl_neto = sum(t["pnl_usdt"] for t in trades_today)
        log.info(
            "Restaurados %d trades de hoy — PnL neto: %+.2f USDT",
            len(trades_today), pnl_neto,
        )

    except Exception as e:
        log.warning("Error restaurando desde CSV: %s — arrancando desde 0", e)


# ── Win rate ─────────────────────────────────────────────────────────────────

def _check_winrate() -> None:
    global _winrate_alerted
    lookback  = config.WINRATE_LOOKBACK
    threshold = config.WINRATE_ALERT_PCT

    with _cache_lock:
        snapshot = list(_cache)

    if len(snapshot) < lookback:
        return

    recent   = snapshot[-lookback:]
    wins     = sum(1 for t in recent if t["pnl_pct"] >= 0)
    win_rate = wins / lookback * 100

    if win_rate < threshold and not _winrate_alerted:
        _winrate_alerted = True
        log.warning(
            "Win rate bajo: %.0f%% en últimos %d trades (umbral %.0f%%)",
            win_rate, lookback, threshold,
        )
        try:
            import telegram as _tg
            _tg.notify(
                f"\u26a0\ufe0f <b>Win rate bajo</b>\n"
                f"\u00daltimos {lookback} trades: <code>{win_rate:.0f}%</code> "
                f"({wins}W / {lookback - wins}L)\n"
                f"Umbral: <code>{threshold:.0f}%</code>\n"
                f"Revisa las condiciones de mercado."
            )
        except Exception:
            pass
    elif win_rate >= threshold and _winrate_alerted:
        _winrate_alerted = False
        log.info("Win rate recuperado: %.0f%% en últimos %d trades", win_rate, lookback)


# ── API pública ──────────────────────────────────────────────────────────────

def get_cache_snapshot() -> list[dict]:
    with _cache_lock:
        return list(_cache)


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
    """Persiste el trade en CSV y _cache. NO envía Telegram — eso lo hace main.py."""
    duration = round((time.time() - open_ts) / 60, 1)
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    row = [
        now_str, symbol, side,
        round(entry, 8), round(exit_price, 8),
        round(pnl_pct, 2), round(pnl_usdt, 4),
        score, reason, duration,
    ]
    _write_csv(row)

    with _cache_lock:
        _cache.append({
            "date":     now_str,
            "symbol":   symbol,
            "side":     side,
            "entry":    entry,
            "exit":     exit_price,
            "pnl_pct":  pnl_pct,
            "pnl_usdt": pnl_usdt,
            "score":    score,
            "reason":   reason,
            "duration": duration,
        })

    _check_winrate()


def is_daily_limit_hit() -> bool:
    """Proxy a bot_state para compatibilidad con código existente."""
    return bot_state.is_daily_limit_hit()


# ── Resumen diario ────────────────────────────────────────────────────────────

def send_daily_summary() -> None:
    try:
        import telegram as _tg
    except Exception:
        return

    with _cache_lock:
        snapshot = list(_cache)

    if not snapshot:
        _tg.notify("\U0001f4ca <b>Resumen del d\u00eda</b>\nSin trades en esta sesi\u00f3n.")
        return

    total     = len(snapshot)
    wins      = sum(1 for t in snapshot if t["pnl_pct"] >= 0)
    losses    = total - wins
    total_pnl = sum(t["pnl_usdt"] for t in snapshot)
    win_rate  = wins / total * 100

    lines = [f"\U0001f4ca <b>Resumen de sesi\u00f3n</b> ({total} trades)\n"]
    lines.append(f"Win rate: <code>{win_rate:.0f}%</code> ({wins}W / {losses}L)")
    lines.append(f"PnL total: <code>{total_pnl:+.4f} USDT</code>\n")
    for t in snapshot:
        icon = "\u2705" if t["pnl_pct"] >= 0 else "\u274c"
        lines.append(
            f"{icon} {t['symbol']} {t['side'].upper()} "
            f"{t['pnl_pct']:+.2f}% ({t['pnl_usdt']:+.2f} USDT)"
        )

    _tg.notify("\n".join(lines))

    with _cache_lock:
        _cache.clear()


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
    """Restaura el estado desde CSV y arranca el scheduler de resumen diario."""
    _restore_from_csv()
    threading.Thread(
        target=_daily_summary_scheduler,
        daemon=True,
        name="daily-summary",
    ).start()
    log.info("Scheduler de resumen diario iniciado (00:00 UTC)")
