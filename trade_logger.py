"""trade_logger.py — Persiste trades cerrados en CSV y restaura el estado al arrancar.

FIX: _cache protegido con _cache_lock (threading.Lock) para evitar race conditions.
FIX: PnL neto real — ganancias y pérdidas se acumulan algebraicamente.
FIX: send_daily_summary limpia solo trades del día anterior, nunca los del día nuevo.

GIST: Si GIST_TOKEN y GIST_ID están configurados en variables de entorno,
      trades.csv se sincroniza con un Gist de GitHub tras cada trade y al arrancar.
      Esto garantiza persistencia entre reinicios del contenedor Railway.
      El CSV local se restaura desde el Gist al arrancar si está vacío.

v17: Añadida columna breakdown para guardar desglose de contribución de indicadores.
"""
import csv
import io
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import httpx

import bot_state
import config

log = logging.getLogger("trade_logger")

LOG_FILE   = os.getenv("TRADES_CSV", "trades.csv")
GIST_TOKEN = os.getenv("GIST_TOKEN", "")
GIST_ID    = os.getenv("GIST_ID", "")
HEADER     = ["date", "symbol", "side", "entry", "exit",
              "pnl_pct", "pnl_usdt", "score", "reason", "duration_min", "breakdown"]

_csv_lock:   threading.Lock = threading.Lock()
_cache_lock: threading.Lock = threading.Lock()
_cache: list[dict] = []

# ── Win rate monitor ───────────────────────────────────────────────────────────────
_winrate_alerted: bool = False


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ── Gist helpers ─────────────────────────────────────────────────────────────────

def _gist_configured() -> bool:
    return bool(GIST_TOKEN and GIST_ID)


def _gist_push(content: str) -> None:
    if not _gist_configured():
        return
    try:
        resp = httpx.patch(
            f"https://api.github.com/gists/{GIST_ID}",
            headers={
                "Authorization": f"Bearer {GIST_TOKEN}",
                "Accept": "application/vnd.github+json",
            },
            json={"files": {"trades.csv": {"content": content}}},
            timeout=10,
        )
        if resp.status_code == 200:
            log.debug("Gist actualizado correctamente")
        else:
            log.warning("Gist push error %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        log.warning("Gist push exception: %s", e)


def _gist_push_async() -> None:
    if not _gist_configured():
        return
    try:
        with _csv_lock:
            if not os.path.exists(LOG_FILE):
                return
            with open(LOG_FILE, "r") as f:
                content = f.read()
        threading.Thread(
            target=_gist_push,
            args=(content,),
            daemon=True,
            name="gist-push",
        ).start()
    except Exception as e:
        log.warning("Gist push async error: %s", e)


def _gist_pull() -> str | None:
    if not _gist_configured():
        return None
    try:
        resp = httpx.get(
            f"https://api.github.com/gists/{GIST_ID}",
            headers={
                "Authorization": f"Bearer {GIST_TOKEN}",
                "Accept": "application/vnd.github+json",
            },
            timeout=10,
        )
        if resp.status_code != 200:
            log.warning("Gist pull error %d", resp.status_code)
            return None
        files = resp.json().get("files", {})
        csv_file = files.get("trades.csv")
        if not csv_file:
            return None
        if csv_file.get("truncated"):
            raw = httpx.get(csv_file["raw_url"], timeout=15)
            return raw.text if raw.status_code == 200 else None
        return csv_file.get("content")
    except Exception as e:
        log.warning("Gist pull exception: %s", e)
        return None


def _restore_from_gist() -> bool:
    content = _gist_pull()
    if not content or not content.strip():
        log.info("Gist vacío o no disponible — arrancando desde 0")
        return False
    try:
        with _csv_lock:
            with open(LOG_FILE, "w", newline="") as f:
                f.write(content)
        lines = [l for l in content.strip().splitlines() if l]
        trade_count = max(0, len(lines) - 1)
        log.info("CSV restaurado desde Gist: %d trades", trade_count)
        return True
    except Exception as e:
        log.warning("Error escribiendo CSV desde Gist: %s", e)
        return False


# ── CSV helpers ────────────────────────────────────────────────────────────────

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
    _gist_push_async()


def _restore_from_csv() -> None:
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
                        "breakdown": row.get("breakdown"),
                    }
                    trades_today.append(t)
                except (ValueError, KeyError):
                    continue

        with _cache_lock:
            _cache.extend(trades_today)

        bot_state.restore_from_csv(trades_today)

        pnl_neto = sum(t["pnl_usdt"] for t in trades_today)
        log.info(
            "Restaurados %d trades de hoy — PnL neto: %+.2f USDT",
            len(trades_today), pnl_neto,
        )

    except Exception as e:
        log.warning("Error restaurando desde CSV: %s — arrancando desde 0", e)


# ── Win rate ───────────────────────────────────────────────────────────────────

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


# ── API pública ──────────────────────────────────────────────────────────────────

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
    breakdown:  dict | None = None,
) -> None:
    duration = round((time.time() - open_ts) / 60, 1)
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    breakdown_json = json.dumps(breakdown) if breakdown else ""

    row = [
        now_str, symbol, side,
        round(entry, 8), round(exit_price, 8),
        round(pnl_pct, 2), round(pnl_usdt, 4),
        score, reason, duration, breakdown_json,
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
            "breakdown": breakdown,
        })

    _check_winrate()


def is_daily_limit_hit() -> bool:
    return bot_state.is_daily_limit_hit()


# ── Resumen diario ──────────────────────────────────────────────────────────────────

def send_daily_summary() -> None:
    try:
        import telegram as _tg
    except Exception:
        return

    today = _today_utc()

    with _cache_lock:
        yesterday_trades = [t for t in _cache if not t["date"].startswith(today)]
        today_trades     = [t for t in _cache if t["date"].startswith(today)]
        snapshot = list(yesterday_trades)
        _cache.clear()
        _cache.extend(today_trades)

    if not snapshot:
        _tg.notify("📊 <b>Resumen del día</b>\nSin trades en esta sesión.")
        return

    total     = len(snapshot)
    wins      = sum(1 for t in snapshot if t["pnl_pct"] >= 0)
    losses    = total - wins
    total_pnl = sum(t["pnl_usdt"] for t in snapshot)
    win_rate  = wins / total * 100

    lines = [f"📊 <b>Resumen de sesión</b> ({total} trades)\n"]
    lines.append(f"Win rate: <code>{win_rate:.0f}%</code> ({wins}W / {losses}L)")
    lines.append(f"PnL total: <code>{total_pnl:+.4f} USDT</code>\n")
    for t in snapshot:
        icon = "✅" if t["pnl_pct"] >= 0 else "❌"
        lines.append(
            f"{icon} {t['symbol']} {t['side'].upper()} "
            f"{t['pnl_pct']:+.2f}% ({t['pnl_usdt']:+.2f} USDT)"
        )
    if today_trades:
        lines.append(
            f"\n<i>ℹ️ {len(today_trades)} trade(s) del nuevo día ya en cache.</i>"
        )

    _tg.notify("\n".join(lines))


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
    if _gist_configured():
        log.info("Gist configurado (ID: %s...) — restaurando CSV", GIST_ID[:8])
        _restore_from_gist()
    else:
        log.info("Gist no configurado — usando CSV local únicamente")

    _restore_from_csv()

    threading.Thread(
        target=_daily_summary_scheduler,
        daemon=True,
        name="daily-summary",
    ).start()
    log.info("Scheduler de resumen diario iniciado (00:00 UTC)")