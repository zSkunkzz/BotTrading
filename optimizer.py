"""optimizer.py — Auto‑optimizaci�n de par�metros cada 3 d�as.

Lee el historial de trades desde el Gist, calcula m�tricas por s�mbolo y
direcci�n, y ajusta los pesos de signals.py + umbrales din�micos.

Los cambios se guardan en weights.json para no modificar el c�digo fuente.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import config
import trade_logger

log = logging.getLogger("optimizer")

WEIGHTS_FILE = "weights.json"
LAST_RUN_FILE = "last_optimization.txt"
OPTIMIZE_INTERVAL_HOURS = 72  # 3 d�as

# Par�metros por defecto (copia de signals.py v6.8)
DEFAULT_WEIGHTS = {
    "W_ADX_1H_30": 15,
    "W_ADX_1H_25": 13,
    "W_ADX_1H_20": 12,
    "W_ADX_1H_15": 9,
    "W_MACD_1H": 10,
    "W_RSI_IDEAL": 16,
    "W_MACD_15M": 10,
    "W_VOLUME_HIGH": 14,
    "W_STRUCTURE": 16,
    "W_DIVERGENCIA": 8,
    "W_VELA": 8,
    "W_VOLUME_LOW": -5,
    "W_RSI_SOBRE": -9,
    "W_STRUCTURE_CONTRA": -5,
    "W_MACRO_CONTRA": -4,
    "W_BEAR_EMA200_15M": -4,
    "W_BEAR_LOW_ADX": -2,
    "W_MACD_15M_CONTRA": -2,
    # Umbrales din�micos
    "MIN_SCORE": 70,
    "SHORT_MIN_SCORE_EXTRA": 0,
    "MARGIN_USDT": 20,
    "SL_MIN_PCT": 0.012,
    "PROTO_ADX_MIN": 22,
}

# L�mites de seguridad
MARGIN_MIN = 10
MARGIN_MAX = 50
SCORE_MIN = 60
SCORE_MAX = 85
SL_MIN_PCT_MIN = 0.008
SL_MIN_PCT_MAX = 0.025
PROTO_ADX_MIN_MIN = 18
PROTO_ADX_MIN_MAX = 28


def _fetch_trades_from_gist(days: int = 90) -> list[dict]:
    """Descarga el CSV desde el Gist y lo convierte a lista de diccionarios."""
    content = trade_logger._gist_pull()
    if not content or not content.strip():
        log.warning("No se pudo descargar Gist — usando cache local")
        return trade_logger.get_cache_snapshot()

    import csv
    from io import StringIO
    reader = csv.DictReader(StringIO(content))
    trades = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    for row in reader:
        try:
            date_str = row.get("date", "")
            if not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            except ValueError:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            if dt.replace(tzinfo=timezone.utc) < cutoff:
                continue
            trades.append({
                "date": date_str,
                "symbol": row["symbol"],
                "side": row["side"],
                "entry": float(row["entry"]),
                "exit": float(row["exit"]),
                "pnl_pct": float(row["pnl_pct"]),
                "pnl_usdt": float(row["pnl_usdt"]),
                "score": int(row.get("score", 0)),
                "reason": row["reason"],
                "duration": float(row.get("duration_min", 0)),
            })
        except (ValueError, KeyError):
            continue
    return trades


def _calculate_metrics(trades: list[dict]) -> dict:
    """Retorna m�tricas agregadas por s�mbolo, direcci�n y global."""
    if not trades:
        return {}
    n = len(trades)
    total_pnl = sum(t["pnl_usdt"] for t in trades)
    wins = [t for t in trades if t["pnl_usdt"] > 0]
    losses = [t for t in trades if t["pnl_usdt"] < 0]
    win_rate = len(wins) / n if n else 0

    avg_win = sum(w["pnl_usdt"] for w in wins) / len(wins) if wins else 0
    avg_loss = abs(sum(l["pnl_usdt"] for l in losses) / len(losses)) if losses else 0
    profit_factor = avg_win / avg_loss if avg_loss > 0 else float('inf')

    returns = [t["pnl_usdt"] for t in trades]
    mean_return = sum(returns) / n
    std_return = (sum((r - mean_return) ** 2 for r in returns) / n) ** 0.5
    sharpe = (mean_return / std_return) * (252 ** 0.5) if std_return > 0 else 0

    # Por s�mbolo
    symbol_stats = {}
    for t in trades:
        sym = t["symbol"]
        if sym not in symbol_stats:
            symbol_stats[sym] = {"trades": 0, "pnl": 0.0, "wins": 0}
        symbol_stats[sym]["trades"] += 1
        symbol_stats[sym]["pnl"] += t["pnl_usdt"]
        if t["pnl_usdt"] > 0:
            symbol_stats[sym]["wins"] += 1

    for sym in symbol_stats:
        s = symbol_stats[sym]
        s["win_rate"] = s["wins"] / s["trades"] if s["trades"] else 0

    # Por direcci�n
    side_stats = {}
    for t in trades:
        side = t["side"]
        if side not in side_stats:
            side_stats[side] = {"trades": 0, "pnl": 0.0, "wins": 0}
        side_stats[side]["trades"] += 1
        side_stats[side]["pnl"] += t["pnl_usdt"]
        if t["pnl_usdt"] > 0:
            side_stats[side]["wins"] += 1

    for side in side_stats:
        s = side_stats[side]
        s["win_rate"] = s["wins"] / s["trades"] if s["trades"] else 0

    return {
        "total_trades": n,
        "total_pnl": total_pnl,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "sharpe": sharpe,
        "symbols": symbol_stats,
        "sides": side_stats,
    }


def _recommend_changes(metrics: dict) -> dict:
    """Genera recomendaciones basadas en m�tricas."""
    recs = {}
    if not metrics:
        return recs

    wr = metrics["win_rate"]
    total_n = metrics["total_trades"]

    # 1. MIN_SCORE
    if total_n >= 20:
        current_min_score = DEFAULT_WEIGHTS.get("MIN_SCORE", 70)
        if wr < 0.40:
            new_score = min(SCORE_MAX, current_min_score + 5)
            recs["MIN_SCORE"] = new_score
        elif wr > 0.60:
            new_score = max(SCORE_MIN, current_min_score - 3)
            recs["MIN_SCORE"] = new_score
        else:
            recs["MIN_SCORE"] = current_min_score

    # 2. SHORT_MIN_SCORE_EXTRA
    sides = metrics.get("sides", {})
    long_stats = sides.get("long", {})
    short_stats = sides.get("short", {})
    if long_stats.get("trades", 0) >= 10 and short_stats.get("trades", 0) >= 10:
        short_wr = short_stats.get("win_rate", 0)
        long_wr = long_stats.get("win_rate", 0)
        if short_wr < long_wr - 0.10:
            recs["SHORT_MIN_SCORE_EXTRA"] = 2
        elif short_wr > long_wr + 0.10:
            recs["SHORT_MIN_SCORE_EXTRA"] = 0
        else:
            recs["SHORT_MIN_SCORE_EXTRA"] = 1
    else:
        recs["SHORT_MIN_SCORE_EXTRA"] = 0

    # 3. MARGIN_USDT (seg�n Sharpe)
    sharpe = metrics.get("sharpe", 0)
    current_margin = float(getattr(config, "MARGIN_USDT", 20))
    if total_n >= 20:
        if sharpe > 1.5:
            new_margin = min(MARGIN_MAX, current_margin * 1.08)
            recs["MARGIN_USDT"] = round(new_margin, 1)
        elif sharpe < 0.5:
            new_margin = max(MARGIN_MIN, current_margin * 0.92)
            recs["MARGIN_USDT"] = round(new_margin, 1)
        else:
            recs["MARGIN_USDT"] = current_margin

    # 4. SL_MIN_PCT (basado en ratio de pérdidas y volatilidad)
    # Si muchos SLs (ratio SL/TP > 1.5) y win_rate bajo, aumentar SL_MIN_PCT ligeramente
    sl_trades = [t for t in metrics.get("trades", []) if t.get("reason") == "SL"]  # not available in metrics
    # Usamos una heurística: si win_rate < 40% y profit_factor < 0.8, SL puede ser demasiado ajustado
    if total_n >= 20:
        current_sl_pct = DEFAULT_WEIGHTS.get("SL_MIN_PCT", 0.012)
        if wr < 0.40 and metrics.get("profit_factor", 0) < 0.8:
            new_sl = min(SL_MIN_PCT_MAX, current_sl_pct + 0.002)
            recs["SL_MIN_PCT"] = round(new_sl, 3)
        elif wr > 0.55 and metrics.get("profit_factor", 0) > 1.2:
            new_sl = max(SL_MIN_PCT_MIN, current_sl_pct - 0.001)
            recs["SL_MIN_PCT"] = round(new_sl, 3)
        else:
            recs["SL_MIN_PCT"] = current_sl_pct

    # 5. PROTO_ADX_MIN (si hay muchos proto y mal rendimiento, endurecer)
    # No tenemos datos de régimen en CSV, así que lo dejamos fijo por ahora
    recs["PROTO_ADX_MIN"] = DEFAULT_WEIGHTS.get("PROTO_ADX_MIN", 22)

    return recs


def _apply_weights(recommendations: dict) -> None:
    """Guarda las recomendaciones en weights.json y aplica en caliente."""
    if not recommendations:
        return

    # Cargar existente o usar default
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE, "r") as f:
                current = json.load(f)
        except:
            current = DEFAULT_WEIGHTS.copy()
    else:
        current = DEFAULT_WEIGHTS.copy()

    # Aplicar solo las keys que vinieron
    for key, value in recommendations.items():
        current[key] = value

    with open(WEIGHTS_FILE, "w") as f:
        json.dump(current, f, indent=2)

    log.info("Pesos actualizados guardados en %s", WEIGHTS_FILE)

    # Aplicar en caliente a config y signals
    try:
        import signals
        import risk
        for key, value in recommendations.items():
            if hasattr(signals, key):
                setattr(signals, key, value)
                log.debug("signals.%s = %s aplicado", key, value)
            if hasattr(config, key):
                setattr(config, key, value)
                log.debug("config.%s = %s aplicado", key, value)
            if hasattr(risk, key):
                setattr(risk, key, value)
                log.debug("risk.%s = %s aplicado", key, value)
        # Forzar recarga de sobreescrituras en signals
        if hasattr(signals, "_WEIGHTS_OVERRIDE"):
            signals._WEIGHTS_OVERRIDE = current
    except Exception as e:
        log.warning("No se pudieron aplicar cambios en caliente: %s", e)


def should_run() -> bool:
    """Devuelve True si pasaron >= OPTIMIZE_INTERVAL_HOURS desde la �ltima ejecuci�n."""
    if not os.path.exists(LAST_RUN_FILE):
        return True
    try:
        with open(LAST_RUN_FILE, "r") as f:
            ts = float(f.read().strip())
        return (time.time() - ts) >= (OPTIMIZE_INTERVAL_HOURS * 3600)
    except:
        return True


def mark_run() -> None:
    with open(LAST_RUN_FILE, "w") as f:
        f.write(str(time.time()))


def optimize() -> dict:
    """Ejecuta todo el flujo de optimizaci�n. Retorna las recomendaciones."""
    log.info("Iniciando optimizaci�n cada 72h...")
    trades = _fetch_trades_from_gist(days=90)
    if len(trades) < 15:
        log.info("Trades insuficientes (%d < 15) — sin cambios", len(trades))
        return {}

    metrics = _calculate_metrics(trades)
    if not metrics:
        return {}

    log.info(
        "M�tricas: %d trades | PnL=%.2f | WR=%.0f%% | Sharpe=%.2f",
        metrics["total_trades"], metrics["total_pnl"],
        metrics["win_rate"] * 100, metrics["sharpe"],
    )

    recs = _recommend_changes(metrics)
    if recs:
        log.info("Recomendaciones generadas: %s", recs)
        _apply_weights(recs)
        # Notificar por Telegram
        try:
            import telegram
            msg = (
                f"\U0001f4ca <b>Optimizaci�n 72h completada</b>\n"
                f"Trades analizados: <code>{metrics['total_trades']}</code>\n"
                f"PnL: <code>{metrics['total_pnl']:+.2f} USDT</code>\n"
                f"Win Rate: <code>{metrics['win_rate']*100:.0f}%</code>\n"
                f"Sharpe: <code>{metrics['sharpe']:.2f}</code>\n\n"
                f"<b>Cambios aplicados:</b>\n"
            )
            for k, v in recs.items():
                msg += f"  {k}: <code>{v}</code>\n"
            telegram.notify(msg)
        except:
            pass
    else:
        log.info("No se generaron recomendaciones significativas")

    mark_run()
    return recs