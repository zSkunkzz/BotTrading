"""optimizer.py — Auto‑optimizaci�n cada 72h con backtesting integrado (60 d�as).

Descarga velas hist�ricas, simula la estrategia en 60 d�as, combina trades
reales (desde Gist) + simulados, calcula m�tricas y ajusta par�metros en caliente.
Incluye optimizaci�n de pesos de indicadores por correlaci�n con PnL.

Ejecuci�n en hilo separado para no bloquear el trading en vivo.
"""
import csv
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from collections import defaultdict

import config
import exchange
import signals
import risk
import trade_logger

log = logging.getLogger("optimizer")

# ── Archivos y constantes ────────────────────────────────────────────────────
WEIGHTS_FILE = "weights.json"
LAST_RUN_FILE = "last_optimization.txt"
OPTIMIZE_INTERVAL_HOURS = 72  # 3 d�as

# ── Par�metros por defecto ──────────────────────────────────────────────────
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

# ── Threading ───────────────────────────────────────────────────────────────
_optimizer_thread = None
_optimizer_running = False
_OPTIMIZER_LOCK = threading.Lock()

# ── Constantes para backtesting ────────────────────────────────────────────
CACHE_DIR = "ohlcv_cache"
CACHE_TTL = 86400 * 3  # 3 d�as
SIMULATE_DAYS = 60  # 60 d�as
TIMEFRAMES = ["15m", "1h", "4h"]

# Copiamos constantes de main.py para simulaci�n
MIN_HOLD_SECS = 90
COOLDOWN_SL = 60 * 60
COOLDOWN_TP = 30 * 60
WEEKDAY_MIN_SCORE = getattr(config, "WEEKDAY_MIN_SCORE", 70)
WEEKEND_MIN_SCORE = getattr(config, "WEEKEND_MIN_SCORE", 90)
VALID_SIDES = {"long", "short"}
MAX_SAME_SIDE = getattr(config, "MAX_SAME_SIDE", 4)


# ════════════════════════════════════════════════════════════════════════════
#   BACKTESTING (simulaci�n hist�rica)
# ════════════════════════════════════════════════════════════════════════════

class _SimulatedPosition:
    def __init__(self, symbol, side, entry, qty, sl, tp, trail_step, score, open_ts):
        self.symbol = symbol
        self.side = side
        self.entry = entry
        self.qty = qty
        self.sl = sl
        self.tp = tp
        self.tp_original = tp
        self.trail_step = trail_step
        self.trail_high = entry
        self.trail_low = entry
        self.score = score
        self.open_ts = open_ts
        self.be_trigger = None
        self.be_sl = None
        self.be_locked = False
        self.tp_extensions = 0
        self._extending = False


class _SimulatedExchange:
    def __init__(self, price_series: dict[str, list[float]]):
        self.price_series = price_series
        self.current_idx = 0
        self.positions = {}
        self.trades = []

    def set_current_index(self, idx):
        self.current_idx = idx

    def get_price(self, symbol):
        return self.price_series[symbol][self.current_idx]

    def get_position(self, symbol):
        return self.positions.get(symbol)

    def get_all_positions(self):
        return self.positions

    def open_position(self, symbol, side, qty, sl, tp):
        price = self.get_price(symbol)
        pos = _SimulatedPosition(symbol, side, price, qty, sl, tp, 0, 0, time.time())
        self.positions[symbol] = pos
        log.debug("[SIM] Abierta %s %s @ %.6f", symbol, side.upper(), price)

    def close_position(self, symbol, exit_price, reason):
        if symbol not in self.positions:
            return
        pos = self.positions.pop(symbol)
        if pos.side == "long":
            price_move = (exit_price - pos.entry) / pos.entry
        else:
            price_move = (pos.entry - exit_price) / pos.entry
        pnl_pct = price_move * config.LEVERAGE * 100
        pnl_usdt = price_move * pos.qty * pos.entry
        trade = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "symbol": symbol,
            "side": pos.side,
            "entry": pos.entry,
            "exit": exit_price,
            "pnl_pct": pnl_pct,
            "pnl_usdt": pnl_usdt,
            "score": pos.score,
            "reason": reason,
            "duration_min": (time.time() - pos.open_ts) / 60,
            "breakdown": None,
        }
        self.trades.append(trade)

    # Dummies para compatibilidad
    def cancel_trigger_orders(self, symbol):
        pass

    def place_stop_order(self, symbol, side, qty, sl):
        pass

    def place_tp_order(self, symbol, side, qty, tp):
        pass

    def _round_price(self, coin, price):
        return exchange._round_price(coin, price)


def _download_historical_data(symbols, days=60) -> dict:
    cache_file = os.path.join(CACHE_DIR, "ohlcv_cache.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                cache = json.load(f)
            cache_time = cache.get("_timestamp", 0)
            if time.time() - cache_time < CACHE_TTL:
                log.info("Cargando velas desde cach� (%s)", cache_file)
                return cache["data"]
        except Exception as e:
            log.warning("Error cargando cach� de velas: %s", e)

    log.info("Descargando velas hist�ricas (%d d�as) para %d pares...", days, len(symbols))
    data = {}
    for sym in symbols:
        log.debug("Descargando %s...", sym)
        sym_data = {}
        for tf in TIMEFRAMES:
            limit = int((days * 24 * 60) / ({"15m": 15, "1h": 60, "4h": 240}[tf])) + 50
            candles = exchange.get_ohlcv(sym, interval=tf, limit=limit)
            candles.reverse()
            sym_data[tf] = candles
        data[sym] = sym_data

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump({"_timestamp": time.time(), "data": data}, f, default=str)
    log.info("Velas descargadas y cacheadas en %s", cache_file)
    return data


def _check_directional_guard_sim(signal, positions, symbol):
    same_side_count = sum(1 for p in positions.values() if p.side == signal)
    if same_side_count >= MAX_SAME_SIDE:
        return False
    return True


def _simulate_symbol(symbol, candles_15m, candles_1h, candles_4h, exchange_sim):
    positions = {}
    _cooldown = {}
    _cooldown_reason = {}

    num_candles = min(len(candles_15m), len(candles_1h))

    for i in range(1, num_candles):
        exchange_sim.set_current_index(i)
        price = candles_15m[i]["close"]

        # Gestionar posiciones abiertas
        for sym, pos in list(exchange_sim.positions.items()):
            # Break-even
            if pos.be_trigger is not None and pos.be_sl is not None and not pos.be_locked:
                if (pos.side == "long" and price >= pos.be_trigger) or \
                   (pos.side == "short" and price <= pos.be_trigger):
                    pos.sl = pos.be_sl
                    pos.be_locked = True

            # Trailing
            if pos.trail_step > 0:
                if pos.side == "long":
                    if price > pos.trail_high + pos.trail_step:
                        pos.trail_high = price
                        new_sl = price - 1.5 * pos.trail_step
                        if new_sl > pos.sl:
                            pos.sl = new_sl
                else:
                    if price < pos.trail_low - pos.trail_step:
                        pos.trail_low = price
                        new_sl = price + 1.5 * pos.trail_step
                        if new_sl < pos.sl:
                            pos.sl = new_sl

            # TP extension simplificada (omitida para velocidad)

            # Verificar SL/TP
            if pos.side == "long":
                if pos.sl is not None and price <= pos.sl:
                    exchange_sim.close_position(sym, pos.sl, "SL")
                elif pos.tp is not None and price >= pos.tp:
                    exchange_sim.close_position(sym, pos.tp, "TP")
            else:
                if pos.sl is not None and price >= pos.sl:
                    exchange_sim.close_position(sym, pos.sl, "SL")
                elif pos.tp is not None and price <= pos.tp:
                    exchange_sim.close_position(sym, pos.tp, "TP")

        # Buscar nueva se�al
        if symbol in _cooldown and time.time() - _cooldown[symbol] < (COOLDOWN_SL if _cooldown_reason.get(symbol, "sl") == "sl" else COOLDOWN_TP):
            continue

        if symbol in exchange_sim.positions:
            continue

        effective_min_score = WEEKDAY_MIN_SCORE

        signal, score, regime, breakdown = signals.evaluate(
            candles_15m[:i+1],
            candles_1h[:i+1],
            candles_4h[:i+1] if candles_4h else None,
            min_score=effective_min_score,
            symbol=symbol,
            coin=exchange._hl_symbol(symbol),
        )

        if signal and signal in VALID_SIDES:
            if not _check_directional_guard_sim(signal, exchange_sim.positions, symbol):
                continue
            params = risk.calc(
                signal, price, candles_15m[:i+1],
                score=score, symbol=symbol, regime=regime,
                candles_1h=candles_1h[:i+1],
            )
            real_leverage = config.LEVERAGE
            qty = (config.MARGIN_USDT * real_leverage) / price
            qty = exchange.floor_qty(qty, symbol)
            if exchange.min_notional_ok(qty, price):
                params["qty"] = qty
                exchange_sim.open_position(symbol, signal, qty, params["sl"], params["tp"])
                pos = exchange_sim.get_position(symbol)
                if pos:
                    pos.trail_step = params["trail_step"]
                    pos.score = score
                    pos.be_trigger = params.get("be_trigger")
                    pos.be_sl = params.get("be_sl")
                    _cooldown[symbol] = time.time()
                    _cooldown_reason[symbol] = "tp"

    return exchange_sim.trades


def _run_backtest(symbols=None, days=60) -> list[dict]:
    if symbols is None:
        symbols = config.SYMBOLS
    log.info("Iniciando backtesting para %d s�mbolos (%d d�as)", len(symbols), days)
    historical = _download_historical_data(symbols, days)

    all_trades = []
    for sym in symbols:
        candles_15m = historical[sym]["15m"]
        candles_1h = historical[sym]["1h"]
        candles_4h = historical[sym].get("4h", [])
        if len(candles_15m) < 100 or len(candles_1h) < 220:
            log.warning("[%s] Velas insuficientes, saltando", sym)
            continue
        price_series = {sym: [c["close"] for c in candles_15m]}
        exchange_sim = _SimulatedExchange(price_series)
        trades = _simulate_symbol(sym, candles_15m, candles_1h, candles_4h, exchange_sim)
        all_trades.extend(trades)
        log.info("[%s] Backtest completado: %d trades", sym, len(trades))

    log.info("Backtesting finalizado: %d trades simulados", len(all_trades))
    return all_trades


# ════════════════════════════════════════════════════════════════════════════
#   OPTIMIZACI�N (m�tricas + recomendaciones)
# ════════════════════════════════════════════════════════════════════════════

def _fetch_trades_from_gist(days: int = 90) -> list[dict]:
    content = trade_logger._gist_pull()
    if not content or not content.strip():
        log.warning("No se pudo descargar Gist — usando cache local")
        return trade_logger.get_cache_snapshot()

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
                "breakdown": row.get("breakdown"),
            })
        except (ValueError, KeyError):
            continue
    return trades


def _calculate_metrics(trades: list[dict]) -> dict:
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


def _optimize_indicator_weights(trades: list[dict]) -> dict:
    """Calcula correlaci�n entre cada indicador y el PnL, ajusta pesos."""
    if len(trades) < 30:
        return {}

    # Recopilar contribuciones por indicador
    contributions = defaultdict(list)
    pnls = []

    for t in trades:
        breakdown = t.get("breakdown")
        if not breakdown:
            continue
        # Si breakdown es string JSON, parsear
        if isinstance(breakdown, str):
            try:
                breakdown = json.loads(breakdown)
            except:
                continue
        if not isinstance(breakdown, dict):
            continue
        pnl = t["pnl_usdt"]
        pnls.append(pnl)
        for key, value in breakdown.items():
            if value != 0:
                contributions[key].append((pnl, value))

    if len(pnls) < 20:
        return {}

    new_weights = {}
    for key, values in contributions.items():
        if len(values) < 10:
            continue
        # Calcular correlaci�n de Pearson entre contribuci�n y PnL
        pnls_vals = [v[0] for v in values]
        contrib_vals = [v[1] for v in values]
        mean_pnl = sum(pnls_vals) / len(pnls_vals)
        mean_contrib = sum(contrib_vals) / len(contrib_vals)
        cov = sum((p - mean_pnl) * (c - mean_contrib) for p, c in zip(pnls_vals, contrib_vals))
        std_pnl = (sum((p - mean_pnl) ** 2 for p in pnls_vals) / len(pnls_vals)) ** 0.5
        std_contrib = (sum((c - mean_contrib) ** 2 for c in contrib_vals) / len(contrib_vals)) ** 0.5
        if std_pnl > 0 and std_contrib > 0:
            corr = cov / (std_pnl * std_contrib)
            # Ajuste proporcional: nuevo peso = old * (1 + corr * 0.2)
            old = DEFAULT_WEIGHTS.get(key, 0)
            if old != 0:
                factor = 1 + corr * 0.2
                factor = max(0.5, min(1.5, factor))
                new = round(old * factor)
                new = max(-10, min(30, new))
                new_weights[key] = new

    return new_weights


def _recommend_changes(metrics: dict) -> dict:
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

    # 3. MARGIN_USDT — fijo en 20 (no ajustable)
    recs["MARGIN_USDT"] = 20.0

    # 4. SL_MIN_PCT
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

    # 5. PROTO_ADX_MIN
    recs["PROTO_ADX_MIN"] = DEFAULT_WEIGHTS.get("PROTO_ADX_MIN", 22)

    return recs


def _apply_weights(recommendations: dict) -> None:
    if not recommendations:
        return

    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE, "r") as f:
                current = json.load(f)
        except:
            current = DEFAULT_WEIGHTS.copy()
    else:
        current = DEFAULT_WEIGHTS.copy()

    for key, value in recommendations.items():
        current[key] = value

    with open(WEIGHTS_FILE, "w") as f:
        json.dump(current, f, indent=2)

    log.info("Pesos actualizados guardados en %s", WEIGHTS_FILE)

    # Aplicar en caliente
    try:
        import signals as _signals
        import risk as _risk
        for key, value in recommendations.items():
            if hasattr(_signals, key):
                setattr(_signals, key, value)
            if hasattr(config, key):
                setattr(config, key, value)
            if hasattr(_risk, key):
                setattr(_risk, key, value)
        if hasattr(_signals, "_WEIGHTS_OVERRIDE"):
            _signals._WEIGHTS_OVERRIDE = current
    except Exception as e:
        log.warning("No se pudieron aplicar cambios en caliente: %s", e)


def _combine_trades(real_trades: list[dict], sim_trades: list[dict]) -> list[dict]:
    combined = real_trades.copy()
    real_keys = {(t["date"], t["symbol"]) for t in real_trades}
    for t in sim_trades:
        key = (t["date"], t["symbol"])
        if key not in real_keys:
            combined.append(t)
    return combined


def optimize() -> dict:
    """Ejecuta el flujo completo de optimizaci�n (backtest + real)."""
    log.info("Iniciando optimizaci�n cada 72h...")

    trades_real = _fetch_trades_from_gist(days=90)

    log.info("Obteniendo trades simulados (60 d�as)...")
    try:
        trades_sim = _run_backtest(days=60)
    except Exception as e:
        log.error("Backtesting fall�: %s", e)
        trades_sim = []

    combined = _combine_trades(trades_real, trades_sim)
    log.info("Combinados: %d reales + %d simulados = %d trades",
             len(trades_real), len(trades_sim), len(combined))

    if len(combined) < 15:
        log.info("Trades insuficientes (%d < 15) — sin cambios", len(combined))
        return {}

    metrics = _calculate_metrics(combined)
    log.info(
        "M�tricas: %d trades | PnL=%.2f | WR=%.0f%% | Sharpe=%.2f",
        metrics["total_trades"], metrics["total_pnl"],
        metrics["win_rate"] * 100, metrics["sharpe"],
    )

    # Recomendaciones b�sicas
    recs = _recommend_changes(metrics)

    # Optimizaci�n de pesos de indicadores por correlaci�n
    weight_recs = _optimize_indicator_weights(combined)
    recs.update(weight_recs)

    if recs:
        log.info("Recomendaciones generadas: %s", recs)
        _apply_weights(recs)

        try:
            import telegram
            msg = (
                f"\U0001f4ca <b>Optimizaci�n 72h + Backtesting</b>\n"
                f"Reales: <code>{len(trades_real)}</code>\n"
                f"Simulados (60d): <code>{len(trades_sim)}</code>\n"
                f"Total: <code>{len(combined)}</code>\n\n"
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


def should_run() -> bool:
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


def run_async() -> None:
    """Lanza el optimizador en un hilo separado si no est� ya en ejecuci�n."""
    global _optimizer_thread, _optimizer_running

    if not should_run():
        log.debug("Optimizador: no toca ejecutar (esperando 72h)")
        return

    with _OPTIMIZER_LOCK:
        if _optimizer_running:
            log.debug("Optimizador ya en ejecuci�n, saltando...")
            return
        _optimizer_running = True

    def _target():
        try:
            log.info("Iniciando optimizaci�n en background (puede tardar varios minutos)...")
            optimize()
        except Exception as e:
            log.error("Optimizaci�n en background fall�: %s", e, exc_info=True)
        finally:
            with _OPTIMIZER_LOCK:
                global _optimizer_running
                _optimizer_running = False
            log.info("Hilo de optimizaci�n finalizado.")

    _optimizer_thread = threading.Thread(target=_target, daemon=True, name="optimizer-thread")
    _optimizer_thread.start()
    log.info("Hilo de optimizaci�n lanzado (daemon)")


if __name__ == "__main__":
    optimize()