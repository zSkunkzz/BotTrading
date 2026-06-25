"""bot_state.py — Estado global compartido entre main.py y tg_commands.py.

AQUI vive el ÚNICO contador de drawdown diario.
Todo el código debe usar este módulo — trade_logger y main.py NO tienen
sus propios contadores de PnL diario.

FIX capital real: _get_capital() consulta exchange.get_balance() para obtener
el equity real de la cuenta. El capital ficticio (MARGIN_USDT × MAX_POSITIONS)
se usa solo si BingX no responde. El valor se cachea 5 minutos para no
spammear la API en cada trade.

FIX DAILY_MAX_LOSS_PCT: se fuerza a negativo. Si el usuario pone 10 o -10
en Railway, ambos se tratan como -10. Así el bot nunca se pausa por ganancias.
"""
import threading
import time
from datetime import datetime, timezone

import config

_lock   = threading.Lock()
_paused = False

# ── Capital real cacheado ────────────────────────────────────────────────────
_capital_cache: float = 0.0
_capital_ts:    float = 0.0
_CAPITAL_TTL:   float = 300.0


def _get_capital() -> float:
    global _capital_cache, _capital_ts
    now = time.monotonic()
    if now - _capital_ts < _CAPITAL_TTL and _capital_cache > 0:
        return _capital_cache

    try:
        import exchange as _ex
        bal = _ex.get_balance()
    except Exception:
        bal = 0.0

    if bal > 0:
        _capital_cache = bal
        _capital_ts    = now
        return bal

    fallback = config.MARGIN_USDT * config.MAX_POSITIONS
    return fallback if fallback > 0 else 1.0


def _get_daily_max() -> float:
    """Devuelve el umbral de pérdida diaria SIEMPRE como valor negativo.

    Acepta tanto -3.0 como 3.0 en DAILY_MAX_LOSS_PCT — ambos se tratan
    como -3.0. Así el bot nunca se pausa cuando el PnL es positivo.
    """
    raw = float(getattr(config, "DAILY_MAX_LOSS_PCT", -3.0))
    return -abs(raw)  # forzar negativo siempre


# ── Drawdown diario ─────────────────────────────────────────────────────────
_daily_pnl_usdt: float = 0.0
_daily_date: str       = ""
_daily_limit_hit: bool = False


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _reset_if_new_day() -> None:
    """Debe llamarse dentro de _lock."""
    global _daily_pnl_usdt, _daily_date, _daily_limit_hit, _capital_ts
    today = _today_utc()
    if today != _daily_date:
        _daily_pnl_usdt  = 0.0
        _daily_date      = today
        _daily_limit_hit = False
        _capital_ts      = 0.0


# ── Pausa manual ────────────────────────────────────────────────────────────

def pause() -> None:
    global _paused
    with _lock:
        _paused = True


def resume() -> None:
    global _paused
    with _lock:
        _paused = False


def is_paused() -> bool:
    with _lock:
        return _paused


# ── API de drawdown ─────────────────────────────────────────────────────────

def record_trade(pnl_usdt: float) -> bool:
    """Registra el PnL neto de un trade. Devuelve True si se supera el límite."""
    global _daily_pnl_usdt, _daily_limit_hit
    with _lock:
        _reset_if_new_day()
        _daily_pnl_usdt += pnl_usdt
        if not _daily_limit_hit:
            capital   = _get_capital()
            daily_max = _get_daily_max()  # siempre negativo, ej. -3.0
            pct = _daily_pnl_usdt / capital * 100
            if pct <= daily_max:
                _daily_limit_hit = True
                return True
        return False


def is_daily_limit_hit() -> bool:
    with _lock:
        _reset_if_new_day()
        return _daily_limit_hit


def reset_daily_if_new_day() -> bool:
    global _daily_limit_hit
    with _lock:
        old_hit = _daily_limit_hit
        _reset_if_new_day()
        return old_hit and not _daily_limit_hit


def get_daily_pnl() -> float:
    with _lock:
        _reset_if_new_day()
        return _daily_pnl_usdt


def restore_from_csv(trades_today: list[dict]) -> None:
    """Recalcula el PnL neto del día desde los trades del CSV al arrancar."""
    global _daily_pnl_usdt, _daily_date, _daily_limit_hit
    with _lock:
        _daily_date     = _today_utc()
        _daily_pnl_usdt = sum(t["pnl_usdt"] for t in trades_today)
        capital         = _get_capital()
        daily_max       = _get_daily_max()  # siempre negativo
        pct             = _daily_pnl_usdt / capital * 100
        _daily_limit_hit = pct <= daily_max
