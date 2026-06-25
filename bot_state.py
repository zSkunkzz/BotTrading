"""bot_state.py — Estado global compartido entre main.py y tg_commands.py.

AQUI vive el ÚNICO contador de drawdown diario.
Todo el código debe usar este módulo — trade_logger y main.py NO tienen
sus propios contadores de PnL diario.
"""
import threading
from datetime import datetime, timezone

import config

_lock   = threading.Lock()
_paused = False

# ── Drawdown diario ─────────────────────────────────────────────────────────
# _daily_pnl es el PnL NETO del día (positivo = ganancia, negativo = pérdida)
_daily_pnl_usdt: float = 0.0
_daily_date: str       = ""       # YYYY-MM-DD UTC
_daily_limit_hit: bool = False


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _reset_if_new_day() -> None:
    """Debe llamarse dentro de _lock."""
    global _daily_pnl_usdt, _daily_date, _daily_limit_hit
    today = _today_utc()
    if today != _daily_date:
        _daily_pnl_usdt  = 0.0
        _daily_date      = today
        _daily_limit_hit = False


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
        _daily_pnl_usdt += pnl_usdt          # suma algebraica — ganancias y pérdidas
        if not _daily_limit_hit:
            capital = config.MARGIN_USDT * config.MAX_POSITIONS
            pct = (_daily_pnl_usdt / capital * 100) if capital else 0.0
            daily_max = float(getattr(config, "DAILY_MAX_LOSS_PCT", -3.0))
            if pct <= daily_max:
                _daily_limit_hit = True
                return True
        return False


def is_daily_limit_hit() -> bool:
    with _lock:
        _reset_if_new_day()
        return _daily_limit_hit


def reset_daily_if_new_day() -> bool:
    """Comprueba si es un día nuevo y resetea. Devuelve True si se reseteó."""
    global _daily_limit_hit
    with _lock:
        old_hit = _daily_limit_hit
        _reset_if_new_day()
        return old_hit and not _daily_limit_hit


def get_daily_pnl() -> float:
    """Devuelve el PnL neto acumulado hoy (negativo = pérdida neta)."""
    with _lock:
        _reset_if_new_day()
        return _daily_pnl_usdt


def restore_from_csv(trades_today: list[dict]) -> None:
    """Recalcula el PnL neto del día desde los trades del CSV al arrancar."""
    global _daily_pnl_usdt, _daily_date, _daily_limit_hit
    with _lock:
        _daily_date     = _today_utc()
        _daily_pnl_usdt = sum(t["pnl_usdt"] for t in trades_today)
        capital         = config.MARGIN_USDT * config.MAX_POSITIONS
        pct             = (_daily_pnl_usdt / capital * 100) if capital else 0.0
        daily_max       = float(getattr(config, "DAILY_MAX_LOSS_PCT", -3.0))
        _daily_limit_hit = pct <= daily_max
