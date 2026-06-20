"""bot_state.py — Estado global compartido entre main.py y tg_commands.py."""
import threading
from datetime import date

import config

_lock   = threading.Lock()
_paused = False

# Pérdida diaria
_daily_loss_usdt: float = 0.0
_daily_loss_date: date  = date.today()


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


def record_trade(pnl_usdt: float) -> None:
    """Registra el PnL de un trade cerrado. pnl_usdt negativo = pérdida."""
    global _daily_loss_usdt, _daily_loss_date
    with _lock:
        today = date.today()
        if today != _daily_loss_date:
            _daily_loss_usdt = 0.0
            _daily_loss_date = today
        if pnl_usdt < 0:
            _daily_loss_usdt += abs(pnl_usdt)


def is_daily_loss_exceeded() -> bool:
    """Devuelve True si la pérdida acumulada hoy supera MAX_DAILY_LOSS_USDT."""
    with _lock:
        today = date.today()
        if today != _daily_loss_date:
            return False
        return _daily_loss_usdt >= config.MAX_DAILY_LOSS_USDT


def get_daily_loss() -> float:
    """Devuelve la pérdida acumulada hoy en USDT (valor positivo = pérdida)."""
    with _lock:
        today = date.today()
        if today != _daily_loss_date:
            return 0.0
        return _daily_loss_usdt
