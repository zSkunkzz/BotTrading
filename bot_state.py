"""bot_state.py — Estado global compartido entre main.py y tg_commands.py."""
import threading

_lock   = threading.Lock()
_paused = False


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
