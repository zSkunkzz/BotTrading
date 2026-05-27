# ============================================================
# bot/state.py  —  Persistencia de posiciones abiertas
# Guarda/recupera estado en /tmp/bot_state.json
# Railway no tiene volumen persistente entre deploys,
# pero sí dentro del mismo proceso/restart del contenedor.
# Para persistencia cross-deploy usa la variable de entorno
# STATE_FILE=/data/bot_state.json (Railway Volume mount)
# ============================================================

import json
import os
import logging
from pathlib import Path

logger = logging.getLogger("State")

STATE_FILE = Path(os.getenv("STATE_FILE", "/tmp/bot_state.json"))


def _load_raw() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception as e:
        logger.warning(f"[State] No se pudo leer {STATE_FILE}: {e}")
    return {}


def _save_raw(data: dict):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logger.error(f"[State] No se pudo guardar {STATE_FILE}: {e}")


def save_position(symbol: str, position: str, entry_price: float,
                  sl: float | None, tp1: float | None,
                  tp2: float | None, tp3: float | None,
                  usdt_amount: float, leverage: int):
    """Persiste una posición abierta."""
    data = _load_raw()
    data[symbol] = {
        "position":    position,
        "entry_price": entry_price,
        "sl":          sl,
        "tp1":         tp1,
        "tp2":         tp2,
        "tp3":         tp3,
        "tp2_hit":     False,
        "usdt_amount": usdt_amount,
        "leverage":    leverage,
    }
    _save_raw(data)
    logger.info(f"[State] Guardado {symbol} {position} @ {entry_price}")


def load_position(symbol: str) -> dict | None:
    """Recupera estado de posición para un símbolo, o None si no hay."""
    return _load_raw().get(symbol)


def clear_position(symbol: str):
    """Borra el estado al cerrar la posición."""
    data = _load_raw()
    if symbol in data:
        del data[symbol]
        _save_raw(data)
        logger.info(f"[State] Borrado {symbol}")


def mark_tp2_hit(symbol: str):
    """Marca que TP2 ya fue parcialmente ejecutado."""
    data = _load_raw()
    if symbol in data:
        data[symbol]["tp2_hit"] = True
        _save_raw(data)


def load_all() -> dict:
    return _load_raw()
