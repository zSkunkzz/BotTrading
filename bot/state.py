# ============================================================
# bot/state.py  —  Persistencia de posiciones abiertas
#
# Por defecto guarda en /tmp/bot_state.json (volátil en Railway).
# Para persistencia cross-deploy DEBES montar un volumen en Railway
# y configurar: STATE_FILE=/data/bot_state.json
# ============================================================

import asyncio
import json
import os
import logging
from pathlib import Path

logger = logging.getLogger("State")

STATE_FILE = Path(os.getenv("STATE_FILE", "/tmp/bot_state.json"))

if str(STATE_FILE).startswith("/tmp"):
    logger.warning(
        "[State] ⚠️  STATE_FILE apunta a /tmp (%s). "
        "El estado se perderá en cada restart/deploy de Railway. "
        "Para persistencia real monta un volumen y configura STATE_FILE=/data/bot_state.json",
        STATE_FILE,
    )

# asyncio.Lock para serializar read→modify→write y evitar corrupción
# de estado cuando dos corrutinas acceden simultáneamente al archivo.
_state_lock = asyncio.Lock()


def _load_raw() -> dict:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text())
    except Exception as e:
        logger.warning(f"[State] No se pudo leer {STATE_FILE}: {e}")
    return {}


def _save_raw(data: dict):
    """Escribe de forma atómica: primero a un .tmp y luego rename().

    rename() es atómico en POSIX — evita archivos parcialmente
    escritos si el proceso muere o el event loop es interrumpido entre
    write() y flush().
    """
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        tmp.replace(STATE_FILE)
    except Exception as e:
        logger.error(f"[State] No se pudo guardar {STATE_FILE}: {e}")


def save_position(symbol: str, position_or_dict, entry_price=None,
                  sl=None, tp1=None, tp2=None, tp3=None,
                  usdc_amount=None, leverage=None,
                  api_version=None, ua_pos_mode=None, v2_pos_mode=None):
    """
    Persiste una posición abierta.

    Acepta dos formas de llamada:
      1. save_position(symbol, dict_con_datos)          <- forma usada en trader.py
      2. save_position(symbol, side, entry, sl, ...)    <- forma posicional legacy

    FIX: envuelto en asyncio.Lock para evitar race conditions read->write.
    FIX: detección explícita de string vacío para evitar que side=""
         caiga silenciosamente en None y la posición se restaure incorrectamente.
    FIX: asyncio.create_task() en vez de ensure_future() (deprecated Python 3.10+).
    """
    if isinstance(position_or_dict, dict):
        d = position_or_dict
        # side="" es falsy; comparar contra None explícitamente
        raw_side = d.get("side")
        raw_pos  = d.get("position")
        side_val = raw_side if raw_side else (raw_pos if raw_pos else None)

        raw_entry = d.get("entry")
        raw_ep    = d.get("entry_price")
        entry_val = raw_entry if raw_entry else (raw_ep if raw_ep else None)

        entry = {
            "position":    side_val,
            "entry_price": entry_val,
            "sl":          d.get("sl"),
            "tp1":         d.get("tp1"),
            "tp2":         d.get("tp2"),
            "tp3":         d.get("tp3"),
            "tp2_hit":     d.get("tp2_hit", False),
            "usdc_amount": d.get("usdc_amount") or d.get("usdt_amount", 0.0),
            "leverage":    d.get("leverage", 1),
        }
    else:
        entry = {
            "position":    position_or_dict,
            "entry_price": entry_price,
            "sl":          sl,
            "tp1":         tp1,
            "tp2":         tp2,
            "tp3":         tp3,
            "tp2_hit":     False,
            "usdc_amount": usdc_amount,
            "leverage":    leverage,
        }
        if api_version:
            entry["api_version"] = api_version
        if ua_pos_mode:
            entry["ua_pos_mode"] = ua_pos_mode
        if v2_pos_mode:
            entry["v2_pos_mode"] = v2_pos_mode

    async def _do():
        async with _state_lock:
            data = _load_raw()
            data[symbol] = entry
            _save_raw(data)
        logger.info(f"[State] Guardado {symbol} {entry['position']} @ {entry['entry_price']}")

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # FIX: create_task en vez de ensure_future (deprecated Python 3.10+)
            asyncio.create_task(_do())
        else:
            loop.run_until_complete(_do())
    except RuntimeError:
        # Sin event loop activo (tests/scripts) — escritura síncrona sin lock
        data = _load_raw()
        data[symbol] = entry
        _save_raw(data)
        logger.info(f"[State] Guardado (sync) {symbol} {entry['position']} @ {entry['entry_price']}")


def load_position(symbol: str) -> dict | None:
    """Recupera estado de posición para un símbolo, o None si no hay."""
    raw = _load_raw().get(symbol)
    if raw is None:
        return None
    if "side" not in raw and "position" in raw:
        raw["side"] = raw["position"]
    if "entry" not in raw and "entry_price" in raw:
        raw["entry"] = raw["entry_price"]
    return raw


def clear_position(symbol: str):
    """Borra el estado al cerrar la posición."""
    async def _do():
        async with _state_lock:
            data = _load_raw()
            if symbol in data:
                del data[symbol]
                _save_raw(data)
        logger.info(f"[State] Borrado {symbol}")

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_do())
        else:
            loop.run_until_complete(_do())
    except RuntimeError:
        data = _load_raw()
        if symbol in data:
            del data[symbol]
            _save_raw(data)
        logger.info(f"[State] Borrado (sync) {symbol}")


def mark_tp2_hit(symbol: str):
    """Marca que TP2 ya fue parcialmente ejecutado."""
    async def _do():
        async with _state_lock:
            data = _load_raw()
            if symbol in data:
                data[symbol]["tp2_hit"] = True
                _save_raw(data)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_do())
        else:
            loop.run_until_complete(_do())
    except RuntimeError:
        data = _load_raw()
        if symbol in data:
            data[symbol]["tp2_hit"] = True
            _save_raw(data)


def load_all() -> dict:
    return _load_raw()
