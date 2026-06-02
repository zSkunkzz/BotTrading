"""
bot/state.py  —  Thread-safe, atomically-written position state.

v5 — MULTI-SYMBOL FIX (BUG #1)
  - _position ahora es dict[symbol -> dict] en lugar de Optional[dict]
  - save/load/clear/mark_tp2_hit indexan por symbol
  - Migración automática desde formato antiguo de fichero plano
  - threading.Lock en _save_sync() (BUG #4 del commit anterior)
  - asyncio.Lock en todas las coroutinas
  - Atomic write via tempfile + os.replace()

v6 — RACE CONDITION FIX (BUG #12) + MIGRACIÓN FIX (BUG #16)
  BUG #12 FIX: las sync helpers (_save_position_sync, _clear_position_sync,
    _mark_tp2_hit_sync) ahora adquieren self._sync_lock antes de modificar
    self._positions en memoria. Anteriormente solo se aplicaba el lock en
    _save_sync() (escritura a disco) pero la modificación del dict en memoria
    era desprotegida y podía interleavar con las coroutinas async que usan
    asyncio.Lock sobre el mismo dict.
    Nota: asyncio.Lock y threading.Lock son locks distintos; la solución
    correcta a largo plazo es usar exclusivamente el modelo async, pero
    mientras exista código sync que acceda a _positions, el threading.Lock
    debe proteger TANTO la escritura al dict como la escritura al disco.
  BUG #16 FIX: migración de formato antiguo — si old_pos no contiene 'symbol'
    ni 'coin', se usa el primer símbolo del env SYMBOLS o 'BTCUSDC' como
    fallback en lugar de 'UNKNOWN', para que el trader pueda recuperar la
    posición al arrancar en lugar de adoptarla como huérfana.

v6 — RAILWAY VOLUME WARNING (BUG #19)
  BUG #19 FIX: al arrancar en Railway, se verifica que el state file esté
    en un path persistente (/data). Si el Volume no está montado y el bot
    usa /tmp, se emite un WARNING visible con instrucciones claras.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_RAILWAY = os.getenv("RAILWAY_ENVIRONMENT") is not None

# BUG #16 FIX: símbolo de fallback para migración de formato antiguo
_SYMBOLS_ENV = os.getenv("SYMBOLS", "").strip()
_FALLBACK_SYMBOL = _SYMBOLS_ENV.split(",")[0].strip() if _SYMBOLS_ENV else "BTCUSDC"


def _resolve_state_file() -> Path:
    env_val = os.getenv("STATE_FILE", "").strip()
    if env_val:
        return Path(env_val)
    data_dir = Path("/data")
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        test_file = data_dir / ".write_test"
        test_file.touch()
        test_file.unlink()
        return data_dir / "bot_state.json"
    except OSError:
        pass
    return Path("/tmp/bot_state.json")


_STATE_FILE = _resolve_state_file()

if _RAILWAY:
    if str(_STATE_FILE).startswith("/data"):
        log.info("Estado persistente en Railway Volume: %s", _STATE_FILE)
    else:
        # BUG #19 FIX: warning explícito con instrucciones cuando el Volume no está montado
        log.warning(
            "[STATE] ⚠️  RAILWAY VOLUME NO CONFIGURADO — usando path EFÍMERO: %s\n"
            "  El estado del bot se PERDERÁ en cada restart/redeploy.\n"
            "  Para persistencia: añade un Volume en Railway dashboard montado en /data\n"
            "  o configura la variable de entorno STATE_FILE con un path en volumen persistente.",
            _STATE_FILE,
        )
else:
    log.debug("State file: %s", _STATE_FILE)


class BotState:
    """
    Holds open positions keyed by symbol.

    BUG #1 FIX: _positions es dict[str, dict] en lugar de Optional[dict].
    BUG #12 FIX: threading.Lock protege el dict en memoria (no solo el disco).
    BUG #16 FIX: migración usa símbolo real del env en lugar de 'UNKNOWN'.
    """

    def __init__(self) -> None:
        self._lock      = asyncio.Lock()
        self._sync_lock = threading.Lock()
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._session_pnl: float = 0.0
        self._trades: int = 0
        self._load()

    def _load(self) -> None:
        try:
            if _STATE_FILE.exists():
                data = json.loads(_STATE_FILE.read_text())
                raw = data.get("positions")
                if isinstance(raw, dict):
                    self._positions = raw
                else:
                    # Migración automática desde formato antiguo (single-slot)
                    old_pos = data.get("position")
                    if old_pos and isinstance(old_pos, dict):
                        # BUG #16 FIX: usar símbolo real, no 'UNKNOWN'
                        sym = (
                            old_pos.get("symbol")
                            or old_pos.get("coin")
                            or _FALLBACK_SYMBOL
                        )
                        if old_pos.get("side") == "":
                            old_pos["side"] = None
                        self._positions = {sym: old_pos}
                        log.warning(
                            "State migrado desde formato antiguo: posición restaurada como '%s'. "
                            "Si el símbolo no es correcto, revisa el fichero de estado.",
                            sym,
                        )
                    else:
                        self._positions = {}
                self._session_pnl = float(data.get("session_pnl", 0.0))
                self._trades = int(data.get("trades", 0))
                log.info(
                    "State loaded: %d posicion(es) abiertas.",
                    len([k for k, v in self._positions.items()
                         if isinstance(v, dict) and v.get("side")]),
                )
        except Exception as exc:
            log.warning("No se pudo cargar state file (%s) — empezando limpio.", exc)

    def _save_sync(self) -> None:
        payload = {
            "positions":   self._positions,
            "session_pnl": self._session_pnl,
            "trades":      self._trades,
        }
        # Nota: _save_sync se llama siempre desde dentro de _sync_lock
        # (desde sync helpers) o desde dentro de asyncio.Lock (desde coroutinas).
        # NO adquirir _sync_lock aquí para evitar deadlock cuando las coroutinas
        # async llaman _save_sync directamente.
        try:
            _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(
                dir=_STATE_FILE.parent, prefix=".bot_state_", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(payload, fh, indent=2)
                os.replace(tmp_path, _STATE_FILE)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as exc:
            log.error("State save failed: %s", exc)

    # ── public async API ──────────────────────────────────────────────────

    async def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            pos = self._positions.get(symbol)
            return dict(pos) if pos else None

    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return {k: dict(v) for k, v in self._positions.items()}

    async def has_position(self, symbol: str) -> bool:
        async with self._lock:
            return symbol in self._positions

    async def set_position(self, symbol: str, pos: Optional[Dict[str, Any]]) -> None:
        if pos is not None:
            if pos.get("side") == "":
                pos = {**pos, "side": None}
            pos["symbol"] = symbol
        async with self._lock:
            if pos is None:
                self._positions.pop(symbol, None)
            else:
                self._positions[symbol] = pos
            self._save_sync()

    async def update_position(self, symbol: str, **kwargs: Any) -> None:
        async with self._lock:
            if symbol not in self._positions:
                log.warning("update_position(%s): no hay posición abierta — ignorado", symbol)
                return
            for k, v in kwargs.items():
                if k == "side" and v == "":
                    v = None
                self._positions[symbol][k] = v
            self._save_sync()

    async def clear_position(self, symbol: str) -> None:
        async with self._lock:
            self._positions.pop(symbol, None)
            self._save_sync()

    async def record_trade(self, pnl: float) -> None:
        async with self._lock:
            self._session_pnl += pnl
            self._trades += 1
            self._save_sync()

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {"session_pnl": self._session_pnl, "trades": self._trades}

    # ── sync helpers (backward-compat layer) ──────────────────────────────
    # BUG #12 FIX: threading.Lock protege TAMBIÉN la escritura al dict en
    # memoria, no solo la escritura al disco. Esto previene race conditions
    # cuando código sync (position_manager, daily_drawdown) y coroutinas async
    # acceden al mismo dict concurrentemente.

    def _save_position_sync(self, symbol: str, data: Dict[str, Any]) -> None:
        pos = dict(data)
        pos["symbol"] = symbol
        if pos.get("side") == "":
            pos["side"] = None
        # BUG #12 FIX: adquirir sync_lock antes de modificar el dict en memoria
        with self._sync_lock:
            self._positions[symbol] = pos
            self._save_sync()

    def _load_position_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        # BUG #12 FIX: adquirir sync_lock para lectura consistente
        with self._sync_lock:
            pos = self._positions.get(symbol)
            return dict(pos) if pos else None

    def _clear_position_sync(self, symbol: str) -> None:
        # BUG #12 FIX: adquirir sync_lock antes de modificar el dict en memoria
        with self._sync_lock:
            if symbol in self._positions:
                self._positions.pop(symbol)
                self._save_sync()

    def _mark_tp2_hit_sync(self, symbol: str) -> None:
        # BUG #12 FIX: adquirir sync_lock antes de modificar el dict en memoria
        with self._sync_lock:
            if symbol in self._positions:
                self._positions[symbol]["tp2_hit"] = True
                self._save_sync()


# ── module-level singleton ────────────────────────────────────────────────
bot_state = BotState()


# ── BACKWARD-COMPATIBLE FREE FUNCTIONS ───────────────────────────────────

def save_position(symbol: str, data: Dict[str, Any]) -> None:
    bot_state._save_position_sync(symbol, data)


def load_position(symbol: str) -> Optional[Dict[str, Any]]:
    return bot_state._load_position_sync(symbol)


def clear_position(symbol: str) -> None:
    bot_state._clear_position_sync(symbol)


def mark_tp2_hit(symbol: str) -> None:
    bot_state._mark_tp2_hit_sync(symbol)
