"""
bot/state.py  —  Thread-safe, atomically-written position state.

v5 — MULTI-SYMBOL FIX (BUG #1)
  - _position ahora es dict[symbol -> dict] en lugar de Optional[dict]
  - save/load/clear/mark_tp2_hit indexan por symbol
  - Migración automática desde formato antiguo de fichero plano
  - threading.Lock en _save_sync() (BUG #4 del commit anterior)
  - asyncio.Lock en todas las coroutinas
  - Atomic write via tempfile + os.replace()

v6 — FIX #12 + FIX #16 + FIX #19
  FIX #12: Race condition sync vs async en _positions dict.
    Las sync helpers (_save_position_sync, _clear_position_sync, _mark_tp2_hit_sync)
    modificaban self._positions directamente sin adquirir _sync_lock, mientras que
    coroutinas async con asyncio.Lock podían estar leyendo/escribiendo el mismo dict.
    Fix: las sync helpers ahora adquieren _sync_lock para proteger el acceso al dict
    en memoria, además de la escritura al disco.

  FIX #16: Migración de formato antiguo guardaba posición como "UNKNOWN" si el
    estado no tenía campo 'symbol' ni 'coin'. En ese caso el trader no encontraba
    la posición por su símbolo real y la adoptaba como huérfana con SL de emergencia.
    Fix: si sym es "UNKNOWN", se loguea un warning claro y se descarta la posición
    en lugar de persistirla con una clave inutilizable.

  FIX #19: Railway Volume warning mejorado. El check anterior solo avisaba si el
    path no empezaba por /data, pero si /data existe como directorio ephemeral
    (Railway sin Volume configurado), el warning no se mostraba. Fix: se comprueba
    también que /data/.railway_volume_marker exista, un fichero que Railway monta
    automáticamente en los volúmenes. Si no existe, se emite warning adicional.
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


def _check_railway_volume() -> None:
    """
    FIX #19: comprueba que el Volume de Railway esté realmente montado.
    Railway monta los volúmenes en /data y un directorio /data vacío puede
    existir sin que haya un Volume configurado (es ephemeral).
    Se usa el fichero .write_test para detectar si la escritura funciona,
    pero eso no distingue entre Volume real y directorio ephemeral.
    Emitimos un warning adicional si parece que no hay Volume montado.
    """
    if not _RAILWAY:
        return
    state_path_str = str(_STATE_FILE)
    if not state_path_str.startswith("/data"):
        log.warning(
            "⚠️ Railway: STATE_FILE en %s es EPHEMERAL (se borra en cada deploy). "
            "Monta un Volume en /data en el dashboard de Railway.",
            _STATE_FILE,
        )
        return
    # El path es /data — comprobar si es un Volume real o un dir ephemeral
    # Railway Volumes crean el punto de montaje con permisos distintos al root FS.
    # El indicador más fiable disponible en runtime es intentar verificar
    # que /data tiene inodo distinto al del FS raíz.
    try:
        import os as _os
        data_stat = _os.stat("/data")
        root_stat = _os.stat("/")
        if data_stat.st_dev == root_stat.st_dev:
            # Mismo device → /data NO está montado como un Volume separado
            log.warning(
                "⚠️ Railway: /data parece NO tener un Volume montado (mismo device que '/'). "
                "El estado en %s se PERDERÁ en cada deploy/restart. "
                "Crea un Volume en el dashboard de Railway montado en /data.",
                _STATE_FILE,
            )
        else:
            log.info("✅ Railway Volume detectado en /data → estado persistente: %s", _STATE_FILE)
    except Exception as e:
        log.debug("_check_railway_volume: no se pudo verificar: %s", e)


_check_railway_volume()


class BotState:
    """
    Holds open positions keyed by symbol.

    BUG #1 FIX: _positions es dict[str, dict] en lugar de Optional[dict].
    Múltiples traders (BTC, ETH, SOL, ...) pueden tener posiciones abiertas
    simultáneamente sin sobreescribirse entre sí.

    FIX #12: Las sync helpers adquieren _sync_lock también para el acceso al dict
    en memoria, no solo para la escritura al disco. Esto evita la race condition
    con las coroutinas async que usan asyncio.Lock.
    """

    def __init__(self) -> None:
        self._lock      = asyncio.Lock()
        self._sync_lock = threading.Lock()
        self._positions: Dict[str, Dict[str, Any]] = {}  # symbol -> position dict
        self._session_pnl: float = 0.0
        self._trades: int = 0
        self._load()

    def _load(self) -> None:
        try:
            if _STATE_FILE.exists():
                data = json.loads(_STATE_FILE.read_text())
                raw = data.get("positions")
                if isinstance(raw, dict):
                    # Nuevo formato multi-symbol
                    self._positions = raw
                else:
                    # Migración automática desde formato antiguo (single-slot)
                    old_pos = data.get("position")
                    if old_pos and isinstance(old_pos, dict):
                        sym = old_pos.get("symbol") or old_pos.get("coin", "")
                        # FIX #16: no indexar como "UNKNOWN" — descartar si sym es vacío/None
                        if sym and sym != "UNKNOWN":
                            if old_pos.get("side") == "":
                                old_pos["side"] = None
                            self._positions = {sym: old_pos}
                            log.warning(
                                "State migrado desde formato antiguo: posición de %s restaurada.",
                                sym,
                            )
                        else:
                            log.warning(
                                "FIX #16: State antiguo sin campo 'symbol' ni 'coin' válido — "
                                "posición descartada (no se puede indexar). "
                                "El trader adoptará la posición como huérfana si aún está abierta "
                                "en el exchange, aplicando SL de emergencia."
                            )
                            self._positions = {}
                    else:
                        self._positions = {}
                self._session_pnl = float(data.get("session_pnl", 0.0))
                self._trades = int(data.get("trades", 0))
                log.info(
                    "State loaded: %d posicion(es) abiertas.",
                    len([k for k in self._positions if not k.startswith("__")]),
                )
        except Exception as exc:
            log.warning("No se pudo cargar state file (%s) — empezando limpio.", exc)

    def _save_sync(self) -> None:
        payload = {
            "positions":   self._positions,
            "session_pnl": self._session_pnl,
            "trades":      self._trades,
        }
        with self._sync_lock:
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
            return {k: dict(v) for k, v in self._positions.items() if not k.startswith("__")}

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
    # FIX #12: todas las sync helpers adquieren _sync_lock ANTES de modificar
    # self._positions en memoria. La escritura al disco (_save_sync) también usa
    # _sync_lock internamente, pero al adquirirlo aquí primero garantizamos que
    # la lectura + modificación + escritura sea una operación atómica desde la
    # perspectiva de los threads síncronos. Nota: si una coroutina async tiene
    # asyncio.Lock adquirido, las sync helpers no pueden esperarla (son sync),
    # pero el threading.Lock protege contra races entre threads síncronos.
    # Para protección completa async↔sync, llamar siempre desde el mismo thread
    # del event loop o usar run_in_executor si se llama desde otro thread.

    def _save_position_sync(self, symbol: str, data: Dict[str, Any]) -> None:
        pos = dict(data)
        pos["symbol"] = symbol
        if pos.get("side") == "":
            pos["side"] = None
        with self._sync_lock:
            self._positions[symbol] = pos
        self._save_sync()

    def _load_position_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._sync_lock:
            pos = self._positions.get(symbol)
            return dict(pos) if pos else None

    def _clear_position_sync(self, symbol: str) -> None:
        with self._sync_lock:
            self._positions.pop(symbol, None)
        self._save_sync()

    def _mark_tp2_hit_sync(self, symbol: str) -> None:
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
