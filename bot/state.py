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
    self._positions en memoria.
  BUG #16 FIX: migración de formato antiguo usa símbolo real del env.

v6 — RAILWAY VOLUME WARNING (BUG #19)
  BUG #19 FIX: al arrancar en Railway se verifica que el state file esté
    en un path persistente (/data).

v7 — COOLDOWN PERSISTENTE (BUG cooldown-rotation)
  El cooldown post-cierre externo vivía solo en TradingLoop._external_close_cooldown_until.
  BitgetBot destruye y recrea traders cada 15 ciclos sin posición, perdiendo el cooldown
  y causando que el bot re-abriera la posición inmediatamente después de un cierre manual.
  Fix: _cooldowns dict[symbol -> expiry_timestamp_utc] persistido en el mismo JSON.
  Nuevas helpers: save_cooldown / load_cooldown / clear_cooldown (sync + async).

v8 — save_position acepta kwargs individuales
  Fix: save_position(symbol, side=..., entry=..., sl=..., tp1=..., be_done=...)
  Backward-compatible: si se pasa data como dict posicional sigue funcionando.
  Nuevos campos: be_done (bool) para persistir estado de break-even.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_RAILWAY = os.getenv("RAILWAY_ENVIRONMENT") is not None

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
    Holds open positions and cooldowns keyed by symbol.
    """

    def __init__(self) -> None:
        self._lock      = asyncio.Lock()
        self._sync_lock = threading.Lock()
        self._positions: Dict[str, Dict[str, Any]] = {}
        self._cooldowns: Dict[str, float] = {}  # symbol -> unix timestamp expiry
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
                    old_pos = data.get("position")
                    if old_pos and isinstance(old_pos, dict):
                        sym = (
                            old_pos.get("symbol")
                            or old_pos.get("coin")
                            or _FALLBACK_SYMBOL
                        )
                        if old_pos.get("side") == "":
                            old_pos["side"] = None
                        self._positions = {sym: old_pos}
                        log.warning(
                            "State migrado desde formato antiguo: posición restaurada como '%s'.",
                            sym,
                        )
                    else:
                        self._positions = {}
                # Cargar cooldowns — ignorar entradas ya expiradas al cargar
                raw_cd = data.get("cooldowns", {})
                now = time.time()
                self._cooldowns = {
                    sym: exp
                    for sym, exp in raw_cd.items()
                    if isinstance(exp, (int, float)) and exp > now
                }
                self._session_pnl = float(data.get("session_pnl", 0.0))
                self._trades = int(data.get("trades", 0))
                log.info(
                    "State loaded: %d posicion(es) abiertas, %d cooldown(s) activos.",
                    len([k for k, v in self._positions.items()
                         if isinstance(v, dict) and v.get("side")]),
                    len(self._cooldowns),
                )
        except Exception as exc:
            log.warning("No se pudo cargar state file (%s) — empezando limpio.", exc)

    def _save_sync(self) -> None:
        payload = {
            "positions":   self._positions,
            "cooldowns":   self._cooldowns,
            "session_pnl": self._session_pnl,
            "trades":      self._trades,
        }
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

    # ── async API ─────────────────────────────────────────────────────

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

    # ── cooldown async API ─────────────────────────────────────────────

    async def set_cooldown(self, symbol: str, duration_s: float) -> None:
        """Activa cooldown para `symbol` durante `duration_s` segundos."""
        expiry = time.time() + duration_s
        async with self._lock:
            self._cooldowns[symbol] = expiry
            self._save_sync()
        log.info(
            "[state] Cooldown post-cierre activado para %s: %.0f s (expira %s)",
            symbol, duration_s,
            __import__("datetime").datetime.fromtimestamp(expiry).strftime("%H:%M:%S"),
        )

    async def get_cooldown_remaining(self, symbol: str) -> float:
        """Segundos restantes de cooldown, 0.0 si expirado o no existe."""
        async with self._lock:
            expiry = self._cooldowns.get(symbol, 0.0)
        remaining = max(0.0, expiry - time.time())
        return remaining

    async def clear_cooldown(self, symbol: str) -> None:
        async with self._lock:
            self._cooldowns.pop(symbol, None)
            self._save_sync()

    # ── sync helpers (backward-compat layer) ────────────────────────────────────────

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
            if symbol in self._positions:
                self._positions.pop(symbol)
                self._save_sync()

    def _mark_tp2_hit_sync(self, symbol: str) -> None:
        with self._sync_lock:
            if symbol in self._positions:
                self._positions[symbol]["tp2_hit"] = True
                self._save_sync()

    # ── cooldown sync helpers ───────────────────────────────────────────────

    def _set_cooldown_sync(self, symbol: str, duration_s: float) -> None:
        expiry = time.time() + duration_s
        with self._sync_lock:
            self._cooldowns[symbol] = expiry
            self._save_sync()
        log.info(
            "[state] Cooldown post-cierre activado para %s: %.0f s (expira %s)",
            symbol, duration_s,
            __import__("datetime").datetime.fromtimestamp(expiry).strftime("%H:%M:%S"),
        )

    def _get_cooldown_remaining_sync(self, symbol: str) -> float:
        with self._sync_lock:
            expiry = self._cooldowns.get(symbol, 0.0)
        return max(0.0, expiry - time.time())

    def _clear_cooldown_sync(self, symbol: str) -> None:
        with self._sync_lock:
            self._cooldowns.pop(symbol, None)
            self._save_sync()


# ── module-level singleton ──────────────────────────────────────────────────────
bot_state = BotState()


# ── BACKWARD-COMPATIBLE FREE FUNCTIONS ───────────────────────────────────────────

def save_position(
    symbol: str,
    data: Optional[Dict[str, Any]] = None,
    *,
    side: Optional[str] = None,
    entry: Optional[float] = None,
    sl: Optional[float] = None,
    tp1: Optional[float] = None,
    tp2: Optional[float] = None,
    tp3: Optional[float] = None,
    qty: Optional[float] = None,
    usdc_amount: Optional[float] = None,
    leverage: Optional[int] = None,
    be_done: bool = False,
    **extra: Any,
) -> None:
    """
    Guarda la posición abierta para `symbol`.

    Acepta dos formas:
      1. Forma antigua (backward-compat): save_position(symbol, {"side": ..., ...})
      2. Forma nueva con kwargs:           save_position(symbol, side=..., entry=..., be_done=False)

    Los kwargs individuales tienen precedencia sobre el dict posicional.
    El campo `be_done` (bool) indica si el break-even ya fue activado.
    """
    if data is None:
        data = {}
    pos = dict(data)   # copia para no mutar el original

    # Aplicar kwargs individuales (sobrescriben el dict si se pasan ambos)
    if side        is not None: pos["side"]        = side
    if entry       is not None: pos["entry"]       = entry
    if sl          is not None: pos["sl"]          = sl
    if tp1         is not None: pos["tp1"]         = tp1
    if tp2         is not None: pos["tp2"]         = tp2
    if tp3         is not None: pos["tp3"]         = tp3
    if qty         is not None: pos["qty"]         = qty
    if usdc_amount is not None: pos["usdc_amount"] = usdc_amount
    if leverage    is not None: pos["leverage"]    = leverage
    pos["be_done"] = be_done
    pos.update(extra)

    bot_state._save_position_sync(symbol, pos)


def load_position(symbol: str) -> Optional[Dict[str, Any]]:
    return bot_state._load_position_sync(symbol)


def clear_position(symbol: str) -> None:
    bot_state._clear_position_sync(symbol)


def mark_tp2_hit(symbol: str) -> None:
    bot_state._mark_tp2_hit_sync(symbol)


# ── cooldown free functions ────────────────────────────────────────────────────

def save_cooldown(symbol: str, duration_s: float) -> None:
    """Persiste un cooldown post-cierre externo para `symbol`."""
    bot_state._set_cooldown_sync(symbol, duration_s)


def get_cooldown_remaining(symbol: str) -> float:
    """Segundos restantes de cooldown activo para `symbol`. 0.0 si no hay."""
    return bot_state._get_cooldown_remaining_sync(symbol)


def clear_cooldown(symbol: str) -> None:
    bot_state._clear_cooldown_sync(symbol)
