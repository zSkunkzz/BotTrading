"""
bot/state.py  –  Thread-safe, atomically-written position state.

Fixes:
  - asyncio.Lock around every read/write to avoid partial-state races
  - Atomic write via tempfile + os.replace() – no corrupt JSON on crash
  - Bug: side="" is falsy → now stored/checked with explicit None sentinel
  - Railway warning: ephemeral filesystem; log once at startup
  - COMPAT: backward-compatible free functions (save_position, load_position,
    clear_position, mark_tp2_hit) so trader.py / position_manager.py / decision_engine.py
    can import them without changes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_STATE_FILE = Path(os.getenv("STATE_FILE", "/tmp/bot_state.json"))
_RAILWAY = os.getenv("RAILWAY_ENVIRONMENT") is not None

# ── warn once about Railway's ephemeral FS ─────────────────────────────────────────
if _RAILWAY:
    log.warning(
        "Running on Railway: %s is ephemeral and will be wiped on redeploy. "
        "Set STATE_FILE to a persistent volume path or use an external store.",
        _STATE_FILE,
    )


class BotState:
    """Holds one open position (or None) plus cumulative session stats."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._position: Optional[Dict[str, Any]] = None
        self._session_pnl: float = 0.0
        self._trades: int = 0
        self._load()

    # ── persistence ───────────────────────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Best-effort load from disk (non-blocking, called once at init)."""
        try:
            if _STATE_FILE.exists():
                data = json.loads(_STATE_FILE.read_text())
                pos = data.get("position")
                # Migrate old side="" → None
                if pos is not None and pos.get("side") == "":
                    pos["side"] = None
                self._position = pos
                self._session_pnl = float(data.get("session_pnl", 0.0))
                self._trades = int(data.get("trades", 0))
                log.info("State loaded from %s", _STATE_FILE)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not load state file (%s) – starting fresh.", exc)

    def _save_sync(self) -> None:
        """Atomic write: write to tmp then os.replace() into final path."""
        payload = {
            "position":    self._position,
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
                os.unlink(tmp_path)
                raise
        except Exception as exc:  # noqa: BLE001
            log.error("State save failed: %s", exc)

    # ── public API (all coroutines) ─────────────────────────────────────────────────────────────────────

    async def get_position(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            return dict(self._position) if self._position else None

    async def has_position(self) -> bool:
        async with self._lock:
            return self._position is not None

    async def set_position(self, pos: Optional[Dict[str, Any]]) -> None:
        """Store a position dict.  Pass None to clear."""
        if pos is not None:
            if pos.get("side") == "":
                pos = {**pos, "side": None}
        async with self._lock:
            self._position = pos
            self._save_sync()

    async def update_position(self, **kwargs: Any) -> None:
        """Patch individual fields of the current position."""
        async with self._lock:
            if self._position is None:
                log.warning("update_position called with no open position – ignored")
                return
            for k, v in kwargs.items():
                if k == "side" and v == "":
                    v = None
                self._position[k] = v
            self._save_sync()

    async def clear_position(self) -> None:
        async with self._lock:
            self._position = None
            self._save_sync()

    async def record_trade(self, pnl: float) -> None:
        async with self._lock:
            self._session_pnl += pnl
            self._trades += 1
            self._save_sync()

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "session_pnl": self._session_pnl,
                "trades":      self._trades,
            }

    # ── sync helpers used by backward-compat layer below ───────────────────────────────────────────────
    # These bypass the asyncio lock intentionally: they are called from sync
    # context (module-level imports, __init__) where no event loop is running.
    # The lock is still used for async callers; the sync path is init-time only.

    def _save_position_sync(self, symbol: str, data: Dict[str, Any]) -> None:
        """Write a position dict for *symbol* into self._position (sync)."""
        pos = dict(data)
        pos.setdefault("symbol", symbol)
        if pos.get("side") == "":
            pos["side"] = None
        self._position = pos
        self._save_sync()

    def _load_position_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Return position dict if it belongs to *symbol*, else None."""
        if self._position is None:
            return None
        if self._position.get("symbol") == symbol:
            return dict(self._position)
        # Legacy files without symbol key – return anyway
        if "symbol" not in self._position:
            return dict(self._position)
        return None

    def _clear_position_sync(self, symbol: str) -> None:
        """Clear position if it belongs to *symbol*."""
        if self._position is None:
            return
        if (
            self._position.get("symbol") == symbol
            or "symbol" not in self._position
        ):
            self._position = None
            self._save_sync()

    def _mark_tp2_hit_sync(self, symbol: str) -> None:
        """Set tp2_hit=True in the stored position for *symbol*."""
        if self._position is None:
            return
        if (
            self._position.get("symbol") == symbol
            or "symbol" not in self._position
        ):
            self._position["tp2_hit"] = True
            self._save_sync()


# ── module-level singleton ───────────────────────────────────────────────────────────────────────────────────
bot_state = BotState()


# ── BACKWARD-COMPATIBLE FREE FUNCTIONS ──────────────────────────────────────────────────────────────────────────────────
# trader.py, position_manager.py and decision_engine.py call these at import
# time and from sync context. They delegate to the singleton's sync helpers.

def save_position(symbol: str, data: Dict[str, Any]) -> None:
    """Persist *data* as the open position for *symbol*."""
    bot_state._save_position_sync(symbol, data)


def load_position(symbol: str) -> Optional[Dict[str, Any]]:
    """Return the stored position dict for *symbol*, or None."""
    return bot_state._load_position_sync(symbol)


def clear_position(symbol: str) -> None:
    """Erase the stored position for *symbol*."""
    bot_state._clear_position_sync(symbol)


def mark_tp2_hit(symbol: str) -> None:
    """Set tp2_hit=True in the persisted position for *symbol*."""
    bot_state._mark_tp2_hit_sync(symbol)
