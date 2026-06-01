"""
global_risk.py — Control de riesgo global (max trades simultáneos + daily loss).

MEJORAS:
  - Persistencia en disco: el estado se guarda en GLOBAL_RISK_STATE_PATH
    para sobrevivir reinicios/deploys. Sin esto, tras un crash el bot
    cree que tiene 0 posiciones abiertas y puede abrir más de las permitidas.
  - register_close() ahora acepta symbol opcional para logging.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

logger = logging.getLogger("GlobalRisk")

_STATE_PATH = Path(os.getenv("GLOBAL_RISK_STATE_PATH", "/tmp/global_risk_state.json"))

if str(_STATE_PATH).startswith("/tmp"):
    logger.warning(
        "[GlobalRisk] ⚠️  GLOBAL_RISK_STATE_PATH apunta a /tmp. "
        "El contador de posiciones abiertas se perderá en cada restart. "
        "Monta un volumen y configura GLOBAL_RISK_STATE_PATH=/data/global_risk_state.json"
    )


class GlobalRisk:
    def __init__(self, max_concurrent_trades: int, max_global_daily_loss_pct: float):
        self.max_concurrent = max_concurrent_trades
        self.max_daily_loss = max_global_daily_loss_pct
        self._open      = 0
        self._daily_pnl = 0.0
        self._lock      = asyncio.Lock()
        self._load_state()

    async def can_open(self) -> tuple[bool, str]:
        async with self._lock:
            if self._open >= self.max_concurrent:
                return False, f"Global max trades ({self.max_concurrent}) alcanzado"
            if self._daily_pnl <= -self.max_daily_loss:
                return False, f"Global daily loss {self.max_daily_loss}% alcanzado — bot pausado"
            return True, "OK"

    async def register_open(self) -> None:
        async with self._lock:
            self._open += 1
            logger.debug(f"Posiciones abiertas: {self._open}/{self.max_concurrent}")
            self._save_state()

    async def register_close(self, pnl_pct: float, symbol: str = "") -> None:
        """
        Decrementa el contador de posiciones abiertas y acumula PnL diario.
        symbol es opcional, solo para logging.
        """
        async with self._lock:
            self._open      = max(0, self._open - 1)
            self._daily_pnl += pnl_pct
            tag = f" [{symbol}]" if symbol else ""
            logger.info(f"Global PnL del día{tag}: {self._daily_pnl:+.2f}% | posiciones abiertas: {self._open}")
            self._save_state()

    def reset_daily(self) -> None:
        self._daily_pnl = 0.0
        self._save_state()
        logger.info("Global daily PnL reseteado")

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _save_state(self) -> None:
        """Escritura atómica: .tmp → rename."""
        try:
            state = {
                "open":      self._open,
                "daily_pnl": self._daily_pnl,
                "saved_at":  time.time(),
            }
            _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = _STATE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2))
            tmp.replace(_STATE_PATH)
        except Exception as e:
            logger.warning(f"[GlobalRisk] _save_state error: {e}")

    def _load_state(self) -> None:
        try:
            if _STATE_PATH.exists():
                state = json.loads(_STATE_PATH.read_text())
                self._open      = int(state.get("open", 0))
                self._daily_pnl = float(state.get("daily_pnl", 0.0))
                if self._open > 0:
                    logger.warning(
                        f"[GlobalRisk] Arrancado con {self._open} posición(es) abierta(s) "
                        f"según estado guardado."
                    )
        except Exception as e:
            logger.warning(f"[GlobalRisk] _load_state error: {e}")
