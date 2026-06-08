"""
global_risk.py — Control de riesgo global (max trades simultáneos + daily loss).

MEJORAS:
  - Persistencia en disco: el estado se guarda en GLOBAL_RISK_STATE_PATH
    para sobrevivir reinicios/deploys. Sin esto, tras un crash el bot
    cree que tiene 0 posiciones abiertas y puede abrir más de las permitidas.
  - register_close() ahora acepta symbol opcional para logging.
  - sync_open_count(n): sincroniza _open con el número real de posiciones
    abiertas en el exchange tras arranque (evita bloqueos por stale state).

Fix (2026-06-08) — GlobalRisk trabaja en USDT absolutos:
  Antes, max_global_daily_loss_pct esperaba un porcentaje pero
  _register_close_safe pasaba pnl en USDT absolutos, corrompiendo
  el acumulado diario. Ahora todo trabaja en USDT absolutos:
  - Parámetro renombrado: max_global_daily_loss_pct → max_global_daily_loss_usdt
  - _daily_pnl acumula USDT reales (positivos = ganancia, negativos = pérdida)
  - El check can_open() compara contra -max_global_daily_loss_usdt
  - Logs actualizados de % a USDT
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
    def __init__(self, max_concurrent_trades: int, max_global_daily_loss_usdt: float):
        """
        Args:
            max_concurrent_trades: Número máximo de posiciones abiertas simultáneamente.
            max_global_daily_loss_usdt: Pérdida máxima diaria en USDT absolutos.
                El bot se pausará cuando _daily_pnl <= -max_global_daily_loss_usdt.
                Ejemplo: 50.0 pausa el bot al perder 50 USDT en el día.
        """
        self.max_concurrent   = max_concurrent_trades
        self.max_daily_loss   = max_global_daily_loss_usdt  # USDT absolutos
        self._open            = 0
        self._daily_pnl       = 0.0  # USDT acumulados en el día (+ ganancia, - pérdida)
        self._lock            = asyncio.Lock()
        self._load_state()

    async def can_open(self) -> tuple[bool, str]:
        async with self._lock:
            if self._open >= self.max_concurrent:
                return False, f"Global max trades ({self.max_concurrent}) alcanzado"
            if self._daily_pnl <= -self.max_daily_loss:
                return False, (
                    f"Global daily loss {self.max_daily_loss:.2f} USDT alcanzado "
                    f"(acumulado: {self._daily_pnl:+.2f} USDT) — bot pausado"
                )
            return True, "OK"

    async def register_open(self) -> None:
        async with self._lock:
            self._open += 1
            logger.debug(f"Posiciones abiertas: {self._open}/{self.max_concurrent}")
            self._save_state()

    async def register_close(self, pnl_pct: float, symbol: str = "") -> None:
        """
        Decrementa el contador de posiciones abiertas y acumula PnL diario.

        Args:
            pnl_pct: PnL del trade en USDT absolutos (nombre mantenido por
                     compatibilidad con callers existentes). Positivo = ganancia,
                     negativo = pérdida.
            symbol:  Opcional, solo para logging.
        """
        async with self._lock:
            self._open      = max(0, self._open - 1)
            self._daily_pnl += pnl_pct  # pnl_pct es en realidad USDT absolutos
            tag = f" [{symbol}]" if symbol else ""
            logger.info(
                f"Global PnL del día{tag}: {self._daily_pnl:+.2f} USDT "
                f"(límite: -{self.max_daily_loss:.2f} USDT) | "
                f"posiciones abiertas: {self._open}"
            )
            self._save_state()

    def reset_daily(self) -> None:
        self._daily_pnl = 0.0
        self._save_state()
        logger.info("Global daily PnL reseteado (USDT)")

    async def sync_open_count(self, real_count: int) -> None:
        """
        Sincroniza _open con el número real de posiciones abiertas en el exchange.
        Llamar después de _purge_stale_state() en el arranque para evitar que
        un contador obsoleto en disco bloquee can_open() para todos los traders.
        """
        async with self._lock:
            if self._open != real_count:
                logger.info(
                    "[GlobalRisk] Corrigiendo contador: disco=%d → real=%d",
                    self._open, real_count,
                )
                self._open = real_count
                self._save_state()
            else:
                logger.debug("[GlobalRisk] Contador ya sincronizado: %d", self._open)

    # ── Persistencia ─────────────────────────────────────────────────────────────

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
