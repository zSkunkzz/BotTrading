import asyncio
import logging

logger = logging.getLogger("GlobalRisk")

class GlobalRisk:
    def __init__(self, max_concurrent_trades, max_global_daily_loss_pct):
        self.max_concurrent = max_concurrent_trades
        self.max_daily_loss = max_global_daily_loss_pct
        self._open = 0
        self._daily_pnl = 0.0
        self._lock = asyncio.Lock()

    async def can_open(self):
        async with self._lock:
            if self._open >= self.max_concurrent:
                return False, f"Global max trades ({self.max_concurrent}) alcanzado"
            if self._daily_pnl <= -self.max_daily_loss:
                return False, f"Global daily loss {self.max_daily_loss}% alcanzado — bot pausado"
            return True, "OK"

    async def register_open(self):
        async with self._lock:
            self._open += 1
            logger.debug(f"Posiciones abiertas: {self._open}/{self.max_concurrent}")

    async def register_close(self, pnl_pct):
        async with self._lock:
            self._open = max(0, self._open - 1)
            self._daily_pnl += pnl_pct
            logger.info(f"Global PnL del día: {self._daily_pnl:+.2f}%")

    def reset_daily(self):
        self._daily_pnl = 0.0
        logger.info("Global daily PnL reseteado")
