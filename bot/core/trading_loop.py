"""
trading_loop.py — Loop principal de trading para un símbolo.

Responsabilidades:
  - Inicialización del trader (leverage, restaurar posición, balance svc)
  - Loop principal: _iteration()
  - Sincronización periódica con el exchange (_get_positions)
  - Delegar decisiones a DecisionEngine
  - Delegar gestión de posición abierta a PositionManager

Extraído de FuturesTrader.run / _iteration / _init en trader.py.
NOTA: FuturesTrader en trader.py sigue siendo el punto de entrada público
para compatibilidad con main.py — este módulo contiene la lógica extraída
para mejorar la legibilidad y testabilidad.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from bot.state import load_position, clear_position
from bot.balance_service import balance_svc
from bot.kill_switch import kill_switch
from bot.core.decision_engine import DecisionEngine
from bot.core.position_manager import PositionManager

logger = logging.getLogger("TradingLoop")

LOOP_SLEEP              = float(os.getenv("LOOP_SLEEP", "10"))
_POS_CHECK_INTERVAL_S   = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))
_TPSL_VERIFY_INTERVAL_S = int(os.getenv("TPSL_VERIFY_INTERVAL_S", "120"))


class TradingLoop:
    """
    Orquesta el loop de trading para un símbolo específico.
    Utiliza DecisionEngine para evaluar entradas y PositionManager
    para gestionar posiciones abiertas.
    """

    def __init__(self, symbol: str):
        self.symbol           = symbol
        self._decision_engine = DecisionEngine(symbol)
        self._position_mgr    = PositionManager(symbol)
        self._last_pos_check_at: float = 0.0

    async def run(self, trader, risk, *, global_risk=None) -> None:
        """Loop principal. Llamar desde FuturesTrader.run()."""
        await self._init(trader, risk.usdc_per_trade)
        while True:
            try:
                await self._iteration(trader, risk, global_risk)
            except asyncio.CancelledError:
                logger.info("[%s] TradingLoop cancelado.", self.symbol)
                raise
            except Exception as e:
                logger.error("[%s] Error en iteración: %s", self.symbol, e, exc_info=True)
            await asyncio.sleep(LOOP_SLEEP)

    async def _init(self, trader, usdc_per_trade: float) -> None:
        """Inicialización: restaura posición guardada y configura apalancamiento."""
        await trader._get_ccxt()
        saved = load_position(self.symbol)
        if saved:
            trader.position       = saved["side"]
            trader.entry_price    = saved["entry"]
            trader.sl             = saved.get("sl")
            trader.tp1            = saved.get("tp1")
            trader.tp2            = saved.get("tp2")
            trader.tp3            = saved.get("tp3")
            trader.tp2_hit        = saved.get("tp2_hit", False)
            trader._open_notional = saved.get("usdc_amount", saved.get("usdt_amount", 0.0))
            trader._open_leverage = saved.get("leverage", trader.leverage)
            trader._protection_ok = True
            logger.info(
                "[%s] Posición restaurada: %s @ %s",
                self.symbol, trader.position, trader.entry_price,
            )

        if not balance_svc.is_ready():
            balance_svc.init_hl(trader._master_addr, trader._info_post)

        await trader._set_leverage(trader.leverage)
        logger.info(
            "[%s] TradingLoop iniciado | coin=%s | master=%s | agent_mode=%s",
            self.symbol, trader.coin,
            trader._master_addr[:10] + "..." if trader._master_addr else "N/A",
            trader._agent_mode,
        )

    async def _iteration(self, trader, risk, global_risk) -> None:
        """Una iteración del loop de trading."""
        if kill_switch.is_halted(self.symbol):
            logger.debug("[%s] Kill switch activo — skip.", self.symbol)
            return

        try:
            price = await trader.get_price()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
            return

        # ── Sincronización periódica con exchange ─────────────────────────────
        now = time.monotonic()
        if now - self._last_pos_check_at >= _POS_CHECK_INTERVAL_S:
            exchange_positions       = await trader._get_positions()
            self._last_pos_check_at = now

            if exchange_positions:
                ep = exchange_positions[0]
                if trader.position is None:
                    trader.position    = ep["side"]
                    trader.entry_price = ep["entryPx"]
                    logger.info(
                        "[%s] Posición detectada en exchange: %s @ %s",
                        self.symbol, trader.position, trader.entry_price,
                    )
            else:
                if trader.position is not None:
                    logger.info("[%s] Posición cerrada externamente.", self.symbol)
                    trader.position    = None
                    trader.entry_price = None
                    trader.sl = trader.tp1 = trader.tp2 = trader.tp3 = None
                    trader.tp2_hit = False
                    clear_position(self.symbol)

        # ── Gestionar posición abierta o evaluar nueva entrada ────────────────
        if trader.position is not None:
            await self._position_mgr.manage(trader, price, risk)
        else:
            await self._decision_engine.evaluate(trader, risk, global_risk)
