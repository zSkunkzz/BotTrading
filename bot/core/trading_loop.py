"""
trading_loop.py — Loop principal de trading para un símbolo.

Responsabilidades:
  - Inicialización del trader (leverage, restaurar posición, balance svc)
  - Loop principal: _iteration()
  - Sincronización periódica con el exchange (_get_positions)
  - Delegar decisiones a DecisionEngine
  - Delegar gestión de posición abierta a PositionManager

FIX v3 (2026-06-02):
  DecisionEngine.__init__() espera (risk_manager, pretrade_risk, signal_engine, cooldown).
  trading_loop.py pasaba (symbol, position_mgr=...) causando TypeError en cada arranque.
  Fix: DecisionEngine se construye lazy en run() cuando ya tenemos todos los objetos,
  y evaluate() se llama con la firma correcta (symbol, price, ohlcv).

FIX v4 (2026-06-02):
  `from bot.cooldown import signal_cooldown` → módulo incorrecto.
  El archivo real es bot/signal_cooldown.py.
  Fix: `from bot.signal_cooldown import signal_cooldown`.
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
        self._position_mgr    = PositionManager(symbol)
        # DecisionEngine se construye lazy en run() — necesita risk/pretrade/signal/cooldown
        self._decision_engine = None
        self._last_pos_check_at: float = 0.0

    def _build_decision_engine(self, risk):
        """Construye DecisionEngine con los objetos correctos del risk manager."""
        from bot import signal_engine
        # FIX v4: el módulo se llama signal_cooldown.py, no cooldown.py
        from bot.signal_cooldown import signal_cooldown

        # risk es el objeto TradeRisk / GlobalRisk que llega desde main.py
        # Intentamos obtener pretrade_risk desde risk; si no existe, usamos risk mismo.
        pretrade_risk = getattr(risk, "pretrade_risk", risk)
        risk_manager  = getattr(risk, "risk_manager",  risk)

        return DecisionEngine(
            risk_manager  = risk_manager,
            pretrade_risk = pretrade_risk,
            signal_engine = signal_engine,
            cooldown      = signal_cooldown,
        )

    async def run(self, trader, risk, *, global_risk=None) -> None:
        """Loop principal. Llamar desde FuturesTrader.run()."""
        # Construir DecisionEngine ahora que tenemos risk disponible
        if self._decision_engine is None:
            self._decision_engine = self._build_decision_engine(global_risk or risk)

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
            trader._tp1_be_done   = False
            entry_mode = saved.get("entry_mode", "")
            if entry_mode:
                self._position_mgr.set_entry_mode(entry_mode)
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
            exchange_positions      = await trader._get_positions()
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
                    logger.info(
                        "[%s] Posición cerrada externamente — delegando a detect_external_close().",
                        self.symbol,
                    )
                    try:
                        trader._hl_client.cancel_all_open_tpsl()
                        logger.info("[%s] Trigger orders huérfanos cancelados.", self.symbol)
                    except Exception as e:
                        logger.warning(
                            "[%s] No se pudieron cancelar triggers huérfanos: %s",
                            self.symbol, e,
                        )
                    self._position_mgr.detect_external_close(trader)

        # ── Gestionar posición abierta o evaluar nueva entrada ────────────────
        if trader.position is not None:
            await self._position_mgr.manage(trader, price, risk)
        else:
            # FIX v3: evaluate() espera (symbol, price, ohlcv=[]) — no (trader, risk, global_risk)
            signal = await self._decision_engine.evaluate(self.symbol, price, [])
            if signal:
                logger.info(
                    "[%s] Señal aceptada por DecisionEngine: action=%s side=%s",
                    self.symbol, signal.get("action"), signal.get("side"),
                )
                # Delegar apertura de posición al PositionManager
                await self._position_mgr.open_position(trader, signal, risk)
