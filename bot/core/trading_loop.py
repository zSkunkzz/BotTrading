"""
trading_loop.py — Loop principal de trading para un símbolo.

FIX ohlcv_fn (2026-06-02):
  trading_loop pasaba ohlcv=[] hardcodeado a evaluate(), causando que
  analyze_pair nunca tuviera velas reales y siempre devolviera HOLD.
  Fix: pasar trader.get_ohlcv_fn() como ohlcv_fn a evaluate().

FIX v3 (2026-06-02):
  DecisionEngine.__init__() espera (risk_manager, pretrade_risk, signal_engine, cooldown).
  Fix: DecisionEngine se construye lazy en run().

FIX v4 (2026-06-02):
  `from bot.cooldown import signal_cooldown` → módulo incorrecto.
  Fix: `from bot.signal_cooldown import signal_cooldown`

FIX v5 Bug Q (2026-06-02):
  _build_decision_engine usaba GlobalRisk como fallback para pretrade_risk.
  Fix: importar el singleton pretrade_risk directamente.

FIX v6 (2026-06-02): adapt to real PositionManager interface
  - PositionManager(trader) not PositionManager(symbol)
  - manage() takes no args (trader bound in __init__)
  - open_position() does not exist → use trader.open_order() directly
  - detect_external_close() and set_entry_mode() do not exist → removed
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
from bot.position_manager import PositionManager

logger = logging.getLogger("TradingLoop")

LOOP_SLEEP              = float(os.getenv("LOOP_SLEEP", "10"))
_POS_CHECK_INTERVAL_S   = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))
_TPSL_VERIFY_INTERVAL_S = int(os.getenv("TPSL_VERIFY_INTERVAL_S", "120"))


class TradingLoop:

    def __init__(self, symbol: str):
        self.symbol           = symbol
        # PositionManager se crea lazy en run() una vez que tenemos el trader
        self._position_mgr    = None
        self._decision_engine = None
        self._last_pos_check_at: float = 0.0

    def _build_decision_engine(self, risk):
        from bot import signal_engine
        from bot.signal_cooldown import signal_cooldown
        from bot.pretrade_risk import pretrade_risk as _pretrade_singleton

        return DecisionEngine(
            risk_manager  = risk,
            pretrade_risk = _pretrade_singleton,
            signal_engine = signal_engine,
            cooldown      = signal_cooldown,
        )

    async def run(self, trader, risk, *, global_risk=None) -> None:
        if self._decision_engine is None:
            self._decision_engine = self._build_decision_engine(global_risk or risk)

        # PositionManager necesita el trader; crearlo aquí la primera vez
        if self._position_mgr is None:
            self._position_mgr = PositionManager(trader)

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
                        "[%s] Posición cerrada externamente — limpiando estado local.",
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
                    # Limpiar estado local — PositionManager no tiene detect_external_close
                    trader.position    = None
                    trader.entry_price = None
                    trader.sl          = None
                    trader.tp1         = None
                    trader.tp2         = None
                    trader.tp3         = None
                    clear_position(self.symbol)

        # ── Gestionar posición abierta o evaluar nueva entrada ────────────────
        if trader.position is not None:
            # manage() no recibe argumentos — trader está bound en __init__
            # Actualizar _last_price para que _check_sl_software funcione
            trader._last_price = price
            await self._position_mgr.manage()
        else:
            # FIX ohlcv_fn: pasar trader.get_ohlcv_fn() para que analyze_pair
            # descargue velas reales. Antes se pasaba [] → siempre HOLD.
            ohlcv_fn = trader.get_ohlcv_fn()
            signal = await self._decision_engine.evaluate(
                self.symbol, price, ohlcv_fn
            )
            if signal:
                logger.info(
                    "[%s] Señal aceptada por DecisionEngine: action=%s side=%s",
                    self.symbol, signal.get("action"), signal.get("side"),
                )
                # open_position() no existe en PositionManager.
                # La apertura es responsabilidad del trader directamente.
                await trader.open_order(signal, risk)
