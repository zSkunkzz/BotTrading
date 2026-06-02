"""
trading_loop.py — Loop principal de trading para un símbolo.

FIX v8 (2026-06-02): notificaciones Telegram + cooldown post-cierre externo
  - Llamar notify_open() justo después de open_order() exitoso.
  - Llamar notify_close() al detectar cierre externo en _iteration().
  - Cooldown post-cierre PERSISTIDO en bot_state (JSON en disco):
    antes vivía solo en la instancia de TradingLoop, que BitgetBot destruye
    y recrea cada 15 ciclos sin posición — perdiendo el cooldown y causando
    que la posición se volviera a abrir inmediatamente.
    Ahora: save_cooldown() / get_cooldown_remaining() en bot/state.py.

FIX v9 (2026-06-03): on_position_closed + global_risk en cierre externo
  BUG CRÍTICO A: al cerrar posición externamente, no se llamaba a
  decision_engine.on_position_closed() → pretrade_risk._open_margin nunca
  se liberaba → Gate 2 bloqueaba TODAS las señales para siempre.
  Fix: llamar _decision_engine.on_position_closed() en cierre externo.

  BUG CRÍTICO B: global_risk.register_open() se llama en open_order pero
  global_risk.register_close() NUNCA se invocaba en cierre externo →
  global_risk._open crecía indefinidamente → can_open() siempre False.
  Fix: llamar global_risk.register_close(pnl_pct=0.0) en cierre externo.

  BUG C (idle rotation): _idle_cycles usaba bot_state._positions como
  fuente de verdad para saber si hay posición. En dry_run esto puede estar
  desincronizado. Fix: usar trader.position (estado en memoria) como
  fuente de verdad primaria.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from bot.state import load_position, clear_position, save_cooldown, get_cooldown_remaining
from bot.balance_service import balance_svc
from bot.kill_switch import kill_switch
from bot.core.decision_engine import DecisionEngine
from bot.position_manager import PositionManager

logger = logging.getLogger("TradingLoop")

LOOP_SLEEP              = float(os.getenv("LOOP_SLEEP", "10"))
_POS_CHECK_INTERVAL_S   = int(os.getenv("POS_CHECK_INTERVAL_S", "30"))
_TPSL_VERIFY_INTERVAL_S = int(os.getenv("TPSL_VERIFY_INTERVAL_S", "120"))
# Segundos que el bot espera para re-entrar tras un cierre externo/manual
_EXTERNAL_CLOSE_COOLDOWN_S = int(os.getenv("EXTERNAL_CLOSE_COOLDOWN_S", "600"))


class TradingLoop:

    def __init__(self, symbol: str):
        self.symbol           = symbol
        self._position_mgr    = None
        self._decision_engine = None
        self._last_pos_check_at: float = 0.0
        # Guardamos referencia al global_risk para poder llamar register_close
        self._global_risk = None

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

        # FIX CRÍTICO B: guardar referencia al global_risk para cierre externo
        if global_risk is not None:
            self._global_risk = global_risk

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

            if saved.get("qty") and float(saved["qty"]) > 0:
                trader._open_qty = float(saved["qty"])
            else:
                entry_px = float(trader.entry_price or 0)
                notional_usdc = float(trader._open_notional or 0)
                lev = int(trader._open_leverage or trader.leverage or 1)
                if entry_px > 0 and notional_usdc > 0:
                    raw_qty = (notional_usdc * lev) / entry_px
                    try:
                        trader._open_qty = trader._hl_client.round_sz(raw_qty)
                    except Exception:
                        trader._open_qty = raw_qty
                    logger.info(
                        "[%s] _open_qty recalculado desde disco: %.6f "
                        "(notional=%.2f lev=%dx entry=%.4f)",
                        self.symbol, trader._open_qty, notional_usdc, lev, entry_px,
                    )
                else:
                    trader._open_qty = 0.0
                    logger.warning(
                        "[%s] _open_qty no se pudo recalcular — TPSL de emergencia deshabilitado.",
                        self.symbol,
                    )

            logger.info(
                "[%s] Posición restaurada: %s @ %s",
                self.symbol, trader.position, trader.entry_price,
            )

        # Informar si hay cooldown activo al iniciar (sobrevive a rotación)
        remaining = get_cooldown_remaining(self.symbol)
        if remaining > 0:
            logger.info(
                "[%s] Cooldown post-cierre activo desde disco: %.0f s restantes.",
                self.symbol, remaining,
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

        # ── Sincronización periódica con exchange ──────────────────────────
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
                if trader._open_qty == 0.0 and ep.get("size", 0) > 0:
                    try:
                        trader._open_qty = trader._hl_client.round_sz(float(ep["size"]))
                    except Exception:
                        trader._open_qty = float(ep["size"])
                    logger.info(
                        "[%s] _open_qty sincronizado desde exchange: %.6f",
                        self.symbol, trader._open_qty,
                    )
            else:
                if trader.position is not None:
                    closed_side = trader.position
                    logger.info(
                        "[%s] Posición cerrada externamente — limpiando estado local.",
                        self.symbol,
                    )
                    # ── Notificar cierre por Telegram ──────────────────────
                    try:
                        from bot.telegram_bot import notify_close
                        await notify_close(
                            symbol   = self.symbol,
                            side     = closed_side,
                            exit_p   = round(price, 6),
                            pnl      = 0.0,
                            entry    = trader.entry_price,
                            reason   = "Cierre manual / externo",
                            dry_run  = trader.dry_run,
                        )
                    except Exception as _te:
                        logger.debug("[%s] notify_close error: %s", self.symbol, _te)

                    try:
                        trader._hl_client.cancel_all_open_tpsl()
                        logger.info("[%s] Trigger orders huérfanos cancelados.", self.symbol)
                    except Exception as e:
                        logger.warning(
                            "[%s] No se pudieron cancelar triggers huérfanos: %s",
                            self.symbol, e,
                        )

                    # ── FIX CRÍTICO A: liberar pretrade_risk._open_margin ──
                    # Sin esto, el margen reservado nunca se libera y Gate 2
                    # bloquea TODAS las señales futuras para siempre.
                    try:
                        if self._decision_engine is not None:
                            await self._decision_engine.on_position_closed(
                                symbol     = self.symbol,
                                pnl        = 0.0,
                                reason     = "MANUAL_CLOSE",
                                entry_mode = "NORMAL",
                            )
                            logger.info(
                                "[%s] on_position_closed() llamado — pretrade_risk liberado.",
                                self.symbol,
                            )
                    except Exception as e:
                        logger.warning(
                            "[%s] on_position_closed() error en cierre externo: %s",
                            self.symbol, e,
                        )

                    # ── FIX CRÍTICO B: decrementar global_risk._open ───────
                    # Sin esto, global_risk._open nunca baja y can_open()
                    # devuelve False para siempre tras el primer trade.
                    _gr = self._global_risk or global_risk
                    if _gr is not None:
                        try:
                            await _gr.register_close(pnl_pct=0.0, symbol=self.symbol)
                            logger.info(
                                "[%s] global_risk.register_close() llamado — slot liberado.",
                                self.symbol,
                            )
                        except Exception as e:
                            logger.warning(
                                "[%s] global_risk.register_close() error en cierre externo: %s",
                                self.symbol, e,
                            )

                    # ── Activar cooldown PERSISTENTE (sobrevive a rotación) ─
                    save_cooldown(self.symbol, _EXTERNAL_CLOSE_COOLDOWN_S)

                    trader.position    = None
                    trader.entry_price = None
                    trader.sl          = None
                    trader.tp1         = None
                    trader.tp2         = None
                    trader.tp3         = None
                    trader._open_qty   = 0.0
                    clear_position(self.symbol)

        # ── Gestionar posición abierta o evaluar nueva entrada ─────────────
        if trader.position is not None:
            trader._last_price = price
            await self._position_mgr.manage()
        else:
            # Consultar cooldown desde disco (persiste entre rotaciones)
            remaining = get_cooldown_remaining(self.symbol)
            if remaining > 0:
                logger.debug(
                    "[%s] En cooldown post-cierre externo — %.0f s restantes.",
                    self.symbol, remaining,
                )
                return

            ohlcv_fn = trader.get_ohlcv_fn()
            signal = await self._decision_engine.evaluate(
                self.symbol, price, ohlcv_fn
            )
            if signal:
                logger.info(
                    "[%s] Señal aceptada por DecisionEngine: action=%s side=%s",
                    self.symbol, signal.get("action"), signal.get("side"),
                )
                pos_before = trader.position
                await trader.open_order(signal, risk)

                # Si la posición se abrió correctamente, notificar por Telegram
                if trader.position is not None and pos_before is None:
                    try:
                        from bot.telegram_bot import notify_open
                        await notify_open(
                            symbol     = self.symbol,
                            side       = trader.position,
                            price      = trader.entry_price,
                            leverage   = trader.leverage,
                            size_usdc  = trader._open_notional,
                            sl         = trader.sl,
                            tp1        = trader.tp1,
                            tp2        = trader.tp2,
                            tp3        = trader.tp3,
                            dry_run    = trader.dry_run,
                            entry_mode = signal.get("entry_mode"),
                        )
                    except Exception as _te:
                        logger.debug("[%s] notify_open error: %s", self.symbol, _te)
