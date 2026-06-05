"""
trading_loop.py — Loop principal de trading para un símbolo.

FIX v8 (2026-06-02): notificaciones Telegram + cooldown post-cierre externo
FIX v9 (2026-06-03): on_position_closed + global_risk en cierre externo
FIX v10 (2026-06-03): timeout en evaluate() + logs de diagnóstico
FIX v11 (2026-06-03): logs INFO visibles en cada scan
FIX v12 (2026-06-03): PnL real en notify_close para cierre externo
FIX v13 (2026-06-05): corrección de bugs B/C/E en notificaciones Telegram
FIX v14 (2026-06-05): corregir AttributeError trader._info_post en _init()
  balance_svc ya está inicializado en main() antes de arrancar traders.
  La llamada redundante balance_svc.init_hl(trader._info_post) lanzaba
  AttributeError porque FuturesTrader no tiene ese atributo.
  FIX: guardar el init_hl con is_ready() check — si ya está listo, skip.
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
_EXTERNAL_CLOSE_COOLDOWN_S = int(os.getenv("EXTERNAL_CLOSE_COOLDOWN_S", "600"))

_GET_PRICE_TIMEOUT_S    = float(os.getenv("GET_PRICE_TIMEOUT_S",    "10"))
_GET_POS_TIMEOUT_S      = float(os.getenv("GET_POS_TIMEOUT_S",      "15"))
_EVALUATE_TIMEOUT_S     = float(os.getenv("EVALUATE_TIMEOUT_S",     "60"))

_SCAN_LOG_EVERY         = int(os.getenv("SCAN_LOG_EVERY", "1"))


def _calc_pnl_pct(entry: float, exit_p: float, is_long: bool, leverage: int) -> float:
    if not entry or entry <= 0:
        return 0.0
    raw = (exit_p - entry) / entry if is_long else (entry - exit_p) / entry
    return raw * leverage * 100


class TradingLoop:

    def __init__(self, symbol: str):
        self.symbol           = symbol
        self._position_mgr    = None
        self._decision_engine = None
        self._last_pos_check_at: float = 0.0
        self._iteration_count: int = 0
        self._global_risk = None
        self._open_notified: bool = False

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
        # _HLCore ya fue pre-calentado en main() — esta llamada es instantánea.
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

            self._open_notified = True
            logger.info(
                "[%s] Posición restaurada: %s @ %s",
                self.symbol, trader.position, trader.entry_price,
            )

        remaining = get_cooldown_remaining(self.symbol)
        if remaining > 0:
            logger.info(
                "[%s] Cooldown post-cierre activo desde disco: %.0f s restantes.",
                self.symbol, remaining,
            )

        # FIX v14: balance_svc ya inicializado en main() — no llamar de nuevo.
        # La llamada anterior usaba trader._info_post (atributo inexistente)
        # y lanzaba AttributeError silenciado por el except de run().
        if not balance_svc.is_ready():
            logger.warning(
                "[%s] balance_svc no listo en _init — se esperaba init en main().",
                self.symbol,
            )

        await trader._set_leverage(trader.leverage)
        logger.info(
            "[%s] TradingLoop iniciado | coin=%s | master=%s | agent_mode=%s",
            self.symbol, trader.coin,
            trader._master_addr[:10] + "..." if trader._master_addr else "N/A",
            trader._agent_mode,
        )

    async def _iteration(self, trader, risk, global_risk) -> None:
        self._iteration_count += 1
        n = self._iteration_count

        logger.debug("[%s] _iteration #%d iniciada", self.symbol, n)

        if kill_switch.is_halted(self.symbol):
            logger.debug("[%s] Kill switch activo — skip.", self.symbol)
            return

        try:
            price = await asyncio.wait_for(
                trader.get_price(),
                timeout=_GET_PRICE_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning("[%s] get_price() timeout (%ss) — skip iteración.",
                           self.symbol, _GET_PRICE_TIMEOUT_S)
            return
        except Exception as e:
            logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
            return

        logger.debug("[%s] #%d precio=%.4f", self.symbol, n, price)

        now = time.monotonic()
        if now - self._last_pos_check_at >= _POS_CHECK_INTERVAL_S:
            try:
                exchange_positions = await asyncio.wait_for(
                    trader._get_positions(),
                    timeout=_GET_POS_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning("[%s] _get_positions() timeout (%ss) — skip sync.",
                               self.symbol, _GET_POS_TIMEOUT_S)
                exchange_positions = None
            except Exception as e:
                logger.warning("[%s] _get_positions() error: %s", self.symbol, e)
                exchange_positions = None

            self._last_pos_check_at = now

            if exchange_positions is not None:
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
                        closed_side  = trader.position
                        entry_price  = trader.entry_price or price
                        leverage     = int(trader._open_leverage or trader.leverage or 1)
                        is_long      = closed_side == "long"
                        exit_price   = round(price, 6)

                        pnl_pct = _calc_pnl_pct(entry_price, exit_price, is_long, leverage)

                        logger.info(
                            "[%s] Posición cerrada externamente — %s | entry=%.4f exit=%.4f "
                            "lev=%dx | PnL=%+.2f%%",
                            self.symbol, closed_side.upper(),
                            entry_price, exit_price, leverage, pnl_pct,
                        )

                        if self._open_notified:
                            try:
                                from bot.telegram_bot import notify_close
                                await notify_close(
                                    symbol  = self.symbol,
                                    side    = closed_side,
                                    exit_p  = exit_price,
                                    pnl     = pnl_pct,
                                    entry   = round(entry_price, 6),
                                    reason  = "Cierre manual / externo",
                                    dry_run = trader.dry_run,
                                )
                            except Exception as _te:
                                logger.debug("[%s] notify_close error: %s", self.symbol, _te)
                        else:
                            logger.info(
                                "[%s] Cierre externo detectado pero apertura nunca notificada "
                                "— notify_close omitido.",
                                self.symbol,
                            )

                        self._open_notified = False

                        try:
                            trader._hl_client.cancel_all_open_tpsl()
                            logger.info("[%s] Trigger orders huérfanos cancelados.", self.symbol)
                        except Exception as e:
                            logger.warning(
                                "[%s] No se pudieron cancelar triggers huérfanos: %s",
                                self.symbol, e,
                            )

                        try:
                            if self._decision_engine is not None:
                                await self._decision_engine.on_position_closed(
                                    symbol     = self.symbol,
                                    pnl        = pnl_pct,
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

                        _gr = self._global_risk or global_risk
                        if _gr is not None:
                            try:
                                await _gr.register_close(pnl_pct=pnl_pct, symbol=self.symbol)
                                logger.info(
                                    "[%s] global_risk.register_close() llamado — slot liberado.",
                                    self.symbol,
                                )
                            except Exception as e:
                                logger.warning(
                                    "[%s] global_risk.register_close() error en cierre externo: %s",
                                    self.symbol, e,
                                )

                        save_cooldown(self.symbol, _EXTERNAL_CLOSE_COOLDOWN_S)

                        trader.position    = None
                        trader.entry_price = None
                        trader.sl          = None
                        trader.tp1         = None
                        trader.tp2         = None
                        trader.tp3         = None
                        trader._open_qty   = 0.0
                        clear_position(self.symbol)

        if trader.position is not None:
            trader._last_price = price

            if n % max(1, _SCAN_LOG_EVERY) == 0:
                entry = trader.entry_price or price
                pnl_pct = ((price - entry) / entry * 100) if trader.position == "long" \
                          else ((entry - price) / entry * 100)
                sl_dist = abs(price - trader.sl) / price * 100 if trader.sl else 0
                tp_dist = abs(trader.tp1 - price) / price * 100 if trader.tp1 else 0
                logger.info(
                    "[%s] \U0001f4ca %s @ %.4f | entry=%.4f | PnL=%+.2f%% | "
                    "SL=%.4f (%.2f%%) | TP1=%.4f (%.2f%%)",
                    self.symbol, trader.position.upper(), price,
                    entry, pnl_pct,
                    trader.sl or 0, sl_dist,
                    trader.tp1 or 0, tp_dist,
                )

            await self._position_mgr.manage()
        else:
            remaining = get_cooldown_remaining(self.symbol)
            if remaining > 0:
                if n % max(1, _SCAN_LOG_EVERY) == 0:
                    logger.info(
                        "[%s] \u23f3 Cooldown activo — %.0f s restantes.",
                        self.symbol, remaining,
                    )
                return

            if n % max(1, _SCAN_LOG_EVERY) == 0:
                logger.info(
                    "[%s] \U0001f50d Scan #%d | precio=%.4f | buscando señal...",
                    self.symbol, n, price,
                )

            ohlcv_fn = trader.get_ohlcv_fn()
            try:
                signal = await asyncio.wait_for(
                    self._decision_engine.evaluate(
                        self.symbol, price, ohlcv_fn
                    ),
                    timeout=_EVALUATE_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "[%s] #%d evaluate() timeout (%ss) — posiblemente OHLCV fetch colgado. "
                    "Verifica conexión a HL API.",
                    self.symbol, n, _EVALUATE_TIMEOUT_S,
                )
                return
            except Exception as e:
                logger.error("[%s] #%d evaluate() error: %s", self.symbol, n, e, exc_info=True)
                return

            if signal:
                score = signal.get("score", signal.get("strength", "?"))
                mode  = signal.get("mode", signal.get("entry_mode", "?"))
                logger.info(
                    "[%s] \u2705 Señal aceptada por DecisionEngine: action=%s side=%s "
                    "score=%s mode=%s entry=%.4f sl=%.4f tp1=%.4f",
                    self.symbol,
                    signal.get("action"), signal.get("side"),
                    score, mode,
                    float(signal.get("entry") or price),
                    float(signal.get("sl") or 0),
                    float(signal.get("tp1") or 0),
                )
                pos_before = trader.position
                await trader.open_order(signal, risk)

                if trader.position is not None and pos_before is None and not self._open_notified:
                    _entry = trader.entry_price or float(signal.get("entry") or price)
                    _sl    = trader.sl    or float(signal.get("sl")  or 0) or None
                    _tp1   = trader.tp1   or float(signal.get("tp1") or 0) or None
                    _tp2   = trader.tp2   or float(signal.get("tp2") or 0) or None
                    _tp3   = trader.tp3   or float(signal.get("tp3") or 0) or None
                    _size  = trader._open_notional or getattr(risk, "usdc_per_trade", None)

                    self._open_notified = True
                    try:
                        from bot.telegram_bot import notify_open
                        await notify_open(
                            symbol     = self.symbol,
                            side       = trader.position,
                            price      = _entry,
                            leverage   = trader.leverage,
                            size_usdc  = _size,
                            sl         = _sl,
                            tp1        = _tp1,
                            tp2        = _tp2,
                            tp3        = _tp3,
                            dry_run    = trader.dry_run,
                            entry_mode = signal.get("entry_mode"),
                        )
                    except Exception as _te:
                        logger.debug("[%s] notify_open error: %s", self.symbol, _te)
            else:
                if n % max(1, _SCAN_LOG_EVERY) == 0:
                    logger.info(
                        "[%s] \u2b1c Sin señal | precio=%.4f",
                        self.symbol, price,
                    )
