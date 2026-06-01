"""
decision_engine.py — Toma de decisiones de trading.

Responsabilidades:
  - Verificar cooldown de reapertura (signal_cooldown)
  - Verificar confirmación de vela cerrada antes de entrar (#7)
  - Verificar balance y risk pre-trade (pretrade_risk con firma correcta)
  - Llamar a decide() + ai_decide()
  - Calcular qty y apalancamiento
  - Aplicar sizing dinámico según win-rate histórico del modo (#8)
  - Abrir posición y persistir estado
  - Registrar exposición en pretrade_risk tras fill confirmado
  - Registrar señal en shadow_mode con entry_mode (#8)
  - Notificar apertura via Telegram
  - [V4] market_regime, daily_drawdown, kelly_sizer,
          structure_analyzer, correlation_guard
"""
from __future__ import annotations

import logging
import os

from bot.strategy import decide
from bot.ai_trader import ai_decide
from bot.telegram_bot import notify_open
from bot.state import save_position
from bot.balance_service import balance_svc
from bot.pretrade_risk import pretrade_risk
from bot.kill_switch import kill_switch
from bot.signal_cooldown import signal_cooldown
from bot.shadow_mode import shadow_mode
from bot.ws_feed import ws_feed

# ── V4 módulos ────────────────────────────────────────────────────────────────
try:
    from bot.market_regime import market_regime
    _REGIME_ENABLED = os.getenv("REGIME_FILTER", "false").lower() == "true"
except ImportError:
    market_regime = None
    _REGIME_ENABLED = False

try:
    from bot.daily_drawdown import daily_drawdown
    _DD_ENABLED = True
except ImportError:
    daily_drawdown = None
    _DD_ENABLED = False

try:
    from bot.kelly_sizer import kelly_sizer
    _KELLY_ENABLED = os.getenv("KELLY_ENABLED", "false").lower() == "true"
except ImportError:
    kelly_sizer = None
    _KELLY_ENABLED = False

try:
    from bot.structure_analyzer import structure_analyzer
    _STRUCTURE_ENABLED = os.getenv("STRUCTURE_ENABLED", "false").lower() == "true"
except ImportError:
    structure_analyzer = None
    _STRUCTURE_ENABLED = False

try:
    from bot.correlation_guard import correlation_guard
    _CORR_ENABLED = os.getenv("CORR_GUARD_ENABLED", "false").lower() == "true"
except ImportError:
    correlation_guard = None
    _CORR_ENABLED = False
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("DecisionEngine")

# Timeframe de referencia para la guardia de vela cerrada (#7)
_CANDLE_CONFIRM_TF        = os.getenv("CANDLE_CONFIRM_TF", "15m")
_CANDLE_CONFIRM_THRESHOLD = float(os.getenv("CANDLE_CONFIRM_THRESHOLD", "0.80"))
_CANDLE_CONFIRM_ENABLED   = os.getenv("CANDLE_CONFIRM_ENABLED", "true").lower() == "true"


class DecisionEngine:
    """
    Encapsula la lógica de decisión de entrada al mercado.
    Recibe el trader como contexto para acceder a precio, OHLCV y órdenes.
    """

    def __init__(self, symbol: str, position_mgr=None):
        self.symbol = symbol
        self._position_mgr = position_mgr

    def bind_position_manager(self, pm) -> None:
        """Conecta el PositionManager para que reciba entry_mode al abrir."""
        self._position_mgr = pm

    async def evaluate(
        self,
        trader,
        risk,
        global_risk=None,
    ) -> None:
        """
        Evalúa si se debe abrir una posición. Si la decisión es BUY/SELL,
        coloca la orden y actualiza el estado del trader.
        """
        if kill_switch.is_halted(self.symbol):
            logger.debug("[%s] Kill switch activo — skip.", self.symbol)
            return

        # ── #4 Cooldown de reapertura ─────────────────────────────────────────
        if signal_cooldown.is_blocked(self.symbol):
            remaining = signal_cooldown.remaining(self.symbol)
            logger.debug(
                "[%s] Cooldown activo — %.0fs restantes.", self.symbol, remaining
            )
            return

        # ── #7 Confirmación de vela cerrada ────────────────────────────────
        if _CANDLE_CONFIRM_ENABLED:
            if not ws_feed.is_candle_closed(
                self.symbol, _CANDLE_CONFIRM_TF, _CANDLE_CONFIRM_THRESHOLD
            ):
                logger.debug(
                    "[%s] Vela %s sin confirmar (progreso<%.0f%%) — skip.",
                    self.symbol, _CANDLE_CONFIRM_TF,
                    _CANDLE_CONFIRM_THRESHOLD * 100,
                )
                return

        # ── [V4] Market Regime filter ─────────────────────────────────────────
        if _REGIME_ENABLED and market_regime is not None:
            try:
                regime = await market_regime.get_regime()
                if regime == "RED":
                    logger.info("[%s] Market regime RED — skip entrada.", self.symbol)
                    return
                if regime == "YELLOW":
                    logger.debug("[%s] Market regime YELLOW — procediendo con cautela.", self.symbol)
            except Exception as e:
                logger.warning("[%s] market_regime error: %s — skip filtro.", self.symbol, e)

        # ── [V4] Daily Drawdown check ─────────────────────────────────────────
        if _DD_ENABLED and daily_drawdown is not None:
            try:
                if daily_drawdown.is_blocked():
                    logger.info("[%s] Daily drawdown alcanzado — skip entrada.", self.symbol)
                    return
            except Exception as e:
                logger.warning("[%s] daily_drawdown error: %s — skip filtro.", self.symbol, e)

        # ── Verificaciones previas ────────────────────────────────────────────
        if global_risk:
            allowed, reason = await global_risk.can_open()
            if not allowed:
                logger.debug("[%s] GlobalRisk: %s", self.symbol, reason)
                return

        balance = await trader.get_balance()
        if balance is not None and balance < risk.usdc_per_trade:
            logger.warning(
                "[%s] Balance insuficiente (%.2f < %.2f USDC).",
                self.symbol, balance, risk.usdc_per_trade,
            )
            return

        # ── Señal de estrategia ───────────────────────────────────────────────
        try:
            exch     = await trader._get_ccxt()
            decision = await decide(
                exch=exch,
                symbol=self.symbol,
                ai_decide_fn=ai_decide,
                has_open_position=False,
                current_pnl=None,
            )
        except Exception as e:
            logger.error("[%s] decide() error: %s", self.symbol, e)
            return

        action = decision.get("action", "HOLD")
        signal = decision.get("signal")
        if action not in ("BUY", "SELL"):
            return

        # ── [V4] Structure Analyzer — boost de score ──────────────────────────
        if _STRUCTURE_ENABLED and structure_analyzer is not None:
            try:
                struct_score = await structure_analyzer.score(
                    symbol=self.symbol,
                    side="long" if action == "BUY" else "short",
                )
                if struct_score < 0:
                    logger.info(
                        "[%s] StructureAnalyzer score negativo (%d) — skip entrada.",
                        self.symbol, struct_score,
                    )
                    return
                logger.debug("[%s] StructureAnalyzer score: %d", self.symbol, struct_score)
            except Exception as e:
                logger.warning("[%s] structure_analyzer error: %s — skip filtro.", self.symbol, e)

        # ── Parámetros de la orden ────────────────────────────────────────────
        try:
            price = await trader.get_price()
        except Exception as e:
            logger.warning("[%s] No se pudo obtener precio: %s", self.symbol, e)
            return

        if signal:
            entry      = signal.entry or price
            sl         = signal.sl
            tp1        = signal.tp1
            tp2        = signal.tp2
            tp3        = getattr(signal, "tp3", None)
            lev        = signal.suggested_lev or trader.leverage
            entry_mode = getattr(signal, "entry_mode", "") or ""
        else:
            entry      = price
            sl = tp1 = tp2 = tp3 = None
            lev        = trader.leverage
            entry_mode = ""

        lev = min(int(lev), trader.leverage)
        if lev != trader.leverage:
            await trader._set_leverage(lev)

        # ── #8 Sizing dinámico según win-rate histórico del modo ──────────────
        sizing_mult = shadow_mode.sizing_multiplier(entry_mode)
        base_usdc   = risk.usdc_per_trade * sizing_mult
        notional    = base_usdc * lev

        if sizing_mult != 1.0:
            logger.info(
                "[%s] Sizing dinámico: %.2f× (modo=%s) → %.2f USDC base",
                self.symbol, sizing_mult, entry_mode or "?", base_usdc,
            )

        # ── [V4] Kelly Sizer ──────────────────────────────────────────────────
        if _KELLY_ENABLED and kelly_sizer is not None:
            try:
                kelly_mult = kelly_sizer.multiplier(symbol=self.symbol)
                if kelly_mult != 1.0:
                    notional = notional * kelly_mult
                    logger.info(
                        "[%s] Kelly sizing: %.2f× → notional=%.2f USDC",
                        self.symbol, kelly_mult, notional,
                    )
            except Exception as e:
                logger.warning("[%s] kelly_sizer error: %s — usando notional base.", self.symbol, e)

        side = "buy" if action == "BUY" else "sell"

        # ── [V4] Correlation Guard ────────────────────────────────────────────
        if _CORR_ENABLED and correlation_guard is not None:
            try:
                corr_ok, corr_reason = await correlation_guard.check(
                    symbol=self.symbol,
                    side=side,
                )
                if not corr_ok:
                    logger.info("[%s] CorrelationGuard bloqueó: %s", self.symbol, corr_reason)
                    return
            except Exception as e:
                logger.warning("[%s] correlation_guard error: %s — skip filtro.", self.symbol, e)

        # ── pretrade_risk.check() con firma correcta ──────────────────────────
        try:
            ok, reason = await pretrade_risk.check(
                symbol=self.symbol,
                side=side,
                notional=notional,
                price=entry,
                balance=balance,
                sl=sl,
                leverage=lev,
            )
            if not ok:
                logger.debug("[%s] pretrade_risk bloqueó: %s", self.symbol, reason)
                return
        except Exception as e:
            logger.warning("[%s] pretrade_risk.check() error: %s — continuando", self.symbol, e)

        qty = trader._round_qty(notional / entry)
        if qty <= 0:
            logger.warning("[%s] qty <= 0 tras redondeo szDecimals, skip.", self.symbol)
            return

        logger.info(
            "[%s] 📈 Abriendo %s · qty=%s · entry=~%s · sl=%s · tp1=%s · modo=%s | %s",
            self.symbol, action, qty, round(entry, 4),
            round(sl, 4) if sl else "N/A",
            round(tp1, 4) if tp1 else "N/A",
            entry_mode or "?",
            decision.get("reason", ""),
        )

        # ── Ejecutar orden ────────────────────────────────────────────────────
        result = (
            {"status": "ok", "_fill_price": entry}
            if trader.dry_run
            else await trader._place_order(side, qty, sl=sl, tp=tp1)
        )

        if result.get("status") != "ok":
            return

        # Registrar exposición en pretrade_risk tras fill confirmado
        pretrade_risk.confirm_order(self.symbol, notional)

        # [V4] Registrar apertura en correlation_guard
        if _CORR_ENABLED and correlation_guard is not None:
            try:
                correlation_guard.on_open(self.symbol, side)
            except Exception:
                pass

        try:
            fill_price = float(
                result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                .get("filled", {}).get("avgPx") or entry
            )
        except Exception:
            fill_price = entry

        if fill_price and fill_price != entry:
            qty = trader._round_qty(notional / fill_price)
            logger.debug(
                "[%s] Fill real: %.4f (estimado: %.4f) — qty ajustada a %s",
                self.symbol, fill_price, entry, qty,
            )

        # ── Actualizar estado del trader ──────────────────────────────────────
        trader.position       = "long" if action == "BUY" else "short"
        trader.entry_price    = fill_price
        trader.sl             = sl
        trader.tp1            = tp1
        trader.tp2            = tp2
        trader.tp3            = tp3
        trader.tp2_hit        = False
        trader._tp1_be_done   = False
        trader._open_notional = notional
        trader._open_leverage = lev
        trader._protection_ok = False
        trader.trade_count   += 1

        # Comunicar entry_mode al PositionManager para cooldown correcto al cerrar
        if self._position_mgr is not None:
            self._position_mgr.set_entry_mode(entry_mode)

        save_position(self.symbol, {
            "side":        trader.position,
            "entry":       trader.entry_price,
            "sl":          trader.sl,
            "tp1":         trader.tp1,
            "tp2":         trader.tp2,
            "tp3":         trader.tp3,
            "tp2_hit":     trader.tp2_hit,
            "usdc_amount": notional,
            "leverage":    lev,
            "entry_mode":  entry_mode,
        })

        # ── #8 Registrar señal en shadow_mode con entry_mode ─────────────────
        shadow_mode.record_signal(
            symbol=self.symbol,
            side=trader.position,
            price=fill_price,
            sl=sl,
            tp=tp3 or tp1,
            entry_mode=entry_mode,
        )
        shadow_mode.record_real_open(self.symbol, fill_price)

        if global_risk:
            await global_risk.register_open()

        await notify_open(
            symbol=self.symbol,
            side=trader.position,
            entry=trader.entry_price,
            sl=trader.sl,
            tp1=trader.tp1,
            tp2=trader.tp2,
            size_usdc=notional,
            leverage=lev,
            signal_block=decision.get("signal_block", ""),
            ai_used=decision.get("ai_used", False),
            ai_confidence=decision.get("ai_confidence", 0),
        )
