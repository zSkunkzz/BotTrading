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

FIX v4.1:
  - kelly_sizer: la función real es kelly_multiplier(entry_mode, rr), no .multiplier(symbol=)
  - market_regime: ahora tiene singleton MarketRegimeSingleton con .refresh()/.regime()/.btc_trend()
  - REGIME_FILTER env var alineada con market_regime (MARKET_REGIME_GATE también funciona)

FIX v4.2:
  - BUG A: on_position_closed llamaba signal_cooldown.on_trade_result() que no existe.
           Método real: mark_closed(symbol, reason, entry_mode).
  - BUG B: confirm_order() recibía `notional` (base_usdc × lev) en vez de `margin` (base_usdc / lev).
           Esto inflaba el contador de open_margin × leverage, bloqueando nuevas entradas incorrectamente.
           Al reiniciarse el bot (Railway), el pretrade_risk se reseteaba a 0 y el bot abría otra
           posición sin detectar que ya existía una en el exchange → acumulación de posiciones duplicadas.
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

# ── V4 módulos ─────────────────────────────────────────────────────────────────────────
try:
    from bot.market_regime import market_regime   # singleton MarketRegimeSingleton
    _REGIME_ENABLED = (
        os.getenv("REGIME_FILTER", "false").lower() == "true"
        or os.getenv("MARKET_REGIME_GATE", "false").lower() == "true"
    )
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
    # FIX: la función real es kelly_multiplier(entry_mode, rr), no un método .multiplier(symbol=)
    from bot.kelly_sizer import kelly_multiplier as _kelly_multiplier
    _KELLY_ENABLED = os.getenv("KELLY_ENABLED", "true").lower() != "false"
except ImportError:
    _kelly_multiplier = None
    _KELLY_ENABLED = False

try:
    from bot.structure_analyzer import analyze_structure
    _STRUCTURE_ENABLED = os.getenv("STRUCTURE_ENABLED", "false").lower() == "true"
except ImportError:
    analyze_structure = None
    _STRUCTURE_ENABLED = False

try:
    from bot.correlation_guard import check_correlation, size_penalty_btc
    _CORR_ENABLED = os.getenv("CORR_GUARD_ENABLED", "false").lower() == "true"
except ImportError:
    check_correlation = None
    size_penalty_btc = None
    _CORR_ENABLED = False
# ──────────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("DecisionEngine")

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

    async def on_position_closed(
        self,
        symbol: str,
        margin: float | None,
        reason: str,
        entry_mode: str,
    ) -> None:
        """
        Callback invocado desde PositionManager al cerrar cualquier posición.
        Actualiza cooldown dinámico y registra resultado en shadow_mode.

        FIX v4.1: convertido a async def para que trader.py pueda llamarlo con await
        sin lanzar 'object NoneType can't be used in await expression'.

        FIX v4.2 BUG A: signal_cooldown.on_trade_result() no existe.
        El método real es mark_closed(symbol, reason, entry_mode).
        """
        is_win = reason in ("TP1", "TP2", "TP3")
        logger.debug(
            "[%s] on_position_closed: reason=%s mode=%s win=%s",
            symbol, reason, entry_mode, is_win,
        )
        try:
            shadow_mode.record_real_close(symbol, won=is_win, entry_mode=entry_mode)
        except Exception as e:
            logger.debug("[%s] shadow_mode.record_real_close error: %s", symbol, e)
        try:
            # FIX v4.2 BUG A: usar mark_closed(symbol, reason, entry_mode)
            # en vez del inexistente on_trade_result(symbol, won, entry_mode)
            signal_cooldown.mark_closed(
                symbol=symbol,
                reason=reason,                        # "SL" | "TP1" | "TP2" | "TP3" | "TIMEOUT"
                entry_mode=entry_mode or "NORMAL",
            )
        except Exception as e:
            logger.debug("[%s] signal_cooldown.mark_closed error: %s", symbol, e)

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

        # ── #4 Cooldown de reapertura ───────────────────────────────────────────────
        if signal_cooldown.is_blocked(self.symbol):
            remaining = signal_cooldown.remaining(self.symbol)
            logger.debug(
                "[%s] Cooldown activo — %.0fs restantes.", self.symbol, remaining
            )
            return

        # ── #7 Confirmación de vela cerrada ────────────────────────────────────
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

        # ── [V4] Market Regime filter ─────────────────────────────────────────────
        # market_regime es ahora MarketRegimeSingleton con .refresh() y .regime() reales.
        if _REGIME_ENABLED and market_regime is not None:
            try:
                exch_for_regime = await trader._get_ccxt()
                await market_regime.refresh(exch=exch_for_regime)
                regime = market_regime.regime()   # "GREEN" | "YELLOW" | "RED"
                if regime == "RED":
                    logger.info("[%s] Market regime RED — skip entrada.", self.symbol)
                    return
                if regime == "YELLOW":
                    logger.debug("[%s] Market regime YELLOW — procediendo con cautela.", self.symbol)
            except Exception as e:
                logger.warning("[%s] market_regime error: %s — skip filtro.", self.symbol, e)

        # ── [V4] Daily Drawdown check ─────────────────────────────────────────────
        if _DD_ENABLED and daily_drawdown is not None:
            try:
                if daily_drawdown.is_blocked():
                    logger.info("[%s] Daily drawdown alcanzado — skip entrada.", self.symbol)
                    return
            except Exception as e:
                logger.warning("[%s] daily_drawdown error: %s — skip filtro.", self.symbol, e)

        # ── Verificaciones previas ─────────────────────────────────────────────────
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

        # ── Señal de estrategia ───────────────────────────────────────────────────────
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

        # ── [V4] Structure Analyzer ────────────────────────────────────────────────
        if _STRUCTURE_ENABLED and analyze_structure is not None:
            try:
                direction_int = 1 if action == "BUY" else -1
                df_struct = ws_feed.get_ohlcv(self.symbol, "1h")
                if df_struct is not None and not df_struct.empty:
                    struct_result = analyze_structure(df_struct, direction=direction_int)
                    struct_score = struct_result.get("score", 0)
                    if struct_score < 0:
                        logger.info(
                            "[%s] StructureAnalyzer score negativo (%d) — skip entrada.",
                            self.symbol, struct_score,
                        )
                        return
                    logger.debug("[%s] StructureAnalyzer score: %d", self.symbol, struct_score)
                else:
                    logger.debug("[%s] StructureAnalyzer sin datos OHLCV 1h — skip filtro.", self.symbol)
            except Exception as e:
                logger.warning("[%s] structure_analyzer error: %s — skip filtro.", self.symbol, e)

        # ── Parámetros de la orden ──────────────────────────────────────────────────
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

        # ── #8 Sizing dinámico según win-rate histórico del modo ─────────────────
        sizing_mult = shadow_mode.sizing_multiplier(entry_mode)
        base_usdc   = risk.usdc_per_trade * sizing_mult
        notional    = base_usdc * lev

        if sizing_mult != 1.0:
            logger.info(
                "[%s] Sizing dinámico: %.2f× (modo=%s) → %.2f USDC base",
                self.symbol, sizing_mult, entry_mode or "?", base_usdc,
            )

        # ── [V4] Kelly Sizer ─────────────────────────────────────────────────────
        # FIX: la función es kelly_multiplier(entry_mode, rr), NO .multiplier(symbol=)
        # rr se calcula como (tp1 - entry) / (entry - sl) para LONG
        if _KELLY_ENABLED and _kelly_multiplier is not None and sl and tp1 and entry:
            try:
                sl_dist = abs(entry - sl)
                tp_dist = abs(tp1 - entry)
                rr = tp_dist / sl_dist if sl_dist > 0 else 1.5
                kelly_mult = _kelly_multiplier(entry_mode=entry_mode, rr=rr)
                if kelly_mult != 1.0:
                    notional = notional * kelly_mult
                    logger.info(
                        "[%s] Kelly sizing: rr=%.2f → %.2f× → notional=%.2f USDC",
                        self.symbol, rr, kelly_mult, notional,
                    )
            except Exception as e:
                logger.warning("[%s] kelly_sizer error: %s — usando notional base.", self.symbol, e)

        side = "buy" if action == "BUY" else "sell"

        # ── [V4] Correlation Guard ──────────────────────────────────────────────
        if _CORR_ENABLED and check_correlation is not None:
            try:
                proposed_dir = "LONG" if action == "BUY" else "SHORT"
                open_pos = {}
                if self._position_mgr is not None and hasattr(self._position_mgr, "get_all_positions"):
                    open_pos = self._position_mgr.get_all_positions() or {}
                elif hasattr(trader, "_all_positions"):
                    open_pos = trader._all_positions or {}
                corr_ok, corr_reason = check_correlation(proposed_dir, open_pos)
                if not corr_ok:
                    logger.info("[%s] CorrelationGuard bloqueado: %s", self.symbol, corr_reason)
                    return
                if size_penalty_btc is not None and market_regime is not None:
                    btc_trend = market_regime.btc_trend()
                    size_mult = size_penalty_btc(proposed_dir, btc_trend)
                    if size_mult != 1.0:
                        notional = notional * size_mult
                        logger.info(
                            "[%s] BTC trend penaliza size %.0f%% → notional=%.2f USDC",
                            self.symbol, size_mult * 100, notional,
                        )
            except Exception as e:
                logger.warning("[%s] correlation_guard error: %s — skip filtro.", self.symbol, e)

        # ── pretrade_risk.check() ───────────────────────────────────────────────────
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
                logger.debug("[%s] pretrade_risk bloqueado: %s", self.symbol, reason)
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

        # ── Ejecutar orden ───────────────────────────────────────────────────────────────
        result = (
            {"status": "ok", "_fill_price": entry}
            if trader.dry_run
            else await trader._place_order(side, qty, sl=sl, tp=tp1)
        )

        if result.get("status") != "ok":
            return

        # FIX v4.2 BUG B: confirm_order debe recibir MARGIN (base_usdc), no notional.
        # notional = base_usdc × lev → pasar notional infla el contador × leverage,
        # lo que hace que pretrade_risk crea que hay mucho más margen abierto del real.
        # Tras un restart de Railway el estado se resetea a 0 y el bot abre otra posición
        # sin detectar la existente en el exchange → posiciones duplicadas acumuladas.
        margin_used = notional / lev if lev > 0 else notional
        pretrade_risk.confirm_order(self.symbol, margin_used)

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

        # ── Actualizar estado del trader ──────────────────────────────────────────
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
        trader._open_margin   = margin_used
        trader._protection_ok = False
        trader.trade_count   += 1

        if self._position_mgr is not None:
            self._position_mgr.set_entry_mode(entry_mode)
            # Inyectar referencia de este DecisionEngine en el PositionManager
            if hasattr(self._position_mgr, "bind_decision_engine"):
                self._position_mgr.bind_decision_engine(self)

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
