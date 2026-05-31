"""
execution_engine.py — Motor de ejecución con TP/SL reales en Hyperliquid.

Cambios respecto a la versión anterior:
  - Usa HLClient (SDK oficial) en lugar de signing manual.
  - Al abrir posición: coloca entrada + trigger TP + trigger SL en bulk.
  - Al cerrar: cancela los trigger orders abiertos antes de la orden de cierre.
  - Si el bot se reinicia, el TP/SL sigue activo en el exchange.

Flujo de apertura:
  1. Guardia dura: si trade_side=="open" y falta sl o tp → abortar inmediatamente.
  2. Calcular arrival_price (mid del orderbook o last)
  3. Intentar limit agresiva si spread <= umbral y depth suficiente
  4. Si llena: colocar TP trigger + SL trigger en bulk
  5. Si no llena en timeout: cancelar → fallback market + TP/SL bulk
     (FIX race condition: se usa `entry_ok` propio del fallback, no el result de la limit)
  6. Registrar telemetría

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          default 4
  EE_MAX_SPREAD_BPS_LIMIT     default 15
  EE_LIMIT_OFFSET_BPS         default 3
  EE_MAX_SLIPPAGE_ALERT_BPS   default 30
  EE_TP_AS_LIMIT              default true  (False = TP como market)
  EE_TP_LIMIT_BUFFER_BPS      default 50    (buffer sobre/bajo trigger px para limit_px del TP)
  EE_BULK_RETRY_ATTEMPTS      default 3
  EE_BULK_RETRY_BASE_DELAY_S  default 1.0
  EE_MARKET_429_RETRIES       default 3
  EE_MARKET_429_DELAY_S       default 2.0
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bot.trader import FuturesTrader

from bot.core.hl_client import HLClient

logger = logging.getLogger("ExecutionEngine")

_AGENT_NOT_FOUND_SUBSTR = "does not exist"
_RATE_LIMIT_SUBSTRS = ("429", "too many requests", "rate limit", "ratelimit")


def _e(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


@dataclass
class TradeRecord:
    symbol:             str
    side:               str
    qty:                float
    arrival_price:      float
    fill_price:         float = 0.0
    slippage_bps:       float = 0.0
    fill_latency_ms:    float = 0.0
    partial_fill_ratio: float = 1.0
    order_type_used:    str   = "market"
    cancel_reason:      str   = ""
    success:            bool  = False
    _t0:                float = field(default=0.0, repr=False)


class ExecutionEngine:
    """
    Motor de ejecución:
      - Usa el SDK oficial de Hyperliquid (HLClient)
      - Coloca TP y SL como trigger orders reales en el exchange
      - TP/SL sobreviven reinicios del bot
      - NUNCA abre una posición sin SL y TP válidos
    """

    def __init__(self) -> None:
        self.limit_timeout_s:        float = _e("EE_LIMIT_TIMEOUT_S",         4.0)
        self.max_spread_bps_limit:   float = _e("EE_MAX_SPREAD_BPS_LIMIT",   15.0)
        self.limit_offset_bps:       float = _e("EE_LIMIT_OFFSET_BPS",        3.0)
        self.max_slippage_alert_bps: float = _e("EE_MAX_SLIPPAGE_ALERT_BPS", 30.0)
        self.tp_as_limit:            bool  = os.getenv("EE_TP_AS_LIMIT", "true").lower() == "true"
        self.tp_limit_buffer_bps:    float = _e("EE_TP_LIMIT_BUFFER_BPS",    50.0)
        self.bulk_retry_attempts:    int   = int(_e("EE_BULK_RETRY_ATTEMPTS", 3))
        self.bulk_retry_base_delay:  float = _e("EE_BULK_RETRY_BASE_DELAY_S", 1.0)
        self.market_429_retries:     int   = int(_e("EE_MARKET_429_RETRIES",  3))
        self.market_429_delay:       float = _e("EE_MARKET_429_DELAY_S",      2.0)
        self._records: dict[str, list[TradeRecord]] = defaultdict(list)
        self._hl_clients: dict[str, HLClient] = {}

    def _get_client(self, symbol: str) -> HLClient:
        """Devuelve o crea el HLClient para el símbolo."""
        if symbol not in self._hl_clients:
            self._hl_clients[symbol] = HLClient(symbol)
        return self._hl_clients[symbol]

    # ── UTILIDADES ──────────────────────────────────────────────────────

    @staticmethod
    def _round_qty(qty: float, sz_decimals: int) -> float:
        """
        Redondea qty al número de decimales permitidos por HL para este coin.

        FIX B — 'Order has invalid size':
          Coins de precio bajo (DOGE, XRP, SHIB...) tienen szDecimals=0,
          lo que significa que HL sólo acepta cantidades enteras.
          Usar math.floor (truncar hacia abajo) para no exceder el balance.
        """
        if sz_decimals <= 0:
            return float(math.floor(qty))
        factor = 10 ** sz_decimals
        return float(math.floor(qty * factor) / factor)

    async def _place_market_with_retry(
        self,
        client: HLClient,
        is_buy: bool,
        qty: float,
        reduce_only: bool,
        ref_price: float,
        sym: str,
    ) -> dict:
        """
        Llama a place_market con retry ante errores 429.
        Lanza la excepción si todos los intentos fallan.
        """
        last_exc: Exception | None = None
        for attempt in range(self.market_429_retries):
            if attempt > 0:
                delay = self.market_429_delay * (2 ** (attempt - 1))
                logger.warning(
                    "[%s] place_market 429 (intento %d/%d) — reintentando en %.1fs",
                    sym, attempt + 1, self.market_429_retries, delay,
                )
                await asyncio.sleep(delay)
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, client.place_market, is_buy, qty, reduce_only, ref_price
                )
                return result
            except Exception as e:
                last_exc = e
                err_str = str(e).lower()
                if any(s in err_str for s in _RATE_LIMIT_SUBSTRS):
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return {"status": "error", "response": "unknown"}

    # ── EXECUTE PRINCIPAL ───────────────────────────────────────────────

    async def execute(
        self,
        trader:        "FuturesTrader",
        side:          str,
        qty:           float,
        arrival_price: float,
        ask:           Optional[float] = None,
        bid:           Optional[float] = None,
        trade_side:    str = "open",
        reduce_only:   bool = False,
        sl:            Optional[float] = None,
        tp:            Optional[float] = None,
    ) -> dict:
        """
        Ejecuta una orden con TP/SL reales en el exchange.

        GUARDIA DURA (apertura):
          Si trade_side=="open" y sl o tp son None/0 → rechaza inmediatamente.
          Ninguna posición se abre sin ambos niveles de protección.

        Si trade_side=="open" y sl+tp válidos:
          1. Abre la posición (limit o market)
          2. Coloca trigger TP + trigger SL en bulk
          FIX race condition: `entry_ok` se actualiza con el resultado REAL
          de la orden ejecutada (limit fill o market fallback), nunca con el
          result de la limit "resting" que todavía no está filled.

        Si reduce_only=True (cierre manual):
          1. Cancela trigger orders abiertos del coin
          2. Ejecuta cierre
        """
        sym    = trader.symbol
        client = self._get_client(sym)
        rec    = TradeRecord(symbol=sym, side=side, qty=qty, arrival_price=arrival_price)
        rec._t0 = time.monotonic()

        is_buy = side in ("buy", "long")

        # ── GUARDIA DURA: NUNCA abrir sin SL y TP ─────────────────────
        if trade_side == "open" and not reduce_only:
            missing = []
            if not sl or sl <= 0:
                missing.append("SL")
            if not tp or tp <= 0:
                missing.append("TP")
            if missing:
                msg = f"Apertura bloqueada — faltan: {', '.join(missing)}. No se abre ninguna posición sin SL y TP."
                logger.error("[%s] 🚫 %s", sym, msg)
                self._finalize_rec(rec, {"status": "error", "response": msg}, side, arrival_price)
                return {"status": "error", "response": msg}

        # FIX B: redondear qty a los decimales permitidos por HL para este coin
        sz_dec = client.get_sz_decimals()
        qty    = self._round_qty(qty, sz_dec)
        rec.qty = qty

        if qty <= 0:
            logger.error("[%s] qty redondeada a 0 (sz_decimals=%d, qty_raw=%.8f) — abortando", sym, sz_dec, rec.qty)
            return {"status": "error", "response": "qty rounded to zero"}

        # Si es cierre manual → cancelar TP/SL previos del exchange
        if reduce_only:
            try:
                cancelled = client.cancel_all_open_tpsl()
                if cancelled:
                    logger.info("[%s] 🗑️ %d trigger order(s) canceladas antes del cierre.", sym, len(cancelled))
            except Exception as e:
                logger.warning("[%s] No se pudieron cancelar trigger orders: %s", sym, e)

        spread_bps = self._calc_spread_bps(ask, bid, arrival_price)
        use_limit  = (
            ask is not None
            and bid is not None
            and spread_bps <= self.max_spread_bps_limit
        )

        # `entry_ok` indica si la orden de entrada fue confirmada — es la
        # única variable que controla si se colocan los TP/SL.
        # FIX race condition: NO se usa result.get("status") directamente porque
        # cuando la limit queda en "resting" su status es "ok" pero NO está filled.
        # Solo ponemos entry_ok=True cuando sabemos con certeza que hay posición abierta.
        entry_ok = False
        result   = {"status": "error", "response": "not executed"}

        if use_limit:
            limit_price = self._calc_limit_price(side, arrival_price, ask, bid)
            limit_result, filled = await self._try_limit_sdk(client, is_buy, qty, limit_price, rec)

            if filled:
                # Limit se ejecutó completamente
                result               = limit_result
                entry_ok             = True
                rec.order_type_used  = "limit"
                rec.fill_price       = limit_price
            else:
                if _AGENT_NOT_FOUND_SUBSTR in rec.cancel_reason:
                    self._log_agent_error(sym, trader)
                    rec.order_type_used = "market"
                    rec.fill_price      = arrival_price
                    self._finalize_rec(rec, limit_result, side, arrival_price)
                    return limit_result

                logger.info("[%s] ⚡ Limit sin fill → fallback market", sym)
                # FIX: usamos el result del MARKET (no del limit resting)
                # para determinar entry_ok correctamente.
                market_result = await self._place_market_with_retry(
                    client, is_buy, qty, reduce_only, arrival_price, sym
                )
                result = market_result
                if result.get("status") == "ok":
                    entry_ok = True
                else:
                    logger.error(
                        "[%s] ❌ Fallback market falló: %s",
                        sym, result.get("response", ""),
                    )
                rec.order_type_used = "market"
                rec.fill_price      = arrival_price
        else:
            reason = (
                f"spread {spread_bps:.1f}bps > {self.max_spread_bps_limit:.0f}bps"
                if ask is not None else "sin datos de orderbook"
            )
            logger.debug("[%s] Market directo (%s)", sym, reason)
            result = await self._place_market_with_retry(
                client, is_buy, qty, reduce_only, arrival_price, sym
            )
            if result.get("status") == "ok":
                entry_ok = True
            else:
                logger.error(
                    "[%s] ❌ Market order falló: %s",
                    sym, result.get("response", ""),
                )
            rec.order_type_used = "market"
            rec.fill_price      = arrival_price
            rec.cancel_reason   = reason

        # ── Colocar TP/SL reales SÓLO si la apertura fue confirmada ─────
        # Usa entry_ok (no result["status"]) para evitar la race condition
        # donde la limit "resting" devuelve status=ok sin estar filled.
        if (
            entry_ok
            and not reduce_only
            and trade_side == "open"
        ):
            # sl y tp ya fueron validados por la guardia dura arriba
            await self._place_tpsl_bulk(client, is_buy, qty, sl, tp, sym)

        self._finalize_rec(rec, result, side, arrival_price)
        return result

    # ── TP / SL TRIGGER ORDERS ──────────────────────────────────────────────

    async def _place_tpsl_bulk(
        self,
        client: HLClient,
        is_buy: bool,
        qty: float,
        sl: Optional[float],
        tp: Optional[float],
        sym: str,
    ) -> None:
        """
        Coloca TP y SL como trigger orders reales en el exchange.

        FIX: todos los precios (triggerPx y limit_px) se redondean con
        client.round_px() para garantizar que sean válidos para CUALQUIER
        coin — evita 'Order has invalid price' en HYPE, DOGE, XRP, etc.

        Regla de limit_px para TP:
          • TP de LONG  → close_side=SELL → limit_px = triggerPx * (1 - buffer)  ✓
          • TP de SHORT → close_side=BUY  → limit_px = triggerPx * (1 + buffer)  ✓
        """
        close_side = not is_buy
        orders_to_place = []

        if tp is not None:
            tp_trigger = client.round_px(float(tp))

            if self.tp_as_limit:
                buffer_multiplier = self.tp_limit_buffer_bps / 10_000
                if close_side:  # cerrar short: compramos, aceptamos pagar más
                    tp_limit_px = client.round_px(tp_trigger * (1 + buffer_multiplier))
                else:           # cerrar long:  vendemos, aceptamos recibir menos
                    tp_limit_px = client.round_px(tp_trigger * (1 - buffer_multiplier))
            else:
                tp_limit_px = None

            orders_to_place.append({
                "coin":       client.coin,
                "is_buy":     close_side,
                "sz":         qty,
                "limit_px":   tp_limit_px,
                "order_type": {
                    "trigger": {
                        "triggerPx": tp_trigger,
                        "isMarket":  not self.tp_as_limit,
                        "tpsl":      "tp",
                    }
                },
                "reduce_only": True,
            })

        if sl is not None:
            sl_px = client.round_px(float(sl))
            orders_to_place.append({
                "coin":       client.coin,
                "is_buy":     close_side,
                "sz":         qty,
                "limit_px":   sl_px,
                "order_type": {
                    "trigger": {
                        "triggerPx": sl_px,
                        "isMarket":  True,
                        "tpsl":      "sl",
                    }
                },
                "reduce_only": True,
            })

        if not orders_to_place:
            return

        last_error: Exception | None = None
        for attempt in range(self.bulk_retry_attempts):
            if attempt > 0:
                delay = self.bulk_retry_base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "[%s] place_bulk intento %d/%d — esperando %.1fs (rate-limit o error transitorio)",
                    sym, attempt + 1, self.bulk_retry_attempts, delay,
                )
                await asyncio.sleep(delay)
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, client.place_bulk, orders_to_place
                )
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                all_ok = True
                for i, st in enumerate(statuses):
                    label = "TP" if (i == 0 and tp is not None) else "SL"
                    if "error" in st:
                        err_msg = st["error"]
                        if any(s in err_msg.lower() for s in _RATE_LIMIT_SUBSTRS):
                            logger.warning("[%s] Trigger %s rate-limited: %s", sym, label, err_msg)
                            all_ok = False
                            last_error = RuntimeError(err_msg)
                        else:
                            logger.error("[%s] ❌ Trigger %s fallido: %s", sym, label, err_msg)
                    else:
                        logger.info("[%s] ✅ Trigger %s colocado en exchange (sobrevive reinicios)", sym, label)
                if all_ok:
                    return
            except Exception as e:
                last_error = e
                err_str = str(e).lower()
                if any(s in err_str for s in _RATE_LIMIT_SUBSTRS):
                    logger.warning("[%s] place_bulk 429/rate-limit: %s", sym, e)
                    continue
                logger.error("[%s] Error colocando trigger orders TP/SL: %s", sym, e)
                return

        if last_error is not None:
            logger.error(
                "[%s] ❌ place_bulk falló tras %d intentos. Último error: %s",
                sym, self.bulk_retry_attempts, last_error,
            )

    # ── LIMIT INTERNO + TELEMETRÍA ─────────────────────────────────────────

    async def _try_limit_sdk(
        self,
        client: HLClient,
        is_buy: bool,
        qty: float,
        price: float,
        rec: TradeRecord,
    ) -> tuple[dict, bool]:
        """Intenta orden límite con timeout y devuelve (result, filled)."""
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, client.place_limit, is_buy, qty, price, False
            )
        except Exception as e:
            rec.cancel_reason = str(e)
            return {"status": "error", "response": str(e)}, False

        if result.get("status") != "ok":
            err_str = str(result.get("response", ""))
            rec.cancel_reason = f"limit_rejected:{err_str}"
            if _AGENT_NOT_FOUND_SUBSTR in err_str:
                rec.cancel_reason = f"agent_not_found:{err_str}"
            return result, False

        try:
            order_id = result["response"]["data"]["statuses"][0].get("resting", {}).get("oid")
        except (KeyError, IndexError, TypeError):
            order_id = None

        deadline = time.monotonic() + self.limit_timeout_s
        filled   = False

        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            if order_id:
                try:
                    open_orders = await asyncio.get_event_loop().run_in_executor(
                        None, client.get_open_orders
                    )
                    still_open = any(o.get("oid") == order_id for o in open_orders)
                    if not still_open:
                        filled = True
                        break
                except Exception:
                    pass

        if not filled and order_id:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, client.cancel_order, order_id
                )
            except Exception as e:
                logger.warning("[%s] Error cancelando limit: %s", rec.symbol, e)
            if not rec.cancel_reason:
                rec.cancel_reason = "timeout"

        return result, filled

    def _log_agent_error(self, sym: str, trader) -> None:
        logger.error(
            "[%s] HL rechazó con 'does not exist'. "
            "Verifica en app.hyperliquid.xyz → Settings → API que el agente esté aprobado. "
            "master=%s | agente=%s",
            sym,
            getattr(trader, "_master_addr", "N/A"),
            getattr(trader, "_agent_addr",  "N/A"),
        )

    def _finalize_rec(self, rec: TradeRecord, result: dict, side: str, arrival_price: float) -> None:
        rec.fill_latency_ms = (time.monotonic() - rec._t0) * 1000
        rec.success         = result.get("status") == "ok"

        if rec.success and arrival_price > 0:
            if side in ("buy", "long"):
                rec.slippage_bps = (rec.fill_price - arrival_price) / arrival_price * 10_000
            else:
                rec.slippage_bps = (arrival_price - rec.fill_price) / arrival_price * 10_000

        self._records[rec.symbol].append(rec)

        log_msg = (
            f"[{rec.symbol}] 📊 Exec: type={rec.order_type_used} side={side} qty={rec.qty} "
            f"arrival={arrival_price:.4f} fill={rec.fill_price:.4f} "
            f"slippage={rec.slippage_bps:+.1f}bps latency={rec.fill_latency_ms:.0f}ms"
        )
        if rec.slippage_bps > self.max_slippage_alert_bps:
            logger.warning("⚠️ SLIPPAGE ALTO — %s", log_msg)
        else:
            logger.info(log_msg)

    def get_stats(self, symbol: str) -> dict:
        recs = [r for r in self._records.get(symbol, []) if r.success]
        if not recs:
            return {"symbol": symbol, "trades": 0}

        def _avg(lst):
            return sum(lst) / len(lst) if lst else 0.0

        buys   = [r for r in recs if r.side in ("buy",  "long")]
        sells  = [r for r in recs if r.side in ("sell", "short")]
        limits = [r for r in recs if r.order_type_used == "limit"]
        mkts   = [r for r in recs if r.order_type_used == "market"]

        return {
            "symbol":            symbol,
            "trades":            len(recs),
            "avg_slippage_bps":  _avg([r.slippage_bps    for r in recs]),
            "avg_latency_ms":    _avg([r.fill_latency_ms for r in recs]),
            "buy_slippage_bps":  _avg([r.slippage_bps    for r in buys]),
            "sell_slippage_bps": _avg([r.slippage_bps    for r in sells]),
            "limit_fill_rate":   len(limits) / len(recs),
            "market_fill_rate":  len(mkts)   / len(recs),
        }

    def get_all_stats(self) -> list[dict]:
        return [self.get_stats(sym) for sym in self._records]

    def _calc_spread_bps(self, ask, bid, price):
        if ask is None or bid is None or price <= 0:
            return 9999.0
        return (ask - bid) / price * 10_000

    def _calc_limit_price(self, side, arrival, ask, bid):
        mid    = (ask + bid) / 2
        offset = arrival * self.limit_offset_bps / 10_000
        if side in ("buy", "long"):
            return round(mid + offset, 6)
        return round(mid - offset, 6)


execution_engine = ExecutionEngine()
