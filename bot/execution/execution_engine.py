"""
execution_engine.py — Motor de ejecución con TP/SL reales en Hyperliquid.

Cambios respecto a la versión anterior:
  - Usa HLClient (SDK oficial) en lugar de signing manual.
  - Al abrir posición: coloca entrada + trigger TP + trigger SL en bulk.
  - Al cerrar: cancela los trigger orders abiertos antes de la orden de cierre.
  - Si el bot se reinicia, el TP/SL sigue activo en el exchange.

Flujo de apertura:
  1. Calcular arrival_price (mid del orderbook o last)
  2. Intentar limit agresiva si spread <= umbral y depth suficiente
  3. Si llena: colocar TP trigger + SL trigger en bulk
  4. Si no llena en timeout: cancelar → fallback market + TP/SL bulk
  5. Registrar telemetría

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          default 4
  EE_MAX_SPREAD_BPS_LIMIT     default 15
  EE_LIMIT_OFFSET_BPS         default 3
  EE_MAX_SLIPPAGE_ALERT_BPS   default 30
  EE_TP_AS_LIMIT              default true  (False = TP como market)
"""
from __future__ import annotations

import asyncio
import logging
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
    """

    def __init__(self) -> None:
        self.limit_timeout_s:        float = _e("EE_LIMIT_TIMEOUT_S",         4.0)
        self.max_spread_bps_limit:   float = _e("EE_MAX_SPREAD_BPS_LIMIT",   15.0)
        self.limit_offset_bps:       float = _e("EE_LIMIT_OFFSET_BPS",        3.0)
        self.max_slippage_alert_bps: float = _e("EE_MAX_SLIPPAGE_ALERT_BPS", 30.0)
        self.tp_as_limit:            bool  = os.getenv("EE_TP_AS_LIMIT", "true").lower() == "true"
        self._records: dict[str, list[TradeRecord]] = defaultdict(list)
        self._hl_clients: dict[str, HLClient] = {}

    def _get_client(self, symbol: str) -> HLClient:
        """Devuelve o crea el HLClient para el símbolo."""
        if symbol not in self._hl_clients:
            self._hl_clients[symbol] = HLClient(symbol)
        return self._hl_clients[symbol]

    # ── EXECUTE PRINCIPAL ─────────────────────────────────────────────────────

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

        Si trade_side=="open" y se proporcionan sl/tp:
          1. Abre la posición (limit o market)
          2. Coloca trigger TP + trigger SL en bulk

        Si reduce_only=True (cierre manual):
          1. Cancela trigger orders abiertos del coin
          2. Ejecuta cierre
        """
        sym    = trader.symbol
        client = self._get_client(sym)
        rec    = TradeRecord(symbol=sym, side=side, qty=qty, arrival_price=arrival_price)
        rec._t0 = time.monotonic()

        is_buy = side in ("buy", "long")

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

        if use_limit:
            limit_price = self._calc_limit_price(side, arrival_price, ask, bid)
            result, filled = await self._try_limit_sdk(client, is_buy, qty, limit_price, rec)

            if filled:
                rec.order_type_used = "limit"
                rec.fill_price      = limit_price
            else:
                if _AGENT_NOT_FOUND_SUBSTR in rec.cancel_reason:
                    self._log_agent_error(sym, trader)
                    rec.order_type_used = "market"
                    rec.fill_price      = arrival_price
                    self._finalize_rec(rec, result, side, arrival_price)
                    return result
                logger.info("[%s] ⚡ Limit sin fill → fallback market", sym)
                result = await asyncio.get_event_loop().run_in_executor(
                    None, client.place_market, is_buy, qty, reduce_only
                )
                rec.order_type_used = "market"
                rec.fill_price      = arrival_price
        else:
            reason = (
                f"spread {spread_bps:.1f}bps > {self.max_spread_bps_limit:.0f}bps"
                if ask is not None else "sin datos de orderbook"
            )
            logger.debug("[%s] Market directo (%s)", sym, reason)
            result = await asyncio.get_event_loop().run_in_executor(
                None, client.place_market, is_buy, qty, reduce_only
            )
            rec.order_type_used = "market"
            rec.fill_price      = arrival_price
            rec.cancel_reason   = reason

        # ── Colocar TP/SL reales si la apertura fue exitosa ───────────────────
        if (
            result.get("status") == "ok"
            and not reduce_only
            and trade_side == "open"
            and (sl is not None or tp is not None)
        ):
            await self._place_tpsl_bulk(client, is_buy, qty, sl, tp, sym)

        self._finalize_rec(rec, result, side, arrival_price)
        return result

    # ── TP / SL TRIGGER ORDERS ────────────────────────────────────────────────

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
        La dirección de cierre es la opuesta a la posición:
          - Long (is_buy=True)  → cierre = vender (is_buy=False)
          - Short (is_buy=False) → cierre = comprar (is_buy=True)
        """
        close_side = not is_buy
        orders_to_place = []

        if tp is not None:
            orders_to_place.append({
                "coin":       client.coin,
                "is_buy":     close_side,
                "sz":         qty,
                "limit_px":   tp if self.tp_as_limit else None,
                "order_type": {
                    "trigger": {
                        "triggerPx": tp,
                        "isMarket":  not self.tp_as_limit,
                        "tpsl":      "tp",
                    }
                },
                "reduce_only": True,
            })

        if sl is not None:
            orders_to_place.append({
                "coin":       client.coin,
                "is_buy":     close_side,
                "sz":         qty,
                "limit_px":   sl,
                "order_type": {
                    "trigger": {
                        "triggerPx": sl,
                        "isMarket":  True,
                        "tpsl":      "sl",
                    }
                },
                "reduce_only": True,
            })

        if not orders_to_place:
            return

        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, client.place_bulk, orders_to_place
            )
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            for i, st in enumerate(statuses):
                label = "TP" if i == 0 and tp else "SL"
                if "error" in st:
                    logger.error("[%s] ❌ Trigger %s fallido: %s", sym, label, st["error"])
                else:
                    logger.info("[%s] ✅ Trigger %s colocado en exchange (sobrevive reinicios)", sym, label)
        except Exception as e:
            logger.error("[%s] Error colocando trigger orders TP/SL: %s", sym, e)

    # ── LIMIT INTERNO + TELEMETRÍA ────────────────────────────────────────────

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
