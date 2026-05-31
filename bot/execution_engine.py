"""
execution_engine.py — Motor de ejecución para Hyperliquid.

En Hyperliquid el SL y TP van DENTRO de la misma orden de apertura.
No existe la race condition de Bitget (llamada separada de TPSL).

Flujo:
  1. Calcular arrival_price (mid del orderbook o last)
  2. Intentar limit agresiva si spread <= umbral y depth suficiente
  3. Esperar timeout corto (EE_LIMIT_TIMEOUT_S)
  4. Si no llena → cancelar y fallback a market
  5. Registrar telemetría

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          default 4
  EE_MAX_SPREAD_BPS_LIMIT     default 15
  EE_LIMIT_OFFSET_BPS         default 3
  EE_MAX_SLIPPAGE_ALERT_BPS   default 30
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bot.trader import FuturesTrader

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


class ExecutionEngine:
    def __init__(self) -> None:
        self.limit_timeout_s:        float = _e("EE_LIMIT_TIMEOUT_S",         4.0)
        self.max_spread_bps_limit:   float = _e("EE_MAX_SPREAD_BPS_LIMIT",   15.0)
        self.limit_offset_bps:       float = _e("EE_LIMIT_OFFSET_BPS",        3.0)
        self.max_slippage_alert_bps: float = _e("EE_MAX_SLIPPAGE_ALERT_BPS", 30.0)
        self._records: dict[str, list[TradeRecord]] = defaultdict(list)

    async def execute(
        self,
        trader:        "FuturesTrader",
        side:          str,
        qty:           float,
        arrival_price: float,
        ask:           float | None = None,
        bid:           float | None = None,
        trade_side:    str = "open",
        reduce_only:   bool = False,
        sl:            float | None = None,
        tp:            float | None = None,
    ) -> dict:
        sym = trader.symbol
        rec = TradeRecord(symbol=sym, side=side, qty=qty, arrival_price=arrival_price)
        t0  = time.monotonic()

        spread_bps = self._calc_spread_bps(ask, bid, arrival_price)
        use_limit  = (
            ask is not None
            and bid is not None
            and spread_bps <= self.max_spread_bps_limit
        )

        if use_limit:
            limit_price = self._calc_limit_price(side, arrival_price, ask, bid)
            result, filled = await self._try_limit(
                trader, side, qty, limit_price, rec,
                trade_side=trade_side, reduce_only=reduce_only, sl=sl, tp=tp,
            )
            if filled:
                rec.order_type_used = "limit"
                rec.fill_price      = limit_price
            else:
                # Si el error es de firma/agente no conocido, ir directo a market
                agent_err = _AGENT_NOT_FOUND_SUBSTR in str(rec.cancel_reason)
                if not agent_err:
                    rec.cancel_reason = rec.cancel_reason or "timeout"
                logger.info(f"[{sym}] ⚡ Limit sin fill → fallback market")
                result = await trader._place_order_raw(
                    side, qty, trade_side=trade_side,
                    reduce_only=reduce_only, sl=sl, tp=tp,
                )
                rec.order_type_used = "market"
                rec.fill_price      = arrival_price
        else:
            reason = (
                f"spread {spread_bps:.1f} bps > {self.max_spread_bps_limit:.0f} bps"
                if ask is not None else "sin datos de orderbook"
            )
            logger.debug(f"[{sym}] Market directo ({reason})")
            result = await trader._place_order_raw(
                side, qty, trade_side=trade_side,
                reduce_only=reduce_only, sl=sl, tp=tp,
            )
            rec.order_type_used = "market"
            rec.fill_price      = arrival_price
            rec.cancel_reason   = reason

        rec.fill_latency_ms = (time.monotonic() - t0) * 1000
        rec.success         = result.get("status") == "ok"

        if rec.success and arrival_price > 0:
            if side in ("buy", "long"):
                rec.slippage_bps = (rec.fill_price - arrival_price) / arrival_price * 10_000
            else:
                rec.slippage_bps = (arrival_price - rec.fill_price) / arrival_price * 10_000

        self._records[sym].append(rec)

        log_msg = (
            f"[{sym}] 📊 Exec: type={rec.order_type_used} side={side} qty={qty} "
            f"tradeSide={trade_side} arrival={arrival_price:.4f} fill={rec.fill_price:.4f} "
            f"slippage={rec.slippage_bps:+.1f}bps latency={rec.fill_latency_ms:.0f}ms"
        )
        if rec.slippage_bps > self.max_slippage_alert_bps:
            logger.warning(f"⚠️ SLIPPAGE ALTO — {log_msg}")
        else:
            logger.info(log_msg)

        return result

    def get_stats(self, symbol: str) -> dict:
        recs = [r for r in self._records.get(symbol, []) if r.success]
        if not recs:
            return {"symbol": symbol, "trades": 0}

        def _avg(lst): return sum(lst) / len(lst) if lst else 0.0

        buys   = [r for r in recs if r.side in ("buy",  "long")]
        sells  = [r for r in recs if r.side in ("sell", "short")]
        limits = [r for r in recs if r.order_type_used == "limit"]
        mkts   = [r for r in recs if r.order_type_used == "market"]

        return {
            "symbol":              symbol,
            "trades":              len(recs),
            "avg_slippage_bps":    _avg([r.slippage_bps    for r in recs]),
            "avg_latency_ms":      _avg([r.fill_latency_ms for r in recs]),
            "buy_slippage_bps":    _avg([r.slippage_bps    for r in buys]),
            "sell_slippage_bps":   _avg([r.slippage_bps    for r in sells]),
            "limit_fill_rate":     len(limits) / len(recs),
            "market_fill_rate":    len(mkts)   / len(recs),
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

    async def _try_limit(
        self, trader, side, qty, price, rec,
        trade_side="open", reduce_only=False, sl=None, tp=None,
    ) -> tuple[dict, bool]:
        sym = trader.symbol
        result = await trader._place_order_raw(
            side, qty, order_type="limit", price=price,
            trade_side=trade_side, reduce_only=reduce_only, sl=sl, tp=tp,
        )
        if result.get("status") != "ok":
            err_str = str(result.get("response", ""))
            rec.cancel_reason = f"limit_rejected:{err_str}"
            # Si es error de agente no registrado, marcar explícitamente para
            # que execute() vaya directo a market sin reintentar otra limit.
            if _AGENT_NOT_FOUND_SUBSTR in err_str:
                rec.cancel_reason = f"agent_not_found:{err_str}"
            return result, False

        # En Hyperliquid el orderId viene en response.data.statuses[0]
        try:
            order_id = result["response"]["data"]["statuses"][0].get("resting", {}).get("oid")
        except (KeyError, IndexError, TypeError):
            order_id = None

        deadline = time.monotonic() + self.limit_timeout_s
        filled   = False

        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            if order_id:
                status = await trader._get_order_status(order_id)
                state  = status.get("order", {}).get("order", {}).get("status", "")
                if state == "filled":
                    filled = True
                    break
                if state == "cancelled":
                    rec.cancel_reason = "cancelled_externally"
                    break

        if not filled and order_id:
            await trader._cancel_order(order_id)
            if not rec.cancel_reason:
                rec.cancel_reason = "timeout"

        return result, filled


execution_engine = ExecutionEngine()
