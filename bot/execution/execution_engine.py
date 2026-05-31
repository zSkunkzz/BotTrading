"""
execution_engine.py — Motor de ejecución con TP/SL reales en Hyperliquid.

Cambios respecto a la versión anterior:
  - Usa HLClient (SDK oficial) en lugar de _place_order_raw.
  - Al abrir posición: coloca entrada + trigger TP + trigger SL individualmente.
  - Al cerrar: cancela los trigger orders abiertos antes de la orden de cierre.
  - Si el bot se reinicia, el TP/SL sigue activo en el exchange.
  - ThreadPoolExecutor con max_workers=4 para no saturar el event loop
    cuando hay muchos símbolos en paralelo.

Flujo de apertura:
  1. Calcular arrival_price (mid del orderbook o last)
  2. Intentar limit agresiva si spread <= umbral y depth suficiente
  3. Si llena: colocar TP trigger + SL trigger
  4. Si no llena en timeout: cancelar → fallback market + TP/SL
  5. Registrar telemetría

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          default 4
  EE_MAX_SPREAD_BPS_LIMIT     default 15
  EE_LIMIT_OFFSET_BPS         default 3
  EE_MAX_SLIPPAGE_ALERT_BPS   default 30
  EE_TP_AS_LIMIT              default true  (False = TP como market)
  EE_EXECUTOR_WORKERS         default 4
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bot.trader import FuturesTrader

from bot.core.hl_client import HLClient

logger = logging.getLogger("ExecutionEngine")

_AGENT_NOT_FOUND_SUBSTR = "does not exist"

_EXECUTOR_WORKERS = int(os.getenv("EE_EXECUTOR_WORKERS", "4"))
_executor = ThreadPoolExecutor(max_workers=_EXECUTOR_WORKERS)


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
      - ThreadPoolExecutor propio con workers limitados
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
        if symbol not in self._hl_clients:
            self._hl_clients[symbol] = HLClient(symbol)
        return self._hl_clients[symbol]

    async def _run(self, fn, *args):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(_executor, fn, *args)

    # ────────────────────────────────────────────────────────────────────
    # EXECUTE PRINCIPAL
    # ────────────────────────────────────────────────────────────────────

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
        sym    = trader.symbol
        client = self._get_client(sym)
        rec    = TradeRecord(symbol=sym, side=side, qty=qty, arrival_price=arrival_price)
        rec._t0 = time.monotonic()

        is_buy = side in ("buy", "long")

        if reduce_only:
            try:
                cancelled = await self._run(client.cancel_all_open_tpsl)
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
                result = await self._run(client.place_market, is_buy, qty, arrival_price, reduce_only)
                rec.order_type_used = "market"
                rec.fill_price      = arrival_price
        else:
            reason = (
                f"spread {spread_bps:.1f}bps > {self.max_spread_bps_limit:.0f}bps"
                if ask is not None else "sin datos de orderbook"
            )
            logger.debug("[%s] Market directo (%s)", sym, reason)
            result = await self._run(client.place_market, is_buy, qty, arrival_price, reduce_only)
            rec.order_type_used = "market"
            rec.fill_price      = arrival_price
            rec.cancel_reason   = reason

        if (
            result.get("status") == "ok"
            and not reduce_only
            and trade_side == "open"
            and (sl is not None or tp is not None)
        ):
            await self._place_tpsl(client, is_buy, qty, sl, tp, sym)

        self._finalize_rec(rec, result, side, arrival_price)
        return result

    # ────────────────────────────────────────────────────────────────────
    # TP / SL TRIGGER ORDERS — usando place_tp / place_sl directamente
    # ────────────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        client: HLClient,
        is_buy: bool,
        qty: float,
        sl: Optional[float],
        tp: Optional[float],
        sym: str,
    ) -> None:
        """
        Coloca TP y SL como trigger orders reales usando place_tp / place_sl.
        La dirección de cierre es la opuesta a la posición:
          - Long (is_buy=True)  → cierre = vender (close_is_buy=False)
          - Short (is_buy=False) → cierre = comprar (close_is_buy=True)
        """
        close_is_buy = not is_buy

        if tp is not None:
            try:
                limit_px = tp if self.tp_as_limit else None
                result = await self._run(client.place_tp, close_is_buy, qty, tp, limit_px)
                st = result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                if "error" in st:
                    logger.error("[%s] ❌ Trigger TP fallido: %s", sym, st["error"])
                else:
                    logger.info("[%s] ✅ Trigger TP colocado en exchange @ %.6f", sym, tp)
            except Exception as e:
                logger.error("[%s] Error colocando trigger TP: %s", sym, e)

        if sl is not None:
            try:
                result = await self._run(client.place_sl, close_is_buy, qty, sl)
                st = result.get("response", {}).get("data", {}).get("statuses", [{}])[0]
                if "error" in st:
                    logger.error("[%s] ❌ Trigger SL fallido: %s", sym, st["error"])
                else:
                    logger.info("[%s] ✅ Trigger SL colocado en exchange @ %.6f", sym, sl)
            except Exception as e:
                logger.error("[%s] Error colocando trigger SL: %s", sym, e)

    # ────────────────────────────────────────────────────────────────────
    # LIMIT INTERNO + TELEMETRÍA
    # ────────────────────────────────────────────────────────────────────

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
            result = await self._run(client.place_limit, is_buy, qty, price, False)
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

        try:
            immediate_fill = result["response"]["data"]["statuses"][0].get("filled")
            if immediate_fill is not None and order_id is None:
                return result, True
        except (KeyError, IndexError, TypeError):
            pass

        deadline = time.monotonic() + self.limit_timeout_s
        filled   = False

        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            if order_id:
                try:
                    open_orders = await self._run(client.get_open_orders)
                    still_open = any(o.get("oid") == order_id for o in open_orders)
                    if not still_open:
                        filled = True
                        break
                except Exception:
                    pass

        if not filled and order_id:
            try:
                await self._run(client.cancel_order, order_id)
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
