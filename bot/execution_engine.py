"""
execution_engine.py — Motor de ejecución profesional.

Flujo por orden:
  1. Calcular arrival_price (mid o last tick)
  2. Intentar limit agresiva si spread <= umbral y depth suficiente
  3. Esperar timeout corto (EE_LIMIT_TIMEOUT_S)
  4. Si no llena → cancelar y hacer fallback a market
  5. Registrar telemetría: arrival, fill, slippage_bps, latencia, fill_ratio

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          Segundos de espera antes de fallback market  (default 4)
  EE_MAX_SPREAD_BPS_LIMIT     Spread máximo para intentar limit (bps)       (default 15)
  EE_LIMIT_OFFSET_BPS         Offset agresivo sobre mid para limit (bps)    (default 3)
  EE_MAX_SLIPPAGE_ALERT_BPS   Umbral de alerta de slippage (bps)            (default 30)

Uso:
  from bot.execution_engine import execution_engine
  result = await execution_engine.execute(
      trader=self, side="buy", qty=0.01,
      arrival_price=67000.0, ask=67002.0, bid=66998.0,
      trade_side="open",
  )
  # result: dict con code, fill_price, slippage_bps, ...
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

# Códigos de error que NO deben reintentarse con fallback market.
# Son errores de configuración/permisos, no de ejecución transitoria.
_FATAL_CODES = {"40085", "40001", "40006", "40009", "40037"}


def _e(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


@dataclass
class TradeRecord:
    symbol:            str
    side:              str
    qty:               float
    arrival_price:     float
    fill_price:        float       = 0.0
    slippage_bps:      float       = 0.0
    fill_latency_ms:   float       = 0.0
    partial_fill_ratio: float      = 1.0
    order_type_used:   str         = "market"  # "limit" o "market"
    cancel_reason:     str         = ""        # por qué se canceló el limit (si aplica)
    success:           bool        = False


class ExecutionEngine:
    """
    Motor de ejecución desacoplado de FuturesTrader.
    Gestiona la estrategia limit→timeout→market y registra telemetría.
    """

    def __init__(self) -> None:
        self.limit_timeout_s:        float = _e("EE_LIMIT_TIMEOUT_S",         4.0)
        self.max_spread_bps_limit:   float = _e("EE_MAX_SPREAD_BPS_LIMIT",   15.0)
        self.limit_offset_bps:       float = _e("EE_LIMIT_OFFSET_BPS",        3.0)
        self.max_slippage_alert_bps: float = _e("EE_MAX_SLIPPAGE_ALERT_BPS", 30.0)

        # Telemetría: {symbol: [TradeRecord]}
        self._records: dict[str, list[TradeRecord]] = defaultdict(list)

    # ── API pública ─────────────────────────────────────────────────────────

    async def execute(
        self,
        trader:        "FuturesTrader",
        side:          str,
        qty:           float,
        arrival_price: float,
        ask:           float | None = None,
        bid:           float | None = None,
        trade_side:    str = "open",
    ) -> dict:
        """
        Intenta primero con limit agresiva; si no llena en timeout, usa market.
        Devuelve el resultado de la orden (dict con 'code' al mínimo).

        Si la order recibe un código FATAL (ej. 40085 UA config error),
        NO hace fallback a market — propaga el error directamente.

        trade_side: "open" para abrir posición, "close" para cerrarla.
        Se propaga a _place_order_raw en todas las rutas.
        """
        sym = trader.symbol
        rec = TradeRecord(
            symbol=sym,
            side=side,
            qty=qty,
            arrival_price=arrival_price,
        )
        t0 = time.monotonic()

        spread_bps = self._calc_spread_bps(ask, bid, arrival_price)
        use_limit  = (
            ask is not None
            and bid is not None
            and spread_bps <= self.max_spread_bps_limit
        )

        if use_limit:
            limit_price = self._calc_limit_price(side, arrival_price, ask, bid)
            result, filled = await self._try_limit(
                trader, side, qty, limit_price, rec, trade_side=trade_side
            )
            if filled:
                rec.order_type_used = "limit"
                rec.fill_price      = limit_price
            else:
                # Si es un error fatal (ej. 40085), NO hacer fallback market.
                error_code = result.get("code", "")
                if error_code in _FATAL_CODES:
                    logger.warning(
                        f"[{sym}] ⛔ Error fatal {error_code} en limit — "
                        f"abortando sin fallback market"
                    )
                    rec.order_type_used = "limit"
                    rec.fill_price      = arrival_price
                    rec.cancel_reason   = f"fatal:{error_code}"
                else:
                    # Fallback a market solo para errores transitorios
                    rec.cancel_reason = "timeout" if not filled else "unfilled"
                    logger.info(
                        f"[{sym}] ⚡ Limit sin fill en {self.limit_timeout_s}s → fallback market"
                    )
                    result = await trader._place_order_raw(side, qty, trade_side=trade_side)
                    rec.order_type_used = "market"
                    rec.fill_price      = arrival_price  # estimación conservadora
        else:
            # Spread demasiado amplio o sin orderbook → directo market
            reason = (
                f"spread {spread_bps:.1f} bps > {self.max_spread_bps_limit:.0f} bps"
                if ask is not None
                else "sin datos de orderbook"
            )
            logger.debug(f"[{sym}] Market directo ({reason})")
            result = await trader._place_order_raw(side, qty, trade_side=trade_side)
            rec.order_type_used = "market"
            rec.fill_price      = arrival_price
            rec.cancel_reason   = reason

        # ── Telemetría ──────────────────────────────────────────────────────────
        rec.fill_latency_ms = (time.monotonic() - t0) * 1000
        rec.success         = result.get("code") == "00000"

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
        """
        Estadísticas de slippage y fill para un símbolo.
        Devuelve dict con medias por tipo de orden y por lado.
        """
        sym   = symbol.replace("/", "").replace(":USDT", "")
        recs  = [r for r in self._records.get(sym, []) if r.success]
        if not recs:
            return {"symbol": sym, "trades": 0}

        def _avg(lst):  return sum(lst) / len(lst) if lst else 0.0

        buys   = [r for r in recs if r.side in ("buy",  "long")]
        sells  = [r for r in recs if r.side in ("sell", "short")]
        limits = [r for r in recs if r.order_type_used == "limit"]
        mkts   = [r for r in recs if r.order_type_used == "market"]

        return {
            "symbol":              sym,
            "trades":              len(recs),
            "avg_slippage_bps":    _avg([r.slippage_bps    for r in recs]),
            "avg_latency_ms":      _avg([r.fill_latency_ms for r in recs]),
            "buy_slippage_bps":    _avg([r.slippage_bps    for r in buys]),
            "sell_slippage_bps":   _avg([r.slippage_bps    for r in sells]),
            "limit_fill_rate":     len(limits) / len(recs),
            "market_fill_rate":    len(mkts)   / len(recs),
            "limit_avg_slip_bps":  _avg([r.slippage_bps    for r in limits]),
            "market_avg_slip_bps": _avg([r.slippage_bps    for r in mkts]),
            "recent_10": [
                {
                    "side":       r.side,
                    "type":       r.order_type_used,
                    "slippage":   round(r.slippage_bps, 2),
                    "latency_ms": round(r.fill_latency_ms, 1),
                    "cancel":     r.cancel_reason,
                }
                for r in recs[-10:]
            ],
        }

    def get_all_stats(self) -> list[dict]:
        """Estadísticas de todos los símbolos."""
        return [self.get_stats(sym) for sym in self._records]

    # ── Internos ────────────────────────────────────────────────────────────

    def _calc_spread_bps(
        self, ask: float | None, bid: float | None, price: float
    ) -> float:
        if ask is None or bid is None or price <= 0:
            return 9999.0
        return (ask - bid) / price * 10_000

    def _calc_limit_price(
        self, side: str, arrival: float,
        ask: float, bid: float
    ) -> float:
        """
        Precio agresivo: cruzar el mid ligeramente para mejorar fill rate.
        BUY  → mid + offset (igual que un limit que 'toma' el ask bajo)
        SELL → mid - offset
        """
        mid    = (ask + bid) / 2
        offset = arrival * self.limit_offset_bps / 10_000
        if side in ("buy", "long"):
            return round(mid + offset, 6)
        return round(mid - offset, 6)

    async def _try_limit(
        self,
        trader: "FuturesTrader",
        side:   str,
        qty:    float,
        price:  float,
        rec:    TradeRecord,
        trade_side: str = "open",
    ) -> tuple[dict, bool]:
        """
        Coloca limit y espera hasta limit_timeout_s.
        Devuelve (result_dict, filled_bool).
        Si no llena, cancela la orden y devuelve filled=False.
        Si la orden devuelve un código fatal, devuelve inmediatamente filled=False
        con el resultado de error para que el llamador no haga fallback.
        """
        sym = trader.symbol
        result = await trader._place_order_raw(
            side, qty, order_type="limit", price=price, trade_side=trade_side
        )
        if result.get("code") != "00000":
            error_code = result.get("code", "")
            rec.cancel_reason = f"limit_rejected:{error_code} {result.get('msg', '')}"
            # Devolver el resultado original para que el llamador decida si es fatal
            return result, False

        order_id = (result.get("data") or {}).get("orderId")
        deadline = time.monotonic() + self.limit_timeout_s
        filled   = False

        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            status = await trader._get_order_status(order_id)
            state  = (status.get("data") or {}).get("state", "")
            if state in ("filled", "full_fill"):
                filled = True
                break
            if state in ("cancelled", "canceled", "cancel"):
                rec.cancel_reason = "cancelled_externally"
                break

        if not filled and order_id:
            await trader._cancel_order(order_id)
            if not rec.cancel_reason:
                rec.cancel_reason = "timeout"

        return result, filled


# Singleton global
execution_engine = ExecutionEngine()
