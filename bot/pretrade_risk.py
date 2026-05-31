"""
pretrade_risk.py — Motor de riesgo pre-trade institucional.

Valida cada intención de orden ANTES de enviarla al exchange.
Si cualquier check falla, devuelve (False, motivo_str) y la orden no se manda.

Defaults ajustados para cuentas pequeñas (10-100 USDC por trade):
  - PT_MAX_NOTIONAL_PER_TRADE : 200 USDC  (antes 500)
  - PT_MAX_SYMBOL_EXPOSURE    : 200 USDC  (antes 1000)
  - PT_MAX_TOTAL_EXPOSURE     : 500 USDC  (antes 3000)
  - PT_MIN_SL_DISTANCE_BPS    :   8 bps   (ok, sin cambios)
  - PT_BALANCE_USAGE_PCT      :  0.90     (ok, sin cambios)
"""
from __future__ import annotations

import logging
import os
import time
from collections import deque

logger = logging.getLogger("PreTradeRisk")


def _e(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


class PreTradeRisk:
    """
    Variables de entorno (todas opcionales):

      PT_MAX_NOTIONAL_PER_TRADE   USDC máximos por operación          (default 200)
      PT_MAX_SYMBOL_EXPOSURE      USDC máximos en un símbolo          (default 200)
      PT_MAX_TOTAL_EXPOSURE       USDC máximos en todas las posiciones (default 500)
      PT_MAX_SPREAD_BPS           Spread máximo permitido en bps       (default 30)
      PT_MIN_SL_DISTANCE_BPS      Distancia mínima SL en bps           (default 8)
      PT_MAX_SLIPPAGE_BPS         Slippage esperado máximo aceptable   (default 50)
      PT_MAX_ORDERS_PER_MIN       Órdenes máximas por minuto POR SÍMBOLO (default 6)
      PT_BALANCE_USAGE_PCT        Máximo % del balance por trade       (default 0.90)
    """

    def __init__(self) -> None:
        self.max_notional_per_trade : float = _e("PT_MAX_NOTIONAL_PER_TRADE", 200.0)
        self.max_symbol_exposure    : float = _e("PT_MAX_SYMBOL_EXPOSURE",   200.0)
        self.max_total_exposure     : float = _e("PT_MAX_TOTAL_EXPOSURE",    500.0)
        self.max_spread_bps         : float = _e("PT_MAX_SPREAD_BPS",         30.0)
        self.min_sl_distance_bps    : float = _e("PT_MIN_SL_DISTANCE_BPS",    8.0)
        self.max_slippage_bps       : float = _e("PT_MAX_SLIPPAGE_BPS",       50.0)
        self.max_orders_per_min     : int   = int(_e("PT_MAX_ORDERS_PER_MIN",  6))
        self.balance_usage_pct      : float = _e("PT_BALANCE_USAGE_PCT",       0.90)

        self._symbol_exposure  : dict[str, float] = {}
        self._order_timestamps : dict[str, deque] = {}

    # ── API pública ────────────────────────────────────────────────────────────────

    async def check(
        self,
        symbol:   str,
        side:     str,
        notional: float,
        price:    float,
        balance:  float | None,
        sl:       float | None = None,
        ask:      float | None = None,
        bid:      float | None = None,
    ) -> tuple[bool, str]:
        sym = symbol.replace("/", "").replace(":USDT", "")

        checks = [
            self._check_notional(notional),
            self._check_balance_usage(notional, balance),
            self._check_symbol_exposure(sym, notional),
            self._check_total_exposure(notional),
            self._check_order_rate(sym),
            self._check_spread(ask, bid, price),
            self._check_sl_distance(price, sl, side),
        ]

        for ok, reason in checks:
            if not ok:
                logger.warning(
                    f"[PreTrade:{sym}] ❌ BLOQUEADO — {reason} "
                    f"| side={side} notional={notional:.2f} price={price:.4f}"
                )
                return False, reason

        self._register_exposure(sym, notional)
        logger.info(
            f"[PreTrade:{sym}] ✅ OK — side={side} notional={notional:.2f} "
            f"price={price:.4f} sl={sl}"
        )
        return True, "OK"

    def confirm_order(self, symbol: str) -> None:
        sym = symbol.replace("/", "").replace(":USDT", "")
        if sym not in self._order_timestamps:
            self._order_timestamps[sym] = deque()
        self._order_timestamps[sym].append(time.monotonic())

    def register_close(self, symbol: str, notional: float) -> None:
        sym  = symbol.replace("/", "").replace(":USDT", "")
        prev = self._symbol_exposure.get(sym, 0.0)
        self._symbol_exposure[sym] = max(0.0, prev - notional)
        logger.debug(
            f"[PreTrade:{sym}] Exposición liberada: -{notional:.2f} USDC "
            f"(queda {self._symbol_exposure[sym]:.2f})"
        )

    def get_total_exposure(self) -> float:
        return sum(self._symbol_exposure.values())

    def get_symbol_exposure(self, symbol: str) -> float:
        sym = symbol.replace("/", "").replace(":USDT", "")
        return self._symbol_exposure.get(sym, 0.0)

    # ── Checks individuales ──────────────────────────────────────────────────────────

    def _check_notional(self, notional: float) -> tuple[bool, str]:
        if notional > self.max_notional_per_trade:
            return False, (
                f"Notional {notional:.2f} USDC supera límite por trade "
                f"{self.max_notional_per_trade:.0f} USDC"
            )
        return True, ""

    def _check_balance_usage(self, notional: float, balance: float | None) -> tuple[bool, str]:
        if balance is None:
            logger.warning(
                "[PreTrade] ⚠️ Balance desconocido (API falló) — "
                f"asumiendo ≥ {notional:.2f} USDC para continuar"
            )
            return True, ""
        if balance <= 0:
            return False, f"Balance {balance:.2f} USDC inválido (cuenta vacía)"
        if notional > balance:
            return False, (
                f"Notional {notional:.2f} USDC supera balance disponible {balance:.2f} USDC"
            )
        usage = notional / balance
        if usage > self.balance_usage_pct:
            return False, (
                f"Uso de balance {usage*100:.1f}% supera límite "
                f"{self.balance_usage_pct*100:.0f}%"
            )
        return True, ""

    def _check_symbol_exposure(self, sym: str, notional: float) -> tuple[bool, str]:
        current   = self._symbol_exposure.get(sym, 0.0)
        projected = current + notional
        if projected > self.max_symbol_exposure:
            return False, (
                f"Exposición en {sym} llegaría a {projected:.2f} USDC "
                f"(límite {self.max_symbol_exposure:.0f} USDC)"
            )
        return True, ""

    def _check_total_exposure(self, notional: float) -> tuple[bool, str]:
        projected = self.get_total_exposure() + notional
        if projected > self.max_total_exposure:
            return False, (
                f"Exposición total llegaría a {projected:.2f} USDC "
                f"(límite {self.max_total_exposure:.0f} USDC)"
            )
        return True, ""

    def _check_order_rate(self, sym: str) -> tuple[bool, str]:
        now = time.monotonic()
        if sym not in self._order_timestamps:
            self._order_timestamps[sym] = deque()
        ts = self._order_timestamps[sym]
        while ts and now - ts[0] > 60.0:
            ts.popleft()
        if len(ts) >= self.max_orders_per_min:
            return False, (
                f"Rate de órdenes: {len(ts)} en 60 s "
                f"(límite {self.max_orders_per_min})"
            )
        return True, ""

    def _check_spread(
        self, ask: float | None, bid: float | None, price: float
    ) -> tuple[bool, str]:
        if ask is None or bid is None or price <= 0:
            return True, ""
        spread_bps = (ask - bid) / price * 10_000
        if spread_bps > self.max_spread_bps:
            return False, (
                f"Spread {spread_bps:.1f} bps supera límite "
                f"{self.max_spread_bps:.0f} bps"
            )
        return True, ""

    def _check_sl_distance(
        self, price: float, sl: float | None, side: str
    ) -> tuple[bool, str]:
        if sl is None or price <= 0:
            return True, ""
        if side in ("buy", "long"):
            dist_bps = (price - sl) / price * 10_000
        else:
            dist_bps = (sl - price) / price * 10_000
        if dist_bps < 0:
            return False, f"SL {sl:.4f} está en dirección incorrecta (price={price:.4f})"
        if dist_bps < self.min_sl_distance_bps:
            return False, (
                f"SL demasiado ajustado: {dist_bps:.1f} bps "
                f"(mínimo {self.min_sl_distance_bps:.0f} bps)"
            )
        return True, ""

    def _register_exposure(self, sym: str, notional: float) -> None:
        self._symbol_exposure[sym] = self._symbol_exposure.get(sym, 0.0) + notional


pretrade_risk = PreTradeRisk()
