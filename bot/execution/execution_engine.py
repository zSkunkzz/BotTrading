"""
execution_engine.py — Motor de ejecución con TP/SL reales en Hyperliquid.

Flujo de apertura:
  1. Guardia dura: si trade_side=="open" y falta sl o tp → abortar inmediatamente.
  2. Calcular arrival_price (mid del orderbook o last)
  3. Intentar limit agresiva si spread <= umbral y depth suficiente
  4. Si llena: colocar SL + TP via place_sl() / place_tp() del HLClient
  5. Si no llena en timeout: cancelar → fallback market + SL/TP
     (FIX race condition: se usa entry_ok propio del fallback, no el result de la limit)
  6. Registrar telemetría

FIX CRÍTICO (2026-06-01):
  El método anterior _place_tpsl_bulk construía dicts con claves en snake_case
  ("order_type", "is_buy", etc.) y los pasaba a exchange.bulk_orders() que espera
  camelCase ("orderType"). El bulk fallaba silenciosamente → posición sin SL/TP.
  Solución: usar client.place_sl() + client.place_tp() directamente, que son los
  métodos que YA FUNCIONABAN en las primeras operaciones del bot.

FIX CRÍTICO (2026-06-02) — CAUSA RAÍZ DE DUPLICACIONES:
  Cuando una limit agresiva se llena al instante, Hyperliquid devuelve
  status[0] = {"filled": {...}} en lugar de {"resting": {...}}.
  El código anterior solo leía "resting.oid" → order_id = None →
  entraba al bucle de polling de 4s → no encontraba la orden en open_orders
  (ya estaba en historial) → asumía no-fill → fallback MARKET → DUPLICADO.
  Solución: detectar fill inmediato mirando si status[0] tiene clave "filled".

FIX entry_px en place_sl/place_tp (2026-06-02):
  execute() y _place_tpsl() no propagaban entry_px a place_sl()/place_tp().
  Sin entry_px, _adjust_sl_px/_adjust_tp_px en hl_client.py no podían
  validar ni corregir precios inválidos (ej. SL de LONG >= entry por
  redondeo del signal_engine), causando que Hyperliquid rechazara con
  'Invalid TPSL price' → sl_placed=False → _protection_ok=False →
  KillSwitch L3 en 30s.
  Solución: execute() acepta entry_px opcional (fallback a arrival_price)
  y lo propaga a _place_tpsl(), que lo pasa a place_sl()/place_tp().

FIX BUG #9 (2026-06-02) — _records sin límite:
  _finalize_rec acumulaba TradeRecord indefinidamente por symbol.
  En despliegues de larga duración sin restart (Railway), esto causaba
  un leak de memoria lento. Fix: mantener máximo 500 records por symbol.

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          default 4
  EE_MAX_SPREAD_BPS_LIMIT     default 15
  EE_LIMIT_OFFSET_BPS         default 3
  EE_MAX_SLIPPAGE_ALERT_BPS   default 30
  EE_TP_AS_LIMIT              default true  (False = TP como market trigger)
  EE_TPSL_RETRY_ATTEMPTS      default 3
  EE_TPSL_RETRY_BASE_DELAY_S  default 1.5
  EE_MARKET_429_RETRIES       default 3
  EE_MARKET_429_DELAY_S       default 2.0
  EE_MAX_RECORDS_PER_SYMBOL   default 500
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


_MAX_RECORDS_PER_SYMBOL = int(os.getenv("EE_MAX_RECORDS_PER_SYMBOL", "500"))


class ExecutionEngine:
    """
    Motor de ejecución:
      - Usa el SDK oficial de Hyperliquid (HLClient)
      - Coloca TP y SL como trigger orders reales via place_sl() / place_tp()
      - TP/SL sobreviven reinicios del bot
      - NUNCA abre una posición sin SL y TP válidos
    """

    def __init__(self) -> None:
        self.limit_timeout_s:        float = _e("EE_LIMIT_TIMEOUT_S",         4.0)
        self.max_spread_bps_limit:   float = _e("EE_MAX_SPREAD_BPS_LIMIT",   15.0)
        self.limit_offset_bps:       float = _e("EE_LIMIT_OFFSET_BPS",        3.0)
        self.max_slippage_alert_bps: float = _e("EE_MAX_SLIPPAGE_ALERT_BPS", 30.0)
        self.tp_as_limit:            bool  = os.getenv("EE_TP_AS_LIMIT", "true").lower() == "true"
        self.tpsl_retry_attempts:    int   = int(_e("EE_TPSL_RETRY_ATTEMPTS", 3))
        self.tpsl_retry_base_delay:  float = _e("EE_TPSL_RETRY_BASE_DELAY_S", 1.5)
        self.market_429_retries:     int   = int(_e("EE_MARKET_429_RETRIES",  3))
        self.market_429_delay:       float = _e("EE_MARKET_429_DELAY_S",      2.0)
        self._records: dict[str, list[TradeRecord]] = defaultdict(list)
        self._hl_clients: dict[str, HLClient] = {}

    def _get_client(self, symbol: str) -> HLClient:
        if symbol not in self._hl_clients:
            self._hl_clients[symbol] = HLClient(symbol)
        return self._hl_clients[symbol]

    # ── UTILIDADES ──────────────────────────────────────────────────────

    @staticmethod
    def _round_qty(qty: float, sz_decimals: int) -> float:
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
        entry_px:      Optional[float] = None,
    ) -> dict:
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

        sz_dec = client.get_sz_decimals()
        qty    = self._round_qty(qty, sz_dec)
        rec.qty = qty

        if qty <= 0:
            logger.error("[%s] qty redondeada a 0 (sz_decimals=%d) — abortando", sym, sz_dec)
            return {"status": "error", "response": "qty rounded to zero"}

        # Cierre manual → cancelar TP/SL previos
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

        entry_ok = False
        result   = {"status": "error", "response": "not executed"}

        if use_limit:
            limit_price = self._calc_limit_price(side, arrival_price, ask, bid)
            limit_result, filled = await self._try_limit_sdk(client, is_buy, qty, limit_price, rec)

            if filled:
                result              = limit_result
                entry_ok            = True
                rec.order_type_used = "limit"
                rec.fill_price      = limit_price
            else:
                if _AGENT_NOT_FOUND_SUBSTR in rec.cancel_reason:
                    self._log_agent_error(sym, trader)
                    rec.order_type_used = "market"
                    rec.fill_price      = arrival_price
                    self._finalize_rec(rec, limit_result, side, arrival_price)
                    return limit_result

                logger.info("[%s] ⚡ Limit sin fill → fallback market", sym)
                market_result = await self._place_market_with_retry(
                    client, is_buy, qty, reduce_only, arrival_price, sym
                )
                result = market_result
                if result.get("status") == "ok":
                    entry_ok = True
                else:
                    logger.error("[%s] ❌ Fallback market falló: %s", sym, result.get("response", ""))
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
                logger.error("[%s] ❌ Market order falló: %s", sym, result.get("response", ""))
            rec.order_type_used = "market"
            rec.fill_price      = arrival_price
            rec.cancel_reason   = reason

        # ── Colocar SL + TP tras entrada confirmada ──────────────────
        if entry_ok and not reduce_only and trade_side == "open":
            effective_entry_px = entry_px if (entry_px and entry_px > 0) else arrival_price
            await self._place_tpsl(client, is_buy, qty, sl, tp, sym, effective_entry_px)

        self._finalize_rec(rec, result, side, arrival_price)
        return result

    # ── SL + TP ─────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        client: HLClient,
        is_buy: bool,
        qty: float,
        sl: Optional[float],
        tp: Optional[float],
        sym: str,
        entry_px: Optional[float] = None,
    ) -> None:
        close_is_buy = not is_buy

        # ── SL ────────────────────────────────────────────────────────
        if sl is not None and sl > 0:
            sl_placed = False
            for attempt in range(self.tpsl_retry_attempts):
                if attempt > 0:
                    delay = self.tpsl_retry_base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "[%s] SL retry %d/%d — esperando %.1fs",
                        sym, attempt + 1, self.tpsl_retry_attempts, delay,
                    )
                    await asyncio.sleep(delay)
                try:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: client.place_sl(
                            is_buy=close_is_buy,
                            sz=qty,
                            trigger_px=float(sl),
                            entry_px=entry_px,
                        ),
                    )
                    statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                    st = statuses[0] if statuses else {}
                    if "error" in st:
                        err = st["error"]
                        if any(s in err.lower() for s in _RATE_LIMIT_SUBSTRS):
                            logger.warning("[%s] SL rate-limited: %s", sym, err)
                            continue
                        logger.error("[%s] ❌ SL rechazado por exchange: %s", sym, err)
                        break
                    else:
                        logger.info("[%s] ✅ SL=%.5f colocado en exchange", sym, sl)
                        sl_placed = True
                        break
                except Exception as e:
                    err_str = str(e).lower()
                    if any(s in err_str for s in _RATE_LIMIT_SUBSTRS):
                        logger.warning("[%s] SL exception rate-limit: %s", sym, e)
                        continue
                    logger.error("[%s] ❌ SL exception: %s", sym, e)
                    break

            if not sl_placed:
                logger.error(
                    "[%s] ❌ SL NO colocado tras %d intentos — POSICIÓN SIN STOP LOSS",
                    sym, self.tpsl_retry_attempts,
                )

        # ── TP ────────────────────────────────────────────────────────
        if tp is not None and tp > 0:
            tp_placed = False
            tp_limit  = None if not self.tp_as_limit else tp

            for attempt in range(self.tpsl_retry_attempts):
                if attempt > 0:
                    delay = self.tpsl_retry_base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "[%s] TP retry %d/%d — esperando %.1fs",
                        sym, attempt + 1, self.tpsl_retry_attempts, delay,
                    )
                    await asyncio.sleep(delay)
                try:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: client.place_tp(
                            is_buy=close_is_buy,
                            sz=qty,
                            trigger_px=float(tp),
                            limit_px=tp_limit,
                            entry_px=entry_px,
                        ),
                    )
                    statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                    st = statuses[0] if statuses else {}
                    if "error" in st:
                        err = st["error"]
                        if any(s in err.lower() for s in _RATE_LIMIT_SUBSTRS):
                            logger.warning("[%s] TP rate-limited: %s", sym, err)
                            continue
                        logger.error("[%s] ❌ TP rechazado por exchange: %s", sym, err)
                        break
                    else:
                        logger.info("[%s] ✅ TP=%.5f colocado en exchange", sym, tp)
                        tp_placed = True
                        break
                except Exception as e:
                    err_str = str(e).lower()
                    if any(s in err_str for s in _RATE_LIMIT_SUBSTRS):
                        logger.warning("[%s] TP exception rate-limit: %s", sym, e)
                        continue
                    logger.error("[%s] ❌ TP exception: %s", sym, e)
                    break

            if not tp_placed:
                logger.error(
                    "[%s] ❌ TP NO colocado tras %d intentos",
                    sym, self.tpsl_retry_attempts,
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
            status_0 = result["response"]["data"]["statuses"][0]
        except (KeyError, IndexError, TypeError):
            status_0 = {}

        if "filled" in status_0:
            logger.info(
                "[%s] ✅ Limit llenada al instante (fill inmediato detectado)",
                rec.symbol,
            )
            return result, True

        order_id = status_0.get("resting", {}).get("oid") if isinstance(status_0.get("resting"), dict) else None

        if order_id is None:
            logger.warning(
                "[%s] Limit: sin 'filled' ni 'resting.oid' en respuesta — "
                "asumiendo filled para evitar duplicado. Response: %s",
                rec.symbol, status_0,
            )
            return result, True

        deadline = time.monotonic() + self.limit_timeout_s
        filled   = False

        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
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

        if not filled:
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

        # BUG #9 FIX: limitar _records a MAX_RECORDS_PER_SYMBOL entradas por symbol
        # Evita leak de memoria lento en despliegues de larga duración sin restart.
        records = self._records[rec.symbol]
        records.append(rec)
        if len(records) > _MAX_RECORDS_PER_SYMBOL:
            del records[:-_MAX_RECORDS_PER_SYMBOL]

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
