"""
execution_engine.py — Motor de ejecución BingX Futures Perpetuos.

Flujo de apertura:
  1. Guardia dura: si trade_side=="open" y falta sl o tp → abortar inmediatamente.
  2. Calcular arrival_price (mid del orderbook o last)
  3. Intentar limit agresiva si spread <= umbral y depth suficiente
  4. Si llena: colocar SL + TP via place_sl() / place_tp() del BingXClient
  5. Si no llena en timeout: cancelar → fallback market + SL/TP
  6. Si el SL no se coloca tras todos los reintentos → cerrar la posición (NUEVO)
  7. Registrar telemetría

Respuestas normalizadas (BingXClient convierte al formato OKX internamente):
  place_order  → {"code":"0", "data":[{"ordId":"...","sCode":"0"}]}
  place_sl/tp  → {"code":"0", "data":[{"algoId":"...","sCode":"0"}]}
  Normalizado en _okx_ok() → bool y _okx_order_id() → str|None

Variables de entorno (todas opcionales):
  EE_LIMIT_TIMEOUT_S          default 4
  EE_MAX_SPREAD_BPS_LIMIT     default 15
  EE_LIMIT_OFFSET_BPS         default 3
  EE_MAX_SLIPPAGE_ALERT_BPS   default 30
  EE_TP_AS_LIMIT              default true
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

from bot.core.bingx_client import BingXClient

logger = logging.getLogger("ExecutionEngine")

_RATE_LIMIT_SUBSTRS = ("429", "too many requests", "rate limit", "ratelimit")


def _e(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


# ── Helpers de respuesta (formato normalizado OKX-compatible) ──────────────────────────────

def _okx_ok(resp: dict) -> bool:
    if not resp or resp.get("code") != "0":
        return False
    data = resp.get("data", [])
    if not data:
        return False
    s_code = str(data[0].get("sCode", "0"))
    return s_code == "0"


def _okx_error_msg(resp: dict) -> str:
    if not resp:
        return "respuesta vacía"
    if resp.get("code") != "0":
        return resp.get("msg", f"code={resp.get('code')}")
    data = resp.get("data", [])
    if data:
        return data[0].get("sMsg", data[0].get("msg", "error desconocido"))
    return "error desconocido"


def _okx_order_id(resp: dict) -> Optional[str]:
    data = (resp or {}).get("data", [])
    if data:
        oid = data[0].get("ordId")
        if oid and oid != "":
            return str(oid)
    return None


def _okx_algo_id(resp: dict) -> Optional[str]:
    data = (resp or {}).get("data", [])
    if data:
        aid = data[0].get("algoId")
        if aid and aid != "":
            return str(aid)
    return None


# ── TradeRecord ────────────────────────────────────────────────────────────────────

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


# ── ExecutionEngine ───────────────────────────────────────────────────────────────────

class ExecutionEngine:
    """
    Motor de ejecución BingX:
      - Usa BingXClient (bot.core.bingx_client)
      - Coloca TP y SL como TAKE_PROFIT_MARKET / STOP_MARKET via place_sl() / place_tp()
      - TP/SL sobreviven reinicios del bot (persisten en el exchange)
      - NUNCA abre una posición sin SL y TP válidos
      - FIX: si el SL no se coloca tras todos los reintentos → cierra la posición
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
        self._clients: dict[str, BingXClient] = {}

    async def _get_client(self, symbol: str) -> BingXClient:
        if symbol not in self._clients:
            self._clients[symbol] = await BingXClient.create(symbol)
        return self._clients[symbol]

    # ── UTILIDADES ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _round_qty(qty: float, sz_decimals: int) -> float:
        if sz_decimals <= 0:
            return float(math.floor(qty))
        factor = 10 ** sz_decimals
        return float(math.floor(qty * factor) / factor)

    async def _place_market_with_retry(
        self,
        client: BingXClient,
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
                result = await asyncio.to_thread(
                    client.place_market, is_buy, qty, reduce_only, ref_price
                )
                if _okx_ok(result):
                    return {"status": "ok", "response": result}
                err = _okx_error_msg(result)
                if any(s in err.lower() for s in _RATE_LIMIT_SUBSTRS):
                    last_exc = Exception(err)
                    continue
                return {"status": "error", "response": err}
            except Exception as e:
                last_exc = e
                if any(s in str(e).lower() for s in _RATE_LIMIT_SUBSTRS):
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return {"status": "error", "response": "unknown"}

    # ── EXECUTE PRINCIPAL ────────────────────────────────────────────────────────

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
        client = await self._get_client(sym)
        rec    = TradeRecord(symbol=sym, side=side, qty=qty, arrival_price=arrival_price)
        rec._t0 = time.monotonic()

        is_buy = side in ("buy", "long")

        # ── GUARDIA DURA: NUNCA abrir sin SL y TP ───────────────────────────────
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
                cancelled = await asyncio.to_thread(client.cancel_all_open_tpsl)
                if cancelled:
                    logger.info("[%s] 🗑️ %d SL/TP canceladas antes del cierre.", sym, len(cancelled))
            except Exception as e:
                logger.warning("[%s] No se pudieron cancelar SL/TP: %s", sym, e)

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
            limit_result, filled = await self._try_limit(
                client, is_buy, qty, limit_price, rec
            )

            if filled:
                result              = limit_result
                entry_ok            = True
                rec.order_type_used = "limit"
                rec.fill_price      = limit_price
            else:
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

        # ── Colocar SL + TP tras entrada confirmada ──────────────────────────────
        if entry_ok and not reduce_only and trade_side == "open":
            # FIX: reset _protection_ok al abrir para que _ensure_tpsl
            # no omita la verificación de la nueva posición
            trader._protection_ok = False

            effective_entry_px = entry_px if (entry_px and entry_px > 0) else arrival_price
            sl_placed = await self._place_tpsl(
                client, is_buy, qty, sl, tp, sym, effective_entry_px
            )

            # FIX CRITICO: si el SL no se pudo colocar, cerrar la posición
            # inmediatamente para no quedar expuestos sin protección
            if not sl_placed:
                logger.error(
                    "[%s] 🚨 SL NO colocado — cerrando posición para evitar exposición sin stop loss",
                    sym,
                )
                try:
                    close_fn = getattr(trader, "close_position", None) or getattr(trader, "_close_position", None)
                    if callable(close_fn):
                        await close_fn(reason="NO_SL")
                    else:
                        # Fallback directo al exchange
                        await self._place_market_with_retry(
                            client, not is_buy, qty, True, arrival_price, sym
                        )
                    logger.warning("[%s] Posición cerrada preventivamente por falta de SL.", sym)
                    result = {"status": "error", "response": "position closed: SL placement failed"}
                except Exception as close_exc:
                    logger.critical(
                        "[%s] ❌❌ FALLO CRITICO: no se pudo colocar SL NI cerrar la posición: %s",
                        sym, close_exc,
                    )
                    result = {"status": "error", "response": f"CRITICAL: no SL and close failed: {close_exc}"}

                # Notificar por Telegram
                try:
                    from bot.telegram_bot import send_message
                    await send_message(
                        f"🚨 *ALERTA CRÍTICA* `{sym}`\n"
                        f"SL no pudo colocarse en BingX tras {self.tpsl_retry_attempts} intentos.\n"
                        f"Posición cerrada preventivamente."
                    )
                except Exception:
                    pass

                self._finalize_rec(rec, result, side, arrival_price)
                return result

            # SL colocado correctamente
            trader._protection_ok = True

        self._finalize_rec(rec, result, side, arrival_price)
        return result

    # ── SL + TP ───────────────────────────────────────────────────────────────────────

    async def _place_tpsl(
        self,
        client: BingXClient,
        is_buy: bool,
        qty: float,
        sl: Optional[float],
        tp: Optional[float],
        sym: str,
        entry_px: Optional[float] = None,
    ) -> bool:
        """
        Coloca SL y TP en BingX.
        Devuelve True si el SL fue colocado con éxito (requisito crítico),
        False si falló tras todos los reintentos.
        """
        sl_placed = False
        tp_placed = False

        # ── SL ─────────────────────────────────────────────────────────────────────
        if sl is not None and sl > 0:
            for attempt in range(self.tpsl_retry_attempts):
                if attempt > 0:
                    delay = self.tpsl_retry_base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "[%s] SL retry %d/%d — esperando %.1fs",
                        sym, attempt + 1, self.tpsl_retry_attempts, delay,
                    )
                    await asyncio.sleep(delay)
                try:
                    result = await asyncio.to_thread(
                        client.place_sl,
                        is_buy=not is_buy,
                        sz=qty,
                        trigger_px=float(sl),
                        entry_px=entry_px,
                    )
                    if _okx_ok(result):
                        algo_id = _okx_algo_id(result)
                        logger.info("[%s] ✅ SL=%.5f colocado en BingX (orderId=%s)", sym, sl, algo_id)
                        sl_placed = True
                        break
                    err = _okx_error_msg(result)
                    if any(s in err.lower() for s in _RATE_LIMIT_SUBSTRS):
                        logger.warning("[%s] SL rate-limited: %s", sym, err)
                        continue  # reintentar
                    # Error definitivo (precio inválido, qty, etc.) — no reintentar
                    logger.error("[%s] ❌ SL rechazado por BingX (definitivo): %s", sym, err)
                    break
                except Exception as e:
                    if any(s in str(e).lower() for s in _RATE_LIMIT_SUBSTRS):
                        logger.warning("[%s] SL rate-limit exception: %s", sym, e)
                        continue  # reintentar
                    logger.error("[%s] ❌ SL exception (definitiva): %s", sym, e)
                    break

            if not sl_placed:
                logger.error(
                    "[%s] 🚨 SL NO colocado tras %d intentos — SE CERRARÁ LA POSICIÓN",
                    sym, self.tpsl_retry_attempts,
                )
                # Devolvemos False para que execute() gestione el cierre
                return False

        # ── TP (no crítico: se loguea si falla pero no cierra la posición) ─────────
        if tp is not None and tp > 0:
            tp_limit = tp if self.tp_as_limit else None

            for attempt in range(self.tpsl_retry_attempts):
                if attempt > 0:
                    delay = self.tpsl_retry_base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "[%s] TP retry %d/%d — esperando %.1fs",
                        sym, attempt + 1, self.tpsl_retry_attempts, delay,
                    )
                    await asyncio.sleep(delay)
                try:
                    result = await asyncio.to_thread(
                        client.place_tp,
                        is_buy=not is_buy,
                        sz=qty,
                        trigger_px=float(tp),
                        limit_px=tp_limit,
                        entry_px=entry_px,
                    )
                    if _okx_ok(result):
                        algo_id = _okx_algo_id(result)
                        logger.info("[%s] ✅ TP=%.5f colocado en BingX (orderId=%s)", sym, tp, algo_id)
                        tp_placed = True
                        break
                    err = _okx_error_msg(result)
                    if any(s in err.lower() for s in _RATE_LIMIT_SUBSTRS):
                        logger.warning("[%s] TP rate-limited: %s", sym, err)
                        continue
                    logger.error("[%s] ❌ TP rechazado por BingX (definitivo): %s", sym, err)
                    break
                except Exception as e:
                    if any(s in str(e).lower() for s in _RATE_LIMIT_SUBSTRS):
                        logger.warning("[%s] TP rate-limit exception: %s", sym, e)
                        continue
                    logger.error("[%s] ❌ TP exception (definitiva): %s", sym, e)
                    break

            if not tp_placed:
                logger.error(
                    "[%s] ⚠️ TP NO colocado tras %d intentos — SL activo, posición protegida.",
                    sym, self.tpsl_retry_attempts,
                )

        return sl_placed

    # ── LIMIT INTERNO ────────────────────────────────────────────────────────────────────

    async def _try_limit(
        self,
        client: BingXClient,
        is_buy: bool,
        qty: float,
        price: float,
        rec: TradeRecord,
    ) -> tuple[dict, bool]:
        try:
            raw = await asyncio.to_thread(
                client.place_limit, is_buy, qty, price, False
            )
        except Exception as e:
            rec.cancel_reason = str(e)
            return {"status": "error", "response": str(e)}, False

        if not _okx_ok(raw):
            err = _okx_error_msg(raw)
            rec.cancel_reason = f"limit_rejected:{err}"
            return {"status": "error", "response": err}, False

        order_id = _okx_order_id(raw)
        if order_id is None:
            logger.warning(
                "[%s] Limit: sin ordId en respuesta — asumiendo fill inmediato. resp=%s",
                rec.symbol, raw,
            )
            return {"status": "ok", "response": raw}, True

        deadline = time.monotonic() + self.limit_timeout_s
        filled   = False

        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            try:
                open_orders = await asyncio.to_thread(client.get_open_orders)
                still_open  = any(
                    str(o.get("ordId")) == order_id for o in open_orders
                )
                if not still_open:
                    filled = True
                    break
            except Exception:
                pass

        if not filled:
            try:
                await asyncio.to_thread(client.cancel_order, order_id)
            except Exception as e:
                logger.warning("[%s] Error cancelando limit ordId=%s: %s", rec.symbol, order_id, e)
            if not rec.cancel_reason:
                rec.cancel_reason = "timeout"

        return {"status": "ok" if filled else "error", "response": raw}, filled

    # ── TELEMETRÍA ────────────────────────────────────────────────────────────────────

    def _finalize_rec(self, rec: TradeRecord, result: dict, side: str, arrival_price: float) -> None:
        rec.fill_latency_ms = (time.monotonic() - rec._t0) * 1000
        rec.success         = result.get("status") == "ok"

        if rec.success and arrival_price > 0:
            if side in ("buy", "long"):
                rec.slippage_bps = (rec.fill_price - arrival_price) / arrival_price * 10_000
            else:
                rec.slippage_bps = (arrival_price - rec.fill_price) / arrival_price * 10_000

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
