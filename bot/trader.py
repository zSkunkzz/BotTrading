#!/usr/bin/env python3
"""
bot/trader.py — FuturesTrader: punto de entrada público para main.py.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable, Optional

from bot.core.hl_client import HLClient, _norm_coin
from bot.core.trading_loop import TradingLoop
from bot.state import save_position

logger = logging.getLogger("FuturesTrader")

_USE_TESTNET = os.getenv("HL_TESTNET", "").lower() in ("true", "1", "yes")
_API_URL = (
    "https://api.hyperliquid-testnet.xyz"
    if _USE_TESTNET
    else "https://api.hyperliquid.xyz"
)

# Cuántas velas pedir por timeframe
_OHLCV_BARS = int(os.getenv("BARS_NEEDED", "100"))

# Mapeo timeframe → intervalo en minutos para candle_snapshot
_TF_MINUTES = {
    "1m":  1,
    "3m":  3,
    "5m":  5,
    "15m": 15,
    "30m": 30,
    "1h":  60,
    "2h":  120,
    "4h":  240,
    "8h":  480,
    "1d":  1440,
}

# Intentos de confirmación de fill tras orden de mercado
# Reducido a 3 intentos x 2s = 6s máx (antes era 6x3s=18s)
_FILL_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "3"))
_FILL_DELAY   = float(os.getenv("POST_FILL_CONFIRM_DELAY", "2.0"))

# Desfase máximo permitido entre ref_price y signal.entry antes de cancelar.
# Aumentado de 1.5% a 3.0% — en cryptos volátiles el precio puede moverse
# 1-2% entre generación del signal y ejecución de la orden.
# Configurable vía variable de entorno MAX_ENTRY_DRIFT_PCT (default: 3.0)
_MAX_ENTRY_DRIFT_PCT = float(os.getenv("MAX_ENTRY_DRIFT_PCT", "3.0")) / 100.0


def _check_price_staleness(
    signal: dict,
    ref_price: float,
    is_long: bool,
) -> Optional[str]:
    """
    Comprueba si el precio actual (ref_price) se ha alejado demasiado del
    entry calculado por el signal_engine.

    Retorna:
      None        → precio aceptable, puede continuar
      str (motivo) → desfase excesivo, abortar la entrada

    Lógica:
      - Si entry del signal es 0 (no calculado), se omite el check.
      - drift = (ref_price - entry) / entry
      - Para LONG: drift > +threshold  → precio subió demasiado (entrada cara)
                   drift < -threshold  → precio cayó demasiado (setup roto)
      - Para SHORT: drift < -threshold → precio bajó demasiado (entrada cara)
                    drift > +threshold → precio subió demasiado (setup roto)
      - |drift| > 2*threshold          → cancelar siempre (mercado volátil)
    """
    entry_signal = float(signal.get("entry") or 0)
    if entry_signal <= 0:
        return None  # sin referencia, no podemos hacer el check

    drift = (ref_price - entry_signal) / entry_signal
    abs_drift = abs(drift)
    threshold = _MAX_ENTRY_DRIFT_PCT

    # Siempre cancelar si el desfase absoluto es enorme (> 2x threshold)
    if abs_drift > threshold * 2:
        return (
            f"⚠️ Precio actual ({ref_price:.4f}) se alejó {drift*100:+.2f}% del entry del signal "
            f"({entry_signal:.4f}) — supera el límite absoluto de ±{threshold*200:.1f}% — entrada cancelada"
        )

    if abs_drift <= threshold:
        return None  # dentro del margen aceptable

    # Desfase entre threshold y 2*threshold: revisar dirección
    if is_long:
        if drift > 0:
            return (
                f"⏫ [LONG] Precio actual ({ref_price:.4f}) subió {drift*100:+.2f}% sobre entry del signal "
                f"({entry_signal:.4f}) — entrada demasiado cara, cancelada "
                f"(límite: +{threshold*100:.1f}%)"
            )
        else:
            return (
                f"⏪ [LONG] Precio actual ({ref_price:.4f}) cayó {drift*100:+.2f}% bajo entry del signal "
                f"({entry_signal:.4f}) — setup roto (precio en caída), cancelado "
                f"(límite: -{threshold*100:.1f}%)"
            )
    else:  # SHORT
        if drift < 0:
            return (
                f"⏪ [SHORT] Precio actual ({ref_price:.4f}) bajó {drift*100:+.2f}% bajo entry del signal "
                f"({entry_signal:.4f}) — entrada demasiado barata/cara para short, cancelada "
                f"(límite: -{threshold*100:.1f}%)"
            )
        else:
            return (
                f"⏫ [SHORT] Precio actual ({ref_price:.4f}) subió {drift*100:+.2f}% sobre entry del signal "
                f"({entry_signal:.4f}) — setup roto (precio en subida), cancelado "
                f"(límite: +{threshold*100:.1f}%)"
            )


def _adjust_levels_to_fill(
    signal: dict,
    filled_price: float,
    ref_price: float,
) -> tuple[float, float, float]:
    """
    Re-escala SL, TP1 y TP2 del signal para que mantengan los mismos
    offsets porcentuales relativos al precio real de fill.
    """
    sl_px  = float(signal.get("sl")  or 0)
    tp1_px = float(signal.get("tp1") or 0)
    tp2_px = float(signal.get("tp2") or 0)

    base = float(signal.get("entry") or 0)
    if base <= 0:
        base = ref_price

    # Si no hay desfase relevante (< 0.05%) no tocar nada
    if abs(filled_price - base) / base < 0.0005:
        return sl_px, tp1_px, tp2_px

    def _rescale(level: float) -> float:
        if level <= 0:
            return level
        pct = (level - base) / base
        return filled_price * (1.0 + pct)

    sl_adj  = _rescale(sl_px)
    tp1_adj = _rescale(tp1_px)
    tp2_adj = _rescale(tp2_px)

    logger.info(
        "Ajuste SL/TP por desfase de fill: base=%.4f → filled=%.4f (%.2f%%) | "
        "SL %.4f→%.4f | TP1 %.4f→%.4f | TP2 %.4f→%.4f",
        base, filled_price, (filled_price - base) / base * 100,
        sl_px, sl_adj, tp1_px, tp1_adj, tp2_px, tp2_adj,
    )
    return sl_adj, tp1_adj, tp2_adj


class FuturesTrader:
    """
    Orquestador principal de un par de trading en Hyperliquid.
    """

    def __init__(
        self,
        api_key: Optional[str],
        api_secret: str,
        passphrase: Optional[str],
        symbol: str,
        leverage: int = 5,
        margin_mode: str = "isolated",
        dry_run: bool = True,
    ) -> None:
        self.symbol      = symbol
        self.coin        = _norm_coin(symbol)
        self.leverage    = leverage
        self.margin_mode = margin_mode
        self.dry_run     = dry_run

        self.position:        Optional[str]   = None
        self.entry_price:     Optional[float] = None
        self.sl:              Optional[float] = None
        self.tp1:             Optional[float] = None
        self.tp2:             Optional[float] = None
        self.tp3:             Optional[float] = None
        self.tp2_hit:         bool            = False
        self._open_notional:  float           = 0.0
        self._open_leverage:  int             = leverage
        self._open_qty:       float           = 0.0
        self._protection_ok:  bool            = False
        self._tp1_be_done:    bool            = False
        self._last_price:     float           = 0.0

        self._api_key    = api_key or ""
        self._api_secret = api_secret or ""

        self._hl_client = HLClient(symbol)

        self._master_addr = self._hl_client._account_addr
        self._agent_mode  = self._hl_client._agent_mode

        self._stopped_event = asyncio.Event()
        self._trading_loop  = TradingLoop(symbol)
        self._ccxt_exchange  = None

    # ── Interfaz pública requerida por main.py ──────────────────────

    async def run(self, risk, *, global_risk=None) -> None:
        try:
            await self._trading_loop.run(self, risk, global_risk=global_risk)
        except asyncio.CancelledError:
            logger.info("[%s] FuturesTrader cancelado.", self.symbol)
        finally:
            self._stopped_event.set()

    async def cleanup(self) -> None:
        try:
            from bot.ai_trader import close_sessions
            await close_sessions()
        except Exception as e:
            logger.debug("[%s] cleanup ai_trader sessions: %s", self.symbol, e)
        self._stopped_event.set()

    # ── Métodos que TradingLoop llama sobre el objeto trader ──────────

    async def _get_ccxt(self) -> None:
        pass

    async def get_price(self) -> float:
        import aiohttp
        import json as _json

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                data = _json.loads(await resp.text())

        price = data.get(self.coin)
        if price is None:
            raise ValueError(f"[{self.symbol}] Precio no encontrado en allMids")
        return float(price)

    async def get_ohlcv(self, timeframe: str) -> list:
        import aiohttp
        import json as _json
        import time as _time

        interval = _TF_MINUTES.get(timeframe, 15)
        n = _OHLCV_BARS + 20
        end_ms   = int(_time.time() * 1000)
        start_ms = end_ms - (n * interval * 60 * 1000)

        payload = {
            "type":      "candleSnapshot",
            "req": {
                "coin":       self.coin,
                "interval":   timeframe,
                "startTime":  start_ms,
                "endTime":    end_ms,
            },
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{_API_URL}/info",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    raw = _json.loads(await resp.text())
        except Exception as e:
            logger.warning("[%s] get_ohlcv(%s) error: %s", self.symbol, timeframe, e)
            return []

        if not isinstance(raw, list):
            logger.warning("[%s] get_ohlcv(%s) respuesta inesperada: %s", self.symbol, timeframe, raw)
            return []

        bars = []
        for candle in raw:
            try:
                bars.append([
                    int(candle["t"]),
                    float(candle["o"]),
                    float(candle["h"]),
                    float(candle["l"]),
                    float(candle["c"]),
                    float(candle["v"]),
                ])
            except (KeyError, TypeError, ValueError):
                continue

        logger.debug(
            "[%s] get_ohlcv(%s): %d velas descargadas",
            self.symbol, timeframe, len(bars),
        )
        return bars

    def get_ohlcv_fn(self) -> Callable:
        async def _fn(tf: str) -> list:
            return await self.get_ohlcv(tf)
        return _fn

    async def _get_positions(self) -> list[dict]:
        import aiohttp
        import json as _json

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_API_URL}/info",
                json={"type": "clearinghouseState", "user": self._master_addr},
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = _json.loads(await resp.text())

        result = []
        for p in data.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("coin", "").upper() != self.coin.upper():
                continue
            try:
                szi = float(pos.get("szi", 0) or 0)
            except (TypeError, ValueError):
                continue
            if abs(szi) == 0:
                continue
            result.append({
                "side":    "long" if szi > 0 else "short",
                "size":    abs(szi),
                "entryPx": float(pos.get("entryPx") or 0),
                "coin":    pos.get("coin", ""),
            })
        return result

    async def _get_open_orders_raw(self) -> list[dict]:
        import aiohttp
        import json as _json

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{_API_URL}/info",
                    json={"type": "openOrders", "user": self._master_addr},
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = _json.loads(await resp.text())
        except Exception as e:
            logger.warning("[%s] _get_open_orders_raw error: %s", self.symbol, e)
            return []

        if not isinstance(data, list):
            logger.warning("[%s] _get_open_orders_raw respuesta inesperada: %s", self.symbol, type(data))
            return []

        return data

    async def _place_tpsl(
        self,
        qty: float,
        sl_price: Optional[float],
        tp_price: Optional[float],
        is_long: bool,
        reduce_only: bool = True,
    ) -> None:
        if self.dry_run:
            logger.info(
                "[%s] DRY_RUN: _place_tpsl sl=%.4f tp=%.4f omitido.",
                self.symbol, sl_price or 0, tp_price or 0,
            )
            return

        if sl_price and sl_price > 0:
            result = self._hl_client.place_sl(
                is_buy=not is_long,
                sz=qty,
                trigger_px=sl_price,
                entry_px=self.entry_price or sl_price,
            )
            logger.info("[%s] _place_tpsl SL=%.4f: %s", self.symbol, sl_price, result)

        if tp_price and tp_price > 0:
            result = self._hl_client.place_tp(
                is_buy=not is_long,
                sz=qty,
                trigger_px=tp_price,
                entry_px=self.entry_price or tp_price,
            )
            logger.info("[%s] _place_tpsl TP=%.4f: %s", self.symbol, tp_price, result)

    def _round_qty(self, qty: float) -> float:
        return self._hl_client.round_sz(qty)

    async def _set_leverage(self, leverage: int) -> None:
        if self.dry_run:
            logger.info("[%s] DRY_RUN: _set_leverage(%d) omitido.", self.symbol, leverage)
            return
        try:
            result = self._hl_client._exchange.update_leverage(
                leverage, self.coin, is_cross=False
            )
            logger.info("[%s] Leverage configurado a %dx: %s", self.symbol, leverage, result)
        except Exception as e:
            logger.warning("[%s] No se pudo configurar leverage: %s", self.symbol, e)

    def _info_post(self, payload: dict) -> dict:
        return self._hl_client._info._session.post(
            f"{_API_URL}/info", json=payload
        ).json()

    # ── open_order: entrada al mercado + SL + TP ────────────────────

    async def open_order(self, signal: dict, risk) -> None:
        """
        Abre una posición en el mercado con SL y TP1 opcionales.

        PROTECCIÓN DE ENTRADA:
          Antes de enviar la orden, comprueba que el precio actual (ref_price)
          no se haya alejado más de MAX_ENTRY_DRIFT_PCT (default 3.0%) del
          entry calculado por el signal_engine. Si se superó ese umbral, la
          entrada se cancela completamente — el setup ya no es válido.

          Tras el fill, SL/TP se re-escalan automáticamente al precio real
          de fill para preservar los offsets porcentuales correctos.
        """
        if self.position is not None:
            logger.info("[%s] open_order ignorado — ya hay posición abierta (%s).", self.symbol, self.position)
            return

        action = signal.get("action", "").upper()
        side   = signal.get("side", "").lower()

        is_long = (action == "BUY" or side == "long")
        is_buy  = is_long

        usdc_per_trade = float(getattr(risk, "usdc_per_trade", 20.0))
        notional       = usdc_per_trade * self.leverage

        # Obtener precio actual
        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio — abortando. %s", self.symbol, e)
            return

        if ref_price <= 0:
            logger.error("[%s] open_order: precio inválido (%s) — abortando.", self.symbol, ref_price)
            return

        # ── CHECK DE DESFASE: cancelar si precio actual se alejó demasiado ───
        stale_reason = _check_price_staleness(signal, ref_price, is_long)
        if stale_reason:
            logger.warning("[%s] open_order: ENTRADA CANCELADA — %s", self.symbol, stale_reason)
            return

        qty = notional / ref_price
        qty = self._hl_client.round_sz(qty)

        if qty <= 0:
            logger.error("[%s] open_order: qty calculada = 0 (notional=%.2f ref_price=%.4f) — abortando.",
                         self.symbol, notional, ref_price)
            return

        logger.info(
            "[%s] open_order: %s | qty=%.6f | ref_price=%.4f | notional=%.2f USDC | lev=%dx | "
            "entry_signal=%.4f | sl_signal=%.4f | tp1_signal=%.4f | drift=%.2f%%",
            self.symbol, "LONG" if is_long else "SHORT",
            qty, ref_price, notional, self.leverage,
            float(signal.get("entry") or 0),
            float(signal.get("sl") or 0),
            float(signal.get("tp1") or 0),
            (ref_price - float(signal.get("entry") or ref_price)) / float(signal.get("entry") or ref_price) * 100,
        )

        if self.dry_run:
            sl_px, tp1_px, tp2_px = _adjust_levels_to_fill(signal, ref_price, ref_price)
            tp3_px = float(signal.get("tp3") or 0)

            logger.info("[%s] DRY_RUN: open_order simulado — sin orden real.", self.symbol)
            self.position    = "long" if is_long else "short"
            self.entry_price = ref_price
            self.sl          = sl_px  if sl_px  > 0 else None
            self.tp1         = tp1_px if tp1_px > 0 else None
            self.tp2         = tp2_px if tp2_px > 0 else None
            self.tp3         = tp3_px if tp3_px > 0 else None
            self._open_notional = notional
            self._open_leverage = self.leverage
            self._open_qty      = qty
            self._protection_ok = (sl_px > 0)
            return

        # ── Orden de mercado ────────────────────────────────────────────
        try:
            result = self._hl_client.place_market(
                is_buy=is_buy,
                sz=qty,
                reduce_only=False,
                ref_price=ref_price,
            )
            logger.info("[%s] Orden de mercado enviada: %s", self.symbol, result)
        except Exception as e:
            logger.error("[%s] open_order: error al enviar orden de mercado: %s", self.symbol, e)
            return

        status = (result or {}).get("status", "")
        if status not in ("ok", ""):
            logger.error("[%s] open_order: orden rechazada por exchange: %s", self.symbol, result)
            return

        # ── Esperar fill y obtener precio real de entrada ──────────────
        filled_price = ref_price
        for attempt in range(_FILL_RETRIES):
            await asyncio.sleep(_FILL_DELAY)
            try:
                positions = await self._get_positions()
                if positions:
                    filled_price = positions[0].get("entryPx", ref_price)
                    logger.info(
                        "[%s] Fill confirmado (intento %d/%d): entryPx=%.4f",
                        self.symbol, attempt + 1, _FILL_RETRIES, filled_price,
                    )
                    break
            except Exception as e:
                logger.warning("[%s] open_order: error confirmando fill: %s", self.symbol, e)
        else:
            logger.warning("[%s] open_order: fill no confirmado tras %d intentos — usando ref_price=%.4f",
                           self.symbol, _FILL_RETRIES, ref_price)

        # ── Re-escalar SL/TP al precio real de fill ────────────────────
        sl_px, tp1_px, tp2_px = _adjust_levels_to_fill(signal, filled_price, ref_price)

        # Fix: re-escalar tp3 usando su propio key, no sobreescribiendo tp2
        tp3_raw = float(signal.get("tp3") or 0)
        tp3_px = 0.0
        if tp3_raw > 0:
            base = float(signal.get("entry") or ref_price)
            if base > 0 and abs(filled_price - base) / base >= 0.0005:
                pct = (tp3_raw - base) / base
                tp3_px = filled_price * (1.0 + pct)
            else:
                tp3_px = tp3_raw

        # ── Actualizar estado interno ───────────────────────────────
        self.position    = "long" if is_long else "short"
        self.entry_price = filled_price
        self.sl          = sl_px  if sl_px  > 0 else None
        self.tp1         = tp1_px if tp1_px > 0 else None
        self.tp2         = tp2_px if tp2_px > 0 else None
        self.tp3         = tp3_px if tp3_px > 0 else None
        self._open_notional = notional
        self._open_leverage = self.leverage
        self._open_qty      = qty
        self._protection_ok = False
        self._tp1_be_done   = False

        # ── Colocar SL ────────────────────────────────────────────
        if sl_px and sl_px > 0:
            try:
                sl_result = self._hl_client.place_sl(
                    is_buy=not is_buy,
                    sz=qty,
                    trigger_px=sl_px,
                    entry_px=filled_price,
                )
                logger.info("[%s] SL colocado en %.4f: %s", self.symbol, sl_px, sl_result)
                self._protection_ok = True
            except Exception as e:
                logger.error("[%s] open_order: error colocando SL: %s", self.symbol, e)

        # ── Colocar TP1 ────────────────────────────────────────────
        if tp1_px and tp1_px > 0:
            try:
                tp_result = self._hl_client.place_tp(
                    is_buy=not is_buy,
                    sz=qty,
                    trigger_px=tp1_px,
                    entry_px=filled_price,
                )
                logger.info("[%s] TP1 colocado en %.4f: %s", self.symbol, tp1_px, tp_result)
            except Exception as e:
                logger.error("[%s] open_order: error colocando TP1: %s", self.symbol, e)

        # ── Persistir estado ────────────────────────────────────────
        try:
            save_position(self.symbol, {
                "side":        self.position,
                "entry":       self.entry_price,
                "sl":          self.sl,
                "tp1":         self.tp1,
                "tp2":         self.tp2,
                "tp3":         self.tp3,
                "tp2_hit":     self.tp2_hit,
                "usdc_amount": usdc_per_trade,
                "leverage":    self.leverage,
                "qty":         self._open_qty,
            })
        except Exception as e:
            logger.warning("[%s] open_order: no se pudo persistir estado: %s", self.symbol, e)

        logger.info(
            "[%s] ✅ Posición abierta: %s @ %.4f | SL=%.4f | TP1=%.4f",
            self.symbol,
            self.position.upper(),
            self.entry_price,
            self.sl or 0,
            self.tp1 or 0,
        )


__all__ = ["FuturesTrader"]
