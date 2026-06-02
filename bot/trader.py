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
_FILL_RETRIES = int(os.getenv("POST_FILL_CONFIRM_RETRIES", "6"))
_FILL_DELAY   = float(os.getenv("POST_FILL_CONFIRM_DELAY", "3.0"))


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
        self._open_qty:       float           = 0.0   # qty en unidades del activo
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
        """
        Descarga velas OHLCV desde Hyperliquid /info candle_snapshot.
        Devuelve lista de [timestamp, open, high, low, close, volume] compatible
        con signal_engine._compute_indicators().
        """
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
        """
        Devuelve un callable async compatible con analyze_pair(ohlcv_fn=...).
        El signal_engine llama a ohlcv_fn(timeframe) para cada TF.
        """
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
        """
        Obtiene las órdenes abiertas del exchange para esta cuenta.
        Devuelve lista de dicts con la estructura nativa de Hyperliquid.
        Requerido por position_manager._ensure_tpsl.
        """
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
        """
        Coloca una orden de SL o TP en el exchange.
        Requerido por position_manager._place_emergency_sl_tp.
        Si sl_price es None se coloca solo TP, y viceversa.
        """
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
        """
        Redondea qty al número de decimales que acepta el exchange.
        Requerido por position_manager._round_qty_safe.
        """
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

        signal keys esperados (producidos por signal_engine / decision_engine):
          action      : "BUY" | "SELL"
          side        : "long" | "short"
          entry       : float  — precio de referencia (puede ser 0 → usar precio actual)
          sl          : float  — precio de stop loss  (0 → sin SL)
          tp1         : float  — precio de take profit 1 (0 → sin TP)
          tp2, tp3    : float  — TPs adicionales (opcionales)
          entry_mode  : str    — "EARLY" | "CONFIRMED" (informativo)
          score       : int    — score de la señal (informativo)

        risk keys esperados:
          usdc_per_trade : float — capital por operación en USDC
        """
        if self.position is not None:
            logger.info("[%s] open_order ignorado — ya hay posición abierta (%s).", self.symbol, self.position)
            return

        action = signal.get("action", "").upper()
        side   = signal.get("side", "").lower()
        sl_px  = float(signal.get("sl")  or 0)
        tp1_px = float(signal.get("tp1") or 0)
        tp2_px = float(signal.get("tp2") or 0)
        tp3_px = float(signal.get("tp3") or 0)

        is_long = (action == "BUY" or side == "long")
        is_buy  = is_long

        usdc_per_trade = float(getattr(risk, "usdc_per_trade", 20.0))
        notional       = usdc_per_trade * self.leverage

        # Obtener precio actual para calcular qty
        try:
            ref_price = await self.get_price()
        except Exception as e:
            logger.error("[%s] open_order: no se pudo obtener precio — abortando. %s", self.symbol, e)
            return

        if ref_price <= 0:
            logger.error("[%s] open_order: precio inválido (%s) — abortando.", self.symbol, ref_price)
            return

        qty = notional / ref_price
        qty = self._hl_client.round_sz(qty)

        if qty <= 0:
            logger.error("[%s] open_order: qty calculada = 0 (notional=%.2f ref_price=%.4f) — abortando.",
                         self.symbol, notional, ref_price)
            return

        logger.info(
            "[%s] open_order: %s | qty=%.6f | ref_price=%.4f | notional=%.2f USDC | lev=%dx | sl=%.4f | tp1=%.4f",
            self.symbol, "LONG" if is_long else "SHORT",
            qty, ref_price, notional, self.leverage,
            sl_px, tp1_px,
        )

        if self.dry_run:
            logger.info("[%s] DRY_RUN: open_order simulado — sin orden real.", self.symbol)
            self.position    = "long" if is_long else "short"
            self.entry_price = ref_price
            self.sl          = sl_px if sl_px > 0 else None
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

        # Verificar éxito básico del resultado
        status = (result or {}).get("status", "")
        if status not in ("ok", ""):
            logger.error("[%s] open_order: orden rechazada por exchange: %s", self.symbol, result)
            return

        # ── Esperar fill y obtener precio real de entrada ──────────────
        filled_price = ref_price  # fallback
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

        # ── Actualizar estado interno ───────────────────────────────
        self.position    = "long" if is_long else "short"
        self.entry_price = filled_price
        self.sl          = sl_px if sl_px > 0 else None
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
                    is_buy=not is_buy,   # opuesto: cierra la posición
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

        # ── Persistir estado (incluye qty para que el próximo restart la restaure) ─
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
                "qty":         self._open_qty,   # ← NUEVO: persiste qty en disco
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
