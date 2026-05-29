"""
microstructure.py — Filtros de microestructura de mercado.

Valida condiciones de mercado ANTES de generar una señal:
  · Spread bid/ask
  · Depth top-of-book (liquidez visible)
  · Imbalance L2 (presión compradora/vendedora)
  · Volatilidad intrabar (rango de la vela actual)
  · Funding rate bias (REST, no disponible en WS Bitget)

Variables de entorno (todas opcionales):
  MS_MAX_SPREAD_BPS         Spread máximo permitido (bps)          (default 25)
  MS_MIN_DEPTH_USDT         Profundidad mínima top-5 (USDT)        (default 5000)
  MS_MAX_IMBALANCE_BLOCK    Imbalance absoluto máximo para bloquear (default 0.85)
  MS_MAX_INTRABAR_RANGE_PCT Rango intrabar máximo (%)               (default 2.0)
  MS_MAX_FUNDING_BIAS       Funding rate máximo absoluto (%)        (default 0.05)
  MS_FUNDING_SIDE_BLOCK     Bloquear si funding confirma lado equivocado (1/0) (default 1)

Uso:
  from bot.microstructure import microstructure_filter
  ok, reason, metrics = await microstructure_filter.check(
      symbol="BTCUSDT", side="buy", bars=bars
  )
  if not ok:
      logger.info(f"Microestructura desfavorable: {reason}")
"""
from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger("Microstructure")


def _e(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


class MicrostructureFilter:
    def __init__(self) -> None:
        self.max_spread_bps          = _e("MS_MAX_SPREAD_BPS",          25.0)
        self.min_depth_usdt          = _e("MS_MIN_DEPTH_USDT",        5_000.0)
        self.max_imbalance_block     = _e("MS_MAX_IMBALANCE_BLOCK",      0.85)
        self.max_intrabar_range_pct  = _e("MS_MAX_INTRABAR_RANGE_PCT",   2.0)
        self.max_funding_bias        = _e("MS_MAX_FUNDING_BIAS",         0.05)
        self.funding_side_block      = bool(int(os.getenv("MS_FUNDING_SIDE_BLOCK", "1")))

    async def check(
        self,
        symbol:    str,
        side:      str,
        bars:      list | None = None,
        price:     float       = 0.0,
    ) -> tuple[bool, str, dict]:
        """
        Ejecuta todos los filtros de microestructura.
        Devuelve (pass, reason, metrics_dict).
        No es bloqueante: un fallo devuelve (False, motivo, métricas).
        """
        sym   = symbol.replace("/", "").replace(":USDT", "").replace("USDTUSDT", "USDT")
        metrics: dict = {"symbol": sym, "side": side}

        # ── 1. Orderbook: spread e imbalance ─────────────────────────────────
        ob = self._get_ob(sym)
        if ob:
            bid       = ob.get("bid", 0.0)
            ask       = ob.get("ask", 0.0)
            mid       = ob.get("mid", price or ((bid + ask) / 2 if bid and ask else 0))
            spread_bps = (ask - bid) / mid * 10_000 if mid > 0 else 0.0
            imbalance  = ob.get("imbalance", 0.0)
            bid_vol    = ob.get("bid_vol", 0.0)
            ask_vol    = ob.get("ask_vol", 0.0)
            depth_usdt = (bid_vol + ask_vol) * mid

            metrics.update({
                "spread_bps": round(spread_bps, 2),
                "imbalance":  round(imbalance, 4),
                "depth_usdt": round(depth_usdt, 0),
            })

            if spread_bps > self.max_spread_bps:
                return False, (
                    f"Spread {spread_bps:.1f} bps > límite {self.max_spread_bps:.0f} bps"
                ), metrics

            if depth_usdt > 0 and depth_usdt < self.min_depth_usdt:
                return False, (
                    f"Depth top-5 {depth_usdt:.0f} USDT < mínimo {self.min_depth_usdt:.0f} USDT"
                ), metrics

            # Imbalance: bloquear si la presión es opuesta y extrema
            if abs(imbalance) >= self.max_imbalance_block:
                bad_long  = side in ("buy",  "long")  and imbalance < -self.max_imbalance_block
                bad_short = side in ("sell", "short") and imbalance >  self.max_imbalance_block
                if bad_long or bad_short:
                    return False, (
                        f"Imbalance L2 {imbalance:+.2f} contrario a {side} (umbral ±{self.max_imbalance_block})"
                    ), metrics
        else:
            metrics["ob"] = "sin_datos"

        # ── 2. Volatilidad intrabar ──────────────────────────────────────────
        if bars and len(bars) >= 1:
            last = bars[-1]
            try:
                hi   = float(last[2])
                lo   = float(last[3])
                cl   = float(last[4])
                ref  = cl if cl > 0 else ((hi + lo) / 2)
                rang = (hi - lo) / ref * 100 if ref > 0 else 0.0
                metrics["intrabar_range_pct"] = round(rang, 3)
                if rang > self.max_intrabar_range_pct:
                    return False, (
                        f"Volatilidad intrabar {rang:.2f}% > {self.max_intrabar_range_pct:.1f}%"
                    ), metrics
            except (IndexError, ValueError, ZeroDivisionError):
                pass

        # ── 3. Funding rate bias (REST best-effort) ──────────────────────────
        funding = await self._get_funding_rate(sym)
        if funding is not None:
            metrics["funding_rate_pct"] = round(funding * 100, 5)
            if abs(funding * 100) > self.max_funding_bias and self.funding_side_block:
                bad_long  = side in ("buy",  "long")  and funding > 0
                bad_short = side in ("sell", "short") and funding < 0
                if bad_long or bad_short:
                    return False, (
                        f"Funding rate {funding*100:+.4f}% desfavorable para {side} "
                        f"(umbral ±{self.max_funding_bias:.3f}%)"
                    ), metrics

        metrics["pass"] = True
        return True, "OK", metrics

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _get_ob(self, sym: str) -> dict | None:
        try:
            from bot.ws_feed import ws_feed
            return ws_feed.get_orderbook_metrics(sym)
        except Exception:
            return None

    async def _get_funding_rate(self, sym: str) -> float | None:
        """
        Consulta el funding rate actual por REST.
        Bitget endpoint: GET /api/v2/mix/market/current-fund-rate
        Devuelve float (ej. 0.0001) o None si falla.
        """
        try:
            import aiohttp
            url    = "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
            params = f"?symbol={sym}&productType=USDT-FUTURES"
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    url + params,
                    timeout=aiohttp.ClientTimeout(total=4),
                ) as r:
                    data = await r.json(content_type=None)
                    if data.get("code") == "00000":
                        item = (data.get("data") or [None])[0]
                        if item:
                            return float(item.get("fundingRate") or 0)
        except Exception as e:
            logger.debug(f"[{sym}] funding rate REST error: {e}")
        return None


# Singleton global
microstructure_filter = MicrostructureFilter()
