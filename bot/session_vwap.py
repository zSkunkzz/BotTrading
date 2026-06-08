"""session_vwap.py — VWAP anclado al inicio de sesión UTC.

Problema con el VWAP acumulado clásico:
  sum(close * vol) / sum(vol) sobre TODAS las velas del buffer depende del
  número de velas cargadas, no del inicio real de la sesión. Si el bot
  carga 100 velas de 15m (~25h), el VWAP acumula precios de ayer, haciendo
  que el nivel sea distinto según cuándo arrancó el bot.

Solución — VWAP de sesión:
  Ancla el cálculo al open de la sesión actual (día UTC por defecto).
  Solo acumula las velas desde ese timestamp. Esto da el mismo valor
  que verías en TradingView con "Anchor: Session".

Config Railway:
  VWAP_SESSION_TF       → '1D' | '8H' | '4H'  (default '1D')
  VWAP_SESSION_FALLBACK → 'true' | 'false'      (default 'true')
    Si true y el buffer no cubre el open de sesión, hace fallback al
    acumulado clásico en lugar de devolver 0.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

log = logging.getLogger(__name__)

_SESSION_TF       = os.getenv("VWAP_SESSION_TF",       "1D").upper()
_SESSION_FALLBACK = os.getenv("VWAP_SESSION_FALLBACK", "true").lower() not in ("false", "0", "no")

_TF_TO_SECONDS: dict[str, int] = {
    "1D": 86_400,
    "8H": 28_800,
    "4H": 14_400,
    "2H":  7_200,
    "1H":  3_600,
}


def _session_open_ms(ref_ts_ms: int, tf: str = _SESSION_TF) -> int:
    """Devuelve el timestamp Unix en ms del inicio de la sesión que contiene ref_ts_ms."""
    period_s = _TF_TO_SECONDS.get(tf, 86_400)
    ref_s = ref_ts_ms // 1_000
    open_s = (ref_s // period_s) * period_s
    return open_s * 1_000


def compute_session_vwap(
    bars: list,
    session_open_ts_ms: Optional[int] = None,
    tf: str = _SESSION_TF,
) -> float:
    """
    Calcula el VWAP anclado al inicio de sesión.

    Args:
        bars: lista de velas OHLCV [ts, open, high, low, close, volume].
              Acepta tanto listas como dicts (mismo formato que signal_engine).
        session_open_ts_ms: timestamp ms del inicio de sesión.
              Si None, se deriva de la última vela disponible.
        tf: granularidad del periodo de sesión.

    Returns:
        float: VWAP de sesión, o 0.0 si no hay datos suficientes.
    """
    if not bars:
        return 0.0

    def _ts(b) -> int:
        return int(b["timestamp"] if isinstance(b, dict) else b[0])

    def _high(b) -> float:
        return float(b["high"] if isinstance(b, dict) else b[2])

    def _low(b) -> float:
        return float(b["low"] if isinstance(b, dict) else b[3])

    def _close(b) -> float:
        return float(b["close"] if isinstance(b, dict) else b[4])

    def _vol(b) -> float:
        return float(b["volume"] if isinstance(b, dict) else b[5])

    # Determinar el inicio de sesión
    if session_open_ts_ms is None:
        last_ts = _ts(bars[-1])
        session_open_ts_ms = _session_open_ms(last_ts, tf)

    # Filtrar velas de la sesión actual (excluir vela activa parcial = last bar)
    session_bars = [
        b for b in bars[:-1]  # velas cerradas únicamente
        if _ts(b) >= session_open_ts_ms
    ]

    if not session_bars:
        if _SESSION_FALLBACK:
            # Fallback: acumulado clásico sobre velas cerradas
            closed = bars[:-1]
            if not closed:
                return 0.0
            cum_pv  = sum((_high(b) + _low(b) + _close(b)) / 3.0 * _vol(b) for b in closed)
            cum_vol = sum(_vol(b) for b in closed)
            vwap_fb = round(cum_pv / cum_vol, 8) if cum_vol > 0 else 0.0
            log.debug(
                "[VWAP-FALLBACK] session_open=%s — sin velas en sesión, "
                "usando acumulado clásico (%d velas) → VWAP=%.6f",
                datetime.fromtimestamp(session_open_ts_ms / 1000, tz=timezone.utc).strftime("%H:%M UTC"),
                len(closed),
                vwap_fb,
            )
            return vwap_fb
        log.debug("[VWAP-SESSION] sin velas desde session_open — devolviendo 0")
        return 0.0

    # VWAP = Σ(typical_price × volume) / Σ(volume)
    # typical_price = (high + low + close) / 3  ← estándar institucional
    cum_pv  = sum((_high(b) + _low(b) + _close(b)) / 3.0 * _vol(b) for b in session_bars)
    cum_vol = sum(_vol(b) for b in session_bars)

    if cum_vol <= 0:
        return 0.0

    vwap = round(cum_pv / cum_vol, 8)
    log.debug(
        "[VWAP-SESSION] ancla=%s — %d velas en sesión → VWAP=%.6f",
        datetime.fromtimestamp(session_open_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        len(session_bars),
        vwap,
    )
    return vwap
