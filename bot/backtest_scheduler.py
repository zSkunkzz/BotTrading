#!/usr/bin/env python3
"""
backtest_scheduler.py — Scheduler automático del backtester completo

Ejecuta el backtester cada N días y envía un informe a Telegram.
También expone `run_backtest_now()` para el comando /backtest.

Variables de entorno:
  BACKTEST_SCHED_ENABLED   → true/false (default: true)
  BACKTEST_SCHED_DAYS      → cada cuántos días ejecutar (default: 7)
  BACKTEST_SCHED_HOUR      → hora UTC de ejecución (default: 3)
  BACKTEST_SCHED_SYMBOLS   → lista separada por comas (opcional, fallback)
  BACKTEST_SCHED_TF        → timeframe a usar (default: 15m)
  BACKTEST_SCHED_HIST_DAYS → días de histórico (default: 90)
  BACKTEST_SCHED_LEVERAGE  → leverage para el backtest (default: 5)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Callable, Optional

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────
SCHED_ENABLED   = os.getenv("BACKTEST_SCHED_ENABLED",  "true").lower() == "true"
SCHED_DAYS      = int(os.getenv("BACKTEST_SCHED_DAYS",     "7"))
SCHED_HOUR      = int(os.getenv("BACKTEST_SCHED_HOUR",     "3"))
SCHED_TF        = os.getenv("BACKTEST_SCHED_TF",          "15m")
SCHED_HIST_DAYS = int(os.getenv("BACKTEST_SCHED_HIST_DAYS", "90"))
SCHED_LEVERAGE  = int(os.getenv("BACKTEST_SCHED_LEVERAGE",  "5"))

_SCHED_SYMBOLS_ENV: Optional[list[str]] = (
    [s.strip() for s in os.getenv("BACKTEST_SCHED_SYMBOLS", "").split(",") if s.strip()]
    or None
)

_get_active_symbols: Optional[Callable[[], list[str]]] = None


def set_active_symbols_fn(fn: Callable[[], list[str]]) -> None:
    """Registrar la función que devuelve los pares activos del bot en tiempo real."""
    global _get_active_symbols
    _get_active_symbols = fn
    log.info("[backtest_sched] Función de símbolos activos registrada.")


def _resolve_symbols(override: Optional[list[str]] = None) -> list[str]:
    """
    Orden de prioridad:
      1. override explícito (/backtest BTC/USDT:USDT)
      2. active_traders en tiempo real
      3. BACKTEST_SCHED_SYMBOLS del .env
      4. Fallback mínimo
    """
    if override:
        return override
    if _get_active_symbols is not None:
        live = _get_active_symbols()
        if live:
            log.info("[backtest_sched] Símbolos de active_traders: %s", live)
            return live
        log.warning("[backtest_sched] active_traders vacío — usando fallback")
    if _SCHED_SYMBOLS_ENV:
        return _SCHED_SYMBOLS_ENV
    fallback = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
    log.warning("[backtest_sched] Usando fallback mínimo: %s", fallback)
    return fallback


# ── Telegram helper ──────────────────────────────────────────────────
async def _send(notifier, text: str) -> None:
    MAX = 4000
    for i in range(0, len(text), MAX):
        await notifier.send(text[i:i + MAX])
        await asyncio.sleep(0.3)


# ── Formateador ──────────────────────────────────────────────────────────
def _fmt_result(r) -> str:
    """
    Adapta BacktestResult real de bot/backtester.py.
    Propiedades disponibles: .symbol .timeframe .n_days .n_trades
    .win_rate (%) .total_pnl .avg_pnl .max_dd .profit_factor .expectancy
    """
    if r.n_trades == 0:
        return f"*{r.symbol}*\n  Sin trades en el período"

    wr  = f"{r.win_rate:.1f}%"
    pnl = f"{r.total_pnl:+.2f}%"
    pf  = f"{r.profit_factor:.2f}" if r.profit_factor != float('inf') else "∞"
    dd  = f"{r.max_dd:.2f}%"
    exp = f"{r.expectancy:+.2f}%"

    return (
        f"*{r.symbol}* ({r.timeframe})\n"
        f"  Trades: {r.n_trades} · WR: {wr} · PnL: {pnl}\n"
        f"  PF: {pf} · MaxDD: {dd} · Exp: {exp}/trade"
    )


def _build_report(symbols, results, hist_days, tf, elapsed) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ok  = sum(1 for r in results if r is not None and not isinstance(r, Exception))
    err = len(results) - ok

    header = (
        f"\U0001f4c8 *Backtest — {now_str}*\n"
        f"Símbolos: {ok} OK"
        + (f" / {err} error" if err else "") +
        f" · TF: {tf} · Hist: {hist_days}d · {elapsed:.0f}s\n"
        f"{'\u2500' * 32}"
    )

    body_lines = []
    for sym, r in zip(symbols, results):
        if r is not None and not isinstance(r, Exception):
            body_lines.append(_fmt_result(r))
        elif isinstance(r, Exception):
            body_lines.append(f"*{sym}* — \u274c `{r}`")
        else:
            body_lines.append(f"*{sym}* — \u274c Error desconocido")

    footer = "\n\n_Generado automáticamente por BotTrading_"
    return f"{header}\n\n" + "\n\n".join(body_lines) + footer


# ── Runner principal ───────────────────────────────────────────────────────────
async def run_backtest_now(
    notifier,
    symbols:   Optional[list] = None,
    hist_days: Optional[int]  = None,
    tf:        Optional[str]  = None,
    leverage:  Optional[int]  = None,
) -> None:
    """
    Descarga OHLCV y ejecuta el backtest vectorizado para cada símbolo.
    Llama directamente a _fetch_full_history() + _run_backtest() de bot/backtester.py.
    """
    resolved = _resolve_symbols(symbols)
    hist_days = hist_days or SCHED_HIST_DAYS
    tf        = tf        or SCHED_TF
    leverage  = leverage  or SCHED_LEVERAGE

    await _send(notifier,
        f"\u23f3 *Backtest iniciado*\n"
        f"Símbolos ({len(resolved)}): {', '.join(resolved)}\n"
        f"TF: {tf} · Histórico: {hist_days}d · Leverage: {leverage}x\n"
        f"Esto puede tardar unos minutos..."
    )

    # Importación diferida — las funciones reales del backtester
    try:
        from bot.backtester import _fetch_full_history, _run_backtest
    except ImportError as e:
        log.error("[backtest_sched] No se pudo importar backtester: %s", e)
        await _send(notifier, f"\u274c *Backtest fallido* — error de importación:\n`{e}`")
        return

    api_key    = os.getenv("BITGET_API_KEY",    "")
    api_secret = os.getenv("BITGET_API_SECRET", "")
    passphrase = os.getenv("BITGET_PASSPHRASE", "")

    t0      = time.monotonic()
    results = []

    for sym in resolved:
        try:
            log.info("[backtest_sched] Descargando histórico %s %s %dd...", sym, tf, hist_days)
            df = await _fetch_full_history(
                symbol=sym, tf=tf, days=hist_days,
                api_key=api_key, api_secret=api_secret, passphrase=passphrase,
            )
            if df.empty:
                raise ValueError("DataFrame vacío — sin datos del exchange")

            log.info("[backtest_sched] Ejecutando backtest %s (%d velas)...", sym, len(df))
            r = await asyncio.to_thread(
                _run_backtest, df, sym, tf, hist_days, leverage
            )
            results.append(r)
            log.info("[backtest_sched] %s — %d trades, WR %.1f%%, PnL %+.2f%%",
                     sym, r.n_trades, r.win_rate, r.total_pnl)

        except Exception as e:
            log.error("[backtest_sched] Error en %s: %s", sym, e)
            results.append(e)

    elapsed = time.monotonic() - t0
    report  = _build_report(resolved, results, hist_days, tf, elapsed)
    await _send(notifier, report)
    log.info("[backtest_sched] Informe enviado (%.0fs)", elapsed)


# ── Scheduler loop ────────────────────────────────────────────────────────────
class BacktestScheduler:
    def __init__(self, notifier) -> None:
        self._notifier  = notifier
        self._last_run: float = 0.0
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if not SCHED_ENABLED:
            log.info("[backtest_sched] Scheduler desactivado (BACKTEST_SCHED_ENABLED=false)")
            return
        self._task = asyncio.create_task(self._loop(), name="backtest_scheduler")
        log.info(
            "[backtest_sched] Scheduler activo — cada %d días a las %02d:00 UTC",
            SCHED_DAYS, SCHED_HOUR,
        )

    def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(3600)
                now = datetime.now(timezone.utc)
                if now.hour != SCHED_HOUR:
                    continue
                elapsed_days = (time.time() - self._last_run) / 86400
                if elapsed_days < SCHED_DAYS:
                    continue
                self._last_run = time.time()
                log.info("[backtest_sched] \U0001f680 Lanzando backtest programado...")
                await run_backtest_now(self._notifier)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("[backtest_sched] Error en el scheduler: %s", e)
                await asyncio.sleep(60)


_scheduler: Optional[BacktestScheduler] = None


def get_scheduler(notifier) -> BacktestScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BacktestScheduler(notifier)
    return _scheduler
