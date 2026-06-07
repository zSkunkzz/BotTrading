#!/usr/bin/env python3
"""
backtest_scheduler.py — Scheduler automático del backtester completo

Ejecuta el backtester con optimizador walk-forward cada N días y envía
un informe detallado a Telegram.

También expone `run_backtest_now()` para que el bot de Telegram lo
lance manualmente via /backtest.

Variables de entorno:
  BACKTEST_SCHED_ENABLED   → true/false (default: true)
  BACKTEST_SCHED_DAYS      → cada cuántos días ejecutar (default: 7)
  BACKTEST_SCHED_HOUR      → hora UTC de ejecución (default: 3)
  BACKTEST_SCHED_SYMBOLS   → lista separada por comas (default: BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT)
  BACKTEST_SCHED_HIST_DAYS → días de histórico a usar (default: 90)
  BACKTEST_SCHED_OPTIMIZE  → true/false — activar optimizador walk-forward (default: false)
  BACKTEST_SCHED_FOLDS     → número de folds para el optimizador (default: 3)
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────────────────
SCHED_ENABLED   = os.getenv("BACKTEST_SCHED_ENABLED",  "true").lower() == "true"
SCHED_DAYS      = int(os.getenv("BACKTEST_SCHED_DAYS",     "7"))
SCHED_HOUR      = int(os.getenv("BACKTEST_SCHED_HOUR",     "3"))   # UTC
SCHED_SYMBOLS   = os.getenv("BACKTEST_SCHED_SYMBOLS",
                             "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT").split(",")
SCHED_HIST_DAYS = int(os.getenv("BACKTEST_SCHED_HIST_DAYS", "90"))
SCHED_OPTIMIZE  = os.getenv("BACKTEST_SCHED_OPTIMIZE",  "false").lower() == "true"
SCHED_FOLDS     = int(os.getenv("BACKTEST_SCHED_FOLDS",    "3"))


# ── Telegram helper (usa el Notifier existente si está disponible) ────────────
async def _send(notifier, text: str) -> None:
    """Envía mensaje partiendo el texto si supera el límite de Telegram (4096 chars)."""
    MAX = 4000
    for i in range(0, len(text), MAX):
        await notifier.send(text[i:i + MAX])
        await asyncio.sleep(0.3)


# ── Formateadores ────────────────────────────────────────────────────────────
def _fmt_result(symbol: str, r) -> str:
    """Formatea un BacktestResult en una línea compacta para Telegram."""
    wr  = f"{r.win_rate * 100:.1f}%"  if r.trades else "—"
    pnl = f"{r.gross_pnl:+.2f}%"    if r.trades else "—"
    pf  = f"{r.profit_factor:.2f}"   if r.trades else "—"
    dd  = f"{r.max_dd:.2f}%"         if r.trades else "—"
    shr = f"{r.sharpe:.2f}"          if hasattr(r, 'sharpe') and r.trades else "—"
    return (
        f"*{symbol}*\n"
        f"  Trades: {r.trades} · WR: {wr} · PnL: {pnl}\n"
        f"  PF: {pf} · MaxDD: {dd} · Sharpe: {shr}"
    )


def _fmt_optimizer_top(top_params: list) -> str:
    """Formatea el top-3 del optimizador walk-forward."""
    if not top_params:
        return ""
    lines = ["\n📊 *Top 3 parámetros (Walk-Forward)*"]
    for i, p in enumerate(top_params[:3], 1):
        lines.append(
            f"  #{i} Score≥{p['min_score']} · RR≥{p['min_rr']} · "
            f"Ratio≥{p['min_ratio']:.2f} → Sharpe {p['sharpe']:.2f} · "
            f"WR {p['wr']*100:.0f}%"
        )
    best = top_params[0]
    lines += [
        "",
        "📌 *Configuración recomendada:*",
        f"  `MIN_SIGNAL_SCORE={best['min_score']}`",
        f"  `MIN_RR_REQUIRED={best['min_rr']}`",
        f"  `MIN_SCORE_RATIO={best['min_ratio']}`",
    ]
    return "\n".join(lines)


def _build_report(
    symbols: list,
    results: list,
    hist_days: int,
    optimized: bool,
    top_params: Optional[list],
    elapsed: float,
) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    icon = "🔬" if optimized else "📈"
    mode = "Optimizador Walk-Forward" if optimized else "Backtest Simple"

    header = (
        f"{icon} *{mode} — {now_str}*\n"
        f"Símbolos: {', '.join(symbols)}\n"
        f"Histórico: {hist_days}d · Tiempo: {elapsed:.0f}s\n"
        f"{'─' * 30}"
    )

    body_lines = []
    for sym, r in zip(symbols, results):
        if r is not None and not isinstance(r, Exception):
            body_lines.append(_fmt_result(sym, r))
        else:
            body_lines.append(f"*{sym}* — ❌ Error al ejecutar")

    body = "\n\n".join(body_lines)

    optimizer_section = _fmt_optimizer_top(top_params) if optimized and top_params else ""

    footer = "\n\n_Generado automáticamente por BotTrading_"

    return f"{header}\n\n{body}{optimizer_section}{footer}"


# ── Runner principal ─────────────────────────────────────────────────────────
async def run_backtest_now(
    notifier,
    symbols: Optional[list] = None,
    hist_days: Optional[int] = None,
    optimize: Optional[bool] = None,
    folds: Optional[int] = None,
) -> None:
    """
    Ejecuta el backtester para cada símbolo y envía el informe a Telegram.
    Puede llamarse desde el scheduler o desde el comando /backtest de Telegram.
    """
    symbols   = symbols   or SCHED_SYMBOLS
    hist_days = hist_days or SCHED_HIST_DAYS
    optimize  = optimize  if optimize is not None else SCHED_OPTIMIZE
    folds     = folds     or SCHED_FOLDS

    await _send(notifier,
        f"⏳ *Backtest iniciado*\n"
        f"Símbolos: {', '.join(symbols)}\n"
        f"Modo: {'Optimizador Walk-Forward' if optimize else 'Backtest Simple'}\n"
        f"Histórico: {hist_days}d — esto puede tardar unos minutos..."
    )

    t0 = time.monotonic()
    results    = []
    top_params = None

    try:
        from bot.backtester import Backtester  # importación diferida

        bt = Backtester()

        for sym in symbols:
            try:
                log.info("[backtest_sched] Ejecutando %s...", sym)
                r = await bt.run(
                    symbol=sym,
                    days=hist_days,
                    optimize=optimize,
                    folds=folds,
                )
                results.append(r)
                if optimize and hasattr(r, 'top_params'):
                    # Fusionamos los top_params de todos los símbolos
                    top_params = (top_params or []) + (r.top_params or [])
            except Exception as e:
                log.error("[backtest_sched] Error en %s: %s", sym, e)
                results.append(None)

        # Ordenar top_params por Sharpe
        if top_params:
            top_params = sorted(top_params, key=lambda x: x.get('sharpe', 0), reverse=True)

    except ImportError:
        log.error("[backtest_sched] No se pudo importar Backtester — ¿rama correcta?")
        await _send(notifier, "❌ *Backtest fallido* — módulo `backtester` no disponible.")
        return
    except Exception as e:
        log.error("[backtest_sched] Error inesperado: %s", e)
        await _send(notifier, f"❌ *Backtest fallido*\n`{e}`")
        return

    elapsed = time.monotonic() - t0
    report  = _build_report(symbols, results, hist_days, optimize, top_params, elapsed)
    await _send(notifier, report)
    log.info("[backtest_sched] Informe enviado a Telegram (%.0fs)", elapsed)


# ── Scheduler loop ────────────────────────────────────────────────────────────
class BacktestScheduler:
    """
    Corre como tarea asyncio en background.
    Se despierta cada hora, comprueba si toca ejecutar y lanza run_backtest_now().
    """

    def __init__(self, notifier) -> None:
        self._notifier  = notifier
        self._last_run: float = 0.0   # timestamp POSIX de la última ejecución
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
                await asyncio.sleep(3600)  # comprobar cada hora
                now = datetime.now(timezone.utc)

                # ¿Es la hora correcta?
                if now.hour != SCHED_HOUR:
                    continue

                # ¿Han pasado suficientes días desde la última ejecución?
                elapsed_days = (time.time() - self._last_run) / 86400
                if elapsed_days < SCHED_DAYS:
                    continue

                self._last_run = time.time()
                log.info("[backtest_sched] 🚀 Lanzando backtest programado...")
                await run_backtest_now(self._notifier)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("[backtest_sched] Error en el scheduler: %s", e)
                await asyncio.sleep(60)


# Singleton
_scheduler: Optional[BacktestScheduler] = None


def get_scheduler(notifier) -> BacktestScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = BacktestScheduler(notifier)
    return _scheduler
