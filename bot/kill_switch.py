"""
Kill Switch — escalera de 4 niveles desacoplada del loop de ejecución.

Niveles
-------
L1  pausa nuevas entradas (trading bloqueado; posiciones existentes siguen vivas)
L2  halt de símbolo/estrategia  (igual que L1 pero con flag de símbolo individual)
L3  cancelar nuevas órdenes + bloquear entradas
L4  cierre forzado de todo + hard kill

Re-arm
------
- Todos los niveles (L1-L4) se resetean con /resetks desde Telegram.
  No se requiere clave. El acceso está protegido únicamente por TELEGRAM_CHAT_ID.
- L2 además tiene un cooldown de auto-reset configurable (KS_L2_COOLDOWN_SECONDS).

Persistencia
------------
Cada activación guarda un snapshot JSON en /tmp/kill_switch_state.json.

FIX #3 (2026-06-02):
  ks2_activated_at_mono usaba time.monotonic(), que NO es persistible entre
  procesos. Al reiniciar Railway, saved_mono era un valor de la sesión anterior
  -> elapsed = time.monotonic() - saved_mono era un número aleatorio gigante
  -> el auto-reset de L2 se disparaba inmediatamente aunque el L2 se hubiera
  activado hace 1 minuto.
  Fix: guardar time.time() (epoch Unix) como ks2_activated_at_epoch.
  En _load_state se recalcula el equivalente monotonic:
    self._ks2_activated_at = time.monotonic() - (time.time() - saved_epoch)
  Así el cooldown restante es correcto incluso tras restart.

FIX Bug L (2026-06-02):
  reset_daily_pnl() era síncrono y modificaba _daily_pnl / _consec_losses /
  _api_reconnects sin asyncio.Lock → race condition con on_trade_result()
  corriendo concurrentemente en el mismo event loop.
  Fix: convertir a async y proteger con self._lock.
  _watchdog_tick actualizado con await.

FIX Bug M (2026-06-02):
  on_state_mismatch() leía self._tpsl_retrying fuera del lock → patrón
  frágil susceptible a race condition aunque el GIL cubra sets en CPython.
  Fix: mover la lectura del set dentro de async with self._lock.

FIX re-arm sin clave (2026-06-02):
  manual_reset() ya no valida ninguna clave. El acceso está protegido
  exclusivamente por TELEGRAM_CHAT_ID (solo el chat autorizado puede enviar
  comandos al bot). Eliminada dependencia de KILL_SWITCH_REARM_KEY.

FIX run_watchdog (2026-06-02):
  main.py llama kill_switch.run_watchdog(trader_instances) pero el método
  no existía (sólo existía _watchdog_tick interno).
  Fix: añadir método público async run_watchdog(traders) que ejecuta
  _watchdog_tick() en loop cada KS_WATCHDOG_INTERVAL_S segundos.

FIX run() alias (2026-06-06):
  main.py llama asyncio.create_task(kill_switch.run()) pero la clase solo
  exponía run_watchdog(). Añadido run() como alias directo de run_watchdog()
  para compatibilidad con la firma de main.py.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("KillSwitch")

_STATE_PATH = os.getenv("KILL_SWITCH_STATE_PATH", "/tmp/kill_switch_state.json")

# Umbrales por defecto (sobreponibles con env vars)
_CFG = {
    "max_daily_loss_pct":     float(os.getenv("KS_MAX_DAILY_LOSS_PCT",      "8.0")),
    "max_consec_losses":      int(os.getenv("KS_MAX_CONSEC_LOSSES",         "5")),
    "max_reject_rate":        float(os.getenv("KS_MAX_REJECT_RATE",         "0.5")),
    "max_slippage_bps":       float(os.getenv("KS_MAX_SLIPPAGE_BPS",        "80.0")),
    "max_api_reconnects":     int(os.getenv("KS_MAX_API_RECONNECTS",        "10")),
    "max_state_mismatch":     int(os.getenv("KS_MAX_STATE_MISMATCH",        "3")),
    "watchdog_interval_s":    int(os.getenv("KS_WATCHDOG_INTERVAL_S",       "30")),
    # FIX #2 — ventana ampliada 10 → 200 para evitar falsos positivos por spikes
    "reject_window_orders":   int(os.getenv("KS_REJECT_WINDOW_ORDERS",      "200")),
    # FIX #1 — cooldown de auto-reset para L2 (segundos; 0 = desactivado)
    "l2_cooldown_seconds":    int(os.getenv("KS_L2_COOLDOWN_SECONDS",       "3600")),
}


class KillSwitch:
    """
    Singleton thread-safe con asyncio.Lock.
    Importar la instancia global `kill_switch` desde este módulo.
    """

    def __init__(self):
        self._lock             = asyncio.Lock()
        self._level: int       = 0          # 0 = OK, 1-4 = activo
        self._trigger: str     = ""
        self._triggered_at: Optional[float] = None
        self._consec_losses: int = 0
        self._daily_pnl: float   = 0.0
        # Ventana deslizante real para reject rate (FIX #2)
        self._order_window: deque = deque()   # timestamps de las últimas N órdenes
        self._reject_window: deque = deque()  # timestamps sólo de rechazos
        self._api_reconnects: int = 0
        self._state_mismatches: int = 0
        self._slippage_samples: list[float] = []
        self._halted_symbols: set[str] = set()
        self._hard_killed: bool = False
        # Símbolos con TPSL retry activo — el watchdog los ignora para state_mismatch
        self._tpsl_retrying: set[str] = set()
        # FIX #3 — timestamp de activación de L2 como epoch (time.time()), no monotonic
        self._ks2_activated_at: Optional[float] = None  # monotonic (solo en memoria)
        self._ks2_activated_epoch: Optional[float] = None  # epoch (persistido en disco)
        self._load_state()

    # ── Estado ────────────────────────────────────────────────────────────────────────────

    def level(self) -> int:
        return self._level

    def is_halted(self, symbol: str | None = None) -> bool:
        """Devuelve True si el bot (o el símbolo) está detenido para nuevas entradas."""
        if self._level >= 1:
            return True
        if symbol and symbol in self._halted_symbols:
            return True
        return False

    def is_hard_killed(self) -> bool:
        return self._hard_killed

    # ── TPSL retry tracking ───────────────────────────────────────────────────────────────────

    def mark_tpsl_retrying(self, symbol: str):
        """Llamar cuando empieza un retry de TPSL (code 31008). Inhibe state_mismatch."""
        self._tpsl_retrying.add(symbol)

    def clear_tpsl_retrying(self, symbol: str):
        """Llamar cuando el TPSL se coloca OK o se agotan los reintentos."""
        self._tpsl_retrying.discard(symbol)

    # ── Activación ───────────────────────────────────────────────────────────────────────────

    async def activate(self, level: int, trigger: str):
        """Activa el kill switch al nivel indicado (no baja de nivel ya activo)."""
        async with self._lock:
            if level <= self._level:
                return
            self._level        = level
            self._trigger      = trigger
            self._triggered_at = time.time()
            # FIX #3 — registrar activación de L2 con epoch Y monotonic
            if level == 2:
                self._ks2_activated_epoch = time.time()
                self._ks2_activated_at    = time.monotonic()
            self._save_state()
            logger.critical(
                f"🛑 KILL SWITCH L{level} activado — trigger: {trigger}"
            )
            try:
                from bot.telegram_bot import notify_kill_switch
                asyncio.create_task(notify_kill_switch(level, trigger))
            except Exception:
                pass

    async def activate_symbol(self, symbol: str, trigger: str):
        """L2: pausa sólo un símbolo concreto."""
        async with self._lock:
            self._halted_symbols.add(symbol)
            logger.warning(f"⚠️ KS L2 — símbolo {symbol} pausado: {trigger}")
            self._save_state()

    async def hard_kill(self, trigger: str):
        """L4: hard kill total."""
        await self.activate(4, trigger)
        async with self._lock:
            self._hard_killed = True
            self._save_state()
            logger.critical("💀 HARD KILL L4 — bot parado completamente")

    # ── Auto-reset de L2 por cooldown ────────────────────────────────────────────────────

    async def _maybe_autoreset_l2(self) -> bool:
        """
        Comprueba si el nivel 2 ha expirado su cooldown y, si es así, lo resetea.
        Retorna True si se hizo el reset.
        """
        cooldown = _CFG["l2_cooldown_seconds"]
        if cooldown <= 0 or self._level != 2:
            return False
        if self._ks2_activated_at is None:
            async with self._lock:
                self._ks2_activated_at    = time.monotonic()
                self._ks2_activated_epoch = time.time()
            return False

        elapsed = time.monotonic() - self._ks2_activated_at
        if elapsed < cooldown:
            return False

        async with self._lock:
            if self._level != 2:
                return False
            self._level               = 0
            self._trigger             = "auto-reset L2 tras cooldown"
            self._ks2_activated_at    = None
            self._ks2_activated_epoch = None
            self._save_state()
            logger.info("✅ KS L2 auto-reseteado tras cooldown de %ds", cooldown)
        try:
            from bot.telegram_bot import send_message
            asyncio.create_task(send_message(
                f"✅ <b>Kill Switch L2 auto-reseteado</b>\n"
                f"Cooldown de {cooldown}s expirado. Bot vuelve a operar."
            ))
        except Exception:
            pass
        return True

    # ── Re-arm manual (sin clave) ────────────────────────────────────────────────────────

    async def manual_reset(self, _key: str = "") -> bool:
        """
        Resetea el kill switch completamente a L0.
        Ya no requiere clave — el acceso está protegido por TELEGRAM_CHAT_ID.
        El parámetro _key se mantiene por compatibilidad pero se ignora.
        """
        async with self._lock:
            self._level               = 0
            self._trigger             = ""
            self._triggered_at        = None
            self._consec_losses       = 0
            self._daily_pnl           = 0.0
            self._api_reconnects      = 0
            self._state_mismatches    = 0
            self._hard_killed         = False
            self._halted_symbols      = set()
            self._ks2_activated_at    = None
            self._ks2_activated_epoch = None
            self._order_window.clear()
            self._reject_window.clear()
            self._slippage_samples.clear()
            self._save_state()
            logger.info("✅ Kill Switch re-armado manualmente — L0 OK")
        return True

    # ── Reset contadores diarios ─────────────────────────────────────────────────────────

    async def reset_daily_pnl(self):
        """Resetea contadores diarios sin cambiar el nivel del KS. Async + lock."""
        async with self._lock:
            self._daily_pnl      = 0.0
            self._consec_losses  = 0
            self._api_reconnects = 0
            self._save_state()
            logger.info("📅 Contadores diarios reseteados (PnL, consec_losses, api_reconnects)")

    # ── Registro de eventos ──────────────────────────────────────────────────────────────

    async def on_trade_result(self, pnl_pct: float, symbol: str = ""):
        """Llamar tras cada cierre de trade con el PnL en %."""
        async with self._lock:
            self._daily_pnl += pnl_pct
            if pnl_pct < 0:
                self._consec_losses += 1
            else:
                self._consec_losses = 0

            if abs(self._daily_pnl) >= _CFG["max_daily_loss_pct"] and self._daily_pnl < 0:
                trigger = f"Pérdida diaria {self._daily_pnl:.2f}% >= límite {_CFG['max_daily_loss_pct']}%"
                self._save_state()

        if abs(self._daily_pnl) >= _CFG["max_daily_loss_pct"] and self._daily_pnl < 0:
            await self.activate(3, trigger)
            return

        if self._consec_losses >= _CFG["max_consec_losses"]:
            await self.activate(
                2,
                f"{self._consec_losses} pérdidas consecutivas >= límite {_CFG['max_consec_losses']}"
            )

    async def on_order_result(self, rejected: bool, symbol: str = ""):
        """Llamar tras cada orden. rejected=True si fue rechazada por el exchange."""
        now = time.monotonic()
        window_n = _CFG["reject_window_orders"]
        async with self._lock:
            self._order_window.append(now)
            if rejected:
                self._reject_window.append(now)
            # Mantener solo las últimas N órdenes
            while len(self._order_window) > window_n:
                self._order_window.popleft()
            # Limpiar rechazos que corresponden a órdenes fuera de la ventana
            cutoff = self._order_window[0] if self._order_window else now
            while self._reject_window and self._reject_window[0] < cutoff:
                self._reject_window.popleft()

            total   = len(self._order_window)
            rejects = len(self._reject_window)
            rate    = rejects / total if total > 0 else 0.0

        if total >= 10 and rate >= _CFG["max_reject_rate"]:
            await self.activate(
                2,
                f"Tasa de rechazos {rate:.0%} en últimas {total} órdenes >= {_CFG['max_reject_rate']:.0%}"
            )

    async def on_slippage(self, slippage_bps: float, symbol: str = ""):
        """Registrar slippage de una orden ejecutada."""
        async with self._lock:
            self._slippage_samples.append(abs(slippage_bps))
            if len(self._slippage_samples) > 20:
                self._slippage_samples = self._slippage_samples[-20:]
            avg = sum(self._slippage_samples) / len(self._slippage_samples)

        if avg >= _CFG["max_slippage_bps"] and len(self._slippage_samples) >= 5:
            await self.activate(
                1,
                f"Slippage medio {avg:.1f}bps >= límite {_CFG['max_slippage_bps']}bps"
            )

    async def on_api_reconnect(self, symbol: str = ""):
        """Llamar cada vez que el WS o el REST reconectan."""
        async with self._lock:
            self._api_reconnects += 1
            count = self._api_reconnects

        if count >= _CFG["max_api_reconnects"]:
            await self.activate(
                2,
                f"{count} reconexiones API >= límite {_CFG['max_api_reconnects']}"
            )

    async def on_state_mismatch(self, symbol: str):
        """Llamar cuando la posición local no coincide con el exchange."""
        async with self._lock:
            # FIX Bug M — leer _tpsl_retrying dentro del lock
            if symbol in self._tpsl_retrying:
                return
            self._state_mismatches += 1
            count = self._state_mismatches

        if count >= _CFG["max_state_mismatch"]:
            await self.activate(
                3,
                f"{count} mismatches de estado >= límite {_CFG['max_state_mismatch']}"
            )

    # ── Watchdog periódico ───────────────────────────────────────────────────────────────

    async def _watchdog_tick(self):
        """Llamar periódicamente desde el loop principal."""
        await self._maybe_autoreset_l2()

    async def run_watchdog(self, traders=None) -> None:
        """
        Loop público del watchdog. Llamado desde main.py como:
            asyncio.create_task(kill_switch.run_watchdog(trader_instances))

        Ejecuta _watchdog_tick() cada KS_WATCHDOG_INTERVAL_S segundos.
        `traders` se acepta por compatibilidad con la firma de main.py pero
        no se usa directamente — el watchdog opera sobre el estado interno del KS.
        """
        interval = _CFG["watchdog_interval_s"]
        logger.info("KillSwitch watchdog arrancado (intervalo=%ds)", interval)
        while True:
            try:
                await self._watchdog_tick()
            except asyncio.CancelledError:
                logger.info("KillSwitch watchdog cancelado.")
                raise
            except Exception as e:
                logger.warning("KillSwitch watchdog error: %s", e)
            await asyncio.sleep(interval)

    # Alias para compatibilidad con main.py que llama kill_switch.run()
    run = run_watchdog

    # ── Persistencia ────────────────────────────────────────────────────────────────────

    def _save_state(self):
        """Guarda estado en disco (llamar dentro del lock)."""
        try:
            state = {
                "level":                  self._level,
                "trigger":                self._trigger,
                "triggered_at":           self._triggered_at,
                "consec_losses":          self._consec_losses,
                "daily_pnl":              self._daily_pnl,
                "api_reconnects":         self._api_reconnects,
                "state_mismatches":       self._state_mismatches,
                "halted_symbols":         list(self._halted_symbols),
                "hard_killed":            self._hard_killed,
                # FIX #3 — guardar epoch, no monotonic
                "ks2_activated_at_epoch": self._ks2_activated_epoch,
                "saved_at":               datetime.now(timezone.utc).isoformat(),
            }
            with open(_STATE_PATH, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning("KillSwitch: no se pudo guardar estado: %s", e)

    def _load_state(self):
        """Carga estado desde disco al arrancar."""
        try:
            with open(_STATE_PATH) as f:
                state = json.load(f)
            self._level             = state.get("level", 0)
            self._trigger           = state.get("trigger", "")
            self._triggered_at      = state.get("triggered_at")
            self._consec_losses     = state.get("consec_losses", 0)
            self._daily_pnl         = state.get("daily_pnl", 0.0)
            self._api_reconnects    = state.get("api_reconnects", 0)
            self._state_mismatches  = state.get("state_mismatches", 0)
            self._halted_symbols    = set(state.get("halted_symbols", []))
            self._hard_killed       = state.get("hard_killed", False)

            # FIX #3 — recalcular monotonic desde epoch persistido
            saved_epoch = state.get("ks2_activated_at_epoch")
            if saved_epoch is not None:
                self._ks2_activated_epoch = saved_epoch
                elapsed_since_activation  = time.time() - saved_epoch
                self._ks2_activated_at    = time.monotonic() - elapsed_since_activation
            else:
                self._ks2_activated_at    = None
                self._ks2_activated_epoch = None

            if self._level > 0:
                logger.warning(
                    "KillSwitch restaurado desde disco — nivel L%d (trigger: %s)",
                    self._level, self._trigger
                )
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("KillSwitch: error cargando estado: %s", e)


# Instancia global
kill_switch = KillSwitch()
