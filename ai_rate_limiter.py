# ============================================================
# ai_rate_limiter.py  —  Fix rate limiting para BitgetProBot
# Aplica en: AITrader, AIFilter
# ============================================================

import asyncio
import time
import logging
from functools import wraps

logger = logging.getLogger("AIRateLimiter")


# ─────────────────────────────────────────────
# CONFIGURACIÓN DE LÍMITES
# Ajusta según tus cuotas reales
# ─────────────────────────────────────────────
GROQ_TPD_LIMIT       = 100_000   # tokens/día (free tier llama-3.3-70b)
GROQ_RPM_LIMIT       = 30        # requests/minuto (free tier)
GEMINI_RPM_LIMIT     = 15        # requests/minuto (free tier Flash)
GEMINI_RPD_LIMIT     = 1_500     # requests/día (free tier Flash)

# Tokens estimados por llamada (prompt + respuesta)
TOKENS_PER_CALL_GROQ = 800       # ajusta si sabes el real
GROQ_SAFE_DAILY_CALLS = int(GROQ_TPD_LIMIT / TOKENS_PER_CALL_GROQ)  # ~125 calls/día


class AIBudgetManager:
    """
    Gestor centralizado de cuotas de IA.
    Singleton compartido entre todos los traders.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        # Semáforos de concurrencia
        self.groq_semaphore   = asyncio.Semaphore(2)   # máx 2 llamadas Groq simultáneas
        self.gemini_semaphore = asyncio.Semaphore(1)   # máx 1 llamada Gemini simultánea

        # Contadores diarios (se resetean a medianoche UTC)
        self._groq_calls_today   = 0
        self._gemini_calls_today = 0
        self._day_start          = self._today()

        # Rate per-minute tracking
        self._groq_minute_calls   = []   # timestamps del último minuto
        self._gemini_minute_calls = []

        # Lock para thread-safety
        self._lock = asyncio.Lock()

        logger.info(
            f"AIBudgetManager iniciado | "
            f"Groq: {GROQ_SAFE_DAILY_CALLS} calls/día, {GROQ_RPM_LIMIT} RPM | "
            f"Gemini: {GEMINI_RPD_LIMIT} calls/día, {GEMINI_RPM_LIMIT} RPM"
        )

    @staticmethod
    def _today():
        return time.gmtime().tm_yday

    async def _reset_if_new_day(self):
        today = self._today()
        if today != self._day_start:
            self._groq_calls_today   = 0
            self._gemini_calls_today = 0
            self._day_start          = today
            logger.info("AIBudgetManager: contadores diarios reseteados (nuevo día UTC)")

    def _cleanup_minute_window(self, call_list):
        """Elimina timestamps fuera de la ventana de 60s."""
        cutoff = time.time() - 60
        call_list[:] = [t for t in call_list if t > cutoff]

    async def can_call_groq(self) -> bool:
        async with self._lock:
            await self._reset_if_new_day()
            self._cleanup_minute_window(self._groq_minute_calls)

            if self._groq_calls_today >= GROQ_SAFE_DAILY_CALLS:
                logger.warning(
                    f"Groq budget diario agotado "
                    f"({self._groq_calls_today}/{GROQ_SAFE_DAILY_CALLS})"
                )
                return False
            if len(self._groq_minute_calls) >= GROQ_RPM_LIMIT:
                logger.warning(
                    f"Groq RPM alcanzado ({len(self._groq_minute_calls)}/{GROQ_RPM_LIMIT})"
                )
                return False
            return True

    async def can_call_gemini(self) -> bool:
        async with self._lock:
            await self._reset_if_new_day()
            self._cleanup_minute_window(self._gemini_minute_calls)

            if self._gemini_calls_today >= GEMINI_RPD_LIMIT:
                logger.warning(
                    f"Gemini budget diario agotado "
                    f"({self._gemini_calls_today}/{GEMINI_RPD_LIMIT})"
                )
                return False
            if len(self._gemini_minute_calls) >= GEMINI_RPM_LIMIT:
                logger.warning(
                    f"Gemini RPM alcanzado ({len(self._gemini_minute_calls)}/{GEMINI_RPM_LIMIT})"
                )
                return False
            return True

    async def register_groq_call(self):
        async with self._lock:
            self._groq_calls_today   += 1
            self._groq_minute_calls.append(time.time())

    async def register_gemini_call(self):
        async with self._lock:
            self._gemini_calls_today   += 1
            self._gemini_minute_calls.append(time.time())

    async def status(self) -> dict:
        async with self._lock:
            self._cleanup_minute_window(self._groq_minute_calls)
            self._cleanup_minute_window(self._gemini_minute_calls)
            return {
                "groq_calls_today":    self._groq_calls_today,
                "groq_daily_limit":    GROQ_SAFE_DAILY_CALLS,
                "groq_rpm_used":       len(self._groq_minute_calls),
                "gemini_calls_today":  self._gemini_calls_today,
                "gemini_daily_limit":  GEMINI_RPD_LIMIT,
                "gemini_rpm_used":     len(self._gemini_minute_calls),
            }


# ─────────────────────────────────────────────
# INSTANCIA GLOBAL (importar desde aquí)
# ─────────────────────────────────────────────
budget = AIBudgetManager()


# ─────────────────────────────────────────────
# WRAPPER para llamadas a Groq
# ─────────────────────────────────────────────
async def call_groq_safe(groq_client, model: str, messages: list, **kwargs):
    """
    Reemplaza llamadas directas a Groq con control de rate limit.

    Uso:
        # ANTES:
        response = await groq_client.chat.completions.create(...)

        # DESPUÉS:
        response = await call_groq_safe(groq_client, model, messages, temperature=0.3)
    """
    if not await budget.can_call_groq():
        raise RateLimitExhausted("groq")

    async with budget.groq_semaphore:
        await budget.register_groq_call()
        response = await groq_client.chat.completions.create(
            model=model,
            messages=messages,
            **kwargs
        )
        return response


# ─────────────────────────────────────────────
# WRAPPER para llamadas a Gemini
# ─────────────────────────────────────────────
async def call_gemini_safe(session, url: str, payload: dict, headers: dict):
    """
    Reemplaza llamadas directas a Gemini con control de rate limit.

    Uso:
        # ANTES:
        async with session.post(gemini_url, json=payload) as r:
            data = await r.json()

        # DESPUÉS:
        data = await call_gemini_safe(session, gemini_url, payload, headers)
    """
    if not await budget.can_call_gemini():
        raise RateLimitExhausted("gemini")

    async with budget.gemini_semaphore:
        await budget.register_gemini_call()
        async with session.post(url, json=payload, headers=headers) as r:
            return await r.json()


class RateLimitExhausted(Exception):
    """Se lanza cuando el budget de IA está agotado (antes de hacer la llamada)."""
    def __init__(self, provider: str):
        self.provider = provider
        super().__init__(f"Budget {provider} agotado — usando fallback técnico")


# ─────────────────────────────────────────────
# FIX PARA ARRANQUE ESCALONADO DE TRADERS
# ─────────────────────────────────────────────
async def start_traders_staggered(pairs: list, start_trader_fn, delay: float = 2.0):
    """
    Inicia traders de forma escalonada para evitar el spike de IA al arranque.

    Args:
        pairs:           lista de pares del scanner
        start_trader_fn: tu función/coroutine start_trader(pair)
        delay:           segundos entre cada trader (default 2s)

    Con 15 traders x 2s = 30s de arranque total.
    Permite que Gemini (15 RPM) reciba máx 1 req cada 4s -> nunca supera el límite.

    Ejemplo de uso en BitgetBot.start():
        await start_traders_staggered(pairs, self.start_trader, delay=2.0)
    """
    logger.info(
        f"Iniciando {len(pairs)} traders escalonados "
        f"(delay={delay}s, total ~{len(pairs)*delay:.0f}s)"
    )
    for i, pair in enumerate(pairs):
        asyncio.create_task(start_trader_fn(pair))
        logger.info(f"Trader {i+1}/{len(pairs)}: {pair}")
        if i < len(pairs) - 1:
            await asyncio.sleep(delay)


# ─────────────────────────────────────────────
# COMANDO TELEGRAM: /ai_status
# ─────────────────────────────────────────────
async def telegram_ai_status(update, context):
    """
    Handler de Telegram para ver el consumo de IA en tiempo real.
    Registrar con: app.add_handler(CommandHandler("ai_status", telegram_ai_status))
    """
    s = await budget.status()
    groq_pct   = s['groq_calls_today']   / s['groq_daily_limit']   * 100
    gemini_pct = s['gemini_calls_today'] / s['gemini_daily_limit'] * 100

    def bar(pct):
        if pct < 50:  return "🟢"
        if pct < 80:  return "🟡"
        return "🔴"

    text = (
        f"📊 *Estado IA — BitgetProBot*\n\n"
        f"*Groq* (llama-3.3-70b)\n"
        f"{bar(groq_pct)} Hoy: {s['groq_calls_today']}/{s['groq_daily_limit']} calls ({groq_pct:.1f}%)\n"
        f"⏱ Último minuto: {s['groq_rpm_used']}/{GROQ_RPM_LIMIT} RPM\n\n"
        f"*Gemini* (Flash)\n"
        f"{bar(gemini_pct)} Hoy: {s['gemini_calls_today']}/{s['gemini_daily_limit']} calls ({gemini_pct:.1f}%)\n"
        f"⏱ Último minuto: {s['gemini_rpm_used']}/{GEMINI_RPM_LIMIT} RPM\n\n"
        f"_Reset a medianoche UTC_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")
