# ============================================================
# ai_rate_limiter.py  —  BitgetProBot
# Controla cuotas de IA para Gemini (pagado) + Groq (free)
# ============================================================

import asyncio
import time
import logging

logger = logging.getLogger("AIRateLimiter")

# ── Groq free tier ──────────────────────────
GROQ_TPD_LIMIT        = 100_000
GROQ_RPM_LIMIT        = 30
TOKENS_PER_CALL_GROQ  = 800
GROQ_SAFE_DAILY_CALLS = int(GROQ_TPD_LIMIT / TOKENS_PER_CALL_GROQ)  # ~125

# ── Gemini pagado ───────────────────────────
GEMINI_RPM_LIMIT = 60
GEMINI_RPD_LIMIT = 5_000


class AIBudgetManager:
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

        self.groq_semaphore   = asyncio.Semaphore(2)
        self.gemini_semaphore = asyncio.Semaphore(3)

        self._groq_calls_today   = 0
        self._gemini_calls_today = 0
        self._day_start          = self._today()

        self._groq_minute_calls   = []
        self._gemini_minute_calls = []

        # Cooldown por símbolo: evita llamar a la IA por el mismo par más de
        # 1 vez cada AI_SYMBOL_COOLDOWN segundos (por defecto 5 minutos).
        import os
        self._symbol_cooldown = int(os.getenv("AI_SYMBOL_COOLDOWN", "300"))
        self._symbol_last_call: dict[str, float] = {}

        self._lock = asyncio.Lock()

        logger.info(
            f"AIBudgetManager iniciado | "
            f"Groq: {GROQ_SAFE_DAILY_CALLS} calls/día, {GROQ_RPM_LIMIT} RPM | "
            f"Gemini pagado: {GEMINI_RPD_LIMIT} calls/día, {GEMINI_RPM_LIMIT} RPM | "
            f"Cooldown por símbolo: {self._symbol_cooldown}s"
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
        cutoff = time.time() - 60
        call_list[:] = [t for t in call_list if t > cutoff]

    async def symbol_on_cooldown(self, symbol: str) -> bool:
        """
        Devuelve True si el símbolo recibió una llamada IA hace menos de
        AI_SYMBOL_COOLDOWN segundos. Llamar ANTES de can_call_groq/gemini.
        """
        async with self._lock:
            last = self._symbol_last_call.get(symbol, 0)
            remaining = self._symbol_cooldown - (time.time() - last)
            if remaining > 0:
                logger.debug(
                    f"[cooldown] {symbol} en espera {remaining:.0f}s — skip IA"
                )
                return True
            return False

    async def register_symbol_call(self, symbol: str):
        """Registra que se hizo una llamada IA para este símbolo ahora."""
        async with self._lock:
            self._symbol_last_call[symbol] = time.time()
            # Evitar crecimiento infinito
            if len(self._symbol_last_call) > 200:
                oldest = min(self._symbol_last_call, key=lambda k: self._symbol_last_call[k])
                del self._symbol_last_call[oldest]

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
                "groq_calls_today":   self._groq_calls_today,
                "groq_daily_limit":   GROQ_SAFE_DAILY_CALLS,
                "groq_rpm_used":      len(self._groq_minute_calls),
                "gemini_calls_today": self._gemini_calls_today,
                "gemini_daily_limit": GEMINI_RPD_LIMIT,
                "gemini_rpm_used":    len(self._gemini_minute_calls),
                "symbol_cooldown_s":  self._symbol_cooldown,
                "symbols_tracked":    len(self._symbol_last_call),
            }


budget = AIBudgetManager()


async def call_groq_safe(groq_client, model: str, messages: list, **kwargs):
    if not await budget.can_call_groq():
        raise RateLimitExhausted("groq")
    async with budget.groq_semaphore:
        await budget.register_groq_call()
        return await groq_client.chat.completions.create(
            model=model, messages=messages, **kwargs
        )


async def call_gemini_safe(session, url: str, payload: dict, headers: dict):
    if not await budget.can_call_gemini():
        raise RateLimitExhausted("gemini")
    async with budget.gemini_semaphore:
        await budget.register_gemini_call()
        async with session.post(url, json=payload, headers=headers) as r:
            return await r.json()


class RateLimitExhausted(Exception):
    def __init__(self, provider: str):
        self.provider = provider
        super().__init__(f"Budget {provider} agotado — usando fallback técnico")


async def start_traders_staggered(pairs: list, start_trader_fn, delay: float = 2.0):
    logger.info(
        f"Iniciando {len(pairs)} traders escalonados "
        f"(delay={delay}s, total ~{len(pairs)*delay:.0f}s)"
    )
    for i, pair in enumerate(pairs):
        asyncio.create_task(start_trader_fn(pair))
        logger.info(f"Trader {i+1}/{len(pairs)}: {pair}")
        if i < len(pairs) - 1:
            await asyncio.sleep(delay)


async def telegram_ai_status(update, context):
    s = await budget.status()
    groq_pct   = s['groq_calls_today']   / max(s['groq_daily_limit'], 1)   * 100
    gemini_pct = s['gemini_calls_today'] / max(s['gemini_daily_limit'], 1) * 100

    def bar(pct):
        if pct < 50:  return "🟢"
        if pct < 80:  return "🟡"
        return "🔴"

    text = (
        f"📊 *Estado IA — BitgetProBot*\n\n"
        f"*Gemini* (pagado)\n"
        f"{bar(gemini_pct)} Hoy: {s['gemini_calls_today']}/{s['gemini_daily_limit']} calls ({gemini_pct:.1f}%)\n"
        f"⏱ Último minuto: {s['gemini_rpm_used']}/{GEMINI_RPM_LIMIT} RPM\n\n"
        f"*Groq* (free fallback)\n"
        f"{bar(groq_pct)} Hoy: {s['groq_calls_today']}/{s['groq_daily_limit']} calls ({groq_pct:.1f}%)\n"
        f"⏱ Último minuto: {s['groq_rpm_used']}/{GROQ_RPM_LIMIT} RPM\n\n"
        f"*Cooldown por símbolo*\n"
        f"⏳ {s['symbol_cooldown_s']}s · {s['symbols_tracked']} símbolos tracked\n\n"
        f"_Reset a medianoche UTC_"
    )
    await update.message.reply_text(text, parse_mode="Markdown")
