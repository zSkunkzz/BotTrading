"""
Notifier — envia alertas a Telegram (solo salida, sin comandos)
"""
import logging
import aiohttp

logger = logging.getLogger("Notifier")


class Notifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        if not self.enabled:
            logger.warning("Telegram no configurado — alertas desactivadas")

    async def send(self, text: str):
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"Telegram error {resp.status}: {body}")
        except Exception as e:
            logger.error(f"Error enviando alerta Telegram: {e}")
