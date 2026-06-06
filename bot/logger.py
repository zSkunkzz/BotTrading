import logging
import sys

def setup_logger(name="TradingBot", level=logging.INFO):
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(handler)

    # Silenciar httpx/httpcore: loguean las URLs completas a nivel INFO,
    # lo que expone el token de Telegram (va en la URL, no en headers).
    # WARNING solo muestra errores reales de red.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpcore.connection").setLevel(logging.WARNING)
    logging.getLogger("httpcore.http11").setLevel(logging.WARNING)

    return logging.getLogger(name)
