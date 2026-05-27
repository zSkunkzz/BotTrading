import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger():
    os.makedirs("logs", exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)-12s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger("BitgetProBot")
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = RotatingFileHandler("logs/bot.log", maxBytes=5_000_000, backupCount=3)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    th = RotatingFileHandler("logs/trades.log", maxBytes=2_000_000, backupCount=5)
    th.setLevel(logging.WARNING)
    th.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.addHandler(th)
    return logger
