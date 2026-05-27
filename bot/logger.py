import logging
import sys

def setup_logger(name="BitgetBot", level=logging.INFO):
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
    return logging.getLogger(name)
