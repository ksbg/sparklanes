"""TODO: better logging"""
import logging
import sys

from .env import INTERNAL_LOGGER_NAME


def make_default_logger(name=INTERNAL_LOGGER_NAME, level=logging.INFO,
                        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    # TODO: better logging
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter(fmt)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
