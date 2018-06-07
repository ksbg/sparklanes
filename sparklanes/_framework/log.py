"""Module handling logging. TODO: improve logging. Allow configuration, etc."""
import logging
import sys

from .env import INTERNAL_LOGGER_NAME


def make_default_logger(name=INTERNAL_LOGGER_NAME, level=logging.INFO,
                        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    """Create a logger with the default configuration"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
