"""Logging utility module."""

import logging
import sys
from typing import Optional


def get_logger(name: str, level: Optional[str] = "INFO") -> logging.Logger:
    """Configure and return a standardized logger.

    Parameters
    ----------
    name : str
        Name of the logger.
    level : Optional[str]
        Logging level (e.g., 'INFO', 'DEBUG', 'WARN', 'ERROR'). Default is 'INFO'.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if level:
        numeric_level = getattr(logging, level.upper(), logging.INFO)
        logger.setLevel(numeric_level)

    return logger
