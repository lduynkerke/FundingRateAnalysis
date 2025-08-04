"""
Logging utility module.

This module provides functionality for setting up and retrieving a configured
logger for the application. It handles log file rotation, formatting, and
different log levels for console and file outputs.
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime


# Global logger instance
_logger = None


def setup_logger(log_dir: str = "logs", log_level: str = "INFO") -> logging.Logger:
    """
    Sets up and configures the application logger.

    :param log_dir: Directory where log files will be stored.
    :type log_dir: str
    :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    :type log_level: str
    :return: Configured logger instance.
    :rtype: logging.Logger
    """
    global _logger

    if _logger is not None:
        return _logger

    # Create logger
    logger = logging.getLogger("FundingRateAnalysis")
    logger.setLevel(logging.DEBUG)  # Set to lowest level, handlers will filter

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create file handler with daily rotation
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, f"app_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30
    )
    file_handler.setLevel(getattr(logging, log_level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info("Logger initialized")
    _logger = logger
    return logger


def get_logger() -> logging.Logger:
    """
    Returns the configured logger instance.

    If the logger has not been set up yet, it will be initialized with default settings.

    :return: Logger instance.
    :rtype: logging.Logger
    """
    global _logger

    if _logger is None:
        return setup_logger()

    return _logger