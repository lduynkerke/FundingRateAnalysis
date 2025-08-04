"""
Configuration loading utility.

This module provides functionality for loading and validating the application
configuration from a YAML file.
"""

import os
import yaml
from utils.logger import get_logger


def load_config(config_path: str = "config.yaml") -> dict:
    """
    Loads configuration from a YAML file.

    :param config_path: Path to the configuration file.
    :type config_path: str
    :return: Configuration dictionary.
    :rtype: dict
    :raises FileNotFoundError: If the configuration file is not found.
    :raises ValueError: If the configuration file is invalid.
    """
    logger = get_logger()
    logger.debug(f"Loading configuration from {config_path}")

    if not os.path.exists(config_path):
        error_msg = f"Configuration file not found: {config_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.debug("Configuration loaded successfully")
        return config
    except Exception as e:
        error_msg = f"Error loading configuration: {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)