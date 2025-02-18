"""
Logging Configuration Module.

This module sets up the standard logging configuration for the entire project.
It ensures that logs follow a consistent format and are printed at the appropriate
levels.
"""

import logging

logging.basicConfig(
    format="[{asctime}] {{ {filename}:{lineno} }} {levelname} - {message}",
    style="{",
    level=logging.DEBUG,  # Ensure INFO, DEBUG, and other levels are printed
)

logger = logging.getLogger(__name__)
