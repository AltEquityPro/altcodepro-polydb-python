# src/polydb/utils.py
"""
Utility functions for validation and logging
"""

import os
import re
import logging
from typing import Dict, Any
from .errors import ValidationError

# Loggers already configured by setup_logger(), so per-request adapter
# construction doesn't reconfigure (and re-clobber) them repeatedly.
_configured_loggers: set[str] = set()


def validate_table_name(table: str) -> str:
    """
    Validate table name to prevent SQL injection
    Only allows alphanumeric, underscore, and hyphen
    """
    if not re.match(r'^[a-zA-Z0-9_-]+$', table):
        raise ValidationError(
            f"Invalid table name: '{table}'. Only alphanumeric, underscore, and hyphen allowed."
        )
    return table


def validate_column_name(column: str) -> str:
    """
    Validate column name to prevent SQL injection
    Only allows alphanumeric and underscore
    """
    if not re.match(r'^[a-zA-Z0-9_]+$', column):
        raise ValidationError(
            f"Invalid column name: '{column}'. Only alphanumeric and underscore allowed."
        )
    return column


def validate_columns(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate all column names in data dictionary
    """
    for key in data.keys():
        validate_column_name(key)
    return data


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger, configuring it at most once.

    Adapters are constructed per request, so running the full setup on every
    call would (a) reset a level the embedding app deliberately set and (b)
    re-attach a duplicate plain-text handler — which is why "Initialized Azure
    Queue Storage client" spammed on every request in both plain and JSON form.

    Behaviour:
    * Idempotent — a given logger name is configured only once.
    * Host-managed root — when the embedding app owns root logging (it sets the
      ``ALTCODEPRO_ROOT_LOGGING_MANAGED`` sentinel), polydb attaches NO handler
      of its own and does NOT force a level. Records propagate to the host's
      formatter and honour whatever level the host pinned (so the host can
      quiet noisy adapters). This is the standard "library shouldn't seize
      logging" contract.
    * Standalone — with no host managing root, keep the original behaviour:
      set the level and install a plain StreamHandler.
    """
    logger = logging.getLogger(name)
    if name in _configured_loggers:
        return logger
    _configured_loggers.add(name)

    host_managed = bool(os.getenv("ALTCODEPRO_ROOT_LOGGING_MANAGED"))
    if host_managed:
        # Let the host own formatting and level; just propagate.
        return logger

    # Respect a level the caller already pinned explicitly on this logger.
    if logger.level == logging.NOTSET:
        logger.setLevel(level)

    # Clear existing handlers to avoid duplication in multiprocess scenarios
    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger