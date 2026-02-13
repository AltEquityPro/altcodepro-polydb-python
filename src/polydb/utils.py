# src/polydb/utils.py
"""
Utility functions for validation and logging
"""

import re
import logging
from typing import Dict, Any
from .errors import ValidationError


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
    """Setup logger with consistent format"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplication in multiprocess scenarios
    if logger.hasHandlers():
        logger.handlers.clear()
    
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger