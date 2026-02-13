# src/polydb/decorators.py
from __future__ import annotations

from typing import Type, TypeVar

from .registry import ModelRegistry

T = TypeVar("T", bound=type)


def polydb_model(cls: T) -> T:
    """
    Decorator to auto-register a model at import time.

    Usage:
        @polydb_model
        class UserEntity:
            __polydb__ = {"storage": "nosql"}
    """
    ModelRegistry.register(cls)
    return cls
