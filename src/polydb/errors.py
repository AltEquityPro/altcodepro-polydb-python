# src/polydb/errors.py
"""
Structured exceptions for cloud database operations
"""
from __future__ import annotations


class CloudDBError(Exception):
    """Base exception for all database errors"""

    pass


class DatabaseError(CloudDBError):
    """SQL database operation failed"""

    pass


class NoSQLError(CloudDBError):
    """NoSQL operation failed"""

    pass


class StorageError(CloudDBError):
    """Object storage operation failed"""

    pass


class QueueError(CloudDBError):
    """Queue operation failed"""

    pass


class ConnectionError(CloudDBError):
    """Connection to service failed"""

    pass


class ValidationError(CloudDBError):
    """Input validation failed"""

    pass

class PolyDBError(Exception):
    """Base exception for polydb."""

    pass


class ModelNotRegisteredError(PolyDBError):
    """Raised when a model has not been registered in the registry."""

    pass


class InvalidModelMetadataError(PolyDBError):
    """Raised when a model has invalid __polydb__ metadata."""

    pass


class UnsupportedStorageTypeError(PolyDBError):
    """Raised when the declared storage type is unknown/unsupported."""

    pass


class AdapterConfigurationError(PolyDBError):
    """Raised when the required adapter is missing or misconfigured."""

    pass


class OperationNotSupportedError(PolyDBError):
    """Raised when an adapter cannot perform a requested operation."""

    pass
