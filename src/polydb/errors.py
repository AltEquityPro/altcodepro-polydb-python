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


class ConcurrencyError(CloudDBError):
    """A conditional write lost the race: the row's version (ETag) is no longer the one the caller read, or the row is gone.
    Re-read the row, re-apply the change and retry. Deliberately NOT a NoSQLError and never retried automatically."""

    def __init__(self, message: str = "etag mismatch: the row changed since it was read", *, missing: bool = False) -> None:
        super().__init__(message)
        self.missing = missing


class StorageError(CloudDBError):
    """Object storage operation failed"""

    pass


class QueueError(CloudDBError):
    """Queue operation failed"""

    pass


class ConnectionError(CloudDBError):
    """Connection to service failed"""

    pass


class CacheError(CloudDBError):
    """Cache/Redis operation failed, or no cache backend is configured"""

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


class InsufficientBalanceError(PolyDBError):
    """Raised when an atomic decrement is attempted but the balance is insufficient."""

    pass


class EncryptionConfigError(PolyDBError):
    """Raised when field encryption is requested but no usable key is configured."""

    pass
