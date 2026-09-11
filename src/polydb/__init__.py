# src/polydb/__init__.py
"""
PolyDB - Enterprise Cloud-Independent Database Abstraction
Full LINQ support, field-level audit, cache, soft delete, overflow storage
"""

__version__ = "2.5.11"

from .audit.context import AuditContext
from .cache import RedisCacheEngine as CacheEngine
from .cloudDatabaseFactory import CloudDatabaseFactory
from .databaseFactory import DatabaseFactory
from .errors import (
    AdapterConfigurationError,
    CloudDBError,
    ConnectionError,
    DatabaseError,
    InsufficientBalanceError,
    InvalidModelMetadataError,
    ModelNotRegisteredError,
    NoSQLError,
    OperationNotSupportedError,
    PolyDBError,
    QueueError,
    StorageError,
    UnsupportedStorageTypeError,
    ValidationError,
)
from .models import CloudProvider, CosmosMongoConfig, MongoConfig, PartitionConfig
from .overflow_gc import GCReport, sweep_overflow_blobs
from .query import Operator, QueryBuilder

__all__ = [
    # Factories
    "CloudDatabaseFactory",
    "DatabaseFactory",
    # Models & Config
    "CloudProvider",
    "PartitionConfig",
    "MongoConfig",
    "CosmosMongoConfig",
    # Query
    "QueryBuilder",
    "Operator",
    # Audit & Cache
    "AuditContext",
    "CacheEngine",
    # Overflow GC
    "sweep_overflow_blobs",
    "GCReport",
    # Errors
    "CloudDBError",
    "DatabaseError",
    "NoSQLError",
    "StorageError",
    "QueueError",
    "ConnectionError",
    "ValidationError",
    "PolyDBError",
    "ModelNotRegisteredError",
    "InvalidModelMetadataError",
    "UnsupportedStorageTypeError",
    "AdapterConfigurationError",
    "OperationNotSupportedError",
    "InsufficientBalanceError",
]
