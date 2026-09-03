# src/polydb/__init__.py
"""
PolyDB - Enterprise Cloud-Independent Database Abstraction
Full LINQ support, field-level audit, cache, soft delete, overflow storage
"""

__version__ = "2.5.6"

from .cloudDatabaseFactory import CloudDatabaseFactory
from .databaseFactory import DatabaseFactory
from .models import CloudProvider, PartitionConfig, MongoConfig, CosmosMongoConfig
from .query import QueryBuilder, Operator
from .audit.context import AuditContext
from .cache import RedisCacheEngine as CacheEngine
from .errors import (
    CloudDBError,
    DatabaseError,
    NoSQLError,
    StorageError,
    QueueError,
    ConnectionError,
    ValidationError,
    PolyDBError,
    ModelNotRegisteredError,
    InvalidModelMetadataError,
    UnsupportedStorageTypeError,
    AdapterConfigurationError,
    OperationNotSupportedError,
    InsufficientBalanceError,
)

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
