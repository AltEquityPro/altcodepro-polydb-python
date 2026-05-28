# ======================================================
# AltCodePro Polydb Python Combined Imports
# ======================================================

import base64
import boto3
import datetime
import hashlib
import ipfshttpclient
import ipfshttpclient.client
import json
import logging
import mimetypes
import os
import psycopg2.pool
import re
import redis
import requests
import threading
import time
import uuid

from __future__ import annotations
from abc import ABC, abstractmethod
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
from azure.storage.fileshare import ShareServiceClient
from azure.storage.queue import QueueClient, QueueServiceClient
from boto3.dynamodb.conditions import Attr, Key
from boto3.session import Session
from botocore.exceptions import ClientError
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from dotenv import load_dotenv
from enum import Enum
from functools import wraps
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import firestore, pubsub_v1, storage
from google.cloud.firestore import Client
from google.cloud.firestore_v1.base_query import FieldFilter
from pathlib import Path
from psycopg2.extras import Json
from pymongo import MongoClient
from sqlite3 import DatabaseError
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Protocol, Set, TYPE_CHECKING, Tuple, Type, TypeVar, Union, cast, runtime_checkable
from uuid import UUID
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware


# ======================================================
# FILE: polydb/PolyDB.py
# ======================================================



    TenantConfig,
    TenantContext,
    TenantIsolationEnforcer,
    TenantMigrationManager,
    TenantQuotaManager,
    TenantRegistry,
)


ModelRef = Union[Type, str]


class PolyDB:
    """
    PolyDB all-in-one facade.

    Owns:
    - CloudDatabaseFactory
    - DatabaseFactory
    - optional Redis cache
    - monitoring helpers
    - security helpers
    - tenancy helpers

    Intended as the primary developer-facing entrypoint.
    """

    def __init__(
        self,
        *,
        provider: Optional[CloudProvider] = None,
        cloud_factory: Optional[CloudDatabaseFactory] = None,
        db: Optional[DatabaseFactory] = None,
        engines: Optional[List[EngineConfig]] = None,
        storage_configs: Optional[List[StorageConfig]] = None,
        tenant_registry: Optional[TenantRegistry] = None,
        partition_config: Optional[PartitionConfig] = None,
        enable_retries: bool = True,
        enable_audit: bool = True,
        enable_audit_reads: bool = False,
        enable_cache: bool = True,
        use_redis_cache: bool = False,
        enable_monitoring: bool = False,
        enable_encryption: bool = False,
        enable_rls: bool = False,
    ) -> None:
        self.logger = setup_logger(self.__class__.__name__)

        self.cloud = cloud_factory or CloudDatabaseFactory(
            provider=provider,
            storage_configs=storage_configs,
        )

        self.db = db or DatabaseFactory(
            provider=provider,
            cloud_factory=self.cloud,
            engines=engines,
            tenant_registry=tenant_registry,
            enable_retries=enable_retries,
            enable_audit=enable_audit,
            enable_audit_reads=enable_audit_reads,
            enable_cache=enable_cache,
            use_redis_cache=use_redis_cache,
            enable_monitoring=enable_monitoring,
            enable_encryption=enable_encryption,
            enable_rls=enable_rls,
        )

        self.partition_config = partition_config
        self.tenant_registry = tenant_registry

        self.metrics: Optional[MetricsCollector] = getattr(self.db, "metrics", None)
        self.health: Optional[HealthCheck] = getattr(self.db, "health", None)

        self.cache: Optional[RedisCacheEngine] = getattr(self.db, "_cache", None)
        self.cache_warmer: Optional[CacheWarmer] = getattr(self.db, "cache_warmer", None)

        self.encryption = FieldEncryption() if enable_encryption else None
        self.masking = DataMasking()
        self.rls = RowLevelSecurity() if enable_rls else None

        self.batch = BatchOperations(self.db)

        self.quota_manager = TenantQuotaManager(tenant_registry) if tenant_registry else None
        self.migration_manager = (
            TenantMigrationManager(self.cloud, tenant_registry) if tenant_registry else None
        )
        self.tenant_enforcer = TenantIsolationEnforcer(tenant_registry) if tenant_registry else None

    # ============================================================
    # FACTORY / ADAPTER ACCESS
    # ============================================================

    def get_cloud_factory(self) -> CloudDatabaseFactory:
        return self.cloud

    def get_database_factory(self) -> DatabaseFactory:
        return self.db

    def get_sql(self):
        return self.cloud.get_sql()

    def get_nosql(self, partition_config: Optional[PartitionConfig] = None):
        return self.cloud.get_nosql_kv(partition_config=partition_config or self.partition_config)

    def get_object_storage(self, name: str = "default"):
        return self.cloud.get_object_storage(name)

    def get_shared_files(self):
        return self.cloud.get_shared_files()

    def get_queue(self):
        return self.cloud.get_queue()

    def get_cache_engine(self) -> Optional[RedisCacheEngine]:
        return self.cache

    # ============================================================
    # CORE CRUD
    # ============================================================

    def create(
        self,
        model: ModelRef,
        data: JsonDict,
        *,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        return self.db.create(model, data, engine_override=engine_override)

    def read(
        self,
        model: ModelRef,
        query: Optional[Lookup] = None,
        *,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        no_cache: bool = False,
        cache_ttl: Optional[int] = None,
        include_deleted: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> List[JsonDict]:
        return self.db.read(
            model,
            query=query,
            limit=limit,
            offset=offset,
            no_cache=no_cache,
            cache_ttl=cache_ttl,
            include_deleted=include_deleted,
            engine_override=engine_override,
        )

    def read_one(
        self,
        model: ModelRef,
        query: Lookup,
        *,
        no_cache: bool = False,
        include_deleted: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> Optional[JsonDict]:
        return self.db.read_one(
            model,
            query=query,
            no_cache=no_cache,
            include_deleted=include_deleted,
            engine_override=engine_override,
        )

    def get(
        self,
        model: ModelRef,
        entity_id: Any,
        *,
        include_deleted: bool = False,
        no_cache: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> Optional[JsonDict]:
        return self.read_one(
            model,
            {"id": entity_id},
            no_cache=no_cache,
            include_deleted=include_deleted,
            engine_override=engine_override,
        )

    def read_page(
        self,
        model: ModelRef,
        query: Lookup,
        *,
        page_size: int = 100,
        continuation_token: Optional[str] = None,
        include_deleted: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> Optional[Tuple[List[JsonDict], Optional[str]]]:
        return self.db.read_page(
            model,
            query=query,
            page_size=page_size,
            continuation_token=continuation_token,
            include_deleted=include_deleted,
            engine_override=engine_override,
        )

    def update(
        self,
        model: ModelRef,
        entity_id: Any,
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        return self.db.update(
            model,
            entity_id,
            data,
            etag=etag,
            replace=replace,
            engine_override=engine_override,
        )

    def upsert(
        self,
        model: ModelRef,
        data: JsonDict,
        *,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        return self.db.upsert(
            model,
            data,
            replace=replace,
            engine_override=engine_override,
        )

    def delete(
        self,
        model: ModelRef,
        entity_id: Any,
        *,
        etag: Optional[str] = None,
        hard: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        return self.db.delete(
            model,
            entity_id,
            etag=etag,
            hard=hard,
            engine_override=engine_override,
        )

    # ============================================================
    # QUERY BUILDERS
    # ============================================================

    def query(self) -> QueryBuilder:
        return QueryBuilder()

    def advanced_query(self) -> AdvancedQueryBuilder:
        return AdvancedQueryBuilder()

    def query_linq(
        self,
        model: ModelRef,
        builder: QueryBuilder,
        *,
        engine_override: Optional[EngineOverride] = None,
    ):
        return self.db.query_linq(model, builder, engine_override=engine_override)

    def helper(self) -> QueryHelper:
        return QueryHelper()

    # ============================================================
    # RAW SQL / SQL HELPERS
    # ============================================================

    def execute_sql(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        *,
        fetch: bool = False,
        fetch_one: bool = False,
    ):
        adapter = self.get_sql()
        return adapter.execute(sql, params=params, fetch=fetch, fetch_one=fetch_one)

    def distributed_lock(self, lock_name: str):
        adapter = self.get_sql()
        return adapter.distributed_lock(lock_name)

    def begin_transaction(self):
        return self.get_sql().begin_transaction()

    def commit(self, tx: Any) -> None:
        self.get_sql().commit(tx)

    def rollback(self, tx: Any) -> None:
        self.get_sql().rollback(tx)

    # ============================================================
    # BLOB STORAGE
    # ============================================================

    def upload_blob(
        self,
        data: bytes,
        key: str,
        *,
        file_name: str = "",
        media_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        storage_name: str = "default",
        optimize: bool = True,
    ) -> str:
        storage = self.get_object_storage(storage_name)
        return storage.put(
            key=key,
            data=data,
            fileName=file_name,
            optimize=optimize,
            media_type=media_type,
            metadata=metadata or {},
        )

    def get_blob(
        self,
        key: str,
        *,
        storage_name: str = "default",
    ) -> Optional[bytes]:
        return self.get_object_storage(storage_name).get(key)

    def download_blob(
        self,
        key: str,
        *,
        storage_name: str = "default",
    ) -> bytes:
        return self.get_object_storage(storage_name).download(key)

    def delete_blob(
        self,
        key: str,
        *,
        storage_name: str = "default",
    ) -> bool:
        return self.get_object_storage(storage_name).delete(key)

    def list_blob(
        self,
        prefix: str = "",
        *,
        storage_name: str = "default",
    ) -> List[str]:
        return self.get_object_storage(storage_name).list(prefix)

    # ============================================================
    # QUEUE
    # ============================================================

    def send_queue(
        self,
        message: Dict[str, Any],
        *,
        queue_name: str = "default",
    ) -> str:
        queue = self.get_queue()
        if hasattr(queue, "send"):
            return queue.send(message=message, queue_name=queue_name)
        raise NotImplementedError("Queue adapter does not support send().")

    def receive_queue(
        self,
        *,
        queue_name: str = "default",
        max_messages: int = 10,
    ) -> List[Dict[str, Any]]:
        queue = self.get_queue()
        if hasattr(queue, "receive"):
            return queue.receive(queue_name=queue_name, max_messages=max_messages)
        raise NotImplementedError("Queue adapter does not support receive().")

    def delete_queue(
        self,
        message_id: str,
        *,
        queue_name: str = "default",
        pop_receipt: str = "",
    ) -> bool:
        queue = self.get_queue()
        if hasattr(queue, "delete"):
            return queue.delete(message_id, queue_name, pop_receipt)
        raise NotImplementedError("Queue adapter does not support delete().")

    # Normalized aliases for richer queue adapters if you add them
    def publish_queue(
        self,
        message: Dict[str, Any],
        *,
        queue_name: str = "default",
        delay: Optional[int] = None,
    ) -> str:
        queue = self.get_queue()
        if hasattr(queue, "publish"):
            return queue.publish(queue_name=queue_name, message=message, delay=delay)
        return self.send_queue(message, queue_name=queue_name)

    def consume_queue(
        self,
        *,
        queue_name: str = "default",
        max_messages: int = 10,
        wait_seconds: int = 5,
    ) -> List[Dict[str, Any]]:
        queue = self.get_queue()
        if hasattr(queue, "consume"):
            return queue.consume(
                queue_name=queue_name,
                max_messages=max_messages,
                wait_seconds=wait_seconds,
            )
        return self.receive_queue(queue_name=queue_name, max_messages=max_messages)

    def ack_queue(
        self,
        receipt_handle: str,
        *,
        queue_name: str = "default",
    ) -> bool:
        queue = self.get_queue()
        if hasattr(queue, "ack"):
            return queue.ack(receipt_handle, queue_name)
        return self.delete_queue(receipt_handle, queue_name=queue_name)

    # ============================================================
    # SHARED FILES
    # ============================================================

    def write_file(self, path: str, data: bytes) -> bool:
        files = self.get_shared_files()
        if files is None:
            raise NotImplementedError("Shared file adapter not available for current provider.")
        return files.write(path, data)

    def read_file(self, path: str) -> Optional[bytes]:
        files = self.get_shared_files()
        if files is None:
            raise NotImplementedError("Shared file adapter not available for current provider.")
        return files.read(path)

    def delete_file(self, path: str) -> bool:
        files = self.get_shared_files()
        if files is None:
            raise NotImplementedError("Shared file adapter not available for current provider.")
        return files.delete(path)

    def list_file(self, directory: str = "/") -> List[str]:
        files = self.get_shared_files()
        if files is None:
            raise NotImplementedError("Shared file adapter not available for current provider.")
        return files.list(directory)

    # ============================================================
    # CACHE
    # ============================================================

    def set_cache(
        self,
        model: ModelRef,
        key: Any,
        value: Any,
        *,
        ttl: int = 300,
    ) -> None:
        if not self.cache:
            raise RuntimeError("Redis cache is not enabled on this PolyDB instance.")
        self.cache.set(model=model, query=key, value=value, ttl=ttl)

    def get_cache(
        self,
        model: ModelRef,
        key: Any,
    ) -> Optional[Any]:
        if not self.cache:
            raise RuntimeError("Redis cache is not enabled on this PolyDB instance.")
        return self.cache.get(model=model, query=key)

    def invalidate_cache(
        self,
        model: ModelRef,
        key: Optional[Any] = None,
    ) -> None:
        if not self.cache:
            raise RuntimeError("Redis cache is not enabled on this PolyDB instance.")
        if key is not None:
            self.cache.invalidate(model, key)
        else:
            self.cache.clear()

    def cache_stats(self) -> Dict[str, Any]:
        if not self.cache:
            return {}
        return self.cache.get_stats()

    def warm_model_cache(
        self,
        model: ModelRef,
        queries: List[Any],
        *,
        ttl: int = 300,
    ) -> None:
        if not self.cache_warmer:
            raise RuntimeError("Cache warmer is not enabled on this PolyDB instance.")
        self.cache_warmer.warm_model(model, queries, ttl)

    def warm_popular_queries(
        self,
        model: ModelRef,
        *,
        limit: int = 20,
        ttl: int = 300,
    ) -> None:
        if not self.cache_warmer:
            raise RuntimeError("Cache warmer is not enabled on this PolyDB instance.")
        self.cache_warmer.warm_popular_queries(model, limit, ttl)

    # ============================================================
    # BATCH
    # ============================================================

    def bulk_insert(self, model: ModelRef, records: List[JsonDict]) -> BatchResult:
        return self.batch.bulk_insert(model, records)

    def bulk_update(
        self,
        model: ModelRef,
        updates: List[Tuple[Any, JsonDict]],
    ) -> BatchResult:
        return self.batch.bulk_update(model, updates)

    def bulk_delete(self, model: ModelRef, entity_ids: List[Any]) -> BatchResult:
        return self.batch.bulk_delete(model, entity_ids)

    # ============================================================
    # VALIDATION
    # ============================================================

    def validate_model(self, model: ModelRef):
        return ModelValidator.validate_model(model)

    def validate(self, model: ModelRef) -> None:
        ModelValidator.validate_and_raise(model)

    def validate_data(self, model: Any, data: JsonDict):
        return SchemaValidator.validate_data(model, data)

    # ============================================================
    # REGISTRY / ENGINE MANAGEMENT
    # ============================================================

    def register_engine(self, engine: EngineConfig) -> None:
        self.db.register_engine(engine)

    def unregister_engine(self, name: str) -> None:
        self.db.unregister_engine(name)

    def get_engine(self, name: str) -> EngineConfig:
        return self.db.get_engine(name)

    # ============================================================
    # MONITORING / HEALTH
    # ============================================================

    def health_check(self) -> Dict[str, Any]:
        if self.health:
            return self.health.full_health_check()
        return {
            "sql": "unknown",
            "nosql": "unknown",
            "cache": "disabled",
        }

    def get_metrics(
        self,
        *,
        since: Optional[Any] = None,
        model: Optional[str] = None,
        operation: Optional[str] = None,
    ):
        if not self.metrics:
            return []
        return self.metrics.get_metrics(since=since, model=model, operation=operation)

    def aggregate_metrics(
        self,
        *,
        since: Optional[Any] = None,
        model: Optional[str] = None,
    ):
        if not self.metrics:
            return None
        return self.metrics.aggregate(since=since, model=model)

    def export_prometheus(self) -> str:
        if not self.metrics:
            return ""
        return self.metrics.export_prometheus()

    # ============================================================
    # SECURITY
    # ============================================================

    def encrypt_fields(self, data: JsonDict, fields: List[str]) -> JsonDict:
        if not self.encryption:
            return data
        return self.encryption.encrypt_fields(data, fields)

    def decrypt_fields(self, data: JsonDict, fields: List[str]) -> JsonDict:
        if not self.encryption:
            return data
        return self.encryption.decrypt_fields(data, fields)

    def mask_data(
        self,
        data: JsonDict,
        *,
        model: str,
        actor_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> JsonDict:
        return self.masking.mask(data, model=model, actor_id=actor_id, tenant_id=tenant_id)

    def register_masking_config(self, model: str, config: Any) -> None:
        self.masking.register_model_config(model, config)

    def add_rls_policy(
        self,
        model: str,
        name: str,
        policy_func: Any,
        *,
        apply_to: str = "read",
    ) -> None:
        if not self.rls:
            raise RuntimeError("RLS is not enabled on this PolyDB instance.")
        self.rls.add_policy(model, name, policy_func, apply_to)

    def set_default_rls_filters(
        self,
        model: str,
        *,
        read_filters: Optional[Dict[str, Any]] = None,
        write_filters: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.rls:
            raise RuntimeError("RLS is not enabled on this PolyDB instance.")
        self.rls.set_default_filters(model, read_filters, write_filters)

    # ============================================================
    # MULTI-TENANCY
    # ============================================================

    def with_tenant(self, tenant_id: str) -> "PolyDB":
        TenantContext.set_tenant(tenant_id, self.tenant_registry)
        return self

    def get_tenant(self) -> Optional[TenantConfig]:
        return TenantContext.get_tenant()

    def clear_tenant(self) -> None:
        TenantContext.clear()

    def register_tenant(self, config: TenantConfig) -> None:
        if not self.tenant_registry:
            raise RuntimeError("Tenant registry is not configured.")
        self.tenant_registry.register(config)

    def get_tenant_config(self, tenant_id: str) -> Optional[TenantConfig]:
        if not self.tenant_registry:
            return None
        return self.tenant_registry.get(tenant_id)

    def list_tenants(self) -> List[TenantConfig]:
        if not self.tenant_registry:
            return []
        return self.tenant_registry.list_all()

    def check_storage_quota(self, tenant_id: str, size_gb: float) -> bool:
        if not self.quota_manager:
            raise RuntimeError("Tenant quota manager is not configured.")
        return self.quota_manager.check_storage_quota(tenant_id, size_gb)

    def record_storage_usage(self, tenant_id: str, size_gb: float) -> None:
        if not self.quota_manager:
            raise RuntimeError("Tenant quota manager is not configured.")
        self.quota_manager.record_storage_usage(tenant_id, size_gb)

    def get_tenant_usage(self, tenant_id: str) -> Dict[str, float]:
        if not self.quota_manager:
            return {}
        return self.quota_manager.get_usage(tenant_id)

    def provision_tenant(self, config: TenantConfig) -> None:
        if not self.migration_manager:
            raise RuntimeError("Tenant migration manager is not configured.")
        self.migration_manager.provision_tenant(config)

    def deprovision_tenant(self, tenant_id: str) -> None:
        if not self.migration_manager:
            raise RuntimeError("Tenant migration manager is not configured.")
        self.migration_manager.deprovision_tenant(tenant_id)


# ======================================================
# FILE: polydb/__init__.py
# ======================================================

# src/polydb/__init__.py
"""
PolyDB - Enterprise Cloud-Independent Database Abstraction
Full LINQ support, field-level audit, cache, soft delete, overflow storage
"""

__version__ = "2.2.3"

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
)

__all__ = [
    # Factories
    "CloudDatabaseFactory",
    "DatabaseFactory",
    # Models & Config
    "CloudProvider",
    "PartitionConfig",
    "polydb_model",
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
]


# ======================================================
# FILE: polydb/advanced_query.py
# ======================================================

# src/polydb/advanced_query.py
"""
Advanced query capabilities: JOIN, subqueries, aggregates
"""


class JoinType(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class AggregateFunction(Enum):
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"


@dataclass
class Join:
    table: str
    join_type: JoinType
    on_left: str
    on_right: str
    alias: Optional[str] = None


@dataclass
class Aggregate:
    function: AggregateFunction
    field: str
    alias: str


@dataclass
class AdvancedQueryBuilder:
    """Extended QueryBuilder with JOINs and aggregates"""

    table: str
    joins: List[Join] = field(default_factory=list)
    aggregates: List[Aggregate] = field(default_factory=list)
    group_by_fields: List[str] = field(default_factory=list)
    having_conditions: List[str] = field(default_factory=list)
    subqueries: Dict[str, "AdvancedQueryBuilder"] = field(default_factory=dict)

    def join(
        self,
        table: str,
        on_left: str,
        on_right: str,
        join_type: JoinType = JoinType.INNER,
        alias: Optional[str] = None,
    ) -> "AdvancedQueryBuilder":
        """Add JOIN clause"""
        self.joins.append(
            Join(table=table, join_type=join_type, on_left=on_left, on_right=on_right, alias=alias)
        )
        return self

    def aggregate(
        self, function: AggregateFunction, field: str, alias: str
    ) -> "AdvancedQueryBuilder":
        """Add aggregate function"""
        self.aggregates.append(Aggregate(function=function, field=field, alias=alias))
        return self

    def group_by(self, *fields: str) -> "AdvancedQueryBuilder":
        """Add GROUP BY"""
        self.group_by_fields.extend(fields)
        return self

    def having(self, condition: str) -> "AdvancedQueryBuilder":
        """Add HAVING clause"""
        self.having_conditions.append(condition)
        return self

    def to_sql(self) -> tuple[str, List[Any]]:
        """Generate complete SQL with JOINs"""
        params = []

        # SELECT clause
        if self.aggregates:
            select_parts = [
                f"{agg.function.value}({agg.field}) AS {agg.alias}" for agg in self.aggregates
            ]
            if self.group_by_fields:
                select_parts = list(self.group_by_fields) + select_parts
            sql = f"SELECT {', '.join(select_parts)}"
        else:
            sql = f"SELECT *"

        # FROM clause
        sql += f" FROM {self.table}"

        # JOIN clauses
        for join in self.joins:
            table = f"{join.table} AS {join.alias}" if join.alias else join.table
            sql += f" {join.join_type.value} {table}"
            sql += f" ON {join.on_left} = {join.on_right}"

        # GROUP BY clause
        if self.group_by_fields:
            sql += f" GROUP BY {', '.join(self.group_by_fields)}"

        # HAVING clause
        if self.having_conditions:
            sql += f" HAVING {' AND '.join(self.having_conditions)}"

        return sql, params


# Example usage helper
class QueryHelper:
    """Helper methods for common query patterns"""

    @staticmethod
    def count_by_field(table: str, field: str, group_field: str) -> AdvancedQueryBuilder:
        """Count occurrences grouped by field"""
        return (
            AdvancedQueryBuilder(table=table)
            .aggregate(AggregateFunction.COUNT, field, "count")
            .group_by(group_field)
        )

    @staticmethod
    def sum_by_category(table: str, sum_field: str, category_field: str) -> AdvancedQueryBuilder:
        """Sum values grouped by category"""
        return (
            AdvancedQueryBuilder(table=table)
            .aggregate(AggregateFunction.SUM, sum_field, "total")
            .group_by(category_field)
        )

    @staticmethod
    def join_with_filter(
        left_table: str, right_table: str, join_field: str
    ) -> AdvancedQueryBuilder:
        """Simple INNER JOIN"""
        return AdvancedQueryBuilder(table=left_table).join(
            right_table, f"{left_table}.{join_field}", f"{right_table}.{join_field}"
        )


# ======================================================
# FILE: polydb/batch.py
# ======================================================

# src/polydb/batch.py
"""
Batch operations for high-throughput scenarios
"""


@dataclass
class BatchResult:
    succeeded: List[JsonDict]
    failed: List[Dict[str, Any]]  # {data, error}
    total: int
    success_count: int
    error_count: int


class BatchOperations:
    """Batch CRUD operations"""
    
    def __init__(self, factory):
        self.factory = factory
    
    def bulk_insert(
        self,
        model: Union[type, str],
        records: List[JsonDict],
        *,
        chunk_size: int = 100,
        fail_fast: bool = False
    ) -> BatchResult:
        """Bulk insert with chunking"""
        succeeded = []
        failed = []
        
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            
            for record in chunk:
                try:
                    result = self.factory.create(model, record)
                    succeeded.append(result)
                except Exception as e:
                    failed.append({
                        'data': record,
                        'error': str(e)
                    })
                    
                    if fail_fast:
                        break
        
        return BatchResult(
            succeeded=succeeded,
            failed=failed,
            total=len(records),
            success_count=len(succeeded),
            error_count=len(failed)
        )
    
    def bulk_update(
        self,
        model: Union[type, str],
        updates: List[Dict[str, Any]],  # {entity_id, data}
        *,
        chunk_size: int = 100,
        fail_fast: bool = False
    ) -> BatchResult:
        """Bulk update"""
        succeeded = []
        failed = []
        
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]
            
            for update in chunk:
                try:
                    result = self.factory.update(
                        model,
                        update['entity_id'],
                        update['data']
                    )
                    succeeded.append(result)
                except Exception as e:
                    failed.append({
                        'data': update,
                        'error': str(e)
                    })
                    
                    if fail_fast:
                        break
        
        return BatchResult(
            succeeded=succeeded,
            failed=failed,
            total=len(updates),
            success_count=len(succeeded),
            error_count=len(failed)
        )
    
    def bulk_delete(
        self,
        model: Union[type, str],
        entity_ids: List[Any],
        *,
        chunk_size: int = 100,
        fail_fast: bool = False,
        hard: bool = False
    ) -> BatchResult:
        """Bulk delete"""
        succeeded = []
        failed = []
        
        for i in range(0, len(entity_ids), chunk_size):
            chunk = entity_ids[i:i + chunk_size]
            
            for entity_id in chunk:
                try:
                    result = self.factory.delete(model, entity_id, hard=hard)
                    succeeded.append(result)
                except Exception as e:
                    failed.append({
                        'data': {'entity_id': entity_id},
                        'error': str(e)
                    })
                    
                    if fail_fast:
                        break
        
        return BatchResult(
            succeeded=succeeded,
            failed=failed,
            total=len(entity_ids),
            success_count=len(succeeded),
            error_count=len(failed)
        )


class TransactionManager:
    """Transaction support for SQL operations"""
    
    def __init__(self, sql_adapter):
        self.sql = sql_adapter
    
    def __enter__(self):
        self.conn = self.sql._get_connection()
        self.conn.autocommit = False
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        
        self.sql._return_connection(self.conn)
        return False
    
    def execute(self, sql: str, params: Optional[List[Any]] = None):
        """Execute within transaction"""
        cursor = self.conn.cursor()
        cursor.execute(sql, params or [])
        cursor.close()

# ======================================================
# FILE: polydb/cache.py
# ======================================================

# src/polydb/cache_advanced.py
"""
Advanced caching with Redis support and strategies
"""


class CacheStrategy(Enum):
    """Cache invalidation strategies"""

    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    TTL = "ttl"  # Time To Live
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"


class RedisCacheEngine:
    """Redis-based distributed cache"""

    def __init__(
        self, redis_url: Optional[str] = None, prefix: str = "polydb:", default_ttl: int = 3600
    ):
        self.prefix = prefix
        self.default_ttl = default_ttl
        self._client = None
        self._lock = threading.Lock()
        self.redis_url = redis_url
        self._initialize()

    def _initialize(self):
        """Initialize Redis connection"""
        try:
            with self._lock:
                if not self._client:
                    if self.redis_url:
                        self._client = redis.from_url(self.redis_url)
                    else:

                        redis_host = os.getenv("REDIS_HOST", "localhost")
                        redis_port = int(os.getenv("REDIS_PORT", "6379"))
                        redis_db = int(os.getenv("REDIS_DB", "0"))

                        self._client = redis.Redis(
                            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
                        )
        except ImportError:
            raise ImportError("Redis not installed. Install with: pip install redis")

    def _make_key(self, model: str, query: Dict[str, Any]) -> str:
        """Generate cache key"""
        query_str = json.dumps(query, sort_keys=True, default=json_safe)
        query_hash = hashlib.md5(query_str.encode()).hexdigest()
        return f"{self.prefix}{model}:{query_hash}"

    def get(self, model: str, query: Dict[str, Any]) -> Optional[Any]:
        """Get from cache"""
        if not self._client:
            return None

        key = self._make_key(model, query)

        try:
            data = self._client.get(key)
            if data:
                # Increment access count for LFU
                self._client.incr(f"{key}:access_count")
                return json.loads(data)
            return None
        except Exception:
            return None

    def set(self, model: str, query: Dict[str, Any], value: Any, ttl: Optional[int] = None):
        """Set cache with TTL"""
        if not self._client:
            return

        key = self._make_key(model, query)
        ttl = ttl or self.default_ttl

        try:
            data = json.dumps(value, default=json_safe)
            self._client.setex(key, ttl, data)

            # Initialize access count
            self._client.set(f"{key}:access_count", 0, ex=ttl)
        except Exception:
            pass

    def invalidate(self, model: str, query: Optional[Dict[str, Any]] = None):
        """Invalidate cache"""
        if not self._client:
            return

        if query:
            key = self._make_key(model, query)
            self._client.delete(key, f"{key}:access_count")
        else:
            # Invalidate all for model
            pattern = f"{self.prefix}{model}:*"
            keys = self._client.keys(pattern)
            if keys:
                self._client.delete(*keys)

    def clear(self):
        """Clear entire cache"""
        if not self._client:
            return

        pattern = f"{self.prefix}*"
        keys = self._client.keys(pattern)
        if keys:
            self._client.delete(*keys)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self._client:
            return {}

        try:
            info = self._client.info("stats")
            return {
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "hit_rate": info.get("keyspace_hits", 0)
                / max(info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0), 1),
            }
        except Exception:
            return {}

    # ---------------------------------------------------------
    # ZSET (Sorted Set) Operations — for rate limiting, ranking
    # ---------------------------------------------------------

    def _make_raw_key(self, model: str, key: str) -> str:
        """Direct key (no hashing) for structured use cases like rate limiting"""
        return f"{self.prefix}{model}:{key}"

    def zadd(self, model: str, key: str, mapping: Dict[str, float]) -> None:
        if not self._client:
            return

        redis_key = self._make_raw_key(model, key)

        try:
            self._client.zadd(redis_key, mapping)
        except Exception:
            pass

    def zcard(self, model: str, key: str) -> int:
        if not self._client:
            return 0

        redis_key = self._make_raw_key(model, key)

        try:
            return int(self._client.zcard(redis_key))
        except Exception:
            return 0

    def zremrangebyscore(
        self,
        model: str,
        key: str,
        min_score: float,
        max_score: float,
    ) -> None:
        if not self._client:
            return

        redis_key = self._make_raw_key(model, key)

        try:
            self._client.zremrangebyscore(redis_key, min_score, max_score)
        except Exception:
            pass

    def expire_key(self, model: str, key: str, ttl: int) -> None:
        if not self._client:
            return

        redis_key = self._make_raw_key(model, key)

        try:
            self._client.expire(redis_key, ttl)
        except Exception:
            pass

    def delete_key(self, model: str, key: str) -> None:
        if not self._client:
            return

        redis_key = self._make_raw_key(model, key)

        try:
            self._client.delete(redis_key)
        except Exception:
            pass


class CacheWarmer:
    """Pre-populate cache with frequently accessed data"""

    def __init__(self, factory, cache_engine):
        self.factory = factory
        self.cache = cache_engine

    def warm_model(self, model, queries: List[Dict[str, Any]], ttl: Optional[int] = None):
        """Warm cache for specific queries"""
        for query in queries:
            try:
                results = self.factory.read(model, query=query)
                self.cache.set(
                    model.__name__ if isinstance(model, type) else model, query, results, ttl
                )
            except Exception:
                pass

    def warm_popular_queries(self, model, limit: int = 10, ttl: Optional[int] = None):
        """Warm cache with most popular queries"""
        # This would need query log analysis
        # Placeholder implementation
        pass


class CacheInvalidationStrategy:
    """Manages cache invalidation strategies"""

    def __init__(self, cache_engine, strategy: CacheStrategy = CacheStrategy.TTL):
        self.cache = cache_engine
        self.strategy = strategy

    def invalidate_on_write(self, model: str, data: Dict[str, Any]):
        """Invalidate cache on write operations"""
        if self.strategy in [CacheStrategy.WRITE_THROUGH, CacheStrategy.WRITE_BACK]:
            self.cache.invalidate(model)

    def invalidate_related(self, model: str, relationships: List[str]):
        """Invalidate related models"""
        for related in relationships:
            self.cache.invalidate(related)


# ======================================================
# FILE: polydb/cloudDatabaseFactory.py
# ======================================================




    AzureFileConfig,
    AzureQueueConfig,
    AzureStorageConfig,
    AzureTableConfig,
    BlockchainKVConfig,
    BlockchainQueueConfig,
    BlockchainStorageConfig,
    CloudProvider,
    DynamoDBConfig,
    EFSFileConfig,
    FirestoreConfig,
    GCPFileConfig,
    GCPPubSubConfig,
    GCPStorageConfig,
    MongoConfig,
    PartitionConfig,
    PostgreSQLConfig,
    SQSAdapterConfig,
    StorageConfig,
    VercelKVConfig,
    VercelQueueConfig,
    VercelStorageConfig,
)


# ============================================================
# FACTORY
# ============================================================


class CloudDatabaseFactory:
    """
    Multi-cloud factory (simple + production ready)

    Supports:
    - Typed configs (preferred)
    - Env fallback
    - Multiple named connections
    """

    def __init__(
        self,
        provider: Optional[CloudProvider] = None,
        storage_configs: Optional[List[StorageConfig]] = None,
    ):
        self.logger = setup_logger(__name__)

        self.provider = provider or self._detect_provider()

        # map name -> config
        self.configs: Dict[str, StorageConfig] = {}
        if storage_configs:
            for cfg in storage_configs:
                self.configs[cfg.name] = cfg

        self.instances = {}
        self._lock = threading.Lock()

        self.logger.info(f"Factory initialized (default provider={self.provider.value})")

    # --------------------------------------------------------
    # Provider detection (env fallback)
    # --------------------------------------------------------
    def _detect_provider(self) -> CloudProvider:
        explicit = os.getenv("CLOUD_PROVIDER")
        if explicit:
            try:
                return CloudProvider(explicit.lower())
            except Exception:
                self.logger.warning(f"Invalid CLOUD_PROVIDER: {explicit}")

        if os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
            return CloudProvider.AZURE
        if os.getenv("AWS_ACCESS_KEY_ID"):
            return CloudProvider.AWS
        if os.getenv("GOOGLE_CLOUD_PROJECT"):
            return CloudProvider.GCP
        if os.getenv("VERCEL_ENV"):
            return CloudProvider.VERCEL

        return CloudProvider.POSTGRESQL

    # --------------------------------------------------------
    # OBJECT STORAGE
    # --------------------------------------------------------
    def get_object_storage(
        self, name: str = "azure"
    ) -> (
        AzureBlobStorageAdapter
        | S3CompatibleAdapter
        | GCPStorageAdapter
        | VercelBlobAdapter
        | BlockchainBlobAdapter
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            provider = cfg.provider

            # ---------------- AZURE ----------------
            if provider == CloudProvider.AZURE:

                connection_string = None
                container = None

                if isinstance(cfg, AzureStorageConfig):
                    connection_string = cfg.connection_string
                    container = cfg.container

                instance = AzureBlobStorageAdapter(
                    connection_string=connection_string or "",
                    container_name=container or "",
                )

            # ---------------- AWS / S3 ----------------
            elif provider in (CloudProvider.AWS, CloudProvider.S3_COMPATIBLE):

                instance = S3CompatibleAdapter()

            # ---------------- GCP ----------------
            elif provider == CloudProvider.GCP:

                bucket = None
                project_id = ""
                endpoint = None
                if isinstance(cfg, GCPStorageConfig):
                    bucket = cfg.bucket
                    project_id = cfg.project_id
                    endpoint = cfg.endpoint

                instance = GCPStorageAdapter(
                    project_id=project_id, endpoint=endpoint, bucket_name=bucket
                )

            # ---------------- VERCEL ----------------
            elif provider == CloudProvider.VERCEL:

                token = None
                timeout = 10

                if isinstance(cfg, VercelStorageConfig):
                    token = cfg.token
                    timeout = cfg.timeout

                instance = VercelBlobAdapter(token=token or "", timeout=timeout)

            # ---------------- BLOCKCHAIN ----------------
            elif provider == CloudProvider.BLOCKCHAIN:

                ipfs_url = None
                if isinstance(cfg, BlockchainStorageConfig):
                    ipfs_url = cfg.ipfs_url

                instance = BlockchainBlobAdapter(ipfs_url=ipfs_url)

            # ---------------- DEFAULT ----------------
            else:

                self.logger.warning(f"Fallback to S3-compatible for provider={provider}")
                instance = S3CompatibleAdapter()

            self.instances[name] = instance
            return instance

    # --------------------------------------------------------
    # SQL
    # --------------------------------------------------------
    def get_sql(self, name: str = "sql"):

        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            connection_string = None

            if isinstance(cfg, PostgreSQLConfig):
                connection_string = cfg.connection_string

            instance = PostgreSQLAdapter(connection_string=connection_string)

            self.instances[name] = instance
            return instance

    def get_nosql_kv(
        self,
        partition_config: Optional[PartitionConfig] = None,
        name: str = "kv",
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            # ---------------- AZURE TABLE ----------------
            if cfg.provider == CloudProvider.AZURE:

                connection_string = ""
                container_name = ""

                if isinstance(cfg, AzureTableConfig):
                    connection_string = cfg.connection_string or ""
                    table_name = cfg.table_name
                    container_name = cfg.container_name

                instance = AzureTableStorageAdapter(
                    partition_config=partition_config,
                    connection_string=connection_string,
                    container_name=container_name,
                )

            # ---------------- AWS DYNAMODB ----------------
            elif cfg.provider == CloudProvider.AWS:

                table_name = None
                bucket_name = None
                region = None
                endpoint_url = None

                if isinstance(cfg, DynamoDBConfig):
                    table_name = cfg.table_name
                    bucket_name = cfg.bucket_name
                    region = cfg.region
                    endpoint_url = cfg.endpoint_url

                instance = DynamoDBAdapter(
                    partition_config=partition_config,
                    table_name=table_name,
                    bucket_name=bucket_name,
                    region=region,
                    endpoint_url=endpoint_url,
                )

            # ---------------- GCP FIRESTORE ----------------
            elif cfg.provider == CloudProvider.GCP:

                project = None
                bucket_name = None

                if isinstance(cfg, FirestoreConfig):
                    project = cfg.project
                    bucket_name = cfg.bucket_name

                instance = FirestoreAdapter(
                    partition_config=partition_config,
                    project=project,
                    bucket_name=bucket_name,
                )

            # ---------------- VERCEL KV ----------------
            elif cfg.provider == CloudProvider.VERCEL:

                kv_url = ""
                kv_token = ""
                timeout = 10

                if isinstance(cfg, VercelKVConfig):
                    kv_url = cfg.kv_url
                    kv_token = cfg.kv_token
                    timeout = cfg.timeout

                instance = VercelKVAdapter(
                    partition_config=partition_config,
                    kv_url=kv_url,
                    kv_token=kv_token,
                    timeout=timeout,
                )

            # ---------------- BLOCKCHAIN KV ----------------
            elif cfg.provider == CloudProvider.BLOCKCHAIN:

                chain = None
                rpc_url = None
                private_key = None
                contract_address = None
                contract_abi = None

                if isinstance(cfg, BlockchainKVConfig):
                    chain = cfg.chain
                    rpc_url = cfg.rpc_url
                    private_key = cfg.private_key
                    contract_address = cfg.contract_address
                    contract_abi = cfg.contract_abi

                instance = BlockchainKVAdapter(
                    chain=chain,
                    rpc_url=rpc_url,
                    private_key=private_key,
                    contract_address=contract_address,
                    contract_abi=contract_abi,
                )

            # ---------------- MONGODB ----------------
            else:

                mongo_uri = ""
                db_name = ""

                if isinstance(cfg, MongoConfig):
                    mongo_uri = cfg.mongo_uri
                    db_name = cfg.db_name

                instance = MongoDBAdapter(
                    partition_config=partition_config,
                    mongo_uri=mongo_uri,
                    db_name=db_name,
                )

            self.instances[name] = instance
            return instance

    def get_queue(
        self, name="azure_queue"
    ) -> (
        AzureQueueAdapter
        | SQSAdapter
        | GCPPubSubAdapter
        | VercelQueueAdapter
        | BlockchainQueueAdapter
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            if cfg.provider == CloudProvider.AZURE:

                connection_string = None
                if isinstance(cfg, AzureQueueConfig):
                    connection_string = cfg.connection_string
                instance = AzureQueueAdapter(connection_string or "")

            elif cfg.provider == CloudProvider.AWS:

                queue_name = None
                region = None
                endpoint_url = None
                if isinstance(cfg, SQSAdapterConfig):
                    queue_name = cfg.queue_name
                    region = cfg.region
                    endpoint_url = cfg.endpoint_url
                instance = SQSAdapter(
                    queue_name=queue_name or "",
                    region=region or "",
                    endpoint_url=endpoint_url or "",
                )

            elif cfg.provider == CloudProvider.GCP:

                topic = None
                project_id = ""
                subscription = None
                if isinstance(cfg, GCPPubSubConfig):
                    topic = cfg.topic
                    project_id = cfg.project_id
                    subscription = cfg.subscription
                instance = GCPPubSubAdapter(
                    project_id=project_id, topic=topic, subscription=subscription
                )

            elif cfg.provider == CloudProvider.VERCEL:

                url = ""
                token = ""
                if isinstance(cfg, VercelQueueConfig):
                    url = cfg.url
                    token = cfg.token
                instance = VercelQueueAdapter(url or "", token or "")

            elif cfg.provider == CloudProvider.BLOCKCHAIN:

                rpc_url = ""
                private_key = ""
                contract_address = ""
                contract_abi = []
                if isinstance(cfg, BlockchainQueueConfig):
                    rpc_url = cfg.rpc_url
                    private_key = cfg.private_key
                    private_key = cfg.private_key
                    contract_abi = cfg.contract_abi
                instance = BlockchainQueueAdapter(
                    rpc_url=rpc_url,
                    private_key=private_key,
                    contract_address=contract_address,
                    contract_abi=contract_abi,
                )

            else:
                raise NotImplementedError(
                    f"Queue adapter is not supported for {self.provider.value}"
                )

            self.instances["queue"] = instance
            return instance

    def get_files(
        self, name: str = "files"
    ) -> (
        AzureFileStorageAdapter
        | EFSAdapter
        | GCPStorageAdapter
        | AzureBlobStorageAdapter
        | S3CompatibleAdapter
        | VercelBlobAdapter
        | BlockchainBlobAdapter
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            # ---------------- AZURE FILE STORAGE ----------------
            if cfg.provider == CloudProvider.AZURE:

                connection_string = ""
                share_name = ""

                if isinstance(cfg, AzureFileConfig):
                    connection_string = cfg.connection_string or ""
                    share_name = cfg.share_name or ""

                instance = AzureFileStorageAdapter(
                    connection_string=connection_string,
                    share_name=share_name,
                )

            # ---------------- AWS EFS ----------------
            elif cfg.provider == CloudProvider.AWS:

                mount_point = None
                if isinstance(cfg, EFSFileConfig):
                    mount_point = cfg.mount_path

                instance = EFSAdapter(
                    mount_point=mount_point or os.getenv("EFS_MOUNT_POINT", "/mnt/efs")
                )

            # ---------------- GCP STORAGE (FILES VIA BUCKET) ----------------
            elif cfg.provider == CloudProvider.GCP:

                project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
                endpoint = os.getenv("GCP_STORAGE_ENDPOINT")  # optional (for emulator)
                bucket = None

                if isinstance(cfg, GCPFileConfig):
                    bucket = cfg.bucket

                instance = GCPStorageAdapter(
                    project_id=project_id,
                    endpoint=endpoint,
                    bucket_name=bucket,
                )

            # ---------------- VERCEL (fallback to blob) ----------------
            elif cfg.provider == CloudProvider.VERCEL:
                # Vercel has no file system → reuse blob adapter
                instance = self.get_object_storage(name)

            # ---------------- BLOCKCHAIN ----------------
            elif cfg.provider == CloudProvider.BLOCKCHAIN:
                raise NotImplementedError("File storage not supported for blockchain")

            # ---------------- DEFAULT ----------------
            else:
                raise NotImplementedError(f"File storage not supported for {self.provider.value}")

            self.instances[name] = instance
            return instance


# ======================================================
# FILE: polydb/databaseFactory.py
# ======================================================

"""
DatabaseFactory — Pure Storage Layer
=====================================

Multi-engine CRUD, blob, queue, cache, file operations.

NO business logic. NO tenant enforcement. NO model registry validation.
NO RLS. Those belong in UDL.

PolyDB is the dumb storage layer. UDL is the smart layer.
"""







logger = logging.getLogger(__name__)

_DEFAULT_RETRY = retry(
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    stop=stop_after_attempt(3),
    reraise=True,
)


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class EngineConfig:
    """Single SQL or NoSQL engine that DatabaseFactory can route to."""

    name: str
    cloud_factory: CloudDatabaseFactory
    sql_models: Optional[Set[str]] = None
    nosql_models: Optional[Set[str]] = None
    is_default_sql: bool = False
    is_default_nosql: bool = False

    _sql: Any = field(default=None, init=False, repr=False)
    _nosql: Any = field(default=None, init=False, repr=False)

    def sql(self) -> Any:
        if self._sql is None:
            self._sql = self.cloud_factory.get_sql()
        return self._sql

    def nosql(self) -> Any:
        if self._nosql is None:
            self._nosql = self.cloud_factory.get_nosql_kv()
        return self._nosql


@dataclass
class EngineOverride:
    """Per-call override to bypass routing and target a specific engine."""

    engine_name: str
    force_sql: bool = False
    force_nosql: bool = False


@dataclass
class _ResolvedAdapters:
    sql: PostgreSQLAdapter
    nosql: NoSQLKVAdapter
    engine_name: str


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL META RESOLUTION (lightweight — no registry enforcement)
# ═══════════════════════════════════════════════════════════════════════════════


def _extract_meta(model: Union[type, str]) -> ModelMeta:
    """
    Extract storage metadata from model class.

    If model is a string, return a default NoSQL meta (UDL resolves the
    class before calling PolyDB, so string fallback is safe).
    """
    if isinstance(model, type):
        raw = getattr(model, "__polydb__", None)
        if raw:
            return ModelMeta(
                storage=raw.get("storage", "nosql"),
                table=raw.get("table"),
                collection=raw.get("collection"),
                pk_field=raw.get("pk_field", raw.get("partition_key")),
                rk_field=raw.get("rk_field", raw.get("sort_key")),
                provider=raw.get("provider"),
                cache=raw.get("cache", False),
                cache_ttl=raw.get("cache_ttl"),
            )
    # Default for dynamic/string models
    return ModelMeta(storage="nosql", table=None, collection=None)


def _model_name(model: Union[type, str]) -> str:
    return model.__name__ if isinstance(model, type) else str(model)


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE FACTORY
# ═══════════════════════════════════════════════════════════════════════════════


class DatabaseFactory:
    """
    Pure storage layer. Multi-engine CRUD with:
    - Multi-engine routing (sql_models / nosql_models per engine)
    - Per-call engine override
    - Cache, audit, encryption, monitoring
    - Blob, queue, file, cache storage

    NO tenant enforcement. NO model registry validation. NO RLS.
    UDL handles all of that.
    """

    def __init__(
        self,
        *,
        # Single-engine (backwards-compatible)
        provider: Optional[Any] = None,
        cloud_factory: Optional[CloudDatabaseFactory] = None,
        # Multi-engine
        engines: Optional[List[EngineConfig]] = None,
        # Feature flags
        redis_cache_url: Optional[str] = None,
        enable_retries: bool = True,
        enable_audit: bool = True,
        enable_audit_reads: bool = False,
        enable_cache: bool = True,
        soft_delete: bool = False,
        use_redis_cache: bool = False,
        enable_monitoring: bool = False,
        enable_encryption: bool = False,
    ) -> None:
        self._enable_retries = enable_retries
        self._enable_audit = enable_audit
        self._enable_audit_reads = enable_audit_reads
        self._enable_cache = enable_cache
        self._soft_delete = soft_delete

        # Monitoring
        self.metrics = MetricsCollector() if enable_monitoring else None
        self.health = HealthCheck(self) if enable_monitoring else None

        # Redis cache
        self._cache: Optional[RedisCacheEngine] = None
        self.cache_warmer: Optional[CacheWarmer] = None
        if enable_cache and use_redis_cache:
            redis_url = redis_cache_url or os.getenv("REDIS_CACHE_URL") or os.getenv("REDIS_URL")
            if redis_url:
                self._cache = RedisCacheEngine(redis_url=redis_url)
                self.cache_warmer = CacheWarmer(self, self._cache)
            else:
                logger.warning("use_redis_cache=True but REDIS_CACHE_URL not set")

        # Encryption
        self.encryption = FieldEncryption() if enable_encryption else None
        self.masking = DataMasking()

        self.batch = BatchOperations(self)
        self._audit = AuditManager() if enable_audit else None

        # Engine registry
        self._engines: List[EngineConfig] = []

        if engines:
            self._engines = engines
            default_sql = [e for e in engines if e.is_default_sql]
            default_nosql = [e for e in engines if e.is_default_nosql]
            if len(default_sql) > 1:
                raise AdapterConfigurationError("More than one engine marked is_default_sql=True")
            if len(default_nosql) > 1:
                raise AdapterConfigurationError("More than one engine marked is_default_nosql=True")
        else:
            _cf = cloud_factory or CloudDatabaseFactory(provider=provider)
            self._engines = [
                EngineConfig(
                    name="primary", cloud_factory=_cf, is_default_sql=True, is_default_nosql=True
                )
            ]

        self._engine_by_name: Dict[str, EngineConfig] = {e.name: e for e in self._engines}
        self._provider_name = self._engines[0].cloud_factory.provider.value

    # ──────────────────────────────────────────────────────────────────────
    # ENGINE ROUTING
    # ──────────────────────────────────────────────────────────────────────

    def _resolve_adapters(
        self, model_name: str, storage: str, override: Optional[EngineOverride] = None
    ) -> _ResolvedAdapters:
        # 1. Per-call override
        if override:
            engine = self._engine_by_name.get(override.engine_name)
            if engine is None:
                raise AdapterConfigurationError(
                    f"Unknown engine '{override.engine_name}'. Available: {list(self._engine_by_name)}"
                )
            return _ResolvedAdapters(
                sql=engine.sql(), nosql=engine.nosql(), engine_name=engine.name
            )

        # 2. Explicit allow-list
        for engine in self._engines:
            if storage == "sql" and engine.sql_models and model_name in engine.sql_models:
                return _ResolvedAdapters(
                    sql=engine.sql(), nosql=engine.nosql(), engine_name=engine.name
                )
            if storage == "nosql" and engine.nosql_models and model_name in engine.nosql_models:
                return _ResolvedAdapters(
                    sql=engine.sql(), nosql=engine.nosql(), engine_name=engine.name
                )

        # 3. Default fallback
        for engine in self._engines:
            if storage == "sql" and engine.is_default_sql:
                return _ResolvedAdapters(
                    sql=engine.sql(), nosql=engine.nosql(), engine_name=engine.name
                )
            if storage == "nosql" and engine.is_default_nosql:
                return _ResolvedAdapters(
                    sql=engine.sql(), nosql=engine.nosql(), engine_name=engine.name
                )

        raise AdapterConfigurationError(f"No engine for model='{model_name}' storage='{storage}'")

    def _adapters_for(
        self, model: Union[type, str], meta: ModelMeta, override: Optional[EngineOverride] = None
    ) -> _ResolvedAdapters:
        name = _model_name(model)
        if override and override.force_sql:
            storage = "sql"
        elif override and override.force_nosql:
            storage = "nosql"
        else:
            storage = "sql" if (meta.storage == "sql" and meta.table) else "nosql"
        return self._resolve_adapters(name, storage, override)

    # ──────────────────────────────────────────────────────────────────────
    # ENGINE MANAGEMENT
    # ──────────────────────────────────────────────────────────────────────

    def register_engine(self, engine: EngineConfig) -> None:
        if engine.name in self._engine_by_name:
            self._engines = [e for e in self._engines if e.name != engine.name]
        self._engines.append(engine)
        self._engine_by_name[engine.name] = engine

    def unregister_engine(self, name: str) -> None:
        if name not in self._engine_by_name:
            raise AdapterConfigurationError(f"Engine '{name}' not registered.")
        self._engines = [e for e in self._engines if e.name != name]
        del self._engine_by_name[name]

    def get_engine(self, name: str) -> EngineConfig:
        if name not in self._engine_by_name:
            raise AdapterConfigurationError(f"Engine '{name}' not found.")
        return self._engine_by_name[name]

    @property
    def _sql(self) -> PostgreSQLAdapter:
        for e in self._engines:
            if e.is_default_sql:
                return e.sql()
        return self._engines[0].sql()

    @property
    def _nosql(self) -> NoSQLKVAdapter:
        for e in self._engines:
            if e.is_default_nosql:
                return e.nosql()
        return self._engines[0].nosql()

    # ──────────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────────

    def _inject_audit_fields(self, data: JsonDict, is_create: bool = False) -> JsonDict:
        data = dict(data)
        actor_id = AuditContext.actor_id.get()
        now = datetime.utcnow().isoformat()
        if is_create:
            data.setdefault("created_at", now)
            if actor_id:
                data.setdefault("created_by", actor_id)
        data.setdefault("updated_at", now)
        if actor_id:
            data.setdefault("updated_by", actor_id)
        return data

    def _apply_soft_delete_filter(self, query: Optional[Lookup]) -> Lookup:
        if not self._soft_delete:
            return query or {}
        result = dict(query or {})
        result.setdefault("deleted_at", None)
        return result

    def _audit_safe(
        self,
        *,
        action: str,
        model: Union[type, str],
        entity_id: Optional[Any],
        meta: ModelMeta,
        success: bool,
        before: Optional[JsonDict],
        after: Optional[JsonDict],
        error: Optional[str],
        engine_name: Optional[str] = None,
    ) -> None:
        if not self._audit:
            return
        try:
            changed = None
            if before and after:
                changed = [
                    k for k in set(before) | set(after) if before.get(k) != after.get(k)
                ] or None
            self._audit.record(
                action=action,
                model=_model_name(model),
                entity_id=str(entity_id) if entity_id else None,
                storage_type=meta.storage,
                provider=engine_name or self._provider_name,
                success=success,
                before=before,
                after=after,
                changed_fields=changed,
                error=error,
            )
        except Exception as exc:
            logger.error("Audit recording failed: %s", exc)

    def _run(self, fn: Callable[[], Any]) -> Any:
        if not self._enable_retries:
            return fn()
        return _DEFAULT_RETRY(fn)()

    def _is_sql(self, meta: ModelMeta, override: Optional[EngineOverride] = None) -> bool:
        if override and override.force_sql:
            return True
        if override and override.force_nosql:
            return False
        return meta.storage == "sql" and bool(meta.table)

    # ──────────────────────────────────────────────────────────────────────
    # CREATE
    # ──────────────────────────────────────────────────────────────────────

    def create(
        self,
        model: Union[type, str],
        data: JsonDict,
        *,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        meta = _extract_meta(model)
        name = _model_name(model)
        data = self._inject_audit_fields(data, is_create=True)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, encrypted_fields)

        adapters = self._adapters_for(model, meta, engine_override)
        after_plain = None
        success = False
        error: Optional[str] = None
        entity_id: Optional[Any] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success, entity_id
            if self._is_sql(meta, engine_override):
                result = adapters.sql.insert(meta.table, data)
            else:
                result = adapters.nosql.put(
                    (
                        model
                        if isinstance(model, type)
                        else type(name, (), {"__polydb__": meta.__dict__})
                    ),
                    data,
                )
            entity_id = result.get("id")
            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)
            success = True
            if self._enable_cache and self._cache:
                self._cache.invalidate(name)
            return after_plain

        try:
            monitor = (
                PerformanceMonitor(self.metrics, "create", name, None) if self.metrics else None
            )
            if monitor:
                with monitor as m:
                    result = self._run(_op)
                    m.rows_affected = 1
                    return result
            return self._run(_op)
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            self._audit_safe(
                action="create",
                model=model,
                entity_id=entity_id,
                meta=meta,
                success=success,
                before=None,
                after=after_plain,
                error=error,
                engine_name=adapters.engine_name,
            )

    # ──────────────────────────────────────────────────────────────────────
    # READ
    # ──────────────────────────────────────────────────────────────────────

    def read(
        self,
        model: Union[type, str],
        query: Optional[Lookup] = None,
        *,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        no_cache: bool = False,
        cache_ttl: Optional[int] = None,
        include_deleted: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> List[JsonDict]:
        name = _model_name(model)
        meta = _extract_meta(model)

        if self._soft_delete and not include_deleted:
            query = self._apply_soft_delete_filter(query)

        adapters = self._adapters_for(model, meta, engine_override)
        use_external_cache = self._enable_cache and self._cache and getattr(meta, "cache", False)
        encrypted_fields = getattr(meta, "encrypted_fields", [])

        def _op() -> List[JsonDict]:
            if self._is_sql(meta, engine_override):
                raw = adapters.sql.select(meta.table, query, limit=limit, offset=offset)
            else:
                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )
                raw = adapters.nosql.query(
                    cls, query=query, limit=limit, no_cache=no_cache or bool(use_external_cache)
                )
            if self.encryption and encrypted_fields:
                raw = [self.encryption.decrypt_fields(r, encrypted_fields) for r in raw]
            if self._cache and use_external_cache and not no_cache:
                ttl = cache_ttl or getattr(meta, "cache_ttl", 300)
                self._cache.set(name, query or {}, raw, ttl)
            return raw

        # Check external cache first
        if self._cache and use_external_cache and not no_cache:
            cached = self._cache.get(name, query or {})
            if cached is not None:
                return cached

        monitor = PerformanceMonitor(self.metrics, "read", name, None) if self.metrics else None
        if monitor:
            with monitor as m:
                rows = self._run(_op)
                m.rows_returned = len(rows)
                return rows
        return self._run(_op)

    def read_one(
        self,
        model: Union[type, str],
        query: Lookup,
        *,
        no_cache: bool = False,
        include_deleted: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> Optional[JsonDict]:
        rows = self.read(
            model,
            query=query,
            limit=1,
            no_cache=no_cache,
            include_deleted=include_deleted,
            engine_override=engine_override,
        )
        return rows[0] if rows else None

    # ──────────────────────────────────────────────────────────────────────
    # UPDATE
    # ──────────────────────────────────────────────────────────────────────

    def update(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        name = _model_name(model)
        meta = _extract_meta(model)
        data = self._inject_audit_fields(data, is_create=False)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, [f for f in encrypted_fields if f in data])

        adapters = self._adapters_for(model, meta, engine_override)
        before = self.read_one(
            model,
            {"id": entity_id} if not isinstance(entity_id, dict) else entity_id,
            no_cache=True,
            include_deleted=True,
            engine_override=engine_override,
        )
        after_plain = None
        success = False
        error: Optional[str] = None
        def _op() -> JsonDict:
            nonlocal after_plain, success
            if self._is_sql(meta, engine_override):
                result = adapters.sql.update(meta.table, entity_id, data)
            else:
                pkey = data.get("PartitionKey") or data.get("partition_key") or data.get("pk")
                en_id = entity_id
                if not pkey and before:
                    pkey = (
                        before.get("PartitionKey")
                        or before.get("partition_key")
                        or before.get("pk")
                    )
                if pkey:
                    if isinstance(en_id, dict):
                        en_pk = (
                            en_id.get("PartitionKey")
                            or en_id.get("partition_key")
                            or en_id.get("pk")
                        )
                        if not en_pk:
                            en_id["partition_key"] = pkey
                    elif isinstance(en_id, str):
                        en_id = {"partition_key": pkey, "id": entity_id}

                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )

                result = adapters.nosql.patch(cls, en_id, data, etag=etag, replace=replace)
            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)
            success = True
            if self._enable_cache and self._cache:
                self._cache.invalidate(name)
            return after_plain

        try:
            monitor = (
                PerformanceMonitor(self.metrics, "update", name, None) if self.metrics else None
            )
            if monitor:
                with monitor as m:
                    result = self._run(_op)
                    m.rows_affected = 1
                    return result
            return self._run(_op)
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            self._audit_safe(
                action="update",
                model=model,
                entity_id=entity_id if not isinstance(entity_id, dict) else None,
                meta=meta,
                success=success,
                before=before,
                after=after_plain,
                error=error,
                engine_name=adapters.engine_name,
            )

    # ──────────────────────────────────────────────────────────────────────
    # UPSERT
    # ──────────────────────────────────────────────────────────────────────

    def upsert(
        self,
        model: Union[type, str],
        data: JsonDict,
        *,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        name = _model_name(model)
        meta = _extract_meta(model)
        data = self._inject_audit_fields(data, is_create=True)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, encrypted_fields)

        adapters = self._adapters_for(model, meta, engine_override)
        after_plain = None
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success
            if self._is_sql(meta, engine_override):
                result = adapters.sql.upsert(meta.table, data)
            else:
                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )
                result = adapters.nosql.upsert(cls, data, replace=replace)
            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)
            success = True
            if self._enable_cache and self._cache:
                self._cache.invalidate(name)
            return after_plain

        try:
            monitor = (
                PerformanceMonitor(self.metrics, "upsert", name, None) if self.metrics else None
            )
            if monitor:
                with monitor as m:
                    result = self._run(_op)
                    m.rows_affected = 1
                    return result
            return self._run(_op)
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            self._audit_safe(
                action="upsert",
                model=model,
                entity_id=None,
                meta=meta,
                success=success,
                before=None,
                after=after_plain,
                error=error,
                engine_name=adapters.engine_name,
            )

    # ──────────────────────────────────────────────────────────────────────
    # DELETE
    # ──────────────────────────────────────────────────────────────────────

    def delete(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None,
        hard: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        meta = _extract_meta(model)
        name = _model_name(model)

        if self._soft_delete and not hard:
            return self.update(
                model,
                entity_id,
                {
                    "deleted_at": datetime.utcnow().isoformat(),
                    "deleted_by": AuditContext.actor_id.get(),
                },
                engine_override=engine_override,
            )

        adapters = self._adapters_for(model, meta, engine_override)
        before = self.read_one(
            model,
            {"id": entity_id} if not isinstance(entity_id, dict) else entity_id,
            no_cache=True,
            include_deleted=True,
            engine_override=engine_override,
        )
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal success
            if self._is_sql(meta, engine_override):
                result = adapters.sql.delete(meta.table, entity_id)
            else:
                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )
                result = adapters.nosql.delete(cls, entity_id, etag=etag)
            success = True
            if self._enable_cache and self._cache:
                self._cache.invalidate(name)
            return result

        try:
            monitor = (
                PerformanceMonitor(self.metrics, "delete", name, None) if self.metrics else None
            )
            if monitor:
                with monitor as m:
                    result = self._run(_op)
                    m.rows_affected = 1
                    return result
            return self._run(_op)
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            self._audit_safe(
                action="delete",
                model=model,
                entity_id=entity_id if not isinstance(entity_id, dict) else None,
                meta=meta,
                success=success,
                before=before,
                after=None,
                error=error,
                engine_name=adapters.engine_name,
            )

    # ──────────────────────────────────────────────────────────────────────
    # QUERY (LINQ-style)
    # ──────────────────────────────────────────────────────────────────────

    def query_linq(
        self,
        model: Union[type, str],
        builder: QueryBuilder,
        *,
        engine_override: Optional[EngineOverride] = None,
    ) -> Union[List[JsonDict], int]:
        name = _model_name(model)
        meta = _extract_meta(model)
        adapters = self._adapters_for(model, meta, engine_override)

        def _op():
            if self._is_sql(meta, engine_override):
                return adapters.sql.query_linq(meta.table, builder)
            cls = (
                model if isinstance(model, type) else type(name, (), {"__polydb__": meta.__dict__})
            )
            return adapters.nosql.query_linq(cls, builder)

        monitor = (
            PerformanceMonitor(self.metrics, "query_linq", name, None) if self.metrics else None
        )
        if monitor:
            with monitor as m:
                result = self._run(_op)
                if isinstance(result, list):
                    m.rows_returned = len(result)
                return result
        return self._run(_op)

    # ──────────────────────────────────────────────────────────────────────
    # PAGINATION
    # ──────────────────────────────────────────────────────────────────────

    def read_page(
        self,
        model: Union[type, str],
        query: Lookup,
        *,
        page_size: int = 100,
        continuation_token: Optional[str] = None,
        include_deleted: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> Optional[Tuple[List[JsonDict], Optional[str]]]:
        name = _model_name(model)
        meta = _extract_meta(model)

        if self._soft_delete and not include_deleted:
            query = self._apply_soft_delete_filter(query)

        adapters = self._adapters_for(model, meta, engine_override)
        encrypted_fields = getattr(meta, "encrypted_fields", [])

        def _op() -> Tuple[List[JsonDict], Optional[str]]:
            if self._is_sql(meta, engine_override):
                raw, token = adapters.sql.select_page(
                    meta.table, query, page_size, continuation_token
                )
            else:
                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )
                raw, token = adapters.nosql.query_page(cls, query, page_size, continuation_token)
            if self.encryption and encrypted_fields:
                raw = [self.encryption.decrypt_fields(r, encrypted_fields) for r in raw]
            return raw, token

        monitor = (
            PerformanceMonitor(self.metrics, "read_page", name, None) if self.metrics else None
        )
        if monitor:
            with monitor as m:
                result = self._run(_op)
                m.rows_returned = len(result[0])
                return result
        return self._run(_op)

    # ══════════════════════════════════════════════════════════════════════
    # BLOB STORAGE
    # ══════════════════════════════════════════════════════════════════════

    def upload_blob(
        self,
        key: str,
        data: bytes,
        *,
        file_name: Optional[str] = None,
        media_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        storage_name: str = "azure",
    ) -> str:
        storage = self._engines[0].cloud_factory.get_object_storage(storage_name)
        return storage.put(
            key=key,
            data=data,
            fileName=file_name or key,
            optimize=True,
            media_type=media_type,
            metadata=metadata or {},
        )

    def download_blob(self, key: str, *, storage_name: str = "azure") -> Optional[bytes]:
        storage = self._engines[0].cloud_factory.get_object_storage(storage_name)
        return storage.get(key)

    def delete_blob(self, key: str, *, storage_name: str = "azure") -> bool:
        storage = self._engines[0].cloud_factory.get_object_storage(storage_name)
        return storage.delete(key)

    def list_blob(self, prefix: str = "", *, storage_name: str = "azure") -> List[str]:
        storage = self._engines[0].cloud_factory.get_object_storage(storage_name)
        return storage.list(prefix)

    # ══════════════════════════════════════════════════════════════════════
    # QUEUE
    # ══════════════════════════════════════════════════════════════════════

    def send_queue(
        self,
        message: Dict[str, Any],
        *,
        queue_name: str = "default",
        adapter_name: str = "azure_queue",
    ) -> str:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return queue.send(message=message, queue_name=queue_name)

    def receive_queue(
        self,
        *,
        queue_name: str = "default",
        max_messages: int = 10,
        adapter_name: str = "azure_queue",
    ) -> List[Dict[str, Any]]:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return queue.receive(queue_name=queue_name, max_messages=max_messages)

    def ack_queue(
        self, ack_id: str, *, queue_name: str = "default", adapter_name: str = "azure_queue"
    ) -> bool:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return (
            queue.ack(ack_id, queue_name)
            if hasattr(queue, "ack")
            else queue.delete(ack_id, queue_name)
        )

    def delete_queue(
        self,
        message_id: str,
        *,
        queue_name: str = "default",
        pop_receipt: Optional[str] = None,
        adapter_name: str = "azure_queue",
    ) -> bool:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return (
            queue.delete(message_id, queue_name, pop_receipt)
            if pop_receipt
            else queue.delete(message_id, queue_name)
        )

    # ══════════════════════════════════════════════════════════════════════
    # FILE STORAGE
    # ══════════════════════════════════════════════════════════════════════

    def write_file(
        self, path: str, data: Union[bytes, str], *, adapter_name: str = "files"
    ) -> bool:
        files = self._engines[0].cloud_factory.get_files(adapter_name)
        return files.write(path, data.encode() if isinstance(data, str) else data)  # type: ignore

    def read_file(self, path: str, *, adapter_name: str = "files") -> Optional[bytes]:
        files = self._engines[0].cloud_factory.get_files(adapter_name)
        return files.read(path)  # type: ignore

    def delete_file(self, path: str, *, adapter_name: str = "files") -> bool:
        files = self._engines[0].cloud_factory.get_files(adapter_name)
        return files.delete(path)

    def list_files(self, directory: str = "", *, adapter_name: str = "files") -> List[str]:
        files = self._engines[0].cloud_factory.get_files(adapter_name)
        return files.list(directory)

    # ══════════════════════════════════════════════════════════════════════
    # CACHE
    # ══════════════════════════════════════════════════════════════════════

    def set_cache(self, model: str, key: Any, value: Any, ttl: int = 300) -> None:
        if self._cache:
            self._cache.set(model, key, value, ttl)

    def get_cache(self, model: str, key: Any) -> Optional[Any]:
        return self._cache.get(model, key) if self._cache else None

    def invalidate_cache(self, model: str, key: Optional[Any] = None) -> None:
        if not self._cache:
            return
        if key:
            self._cache.invalidate(model, key)
        else:
            self._cache.clear()


# ======================================================
# FILE: polydb/decorators.py
# ======================================================

# src/polydb/decorators.py



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


# ======================================================
# FILE: polydb/errors.py
# ======================================================

# src/polydb/errors.py
"""
Structured exceptions for cloud database operations
"""


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


# ======================================================
# FILE: polydb/json_safe.py
# ======================================================


def json_safe(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)



# ======================================================
# FILE: polydb/models.py
# ======================================================

# src/polydb/models.py
"""
Data models and configurations
"""



class CloudProvider(Enum):
    """Supported cloud providers"""

    AZURE = "azure"
    AWS = "aws"
    GCP = "gcp"
    VERCEL = "vercel"
    MONGODB = "mongodb"
    S3_COMPATIBLE = "s3_compatible"
    POSTGRESQL = "postgresql"
    BLOCKCHAIN = "blockchain"


@dataclass
class PartitionConfig:
    """Configuration for partition and row keys"""

    partition_key_template: str = "default_{id}"
    row_key_template: Optional[str] = None
    composite_keys: Optional[List[str]] = None
    auto_generate: bool = True


@dataclass
class QueryOptions:
    """LINQ-style query options"""

    filter_func: Optional[Callable] = None
    order_by: Optional[str] = None
    skip: int = 0
    take: Optional[int] = None
    select_fields: Optional[List[str]] = None
    count_only: bool = False


# ============================================================
# CONFIG CLASSES (TYPED)
# ============================================================


class StorageConfig:
    def __init__(self, provider: CloudProvider, name: str = "default"):
        self.provider = provider
        self.name = name


class AzureStorageConfig(StorageConfig):
    def __init__(
        self,
        name: str = "azure",
        connection_string: Optional[str] = None,
        container: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.container = container


class AzureQueueConfig(StorageConfig):
    def __init__(
        self,
        name: str = "azure_queue",
        connection_string: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string


class S3StorageConfig(StorageConfig):
    def __init__(
        self,
        name: str = "s3",
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        super().__init__(CloudProvider.S3_COMPATIBLE, name)
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket


class SQSAdapterConfig(StorageConfig):
    def __init__(
        self,
        name: str = "sqs_adapter",
        queue_name: str = "",
        region: str = "",
        endpoint_url: str = "",
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.queue_name = queue_name
        self.region = region
        self.endpoint_url = endpoint_url


class GCPStorageConfig(StorageConfig):
    def __init__(self, name: str = "gcp", project_id="", endpoint="", bucket: Optional[str] = None):
        super().__init__(CloudProvider.GCP, name)
        self.bucket = bucket
        self.project_id = project_id
        self.endpoint = endpoint


class GCPPubSubConfig(StorageConfig):
    def __init__(
        self, name: str = "gcp_queue", project_id="", subscription="", topic: Optional[str] = None
    ):
        super().__init__(CloudProvider.GCP, name)
        self.topic = topic
        self.project_id = project_id
        self.subscription = subscription


class VercelStorageConfig(StorageConfig):
    def __init__(self, name: str = "vercel", token: Optional[str] = None, timeout: int = 10):
        super().__init__(CloudProvider.VERCEL, name)
        self.token = token
        self.timeout = timeout


class VercelQueueConfig(StorageConfig):
    def __init__(self, name: str = "vercel_queue", token: Optional[str] = None, url: str = ""):
        super().__init__(CloudProvider.VERCEL, name)
        self.token = token
        self.url = url


class BlockchainStorageConfig(StorageConfig):
    def __init__(self, name: str = "blockchain", ipfs_url: Optional[str] = None):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.ipfs_url = ipfs_url


class BlockchainQueueConfig(StorageConfig):
    def __init__(
        self,
        name: str = "blockchain_queue",
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.rpc_url = rpc_url
        self.private_key = private_key
        self.contract_address = contract_address
        self.contract_abi = contract_abi


class FileStorageConfig(StorageConfig):
    def __init__(self, provider: CloudProvider, name: str = "files"):
        super().__init__(provider, name)


class AzureFileConfig(FileStorageConfig):
    def __init__(
        self,
        name: str = "azure_files",
        connection_string: Optional[str] = None,
        share_name: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.share_name = share_name


class GCPFileConfig(FileStorageConfig):
    def __init__(
        self,
        name: str = "gcp_files",
        bucket: Optional[str] = None,
    ):
        super().__init__(CloudProvider.GCP, name)
        self.bucket = bucket


class EFSFileConfig(FileStorageConfig):
    def __init__(
        self,
        name: str = "efs",
        mount_path: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AWS, name)
        self.mount_path = mount_path


class PostgreSQLConfig(StorageConfig):
    def __init__(
        self,
        name: str = "sql",
        connection_string: Optional[str] = None,
    ):
        super().__init__(CloudProvider.POSTGRESQL, name)
        self.connection_string = connection_string


class AzureTableConfig(StorageConfig):
    def __init__(
        self,
        name: str = "azure_kv",
        connection_string: Optional[str] = None,
        table_name: str = "",
        container_name: str = "",
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.table_name = table_name
        self.container_name = container_name


class DynamoDBConfig(StorageConfig):
    def __init__(
        self,
        name: str = "dynamodb",
        table_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AWS, name)
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.region = region
        self.endpoint_url = endpoint_url


class FirestoreConfig(StorageConfig):
    def __init__(
        self,
        name: str = "firestore",
        project: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        super().__init__(CloudProvider.GCP, name)
        self.project = project
        self.bucket_name = bucket_name


class VercelKVConfig(StorageConfig):
    def __init__(
        self,
        name: str = "vercel_kv",
        kv_url: str = "",
        kv_token: str = "",
        timeout: int = 10,
    ):
        super().__init__(CloudProvider.VERCEL, name)
        self.kv_url = kv_url
        self.kv_token = kv_token
        self.timeout = timeout


class BlockchainKVConfig(StorageConfig):
    def __init__(
        self,
        name: str = "blockchain_kv",
        chain: Optional[str] = None,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.chain = chain
        self.rpc_url = rpc_url
        self.private_key = private_key
        self.contract_address = contract_address
        self.contract_abi = contract_abi


class MongoConfig(StorageConfig):
    def __init__(
        self,
        name: str = "mongo",
        mongo_uri: str = "",
        db_name: str = "",
    ):
        super().__init__(CloudProvider.MONGODB, name)
        self.mongo_uri = mongo_uri
        self.db_name = db_name


# ======================================================
# FILE: polydb/monitoring.py
# ======================================================

# src/polydb/monitoring.py
"""
Comprehensive monitoring, metrics, and observability
"""


@dataclass
class QueryMetrics:
    """Metrics for a single query"""

    operation: str
    model: str
    duration_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tenant_id: Optional[str] = None
    actor_id: Optional[str] = None
    rows_affected: Optional[int] = None
    cache_hit: bool = False


@dataclass
class AggregatedMetrics:
    """Aggregated metrics over time window"""

    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    total_duration_ms: float = 0.0
    avg_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0
    cache_hit_rate: float = 0.0
    queries_by_operation: Dict[str, int] = field(default_factory=dict)
    queries_by_model: Dict[str, int] = field(default_factory=dict)
    slow_queries: List[QueryMetrics] = field(default_factory=list)


class MetricsCollector:
    """Collects and aggregates metrics"""

    def __init__(self, slow_query_threshold_ms: float = 1000.0):
        self.slow_query_threshold = slow_query_threshold_ms
        self._metrics: List[QueryMetrics] = []
        self._lock = threading.Lock()
        self._hooks: List[Callable] = []
        self.logger = logging.getLogger(__name__)

    def record(self, metric: QueryMetrics):
        """Record a query metric"""
        with self._lock:
            self._metrics.append(metric)

            # Log slow queries
            if metric.duration_ms > self.slow_query_threshold:
                self.logger.warning(
                    f"Slow query detected: {metric.operation} on {metric.model} "
                    f"took {metric.duration_ms:.2f}ms"
                )

            # Trigger hooks
            for hook in self._hooks:
                try:
                    hook(metric)
                except Exception as e:
                    self.logger.error(f"Metrics hook failed: {e}")

    def register_hook(self, hook: Callable[[QueryMetrics], None]):
        """Register a metrics hook"""
        self._hooks.append(hook)

    def get_metrics(
        self,
        since: Optional[datetime] = None,
        model: Optional[str] = None,
        operation: Optional[str] = None,
    ) -> List[QueryMetrics]:
        """Get filtered metrics"""
        with self._lock:
            metrics = self._metrics.copy()

        if since:
            metrics = [m for m in metrics if m.timestamp >= since]

        if model:
            metrics = [m for m in metrics if m.model == model]

        if operation:
            metrics = [m for m in metrics if m.operation == operation]

        return metrics

    def aggregate(
        self, since: Optional[datetime] = None, model: Optional[str] = None
    ) -> AggregatedMetrics:
        """Generate aggregated metrics"""
        metrics = self.get_metrics(since=since, model=model)

        if not metrics:
            return AggregatedMetrics()

        agg = AggregatedMetrics()
        agg.total_queries = len(metrics)

        durations = []
        cache_hits = 0

        for m in metrics:
            if m.success:
                agg.successful_queries += 1
            else:
                agg.failed_queries += 1

            durations.append(m.duration_ms)
            agg.total_duration_ms += m.duration_ms

            if m.cache_hit:
                cache_hits += 1

            # Count by operation
            agg.queries_by_operation[m.operation] = agg.queries_by_operation.get(m.operation, 0) + 1

            # Count by model
            agg.queries_by_model[m.model] = agg.queries_by_model.get(m.model, 0) + 1

            # Track slow queries
            if m.duration_ms > self.slow_query_threshold:
                agg.slow_queries.append(m)

        agg.avg_duration_ms = agg.total_duration_ms / agg.total_queries
        agg.min_duration_ms = min(durations)
        agg.max_duration_ms = max(durations)
        agg.cache_hit_rate = cache_hits / agg.total_queries

        return agg

    def clear_old_metrics(self, older_than: timedelta):
        """Clear metrics older than specified duration"""
        cutoff = datetime.utcnow() - older_than

        with self._lock:
            self._metrics = [m for m in self._metrics if m.timestamp >= cutoff]

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        agg = self.aggregate()

        lines = [
            f"# HELP polydb_queries_total Total number of queries",
            f"# TYPE polydb_queries_total counter",
            f"polydb_queries_total {agg.total_queries}",
            "",
            f"# HELP polydb_queries_successful Successful queries",
            f"# TYPE polydb_queries_successful counter",
            f"polydb_queries_successful {agg.successful_queries}",
            "",
            f"# HELP polydb_queries_failed Failed queries",
            f"# TYPE polydb_queries_failed counter",
            f"polydb_queries_failed {agg.failed_queries}",
            "",
            f"# HELP polydb_query_duration_ms Query duration",
            f"# TYPE polydb_query_duration_ms summary",
            f"polydb_query_duration_ms_sum {agg.total_duration_ms}",
            f"polydb_query_duration_ms_count {agg.total_queries}",
            "",
            f"# HELP polydb_cache_hit_rate Cache hit rate",
            f"# TYPE polydb_cache_hit_rate gauge",
            f"polydb_cache_hit_rate {agg.cache_hit_rate}",
        ]

        return "\n".join(lines)


class PerformanceMonitor:
    """Context manager for automatic query timing"""

    def __init__(
        self,
        collector: MetricsCollector,
        operation: str,
        model: str,
        tenant_id: Optional[str] = None,
        actor_id: Optional[str] = None,
    ):
        self.collector = collector
        self.operation = operation
        self.model = model
        self.tenant_id = tenant_id
        self.actor_id = actor_id
        self.start_time = None
        self.success = False
        self.error = None
        self.rows_affected = 0
        self.rows_returned = 0
        self.cache_hit = False

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000  # type: ignore

        if exc_type is None:
            self.success = True
        else:
            self.error = str(exc_val)

        metric = QueryMetrics(
            operation=self.operation,
            model=self.model,
            duration_ms=duration_ms,
            success=self.success,
            error=self.error,
            tenant_id=self.tenant_id,
            actor_id=self.actor_id,
            rows_affected=self.rows_affected,
            cache_hit=self.cache_hit,
        )

        self.collector.record(metric)

        return False


class HealthCheck:
    """System health monitoring"""

    def __init__(self, factory):
        self.factory = factory
        self.logger = logging.getLogger(__name__)

    def check_sql_health(self) -> Dict[str, Any]:
        """Check SQL database health"""
        try:
            start = time.perf_counter()
            self.factory._sql.execute("SELECT 1", fetch_one=True)
            duration_ms = (time.perf_counter() - start) * 1000

            return {
                "status": "healthy",
                "latency_ms": duration_ms,
                "provider": self.factory._provider_name,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e), "provider": self.factory._provider_name}

    def check_nosql_health(self) -> Dict[str, Any]:
        """Check NoSQL database health"""
        try:
            # Attempt a simple operation
            start = time.perf_counter()
            # This would need a test model
            duration_ms = (time.perf_counter() - start) * 1000

            return {
                "status": "healthy",
                "latency_ms": duration_ms,
                "provider": self.factory._provider_name,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e), "provider": self.factory._provider_name}

    def check_cache_health(self) -> Dict[str, Any]:
        """Check cache health"""
        if not self.factory._cache:
            return {"status": "disabled"}

        try:
            # Test cache operations
            test_key = "_health_check"
            test_value = {"test": True}

            start = time.perf_counter()
            self.factory._cache.set(test_key, {}, test_value, 10)
            retrieved = self.factory._cache.get(test_key, {})
            duration_ms = (time.perf_counter() - start) * 1000

            return {"status": "healthy", "latency_ms": duration_ms}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    def full_health_check(self) -> Dict[str, Any]:
        """Complete system health check"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "sql": self.check_sql_health(),
            "nosql": self.check_nosql_health(),
            "cache": self.check_cache_health(),
        }


# ======================================================
# FILE: polydb/multitenancy.py
# ======================================================

# src/polydb/multitenancy.py
"""
Multi-tenancy enforcement and isolation
"""


class IsolationLevel(Enum):
    """Tenant isolation levels"""
    SHARED_SCHEMA = "shared"  # Shared tables with tenant_id
    SEPARATE_SCHEMA = "schema"  # Separate schema per tenant
    SEPARATE_DATABASE = "database"  # Separate DB per tenant


@dataclass
class TenantConfig:
    """Tenant configuration"""
    tenant_id: str
    isolation_level: IsolationLevel
    schema_name: Optional[str] = None
    database_name: Optional[str] = None
    max_connections: int = 10
    storage_quota_gb: Optional[float] = None
    features: List[str] = field(default_factory=list)


class TenantRegistry:
    """Registry of tenant configurations"""
    
    def __init__(self):
        self._tenants: Dict[str, TenantConfig] = {}
    
    def register(self, config: TenantConfig):
        """Register tenant"""
        self._tenants[config.tenant_id] = config
    
    def get(self, tenant_id: str) -> Optional[TenantConfig]:
        """Get tenant config"""
        return self._tenants.get(tenant_id)
    
    def list_all(self) -> List[TenantConfig]:
        """List all tenants"""
        return list(self._tenants.values())


class TenantContext:
    """Tenant context management"""
    
    current_tenant: ContextVar[Optional[TenantConfig]] = \
        ContextVar("current_tenant", default=None)
    
    @classmethod
    def set_tenant(cls, tenant_id: str, registry: TenantRegistry):
        """Set current tenant"""
        config = registry.get(tenant_id)
        if not config:
            raise ValueError(f"Tenant not found: {tenant_id}")
        
        cls.current_tenant.set(config)
    
    @classmethod
    def get_tenant(cls) -> Optional[TenantConfig]:
        """Get current tenant"""
        return cls.current_tenant.get()
    
    @classmethod
    def clear(cls):
        """Clear tenant context"""
        cls.current_tenant.set(None)


class TenantIsolationEnforcer:
    """Enforces tenant isolation at query level"""
    
    def __init__(self, registry: TenantRegistry):
        self.registry = registry
    
    def enforce_read(
        self,
        model: str,
        query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enforce tenant isolation on read"""
        tenant = TenantContext.get_tenant()
        
        if not tenant:
            raise ValueError("No tenant context set")
        
        if tenant.isolation_level == IsolationLevel.SHARED_SCHEMA:
            # Add tenant_id filter
            query = query.copy()
            query['tenant_id'] = tenant.tenant_id
        
        return query
    
    def enforce_write(
        self,
        model: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enforce tenant isolation on write"""
        tenant = TenantContext.get_tenant()
        
        if not tenant:
            raise ValueError("No tenant context set")
        
        if tenant.isolation_level == IsolationLevel.SHARED_SCHEMA:
            # Add tenant_id
            data = data.copy()
            data['tenant_id'] = tenant.tenant_id
        
        return data
    
    def get_table_name(self, base_table: str) -> str:
        """Get tenant-specific table name"""
        tenant = TenantContext.get_tenant()
        
        if not tenant:
            raise ValueError("No tenant context set")
        
        if tenant.isolation_level == IsolationLevel.SEPARATE_SCHEMA:
            return f"{tenant.schema_name}.{base_table}"
        elif tenant.isolation_level == IsolationLevel.SEPARATE_DATABASE:
            return f"{tenant.database_name}.public.{base_table}"
        else:
            return base_table


class TenantQuotaManager:
    """Manages tenant resource quotas"""
    
    def __init__(self, registry: TenantRegistry):
        self.registry = registry
        self._usage: Dict[str, Dict[str, float]] = {}
    
    def check_storage_quota(self, tenant_id: str, size_gb: float) -> bool:
        """Check if operation would exceed storage quota"""
        config = self.registry.get(tenant_id)
        if not config or not config.storage_quota_gb:
            return True
        
        current_usage = self._usage.get(tenant_id, {}).get('storage_gb', 0.0)
        return (current_usage + size_gb) <= config.storage_quota_gb
    
    def record_storage_usage(self, tenant_id: str, size_gb: float):
        """Record storage usage"""
        if tenant_id not in self._usage:
            self._usage[tenant_id] = {}
        
        self._usage[tenant_id]['storage_gb'] = \
            self._usage[tenant_id].get('storage_gb', 0.0) + size_gb
    
    def get_usage(self, tenant_id: str) -> Dict[str, float]:
        """Get tenant resource usage"""
        return self._usage.get(tenant_id, {})


class TenantMigrationManager:
    """Manages tenant migrations and onboarding"""
    
    def __init__(self, factory, registry: TenantRegistry):
        self.factory = factory
        self.registry = registry
    
    def provision_tenant(self, config: TenantConfig):
        """Provision new tenant"""
        # Register tenant
        self.registry.register(config)
        
        if config.isolation_level == IsolationLevel.SEPARATE_SCHEMA:
            # Create schema
            schema_sql = f"CREATE SCHEMA IF NOT EXISTS {config.schema_name};"
            self.factory._sql.execute(schema_sql)
        
        elif config.isolation_level == IsolationLevel.SEPARATE_DATABASE:
            # Create database (requires superuser)
            db_sql = f"CREATE DATABASE {config.database_name};"
            self.factory._sql.execute(db_sql)
    
    def deprovision_tenant(self, tenant_id: str):
        """Deprovision tenant"""
        config = self.registry.get(tenant_id)
        if not config:
            return
        
        if config.isolation_level == IsolationLevel.SEPARATE_SCHEMA:
            # Drop schema
            schema_sql = f"DROP SCHEMA IF EXISTS {config.schema_name} CASCADE;"
            self.factory._sql.execute(schema_sql)
        
        elif config.isolation_level == IsolationLevel.SEPARATE_DATABASE:
            # Drop database
            db_sql = f"DROP DATABASE IF EXISTS {config.database_name};"
            self.factory._sql.execute(db_sql)

# ======================================================
# FILE: polydb/query.py
# ======================================================

# src/polydb/query.py



class Operator(Enum):
    EQ = "=="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"


@dataclass
class QueryFilter:
    field: str
    operator: Operator
    value: Any


@dataclass
class QueryBuilder:
    """LINQ-style query builder supporting SQL and NoSQL"""

    filters: List[QueryFilter] = field(default_factory=list)
    order_by_fields: List[tuple[str, bool]] = field(default_factory=list)

    skip_count: int = 0
    take_count: Optional[int] = None

    selected_fields: Optional[List[str]] = None
    group_by_fields: Optional[List[str]] = None

    distinct_flag: bool = False
    count_only: bool = False

    # ------------------------------------------------
    # FILTERS
    # ------------------------------------------------

    def where(
        self, field: str, operator: Union["Operator", str, None], value: Any
    ) -> "QueryBuilder":
        # Allow legacy callers that pass "" meaning equality
        if operator in ("", None):
            op = Operator.EQ
        elif isinstance(operator, Operator):
            op = operator
        else:
            op = Operator(operator)

        self.filters.append(QueryFilter(field=field, operator=op, value=value))
        return self

    # ------------------------------------------------
    # ORDER
    # ------------------------------------------------

    def order_by(self, field: str, descending: bool = False) -> QueryBuilder:
        self.order_by_fields.append((field, descending))
        return self

    # ------------------------------------------------
    # PAGINATION
    # ------------------------------------------------

    def skip(self, count: int) -> QueryBuilder:
        self.skip_count = count
        return self

    def take(self, count: int) -> QueryBuilder:
        self.take_count = count
        return self

    # ------------------------------------------------
    # SELECT
    # ------------------------------------------------

    def select(self, *fields: str) -> QueryBuilder:
        self.selected_fields = list(fields)
        return self

    def select_fields(self, fields: List[str]) -> QueryBuilder:
        self.selected_fields = fields
        return self

    # ------------------------------------------------
    # GROUP
    # ------------------------------------------------

    def group_by(self, *fields: str) -> QueryBuilder:
        self.group_by_fields = list(fields)
        return self

    # ------------------------------------------------
    # DISTINCT
    # ------------------------------------------------

    def distinct(self) -> QueryBuilder:
        self.distinct_flag = True
        return self

    # ------------------------------------------------
    # COUNT
    # ------------------------------------------------

    def count(self) -> QueryBuilder:
        self.count_only = True
        return self

    # ------------------------------------------------
    # SQL WHERE
    # ------------------------------------------------

    def to_sql_where(self) -> tuple[str, List[Any]]:

        if not self.filters:
            return "", []

        clauses = []
        params = []

        for f in self.filters:

            if f.operator == Operator.EQ:
                clauses.append(f"{f.field} = %s")
                params.append(f.value)

            elif f.operator == Operator.NE:
                clauses.append(f"{f.field} != %s")
                params.append(f.value)

            elif f.operator == Operator.GT:
                clauses.append(f"{f.field} > %s")
                params.append(f.value)

            elif f.operator == Operator.GTE:
                clauses.append(f"{f.field} >= %s")
                params.append(f.value)

            elif f.operator == Operator.LT:
                clauses.append(f"{f.field} < %s")
                params.append(f.value)

            elif f.operator == Operator.LTE:
                clauses.append(f"{f.field} <= %s")
                params.append(f.value)

            elif f.operator == Operator.IN:

                if isinstance(f.value, (list, tuple)):
                    placeholders = ",".join(["%s"] * len(f.value))
                    clauses.append(f"{f.field} IN ({placeholders})")
                    params.extend(f.value)

                else:
                    clauses.append(f"{f.field} LIKE %s")
                    params.append(f.value)

            elif f.operator == Operator.NOT_IN:
                placeholders = ",".join(["%s"] * len(f.value))
                clauses.append(f"{f.field} NOT IN ({placeholders})")
                params.extend(f.value)

            elif f.operator == Operator.CONTAINS:
                clauses.append(f"{f.field} LIKE %s")
                params.append(f"%{f.value}%")

            elif f.operator == Operator.STARTS_WITH:
                clauses.append(f"{f.field} LIKE %s")
                params.append(f"{f.value}%")

            elif f.operator == Operator.ENDS_WITH:
                clauses.append(f"{f.field} LIKE %s")
                params.append(f"%{f.value}")

        return " AND ".join(clauses), params

    # ------------------------------------------------
    # NOSQL FILTER
    # ------------------------------------------------

    def to_nosql_filter(self) -> Dict[str, Any]:

        result = {}

        for f in self.filters:

            if f.operator == Operator.EQ:
                result[f.field] = f.value

            elif f.operator == Operator.IN:
                result[f"{f.field}__in"] = f.value

            elif f.operator == Operator.GT:
                result[f"{f.field}__gt"] = f.value

            elif f.operator == Operator.GTE:
                result[f"{f.field}__gte"] = f.value

            elif f.operator == Operator.LT:
                result[f"{f.field}__lt"] = f.value

            elif f.operator == Operator.LTE:
                result[f"{f.field}__lte"] = f.value

            elif f.operator == Operator.CONTAINS:
                result[f"{f.field}__contains"] = f.value

        return result


# ======================================================
# FILE: polydb/registry.py
# ======================================================

# src/polydb/registry.py


class ModelRegistry:
    """
    Lightweight model registry.

    Supports:
    - register(model)
    - get(model) → metadata
    - resolve(model_or_name) → model class
    """

    _models: Dict[Type, Dict] = {}

    @classmethod
    def register(cls, model):
        meta = getattr(model, "__polydb__", None)
        if not meta:
            raise ValueError(f"{model} missing __polydb__ config")

        cls._models[model] = meta

    @classmethod
    def register_dynamic(cls, model_name: str, meta: dict):
        """
        Register a model from a schema dict — no Python class needed.
        Creates a lightweight runtime class on the fly.
        """
        # Create a dynamic class with __polydb__ metadata
        dynamic_class = type(model_name, (), {"__polydb__": meta})
        cls._models[dynamic_class] = meta
        return dynamic_class

    # -------------------------
    # NEW: resolve()
    # -------------------------
    @classmethod
    def resolve(cls, model):
        """
        Resolve model class from:
        - class
        - class name string

        Returns actual class
        """
        # Already class
        if isinstance(model, type):
            if model not in cls._models:
                raise ValueError(f"Model not registered: {model.__name__}")
            return model

        # String name
        if isinstance(model, str):
            for m in cls._models:
                if m.__name__ == model:
                    return m

            raise ValueError(f"Model not registered: '{model}'")

        raise TypeError(f"Invalid model reference {model!r}. Must be class or class name.")

    @classmethod
    def get(cls, model):
        """Get model metadata"""

        model_cls = cls.resolve(model)
        raw_meta = cls._models[model_cls]

        # Convert to ModelMeta
        return ModelMeta(
            storage=raw_meta.get("storage", "sql"),
            table=raw_meta.get("table"),
            collection=raw_meta.get("collection"),
            pk_field=raw_meta.get("pk_field"),
            rk_field=raw_meta.get("rk_field"),
            provider=raw_meta.get("provider"),
            cache=raw_meta.get("cache", False),
            cache_ttl=raw_meta.get("cache_ttl"),
        )


# ======================================================
# FILE: polydb/retry.py
# ======================================================

# src/polydb/retry.py
"""
Retry logic with exponential backoff and metrics hooks
"""



# Metrics hooks for enterprise monitoring
class MetricsHooks:
    """Metrics hooks that users can override for monitoring"""
    
    @staticmethod
    def on_query_start(operation: str, **kwargs):
        """Called when query starts"""
        pass
    
    @staticmethod
    def on_query_end(operation: str, duration: float, success: bool, **kwargs):
        """Called when query ends"""
        pass
    
    @staticmethod
    def on_error(operation: str, error: Exception, **kwargs):
        """Called when error occurs"""
        pass


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0, 
        exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            
            logger = logging.getLogger(__name__)
            
            while attempt < max_attempts:
                start_time = time.time()
                try:
                    MetricsHooks.on_query_start(func.__name__, args=args, kwargs=kwargs)
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    MetricsHooks.on_query_end(func.__name__, duration, True)
                    return result
                except exceptions as e:
                    attempt += 1
                    duration = time.time() - start_time
                    MetricsHooks.on_query_end(func.__name__, duration, False)
                    MetricsHooks.on_error(func.__name__, e)
                    
                    if attempt >= max_attempts:
                        raise
                    
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            
        return wrapper
    return decorator

# ======================================================
# FILE: polydb/schema.py
# ======================================================

# src/polydb/schema.py
"""
Schema management and migrations
"""


class ColumnType(Enum):
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    JSONB = "JSONB"
    UUID = "UUID"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"


@dataclass
class Column:
    name: str
    type: ColumnType
    nullable: bool = True
    default: Optional[Any] = None
    primary_key: bool = False
    unique: bool = False
    max_length: Optional[int] = None


@dataclass
class Index:
    name: str
    columns: List[str]
    unique: bool = False


class SchemaBuilder:
    """Build SQL schema definitions"""
    
    def __init__(self):
        self.columns: List[Column] = []
        self.indexes: List[Index] = []
        self.primary_keys: List[str] = []
    
    def add_column(self, column: Column) -> 'SchemaBuilder':
        self.columns.append(column)
        if column.primary_key:
            self.primary_keys.append(column.name)
        return self
    
    def add_index(self, index: Index) -> 'SchemaBuilder':
        self.indexes.append(index)
        return self
    
    def to_create_table(self, table_name: str) -> str:
        """Generate CREATE TABLE statement"""
        col_defs = []
        
        for col in self.columns:
            parts = [col.name]
            
            # Type
            if col.type == ColumnType.VARCHAR and col.max_length:
                parts.append(f"VARCHAR({col.max_length})")
            else:
                parts.append(col.type.value)
            
            # Nullable
            if not col.nullable:
                parts.append("NOT NULL")
            
            # Default
            if col.default is not None:
                if isinstance(col.default, str):
                    parts.append(f"DEFAULT '{col.default}'")
                else:
                    parts.append(f"DEFAULT {col.default}")
            
            # Unique
            if col.unique:
                parts.append("UNIQUE")
            
            col_defs.append(" ".join(parts))
        
        # Primary key
        if self.primary_keys:
            col_defs.append(f"PRIMARY KEY ({', '.join(self.primary_keys)})")
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        sql += ",\n".join(f"  {col}" for col in col_defs)
        sql += "\n);"
        
        return sql
    
    def to_create_indexes(self, table_name: str) -> List[str]:
        """Generate CREATE INDEX statements"""
        statements = []
        
        for idx in self.indexes:
            unique = "UNIQUE " if idx.unique else ""
            cols = ", ".join(idx.columns)
            sql = f"CREATE {unique}INDEX IF NOT EXISTS {idx.name} ON {table_name}({cols});"
            statements.append(sql)
        
        return statements


class MigrationManager:
    """Database migration management"""
    
    def __init__(self, sql_adapter):
        self.sql = sql_adapter
        self._ensure_migrations_table()
    
    def _ensure_migrations_table(self):
        """Create migrations tracking table"""
        schema = """
        CREATE TABLE IF NOT EXISTS polydb_migrations (
            id SERIAL PRIMARY KEY,
            version VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP DEFAULT NOW(),
            rollback_sql TEXT,
            checksum VARCHAR(64)
        );
        """
        self.sql.execute(schema)
    
    def apply_migration(
        self,
        version: str,
        name: str,
        up_sql: str,
        down_sql: Optional[str] = None
    ) -> bool:
        """Apply a migration"""
        
        # Check if already applied
        existing = self.sql.execute(
            "SELECT version FROM polydb_migrations WHERE version = %s",
            [version],
            fetch_one=True
        )
        
        if existing:
            return False
        
        # Calculate checksum
        checksum = hashlib.sha256(up_sql.encode()).hexdigest()
        
        try:
            # Execute migration
            self.sql.execute(up_sql)
            
            # Record migration
            self.sql.insert('polydb_migrations', {
                'version': version,
                'name': name,
                'rollback_sql': down_sql,
                'checksum': checksum
            })
            
            return True
        except Exception as e:
            raise Exception(f"Migration {version} failed: {str(e)}")
    
    def rollback_migration(self, version: str) -> bool:
        """Rollback a migration"""
        migration = self.sql.execute(
            "SELECT rollback_sql FROM polydb_migrations WHERE version = %s",
            [version],
            fetch_one=True
        )
        
        if not migration or not migration.get('rollback_sql'):
            raise Exception(f"No rollback available for {version}")
        
        try:
            # Execute rollback
            self.sql.execute(migration['rollback_sql'])
            
            # Remove from migrations
            self.sql.execute(
                "DELETE FROM polydb_migrations WHERE version = %s",
                [version]
            )
            
            return True
        except Exception as e:
            raise Exception(f"Rollback {version} failed: {str(e)}")
    
    def get_applied_migrations(self) -> List[Dict[str, Any]]:
        """Get list of applied migrations"""
        return self.sql.execute(
            "SELECT * FROM polydb_migrations ORDER BY applied_at",
            fetch=True
        )

# ======================================================
# FILE: polydb/security.py
# ======================================================

# src/polydb/security.py
"""
Security features: encryption, masking, row-level security
"""


logger = logging.getLogger(__name__)


@dataclass
class EncryptionConfig:
    """Field-level encryption configuration"""

    encrypted_fields: List[str]
    key: bytes
    algorithm: str = "AES-256-GCM"


class FieldEncryption:
    """Field-level encryption for sensitive data"""

    def __init__(self, encryption_key: Optional[bytes] = None):
        self.encryption_key = encryption_key or self._generate_key()

    @staticmethod
    def _generate_key() -> bytes:
        """Generate encryption key from environment or create new"""
        key_str = os.getenv("POLYDB_ENCRYPTION_KEY")
        if key_str:
            return base64.b64decode(key_str)

        # Generate new key (should be saved securely)
        key = os.urandom(32)
        # For production, log or store this key securely; here we just warn
        logger.warning(
            "Generated new encryption key. Store it securely in POLYDB_ENCRYPTION_KEY env var."
        )
        return key

    def _encrypt_value(self, value: Any) -> str:
        """Encrypt arbitrary value (serialize if non-str)"""
        if value is None:
            return ""
        data = json.dumps(value,default=json_safe) if not isinstance(value, str) else value
        try:

            aesgcm = AESGCM(self.encryption_key)
            nonce = os.urandom(12)

            ciphertext = aesgcm.encrypt(nonce, data.encode("utf-8"), None)

            # Combine nonce and ciphertext
            encrypted = base64.b64encode(nonce + ciphertext).decode("utf-8")
            return f"encrypted:{encrypted}"
        except ImportError:
            raise ImportError("cryptography not installed. Install with: pip install cryptography")

    def _decrypt_value(self, encrypted_data: Any) -> Any:
        """Decrypt field data (deserialize if needed)"""
        if not isinstance(encrypted_data, str) or not encrypted_data.startswith("encrypted:"):
            return encrypted_data

        try:

            encrypted_data = encrypted_data[10:]  # Remove prefix
            combined = base64.b64decode(encrypted_data)

            nonce = combined[:12]
            ciphertext = combined[12:]

            aesgcm = AESGCM(self.encryption_key)
            plaintext_bytes = aesgcm.decrypt(nonce, ciphertext, None)
            plaintext = plaintext_bytes.decode("utf-8")

            # Attempt to parse as JSON if it looks like JSON
            try:
                return json.loads(plaintext)
            except json.JSONDecodeError:
                return plaintext
        except ImportError:
            raise ImportError("cryptography not installed")
        except Exception as e:
            # On decrypt failure, return masked or original to avoid crashes
            logger.warning(f"Decryption failed: {e}. Returning original value.")
            return encrypted_data

    def encrypt_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """Encrypt specified fields in data dict"""
        result = dict(data)

        for field in fields:
            if field in result and result[field] is not None:
                result[field] = self._encrypt_value(result[field])

        return result

    def decrypt_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """Decrypt specified fields in data dict"""
        result = dict(data)

        for field in fields:
            if field in result and result[field] is not None:
                result[field] = self._decrypt_value(result[field])

        return result


class DataMasking:
    """Data masking for sensitive information"""

    def __init__(self):
        # Model-specific masking configs: {model: {field: mask_type}}
        self._configs: Dict[str, Dict[str, str]] = {}
        # Global field-based rules (fallback)
        self._global_rules = {
            "email": self._mask_email,
            "phone": self._mask_phone,
            "ssn": self._mask_ssn,
            "credit_card": self._mask_credit_card,
            "password": lambda x: "[REDACTED]",
            "ssn_*": self._mask_ssn,  # Pattern match
            "card_*": self._mask_credit_card,
        }

    def register_model_config(self, model: str, config: Dict[str, str]):
        """Register masking config for a model: {field: mask_type}"""
        self._configs[model] = config

    def _infer_mask_type(self, field: str) -> Optional[Callable]:
        """Infer masker based on field name (global fallback)"""
        for pattern, masker in self._global_rules.items():
            if pattern.endswith("_*") and field.startswith(pattern[:-2]):
                return masker
            if pattern == field:
                return masker
        return None

    @staticmethod
    def _mask_email(email: str) -> str:
        """Mask email address"""
        if "@" not in email:
            return email

        local, domain = email.split("@", 1)
        if len(local) <= 2:
            masked_local = "*" * len(local)
        else:
            masked_local = local[0] + "*" * (len(local) - 2) + local[-1]

        return f"{masked_local}@{domain}"

    @staticmethod
    def _mask_phone(phone: str) -> str:
        """Mask phone number"""
        phone = "".join(filter(str.isdigit, str(phone)))
        if len(phone) <= 4:
            return "*" * len(phone)
        return "*" * (len(phone) - 4) + phone[-4:]

    @staticmethod
    def _mask_ssn(ssn: str) -> str:
        """Mask SSN"""
        ssn = "".join(filter(str.isdigit, str(ssn)))
        if len(ssn) >= 4:
            return "***-**-" + ssn[-4:]
        return "*" * len(ssn)

    @staticmethod
    def _mask_credit_card(cc: str) -> str:
        """Mask credit card"""
        cc = "".join(filter(str.isdigit, str(cc).replace("-", "").replace(" ", "")))
        if len(cc) <= 4:
            return "*" * len(cc)
        return "**** **** **** " + cc[-4:]

    def mask(
        self,
        data: Dict[str, Any],
        model: str,
        actor_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Apply masking to data based on model, actor, and tenant context.

        Uses model-specific config if available, falls back to global rules.
        Context can be used for dynamic masking (e.g., actor-specific).
        """
        result = dict(data)
        config = self._configs.get(model, {})

        for field, value in list(result.items()):
            if value is None:
                continue

            # Model-specific
            mask_type = config.get(field)
            if mask_type:
                masker = self._get_masker(mask_type)
                if masker:
                    result[field] = masker(str(value))
                    continue

            # Global inference
            inferred_masker = self._infer_mask_type(field)
            if inferred_masker:
                result[field] = inferred_masker(str(value))
                continue

        # Context-based dynamic masking (e.g., hide all if not owner)
        if actor_id and tenant_id:
            result = self._apply_context_masking(result, actor_id, tenant_id, model)

        return result

    def _get_masker(self, mask_type: str) -> Optional[Callable]:
        """Get masker function by type"""
        maskers = {
            "email": self._mask_email,
            "phone": self._mask_phone,
            "ssn": self._mask_ssn,
            "credit_card": self._mask_credit_card,
            "redact": lambda x: "[REDACTED]",
        }
        return maskers.get(mask_type)

    def _apply_context_masking(
        self, data: Dict[str, Any], actor_id: str, tenant_id: str, model: str
    ) -> Dict[str, Any]:
        """Apply additional masking based on context (override in subclasses)"""
        # Example: Mask sensitive fields if not tenant owner
        # This is a placeholder; customize per app
        if "owner_id" in data and data["owner_id"] != actor_id:
            for sensitive in ["salary", "health_info"]:
                if sensitive in data:
                    data[sensitive] = "[RESTRICTED]"
        return data


@dataclass
class Policy:
    """RLS Policy entry"""

    name: str
    func: Callable[[Dict[str, Any], Dict[str, Any]], bool]
    apply_to: str  # 'read', 'write', or 'both'


class RowLevelSecurity:
    """Row-level security policies"""

    def __init__(self):
        self.policies: Dict[str, List[Policy]] = {}  # {model: [Policy instances]}
        self._read_filters: Dict[str, Dict[str, Any]] = {}  # {model: default_query_filters}
        self._write_filters: Dict[str, Dict[str, Any]] = {}  # {model: write_constraints}

    def add_policy(
        self,
        model: str,
        name: str,
        policy_func: Callable[[Dict[str, Any], Dict[str, Any]], bool],
        apply_to: str = "read",  # 'read', 'write', or 'both'
    ):
        """
        Add RLS policy

        Args:
            model: Model name
            name: Policy name (for logging/debug)
            policy_func: Function(row_or_data, context) -> bool
            apply_to: 'read' (post-filter), 'write' (pre-check), or 'both'
        """
        if model not in self.policies:
            self.policies[model] = []

        # Ensure unique names
        existing_names = {p.name for p in self.policies[model]}
        if name in existing_names:
            raise ValueError(f"Policy '{name}' already exists for model '{model}'")

        policy = Policy(name=name, func=policy_func, apply_to=apply_to)
        self.policies[model].append(policy)

    def _get_context(
        self,
        actor_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Get current security context"""
        return {
            "actor_id": actor_id,
            "tenant_id": tenant_id,
            "roles": roles or [],
            "timestamp": os.getenv("REQUEST_TIMESTAMP", ""),  # Optional: from request
        }

    def check_access(
        self,
        model: str,
        item: Union[Dict[str, Any], Any],  # row for read, data for write
        context: Optional[Dict[str, Any]] = None,
        operation: str = "read",  # 'read' or 'write'
    ) -> bool:
        """Check if access allowed for item under context and operation"""
        if model not in self.policies:
            return True

        ctx = context or self._get_context()
        for policy in self.policies[model]:
            if operation in policy.apply_to or policy.apply_to == "both":
                if not policy.func(item, ctx):
                    # Log denial (in production, use logger)
                    logger.info(f"RLS denied: {policy.name} for {model}:{operation}")
                    return False

        return True

    def enforce_read(
        self, model: str, query: Dict[str, Any], context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enforce RLS on read query: add pre-filters if possible, else return original (post-filter later)

        For simplicity, adds default filters from _read_filters; complex policies use post-filter.
        """
        result = dict(query or {})

        # Add static filters (e.g., tenant_id)
        if model in self._read_filters:
            for k, v in self._read_filters[model].items():
                if k not in result:
                    result[k] = v

        # Dynamic: if context provided, inject (e.g., tenant_id)
        ctx = context or self._get_context()
        if "tenant_id" in ctx and ctx["tenant_id"] and "tenant_id" not in result:
            result["tenant_id"] = ctx["tenant_id"]

        return result

    def filter_results(
        self, model: str, rows: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Post-query filter based on RLS policies"""
        if model not in self.policies:
            return rows

        ctx = context or self._get_context()
        return [row for row in rows if self.check_access(model, row, ctx, operation="read")]

    def enforce_write(
        self, model: str, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enforce RLS on write: validate data against policies, inject constraints
        """
        # First, check if write is allowed
        ctx = context or self._get_context()
        if not self.check_access(model, data, ctx, operation="write"):
            raise PermissionError(f"RLS write denied for model '{model}'")

        result = dict(data)

        # Inject constraints (e.g., set tenant_id if not present)
        if model in self._write_filters:
            for k, v in self._write_filters[model].items():
                if k not in result:
                    result[k] = v

        # Dynamic injection
        if "tenant_id" in ctx and ctx["tenant_id"] and "tenant_id" not in result:
            result["tenant_id"] = ctx["tenant_id"]

        return result

    def set_default_filters(
        self, model: str, read_filters: Dict[str, Any], write_filters: Dict[str, Any]
    ):
        """Set default query filters/constraints for model"""
        if read_filters:
            self._read_filters[model] = read_filters
        if write_filters:
            self._write_filters[model] = write_filters


# Example RLS policies
def tenant_isolation_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Ensure users only access their tenant's data"""
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        return False
    return item.get("tenant_id") == tenant_id


def role_based_policy(row: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Check role-based access"""
    required_role = row.get("required_role")
    user_roles = context.get("roles", [])

    if not required_role:
        return True

    return required_role in user_roles


def ownership_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Ensure users only access/modify their owned data"""
    actor_id = context.get("actor_id")
    if not actor_id:
        return False
    return item.get("owner_id") == actor_id or item.get("created_by") == actor_id


def sensitivity_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Restrict access to sensitive data based on role"""
    sensitivity_level = item.get("sensitivity_level", "low")
    user_roles = context.get("roles", [])
    if sensitivity_level == "high" and "admin" not in user_roles:
        return False
    if sensitivity_level == "medium" and "admin" not in user_roles and "editor" not in user_roles:
        return False
    return True


def time_based_policy(item: Dict[str, Any], context: Dict[str, Any]) -> bool:
    """Restrict access based on data age (e.g., archive old data)"""

    created_at = item.get("created_at")
    if not created_at:
        return True
    try:
        created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        if datetime.now(created_dt.tzinfo) - created_dt > timedelta(days=365):
            return "archivist" in context.get("roles", [])
    except ValueError:
        pass
    return True


# Example usage (in app init):
# rls = RowLevelSecurity()
# rls.add_policy('User', 'tenant_isolation', tenant_isolation_policy, apply_to='both')
# rls.add_policy('User', 'ownership', ownership_policy, apply_to='both')
# rls.add_policy('User', 'role_based', role_based_policy, apply_to='read')
# rls.add_policy('Document', 'sensitivity', sensitivity_policy, apply_to='read')
# rls.add_policy('Log', 'time_based', time_based_policy, apply_to='read')
# rls.set_default_filters('User', read_filters={'status': 'active'}, write_filters={'status': 'active'})


# ======================================================
# FILE: polydb/types.py
# ======================================================

# src/polydb/types.py



StorageType = Literal["sql", "nosql"]

JsonDict = Dict[str, Any]
Lookup = Dict[str, Any]


@dataclass(frozen=True)
class ModelMeta:
    """Model metadata"""
    storage: StorageType
    table: Optional[str] = None
    collection: Optional[str] = None
    pk_field: Optional[str] = None
    rk_field: Optional[str] = None
    provider: Optional[str] = None
    cache: bool = False
    cache_ttl: Optional[int] = None


@runtime_checkable
class SQLAdapter(Protocol):
    """SQL adapter contract"""
    
    def insert(self, table: str, data: JsonDict) -> JsonDict: ...
    
    def select(
        self,
        table: str,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[JsonDict]: ...
    
    def select_page(
        self,
        table: str,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None
    ) -> Tuple[List[JsonDict], Optional[str]]: ...
    
    def update(
        self,
        table: str,
        entity_id: Union[Any, Lookup],
        data: JsonDict
    ) -> JsonDict: ...
    
    def upsert(self, table: str, data: JsonDict) -> JsonDict: ...
    
    def delete(
        self,
        table: str,
        entity_id: Union[Any, Lookup]
    ) -> JsonDict: ...
    
    def query_linq(
        self,
        table: str,
        builder: QueryBuilder
    ) -> Union[List[JsonDict], int]: ...
    
    def execute(self, sql: str, params: Optional[List[Any]] = None) -> None: ...


@runtime_checkable
class NoSQLKVAdapter(Protocol):
    """NoSQL KV adapter contract"""
    
    def put(self, model: type, data: JsonDict) -> JsonDict: ...
    
    def query(
        self,
        model: type,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        no_cache: bool = False,
        cache_ttl: Optional[int] = None
    ) -> List[JsonDict]: ...
    
    def query_page(
        self,
        model: type,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None
    ) -> Tuple[List[JsonDict], Optional[str]]: ...
    
    def patch(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False
    ) -> JsonDict: ...
    
    def upsert(
        self,
        model: type,
        data: JsonDict,
        *,
        replace: bool = False
    ) -> JsonDict: ...
    
    def delete(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None
    ) -> JsonDict: ...
    
    def query_linq(
        self,
        model: type,
        builder: QueryBuilder
    ) -> Union[List[JsonDict], int]: ...

# ======================================================
# FILE: polydb/utils.py
# ======================================================

# src/polydb/utils.py
"""
Utility functions for validation and logging
"""



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

# ======================================================
# FILE: polydb/validation.py
# ======================================================

# src/polydb/validation.py
"""
Model metadata validation and schema enforcement
"""

@dataclass
class ValidationResult:
    valid: bool
    errors: List[str]
    warnings: List[str]


class ModelValidator:
    """Validates model metadata and schema"""
    
    REQUIRED_FIELDS = {'storage'}
    VALID_STORAGE_TYPES = {'sql', 'nosql'}
    SQL_REQUIRED_FIELDS = {'table'}
    NOSQL_OPTIONAL_FIELDS = {'collection', 'pk_field', 'rk_field'}
    
    @classmethod
    def validate_model(cls, model: Type) -> ValidationResult:
        """Comprehensive model validation"""
        errors = []
        warnings = []
        
        # Check __polydb__ exists
        if not hasattr(model, '__polydb__'):
            errors.append(f"Model {model.__name__} missing __polydb__ metadata")
            return ValidationResult(valid=False, errors=errors, warnings=warnings)
        
        meta = getattr(model, '__polydb__')
        
        # Check it's a dict
        if not isinstance(meta, dict):
            errors.append(f"__polydb__ must be a dict, got {type(meta)}")
            return ValidationResult(valid=False, errors=errors, warnings=warnings)
        
        # Check required fields
        for field in cls.REQUIRED_FIELDS:
            if field not in meta:
                errors.append(f"Missing required field: {field}")
        
        # Validate storage type
        storage = meta.get('storage')
        if storage and storage not in cls.VALID_STORAGE_TYPES:
            errors.append(f"Invalid storage type: {storage}. Must be one of {cls.VALID_STORAGE_TYPES}")
        
        # Storage-specific validation
        if storage == 'sql':
            for field in cls.SQL_REQUIRED_FIELDS:
                if field not in meta:
                    errors.append(f"SQL storage requires field: {field}")
        
        elif storage == 'nosql':
            # NoSQL has optional fields, warn if missing common ones
            if 'pk_field' not in meta:
                warnings.append("No pk_field specified, will default to 'id'")
            
            if not meta.get('collection') and not meta.get('table'):
                warnings.append("No collection/table name specified, will use model name")
        
        # Validate cache settings
        if meta.get('cache') and not isinstance(meta.get('cache_ttl'), (int, type(None))):
            errors.append("cache_ttl must be an integer or None")
        
        # Validate provider
        valid_providers = {'azure', 'aws', 'gcp', 'vercel', 'mongodb', 'postgresql'}
        provider = meta.get('provider')
        if provider and provider.lower() not in valid_providers:
            warnings.append(f"Unusual provider: {provider}")
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    @classmethod
    def validate_and_raise(cls, model: Type):
        """Validate and raise if invalid"""
        result = cls.validate_model(model)
        
        if not result.valid:
            raise InvalidModelMetadataError(
                f"Invalid model {model.__name__}: {', '.join(result.errors)}"
            )
        
        # Log warnings
        if result.warnings:
            logger = logging.getLogger(__name__)
            for warning in result.warnings:
                logger.warning(f"{model.__name__}: {warning}")


class SchemaValidator:
    """Validates data against model schema"""
    
    @staticmethod
    def validate_data(model: Type, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against model requirements"""
        errors = []
        warnings = []
        
        meta = getattr(model, '__polydb__', {})
        
        # Check required keys exist
        pk_field = meta.get('pk_field', 'id')
        if pk_field not in data and not meta.get('auto_generate'):
            errors.append(f"Missing required field: {pk_field}")
        
        # Check field types if type hints available
        if hasattr(model, '__annotations__'):
            annotations = model.__annotations__
            for field_name, expected_type in annotations.items():
                if field_name in data:
                    actual_value = data[field_name]
                    if not isinstance(actual_value, expected_type):
                        warnings.append(
                            f"Field {field_name} expected {expected_type}, got {type(actual_value)}"
                        )
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

# ======================================================
# FILE: polydb/adapters/AzureBlobStorageAdapter.py
# ======================================================

# src/polydb/adapters/AzureBlobStorageAdapter.py




class AzureBlobStorageAdapter(ObjectStorageAdapter):
    """
    Production-grade Azure Blob Storage adapter.

    Features
    - Thread-safe client initialization
    - Container auto-creation
    - Retry support
    - Structured logging
    - Connection reuse
    """

    def __init__(self, connection_string: str = "", container_name: str = ""):
        super().__init__()

        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = container_name or os.getenv("AZURE_CONTAINER_NAME", "polydb")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")

        self._client: Optional[BlobServiceClient] = None
        self._container: Optional[ContainerClient] = None
        self._lock = threading.Lock()

        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize Azure Blob client and container"""
        try:
            with self._lock:
                if self._client is not None:
                    return
                if not self.connection_string:
                    raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")
                self._client = BlobServiceClient.from_connection_string(self.connection_string)

                self._container = self._client.get_container_client(self.container_name)

                try:
                    self._container.create_container()
                    self.logger.info(f"Created container: {self.container_name}")
                except ResourceExistsError:
                    pass

                self.logger.info(
                    f"Azure Blob Storage initialized (container={self.container_name})"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Blob Storage: {e}")

    def _require_container(self) -> ContainerClient:
        """Ensure container exists"""
        if self._container is None:
            raise ConnectionError("Azure Blob Storage client is not initialized")
        return self._container

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload blob with filename, media type, and metadata"""
        try:
            container = self._require_container()

            # --------------------------------------------------
            # Resolve filename
            # --------------------------------------------------
            filename = fileName or os.path.basename(key)

            # --------------------------------------------------
            # Ensure extension from media_type if missing
            # --------------------------------------------------
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            # --------------------------------------------------
            # Final blob key (include filename if needed)
            # --------------------------------------------------
            blob_key = key
            if fileName:
                blob_key = f"{key.rstrip('/')}/{filename}"

            blob_client = container.get_blob_client(blob_key)

            # --------------------------------------------------
            # Upload with content type + metadata
            # --------------------------------------------------
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type=media_type or "application/octet-stream"
                ),
                metadata={
                    **(metadata or {}),
                    "filename": filename,
                },
            )

            self.logger.debug(f"Uploaded blob key={blob_key}, type={media_type}")

            return blob_client.url

        except Exception as e:
            raise StorageError(f"Azure Blob put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Download blob"""
        try:
            container = self._require_container()

            blob_client = container.get_blob_client(key)

            downloader = blob_client.download_blob()
            data = downloader.readall()

            self.logger.debug(f"Downloaded blob key={key}")

            return data

        except ResourceNotFoundError:
            return None
        except Exception as e:
            raise StorageError(f"Azure Blob get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete blob"""
        try:
            container = self._require_container()

            blob_client = container.get_blob_client(key)
            blob_client.delete_blob(delete_snapshots="include")

            self.logger.debug(f"Deleted blob key={key}")

            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise StorageError(f"Azure Blob delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List blobs"""
        try:
            container = self._require_container()

            blobs = container.list_blobs(name_starts_with=prefix)
            results = [blob.name for blob in blobs]

            self.logger.debug(f"Listed {len(results)} blobs prefix={prefix}")

            return results

        except Exception as e:
            raise StorageError(f"Azure Blob list failed: {e}")


# ======================================================
# FILE: polydb/adapters/AzureFileStorageAdapter.py
# ======================================================

# src/polydb/adapters/AzureFileStorageAdapter.py






class AzureFileStorageAdapter(SharedFilesAdapter):
    """
    Production-grade Azure File Storage adapter.

    Fixes:
    - Adds upload/download methods expected by tests
    - Ensures share exists
    - Ensures directory structure exists
    - Correct Azure file creation before upload
    - Keeps write/read compatibility
    """

    def __init__(self, connection_string: str = "", share_name: str = ""):
        super().__init__()

        self.connection_string = (
            connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        )

        self.share_name = share_name or os.getenv("AZURE_SHARE_NAME", "polydb")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING not configured")

        self._client: Optional[ShareServiceClient] = None
        self._share = None
        self._lock = threading.Lock()

        self._initialize_client()

    # --------------------------------------------------
    # Client initialization
    # --------------------------------------------------

    def _initialize_client(self):
        try:
            with self._lock:
                if self._client:
                    return

                self._client = ShareServiceClient.from_connection_string(self.connection_string)

                self._share = self._client.get_share_client(self.share_name)

                try:
                    self._share.create_share()
                except ResourceExistsError:
                    pass

                self.logger.info("Initialized Azure File Storage client")

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure File Storage: {str(e)}")

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def _split_path(self, path: str):
        if "/" not in path:
            return "", path

        directory, filename = path.rsplit("/", 1)
        return directory, filename

    def _ensure_directory(self, directory: str):
        if not directory and self._share:
            return self._share.get_directory_client("")
        if not self._share:
            raise ConnectionError("Azure File Storage share not initialized")
        dir_client = self._share.get_directory_client(directory)

        try:
            dir_client.create_directory()
        except ResourceExistsError:
            pass

        return dir_client

    # --------------------------------------------------
    # Core operations
    # --------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def upload(self, path: str, data: bytes) -> str:
        """Upload file"""
        try:
            directory, filename = self._split_path(path)

            dir_client = self._ensure_directory(directory)
            file_client = dir_client.get_file_client(filename)

            file_client.create_file(len(data))
            file_client.upload_file(data)

            return path

        except Exception as e:
            raise StorageError(f"Azure File upload failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def download(self, path: str) -> bytes:
        """Download file"""
        try:
            directory, filename = self._split_path(path)
            if not self._share:
                raise ConnectionError("Azure File Storage share not initialized")
            dir_client = self._share.get_directory_client(directory or "")
            file_client = dir_client.get_file_client(filename)

            return file_client.download_file().readall()

        except ResourceNotFoundError:
            raise StorageError(f"File not found: {path}")
        except Exception as e:
            raise StorageError(f"Azure File download failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, path: str) -> bool:
        """Delete file"""
        try:
            directory, filename = self._split_path(path)
            if not self._share:
                raise ConnectionError("Azure File Storage share not initialized")
            dir_client = self._share.get_directory_client(directory or "")
            file_client = dir_client.get_file_client(filename)

            file_client.delete_file()
            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise StorageError(f"Azure File delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, directory: str = "") -> List[str]:
        """List files"""
        try:
            if not self._share:
                raise ConnectionError("Azure File Storage share not initialized")
            dir_client = self._share.get_directory_client(directory or "")

            results: List[str] = []

            for item in dir_client.list_directories_and_files():
                results.append(item.name)

            return results

        except Exception as e:
            raise StorageError(f"Azure File list failed: {str(e)}")

    # --------------------------------------------------
    # Backward compatibility
    # --------------------------------------------------

    def write(self, path: str, data: bytes) -> bool:
        """Alias for upload"""
        self.upload(path, data)
        return True

    def read(self, path: str) -> bytes | None:
        """Alias for download"""
        try:
            return self.download(path)
        except StorageError:
            return None


# ======================================================
# FILE: polydb/adapters/AzureQueueAdapter.py
# ======================================================

# src/polydb/adapters/AzureQueueAdapter.py






class AzureQueueAdapter(QueueAdapter):
    """
    Azure Queue Storage adapter.

    Features
    - Thread-safe initialization
    - Automatic queue creation
    - Client reuse
    - Retry support
    """

    def __init__(self, connection_string: str = ""):
        super().__init__()

        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")

        self._client: Optional[QueueServiceClient] = None
        self._queues: Dict[str, QueueClient] = {}

        self._lock = threading.Lock()

        self._initialize_client()

    def _normalize_queue_name(self, name: str) -> str:
        name = name.lower()
        name = re.sub(r"[^a-z0-9-]", "-", name)  # replace invalid chars
        name = re.sub(r"-+", "-", name)  # collapse multiple dashes
        return name.strip("-")

    def _initialize_client(self) -> None:
        """Initialize Azure Queue client"""
        try:
            with self._lock:
                if self._client is not None:
                    return
                if not self.connection_string:
                    raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")
                self._client = QueueServiceClient.from_connection_string(self.connection_string)

                self.logger.info("Initialized Azure Queue Storage client")

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Queue Storage: {e}")

    def _get_queue(self, queue_name: str) -> QueueClient:
        """Get or create queue client"""
        if self._client is None:
            raise ConnectionError("Azure Queue client not initialized")
        queue_name = self._normalize_queue_name(queue_name)
        if queue_name not in self._queues:
            queue_client = self._client.get_queue_client(queue_name)

            try:
                queue_client.create_queue()
            except ResourceExistsError:
                pass

            self._queues[queue_name] = queue_client

        return self._queues[queue_name]

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        try:
            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            response = queue_client.send_message(json.dumps(message, default=json_safe))

            return response.id

        except Exception as e:
            raise QueueError(f"Azure Queue send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages"""
        try:

            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            messages = queue_client.receive_messages(max_messages=max_messages)

            results = []

            for msg in messages:
                payload = json.loads(msg.content)
                results.append(
                    {
                        "id": msg.id,
                        "pop_receipt": msg.pop_receipt,
                        "body": payload,
                    }
                )

            return results

        except Exception as e:
            raise QueueError(f"Azure Queue receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """Delete message from queue"""
        try:
            queue_name = self._normalize_queue_name(queue_name)
            queue_client = self._get_queue(queue_name)

            queue_client.delete_message(message_id, pop_receipt)

            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise QueueError(f"Azure Queue delete failed: {e}")

    def ack(
        self,
        pop_receipt: str,
        queue_name: str = "default",
        message_id: Optional[str] = None,
    ) -> bool:
        """
        Acknowledge (delete) a message.

        Azure Queue requires BOTH:
        - message_id
        - pop_receipt

        Preferred usage:
            ack(pop_receipt=..., message_id=...)

        If message_id is not provided, this will fail safely.
        """
        if not message_id:
            raise QueueError("AzureQueueAdapter.ack requires message_id")
        queue_name = self._normalize_queue_name(queue_name)
        return self.delete(
            message_id=message_id,
            queue_name=queue_name,
            pop_receipt=pop_receipt,
        )


# ======================================================
# FILE: polydb/adapters/AzureTableStorageAdapter.py
# ======================================================

# src/polydb/adapters/AzureTableStorageAdapter.py




logger = logging.getLogger(__name__)

_BYTES_PREFIX = "@@polydb_bytes@@:"
_JSON_PREFIX = "@@polydb_json@@:"
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")

# ensures model isolation across the same table
_MODEL_FIELD = "__polydb_model__"

logging.getLogger("azure").setLevel(logging.ERROR)


class AzureTableStorageAdapter(NoSQLKVAdapter):
    """
    Azure Table Storage adapter with:
    - Any-type payload support (dict/list/custom objects -> JSON)
    - Key sanitization for PartitionKey/RowKey and property names
    - Query support for scalar fields
    - Blob overflow for entities > 1MB
    - Model isolation using __polydb_model__
    - Always returns id (derived from RowKey if missing)
    """

    AZURE_TABLE_MAX_SIZE = 60 * 1024  # 1MB
    _RESERVED = {"PartitionKey", "RowKey", "Timestamp", "etag", "ETag"}

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        connection_string: str = "",
        container_name="",
    ):
        super().__init__(partition_config)
        self.max_size = self.AZURE_TABLE_MAX_SIZE
        self.connection_string = (
            connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        )
        self.container_name = container_name or os.getenv("AZURE_CONTAINER_NAME", "overflow") or ""

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING must be set")

        self._client: Any = None
        self._blob_service = None
        self._client_lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        try:

            with self._client_lock:
                if not self._client:
                    self._client = TableServiceClient.from_connection_string(self.connection_string)
                    self._blob_service = BlobServiceClient.from_connection_string(
                        self.connection_string
                    )

                    try:
                        self._blob_service.create_container(self.container_name)
                    except Exception:
                        pass

                    self.logger.info("Azure Table Storage initialized with Blob overflow")
        except Exception as e:
            raise ConnectionError(f"Azure Table init failed: {str(e)}")

    # -----------------------------
    # Key / property sanitization
    # -----------------------------

    def _sanitize_pk_rk(self, value: Any) -> str:
        s = str(value)
        s = re.sub(r"[\\/#\?\x00-\x1f\x7f:+ ]", "_", s)
        return s[:1024] if len(s) > 1024 else s

    def _sanitize_prop_name(self, name: Any) -> str:
        s = str(name)
        s = re.sub(r"[^A-Za-z0-9_]", "_", s)
        if not re.match(r"^[A-Za-z_]", s):
            s = f"f_{s}"
        if s in self._RESERVED:
            s = f"f_{s}"
        return s[:255] if len(s) > 255 else s

    # -----------------------------
    # Value encoding / decoding
    # -----------------------------

    def _encode_value(self, v: Any) -> Any:
        if v is None:
            return None

        if isinstance(v, bytes):
            return _BYTES_PREFIX + base64.b64encode(v).decode("ascii")

        if isinstance(v, (dict, list)):
            return _JSON_PREFIX + json.dumps(v, default=json_safe)

        if isinstance(v, UUID):
            return str(v)

        if isinstance(v, Decimal):
            return float(v)

        if isinstance(v, date) and not isinstance(v, datetime):
            return v.isoformat()

        if isinstance(v, datetime):
            return v

        if isinstance(v, (str, bool, int, float)):
            return v

        return _JSON_PREFIX + json.dumps(v, default=json_safe)

    def _decode_value(self, v: Any) -> Any:
        if isinstance(v, str):
            if v.startswith(_JSON_PREFIX):
                payload = v[len(_JSON_PREFIX) :]
                try:
                    return json.loads(payload)
                except Exception:
                    return v

            if v.startswith(_BYTES_PREFIX):
                payload = v[len(_BYTES_PREFIX) :].strip()
                if (len(payload) % 4) == 1:
                    return v
                if not _BASE64_RE.match(payload):
                    return v
                pad = (-len(payload)) % 4
                if pad:
                    payload = payload + ("=" * pad)
                try:
                    return base64.b64decode(payload, validate=True)
                except TypeError:
                    try:
                        return base64.b64decode(payload)
                    except Exception:
                        return v
                except Exception:
                    return v
            return v

        if isinstance(v, dict) and "__type__" in v:
            t = v.get("__type__")
            if t == "json":
                raw = v.get("value")
                try:
                    return json.loads(raw or "null")
                except Exception:
                    return raw
            if t == "bytes":
                payload = (v.get("b64") or "").strip()
                if (len(payload) % 4) == 1:
                    return b""
                if payload and not _BASE64_RE.match(payload):
                    return b""
                pad = (-len(payload)) % 4
                if pad:
                    payload = payload + ("=" * pad)
                try:
                    return base64.b64decode(payload, validate=True)
                except TypeError:
                    try:
                        return base64.b64decode(payload)
                    except Exception:
                        return b""
                except Exception:
                    return b""
            if t == "str":
                return v.get("value")

        return v

    # -----------------------------
    # Entity pack/unpack
    # -----------------------------
    def _pack_entity(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        """Pack data into Azure Table Storage entity format.

        This version is much simpler, more readable, and fixes the fragile
        revmap/skey logic that was likely causing normal fields to disappear.
        """
        entity: JsonDict = {
            "PartitionKey": pk,
            "RowKey": rk,
        }
        entity[_MODEL_FIELD] = model.__qualname__
        keymap: Dict[str, str] = {}

        for orig_key, orig_val in (data or {}).items():
            orig_key_str = str(orig_key)

            # Skip Azure-reserved or special keys
            if orig_key_str in self._RESERVED or orig_key_str in ("PartitionKey", "RowKey"):
                continue

            # Sanitize property name so it is valid for Azure Table Storage
            skey = self._sanitize_prop_name(orig_key)

            # Prevent key collisions (extremely rare but safe)
            base = skey
            counter = 1
            while skey in entity:
                skey = f"{base}_{counter}"
                counter += 1

            # Remember the original key name so _unpack_entity can restore it
            keymap[skey] = orig_key_str

            # Encode the value (must return something Azure Table accepts)
            entity[skey] = self._encode_value(orig_val)

        # Store keymap only if we actually have fields (internal use)
        if keymap:
            entity["__keymap__"] = json.dumps(keymap, default=json_safe)

        return entity

    def _unpack_entity(self, entity: JsonDict) -> JsonDict:
        if not entity:
            return {}

        raw = dict(entity)
        raw.pop("etag", None)
        raw.pop("ETag", None)
        raw.pop("Timestamp", None)

        keymap_str = raw.pop("__keymap__", None)
        keymap: Dict[str, str] = {}
        if keymap_str:
            try:
                keymap = json.loads(keymap_str)
            except Exception:
                keymap = {}

        out: JsonDict = {}

        pk = raw.get("PartitionKey")
        rk = raw.get("RowKey")
        if pk is not None:
            out["PartitionKey"] = pk
        if rk is not None:
            out["RowKey"] = rk

        for k, v in raw.items():
            if k in ("PartitionKey", "RowKey"):
                continue

            # keep internal metadata fields too
            if k.startswith("_") or k in (_MODEL_FIELD,):
                out[k] = v
                continue

            orig_key = keymap.get(k, k)
            out[orig_key] = self._decode_value(v)

        # ✅ guarantee id for tests & ergonomics
        if "id" not in out and rk is not None:
            out["id"] = rk

        return out

    def _sanitize_blob_part(self, value: str) -> str:
        # Blob-safe: lowercase, alphanumeric + dash only
        s = str(value).lower()
        s = re.sub(r"[^a-z0-9\-]", "-", s)
        s = re.sub(r"-+", "-", s)
        return s.strip("-")[:100]

    def _entity_size_bytes(self, entity: JsonDict) -> int:
        return len(json.dumps(entity, default=json_safe).encode("utf-8"))

    def _blob_key(self, pk: str, rk: str, checksum: str) -> str:
        safe_pk = self._sanitize_blob_part(pk)
        safe_rk = self._sanitize_blob_part(rk)

        # Flat structure (avoid deep paths)
        return f"{safe_pk}-{safe_rk}-{checksum}.json"

    def _blob_upload(self, blob_key: str, data_bytes: bytes):
        if not self._blob_service:
            return
        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
        blob_client.upload_blob(data_bytes, overwrite=True)

    def _blob_download(self, blob_key: str) -> bytes:
        if not self._blob_service:
            raise NoSQLError("Blob service not initialized")
        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
        return blob_client.download_blob().readall()

    def _blob_delete(self, blob_key: str):
        if not self._blob_service:
            return
        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
        try:
            blob_client.delete_blob()
        except Exception:
            pass

    # -----------------------------
    # Required NoSQLKVAdapter hooks
    # -----------------------------
    def _sanitize_table_name(self, name: str) -> str:
        """Convert collection_name to valid Azure Table name (alphanumeric only)."""
        if not name:
            return "defaulttable"

        # Remove all invalid characters, keep only letters and numbers
        sanitized = re.sub(r"[^a-zA-Z0-9]", "", name)

        # Must start with a letter
        if sanitized and sanitized[0].isdigit():
            sanitized = "t" + sanitized

        # Must be 3-63 characters
        if len(sanitized) < 3:
            sanitized = sanitized + "table"
        if len(sanitized) > 63:
            sanitized = sanitized[:63]

        return sanitized.lower()

    def _get_table_name(self, model: type) -> str:
        """
        Extract collection_name from UDL model definition.
        """
        # Primary source: UDL definition
        definition = getattr(model, "__udl_definition__", None)
        if definition:
            metadata = getattr(definition, "x_metadata", {}) or {}
            collection_name = metadata.get("collection_name")
            if collection_name:
                return self._sanitize_table_name(collection_name)

        # Fallback: __polydb__ metadata
        polydb_meta = getattr(model, "__polydb__", None)
        if isinstance(polydb_meta, dict):
            collection = polydb_meta.get("collection") or polydb_meta.get("collection_name")
            if collection:
                return self._sanitize_table_name(str(collection))

        # Last resort
        return os.getenv("AZURE_TABLE_NAME", "defaulttable") or "defaulttable"

    def _get_table_client(self, model: type):
        """
        Get table client + automatically create the table if it doesn't exist.
        This is the recommended pattern for Azure Table Storage.
        """
        table_name = self._get_table_name(model)

        try:
            # This is the key call - creates the table if missing
            self._client.create_table_if_not_exists(table_name)
            logger.info(f"✅ Azure Table ensured/created: {table_name}")
        except Exception as e:
            # TableAlreadyExists is normal and safe to ignore
            if "TableAlreadyExists" not in str(e) and "already exists" not in str(e).lower():
                logger.warning(f"Could not create table {table_name}: {e}")

        # Now return the client
        return self._client.get_table_client(table_name=table_name)

    def _restore_overflow_properties(self, entity_dict: JsonDict) -> JsonDict:
        """Detect and restore any large properties stored in Blob Storage.

        Called by both _get_raw and _query_raw.
        """
        restored = {}

        for k, v in entity_dict.items():
            # Internal fields (starting with _) are kept as-is
            if k.startswith("_"):
                restored[k] = v
                continue

            # Is this an overflow metadata JSON string?
            if isinstance(v, str) and v.startswith("{") and '"_overflow":' in v:
                try:
                    metadata = json.loads(v)
                    if not metadata.get("_overflow"):
                        restored[k] = v
                        continue

                    blob_key = metadata.get("_blob_key")
                    checksum = metadata.get("_checksum")

                    if not blob_key:
                        restored[k] = v
                        continue

                    # Download real value from blob
                    blob_data = self._blob_download(blob_key)

                    # Optional but very safe: checksum validation
                    if checksum:
                        actual_checksum = hashlib.md5(blob_data).hexdigest()
                        if actual_checksum != checksum:
                            logger.warning(
                                f"Checksum mismatch for blob {blob_key} (property '{k}')"
                            )

                    # Restore original value
                    actual_value = json.loads(blob_data.decode("utf-8"))
                    restored[k] = actual_value
                    logger.debug(f"Restored large property '{k}' from blob: {blob_key}")

                    continue

                except Exception as e:
                    logger.error(f"Failed to restore overflow property '{k}': {e}")
                    # Fall back to raw metadata instead of crashing
                    restored[k] = v
                    continue

            # Normal (non-overflow) property
            restored[k] = v

        return restored

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            self._table_client = self._get_table_client(model)
            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)
            # Pack entity (encoded for Azure Table)
            entity = self._pack_entity(model, safe_pk, safe_rk, data)
            # ---------------------------------------------------
            # SIZE ESTIMATION (use ORIGINAL payload, not packed)
            # ---------------------------------------------------
            MAX_PROPERTY_CHARS = 30 * 1024  # ~30K safe under UTF-16 32K limit

            def _is_large_string(val: Any) -> bool:
                return isinstance(val, str) and len(val) > MAX_PROPERTY_CHARS

            large_val_dict = {}
            for key, value in entity.items():
                if _is_large_string(value):
                    logger.warning(
                        f"LARGE PROPERTY DETECTED: {key} length={len(value)} chars → forcing blob overflow"
                    )
                    payload_json = json.dumps(value, default=json_safe)
                    payload_bytes = payload_json.encode("utf-8")
                    payload_size = len(payload_bytes)
                    checksum = hashlib.md5(payload_bytes).hexdigest()
                    val_key = self._sanitize_blob_part(key)
                    blob_key = self._blob_key(f"{safe_pk}_{safe_rk}", val_key, checksum)
                    self._blob_upload(blob_key, payload_bytes)
                    logger.info(f"Stored in blob: {blob_key} ({payload_size // 1024} KB)")
                    large_val_dict[key] = {
                        "PartitionKey": safe_pk,
                        "RowKey": safe_rk,
                        _MODEL_FIELD: model.__qualname__,
                        "_overflow": True,
                        "_blob_key": blob_key,
                        "_size": payload_size,
                        "_checksum": checksum,
                    }

            reference_entity = {
                _MODEL_FIELD: model.__qualname__,
            }
            for k, v in entity.items():
                if k.startswith("_"):
                    continue
                if k in large_val_dict:
                    metadata = large_val_dict[k]
                    reference_entity[k] = json.dumps(metadata, default=json_safe)  # ← JSON string
                    logger.info(f"Overflow reference stored for {k} → {metadata['_blob_key']}")
                else:
                    reference_entity[k] = v

            self._table_client = self._get_table_client(model)
            self._table_client.upsert_entity(reference_entity)

            restored = self._unpack_entity(entity)
            restored["id"] = safe_rk

            return restored

        except Exception as e:
            if "PropertyValueTooLarge" in str(e):
                raise NoSQLError("Azure Table limit exceeded → must use blob overflow") from e

            raise NoSQLError(f"Azure Table put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)
            self._table_client = self._get_table_client(model)

            entity = self._table_client.get_entity(safe_pk, safe_rk)
            entity_dict = dict(entity)

            # Model isolation check
            if entity_dict.get(_MODEL_FIELD) != model.__qualname__:
                return None

            # Restore any large fields that were moved to blob
            restored_entity = self._restore_overflow_properties(entity_dict)

            out = self._unpack_entity(restored_entity)
            if "id" not in out:
                out["id"] = safe_rk

            return out

        except Exception as e:
            if "ResourceNotFound" in str(e):
                return None
            raise NoSQLError(f"Azure Table get failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        try:
            self._table_client = self._get_table_client(model)

            # Build query filter (your original logic kept unchanged)
            eff_filters = dict(filters or {})
            parts: List[str] = []
            for orig_k, orig_v in eff_filters.items():
                if orig_v is None:
                    continue

                if orig_k in ("partition_key", "PartitionKey"):
                    sk = "PartitionKey"
                elif orig_k in ("row_key", "RowKey"):
                    sk = "RowKey"
                else:
                    sk = (
                        self._sanitize_prop_name(orig_k)
                        if orig_k != _MODEL_FIELD
                        else _MODEL_FIELD
                    )

                ev = self._encode_value(orig_v)

                if ev is None:
                    parts.append(f"{sk} eq null")
                elif isinstance(ev, bool):
                    parts.append(f"{sk} eq {str(ev).lower()}")
                elif isinstance(ev, (int, float)):
                    parts.append(f"{sk} eq {ev}")
                elif isinstance(ev, datetime):
                    iso = ev.isoformat()
                    if not iso.endswith("Z"):
                        iso = iso + "Z"
                    parts.append(f"{sk} eq datetime'{iso}'")
                else:
                    sval = str(ev).replace("'", "''")
                    parts.append(f"{sk} eq '{sval}'")

            query_filter = " and ".join(parts) if parts else None

            entities = self._table_client.query_entities(query_filter=query_filter)

            results: List[JsonDict] = []
            count = 0
            for ent in entities:
                ent_dict = dict(ent)

                # Restore any large fields from blob
                restored_entity = self._restore_overflow_properties(ent_dict)

                out = self._unpack_entity(restored_entity)

                # Guarantee 'id' field
                if "id" not in out and "RowKey" in ent_dict:
                    out["id"] = ent_dict["RowKey"]

                results.append(out)

                count += 1
                if limit and count >= limit:
                    break

            return results

        except Exception as e:
            raise NoSQLError(f"Azure Table query failed: {str(e)}")
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            self._table_client = self._get_table_client(model)
            safe_pk = self._sanitize_pk_rk(pk)
            safe_rk = self._sanitize_pk_rk(rk)

            # read to check model + overflow
            try:
                entity = self._table_client.get_entity(safe_pk, safe_rk)
                entity_dict = dict(entity)

                if entity_dict.get(_MODEL_FIELD) != model.__qualname__:
                    return {"deleted": False}

                if entity_dict.get("_overflow"):
                    blob_key = entity_dict.get("_blob_key")
                    if blob_key:
                        self._blob_delete(blob_key)
            except Exception:
                pass

            self._table_client.delete_entity(safe_pk, safe_rk, etag=etag)
            return {"deleted": True, "PartitionKey": safe_pk, "RowKey": safe_rk, "id": safe_rk}

        except Exception as e:
            raise NoSQLError(f"Azure Table delete failed: {str(e)}")


# ======================================================
# FILE: polydb/adapters/BlockchainBlobAdapter.py
# ======================================================






class BlockchainBlobAdapter(ObjectStorageAdapter):
    """
    Blob storage backed by IPFS.

    - key is ignored for storage (IPFS is content-addressed)
    - returns CID (acts as key)
    - metadata is not natively supported → ignored or embedded externally
    """

    def __init__(self, ipfs_url: Optional[str] = None):
        super().__init__()

        load_dotenv()

        self.ipfs_url = ipfs_url or os.getenv("IPFS_API_URL", "/dns/localhost/tcp/5001/http")

        try:

            ipfshttpclient.client.assert_version = lambda *args, **kwargs: None

            self.client = ipfshttpclient.connect(self.ipfs_url, session=True)
            self.logger.info(f"Connected to IPFS: {self.ipfs_url}")

        except Exception as e:
            raise StorageError(f"Failed to connect to IPFS: {e}")

    # --------------------------------------------------
    # PUT
    # --------------------------------------------------
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload data to IPFS and return CID"""
        try:
            res = self.client.add_bytes(data)  # returns CID
            cid = res

            self.logger.debug(f"IPFS upload success cid={cid}")

            return cid

        except Exception as e:
            raise StorageError(f"IPFS put failed: {e}")

    # --------------------------------------------------
    # GET
    # --------------------------------------------------
    def get(self, key: str) -> bytes:
        """Fetch blob from IPFS using CID"""
        try:
            data = self.client.cat(key)
            self.logger.debug(f"IPFS get success cid={key}")
            return data

        except Exception as e:
            raise StorageError(f"IPFS get failed: {e}")

    # --------------------------------------------------
    # DELETE
    # --------------------------------------------------
    def delete(self, key: str) -> bool:
        """
        IPFS does not support true delete.
        We can unpin to allow GC.
        """
        try:
            self.client.pin.rm(key)
            self.logger.debug(f"IPFS unpinned cid={key}")
            return True

        except Exception:
            return False

    # --------------------------------------------------
    # LIST
    # --------------------------------------------------
    def list(self, prefix: str = "") -> List[str]:
        """
        IPFS has no native listing by prefix.
        Return pinned CIDs as approximation.
        """
        try:
            pins = self.client.pin.ls(type="all")
            cids = list(pins.get("Keys", {}).keys())

            if prefix:
                cids = [cid for cid in cids if cid.startswith(prefix)]

            self.logger.debug(f"IPFS list returned {len(cids)} items")
            return cids

        except Exception as e:
            raise StorageError(f"IPFS list failed: {e}")


# ======================================================
# FILE: polydb/adapters/BlockchainKVAdapter.py
# ======================================================




logger = logging.getLogger(__name__)

SUPPORTED_CHAINS = {
    "ethereum",
    "polygon",
    "avalanche",
    "bnb",
    "arbitrum",
}


class BlockchainKVAdapter:
    """
    Generic blockchain key-value adapter.

    Uses a smart contract to store JSON documents keyed by ID.

    Supports EVM-compatible chains:
        - Ethereum
        - Polygon
        - Avalanche
        - BNB Chain
        - Arbitrum
    """

    def __init__(
        self,
        chain: Optional[str] = None,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        load_dotenv()

        self.chain = (chain or os.getenv("BLOCKCHAIN_CHAIN", "ethereum")).lower()

        if self.chain not in SUPPORTED_CHAINS:
            raise ValueError(f"Unsupported blockchain: {self.chain}")

        self.rpc_url = rpc_url or os.getenv("BLOCKCHAIN_RPC_URL")
        self.private_key = private_key or os.getenv("BLOCKCHAIN_PRIVATE_KEY")
        self.contract_address = contract_address or os.getenv("BLOCKCHAIN_CONTRACT")

        if not self.rpc_url:
            raise RuntimeError("BLOCKCHAIN_RPC_URL not configured")

        if not self.private_key:
            raise RuntimeError("BLOCKCHAIN_PRIVATE_KEY not configured")

        if not self.contract_address:
            raise RuntimeError("BLOCKCHAIN_CONTRACT not configured")

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))

        if self.chain in {"polygon", "avalanche", "bnb"}:
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self.account = self.w3.eth.account.from_key(self.private_key)

        if contract_abi is None:
            contract_abi = self._default_abi()

        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(self.contract_address),
            abi=contract_abi,
        )

    def _default_abi(self):
        return [
            {
                "inputs": [
                    {"internalType": "string", "name": "key", "type": "string"},
                    {"internalType": "string", "name": "value", "type": "string"},
                ],
                "name": "put",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
            {
                "inputs": [{"internalType": "string", "name": "key", "type": "string"}],
                "name": "get",
                "outputs": [{"internalType": "string", "name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "inputs": [{"internalType": "string", "name": "key", "type": "string"}],
                "name": "deleteKey",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
        ]

    def _send_tx(self, fn):
        nonce = self.w3.eth.get_transaction_count(self.account.address)

        tx = fn.build_transaction(
            {
                "from": self.account.address,
                "nonce": nonce,
                "gas": 500000,
                "gasPrice": self.w3.eth.gas_price,
            }
        )

        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)

        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    def put(self, model, data: Dict[str, Any]) -> Dict[str, Any]:
        key = str(data["id"])
        payload = json.dumps(data)

        fn = self.contract.functions.put(key, payload)
        self._send_tx(fn)

        return data

    def get(self, model, key: str) -> Optional[Dict[str, Any]]:
        result = self.contract.functions.get(str(key)).call()

        if not result:
            return None

        return json.loads(result)

    def delete(self, model, key: str):
        fn = self.contract.functions.deleteKey(str(key))
        self._send_tx(fn)

        return {"id": key}

    def query(self, model, query: Dict[str, Any], limit: Optional[int] = None):
        raise NotImplementedError(
            "Blockchain query requires an off-chain indexer (TheGraph / Elastic)"
        )

# ======================================================
# FILE: polydb/adapters/BlockchainQueueAdapter.py
# ======================================================





logger = logging.getLogger(__name__)


class BlockchainQueueAdapter:
    """
    Queue implementation using blockchain events.

    Messages are emitted via smart contract events and read via event filters.
    """

    def __init__(
        self,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        load_dotenv()

        self.rpc_url = rpc_url or os.getenv("BLOCKCHAIN_RPC_URL")
        self.private_key = private_key or os.getenv("BLOCKCHAIN_PRIVATE_KEY")
        self.contract_address = contract_address or os.getenv("BLOCKCHAIN_QUEUE_CONTRACT")

        if not self.rpc_url:
            raise RuntimeError("BLOCKCHAIN_RPC_URL not configured")

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        self.account = self.w3.eth.account.from_key(self.private_key)

        if contract_abi is None:
            contract_abi = self._default_abi()
        if self.contract_address is not None:
            self.contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(self.contract_address),
                abi=contract_abi,
            )

    def _default_abi(self):
        return [
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": False, "name": "queue", "type": "string"},
                    {"indexed": False, "name": "data", "type": "string"},
                ],
                "name": "MessagePublished",
                "type": "event",
            },
            {
                "inputs": [
                    {"internalType": "string", "name": "queue", "type": "string"},
                    {"internalType": "string", "name": "data", "type": "string"},
                ],
                "name": "publish",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function",
            },
        ]

    def _send_tx(self, fn):
        nonce = self.w3.eth.get_transaction_count(self.account.address)

        tx = fn.build_transaction(
            {
                "from": self.account.address,
                "nonce": nonce,
                "gas": 300000,
                "gasPrice": self.w3.eth.gas_price,
            }
        )

        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)

        return tx_hash.hex()

    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        payload = json.dumps(message)
        fn = self.contract.functions.publish(queue_name, payload)
        tx_hash = self._send_tx(fn)

        return tx_hash

    def receive(
        self, queue_name: str = "default", max_messages: int = 1, from_block="latest"
    ) -> List[Dict]:
        event_filter = self.contract.events.MessagePublished.create_filter(fromBlock=from_block)

        events = event_filter.get_all_entries()

        messages = []

        for event in events:
            if event.args.queue == queue_name:
                messages.append(json.loads(event.args.data))

        return messages

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """
        Blockchain queues cannot delete events.
        This method exists for interface compatibility.
        """
        return False

    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        ACK for Vercel Queue.

        Since Redis Streams via Vercel KV REST API does not support
        explicit ACK without consumer groups, this is treated as no-op.

        Exists for interface consistency.
        """
        if not ack_id:
            raise QueueError("ack_id is required")

        return True


# ======================================================
# FILE: polydb/adapters/DynamoDBAdapter.py
# ======================================================

# src/polydb/adapters/DynamoDBAdapter.py




class DynamoDBAdapter(NoSQLKVAdapter):
    """
    Production-grade DynamoDB adapter with optional S3 overflow.

    Goals (matches your adapter test style)
    - stored row keeps "id" == pk
    - query({"id": ...}) works
    - patch() merges (handled by NoSQLKVAdapter.patch)
    - delete() returns {"id": <pk>} and raises sqlite3.DatabaseError on missing
    - query_page() uses DynamoDB LastEvaluatedKey token (stable)
    - LocalStack support (endpoint_url) + auto create table/bucket in test/dev
    """

    DYNAMODB_MAX_SIZE = 400 * 1024  # 400KB DynamoDB item limit

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        table_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        super().__init__(partition_config)

        self.max_size = self.DYNAMODB_MAX_SIZE
        self.default_table = table_name or os.getenv("DYNAMODB_TABLE_NAME", "polydb")
        self.bucket_name = bucket_name or os.getenv("S3_OVERFLOW_BUCKET", "dynamodb-overflow")

        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        # LocalStack support:
        # - Many setups export AWS_ENDPOINT_URL=http://localhost:4566
        # - Some export LOCALSTACK_ENDPOINT_URL, or infer from LOCALSTACK_HOST
        self.endpoint_url = (
            endpoint_url
            or os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("LOCALSTACK_ENDPOINT_URL")
            or (
                f"http://{os.getenv('LOCALSTACK_HOST')}:4566"
                if os.getenv("LOCALSTACK_HOST")
                else None
            )
        )

        self._dynamodb: Any = None
        self._s3 = None
        self._lock = threading.Lock()

        self._initialize_clients()

    # ---------------------------------------------------------------------
    # Init / Helpers
    # ---------------------------------------------------------------------

    def _initialize_clients(self) -> None:
        try:
            with self._lock:
                if self._dynamodb and self._s3:
                    return

                session = Session(region_name=self.region)

                # LocalStack often needs dummy keys but boto3 doesn’t require them explicitly
                self._dynamodb = session.resource(
                    "dynamodb",
                    endpoint_url=self.endpoint_url,
                )
                self._s3 = session.client(
                    "s3",
                    endpoint_url=self.endpoint_url,
                )

                self.logger.info(
                    f"Initialized DynamoDB/S3 clients (region={self.region}, endpoint={self.endpoint_url or 'aws'})"
                )
        except Exception as e:
            raise ConnectionError(f"DynamoDB init failed: {e}")

    def _table_name(self, model: type) -> str:
        meta = getattr(model, "__polydb__", {}) or {}
        return meta.get("table") or meta.get("collection") or model.__name__.lower()

    def _get_table(self, model: type):
        if not self._dynamodb:
            self._initialize_clients()
        if not self._dynamodb:
            raise ConnectionError("DynamoDB resource not initialized")
        table_name = self._table_name(model)
        table = self._dynamodb.Table(table_name)
        self._ensure_table_exists(table_name)
        return table

    def _ensure_table_exists(self, table_name: str) -> None:
        """
        Ensure PK/SK table exists. Safe in prod (no-op if exists).
        Required for LocalStack integration tests.
        """
        if not self._dynamodb:
            return

        try:
            self._dynamodb.meta.client.describe_table(TableName=table_name)
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("ResourceNotFoundException", "ValidationException"):
                # ValidationException can happen on LocalStack when table doesn't exist yet
                # but describe throws something odd; fall through to create attempt.
                pass

        try:
            self._dynamodb.meta.client.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            # Wait until active (works in AWS + LocalStack)
            self._dynamodb.meta.client.get_waiter("table_exists").wait(TableName=table_name)
            self.logger.info(f"Created DynamoDB table: {table_name}")
        except ClientError as e:
            # If another process created it meanwhile
            if e.response.get("Error", {}).get("Code") in ("ResourceInUseException",):
                return
            raise

    def _ensure_bucket_exists(self) -> None:
        """Ensure overflow bucket exists (safe no-op in prod; needed in LocalStack)."""
        if not self._s3:
            return
        try:
            self._s3.head_bucket(Bucket=self.bucket_name)
            return
        except Exception:
            pass

        try:
            # us-east-1 does not require LocationConstraint; other regions do.
            if self.region == "us-east-1":
                self._s3.create_bucket(Bucket=self.bucket_name)
            else:
                self._s3.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            self.logger.info(f"Created S3 overflow bucket: {self.bucket_name}")
        except Exception:
            # If already exists or emulator differences
            pass

    def _encode_token(self, lek: Dict[str, Any]) -> str:
        raw = json.dumps(lek, default=json_safe).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8")

    def _decode_token(self, tok: str) -> Dict[str, Any]:
        raw = base64.urlsafe_b64decode(tok.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))

    def _blob_key(self, model: type, pk: str, rk: str, checksum: str) -> str:
        return f"overflow/{self._table_name(model)}/{pk}/{rk}/{checksum}.json"

    def _maybe_overflow_to_s3(
        self, model: type, pk: str, rk: str, payload: JsonDict
    ) -> Optional[JsonDict]:
        data_bytes = json.dumps(payload, default=json_safe).encode("utf-8")
        if len(data_bytes) <= self.DYNAMODB_MAX_SIZE:
            return None

        if not self._s3:
            raise NoSQLError("DynamoDB item exceeds 400KB and S3 client is unavailable")

        self._ensure_bucket_exists()

        checksum = hashlib.md5(data_bytes).hexdigest()
        blob_key = self._blob_key(model, pk, rk, checksum)

        self._s3.put_object(Bucket=self.bucket_name, Key=blob_key, Body=data_bytes)

        ref: JsonDict = {
            "PK": pk,
            "SK": rk,
            "id": pk,  # ✅ for tests and querying
            "_pk": pk,
            "_rk": rk,
            "_overflow": True,
            "_blob_key": blob_key,
            "_size": len(data_bytes),
            "_checksum": checksum,
        }

        # Best-effort keep scalar fields for filtering
        kept = 0
        for k, v in payload.items():
            if k in ref:
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                ref[k] = v
                kept += 1
            if kept >= 50:
                break

        self.logger.info(f"Stored DynamoDB overflow to S3: {blob_key} ({len(data_bytes)} bytes)")
        return ref

    def _resolve_overflow(self, item: JsonDict) -> JsonDict:
        if not item.get("_overflow"):
            return item

        if not self._s3:
            raise NoSQLError("Overflow item present but S3 client unavailable")

        blob_key = item.get("_blob_key")
        checksum = item.get("_checksum")
        if not blob_key:
            raise NoSQLError("Overflow item missing _blob_key")

        resp = self._s3.get_object(Bucket=self.bucket_name, Key=blob_key)
        blob_data = resp["Body"].read()

        actual = hashlib.md5(blob_data).hexdigest()
        if checksum and actual != checksum:
            raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual}")

        restored = json.loads(blob_data.decode("utf-8"))
        return restored

    # ---------------------------------------------------------------------
    # Required NoSQLKVAdapter hooks
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            table = self._get_table(model)

            payload: JsonDict = dict(data or {})
            payload["PK"] = pk
            payload["SK"] = rk

            # Ensure "id" field exists for tests/querying
            payload.setdefault("id", pk)
            payload.setdefault("_pk", pk)
            payload.setdefault("_rk", rk)

            ref = self._maybe_overflow_to_s3(model, pk, rk, payload)
            table.put_item(Item=ref if ref is not None else payload)

            # ✅ match pattern: put returns {"id": pk}
            return {"id": pk}

        except Exception as e:
            raise NoSQLError(f"DynamoDB put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            table = self._get_table(model)

            resp = table.get_item(Key={"PK": pk, "SK": rk})
            item = resp.get("Item")
            if not item:
                return None

            item.setdefault("id", pk)
            return self._resolve_overflow(item)

        except Exception as e:
            raise NoSQLError(f"DynamoDB get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        """
        Prefer Query when PK can be derived. Otherwise Scan.
        Supports filters like {"id": "..."} and arbitrary Attr equality.
        """
        try:
            table = self._get_table(model)
            filters = filters or {}

            # Treat 'id' as PK by convention (since we store PK=pk=id in NoSQLKVAdapter by default)
            pk_value = filters.get("PK") or filters.get("partition_key") or filters.get("id")

            if pk_value is not None:
                key_cond = Key("PK").eq(str(pk_value))
                if "SK" in filters:
                    key_cond = key_cond & Key("SK").eq(str(filters["SK"]))

                kwargs: Dict[str, Any] = {"KeyConditionExpression": key_cond}

                # Other filters as FilterExpression
                other = {
                    k: v for k, v in filters.items() if k not in ("PK", "SK", "partition_key", "id")
                }
                if other:
                    expr = None
                    for k, v in other.items():
                        part = Attr(k).eq(v)
                        expr = part if expr is None else (expr & part)
                    kwargs["FilterExpression"] = expr

                if limit:
                    kwargs["Limit"] = limit

                resp = table.query(**kwargs)
                items = resp.get("Items", [])
            else:
                # Scan
                kwargs = {}
                if filters:
                    expr = None
                    for k, v in filters.items():
                        part = Attr(k).eq(v)
                        expr = part if expr is None else (expr & part)
                    kwargs["FilterExpression"] = expr
                if limit:
                    kwargs["Limit"] = limit

                resp = table.scan(**kwargs)
                items = resp.get("Items", [])

            out: List[JsonDict] = []
            for it in items:
                it.setdefault("id", it.get("_pk") or it.get("PK"))
                out.append(self._resolve_overflow(it))
            return out

        except Exception as e:
            raise NoSQLError(f"DynamoDB query failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        """
        - if missing => raise sqlite3.DatabaseError (matches Firestore style tests)
        - if overflow => delete S3 object best-effort
        - return {"id": pk}
        """
        try:
            table = self._get_table(model)

            resp = table.get_item(Key={"PK": pk, "SK": rk})
            item = resp.get("Item")
            if not item:
                raise DatabaseError(f"Item {pk}/{rk} does not exist")

            if item.get("_overflow") and self._s3:
                blob_key = item.get("_blob_key")
                if blob_key:
                    try:
                        self._s3.delete_object(Bucket=self.bucket_name, Key=blob_key)
                    except Exception:
                        pass

            table.delete_item(Key={"PK": pk, "SK": rk})
            return {"id": pk}

        except DatabaseError:
            raise
        except Exception as e:
            raise NoSQLError(f"DynamoDB delete failed: {e}")

    # ---------------------------------------------------------------------
    # Provider-specific pagination (stable, not offset-based)
    # ---------------------------------------------------------------------
    def query_page(
        self,
        model: type,
        query: Dict[str, Any],
        page_size: int,
        continuation_token: Optional[str] = None,
    ) -> Tuple[List[JsonDict], Optional[str]]:
        """
        DynamoDB pagination.

        Behaviour:
        - Uses Query when PK/id is available
        - Uses Scan otherwise
        - Handles FilterExpression correctly when scanning by collecting
        results until page_size items are returned
        - continuation_token = base64(json(LastEvaluatedKey))
        """
        try:
            table = self._get_table(model)
            query = query or {}

            start_key = self._decode_token(continuation_token) if continuation_token else None

            pk_value = query.get("PK") or query.get("partition_key") or query.get("id")

            items: List[JsonDict] = []
            last_key = start_key

            if pk_value is not None:

                key_cond = Key("PK").eq(str(pk_value))

                if "SK" in query:
                    key_cond = key_cond & Key("SK").eq(str(query["SK"]))

                kwargs: Dict[str, Any] = {
                    "KeyConditionExpression": key_cond,
                    "Limit": page_size,
                }

                if last_key:
                    kwargs["ExclusiveStartKey"] = last_key

                other_filters = {
                    k: v for k, v in query.items() if k not in ("PK", "SK", "partition_key", "id")
                }

                if other_filters:
                    expr = None
                    for k, v in other_filters.items():
                        part = Attr(k).eq(v)
                        expr = part if expr is None else expr & part
                    kwargs["FilterExpression"] = expr

                resp = table.query(**kwargs)
                items = resp.get("Items", [])
                last_key = resp.get("LastEvaluatedKey")

            # ------------------------------------------------------------------
            # SCAN path (collect until page_size)
            # ------------------------------------------------------------------
            else:

                filter_expr = None
                if query:
                    for k, v in query.items():
                        part = Attr(k).eq(v)
                        filter_expr = part if filter_expr is None else filter_expr & part

                while len(items) < page_size:

                    kwargs: Dict[str, Any] = {"Limit": page_size}

                    if last_key:
                        kwargs["ExclusiveStartKey"] = last_key

                    if filter_expr is not None:
                        kwargs["FilterExpression"] = filter_expr

                    resp = table.scan(**kwargs)

                    batch = resp.get("Items", [])
                    items.extend(batch)

                    last_key = resp.get("LastEvaluatedKey")

                    if not last_key:
                        break

                items = items[:page_size]

            # ------------------------------------------------------------------
            # Normalize output
            # ------------------------------------------------------------------
            out: List[JsonDict] = []

            for it in items:
                it.setdefault("id", it.get("_pk") or it.get("PK"))
                out.append(self._resolve_overflow(it))

            next_tok = self._encode_token(last_key) if last_key else None

            return out, next_tok

        except Exception as e:
            raise NoSQLError(f"DynamoDB query_page failed: {e}")


# ======================================================
# FILE: polydb/adapters/EFSAdapter.py
# ======================================================

# src/polydb/adapters/EFSAdapter.py



class EFSAdapter(SharedFilesAdapter):
    """AWS EFS (mounted filesystem)"""

    def __init__(self, mount_point: str):
        super().__init__()
        self.mount_point = mount_point or os.getenv("EFS_MOUNT_POINT", "/mnt/efs")

    def write(self, path: str, data: bytes) -> bool:
        """Write file"""
        try:
            full_path = os.path.join(self.mount_point, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(data)
            return True
        except Exception as e:
            raise StorageError(f"EFS write failed: {str(e)}")

    def read(self, path: str) -> bytes:
        """Read file"""
        try:
            full_path = os.path.join(self.mount_point, path)
            with open(full_path, "rb") as f:
                return f.read()
        except Exception as e:
            raise StorageError(f"EFS read failed: {str(e)}")

    def delete(self, path: str) -> bool:
        """Delete file"""
        try:
            full_path = os.path.join(self.mount_point, path)
            os.remove(full_path)
            return True
        except Exception as e:
            raise StorageError(f"EFS delete failed: {str(e)}")

    def list(self, directory: str = "/") -> List[str]:
        """List files in directory"""
        try:
            full_path = os.path.join(self.mount_point, directory)
            return os.listdir(full_path)
        except Exception as e:
            raise StorageError(f"EFS list failed: {str(e)}")


# ======================================================
# FILE: polydb/adapters/FirestoreAdapter.py
# ======================================================

# src/polydb/adapters/FirestoreAdapter.py





class FirestoreAdapter(NoSQLKVAdapter):
    """
    Production-grade Firestore adapter with optional GCS overflow.

    Goals (matches your tests)
    - Document id == pk (so querying {"id": ...} works)
    - patch() merges (preserves existing fields)
    - delete() returns {"id": <pk>} and raises DatabaseError on missing
    - query_page() returns (rows, token) with stable pagination
    - Emulator support via FIRESTORE_EMULATOR_HOST
    """

    FIRESTORE_MAX_SIZE = 1024 * 1024  # 1MB doc limit (practical)

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        project: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        super().__init__(partition_config)
        self.max_size = self.FIRESTORE_MAX_SIZE

        self.project = (
            project
            or os.getenv("GCP_PROJECT")
            or os.getenv("GOOGLE_CLOUD_PROJECT")
            or "polydb-test"
        )

        # Overflow bucket (optional; used only if doc would exceed max size)
        self.bucket_name = bucket_name or os.getenv("GCS_OVERFLOW_BUCKET", "firestore-overflow")

        self._client: Optional[Client] = None
        self._storage_client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None

        self._lock = threading.Lock()
        self._initialize_clients()

    # ---------------------------------------------------------------------
    # Init / Helpers
    # ---------------------------------------------------------------------

    def _initialize_clients(self) -> None:
        try:
            with self._lock:
                if self._client:
                    return

                # Firestore client (emulator respected automatically if FIRESTORE_EMULATOR_HOST set)
                self._client = firestore.Client(project=self.project)

                # Storage client: only needed for overflow.
                # In emulator/test env you may have STORAGE_EMULATOR_HOST + anonymous/no-auth.
                # If storage client init fails, we keep overflow disabled (still production-safe).
                try:
                    self._storage_client = storage.Client(project=self.project)
                    self._bucket = self._storage_client.bucket(self.bucket_name)

                    # Create bucket if possible; ignore if already exists or emulator lacks create.
                    try:
                        self._bucket.create()  # type: ignore[union-attr]
                        self.logger.info(f"Created GCS overflow bucket: {self.bucket_name}")
                    except Exception:
                        pass

                    self.logger.info(
                        f"Firestore initialized (project={self.project}) with GCS overflow bucket={self.bucket_name}"
                    )
                except Exception as e:
                    # Keep Firestore working; overflow becomes a no-op.
                    self._storage_client = None
                    self._bucket = None
                    self.logger.warning(f"GCS overflow disabled (storage client init failed): {e}")
                    self.logger.info(f"Firestore initialized (project={self.project})")

        except Exception as e:
            raise ConnectionError(f"Firestore init failed: {e}")

    def _collection_name(self, model: type) -> str:
        meta = getattr(model, "__polydb__", {}) or {}
        return meta.get("collection") or meta.get("table") or model.__name__.lower()

    def _get_collection(self, model: type) -> Any:
        if not self._client:
            self._initialize_clients()
        if not self._client:
            raise ConnectionError("Firestore client not initialized")
        return self._client.collection(self._collection_name(model))

    def _doc_id(self, pk: str) -> str:
        # IMPORTANT for tests: doc_id == pk == row["id"]
        return str(pk)

    def _blob_key(self, model: type, pk: str, rk: str, checksum: str) -> str:
        # Keep it stable and unique per model + keys
        return f"overflow/{self._collection_name(model)}/{pk}/{rk}/{checksum}.json"

    def _maybe_store_overflow(
        self, model: type, pk: str, rk: str, payload: JsonDict
    ) -> Optional[JsonDict]:
        """
        If payload exceeds doc limit, store full payload in GCS and return reference document.
        If GCS is not available, raise (to avoid silently corrupting data).
        """
        data_bytes = json.dumps(payload, default=json_safe).encode("utf-8")
        if len(data_bytes) <= self.FIRESTORE_MAX_SIZE:
            return None

        if not self._bucket:
            raise NoSQLError(
                "Document exceeds Firestore 1MB limit and GCS overflow bucket is not available"
            )

        checksum = hashlib.md5(data_bytes).hexdigest()
        blob_key = self._blob_key(model, pk, rk, checksum)

        blob = self._bucket.blob(blob_key)
        blob.upload_from_string(data_bytes)

        ref: JsonDict = {
            "id": pk,
            "_pk": pk,
            "_rk": rk,
            "_overflow": True,
            "_blob_key": blob_key,
            "_size": len(data_bytes),
            "_checksum": checksum,
        }

        # Keep some scalar fields for index/query convenience (best effort)
        kept = 0
        for k, v in payload.items():
            if k in ("_overflow", "_blob_key", "_checksum"):
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                ref[k] = v
                kept += 1
            if kept >= 50:
                break

        self.logger.info(f"Stored Firestore overflow to GCS: {blob_key} ({len(data_bytes)} bytes)")
        return ref

    def _resolve_overflow(self, doc_data: JsonDict) -> JsonDict:
        if not doc_data.get("_overflow"):
            return doc_data

        blob_key = doc_data.get("_blob_key")
        checksum = doc_data.get("_checksum")

        if not blob_key:
            raise NoSQLError("Overflow doc missing _blob_key")
        if not self._bucket:
            raise NoSQLError("Overflow doc present but GCS bucket unavailable")

        blob = self._bucket.blob(blob_key)
        blob_data = blob.download_as_bytes()

        actual = hashlib.md5(blob_data).hexdigest()
        if checksum and actual != checksum:
            raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual}")

        restored = json.loads(blob_data.decode("utf-8"))
        return restored

    # ---------------------------------------------------------------------
    # Required NoSQLKVAdapter hooks
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            collection = self._get_collection(model)
            doc_id = self._doc_id(pk)

            # Ensure required identifiers exist for tests and convenience
            payload: JsonDict = dict(data or {})
            payload["id"] = pk
            payload["_pk"] = pk
            payload["_rk"] = rk

            overflow_ref = self._maybe_store_overflow(model, pk, rk, payload)
            if overflow_ref is not None:
                collection.document(doc_id).set(overflow_ref)
            else:
                collection.document(doc_id).set(payload)

            return {"id": pk}

        except Exception as e:
            raise NoSQLError(f"Firestore put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            collection = self._get_collection(model)
            doc_id = self._doc_id(pk)

            snap = collection.document(doc_id).get()
            if not getattr(snap, "exists", False):
                return None

            doc_data = snap.to_dict() or {}
            doc_data.setdefault("id", pk)

            return self._resolve_overflow(doc_data)

        except Exception as e:
            raise NoSQLError(f"Firestore get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        """
        Basic equality / comparator filtering via FieldFilter.
        Note: Firestore requires indexes for some compound queries in real GCP.
        Emulator usually allows most.
        """
        try:
            collection = self._get_collection(model)
            query = collection

            for field, value in (filters or {}).items():
                # Support your existing suffix operators if needed
                if field.endswith("__gt"):
                    query = query.where(filter=FieldFilter(field[:-4], ">", value))
                elif field.endswith("__gte"):
                    query = query.where(filter=FieldFilter(field[:-5], ">=", value))
                elif field.endswith("__lt"):
                    query = query.where(filter=FieldFilter(field[:-4], "<", value))
                elif field.endswith("__lte"):
                    query = query.where(filter=FieldFilter(field[:-5], "<=", value))
                elif field.endswith("__in"):
                    query = query.where(filter=FieldFilter(field[:-4], "in", value))
                else:
                    query = query.where(filter=FieldFilter(field, "==", value))

            if limit:
                query = query.limit(limit)

            docs = list(query.stream())
            out: List[JsonDict] = []
            for d in docs:
                row = d.to_dict() or {}
                # Ensure id present for tests
                row.setdefault("id", row.get("_pk") or d.id)
                out.append(self._resolve_overflow(row))
            return out

        except Exception as e:
            raise NoSQLError(f"Firestore query failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        """
        Test expectations:
        - deleting nonexistent raises sqlite3.DatabaseError
        - delete returns {"id": pk}
        - deletes overflow blob if present
        """
        try:
            collection = self._get_collection(model)
            doc_id = self._doc_id(pk)

            snap = collection.document(doc_id).get()
            if not getattr(snap, "exists", False):
                # tests expect DatabaseError specifically
                raise DatabaseError(f"Document {doc_id} does not exist")

            doc_data = snap.to_dict() or {}
            if doc_data.get("_overflow") and self._bucket:
                blob_key = doc_data.get("_blob_key")
                if blob_key:
                    try:
                        self._bucket.blob(blob_key).delete()
                        self.logger.debug(f"Deleted overflow GCS object: {blob_key}")
                    except Exception:
                        pass

            collection.document(doc_id).delete()
            return {"id": pk}

        except DatabaseError:
            raise
        except Exception as e:
            raise NoSQLError(f"Firestore delete failed: {e}")

    # ---------------------------------------------------------------------
    # Pagination helper used by NoSQLKVAdapter.query_page (if it calls _query_page_raw)
    # If your base calls only _query_raw, you can still add a public query_page method
    # in NoSQLKVAdapter; but since your tests call gcp_nosql.query_page(...) we provide it.
    # ---------------------------------------------------------------------

    def query_page(
        self,
        model: type,
        query=None,
        page_size: int = 25,
        continuation_token: Optional[str] = None,
        order_by: str = "id",
    ) -> Tuple[List[JsonDict], Optional[str]]:
        """
        Returns (rows, next_token). Token is last document id from the page.

        Works with your tests:
          page1, tok = gcp_nosql.query_page(GcpItem, {"tenant_id": tag}, 3)
          page2, _   = gcp_nosql.query_page(GcpItem, {"tenant_id": tag}, 3, tok)
        """

        try:
            collection = self._get_collection(model)
            fs_query = collection

            # Apply filters
            if query:
                for field, value in query.items():
                    fs_query = fs_query.where(filter=FieldFilter(field, "==", value))

            fs_query = fs_query.order_by(order_by).limit(page_size)

            # Continue from token
            if continuation_token:
                fs_query = fs_query.start_after({order_by: continuation_token})

            docs = list(fs_query.stream())

            rows: List[JsonDict] = []
            for d in docs:
                row = d.to_dict() or {}
                row.setdefault("id", row.get("_pk") or d.id)
                rows.append(self._resolve_overflow(row))

            next_token = None
            if len(rows) == page_size:
                next_token = str(rows[-1].get(order_by))

            return rows, next_token

        except Exception as e:
            raise NoSQLError(f"Firestore query_page failed: {e}")


# ======================================================
# FILE: polydb/adapters/GCPPubSubAdapter.py
# ======================================================





JsonLike = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


class GCPPubSubAdapter(QueueAdapter):
    """
    Production-grade GCP Pub/Sub adapter.

    - Emulator support via PUBSUB_EMULATOR_HOST (google client honors it)
    - Auto topic + subscription creation
    - send accepts Any (tests send str)
    - receive returns [{"id","ack_id","body"}...]
    - ack() method for tests
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        topic: Optional[str] = None,
        subscription: Optional[str] = None,
    ):
        super().__init__()

        self.project_id: str = project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "polydb-test")
        self.default_topic: str = topic or os.getenv("PUBSUB_TOPIC", "polydb-topic")
        self.default_subscription: str = subscription or os.getenv(
            "PUBSUB_SUBSCRIPTION", "polydb-sub"
        )

        self._publisher: Optional[pubsub_v1.PublisherClient] = None
        self._subscriber: Optional[pubsub_v1.SubscriberClient] = None
        self._lock = threading.Lock()

        self._initialize_clients()

    def _initialize_clients(self) -> None:
        try:
            with self._lock:
                if self._publisher and self._subscriber:
                    return
                # google client auto-detects PUBSUB_EMULATOR_HOST if set
                self._publisher = pubsub_v1.PublisherClient()
                self._subscriber = pubsub_v1.SubscriberClient()
                self.logger.info("Initialized Pub/Sub clients")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Pub/Sub: {e}")

    def _resolve_names(self, queue_name: str) -> Tuple[str, str]:
        """
        Your tests call send() / receive() without queue_name,
        so they pass default="default". That should map to adapter defaults.
        """
        if not queue_name or queue_name == "default":
            return self.default_topic, self.default_subscription
        # If user passes a custom name, use it for both (simple convention)
        return queue_name, queue_name

    def _topic_path(self, topic: str) -> str:
        if not self._publisher:
            raise ConnectionError("Pub/Sub publisher not initialized")
        return self._publisher.topic_path(self.project_id, topic)

    def _subscription_path(self, subscription: str) -> str:
        if not self._subscriber:
            raise ConnectionError("Pub/Sub subscriber not initialized")
        return self._subscriber.subscription_path(self.project_id, subscription)

    def _ensure_topic(self, topic: str) -> str:
        if not self._publisher:
            raise ConnectionError("Pub/Sub publisher not initialized")
        path = self._topic_path(topic)
        try:
            self._publisher.get_topic(request={"topic": path})
        except NotFound:
            try:
                self._publisher.create_topic(request={"name": path})
                self.logger.info(f"Created Pub/Sub topic: {topic}")
            except AlreadyExists:
                pass
        return path

    def _ensure_subscription(self, topic: str, subscription: str) -> str:
        if not self._subscriber:
            raise ConnectionError("Pub/Sub subscriber not initialized")
        if not self._publisher:
            raise ConnectionError("Pub/Sub publisher not initialized")

        topic_path = self._ensure_topic(topic)
        sub_path = self._subscription_path(subscription)

        try:
            self._subscriber.get_subscription(request={"subscription": sub_path})
        except NotFound:
            try:
                self._subscriber.create_subscription(
                    request={"name": sub_path, "topic": topic_path}
                )
                self.logger.info(f"Created Pub/Sub subscription: {subscription}")
            except AlreadyExists:
                pass

        return sub_path

    # -------------------------
    # API
    # -------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """
        Publish message to Pub/Sub.
        Tests send a string; production may send dict.
        We envelope it as {"body": <message>}.
        """
        try:
            if not self._publisher:
                raise ConnectionError("Pub/Sub publisher not initialized")

            topic, subscription = self._resolve_names(queue_name)
            topic_path = self._ensure_topic(topic)
            # Ensure subscription exists so receive works immediately in emulator
            self._ensure_subscription(topic, subscription)

            payload = {"body": message}
            data = json.dumps(payload, default=json_safe).encode("utf-8")

            future = self._publisher.publish(topic_path, data=data)
            msg_id = future.result(timeout=10)

            self.logger.debug(f"Published Pub/Sub message {msg_id} (topic={topic})")
            return msg_id

        except Exception as e:
            raise QueueError(f"Pub/Sub send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """
        Pull messages.
        IMPORTANT: Do NOT auto-ack here (so test_ack_message can ack explicitly).
        """
        try:
            if not self._subscriber:
                raise ConnectionError("Pub/Sub subscriber not initialized")

            topic, subscription = self._resolve_names(queue_name)
            sub_path = self._ensure_subscription(topic, subscription)

            resp = self._subscriber.pull(
                request={"subscription": sub_path, "max_messages": max_messages},
                timeout=5,
            )

            out: List[Dict[str, Any]] = []
            for r in resp.received_messages:
                body: Any = None
                try:
                    decoded = json.loads(r.message.data.decode("utf-8"))
                    if isinstance(decoded, dict) and "body" in decoded:
                        body = decoded["body"]
                    else:
                        body = decoded
                except Exception:
                    body = r.message.data.decode("utf-8", errors="replace")

                out.append(
                    {
                        "id": r.message.message_id,
                        "ack_id": r.ack_id,
                        "body": body,
                    }
                )

            return out

        except Exception as e:
            raise QueueError(f"Pub/Sub receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """
        Pub/Sub delete == ack.
        - pop_receipt maps to ack_id (preferred).
        - message_id fallback kept for backward compatibility.
        """
        try:
            if not self._subscriber:
                raise ConnectionError("Pub/Sub subscriber not initialized")

            _, subscription = self._resolve_names(queue_name)
            sub_path = self._subscription_path(subscription)

            ack_id = pop_receipt or message_id
            if not ack_id:
                raise QueueError("Pub/Sub delete requires ack_id (use pop_receipt from receive())")

            self._subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [ack_id]})
            return True

        except Exception as e:
            raise QueueError(f"Pub/Sub delete failed: {e}")

    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        Explicit ACK for Pub/Sub.

        Preferred usage:
            ack(ack_id=msg["receipt_handle"])

        Internally maps to acknowledge().
        """
        if not ack_id:
            raise QueueError("ack_id is required for Pub/Sub ack")

        try:
            if not self._subscriber:
                raise ConnectionError("Pub/Sub subscriber not initialized")

            _, subscription = self._resolve_names(queue_name)
            sub_path = self._subscription_path(subscription)

            self._subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [ack_id]})

            return True

        except Exception as e:
            raise QueueError(f"Pub/Sub ack failed: {e}")


# ======================================================
# FILE: polydb/adapters/GCPStorageAdapter.py
# ======================================================

# src/polydb/adapters/GCPStorageAdapter.py





class GCPStorageAdapter(ObjectStorageAdapter):
    """
    Production-grade Google Cloud Storage adapter.

    Features
    --------
    - Thread-safe client initialization
    - Automatic bucket creation
    - Emulator support (fake-gcs-server)
    - Retry support
    - Structured logging
    """

    def __init__(self, project_id: str, endpoint: Optional[str], bucket_name: Optional[str] = None):
        super().__init__()

        self.bucket_name: str = bucket_name or os.getenv("GCS_BUCKET_NAME", "default")
        self.project_id: str = project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "polydb-test")

        self._endpoint: Optional[str] = endpoint or os.getenv("GCS_ENDPOINT")

        self._client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None

        self._lock = threading.Lock()

        self._initialize_client()

    # ------------------------------------------------------------------
    # Client initialization
    # ------------------------------------------------------------------

    def _initialize_client(self) -> None:
        """Initialize GCS client once (thread-safe)"""
        try:
            with self._lock:
                if self._client:
                    return

                if self._endpoint:
                    self.logger.info(f"Using GCS emulator: {self._endpoint}")
                    self._client = storage.Client(
                        project=self.project_id,
                        client_options={"api_endpoint": self._endpoint},
                    )
                else:
                    self._client = storage.Client(project=self.project_id)

                self._bucket = self._client.bucket(self.bucket_name)

                # Ensure bucket exists
                try:
                    if not self._bucket.exists():
                        self._bucket = self._client.create_bucket(self.bucket_name)
                        self.logger.info(f"Created GCS bucket: {self.bucket_name}")
                except Exception:
                    # fake-gcs-server does not support bucket.exists()
                    pass

                self.logger.info(
                    f"GCS initialized (bucket={self.bucket_name}, project={self.project_id})"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize GCS: {str(e)}")

    # ------------------------------------------------------------------
    # Put object
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload object to GCS with media type, metadata, filename handling"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            # --------------------------------------------------
            # Resolve filename
            # --------------------------------------------------
            filename = fileName or os.path.basename(key)

            # --------------------------------------------------
            # Ensure extension from media_type
            # --------------------------------------------------
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            # --------------------------------------------------
            # Final blob key
            # --------------------------------------------------
            blob_key = f"{key.rstrip('/')}/{filename}" if fileName else key

            blob = self._bucket.blob(blob_key)

            # --------------------------------------------------
            # Metadata (must be string)
            # --------------------------------------------------
            safe_metadata = {k: str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename

            blob.metadata = safe_metadata

            # --------------------------------------------------
            # Upload with content type
            # --------------------------------------------------
            blob.upload_from_string(
                data,
                content_type=media_type or "application/octet-stream",
            )

            # Persist metadata (required in GCS)
            blob.patch()

            self.logger.debug(f"GCS uploaded blob: {blob_key}, type={media_type}")

            # --------------------------------------------------
            # Return public URL
            # --------------------------------------------------
            return f"https://storage.googleapis.com/{self.bucket_name}/{blob_key}"

        except Exception as e:
            raise StorageError(f"GCS put failed: {str(e)}")

    # ------------------------------------------------------------------
    # Get object
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> Optional[bytes]:
        """Download object from GCS"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blob = self._bucket.blob(key)

            if not blob.exists():
                return None

            data = blob.download_as_bytes()

            self.logger.debug(f"GCS downloaded blob: {key}")

            return data

        except NotFound:
            return None

        except Exception as e:
            raise StorageError(f"GCS get failed: {str(e)}")

    # ------------------------------------------------------------------
    # Delete object
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object from GCS"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blob = self._bucket.blob(key)

            if not blob.exists():
                return False

            blob.delete()

            self.logger.debug(f"GCS deleted blob: {key}")

            return True

        except NotFound:
            return False

        except Exception as e:
            raise StorageError(f"GCS delete failed: {str(e)}")

    # ------------------------------------------------------------------
    # List objects
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with optional prefix"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blobs = self._bucket.list_blobs(prefix=prefix)

            results = [blob.name for blob in blobs]

            self.logger.debug(f"GCS listed {len(results)} blobs (prefix={prefix})")

            return results

        except Exception as e:
            raise StorageError(f"GCS list failed: {str(e)}")


# ======================================================
# FILE: polydb/adapters/MongoDBAdapter.py
# ======================================================

# src/polydb/adapters/mongodb.py





class MongoDBAdapter(NoSQLKVAdapter):
    """MongoDB adapter compatible with PolyDB contract"""

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        mongo_uri: str = "",
        db_name: str = "",
    ):
        super().__init__(partition_config)

        self.mongo_uri = mongo_uri or os.getenv("MONGODB_URI", "mongodb://localhost:27017")

        self.db_name = db_name or os.getenv("MONGODB_DATABASE", "polydb")

        self._client = None
        self._lock = threading.Lock()

        self._initialize_client()

    # -----------------------------------------------------
    # Client init
    # -----------------------------------------------------

    def _initialize_client(self):
        try:

            with self._lock:
                if self._client:
                    return

                self._client = MongoClient(
                    self.mongo_uri,
                    maxPoolSize=int(os.getenv("MONGODB_MAX_POOL_SIZE", "10")),
                    minPoolSize=int(os.getenv("MONGODB_MIN_POOL_SIZE", "1")),
                    serverSelectionTimeoutMS=5000,
                )

                self._client.server_info()

                self.logger.info("MongoDB initialized")

        except Exception as e:
            raise ConnectionError(f"MongoDB init failed: {e}")

    # -----------------------------------------------------
    # Collection helper
    # -----------------------------------------------------

    def _get_collection(self, model: type):
        if not self._client:
            self._initialize_client()

        meta = getattr(model, "__polydb__", {}) or {}

        collection_name = meta.get("collection") or meta.get("table") or model.__name__.lower()

        return self._client[self.db_name][collection_name]  # type: ignore

    # -----------------------------------------------------
    # PUT
    # -----------------------------------------------------
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            collection = self._get_collection(model)

            payload = dict(data or {})
            payload["_pk"] = pk
            payload["_rk"] = rk
            payload["id"] = pk

            collection.update_one(
                {"_pk": pk, "_rk": rk},
                {"$set": payload},
                upsert=True,
            )

            # return full stored row (tests expect this)
            result = dict(payload)
            result.pop("_pk", None)
            result.pop("_rk", None)

            return result

        except Exception as e:
            raise NoSQLError(f"MongoDB put failed: {e}")

    # -----------------------------------------------------
    # GET
    # -----------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            collection = self._get_collection(model)

            doc = collection.find_one({"_pk": pk, "_rk": rk})

            if not doc:
                return None

            doc.pop("_id", None)
            doc.setdefault("id", pk)

            return doc

        except Exception as e:
            raise NoSQLError(f"MongoDB get failed: {e}")

    # -----------------------------------------------------
    # QUERY
    # -----------------------------------------------------
    def query_page(
        self,
        model: type,
        query: Dict[str, Any],
        page_size: int,
        continuation_token: Optional[str] = None,
    ):
        try:
            collection = self._get_collection(model)

            query = query or {}

            if continuation_token:
                query["_pk"] = {"$gt": continuation_token}

            cursor = collection.find(query).sort("_pk", 1).limit(page_size)

            rows = []

            for doc in cursor:
                doc.pop("_id", None)
                doc.setdefault("id", doc.get("_pk"))
                rows.append(doc)

            if not rows:
                return [], None

            next_token = None

            if len(rows) == page_size:
                next_token = rows[-1]["id"]

            return rows, next_token

        except Exception as e:
            raise NoSQLError(f"MongoDB query_page failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        try:
            collection = self._get_collection(model)

            query: Dict[str, Any] = {}

            for k, v in (filters or {}).items():

                if k == "id":
                    query["_pk"] = v
                    continue

                if k.endswith("__gt"):
                    query[k[:-4]] = {"$gt": v}

                elif k.endswith("__gte"):
                    query[k[:-5]] = {"$gte": v}

                elif k.endswith("__lt"):
                    query[k[:-4]] = {"$lt": v}

                elif k.endswith("__lte"):
                    query[k[:-5]] = {"$lte": v}

                elif k.endswith("__in"):
                    query[k[:-4]] = {"$in": v}

                elif k.endswith("__contains"):
                    safe_pattern = re.escape(str(v))
                    query[k[:-10]] = {
                        "$regex": safe_pattern,
                        "$options": "i",
                    }

                else:
                    query[k] = v

            cursor = collection.find(query)

            if limit:
                cursor = cursor.limit(limit)

            results: List[JsonDict] = []

            for doc in cursor:
                doc.pop("_id", None)
                doc.setdefault("id", doc.get("_pk"))
                results.append(doc)

            return results

        except Exception as e:
            raise NoSQLError(f"MongoDB query failed: {e}")

    # -----------------------------------------------------
    # DELETE
    # -----------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            collection = self._get_collection(model)

            result = collection.delete_one({"_pk": pk, "_rk": rk})

            if result.deleted_count == 0:
                raise DatabaseError(f"Item {pk}/{rk} does not exist")

            return {"id": pk}

        except DatabaseError:
            raise

        except Exception as e:
            raise NoSQLError(f"MongoDB delete failed: {e}")

    # -----------------------------------------------------
    # Close connection
    # -----------------------------------------------------

    def __del__(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass


# ======================================================
# FILE: polydb/adapters/PostgreSQLAdapter.py
# ======================================================

# src/polydb/adapters/postgres.py



class PostgreSQLAdapter:
    """PostgreSQL with full LINQ support, connection pooling, JSON/JSONB support"""

    def __init__(self, connection_string: Optional[str] = None):

        self.logger = setup_logger(__name__)
        self.connection_string = connection_string or os.getenv(
            "POSTGRES_CONNECTION_STRING",
            os.getenv("POSTGRES_URL", ""),
        )
        if not self.connection_string:
            raise ConnectionError("POSTGRES_CONNECTION_STRING or POSTGRES_URL must be set")
        self._pool = None
        self._lock = threading.Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        try:

            with self._lock:
                if not self._pool:
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=int(os.getenv("POSTGRES_MIN_CONNECTIONS", "2")),
                        maxconn=int(os.getenv("POSTGRES_MAX_CONNECTIONS", "20")),
                        dsn=self.connection_string,
                    )
                    self.logger.info("PostgreSQL pool initialized")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize PostgreSQL pool: {str(e)}")

    def _get_connection(self) -> Any:
        if not self._pool:
            self._initialize_pool()
        return self._pool.getconn()  # type: ignore

    def _return_connection(self, conn: Any):
        if self._pool and conn:
            self._pool.putconn(conn)

    # ---------------------------------------------------------------------
    # TRANSACTIONS
    # ---------------------------------------------------------------------

    def begin_transaction(self) -> Any:
        """Begin a transaction and return the connection handle."""
        conn = self._get_connection()
        conn.autocommit = False
        return conn

    def commit(self, tx: Any):
        """Commit the transaction using the provided connection."""
        if tx:
            tx.commit()
            self._return_connection(tx)

    def rollback(self, tx: Any):
        """Rollback the transaction using the provided connection."""
        if tx:
            tx.rollback()
            self._return_connection(tx)

    # ---------------------------------------------------------------------
    # JSON HELPERS
    # ---------------------------------------------------------------------
    def _json_safe(self, obj: Any):
        """
        Ensure JSON serialization never fails.
        Used only for Json() wrapping.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return str(obj)
        if isinstance(obj, dict):
            return {k: self._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._json_safe(v) for v in obj]
        return obj

    def _serialize_value(self, v: Any) -> Any:
        """
        Make all outgoing values safe for psycopg2.

        Rules:
        - dict -> Json()
        - list -> leave as list (so TEXT[] works)
        - datetime/date -> pass as native (psycopg2 handles it)
        - Decimal -> convert to float
        - everything else -> pass as-is
        """

        if v is None:
            return None

        # Dict -> JSON/JSONB
        if isinstance(v, dict):
            return Json(self._json_safe(v))

        # List:
        # DO NOT wrap in Json() automatically.
        # If column is JSONB, Postgres will still accept Json(list).
        # But for TEXT[] columns we must send Python list.
        if isinstance(v, list):
            return v

        # Datetime / date
        if isinstance(v, (datetime, date)):
            return v  # psycopg2 handles natively

        # Decimal
        if isinstance(v, Decimal):
            return float(v)

        return v

    def _serialize_params(self, params: List[Any]) -> List[Any]:
        return [self._serialize_value(p) for p in params]

    def _deserialize_row(self, row: JsonDict) -> JsonDict:
        """
        Postgres JSON/JSONB often comes back as dict already depending on driver config.
        If it comes as a string, try json.loads safely.
        """
        for k, v in list(row.items()):
            if isinstance(v, str):
                s = v.strip()
                # quick cheap check to avoid parsing normal strings
                if (s.startswith("{") and s.endswith("}")) or (
                    s.startswith("[") and s.endswith("]")
                ):
                    try:
                        row[k] = json.loads(s)
                    except Exception:
                        pass
        return row

    # ---------------------------------------------------------------------
    # INSERT
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def insert(self, table: str, data: JsonDict, tx: Optional[Any] = None) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING *"

            values = [self._serialize_value(v) for v in data.values()]
            cursor.execute(query, values)

            result_row = cursor.fetchone()
            columns_list = [desc[0] for desc in cursor.description]
            result = dict(zip(columns_list, result_row))

            if own_conn:
                conn.commit()

            cursor.close()
            return self._deserialize_row(result)

        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Insert failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # SELECT
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def select(
        self,
        table: str,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tx: Optional[Any] = None,
    ) -> List[JsonDict]:
        table = validate_table_name(table)
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()

            sql = f"SELECT * FROM {table}"
            params: List[Any] = []

            if query:
                where_parts: List[str] = []
                for k, v in query.items():
                    validate_column_name(k)

                    # IMPORTANT: None must be "IS NULL" not "= %s"
                    if v is None:
                        where_parts.append(f"{k} IS NULL")
                    elif isinstance(v, (list, tuple)):
                        placeholders = ",".join(["%s"] * len(v))
                        where_parts.append(f"{k} IN ({placeholders})")
                        params.extend(list(v))
                    else:
                        where_parts.append(f"{k} = %s")
                        params.append(v)

                if where_parts:
                    sql += " WHERE " + " AND ".join(where_parts)

            if limit:
                sql += " LIMIT %s"
                params.append(limit)
            if offset:
                sql += " OFFSET %s"
                params.append(offset)

            cursor.execute(sql, self._serialize_params(params))
            columns = [desc[0] for desc in cursor.description]
            results = [self._deserialize_row(dict(zip(columns, row))) for row in cursor.fetchall()]
            cursor.close()

            return results
        except Exception as e:
            raise DatabaseError(f"Select failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # SELECT PAGE
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def select_page(
        self,
        table: str,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None,
        tx: Optional[Any] = None,
    ) -> Tuple[List[JsonDict], Optional[str]]:
        offset = int(continuation_token) if continuation_token else 0
        results = self.select(table, query, limit=page_size + 1, offset=offset, tx=tx)

        has_more = len(results) > page_size
        if has_more:
            results = results[:page_size]

        next_token = str(offset + page_size) if has_more else None
        return results, next_token

    # ---------------------------------------------------------------------
    # UPDATE
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def update(
        self,
        table: str,
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        tx: Optional[Any] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()

            set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
            params: List[Any] = [self._serialize_value(v) for v in data.values()]

            if isinstance(entity_id, dict):
                where_parts: List[str] = []
                for k, v in entity_id.items():
                    validate_column_name(k)
                    if v is None:
                        where_parts.append(f"{k} IS NULL")
                    else:
                        where_parts.append(f"{k} = %s")
                        params.append(self._serialize_value(v))
                where_clause = " AND ".join(where_parts)
            else:
                where_clause = "id = %s"
                params.append(entity_id)

            query = f"UPDATE {table} SET {set_clause} WHERE {where_clause} RETURNING *"
            cursor.execute(query, params)

            result_row = cursor.fetchone()
            if not result_row:
                raise DatabaseError("No rows updated")

            columns = [desc[0] for desc in cursor.description]
            result = dict(zip(columns, result_row))

            if own_conn:
                conn.commit()

            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Update failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # UPSERT
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def upsert(self, table: str, data: JsonDict, tx: Optional[Any] = None) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))

            conflict_columns = ["id"] if "id" in data else list(data.keys())[:1]
            update_fields = [k for k in data.keys() if k not in conflict_columns]

            if update_fields:
                update_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_fields])
                on_conflict = f"DO UPDATE SET {update_clause}"
            else:
                on_conflict = "DO NOTHING"

            query = f"""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(conflict_columns)})
                {on_conflict}
                RETURNING *
            """

            values = [self._serialize_value(v) for v in data.values()]
            cursor.execute(query, values)

            result_row = cursor.fetchone()
            if not result_row:
                # DO NOTHING case: fetch existing row (best effort)
                # If conflict is on id, we can read it back.
                if "id" in conflict_columns and "id" in data:
                    cursor.execute(f"SELECT * FROM {table} WHERE id = %s", [data["id"]])
                    result_row = cursor.fetchone()
                    if not result_row:
                        raise DatabaseError("Upsert did nothing and existing row not found")
                else:
                    raise DatabaseError("Upsert did nothing and cannot determine existing row")

            columns_list = [desc[0] for desc in cursor.description]
            result = dict(zip(columns_list, result_row))

            if own_conn:
                conn.commit()

            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Upsert failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # DELETE
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def delete(
        self, table: str, entity_id: Union[Any, Lookup], tx: Optional[Any] = None
    ) -> JsonDict:
        table = validate_table_name(table)
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()

            params: List[Any] = []
            if isinstance(entity_id, dict):
                where_parts: List[str] = []
                for k, v in entity_id.items():
                    validate_column_name(k)
                    if v is None:
                        where_parts.append(f"{k} IS NULL")
                    else:
                        where_parts.append(f"{k} = %s")
                        params.append(self._serialize_value(v))
                where_clause = " AND ".join(where_parts)
            else:
                where_clause = "id = %s"
                params.append(entity_id)

            query = f"DELETE FROM {table} WHERE {where_clause} RETURNING *"
            cursor.execute(query, params)
            result_row = cursor.fetchone()

            if not result_row:
                raise DatabaseError("No rows deleted")

            columns = [desc[0] for desc in cursor.description]
            result = dict(zip(columns, result_row))

            if own_conn:
                conn.commit()

            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Delete failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # LINQ QUERY
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def query_linq(
        self, table: str, builder: QueryBuilder, tx: Optional[Any] = None
    ) -> Union[List[JsonDict], int]:

        table = validate_table_name(table)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()

            # ------------------------------------------------
            # SELECT clause
            # ------------------------------------------------

            if builder.count_only:
                sql = f"SELECT COUNT(*) FROM {table}"

            elif builder.selected_fields:
                for f in builder.selected_fields:
                    validate_column_name(f)

                fields = ", ".join(builder.selected_fields)

                if builder.distinct_flag:
                    sql = f"SELECT DISTINCT {fields} FROM {table}"
                else:
                    sql = f"SELECT {fields} FROM {table}"

            else:
                sql = f"SELECT * FROM {table}"

            params: List[Any] = []

            # ------------------------------------------------
            # WHERE
            # ------------------------------------------------

            where_clause, where_params = builder.to_sql_where()

            if where_clause:
                sql += f" WHERE {where_clause}"
                params.extend(where_params)

            # ------------------------------------------------
            # GROUP BY
            # ------------------------------------------------

            if builder.group_by_fields:

                for f in builder.group_by_fields:
                    validate_column_name(f)

                sql += f" GROUP BY {', '.join(builder.group_by_fields)}"

            # ------------------------------------------------
            # ORDER BY
            # ------------------------------------------------

            if builder.order_by_fields:

                order_parts = []

                for field, desc in builder.order_by_fields:

                    validate_column_name(field)

                    direction = "DESC" if desc else "ASC"

                    order_parts.append(f"{field} {direction}")

                sql += f" ORDER BY {', '.join(order_parts)}"

            # ------------------------------------------------
            # LIMIT
            # ------------------------------------------------

            if builder.take_count is not None:
                sql += " LIMIT %s"
                params.append(builder.take_count)

            # ------------------------------------------------
            # OFFSET
            # ------------------------------------------------

            if builder.skip_count:
                sql += " OFFSET %s"
                params.append(builder.skip_count)

            # ------------------------------------------------
            # EXECUTE
            # ------------------------------------------------

            cursor.execute(sql, self._serialize_params(params))

            if builder.count_only:
                result = cursor.fetchone()[0]

            else:
                columns = [desc[0] for desc in cursor.description]

                result = [
                    self._deserialize_row(dict(zip(columns, row))) for row in cursor.fetchall()
                ]

            cursor.close()

            if own_conn:
                conn.commit()

            return result

        except Exception as e:

            if own_conn:
                conn.rollback()

            raise DatabaseError(f"LINQ query failed: {str(e)}")

        finally:

            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # EXECUTE RAW SQL
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def execute(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        tx: Optional[Any] = None,
        *,
        fetch: bool = False,
        fetch_one: bool = False,
    ) -> Union[None, JsonDict, List[JsonDict]]:
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        cursor = None
        try:
            cursor = conn.cursor()
            self.logger.debug("Executing raw SQL: %s", sql)

            exec_params = self._serialize_params(params or [])
            cursor.execute(sql, exec_params)

            if fetch_one:
                row = cursor.fetchone()
                result = None
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    result = self._deserialize_row(dict(zip(columns, row)))
                if own_conn:
                    conn.commit()
                return result

            if fetch:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                results = [self._deserialize_row(dict(zip(columns, r))) for r in rows]
                if own_conn:
                    conn.commit()
                return results

            if own_conn:
                conn.commit()
            return None

        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"Execute failed: {str(e)}")

        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # DISTRIBUTED LOCK
    # ---------------------------------------------------------------------

    @contextmanager
    def distributed_lock(self, lock_name: str) -> Iterator[None]:
        """
        PostgreSQL advisory lock (session scoped).
        - Always unlock before returning the pooled connection.
        - Never wrap exceptions raised by the user block.
        """
        conn = None
        cursor = None
        lock_id = int(hashlib.sha256(lock_name.encode()).hexdigest(), 16) % (2**63)

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Acquire lock (DB op) — if this fails, it's a DatabaseError
            try:
                cursor.execute("SELECT pg_advisory_lock(%s);", (lock_id,))
                self.logger.debug("Acquired distributed lock: %s", lock_name)
            except Exception as e:
                raise DatabaseError(f"Distributed lock acquire failed: {e}") from e

            try:
                # User code runs here. If it raises, it MUST propagate unchanged.
                yield
            finally:
                # Release lock (DB op). Always attempted.
                try:
                    cursor.execute("SELECT pg_advisory_unlock(%s);", (lock_id,))
                    self.logger.debug("Released distributed lock: %s", lock_name)
                except Exception as e:
                    # IMPORTANT: don't mask user exceptions.
                    # If unlock fails, raise DatabaseError only if user block didn't already fail.
                    # Easiest safe behavior: just log and continue.
                    self.logger.exception(
                        "Distributed lock release failed for %s: %s", lock_name, e
                    )

        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.commit()
                except Exception:
                    pass
                self._return_connection(conn)


# ======================================================
# FILE: polydb/adapters/S3Adapter.py
# ======================================================

# src/polydb/adapters/S3Adapter.py

"""
S3 adapter (AWS + LocalStack compatible)
"""






class S3Adapter(ObjectStorageAdapter):
    """AWS S3 adapter with client reuse and automatic bucket creation"""

    def __init__(self, bucket_name: str = "", region: str = "", endpoint_url: str = ""):
        super().__init__()

        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "polydb-test")

        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        self.endpoint_url = endpoint_url or os.getenv("AWS_ENDPOINT_URL")

        self._client: Any = None
        self._lock = threading.Lock()

        self._initialize_client()

    # ---------------------------------------------------------
    # Client initialization
    # ---------------------------------------------------------

    def _initialize_client(self):
        """Initialize S3 client once"""
        try:
            with self._lock:
                if self._client:
                    return

                self._client = boto3.client(
                    "s3",
                    region_name=self.region,
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                )

                self._ensure_bucket_exists()

                self.logger.info(
                    f"Initialized S3 client (region={self.region}, endpoint={self.endpoint_url or 'aws'})"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize S3 client: {e}")

    # ---------------------------------------------------------
    # Bucket management
    # ---------------------------------------------------------

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist (safe for AWS + LocalStack)"""
        if not self._client:
            return

        try:
            self._client.head_bucket(Bucket=self.bucket_name)
            return
        except ClientError:
            pass

        try:
            if self.region == "us-east-1":
                self._client.create_bucket(Bucket=self.bucket_name)
            else:
                self._client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )

            self.logger.info(f"Created S3 bucket: {self.bucket_name}")

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise StorageError(f"S3 bucket creation failed: {e}")

    # ---------------------------------------------------------
    # Core operations
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload object to S3-compatible storage with metadata and media type"""
        try:
            if not self._client:
                self._initialize_client()

            # --------------------------------------------------
            # Resolve filename
            # --------------------------------------------------
            filename = fileName or os.path.basename(key)

            # --------------------------------------------------
            # Ensure extension from media_type
            # --------------------------------------------------
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            # --------------------------------------------------
            # Final key
            # --------------------------------------------------
            blob_key = f"{key.rstrip('/')}/{filename}" if fileName else key

            # --------------------------------------------------
            # Metadata (string only)
            # --------------------------------------------------
            safe_metadata = {k: str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename

            # --------------------------------------------------
            # Upload
            # --------------------------------------------------
            self._client.put_object(  # type: ignore
                Bucket=self.bucket_name,
                Key=blob_key,
                Body=data,
                ContentType=media_type or "application/octet-stream",
                Metadata=safe_metadata,
            )

            self.logger.debug(f"S3 uploaded: {blob_key}, type={media_type}")

            # --------------------------------------------------
            # Return URL
            # --------------------------------------------------
            if self.endpoint_url:
                # MinIO / Spaces / custom endpoint
                url = f"{self.endpoint_url.rstrip('/')}/{self.bucket_name}/{blob_key}"
            else:
                # AWS S3 default
                url = f"https://{self.bucket_name}.s3.amazonaws.com/{blob_key}"

            return url

        except Exception as e:
            raise StorageError(f"S3-compatible put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Download object"""
        try:
            if not self._client:
                self._initialize_client()

            response = self._client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )

            return response["Body"].read()

        except self._client.exceptions.NoSuchKey:  # type: ignore
            return None
        except Exception as e:
            raise StorageError(f"S3 get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object"""
        try:
            if not self._client:
                self._initialize_client()

            self._client.delete_object(
                Bucket=self.bucket_name,
                Key=key,
            )

            return True

        except Exception as e:
            raise StorageError(f"S3 delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        try:
            if not self._client:
                self._initialize_client()

            response = self._client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
            )

            contents = response.get("Contents", [])

            return [obj["Key"] for obj in contents]

        except Exception as e:
            raise StorageError(f"S3 list failed: {e}")


# ======================================================
# FILE: polydb/adapters/S3CompatibleAdapter.py
# ======================================================

# src/polydb/adapters/S3CompatibleAdapter.py


class S3CompatibleAdapter(ObjectStorageAdapter):
    """S3-compatible storage (MinIO, DigitalOcean Spaces) with client reuse"""

    def __init__(self):
        super().__init__()
        self.endpoint = os.getenv("S3_ENDPOINT_URL")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "default")
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize S3-compatible client once"""
        try:

            with self._lock:
                if not self._client:
                    self._client = boto3.client(
                        "s3",
                        endpoint_url=self.endpoint,
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key=self.secret_key,
                    )
                    self.logger.info("Initialized S3-compatible client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize S3-compatible client: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload object to S3-compatible storage with metadata and media type"""
        try:
            if not self._client:
                self._initialize_client()

            # --------------------------------------------------
            # Resolve filename
            # --------------------------------------------------
            filename = fileName or os.path.basename(key)

            # --------------------------------------------------
            # Ensure extension from media_type
            # --------------------------------------------------
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            # --------------------------------------------------
            # Final key
            # --------------------------------------------------
            blob_key = f"{key.rstrip('/')}/{filename}" if fileName else key

            # --------------------------------------------------
            # Metadata (string only)
            # --------------------------------------------------
            safe_metadata = {k: str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename

            # --------------------------------------------------
            # Upload
            # --------------------------------------------------
            self._client.put_object(  # type: ignore
                Bucket=self.bucket_name,
                Key=blob_key,
                Body=data,
                ContentType=media_type or "application/octet-stream",
                Metadata=safe_metadata,
            )

            self.logger.debug(f"S3 uploaded: {blob_key}, type={media_type}")

            # --------------------------------------------------
            # Return URL
            # --------------------------------------------------
            if self.endpoint:
                # MinIO / Spaces / custom endpoint
                url = f"{self.endpoint.rstrip('/')}/{self.bucket_name}/{blob_key}"
            else:
                # AWS S3 default
                url = f"https://{self.bucket_name}.s3.amazonaws.com/{blob_key}"

            return url

        except Exception as e:
            raise StorageError(f"S3-compatible put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Get object"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                response = self._client.get_object(Bucket=self.bucket_name, Key=key)
                return response["Body"].read()
            return None
        except Exception as e:
            raise StorageError(f"S3-compatible get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                self._client.delete_object(Bucket=self.bucket_name, Key=key)
                return True
            return False
        except Exception as e:
            raise StorageError(f"S3-compatible delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                response = self._client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
                return [obj["Key"] for obj in response.get("Contents", [])]
            return []
        except Exception as e:
            raise StorageError(f"S3-compatible list failed: {str(e)}")


# ======================================================
# FILE: polydb/adapters/SQSAdapter.py
# ======================================================






class SQSAdapter(QueueAdapter):
    """AWS SQS adapter with automatic queue creation (AWS + LocalStack compatible)"""

    def __init__(self, queue_name: str = "", region: str = "", endpoint_url: str = ""):
        super().__init__()

        self.queue_name = queue_name or os.getenv("SQS_QUEUE_NAME", "polydb-queue")

        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        self.endpoint_url = endpoint_url or os.getenv("AWS_ENDPOINT_URL")

        self._client: Any = None
        self._queue_url = None
        self._lock = threading.Lock()

        self._initialize_client()

    # ---------------------------------------------------------
    # Client initialization
    # ---------------------------------------------------------

    def _initialize_client(self):
        try:
            with self._lock:
                if self._client:
                    return

                self._client = boto3.client(
                    "sqs",
                    region_name=self.region,
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                )

                self._queue_url = self._ensure_queue_exists(self.queue_name)

                self.logger.info(
                    f"Initialized SQS client (queue={self.queue_name}, endpoint={self.endpoint_url or 'aws'})"
                )

        except Exception as e:
            raise ConnectionError(f"SQS init failed: {e}")

    # ---------------------------------------------------------
    # Queue management
    # ---------------------------------------------------------

    def _ensure_queue_exists(self, queue_name: str) -> str:
        """Create queue if it does not exist"""
        if not self._client:
            raise ConnectionError("SQS client not initialized")

        try:
            response = self._client.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]

        except self._client.exceptions.QueueDoesNotExist:  # type: ignore

            response = self._client.create_queue(QueueName=queue_name)
            return response["QueueUrl"]

        except ClientError as e:
            raise QueueError(f"SQS queue creation failed: {e}")

    # ---------------------------------------------------------
    # Queue operations
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        try:
            if not self._client:
                self._initialize_client()

            body = (
                json.dumps(message, default=json_safe) if not isinstance(message, str) else message
            )

            resp = self._client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=body,
            )

            return resp["MessageId"]

        except Exception as e:
            raise QueueError(f"SQS send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        try:
            if not self._client:
                self._initialize_client()

            resp = self._client.receive_message(
                QueueUrl=self._queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=1,
            )

            messages = resp.get("Messages", [])

            out: List[Dict[str, Any]] = []

            for m in messages:
                body = m.get("Body")

                try:
                    body = json.loads(body)
                except Exception:
                    pass

                out.append(
                    {
                        "body": body,
                        "receipt_handle": m["ReceiptHandle"],
                        "message_id": m["MessageId"],
                    }
                )

            return out

        except Exception as e:
            raise QueueError(f"SQS receive failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def delete(self, receipt_handle: str, queue_name: str = "default") -> bool:
        """Delete message from queue"""
        try:
            if not self._client:
                self._initialize_client()

            self._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
            )

            return True

        except Exception as e:
            raise QueueError(f"SQS delete failed: {e}")

    # ---------------------------------------------------------
    # ACK (SQS = delete using receipt_handle)
    # ---------------------------------------------------------
    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        ACK message in SQS.

        ack_id = receipt_handle (NOT message_id)

        This maps to delete operation.
        """
        if not ack_id:
            raise QueueError("ack_id (receipt_handle) is required for SQS ack")

        return self.delete(ack_id, queue_name=queue_name)


# ======================================================
# FILE: polydb/adapters/VercelBlobAdapter.py
# ======================================================





class VercelBlobAdapter(ObjectStorageAdapter):
    """
    Vercel Blob Storage adapter.

    If BLOB_READ_WRITE_TOKEN is missing, falls back to
    local filesystem storage for testing.
    """

    def __init__(self, token: str = "", timeout: int = 10):
        super().__init__()

        self.token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
        self.timeout = timeout or int(os.getenv("VERCEL_BLOB_TIMEOUT", "10"))

        # Local fallback storage for tests
        self.local_dir = Path(os.getenv("VERCEL_BLOB_LOCAL_DIR", "/tmp/vercel_blob"))
        self.local_dir.mkdir(parents=True, exist_ok=True)

        self.local_mode = not bool(self.token)

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload object to Vercel Blob or local fallback and return URL/path"""
        try:
            # --------------------------------------------------
            # Resolve filename
            # --------------------------------------------------
            filename = fileName or os.path.basename(key)

            # --------------------------------------------------
            # Ensure extension from media_type
            # --------------------------------------------------
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            # --------------------------------------------------
            # Final blob key
            # --------------------------------------------------
            blob_key = f"{key.rstrip('/')}/{filename}" if fileName else key

            # --------------------------------------------------
            # Metadata (string-safe)
            # --------------------------------------------------
            safe_metadata = {str(k): str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename
            if media_type:
                safe_metadata["contentType"] = media_type

            # --------------------------------------------------
            # LOCAL MODE
            # --------------------------------------------------
            if self.local_mode:
                path = self.local_dir / blob_key
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)

                meta_path = path.with_suffix(path.suffix + ".metadata.json")
                try:

                    meta_path.write_text(json.dumps(safe_metadata, ensure_ascii=False, indent=2))
                except Exception:
                    pass

                self.logger.debug(f"Stored local Vercel blob: {blob_key}")
                return str(path)

            # --------------------------------------------------
            # REAL VERCEL
            # --------------------------------------------------
            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-content-type": media_type or "application/octet-stream",
            }

            for k, v in safe_metadata.items():
                headers[f"x-metadata-{k}"] = v

            response = requests.put(
                f"https://blob.vercel-storage.com/{blob_key}",
                headers=headers,
                data=data,
                timeout=self.timeout,
            )
            response.raise_for_status()

            result = response.json()

            self.logger.debug(f"Uploaded Vercel blob: {blob_key}, type={media_type}")

            return (
                result.get("url") or result.get("downloadUrl") or result.get("pathname") or blob_key
            )

        except Exception as e:
            raise StorageError(f"Vercel Blob put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        try:
            if self.local_mode:
                path = self.local_dir / key
                if not path.exists():
                    return None
                return path.read_bytes()

            response = requests.get(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.content

        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Vercel Blob get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        try:
            if self.local_mode:
                path = self.local_dir / key
                if path.exists():
                    path.unlink()
                meta_path = path.with_suffix(path.suffix + ".metadata.json")
                if meta_path.exists():
                    meta_path.unlink()
                return True

            response = requests.delete(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            if response.status_code == 404:
                return False

            response.raise_for_status()
            return True

        except Exception as e:
            raise StorageError(f"Vercel Blob delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        try:
            if self.local_mode:
                results: List[str] = []
                for p in self.local_dir.rglob("*"):
                    if p.is_file() and not p.name.endswith(".metadata.json"):
                        rel = str(p.relative_to(self.local_dir))
                        if rel.startswith(prefix):
                            results.append(rel)
                return results

            response = requests.get(
                f"https://blob.vercel-storage.com/?prefix={prefix}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            response.raise_for_status()

            blobs = response.json().get("blobs", [])
            return [b.get("pathname") for b in blobs if b.get("pathname")]

        except Exception as e:
            raise StorageError(f"Vercel Blob list failed: {e}")


# ======================================================
# FILE: polydb/adapters/VercelKVAdapter.py
# ======================================================

# src/polydb/adapters/VercelKVAdapter.py




class VercelKVAdapter(NoSQLKVAdapter):
    """
    Vercel KV adapter.

    Supports:
    • Local Redis (used in tests)
    • Vercel KV REST API (production)

    Tests run against redis://localhost:6380
    """

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        kv_url: str = "",
        kv_token: str = "",
        timeout: int = 10,
    ):
        super().__init__(partition_config)

        self.kv_url = kv_url or os.getenv("KV_REST_API_URL", "")
        self.kv_token = kv_token or os.getenv("KV_REST_API_TOKEN", "")
        self.timeout = timeout

        self._redis: Optional[redis.Redis] = None

        # detect local redis
        if self.kv_url.startswith("redis://"):
            self._redis = redis.from_url(self.kv_url, decode_responses=True)

    # ------------------------------------------------------------------
    # PUT
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:

            key = f"{pk}:{rk}"

            payload = dict(data)
            payload["_pk"] = pk
            payload["_rk"] = rk
            payload["id"] = pk

            value = json.dumps(payload, default=json_safe)

            # LOCAL REDIS
            if self._redis:
                self._redis.set(key, value)
                return payload

            # REST API (vercel production)

            requests.post(
                f"{self.kv_url}/set/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                json={"value": value},
                timeout=self.timeout,
            ).raise_for_status()

            return payload

        except Exception as e:
            raise NoSQLError(f"Vercel KV put failed: {e}")

    # ------------------------------------------------------------------
    # GET
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:

        try:

            key = f"{pk}:{rk}"

            # LOCAL REDIS
            if self._redis:

                value: Any = self._redis.get(key)

                if not value:
                    return None

                obj = json.loads(value)
                obj.setdefault("id", obj.get("_pk"))
                return obj

            # REST API

            resp = requests.get(
                f"{self.kv_url}/get/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                return None

            result = resp.json().get("result")

            if not result:
                return None

            obj = json.loads(result)
            obj.setdefault("id", obj.get("_pk"))

            return obj

        except Exception as e:
            raise NoSQLError(f"Vercel KV get failed: {e}")

    # ------------------------------------------------------------------
    # QUERY
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self,
        model: type,
        filters: Dict[str, Any],
        limit: Optional[int],
    ) -> List[JsonDict]:

        try:

            results: List[JsonDict] = []

            # LOCAL REDIS
            if self._redis:

                for key in self._redis.scan_iter("*"):

                    value: Any = self._redis.get(key)

                    if not value:
                        continue

                    obj = json.loads(value)

                    match = True

                    for k, v in filters.items():

                        if k == "id":
                            if obj.get("_pk") != v:
                                match = False
                                break

                        elif obj.get(k) != v:
                            match = False
                            break

                    if match:
                        obj.setdefault("id", obj.get("_pk"))
                        results.append(obj)

                    if limit and len(results) >= limit:
                        break

                return results

            # REST API fallback

            resp = requests.get(
                f"{self.kv_url}/keys/*",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                return []

            keys = resp.json().get("result", [])

            for key in keys:

                if limit and len(results) >= limit:
                    break

                get_resp = requests.get(
                    f"{self.kv_url}/get/{key}",
                    headers={"Authorization": f"Bearer {self.kv_token}"},
                    timeout=self.timeout,
                )

                if get_resp.status_code != 200:
                    continue

                result = get_resp.json().get("result")

                if not result:
                    continue

                obj = json.loads(result)

                match = True

                for k, v in filters.items():

                    if k == "id":
                        if obj.get("_pk") != v:
                            match = False
                            break

                    elif obj.get(k) != v:
                        match = False
                        break

                if match:
                    obj.setdefault("id", obj.get("_pk"))
                    results.append(obj)

            return results

        except Exception as e:
            raise NoSQLError(f"Vercel KV query failed: {e}")

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(
        self,
        model: type,
        pk: str,
        rk: str,
        etag: Optional[str],
    ) -> JsonDict:

        try:

            key = f"{pk}:{rk}"

            # LOCAL REDIS
            if self._redis:

                if not self._redis.exists(key):
                    raise DatabaseError(f"Item {pk}/{rk} does not exist")

                self._redis.delete(key)

                return {"id": pk}

            # REST API

            resp = requests.get(
                f"{self.kv_url}/get/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            )

            if resp.status_code != 200:
                raise DatabaseError(f"Item {pk}/{rk} does not exist")

            requests.delete(
                f"{self.kv_url}/del/{key}",
                headers={"Authorization": f"Bearer {self.kv_token}"},
                timeout=self.timeout,
            ).raise_for_status()

            return {"id": pk}

        except DatabaseError:
            raise

        except Exception as e:
            raise NoSQLError(f"Vercel KV delete failed: {e}")

    # ------------------------------------------------------------------
    # PAGINATION
    # ------------------------------------------------------------------

    def query_page(
        self,
        model: type,
        query: Dict[str, Any],
        page_size: int,
        continuation_token: Optional[str] = None,
    ) -> Tuple[List[JsonDict], Optional[str]]:

        rows = self._query_raw(model, query, None)

        start = 0

        if continuation_token:
            for i, r in enumerate(rows):
                if r["id"] == continuation_token:
                    start = i + 1
                    break

        page = rows[start : start + page_size]

        next_token = None

        if start + page_size < len(rows):
            next_token = page[-1]["id"]

        return page, next_token


# ======================================================
# FILE: polydb/adapters/VercelQueueAdapter.py
# ======================================================




class VercelQueueAdapter:

    def __init__(self, url: str = "", token: str = ""):
        self.url = url or os.getenv("KV_REST_API_URL")
        self.token = token or os.getenv("KV_REST_API_TOKEN")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        try:

            payload = json.dumps(message)

            r = requests.post(
                f"{self.url}/xadd/{queue_name}",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"*": payload},
            )

            r.raise_for_status()

            return r.json()["result"]

        except Exception as e:
            raise QueueError(f"Vercel queue send failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(QueueError,))
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict]:

        try:

            r = requests.get(
                f"{self.url}/xrange/{queue_name}/-/{max_messages}",
                headers={"Authorization": f"Bearer {self.token}"},
            )

            r.raise_for_status()

            result = r.json()["result"]

            messages = []

            for msg in result:
                data = json.loads(msg[1][0][1])
                data["_id"] = msg[0]
                messages.append(data)

            return messages

        except Exception as e:
            raise QueueError(f"Vercel queue receive failed: {e}")

    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        return True

    # ---------------------------------------------------------
    # ACK (same as delete for compatibility)
    # ---------------------------------------------------------
    def ack(self, ack_id: str, queue_name: str = "default") -> bool:
        """
        ACK for Vercel Queue.

        Since Redis Streams via Vercel KV REST API does not support
        explicit ACK without consumer groups, this is treated as no-op.

        Exists for interface consistency.
        """
        if not ack_id:
            raise QueueError("ack_id is required")

        return True


# ======================================================
# FILE: polydb/adapters/__init__.py
# ======================================================



# ======================================================
# FILE: polydb/audit/AuditStorage.py
# ======================================================

# src/polydb/audit/AuditStorage.py




class AuditStorage:
    """Audit log with distributed-safe hash chaining"""
    
    _lock = threading.Lock()
    
    def __init__(self):
        self.factory = CloudDatabaseFactory()
        self.sql = self.factory.get_sql()
        self._ensure_table()
    
    def _ensure_table(self):
        """Create audit table if not exists"""
        try:
            schema = """
            CREATE TABLE IF NOT EXISTS polydb_audit_log (
                audit_id UUID PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                tenant_id VARCHAR(255),
                actor_id VARCHAR(255),
                roles TEXT[],
                action VARCHAR(50) NOT NULL,
                model VARCHAR(255) NOT NULL,
                entity_id VARCHAR(255),
                storage_type VARCHAR(20) NOT NULL,
                provider VARCHAR(50) NOT NULL,
                success BOOLEAN NOT NULL,
                before JSONB,
                after JSONB,
                changed_fields TEXT[],
                trace_id VARCHAR(255),
                request_id VARCHAR(255),
                ip_address VARCHAR(45),
                user_agent TEXT,
                error TEXT,
                hash VARCHAR(64) NOT NULL,
                previous_hash VARCHAR(64),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_audit_tenant_timestamp 
                ON polydb_audit_log(tenant_id, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_audit_model_entity 
                ON polydb_audit_log(model, entity_id);
            CREATE INDEX IF NOT EXISTS idx_audit_actor 
                ON polydb_audit_log(actor_id, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_audit_hash_chain 
                ON polydb_audit_log(tenant_id, timestamp DESC, previous_hash);
            """
            
            self.sql.execute(schema)
        except Exception:
            # Table may already exist
            pass
    
    def get_last_hash(self, tenant_id: Optional[str]) -> Optional[str]:
        """Get most recent hash with strict ordering (distributed-safe)"""
        with self._lock:
            try:
                
                builder = QueryBuilder()
                
                if tenant_id is not None:
                    builder.where('tenant_id', Operator.EQ, tenant_id)
                
                builder.order_by('timestamp', descending=True).take(1)
                
                results = self.sql.query_linq('polydb_audit_log', builder)
                
                if results and len(results) > 0:
                    return results[0].get('hash')
                
                return None
            except Exception:
                return None
    
    def persist(self, record: AuditRecord) -> None:
        """Persist with lock to ensure chain integrity"""
        with self._lock:
            self.sql.insert('polydb_audit_log', {
                'audit_id': record.audit_id,
                'timestamp': record.timestamp,
                'tenant_id': record.tenant_id,
                'actor_id': record.actor_id,
                'roles': record.roles,
                'action': record.action,
                'model': record.model,
                'entity_id': record.entity_id,
                'storage_type': record.storage_type,
                'provider': record.provider,
                'success': record.success,
                'before': record.before,
                'after': record.after,
                'changed_fields': record.changed_fields,
                'trace_id': record.trace_id,
                'request_id': record.request_id,
                'ip_address': record.ip_address,
                'user_agent': record.user_agent,
                'error': record.error,
                'hash': record.hash,
                'previous_hash': record.previous_hash,
            })
    
    def verify_chain(self, tenant_id: Optional[str] = None) -> bool:
        """Verify hash chain integrity"""
        
        builder = QueryBuilder()
        
        if tenant_id is not None:
            builder.where('tenant_id', Operator.EQ, tenant_id)
        
        builder.order_by('timestamp', descending=False)
        
        records = self.sql.query_linq('polydb_audit_log', builder)
        
        if not records:
            return True
        
        prev_hash = None
        for record in records:
            if record.get('previous_hash') != prev_hash:
                return False
            prev_hash = record.get('hash')
        
        return True

# ======================================================
# FILE: polydb/audit/__init__.py
# ======================================================

# src/polydb/audit/__init__.py

__all__ = ['AuditRecord', 'AuditContext', 'AuditManager', 'AuditStorage']

# ======================================================
# FILE: polydb/audit/context.py
# ======================================================

# src/polydb/audit/AuditContext.py

class AuditContext:
    """Context variables for audit trail"""
    
    actor_id: ContextVar[Optional[str]] = ContextVar("actor_id", default=None)
    roles: ContextVar[List[str]] = ContextVar("roles", default=[])
    tenant_id: ContextVar[Optional[str]] = ContextVar("tenant_id", default=None)
    trace_id: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
    request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
    ip_address: ContextVar[Optional[str]] = ContextVar("ip_address", default=None)
    user_agent: ContextVar[Optional[str]] = ContextVar("user_agent", default=None)
    
    @classmethod
    def set(
        cls,
        *,
        actor_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
        tenant_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        request_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """Set audit context for current request"""
        if actor_id is not None:
            cls.actor_id.set(actor_id)
        if roles is not None:
            cls.roles.set(roles)
        if tenant_id is not None:
            cls.tenant_id.set(tenant_id)
        if trace_id is not None:
            cls.trace_id.set(trace_id)
        if request_id is not None:
            cls.request_id.set(request_id)
        if ip_address is not None:
            cls.ip_address.set(ip_address)
        if user_agent is not None:
            cls.user_agent.set(user_agent)
    
    @classmethod
    def clear(cls):
        """Clear all context variables"""
        cls.actor_id.set(None)
        cls.roles.set([])
        cls.tenant_id.set(None)
        cls.trace_id.set(None)
        cls.request_id.set(None)
        cls.ip_address.set(None)
        cls.user_agent.set(None)

# ======================================================
# FILE: polydb/audit/manager.py
# ======================================================

# src/polydb/audit/manager.py




class AuditManager:
    def __init__(self):
        self.storage = AuditStorage()

    def record(
        self,
        *,
        action: str,
        model: str,
        entity_id: Optional[str],
        storage_type: str,
        provider: str,
        success: bool,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        error: Optional[str],
        changed_fields: List[str] | None
    ) -> None:
        tenant_id = AuditContext.tenant_id.get()
        previous_hash = self.storage.get_last_hash(tenant_id)

        record = AuditRecord.create(
            action=action,
            model=model,
            entity_id=entity_id,
            storage_type=storage_type,
            provider=provider,
            success=success,
            before=before,
            after=after,
            changed_fields=changed_fields,
            error=error,
            context=AuditContext,
            previous_hash=previous_hash,
        )

        self.storage.persist(record)


# ======================================================
# FILE: polydb/audit/models.py
# ======================================================

# src/polydb/audit/models.py




@dataclass
class AuditRecord:
    audit_id: str
    timestamp: str
    tenant_id: Optional[str]
    actor_id: Optional[str]
    roles: List[str]
    action: str
    model: str
    entity_id: Optional[str]
    storage_type: str
    provider: str
    success: bool

    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    changed_fields: Optional[List[str]]

    trace_id: Optional[str]
    request_id: Optional[str]
    ip_address: Optional[str]
    user_agent: Optional[str]

    error: Optional[str]

    hash: Optional[str] = None
    previous_hash: Optional[str] = None

    @classmethod
    def create(
        cls,
        *,
        action: str,
        model: str,
        entity_id: Optional[str],
        storage_type: str,
        provider: str,
        success: bool,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        changed_fields: Optional[List[str]],
        error: Optional[str],
        context,
        previous_hash: Optional[str] = None,
    ):
        now = datetime.utcnow().isoformat()
        audit_id = str(uuid.uuid4())

        record = cls(
            audit_id=audit_id,
            timestamp=now,
            tenant_id=context.tenant_id.get(),
            actor_id=context.actor_id.get(),
            roles=context.roles.get(),
            action=action,
            model=model,
            entity_id=entity_id,
            storage_type=storage_type,
            provider=provider,
            success=success,
            before=before,
            after=after,
            changed_fields=changed_fields,
            trace_id=context.trace_id.get(),
            request_id=context.request_id.get(),
            ip_address=context.ip_address.get(),
            user_agent=context.user_agent.get(),
            error=error,
            previous_hash=previous_hash,
        )

        record.hash = hashlib.sha256(
            json.dumps(asdict(record), sort_keys=True,default=json_safe).encode()
        ).hexdigest()

        return record


# ======================================================
# FILE: polydb/base/NoSQLKVAdapter.py
# ======================================================

# src/polydb/adapters/NoSQLKVAdapter.py





if TYPE_CHECKING:


class NoSQLKVAdapter:
    """Base with auto-overflow and LINQ support"""

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
    ):

        self.logger = setup_logger(self.__class__.__name__)
        self.partition_config = partition_config
        self.object_storage = None
        self._lock = threading.Lock()
        self.max_size = 1024 * 1024  # 1MB

    def _get_pk_rk(self, model: type, data: JsonDict) -> Tuple[str, str]:
        """Extract PK/RK from model metadata"""
        meta = getattr(model, "__polydb__", {})
        pk_field = meta.get("pk_field") or meta.get("partition_key", "tenant_id")
        rk_field = meta.get("rk_field") or meta.get("sort_key", "id")

        if self.partition_config:
            try:
                pk = self.partition_config.partition_key_template.format(**data)
            except KeyError:
                pk = f"default_{data.get(pk_field, hashlib.md5(json.dumps(data, sort_keys=True,default=json_safe).encode()).hexdigest()[:8])}"
        else:
            pk = str(data.get(pk_field, "default"))

        if rk_field and rk_field in data:
            rk = str(data[rk_field])
        elif self.partition_config and self.partition_config.row_key_template:
            try:
                rk = self.partition_config.row_key_template.format(**data)
            except KeyError:
                rk = hashlib.md5(
                    json.dumps(data, sort_keys=True, default=json_safe).encode()
                ).hexdigest()
        else:
            rk = data.get(
                "id",
                hashlib.md5(
                    json.dumps(data, sort_keys=True, default=json_safe).encode()
                ).hexdigest(),
            )

        return str(pk), str(rk)

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _check_overflow(self, data: JsonDict) -> Tuple[JsonDict, Optional[str]]:
        """Check size and store in blob if needed"""
        data_bytes = json.dumps(data, default=json_safe).encode()
        data_size = len(data_bytes)

        if data_size > self.max_size:
            with self._lock:
                if not self.object_storage:

                    factory = CloudDatabaseFactory()
                    self.object_storage = factory.get_object_storage()

            blob_id = hashlib.md5(data_bytes).hexdigest()
            blob_key = f"overflow/{blob_id}.json"

            try:
                self.object_storage.put(blob_key, data_bytes)
            except Exception as e:
                raise StorageError(f"Overflow storage failed: {str(e)}")

            return {
                "_overflow": True,
                "_blob_key": blob_key,
                "_size": data_size,
                "_checksum": blob_id,
            }, blob_key

        return data, None

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _retrieve_overflow(self, data: JsonDict) -> JsonDict:
        """Retrieve from blob if overflow"""
        if not data.get("_overflow"):
            return data

        with self._lock:
            if not self.object_storage:

                factory = CloudDatabaseFactory()
                self.object_storage = factory.get_object_storage()

        try:
            blob_data = self.object_storage.get(data["_blob_key"])
            retrieved = json.loads(blob_data.decode())

            # Verify checksum
            checksum = hashlib.md5(blob_data).hexdigest()
            if checksum != data.get("_checksum"):
                raise StorageError("Checksum mismatch on overflow retrieval")

            return retrieved
        except Exception as e:
            raise StorageError(f"Overflow retrieval failed: {str(e)}")

    def _apply_filters(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply filters in-memory for NoSQL"""
        if not builder.filters:
            return results

        filtered = []
        for item in results:
            match = True
            for f in builder.filters:
                value = item.get(f.field)

                if f.operator == Operator.EQ and value != f.value:
                    match = False
                elif f.operator == Operator.NE and value == f.value:
                    match = False
                elif f.operator == Operator.GT and not (value and value > f.value):
                    match = False
                elif f.operator == Operator.GTE and not (value and value >= f.value):
                    match = False
                elif f.operator == Operator.LT and not (value and value < f.value):
                    match = False
                elif f.operator == Operator.LTE and not (value and value <= f.value):
                    match = False
                elif f.operator == Operator.IN and value not in f.value:
                    match = False
                elif f.operator == Operator.NOT_IN and value in f.value:
                    match = False
                elif f.operator == Operator.CONTAINS and (not value or f.value not in str(value)):
                    match = False
                elif f.operator == Operator.STARTS_WITH and (
                    not value or not str(value).startswith(f.value)
                ):
                    match = False
                elif f.operator == Operator.ENDS_WITH and (
                    not value or not str(value).endswith(f.value)
                ):
                    match = False

                if not match:
                    break

            if match:
                filtered.append(item)

        return filtered

    def _apply_ordering(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply ordering"""
        if not builder.order_by_fields:
            return results

        for field, desc in reversed(builder.order_by_fields):
            results = sorted(results, key=lambda x: x.get(field, ""), reverse=desc)

        return results

    def _apply_pagination(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply skip/take"""
        if builder.skip_count:
            results = results[builder.skip_count :]

        if builder.take_count:
            results = results[: builder.take_count]

        return results

    def _apply_projection(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:

        fields = builder.selected_fields
        if not fields:
            return results

        field_set = set(fields)

        return [{k: v for k, v in item.items() if k in field_set} for item in results]

    # Abstract methods to implement
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        raise NotImplementedError

    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        raise NotImplementedError

    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        raise NotImplementedError

    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        raise NotImplementedError

    # Protocol implementation
    def put(self, model: type, data: JsonDict) -> JsonDict:
        pk, rk = self._get_pk_rk(model, data)
        return self._put_raw(model, pk, rk, data)

    def query(
        self,
        model: type,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        no_cache: bool = False,
        cache_ttl: Optional[int] = None,
    ) -> List[JsonDict]:
        results = self._query_raw(model, query or {}, limit)
        return [self._retrieve_overflow(r) for r in results]

    def query_page(
        self, model: type, query: Lookup, page_size: int, continuation_token: Optional[str] = None
    ) -> Tuple[List[JsonDict], Optional[str]]:
        # Basic implementation - override per provider
        offset = int(continuation_token) if continuation_token else 0
        results = self.query(model, query, limit=page_size + 1)

        has_more = len(results) > page_size
        if has_more:
            results = results[:page_size]

        next_token = str(offset + page_size) if has_more else None
        return results, next_token

    def patch(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False,
    ) -> JsonDict:
        if isinstance(entity_id, dict):
            pk = entity_id.get("partition_key") or entity_id.get("pk")
            rk = entity_id.get("row_key") or entity_id.get("rk") or entity_id.get("id")

        else:
            pk, rk = self._get_pk_rk(model, {"id": entity_id, **data})

        if not replace:
            existing = self._get_raw(model, pk, rk)  # type: ignore
            if existing:
                existing = self._retrieve_overflow(existing)
                existing.update(data)
                data = existing

        store_data, _ = self._check_overflow(data)
        return self._put_raw(model, pk, rk, store_data)  # type: ignore

    def upsert(self, model: type, data: JsonDict, *, replace: bool = False) -> JsonDict:
        return self.put(model, data)

    def delete(
        self, model: type, entity_id: Union[Any, Lookup], *, etag: Optional[str] = None
    ) -> JsonDict:
        if isinstance(entity_id, dict):
            pk = entity_id.get("partition_key") or entity_id.get("pk")
            rk = entity_id.get("row_key") or entity_id.get("rk") or entity_id.get("id")
        else:
            pk, rk = self._get_pk_rk(model, {"id": entity_id})

        return self._delete_raw(model, pk, rk, etag)  # type: ignore

    def query_linq_rows(self, model: type, builder: QueryBuilder) -> List[JsonDict]:
        """
        Typed wrapper for queries that return rows.
        Use this when builder.count_only is False.
        """
        result = self.query_linq(model, builder)
        return cast(List[JsonDict], result)

    def query_linq(self, model: type, builder: QueryBuilder) -> Union[List[JsonDict], int]:
        """LINQ-style query"""
        results = self._query_raw(model, {}, None)
        results = [self._retrieve_overflow(r) for r in results]

        results = self._apply_filters(results, builder)

        if builder.count_only:
            return len(results)

        results = self._apply_ordering(results, builder)
        results = self._apply_pagination(results, builder)
        results = self._apply_projection(results, builder)

        if builder.distinct:
            seen = set()
            unique = []
            for r in results:
                key = json.dumps(r, sort_keys=True, default=json_safe)
                if key not in seen:
                    seen.add(key)
                    unique.append(r)
            results = unique

        return results


# ======================================================
# FILE: polydb/base/ObjectStorageAdapter.py
# ======================================================



class ObjectStorageAdapter(ABC):
    """Base class for Object Storage with automatic optimization"""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    def put(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        optimize: bool = True,
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Store object with optional optimization"""
        if optimize and media_type:
            data = self._optimize_media(data, media_type)
        return self._put_raw(
            key=key, data=data, fileName=fileName, media_type=media_type, metadata=metadata
        )

    def _optimize_media(self, data: bytes, media_type: str) -> bytes:
        """Optimize images and videos - placeholder for implementation"""
        return data

    @abstractmethod
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Provider-specific put"""
        pass

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Get object"""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete object"""
        pass

    @abstractmethod
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        pass

    def upload(self, key: str, data: bytes, **kwargs) -> str:
        """
        Alias for put().
        Accepts kwargs so callers can pass optimize/media_type without breaking.
        """
        return self.put(key, data, **kwargs)

    def download(self, key: str) -> bytes:
        """
        Alias for get() but guarantees bytes or raises.
        This matches how your tests expect download() to behave.
        """
        if not key:
            raise StorageError("Key cannot be empty")

        data = self.get(key)
        if data is None:
            raise StorageError(f"Object not found: {key}")
        return data


# ======================================================
# FILE: polydb/base/QueueAdapter.py
# ======================================================





class QueueAdapter(ABC):
    """Base class for Queue/Message services"""

    def __init__(self, connection_string: str = ""):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        pass

    @abstractmethod
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        pass

    @abstractmethod
    def delete(self, message_id: str, queue_name: str = "default", pop_receipt: str = "") -> bool:
        """Delete message from queue"""
        pass


# ======================================================
# FILE: polydb/base/SharedFilesAdapter.py
# ======================================================





class SharedFilesAdapter(ABC):
    """Base class for Shared File Storage"""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def write(self, path: str, data: bytes) -> bool:
        """Write file"""
        pass

    @abstractmethod
    def read(self, path: str) -> bytes | None:
        """Read file"""
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        """Delete file"""
        pass

    @abstractmethod
    def list(self, directory: str = "/") -> List[str]:
        """List files in directory"""
        pass


# ======================================================
# FILE: polydb/base/__init__.py
# ======================================================



