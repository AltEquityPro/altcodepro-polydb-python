from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Type, Union

from .advanced_query import AdvancedQueryBuilder, QueryHelper
from .batch import BatchOperations, BatchResult
from .cache import CacheWarmer, RedisCacheEngine
from .cloudDatabaseFactory import CloudDatabaseFactory
from .databaseFactory import DatabaseFactory, EngineConfig, EngineOverride
from .models import CloudProvider, PartitionConfig, StorageConfig
from .monitoring import HealthCheck, MetricsCollector
from .multitenancy import (
    TenantConfig,
    TenantContext,
    TenantIsolationEnforcer,
    TenantMigrationManager,
    TenantQuotaManager,
    TenantRegistry,
)
from .query import QueryBuilder
from .security import DataMasking, FieldEncryption, RowLevelSecurity
from .types import JsonDict, Lookup
from .utils import setup_logger
from .validation import ModelValidator, SchemaValidator


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
