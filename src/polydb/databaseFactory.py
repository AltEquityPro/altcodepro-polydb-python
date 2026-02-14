from __future__ import annotations

import logging
import os
from typing import Any, Callable, List, Optional, Tuple, Union
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from polydb.batch import BatchOperations
from polydb.cache import CacheWarmer, RedisCacheEngine
from polydb.monitoring import HealthCheck, MetricsCollector, PerformanceMonitor
from polydb.multitenancy import TenantContext, TenantIsolationEnforcer, TenantRegistry
from polydb.security import DataMasking, FieldEncryption, RowLevelSecurity
from polydb.validation import ModelValidator
from .errors import AdapterConfigurationError
from .registry import ModelRegistry
from .types import JsonDict, Lookup, ModelMeta
from .audit.manager import AuditManager
from .audit.context import AuditContext
from .query import Operator, QueryBuilder
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_DEFAULT_RETRY = retry(
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    stop=stop_after_attempt(3),
    reraise=True,
)


class DatabaseFactory:
    """Universal CRUD with cache, soft delete, audit, multi-tenancy, RLS, encryption, monitoring"""

    def __init__(
        self,
        *,
        provider: Optional[Any] = None,
        cloud_factory: Optional[Any] = None,
        tenant_registry: Optional[TenantRegistry] = None,
        enable_retries: bool = True,
        enable_audit: bool = True,
        enable_audit_reads: bool = False,
        enable_cache: bool = True,
        soft_delete: bool = False,
        use_redis_cache: bool = False,
        enable_monitoring: bool = False,
        enable_encryption: bool = False,
        enable_rls: bool = False,
    ):
        from .factory import CloudDatabaseFactory

        self._enable_retries = enable_retries
        self._enable_audit = enable_audit
        self._enable_audit_reads = enable_audit_reads
        self._enable_cache = enable_cache
        self._soft_delete = soft_delete

        # Monitoring
        if enable_monitoring:
            self.metrics = MetricsCollector()
            self.health = HealthCheck(self)
        else:
            self.metrics = None
            self.health = None

        # Redis cache (only if explicitly enabled + URL present)
        self._cache: Optional[RedisCacheEngine] = None
        self.cache_warmer: Optional[CacheWarmer] = None
        try:
            load_dotenv()
        except Exception:
            pass

        if enable_cache and use_redis_cache:
            redis_url = os.getenv("REDIS_CACHE_URL")
            if redis_url:
                self._cache = RedisCacheEngine(redis_url=redis_url)
                self.cache_warmer = CacheWarmer(self, self._cache)
            else:
                logger.warning("use_redis_cache=True but REDIS_CACHE_URL not set")

        # Encryption
        self.encryption = FieldEncryption() if enable_encryption else None

        # Always available (can be no-op if not configured)
        self.masking = DataMasking()

        # Row-level security
        self.rls = RowLevelSecurity() if enable_rls else None

        # Multi-tenancy
        self.tenant_registry = tenant_registry
        self.tenant_enforcer = TenantIsolationEnforcer(tenant_registry) if tenant_registry else None

        self.batch = BatchOperations(self)
        self._cloud_factory = cloud_factory or CloudDatabaseFactory(provider=provider)
        self._provider_name = self._cloud_factory.provider.value

        self._sql = self._cloud_factory.get_sql()
        self._nosql = self._cloud_factory.get_nosql_kv()

        if not self._sql or not self._nosql:
            raise AdapterConfigurationError("Adapters not initialized")

        self._audit = AuditManager() if enable_audit else None

    def _meta(self, model: Union[type, str]) -> ModelMeta:
        return ModelRegistry.get(model)

    def _model_type(self, model: Union[type, str]) -> type:
        return ModelRegistry.resolve(model)

    def _model_name(self, model: Union[type, str]) -> str:
        return model.__name__ if isinstance(model, type) else str(model)

    def _current_tenant_id(self) -> Optional[str]:
        # Prefer TenantContext if present
        try:
            tenant = TenantContext.get_tenant()
            if tenant and tenant.tenant_id:
                return tenant.tenant_id
        except Exception:
            pass

        # Fallback to AuditContext
        return AuditContext.tenant_id.get()

    def _current_actor_id(self) -> Optional[str]:
        return AuditContext.actor_id.get()

    def _inject_tenant(self, data: JsonDict) -> JsonDict:
        tenant_id = self._current_tenant_id()

        if not tenant_id:
            raise ValueError("Tenant ID is required but not set in AuditContext or TenantContext")

        data = dict(data)
        data.setdefault("tenant_id", tenant_id)
        return data

    def _inject_audit_fields(self, data: JsonDict, is_create: bool = False) -> JsonDict:
        data = dict(data)
        actor_id = self._current_actor_id()
        now = datetime.utcnow().isoformat()

        if is_create:
            if "created_at" not in data:
                data["created_at"] = now
            if "created_by" not in data and actor_id:
                data["created_by"] = actor_id

        if "updated_at" not in data:
            data["updated_at"] = now
        if "updated_by" not in data and actor_id:
            data["updated_by"] = actor_id

        return data

    def _apply_soft_delete_filter(self, query: Optional[Lookup]) -> Lookup:
        if not self._soft_delete:
            return query or {}

        result = dict(query or {})
        result.setdefault("deleted_at", None)
        return result

    def _compute_field_changes(
        self, before: Optional[JsonDict], after: Optional[JsonDict]
    ) -> Optional[List[str]]:
        if not before or not after:
            return None
        changed = [
            key
            for key in set(before.keys()) | set(after.keys())
            if before.get(key) != after.get(key)
        ]
        return changed or None

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
    ):
        if not self._audit:
            return

        try:
            changed_fields = self._compute_field_changes(before, after)
            self._audit.record(
                action=action,
                model=self._model_name(model),
                entity_id=str(entity_id) if entity_id else None,
                storage_type=meta.storage,
                provider=self._provider_name,
                success=success,
                before=before,
                after=after,
                changed_fields=changed_fields,
                error=error,
            )
        except Exception as exc:
            logger.error(f"Audit recording failed: {exc}")

    def _run(self, fn: Callable[[], Any]) -> Any:
        if not self._enable_retries:
            return fn()
        retry_fn = _DEFAULT_RETRY(fn)
        return retry_fn()

    def create(self, model: Union[type, str], data: JsonDict) -> JsonDict:
        model_type = self._model_type(model)
        ModelValidator.validate_and_raise(model_type)

        model_name = self._model_name(model)
        meta = self._meta(model)

        tenant_id = self._current_tenant_id()
        actor_id = self._current_actor_id()

        # Security & policies
        if self.tenant_enforcer:
            data = self.tenant_enforcer.enforce_write(model_name, data)
        if self.rls:
            data = self.rls.enforce_write(model_name, data)

        data = self._inject_tenant(data)
        data = self._inject_audit_fields(data, is_create=True)

        # Encryption (uses meta.encrypted_fields – assumed defined on model)
        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, encrypted_fields)

        before = None
        after_plain = None
        success = False
        error: Optional[str] = None
        entity_id: Optional[Any] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success, entity_id

            if meta.storage == "sql" and meta.table:
                result = self._sql.insert(meta.table, data)
            else:
                result = self._nosql.put(model_type, data)

            entity_id = result.get("id")
            # Decrypt for audit / returned value (plain text)
            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)

            success = True

            # Cache invalidation
            if self._enable_cache and self._cache:
                self._cache.invalidate(model_name)

            return after_plain

        monitor_ctx = (
            PerformanceMonitor(self.metrics, "create", model_name, tenant_id)
            if self.metrics
            else None
        )
        try:
            if monitor_ctx:
                with monitor_ctx as m:
                    result = self._run(_op)
                    m.rows_affected = 1  # type: ignore
                    return result
            else:
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
                before=before,
                after=after_plain,
                error=error,
            )

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
    ) -> List[JsonDict]:

        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()
        actor_id = self._current_actor_id()
        query = self._apply_soft_delete_filter(query if not include_deleted else None)
        # Multi-tenancy & RLS filters
        if self.tenant_enforcer:
            query = self.tenant_enforcer.enforce_read(model_name, query or {})
        if self.rls:
            query = self.rls.enforce_read(model_name, query or {})
        # Inject tenant filter (mandatory isolation)
        tenant_id = self._current_tenant_id()
        if tenant_id:
            query = dict(query or {})
            query.setdefault("tenant_id", tenant_id)
        use_external_cache = self._enable_cache and self._cache and getattr(meta, "cache", False)
        encrypted_fields = getattr(meta, "encrypted_fields", [])

        def _op() -> List[JsonDict]:
            if meta.storage == "sql" and meta.table:
                raw_rows = self._sql.select(meta.table, query, limit=limit, offset=offset)
            else:
                model_type = self._model_type(model)
                eff_no_cache = no_cache or use_external_cache
                eff_ttl = cache_ttl if cache_ttl is not None else getattr(meta, "cache_ttl", None)
                raw_rows = self._nosql.query(
                    model_type,
                    query=query,
                    limit=limit,
                    no_cache=eff_no_cache,  # type: ignore
                    cache_ttl=None if eff_no_cache else eff_ttl,
                )

            # Decrypt + mask
            if self.encryption and encrypted_fields:
                raw_rows = [self.encryption.decrypt_fields(r, encrypted_fields) for r in raw_rows]
            rows = [
                self.masking.mask(r, model=model_name, actor_id=actor_id, tenant_id=tenant_id)
                for r in raw_rows
            ]

            # Set external cache
            if use_external_cache and not no_cache:
                ttl = cache_ttl or getattr(meta, "cache_ttl", 300)
                self._cache.set(model_name, query or {}, rows, ttl)  # type: ignore

            return rows

        # Cache check
        rows: List[JsonDict] = []
        cached = None
        if use_external_cache and not no_cache:
            cached = self._cache.get(model_name, query or {})  # type: ignore
        if cached is not None:
            rows = cached
        else:
            # Run op with monitoring
            monitor_ctx = (
                PerformanceMonitor(self.metrics, "read", model_name, tenant_id)
                if self.metrics
                else None
            )
            if monitor_ctx:
                with monitor_ctx as m:
                    rows = self._run(_op)
                m.rows_returned = len(rows)  # type: ignore
            else:
                rows = self._run(_op)

        # Audit on success
        if self._audit and self._enable_audit_reads:
            self._audit_safe(
                action="read",
                model=model,
                entity_id=None,
                meta=meta,
                success=True,
                before=None,
                after={"count": len(rows)},
                error=None,
            )

        return rows

    def read_one(
        self,
        model: Union[type, str],
        query: Lookup,
        *,
        no_cache: bool = False,
        include_deleted: bool = False,
    ) -> Optional[JsonDict]:
        rows = self.read(
            model,
            query=query,
            limit=1,
            no_cache=no_cache,
            include_deleted=include_deleted,
        )
        return rows[0] if rows else None

    def read_page(
        self,
        model: Union[type, str],
        query: Lookup,
        *,
        page_size: int = 100,
        continuation_token: Optional[str] = None,
        include_deleted: bool = False,
    ) -> Tuple[List[JsonDict], Optional[str]] | None:
        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()
        actor_id = self._current_actor_id()

        query = self._apply_soft_delete_filter(query if not include_deleted else None)

        if self.tenant_enforcer:
            query = self.tenant_enforcer.enforce_read(model_name, query or {})
        if self.rls:
            query = self.rls.enforce_read(model_name, query or {})
        # Inject tenant filter (mandatory isolation)
        tenant_id = self._current_tenant_id()
        if tenant_id:
            query = dict(query or {})
            query.setdefault("tenant_id", tenant_id)
        encrypted_fields = getattr(meta, "encrypted_fields", [])

        def _op() -> Tuple[List[JsonDict], Optional[str]]:
            if meta.storage == "sql" and meta.table:
                raw_rows, next_token = self._sql.select_page(
                    meta.table, query, page_size, continuation_token
                )
            else:
                model_type = self._model_type(model)
                raw_rows, next_token = self._nosql.query_page(
                    model_type, query, page_size, continuation_token
                )

            if self.encryption and encrypted_fields:
                raw_rows = [self.encryption.decrypt_fields(r, encrypted_fields) for r in raw_rows]
            rows = [
                self.masking.mask(r, model=model_name, actor_id=actor_id, tenant_id=tenant_id)
                for r in raw_rows
            ]
            return rows, next_token

        # Run with monitoring
        monitor_ctx = (
            PerformanceMonitor(self.metrics, "read_page", model_name, tenant_id)
            if self.metrics
            else None
        )
        result: Optional[Tuple[List[JsonDict], Optional[str]]] = None
        try:
            if monitor_ctx:
                with monitor_ctx as m:
                    result = self._run(_op)
                m.rows_returned = len(result[0])  # type: ignore
            else:
                result = self._run(_op)

            # Audit on success
            if self._audit and self._enable_audit_reads and result:
                count = len(result[0])
                self._audit_safe(
                    action="read_page",
                    model=model,
                    entity_id=None,
                    meta=meta,
                    success=True,
                    before=None,
                    after={"count": count},
                    error=None,
                )

            return result
        except Exception:
            raise

    def update(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False,
    ) -> JsonDict:
        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()

        data = self._inject_audit_fields(data, is_create=False)

        # Security on changed fields
        if self.tenant_enforcer:
            data = self.tenant_enforcer.enforce_write(model_name, data)
        if self.rls:
            data = self.rls.enforce_write(model_name, data)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, [f for f in encrypted_fields if f in data])

        before = self._fetch_before(model, meta, entity_id, etag=etag)
        after_plain = None
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success

            if meta.storage == "sql" and meta.table:
                if tenant_id:
                    if isinstance(entity_id, dict):
                        entity_id.setdefault("tenant_id", tenant_id)
                result = self._sql.update(meta.table, entity_id, data)
            else:
                model_type = self._model_type(model)
                result = self._nosql.patch(model_type, entity_id, data, etag=etag, replace=replace)

            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)

            success = True

            if self._enable_cache and self._cache:
                self._cache.invalidate(model_name)

            return after_plain

        monitor_ctx = (
            PerformanceMonitor(self.metrics, "update", model_name, tenant_id)
            if self.metrics
            else None
        )
        try:
            if monitor_ctx:
                with monitor_ctx as m:
                    result = self._run(_op)
                    m.rows_affected = 1  # type: ignore
                    return result
            else:
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
            )

    def upsert(self, model: Union[type, str], data: JsonDict, *, replace: bool = False) -> JsonDict:
        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()

        if self.tenant_enforcer:
            data = self.tenant_enforcer.enforce_write(model_name, data)
        if self.rls:
            data = self.rls.enforce_write(model_name, data)

        data = self._inject_tenant(data)
        data = self._inject_audit_fields(data, is_create=True)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, encrypted_fields)

        after_plain = None
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success

            if meta.storage == "sql" and meta.table:
                result = self._sql.upsert(meta.table, data)
            else:
                model_type = self._model_type(model)
                result = self._nosql.upsert(model_type, data, replace=replace)

            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)

            success = True

            if self._enable_cache and self._cache:
                self._cache.invalidate(model_name)

            return after_plain

        monitor_ctx = (
            PerformanceMonitor(self.metrics, "upsert", model_name, tenant_id)
            if self.metrics
            else None
        )
        try:
            if monitor_ctx:
                with monitor_ctx as m:
                    result = self._run(_op)
                    m.rows_affected = 1  # type: ignore
                    return result
            else:
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
            )

    def delete(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None,
        hard: bool = False,
    ) -> JsonDict:
        meta = self._meta(model)
        model_name = self._model_name(model)
        tenant_id = self._current_tenant_id()

        if self._soft_delete and not hard:
            now = datetime.utcnow().isoformat()
            delete_payload = {
                "deleted_at": now,
                "deleted_by": self._current_actor_id(),
            }
            return self.update(model, entity_id, delete_payload)

        before = self._fetch_before(model, meta, entity_id, etag=etag)
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal success

            if meta.storage == "sql" and meta.table:
                result = self._sql.delete(meta.table, entity_id)
            else:
                model_type = self._model_type(model)
                result = self._nosql.delete(model_type, entity_id, etag=etag)

            success = True

            if self._enable_cache and self._cache:
                self._cache.invalidate(model_name)

            return result

        monitor_ctx = (
            PerformanceMonitor(self.metrics, "delete", model_name, tenant_id)
            if self.metrics
            else None
        )
        try:
            if monitor_ctx:
                with monitor_ctx as m:
                    result = self._run(_op)
                    m.rows_affected = 1  # type: ignore
                    return result
            else:
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
            )

    def _fetch_before(
        self,
        model: Union[type, str],
        meta: ModelMeta,
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None,
    ) -> Optional[JsonDict]:
        lookup = {"id": entity_id} if not isinstance(entity_id, dict) else entity_id
        # read_one already applies tenant + RLS + decryption + masking
        return self.read_one(model, lookup, no_cache=True, include_deleted=True)

    def query_linq(
        self, model: Union[type, str], builder: QueryBuilder
    ) -> Union[List[JsonDict], int]:
        model_name = self._model_name(model)
        meta = self._meta(model)
        extra_filter = {}
        if self.tenant_enforcer:
            extra_filter = self.tenant_enforcer.enforce_read(model_name, extra_filter)
        if self.rls:
            extra_filter = self.rls.enforce_read(model_name, extra_filter)

        if extra_filter:
            for field, value in extra_filter.items():
                builder = builder.where(field, Operator.EQ, value)

        tenant_id = self._current_tenant_id()

        def _op():
            if meta.storage == "sql" and meta.table:
                return self._sql.query_linq(meta.table, builder)
            else:
                model_type = self._model_type(model)
                return self._nosql.query_linq(model_type, builder)

        # Run with monitoring
        monitor_ctx = (
            PerformanceMonitor(self.metrics, "query_linq", model_name, tenant_id)
            if self.metrics
            else None
        )
        result: Union[List[JsonDict], int]
        try:
            if monitor_ctx:
                with monitor_ctx as m:
                    result = self._run(_op)
                if isinstance(result, list):
                    m.rows_returned = len(result)  # type: ignore
            else:
                result = self._run(_op)

            # Audit on success
            if self._audit and self._enable_audit_reads and isinstance(result, list):
                self._audit_safe(
                    action="query_linq",
                    model=model,
                    entity_id=None,
                    meta=meta,
                    success=True,
                    before=None,
                    after={"count": len(result)},
                    error=None,
                )

            return result
        except Exception:
            raise
