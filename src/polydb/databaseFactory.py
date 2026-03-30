"""
DatabaseFactory — Pure Storage Layer
=====================================

Multi-engine CRUD, blob, queue, cache, file operations.

NO business logic. NO tenant enforcement. NO model registry validation.
NO RLS. Those belong in UDL.

PolyDB is the dumb storage layer. UDL is the smart layer.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from tenacity import retry, stop_after_attempt, wait_exponential

from .batch import BatchOperations
from .cache import CacheWarmer, RedisCacheEngine
from .monitoring import HealthCheck, MetricsCollector, PerformanceMonitor
from .security import DataMasking, FieldEncryption
from .errors import AdapterConfigurationError
from .types import JsonDict, Lookup, ModelMeta
from .audit.manager import AuditManager
from .audit.context import AuditContext
from .query import Operator, QueryBuilder
from .cloudDatabaseFactory import CloudDatabaseFactory

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
    sql: Any
    nosql: Any
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
    def _sql(self) -> Any:
        for e in self._engines:
            if e.is_default_sql:
                return e.sql()
        return self._engines[0].sql()

    @property
    def _nosql(self) -> Any:
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
                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )
                result = adapters.nosql.patch(cls, entity_id, data, etag=etag, replace=replace)
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
