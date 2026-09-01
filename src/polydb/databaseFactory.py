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

from .adapters.PostgreSQLAdapter import PostgreSQLAdapter

from .base.NoSQLKVAdapter import NoSQLKVAdapter

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
from .models import PageRequest, PageResult
import re as _re

logger = logging.getLogger(__name__)

_DEFAULT_RETRY = retry(
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    stop=stop_after_attempt(3),
    reraise=True,
)

_UNIQUE_VIOLATION_MARKERS = (
    "23505",  # Postgres SQLSTATE
    "duplicate key value violates",  # Postgres message
    "unique constraint",  # Postgres, generic
    "UniqueViolation",  # psycopg / SQLAlchemy class name
    "Duplicate entry",  # MySQL
    "UNIQUE constraint failed",  # SQLite
)
_UNIQUE_KEY_RE = _re.compile(r"Key \(([^)]+)\)=")


def _is_unique_violation(exc: BaseException) -> bool:
    s = str(exc)
    return any(m in s for m in _UNIQUE_VIOLATION_MARKERS)


def _parse_unique_violation_columns(exc: BaseException) -> list:
    m = _UNIQUE_KEY_RE.search(str(exc))
    if not m:
        return []
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


# ═══════════════════════════════════════════════════════════════════════════════════
# ENGINE CONFIG
# ═══════════════════════════════════════════════════════════════════════════════════


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
    sql: Optional[PostgreSQLAdapter]
    nosql: Optional[NoSQLKVAdapter]
    engine_name: str


# ════════════════════════════════════════════════════════════════════════════════════
# MODEL META RESOLUTION (lightweight — no registry enforcement)
# ════════════════════════════════════════════════════════════════════════════════════


def _extract_meta(model: Union[type, str]) -> ModelMeta:
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
    return ModelMeta(storage="nosql", table=None, collection=None)


def _model_name(model: Union[type, str]) -> str:
    return model.__name__ if isinstance(model, type) else str(model)


# ════════════════════════════════════════════════════════════════════════════════════
# DATABASE FACTORY
# ═══════════════════════════════════════════════════════════════════════════════════


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

    # ─────────────────────────────────────────────────────────────
    # ENGINE ROUTING
    # ─────────────────────────────────────────────────────────────

    def _resolve_adapters(
        self, model_name: str, storage: str, override: Optional[EngineOverride] = None
    ) -> _ResolvedAdapters:
        # Only construct the adapter this call actually needs. Every caller
        # (create/read/update/upsert/delete/query*) branches on `storage` and
        # touches exactly one of .sql/.nosql -- building both unconditionally
        # meant a purely-SQL deployment with no reachable NoSQL backend (or
        # vice versa) failed on every single call, not just ones that needed
        # the missing engine.
        def _build(engine: "EngineConfig") -> _ResolvedAdapters:
            return _ResolvedAdapters(
                sql=engine.sql() if storage == "sql" else None,
                nosql=engine.nosql() if storage == "nosql" else None,
                engine_name=engine.name,
            )

        if override:
            engine = self._engine_by_name.get(override.engine_name)
            if engine is None:
                raise AdapterConfigurationError(
                    f"Unknown engine '{override.engine_name}'. Available: {list(self._engine_by_name)}"
                )
            return _build(engine)

        for engine in self._engines:
            if storage == "sql" and engine.sql_models and model_name in engine.sql_models:
                return _build(engine)
            if storage == "nosql" and engine.nosql_models and model_name in engine.nosql_models:
                return _build(engine)

        for engine in self._engines:
            if storage == "sql" and engine.is_default_sql:
                return _build(engine)
            if storage == "nosql" and engine.is_default_nosql:
                return _build(engine)

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

    # ──────────────────────────────────────────────────────────
    # ENGINE MANAGEMENT
    # ──────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────

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

    def _run(self, fn: Callable[[], Any]) -> Any:
        return fn()

    def _is_sql(self, meta: ModelMeta, override: Optional[EngineOverride] = None) -> bool:
        if override and override.force_sql:
            return True
        if override and override.force_nosql:
            return False
        return meta.storage == "sql" and bool(meta.table)

    # ──────────────────────────────────────────────────────────
    # CREATE
    # ──────────────────────────────────────────────────────────

    def create(
        self,
        model: Union[type, str],
        data: JsonDict,
        *,
        engine_override: Optional[EngineOverride] = None,
        session_vars: Optional[Dict[str, str]] = None,
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
                try:
                    result = adapters.sql.insert(meta.table, data, session_vars=session_vars)
                except Exception as exc:
                    if not _is_unique_violation(exc):
                        raise
                    conflict_cols = _parse_unique_violation_columns(exc)
                    if not conflict_cols or not all(c in data for c in conflict_cols):
                        raise
                    where = {c: data[c] for c in conflict_cols}
                    logger.warning(
                        "insert %s hit unique violation on %s — falling through to update",
                        meta.table,
                        conflict_cols,
                    )
                    update_data = {k: v for k, v in data.items() if k not in conflict_cols}
                    result = adapters.sql.update(meta.table, where, update_data, session_vars=session_vars)
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
                try:
                    self._cache.invalidate(name)
                except Exception as _ce:
                    logger.warning("Cache invalidate failed (non-fatal): %s", _ce)
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
        except Exception:
            raise

    # ──────────────────────────────────────────────────────────
    # READ
    # ──────────────────────────────────────────────────────────

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
        session_vars: Optional[Dict[str, str]] = None,
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
                raw = adapters.sql.select(
                    meta.table, query, limit=limit, offset=offset, session_vars=session_vars
                )
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
                try:
                    self._cache.set(name, query or {}, raw, ttl)
                except Exception as _ce:
                    logger.warning("Cache set failed (non-fatal): %s", _ce)
            return raw

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
        session_vars: Optional[Dict[str, str]] = None,
    ) -> Optional[JsonDict]:
        rows = self.read(
            model,
            query=query,
            limit=1,
            no_cache=no_cache,
            include_deleted=include_deleted,
            engine_override=engine_override,
            session_vars=session_vars,
        )
        return rows[0] if rows else None

    # ──────────────────────────────────────────────────────────
    # UPDATE
    # ──────────────────────────────────────────────────────────

    def update(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
        session_vars: Optional[Dict[str, str]] = None,
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
            session_vars=session_vars,
        )
        after_plain = None
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success
            if self._is_sql(meta, engine_override):
                result = adapters.sql.update(meta.table, entity_id, data, session_vars=session_vars)
            else:
                # A scalar entity_id is usually the record's "id" property,
                # not its physical PartitionKey/RowKey — those come from the
                # model's declared pk_field/rk_field (x-metadata partition_key
                # /sort_key), which for most schema models is a different
                # field entirely (e.g. Artifact: pk=project_slug, rk=
                # template_key). Recover both from the patch payload, or —
                # more reliably — from the record loaded just above as
                # `before`, so a plain id-addressed update() still lands on
                # the entity's real physical key instead of a mismatched
                # default partition / the id used as a row key it never had.
                pkey = data.get("PartitionKey") or data.get("partition_key") or data.get("pk")
                if not pkey and meta.pk_field:
                    pkey = data.get(meta.pk_field)
                if not pkey and before:
                    pkey = (
                        before.get("PartitionKey")
                        or before.get("partition_key")
                        or before.get("pk")
                        or before.get("_pk")
                        or (before.get(meta.pk_field) if meta.pk_field else None)
                    )
                rkey = None
                if meta.rk_field and meta.rk_field != "id":
                    rkey = data.get(meta.rk_field) or (
                        before.get(meta.rk_field) if before else None
                    )
                en_id = entity_id
                if pkey:
                    if isinstance(en_id, dict):
                        en_pk = (
                            en_id.get("PartitionKey")
                            or en_id.get("partition_key")
                            or en_id.get("pk")
                        )
                        if not en_pk:
                            en_id["partition_key"] = pkey
                        en_rk = (
                            en_id.get("RowKey")
                            or en_id.get("row_key")
                            or en_id.get("rk")
                            or en_id.get("id")
                        )
                        if not en_rk and rkey:
                            en_id["row_key"] = rkey
                    elif isinstance(en_id, str):
                        en_id = {"partition_key": pkey, "row_key": rkey or entity_id}

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
                try:
                    self._cache.invalidate(name)
                except Exception as _ce:
                    logger.warning("Cache invalidate failed (non-fatal): %s", _ce)
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
        except Exception:
            raise

    # ──────────────────────────────────────────────────────────
    # UPSERT
    # ──────────────────────────────────────────────────────────

    def upsert(
        self,
        model: Union[type, str],
        data: JsonDict,
        *,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
        session_vars: Optional[Dict[str, str]] = None,
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
                result = adapters.sql.upsert(meta.table, data, session_vars=session_vars)
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
                try:
                    self._cache.invalidate(name)
                except Exception as _ce:
                    logger.warning("Cache invalidate failed (non-fatal): %s", _ce)
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
        except Exception:
            raise

    # ──────────────────────────────────────────────────────────
    # DELETE
    # ──────────────────────────────────────────────────────────

    def delete(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None,
        hard: bool = False,
        engine_override: Optional[EngineOverride] = None,
        session_vars: Optional[Dict[str, str]] = None,
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
                session_vars=session_vars,
            )

        adapters = self._adapters_for(model, meta, engine_override)
        before = self.read_one(
            model,
            {"id": entity_id} if not isinstance(entity_id, dict) else entity_id,
            no_cache=True,
            include_deleted=True,
            engine_override=engine_override,
            session_vars=session_vars,
        )
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal success
            if self._is_sql(meta, engine_override):
                result = adapters.sql.delete(meta.table, entity_id, session_vars=session_vars)
            else:
                cls = (
                    model
                    if isinstance(model, type)
                    else type(name, (), {"__polydb__": meta.__dict__})
                )
                # Same physical-key recovery as update() (see there for why a
                # scalar entity_id alone isn't enough once a model's pk_field
                # /rk_field differ from "id").
                en_id = entity_id
                if before:
                    pkey = (
                        before.get("PartitionKey")
                        or before.get("partition_key")
                        or before.get("pk")
                        or before.get("_pk")
                        or (before.get(meta.pk_field) if meta.pk_field else None)
                    )
                    if pkey:
                        rkey = None
                        if meta.rk_field and meta.rk_field != "id":
                            rkey = before.get(meta.rk_field)
                        if isinstance(en_id, dict):
                            en_pk = (
                                en_id.get("PartitionKey")
                                or en_id.get("partition_key")
                                or en_id.get("pk")
                            )
                            if not en_pk:
                                en_id["partition_key"] = pkey
                            en_rk = (
                                en_id.get("RowKey")
                                or en_id.get("row_key")
                                or en_id.get("rk")
                                or en_id.get("id")
                            )
                            if not en_rk and rkey:
                                en_id["row_key"] = rkey
                        else:
                            en_id = {"partition_key": pkey, "row_key": rkey or entity_id}
                result = adapters.nosql.delete(cls, en_id, etag=etag)
            success = True
            if self._enable_cache and self._cache:
                try:
                    self._cache.invalidate(name)
                except Exception as _ce:
                    logger.warning("Cache invalidate failed (non-fatal): %s", _ce)
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
        except Exception:
            raise

    # ──────────────────────────────────────────────────────────
    # QUERY (LINQ-style)
    # ──────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────
    # PAGINATION (legacy simple)
    # ──────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────
    # PAGINATION (generic — order_by, cursor, field projection)
    # ──────────────────────────────────────────────────────────

    def query_paged(
        self,
        model: Union[type, str],
        request: PageRequest,
        *,
        engine_override: Optional[EngineOverride] = None,
    ) -> PageResult:
        """Unified pagination with order_by, opaque cursor, and field projection.

        Routes to the best backend implementation:
        - PostgreSQL: server-side ORDER BY + LIMIT/OFFSET
        - Azure Table (no order_by): native continuation tokens
        - Azure Table (with order_by) / all other NoSQL: in-memory sort + offset cursor
        """
        name = _model_name(model)
        meta = _extract_meta(model)
        adapters = self._adapters_for(model, meta, engine_override)

        if self._is_sql(meta, engine_override):
            return adapters.sql.query_paged(meta.table, request)

        cls = (
            model if isinstance(model, type)
            else type(name, (), {"__polydb__": meta.__dict__})
        )
        return adapters.nosql.query_paged(cls, request)

    # ════════════════════════════════════════════════════════════════════════════════════
    # BLOB STORAGE
    # ═══════════════════════════════════════════════════════════════════════════════════

    def upload_blob(
        self,
        key: str,
        data: bytes,
        *,
        file_name: Optional[str] = None,
        media_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        storage_name: str = "azure",
        container_name: Optional[str] = None,
    ) -> str:
        storage = self._engines[0].cloud_factory.get_object_storage(
            storage_name, container_name=container_name
        )
        return storage.put(
            key=key,
            data=data,
            fileName=file_name or key,
            optimize=True,
            media_type=media_type,
            metadata=metadata or {},
        )

    def download_blob(
        self, key: str, *, storage_name: str = "azure", container_name: Optional[str] = None
    ) -> Optional[bytes]:
        storage = self._engines[0].cloud_factory.get_object_storage(
            storage_name, container_name=container_name
        )
        return storage.get(key)

    def delete_blob(
        self, key: str, *, storage_name: str = "azure", container_name: Optional[str] = None
    ) -> bool:
        storage = self._engines[0].cloud_factory.get_object_storage(
            storage_name, container_name=container_name
        )
        return storage.delete(key)

    def list_blob(
        self, prefix: str = "", *, storage_name: str = "azure", container_name: Optional[str] = None
    ) -> List[str]:
        storage = self._engines[0].cloud_factory.get_object_storage(
            storage_name, container_name=container_name
        )
        return storage.list(prefix)

    # ════════════════════════════════════════════════════════════════════════════════════
    # QUEUE
    # ═══════════════════════════════════════════════════════════════════════════════════

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

    def nack_queue(
        self, ack_id: str, *, queue_name: str = "default", adapter_name: str = "azure_queue"
    ) -> bool:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return queue.nack(ack_id, queue_name)

    def purge_queue(self, *, queue_name: str = "default", adapter_name: str = "azure_queue") -> int:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return queue.purge(queue_name)

    def declare_queue(
        self,
        *,
        queue_name: str = "default",
        durable: bool = True,
        dead_letter_queue: Optional[str] = None,
        adapter_name: str = "azure_queue",
    ) -> bool:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return queue.declare(queue_name, durable=durable, dead_letter_queue=dead_letter_queue)

    def queue_status(
        self, *, queue_name: str = "default", adapter_name: str = "azure_queue"
    ) -> Dict[str, Any]:
        queue = self._engines[0].cloud_factory.get_queue(adapter_name)
        return queue.status(queue_name)

    # ════════════════════════════════════════════════════════════════════════════════════
    # FILE STORAGE
    # ═══════════════════════════════════════════════════════════════════════════════════

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

    # ════════════════════════════════════════════════════════════════════════════════════
    # CACHE
    # ═══════════════════════════════════════════════════════════════════════════════════

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

    # ------------------------------------------------------------------
    # Generic Redis KV -- distinct from set_cache/get_cache/invalidate_cache
    # above (which back the automatic, best-effort query-result cache and
    # degrade silently with no Redis configured). These back a caller's
    # own explicit business logic reached from outside this class (e.g. a
    # manifest workflow step's "redis" integration doing rate limiting or
    # a distributed lock), so a missing/misconfigured cache backend raises
    # CacheError instead of returning a silently-wrong 0/None/False.
    # ------------------------------------------------------------------

    def redis_get(self, key: str, *, namespace: str = "generic") -> Optional[Any]:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        return self._cache.get_raw(namespace, key)

    def redis_set(
        self, key: str, value: Any, *, namespace: str = "generic", ttl: Optional[int] = None
    ) -> bool:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        return self._cache.set_raw(namespace, key, value, ttl)

    def redis_delete(self, key: str, *, namespace: str = "generic") -> None:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        self._cache.delete_key(namespace, key)

    def redis_incr(self, key: str, *, namespace: str = "generic", amount: int = 1) -> int:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        return self._cache.incrby(namespace, key, amount)

    def redis_decr(self, key: str, *, namespace: str = "generic", amount: int = 1) -> int:
        return self.redis_incr(key, namespace=namespace, amount=-amount)

    def redis_exists(self, key: str, *, namespace: str = "generic") -> bool:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        return self._cache.exists_raw(namespace, key)

    def redis_ttl(self, key: str, *, namespace: str = "generic") -> int:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        return self._cache.ttl_raw(namespace, key)

    def redis_expire(self, key: str, ttl: int, *, namespace: str = "generic") -> None:
        from .errors import CacheError

        if not self._cache:
            raise CacheError("No Redis cache backend configured (set REDIS_CACHE_URL/REDIS_URL)")
        self._cache.expire_key(namespace, key, ttl)
