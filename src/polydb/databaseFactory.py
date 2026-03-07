from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from .batch import BatchOperations
from .cache import CacheWarmer, RedisCacheEngine
from .monitoring import HealthCheck, MetricsCollector, PerformanceMonitor
from .multitenancy import TenantContext, TenantIsolationEnforcer, TenantRegistry
from .security import DataMasking, FieldEncryption, RowLevelSecurity
from .validation import ModelValidator
from .errors import AdapterConfigurationError
from .registry import ModelRegistry
from .types import JsonDict, Lookup, ModelMeta
from .audit.manager import AuditManager
from .audit.context import AuditContext
from .query import Operator, QueryBuilder
from .cloudDatabaseFactory import CloudDatabaseFactory
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_DEFAULT_RETRY = retry(
    wait=wait_exponential(multiplier=0.5, min=0.5, max=6),
    stop=stop_after_attempt(3),
    reraise=True,
)


# ---------------------------------------------------------------------------
# Engine descriptor — one per registered SQL or NoSQL engine
# ---------------------------------------------------------------------------


@dataclass
class EngineConfig:
    """
    Describes a single SQL or NoSQL engine that DatabaseFactory can route to.

    Parameters
    ----------
    name : str
        Logical name for this engine, e.g. "primary", "archive", "analytics".
    cloud_factory : CloudDatabaseFactory
        Fully initialised factory bound to this engine's credentials /
        connection string.
    sql_models : set[str] | None
        Model class names that should be routed to this engine's SQL adapter.
        None means "match nothing by default" (use as fallback only).
    nosql_models : set[str] | None
        Model class names routed to this engine's NoSQL adapter.
    is_default_sql : bool
        If True, this engine's SQL adapter is used for any model not matched
        by an explicit sql_models list in any engine.
    is_default_nosql : bool
        Same concept for NoSQL.
    """

    name: str
    cloud_factory: CloudDatabaseFactory
    sql_models: Optional[Set[str]] = None  # explicit allow-list
    nosql_models: Optional[Set[str]] = None  # explicit allow-list
    is_default_sql: bool = False
    is_default_nosql: bool = False

    # Lazily initialised adapters
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


# ---------------------------------------------------------------------------
# Per-call override — passed into any CRUD method via `engine_override=`
# ---------------------------------------------------------------------------


@dataclass
class EngineOverride:
    """
    Optionally supplied at call-site to bypass routing and target a specific
    engine (and optionally force SQL or NoSQL regardless of model metadata).

    Example
    -------
    db.create(MyModel, data, engine_override=EngineOverride(engine_name="archive"))
    db.read(MyModel, engine_override=EngineOverride(engine_name="analytics", force_sql=True))
    """

    engine_name: str
    force_sql: bool = False
    force_nosql: bool = False


# ---------------------------------------------------------------------------
# Internal resolved pair
# ---------------------------------------------------------------------------


@dataclass
class _ResolvedAdapters:
    sql: Any
    nosql: Any
    engine_name: str


# ---------------------------------------------------------------------------
# DatabaseFactory
# ---------------------------------------------------------------------------


class DatabaseFactory:
    """
    Universal CRUD with:
    - Multi-engine routing (multiple SQL + NoSQL engines simultaneously)
    - Routing by model name (sql_models / nosql_models allow-lists per engine)
    - Per-call engine override via `engine_override=EngineOverride(...)`
    - Cache, soft-delete, audit, multi-tenancy, RLS, encryption, monitoring
    """

    def __init__(
        self,
        *,
        # ── single-engine convenience (backwards-compatible) ──────────────
        provider: Optional[Any] = None,
        cloud_factory: Optional[CloudDatabaseFactory] = None,
        # ── multi-engine ─────────────────────────────────────────────────
        engines: Optional[List[EngineConfig]] = None,
        # ── existing feature flags ────────────────────────────────────────
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
        self._enable_retries = enable_retries
        self._enable_audit = enable_audit
        self._enable_audit_reads = enable_audit_reads
        self._enable_cache = enable_cache
        self._soft_delete = soft_delete

        # ── monitoring ────────────────────────────────────────────────────
        if enable_monitoring:
            self.metrics = MetricsCollector()
            self.health = HealthCheck(self)
        else:
            self.metrics = None
            self.health = None

        # ── Redis cache ───────────────────────────────────────────────────
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

        # ── security ──────────────────────────────────────────────────────
        self.encryption = FieldEncryption() if enable_encryption else None
        self.masking = DataMasking()
        self.rls = RowLevelSecurity() if enable_rls else None

        # ── multi-tenancy ─────────────────────────────────────────────────
        self.tenant_registry = tenant_registry
        self.tenant_enforcer = TenantIsolationEnforcer(tenant_registry) if tenant_registry else None

        self.batch = BatchOperations(self)
        self._audit = AuditManager() if enable_audit else None

        # ── engine registry ───────────────────────────────────────────────
        # Build from the `engines` list if supplied; otherwise fall back to
        # the legacy single-factory path for backwards compatibility.
        self._engines: List[EngineConfig] = []

        if engines:
            self._engines = engines
            # Validate: exactly one default SQL and one default NoSQL
            default_sql = [e for e in engines if e.is_default_sql]
            default_nosql = [e for e in engines if e.is_default_nosql]
            if len(default_sql) > 1:
                raise AdapterConfigurationError("More than one engine marked is_default_sql=True")
            if len(default_nosql) > 1:
                raise AdapterConfigurationError("More than one engine marked is_default_nosql=True")
            if not default_sql:
                logger.warning(
                    "No engine has is_default_sql=True; SQL models without an "
                    "explicit engine mapping will raise AdapterConfigurationError."
                )
            if not default_nosql:
                logger.warning(
                    "No engine has is_default_nosql=True; NoSQL models without "
                    "an explicit engine mapping will raise AdapterConfigurationError."
                )
        else:
            # Legacy single-engine path
            _cf = cloud_factory or CloudDatabaseFactory(provider=provider)
            _engine = EngineConfig(
                name="primary",
                cloud_factory=_cf,
                is_default_sql=True,
                is_default_nosql=True,
            )
            self._engines = [_engine]

        # Convenience: build name → EngineConfig index for O(1) override lookup
        self._engine_by_name: Dict[str, EngineConfig] = {e.name: e for e in self._engines}

        # Expose a primary provider name for audit (first engine)
        self._provider_name = self._engines[0].cloud_factory.provider.value

    # -----------------------------------------------------------------------
    # Engine routing
    # -----------------------------------------------------------------------

    def _resolve_adapters(
        self,
        model_name: str,
        storage: str,  # "sql" or "nosql"
        override: Optional[EngineOverride] = None,
    ) -> _ResolvedAdapters:
        """
        Return (sql_adapter, nosql_adapter, engine_name) for the given model.

        Resolution order
        ----------------
        1. EngineOverride.engine_name  (per-call explicit override)
        2. Explicit sql_models / nosql_models allow-list on any engine
        3. is_default_sql / is_default_nosql fallback
        """
        # 1. Per-call override
        if override:
            engine = self._engine_by_name.get(override.engine_name)
            if engine is None:
                raise AdapterConfigurationError(
                    f"EngineOverride references unknown engine '{override.engine_name}'. "
                    f"Registered engines: {list(self._engine_by_name)}"
                )
            return _ResolvedAdapters(
                sql=engine.sql(),
                nosql=engine.nosql(),
                engine_name=engine.name,
            )

        # 2. Explicit allow-list match
        for engine in self._engines:
            if storage == "sql" and engine.sql_models and model_name in engine.sql_models:
                return _ResolvedAdapters(
                    sql=engine.sql(),
                    nosql=engine.nosql(),
                    engine_name=engine.name,
                )
            if storage == "nosql" and engine.nosql_models and model_name in engine.nosql_models:
                return _ResolvedAdapters(
                    sql=engine.sql(),
                    nosql=engine.nosql(),
                    engine_name=engine.name,
                )

        # 3. Default fallback
        for engine in self._engines:
            if storage == "sql" and engine.is_default_sql:
                return _ResolvedAdapters(
                    sql=engine.sql(),
                    nosql=engine.nosql(),
                    engine_name=engine.name,
                )
            if storage == "nosql" and engine.is_default_nosql:
                return _ResolvedAdapters(
                    sql=engine.sql(),
                    nosql=engine.nosql(),
                    engine_name=engine.name,
                )

        raise AdapterConfigurationError(
            f"No engine found for model='{model_name}' storage='{storage}'. "
            f"Add an explicit mapping or mark an engine as default."
        )

    def _adapters_for(
        self,
        model: Union[type, str],
        meta: ModelMeta,
        override: Optional[EngineOverride] = None,
    ) -> _ResolvedAdapters:
        """Determine storage type from meta, then resolve adapters."""
        model_name = self._model_name(model)
        # EngineOverride can force the storage type
        if override and override.force_sql:
            storage = "sql"
        elif override and override.force_nosql:
            storage = "nosql"
        else:
            storage = "sql" if (meta.storage == "sql" and meta.table) else "nosql"
        return self._resolve_adapters(model_name, storage, override)

    # -----------------------------------------------------------------------
    # Runtime engine registration helpers
    # -----------------------------------------------------------------------

    def register_engine(self, engine: EngineConfig) -> None:
        """
        Add or replace an engine at runtime.

        Useful when connection strings are only known after initialisation
        (e.g. per-tenant sharding resolved from a config store).
        """
        if engine.name in self._engine_by_name:
            logger.info("Replacing existing engine '%s'", engine.name)
            self._engines = [e for e in self._engines if e.name != engine.name]
        self._engines.append(engine)
        self._engine_by_name[engine.name] = engine
        logger.info(
            "Registered engine '%s' (provider=%s)", engine.name, engine.cloud_factory.provider.value
        )

    def unregister_engine(self, name: str) -> None:
        """Remove an engine by name."""
        if name not in self._engine_by_name:
            raise AdapterConfigurationError(f"Engine '{name}' not registered.")
        self._engines = [e for e in self._engines if e.name != name]
        del self._engine_by_name[name]
        logger.info("Unregistered engine '%s'", name)

    def get_engine(self, name: str) -> EngineConfig:
        if name not in self._engine_by_name:
            raise AdapterConfigurationError(
                f"Engine '{name}' not found. Registered: {list(self._engine_by_name)}"
            )
        return self._engine_by_name[name]

    # -----------------------------------------------------------------------
    # Backwards-compatible _sql / _nosql properties (primary engine)
    # -----------------------------------------------------------------------

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

    # -----------------------------------------------------------------------
    # Internal helpers (unchanged from original)
    # -----------------------------------------------------------------------

    def _meta(self, model: Union[type, str]) -> ModelMeta:
        return ModelRegistry.get(model)

    def _model_type(self, model: Union[type, str]) -> type:
        return ModelRegistry.resolve(model)

    def _model_name(self, model: Union[type, str]) -> str:
        return model.__name__ if isinstance(model, type) else str(model)

    def _current_tenant_id(self) -> Optional[str]:
        try:
            tenant = TenantContext.get_tenant()
            if tenant and tenant.tenant_id:
                return tenant.tenant_id
        except Exception:
            pass
        return AuditContext.tenant_id.get()

    def _current_actor_id(self) -> Optional[str]:
        return AuditContext.actor_id.get()

    def _inject_tenant(self, data: JsonDict) -> JsonDict:
        tenant_id = self._current_tenant_id()
        if not tenant_id:
            return data
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
        engine_name: Optional[str] = None,
    ):
        if not self._audit:
            return
        try:
            changed_fields = self._compute_field_changes(before, after)
            provider = engine_name or self._provider_name
            self._audit.record(
                action=action,
                model=self._model_name(model),
                entity_id=str(entity_id) if entity_id else None,
                storage_type=meta.storage,
                provider=provider,
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

    # -----------------------------------------------------------------------
    # CRUD — all methods now accept optional engine_override kwarg
    # -----------------------------------------------------------------------

    def create(
        self,
        model: Union[type, str],
        data: JsonDict,
        *,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        model_type = self._model_type(model)
        meta = self._meta(model)

        # Only enforce validator when model actually declares __polydb__
        if getattr(model_type, "__polydb__", None):
            ModelValidator.validate_and_raise(model_type)

        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()
        actor_id = self._current_actor_id()

        if self.tenant_enforcer:
            data = self.tenant_enforcer.enforce_write(model_name, data)
        if self.rls:
            data = self.rls.enforce_write(model_name, data)

        data = self._inject_tenant(data)
        data = self._inject_audit_fields(data, is_create=True)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, encrypted_fields)

        adapters = self._adapters_for(model, meta, engine_override)

        before = None
        after_plain = None
        success = False
        error: Optional[str] = None
        entity_id: Optional[Any] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success, entity_id

            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                result = adapters.sql.insert(meta.table, data)
            else:
                result = adapters.nosql.put(model_type, data)

            entity_id = result.get("id")
            after_plain = result
            if self.encryption and encrypted_fields:
                after_plain = self.encryption.decrypt_fields(result, encrypted_fields)

            success = True

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
                engine_name=adapters.engine_name,
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
        engine_override: Optional[EngineOverride] = None,
    ) -> List[JsonDict]:
        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()
        actor_id = self._current_actor_id()

        if self._soft_delete and not include_deleted:
            query = self._apply_soft_delete_filter(query)
        if self.tenant_enforcer:
            query = self.tenant_enforcer.enforce_read(model_name, query or {})
        if self.rls:
            query = self.rls.enforce_read(model_name, query or {})
        if tenant_id:
            query = dict(query or {})
            query.setdefault("tenant_id", tenant_id)

        adapters = self._adapters_for(model, meta, engine_override)
        use_external_cache = self._enable_cache and self._cache and getattr(meta, "cache", False)
        encrypted_fields = getattr(meta, "encrypted_fields", [])

        def _op() -> List[JsonDict]:
            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                raw_rows = adapters.sql.select(meta.table, query, limit=limit, offset=offset)
            else:
                model_type = self._model_type(model)
                eff_no_cache = no_cache or use_external_cache
                eff_ttl = cache_ttl if cache_ttl is not None else getattr(meta, "cache_ttl", None)
                raw_rows = adapters.nosql.query(
                    model_type,
                    query=query,
                    limit=limit,
                    no_cache=eff_no_cache,
                    cache_ttl=None if eff_no_cache else eff_ttl,
                )

            if self.encryption and encrypted_fields:
                raw_rows = [self.encryption.decrypt_fields(r, encrypted_fields) for r in raw_rows]
            rows = [
                self.masking.mask(r, model=model_name, actor_id=actor_id, tenant_id=tenant_id)
                for r in raw_rows
            ]

            if use_external_cache and not no_cache:
                ttl = cache_ttl or getattr(meta, "cache_ttl", 300)
                self._cache.set(model_name, query or {}, rows, ttl)  # type: ignore

            return rows

        rows: List[JsonDict] = []
        cached = None
        if use_external_cache and not no_cache:
            cached = self._cache.get(model_name, query or {})  # type: ignore
        if cached is not None:
            rows = cached
        else:
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
                engine_name=adapters.engine_name,
            )

        return rows

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
        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()
        actor_id = self._current_actor_id()

        if self._soft_delete and not include_deleted:
            query = self._apply_soft_delete_filter(query)
        if self.tenant_enforcer:
            query = self.tenant_enforcer.enforce_read(model_name, query or {})
        if self.rls:
            query = self.rls.enforce_read(model_name, query or {})
        if tenant_id:
            query = dict(query or {})
            query.setdefault("tenant_id", tenant_id)

        adapters = self._adapters_for(model, meta, engine_override)
        encrypted_fields = getattr(meta, "encrypted_fields", [])

        def _op() -> Tuple[List[JsonDict], Optional[str]]:
            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                raw_rows, next_token = adapters.sql.select_page(
                    meta.table, query, page_size, continuation_token
                )
            else:
                model_type = self._model_type(model)
                raw_rows, next_token = adapters.nosql.query_page(
                    model_type, query, page_size, continuation_token
                )

            if self.encryption and encrypted_fields:
                raw_rows = [self.encryption.decrypt_fields(r, encrypted_fields) for r in raw_rows]
            rows = [
                self.masking.mask(r, model=model_name, actor_id=actor_id, tenant_id=tenant_id)
                for r in raw_rows
            ]
            return rows, next_token

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

            if self._audit and self._enable_audit_reads and result:
                self._audit_safe(
                    action="read_page",
                    model=model,
                    entity_id=None,
                    meta=meta,
                    success=True,
                    before=None,
                    after={"count": len(result[0])},
                    error=None,
                    engine_name=adapters.engine_name,
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
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
        model_name = self._model_name(model)
        meta = self._meta(model)
        tenant_id = self._current_tenant_id()

        data = self._inject_audit_fields(data, is_create=False)

        if self.tenant_enforcer:
            data = self.tenant_enforcer.enforce_write(model_name, data)
        if self.rls:
            data = self.rls.enforce_write(model_name, data)

        encrypted_fields = getattr(meta, "encrypted_fields", [])
        if self.encryption and encrypted_fields:
            data = self.encryption.encrypt_fields(data, [f for f in encrypted_fields if f in data])

        adapters = self._adapters_for(model, meta, engine_override)
        before = self._fetch_before(
            model, meta, entity_id, etag=etag, engine_override=engine_override
        )
        after_plain = None
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success

            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                if tenant_id and isinstance(entity_id, dict):
                    entity_id.setdefault("tenant_id", tenant_id)
                result = adapters.sql.update(meta.table, entity_id, data)
            else:
                model_type = self._model_type(model)
                result = adapters.nosql.patch(
                    model_type, entity_id, data, etag=etag, replace=replace
                )

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
                engine_name=adapters.engine_name,
            )

    def upsert(
        self,
        model: Union[type, str],
        data: JsonDict,
        *,
        replace: bool = False,
        engine_override: Optional[EngineOverride] = None,
    ) -> JsonDict:
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

        adapters = self._adapters_for(model, meta, engine_override)
        after_plain = None
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal after_plain, success

            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                result = adapters.sql.upsert(meta.table, data)
            else:
                model_type = self._model_type(model)
                result = adapters.nosql.upsert(model_type, data, replace=replace)

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
                engine_name=adapters.engine_name,
            )

    def delete(
        self,
        model: Union[type, str],
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None,
        hard: bool = False,
        engine_override: Optional[EngineOverride] = None,
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
            return self.update(model, entity_id, delete_payload, engine_override=engine_override)

        adapters = self._adapters_for(model, meta, engine_override)
        before = self._fetch_before(
            model, meta, entity_id, etag=etag, engine_override=engine_override
        )
        success = False
        error: Optional[str] = None

        def _op() -> JsonDict:
            nonlocal success

            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                result = adapters.sql.delete(meta.table, entity_id)
            else:
                model_type = self._model_type(model)
                result = adapters.nosql.delete(model_type, entity_id, etag=etag)

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
                engine_name=adapters.engine_name,
            )

    def _fetch_before(
        self,
        model: Union[type, str],
        meta: ModelMeta,
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None,
        engine_override: Optional[EngineOverride] = None,
    ) -> Optional[JsonDict]:
        lookup = {"id": entity_id} if not isinstance(entity_id, dict) else entity_id
        return self.read_one(
            model,
            lookup,
            no_cache=True,
            include_deleted=True,
            engine_override=engine_override,
        )

    def query_linq(
        self,
        model: Union[type, str],
        builder: QueryBuilder,
        *,
        engine_override: Optional[EngineOverride] = None,
    ) -> Union[List[JsonDict], int]:
        model_name = self._model_name(model)
        meta = self._meta(model)
        extra_filter: Lookup = {}

        if self.tenant_enforcer:
            extra_filter = self.tenant_enforcer.enforce_read(model_name, extra_filter)
        if self.rls:
            extra_filter = self.rls.enforce_read(model_name, extra_filter)

        if extra_filter:
            for filter_field, filter_value in extra_filter.items():
                builder = builder.where(filter_field, Operator.EQ, filter_value)

        tenant_id = self._current_tenant_id()
        adapters = self._adapters_for(model, meta, engine_override)

        def _op():
            if (
                meta.storage == "sql"
                and meta.table
                and not (engine_override and engine_override.force_nosql)
            ):
                return adapters.sql.query_linq(meta.table, builder)
            else:
                model_type = self._model_type(model)
                return adapters.nosql.query_linq(model_type, builder)

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
                    engine_name=adapters.engine_name,
                )

            return result
        except Exception:
            raise
