# src/polydb/multitenancy.py
"""
Multi-tenancy enforcement and isolation
"""
import re
from typing import Dict, Any, List, Optional, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum

from .errors import ValidationError

# Schema/database names get interpolated into DDL, which cannot be
# parameterised - a bind parameter is a *value*, and these are identifiers.
# So they need the same allowlist the SQL adapter already applies to every
# table and column name via utils.validate_table_name. These DDL paths were
# the one place that skipped it.
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
#: Longest identifier PostgreSQL accepts.
_MAX_IDENTIFIER_LENGTH = 63


def _validate_sql_identifier(value: str, *, kind: str) -> str:
    """Allowlist a schema/database name before it is spliced into DDL.

    Rejects anything that is not a bare identifier, so a tenant-derived name
    such as ``x; DROP SCHEMA public CASCADE; --`` can never reach the database.
    """
    if not isinstance(value, str) or not value:
        raise ValidationError(f"Invalid {kind}: {value!r} (must be a non-empty string)")
    if len(value) > _MAX_IDENTIFIER_LENGTH:
        raise ValidationError(
            f"Invalid {kind}: {value!r} exceeds {_MAX_IDENTIFIER_LENGTH} characters"
        )
    if not _SQL_IDENTIFIER_RE.match(value):
        raise ValidationError(
            f"Invalid {kind}: {value!r}. Only letters, digits and underscores are "
            "allowed, and it may not start with a digit."
        )
    return value


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
            schema = _validate_sql_identifier(config.schema_name, kind="schema_name")
            schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            self.factory._sql.execute(schema_sql)
        
        elif config.isolation_level == IsolationLevel.SEPARATE_DATABASE:
            # Create database (requires superuser)
            database = _validate_sql_identifier(
                config.database_name, kind="database_name"
            )
            db_sql = f"CREATE DATABASE {database};"
            self.factory._sql.execute(db_sql)
    
    def deprovision_tenant(self, tenant_id: str):
        """Deprovision tenant"""
        config = self.registry.get(tenant_id)
        if not config:
            return
        
        if config.isolation_level == IsolationLevel.SEPARATE_SCHEMA:
            # Drop schema
            schema = _validate_sql_identifier(config.schema_name, kind="schema_name")
            schema_sql = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
            self.factory._sql.execute(schema_sql)
        
        elif config.isolation_level == IsolationLevel.SEPARATE_DATABASE:
            # Drop database
            database = _validate_sql_identifier(
                config.database_name, kind="database_name"
            )
            db_sql = f"DROP DATABASE IF EXISTS {database};"
            self.factory._sql.execute(db_sql)