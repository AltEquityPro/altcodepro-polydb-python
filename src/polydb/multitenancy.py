# src/polydb/multitenancy.py
"""
Multi-tenancy enforcement and isolation
"""
from typing import Dict, Any, List, Optional, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum


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