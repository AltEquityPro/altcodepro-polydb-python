# src/polydb/audit/AuditStorage.py
from __future__ import annotations

import threading
from typing import Optional, Dict, Any

from .models import AuditRecord
from ..factory import CloudDatabaseFactory


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
                from ..query import QueryBuilder, Operator
                
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
        from ..query import QueryBuilder, Operator
        
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