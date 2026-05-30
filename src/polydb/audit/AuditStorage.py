# src/polydb/audit/AuditStorage.py
from __future__ import annotations

import threading
from typing import Optional, Dict, Any

from .models import AuditRecord
from ..cloudDatabaseFactory import CloudDatabaseFactory


class AuditStorage:
    """Audit log with distributed-safe hash chaining"""

    _lock = threading.Lock()

    def __init__(self):
        self.factory = CloudDatabaseFactory()
        self.sql = self.factory.get_sql()
        self._ensure_table()

    @staticmethod
    def _is_unique_violation(exc: Exception) -> bool:
        s = str(exc).lower()
        return "23505" in s or "duplicate key" in s or "unique constraint" in s

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
                previous_hash VARCHAR(64) NOT NULL DEFAULT '',
                CONSTRAINT uq_audit_chain UNIQUE (tenant_id, previous_hash),
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
                    builder.where("tenant_id", Operator.EQ, tenant_id)

                builder.order_by("timestamp", descending=True).take(1)

                results = self.sql.query_linq("polydb_audit_log", builder)

                if results and len(results) > 0:
                    return results[0].get("hash")

                return None
            except Exception:
                return None

    def persist(self, record: AuditRecord) -> None:
        """Append to the hash chain. Concurrency-safe ACROSS PROCESSES via the
        UNIQUE(tenant_id, previous_hash) constraint + bounded retry: if two
        writers race on the same predecessor, the loser re-reads the new tail
        and re-chains instead of forking. (threading.Lock alone was only
        process-local — the old "distributed-safe" claim was false.)"""
        from dataclasses import asdict
        from .models import compute_audit_hash

        last_err: Optional[Exception] = None
        for _ in range(8):
            with self._lock:
                prev = self.get_last_hash(record.tenant_id) or ""
                record.previous_hash = prev
                record.hash = compute_audit_hash(asdict(record))
                row = {
                    "audit_id": record.audit_id,
                    "timestamp": record.timestamp,
                    "tenant_id": record.tenant_id,
                    "actor_id": record.actor_id,
                    "roles": record.roles or None,
                    "action": record.action,
                    "model": record.model,
                    "entity_id": record.entity_id,
                    "storage_type": record.storage_type,
                    "provider": record.provider,
                    "success": record.success,
                    "before": record.before,
                    "after": record.after,
                    "changed_fields": record.changed_fields or None,
                    "trace_id": record.trace_id,
                    "request_id": record.request_id,
                    "ip_address": record.ip_address,
                    "user_agent": record.user_agent,
                    "error": record.error,
                    "hash": record.hash,
                    "previous_hash": record.previous_hash,
                }
                try:
                    self.sql.insert("polydb_audit_log", row)
                    return
                except Exception as e:
                    if self._is_unique_violation(e):
                        last_err = e
                        continue
                    raise
        raise last_err or RuntimeError("audit persist failed after retries")

    def verify_chain(self, tenant_id: Optional[str] = None) -> bool:
        """Verify BOTH chain linkage AND per-record content integrity.
        The old version only checked previous_hash linkage, so editing
        before/after/action while leaving `hash` intact passed silently."""
        from ..query import QueryBuilder, Operator
        from .models import compute_audit_hash

        builder = QueryBuilder()
        if tenant_id is not None:
            builder.where("tenant_id", Operator.EQ, tenant_id)
        builder.order_by("timestamp", descending=False)

        records = self.sql.query_linq("polydb_audit_log", builder)
        if not records:
            return True

        prev = ""
        for r in records:
            if (r.get("previous_hash") or "") != prev:
                return False
            if r.get("hash") != compute_audit_hash(r):  # content tamper check
                return False
            prev = r.get("hash")
        return True
