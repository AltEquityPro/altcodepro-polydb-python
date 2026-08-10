# src/polydb/audit/AuditStorage.py
from __future__ import annotations

import hmac
import threading
from dataclasses import dataclass
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
        from .models import (
            AuditKeyMissingError,
            AUDIT_HMAC_KEY_ENV,
            audit_hmac_key,
            compute_audit_hash,
            require_hmac,
        )

        if require_hmac() and audit_hmac_key() is None:
            raise AuditKeyMissingError(
                f"{AUDIT_HMAC_KEY_ENV} is unset. An unkeyed audit hash can be "
                "recomputed by anyone who can write the log, so this "
                "deployment has asked to refuse writing one."
            )

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
        """Verify BOTH chain linkage AND per-record content integrity."""
        return self.verify_chain_detailed(tenant_id).ok

    def verify_chain_detailed(
        self, tenant_id: Optional[str] = None
    ) -> "ChainVerification":
        """Verify the chain, reporting *why* rather than only whether.

        Two things are checked per record: that it links to its predecessor,
        and that its own digest still matches its content. The second is what
        catches an editor who changed ``before``/``after``/``action`` and left
        ``hash`` alone.

        Records written before ``POLYDB_AUDIT_HMAC_KEY`` was configured carry
        an unkeyed SHA-256 and cannot be re-verified with the key. They are
        accepted, but only *before* the first keyed record in the chain -- and
        that restriction is what keeps it from being a loophole rather than a
        migration path:

        * An attacker cannot append unkeyed records after the cutover, because
          once a keyed record has been seen every later record must be keyed.
        * An attacker cannot rewrite the legacy prefix either. Rewriting record
          N means recomputing ``hash(N)``, which is ``previous_hash`` inside
          record N+1's payload -- and if N+1 is keyed, its digest cannot be
          recomputed without the key.

        So configuring a key seals everything written up to that moment as well
        as everything after it. ``legacy_records`` reports how much of the
        chain is still resting on that seal.
        """
        from ..query import QueryBuilder, Operator
        from .models import audit_hmac_key, compute_audit_hash

        builder = QueryBuilder()
        if tenant_id is not None:
            builder.where("tenant_id", Operator.EQ, tenant_id)
        builder.order_by("timestamp", descending=False)

        records = self.sql.query_linq("polydb_audit_log", builder)
        if not records:
            return ChainVerification(ok=True)

        key = audit_hmac_key()
        prev = ""
        legacy = 0
        seen_keyed = False

        for index, r in enumerate(records):
            audit_id = r.get("audit_id")

            if (r.get("previous_hash") or "") != prev:
                return ChainVerification(
                    ok=False,
                    broken_at=index,
                    audit_id=audit_id,
                    reason="link",
                    legacy_records=legacy,
                )

            stored = r.get("hash") or ""

            if key is not None and hmac.compare_digest(
                stored, compute_audit_hash(r, key=key)
            ):
                seen_keyed = True
            elif not seen_keyed and hmac.compare_digest(
                stored, compute_audit_hash(r, key=None)
            ):
                # Predates the key. Sealed by the first keyed record that
                # follows it, so it is only trusted while none has appeared.
                legacy += 1
            else:
                # Distinguish the two ways this arrives. If the *unkeyed*
                # digest matches the record as stored, the content is intact
                # and the record simply was not keyed - which after the
                # cutover is what stripping the key looks like. Otherwise the
                # record itself was edited.
                downgraded = seen_keyed and hmac.compare_digest(
                    stored, compute_audit_hash(r, key=None)
                )
                return ChainVerification(
                    ok=False,
                    broken_at=index,
                    audit_id=audit_id,
                    reason="unkeyed_after_cutover" if downgraded else "content",
                    legacy_records=legacy,
                )

            prev = stored

        return ChainVerification(ok=True, legacy_records=legacy)


@dataclass(frozen=True)
class ChainVerification:
    """The outcome of :meth:`AuditStorage.verify_chain_detailed`."""

    ok: bool
    broken_at: Optional[int] = None
    audit_id: Optional[str] = None
    #: "link" (previous_hash mismatch), "content" (record edited), or
    #: "unkeyed_after_cutover" (an unkeyed record appended after keying began,
    #: which is what stripping the key would look like).
    reason: Optional[str] = None
    #: How many leading records still carry a pre-key unkeyed digest.
    legacy_records: int = 0
