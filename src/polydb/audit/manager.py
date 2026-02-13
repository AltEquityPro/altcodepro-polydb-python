# src/polydb/audit/manager.py
from __future__ import annotations

from typing import Optional, Dict, Any, List

from .models import AuditRecord
from .AuditStorage import AuditStorage
from .context import AuditContext


class AuditManager:
    def __init__(self):
        self.storage = AuditStorage()

    def record(
        self,
        *,
        action: str,
        model: str,
        entity_id: Optional[str],
        storage_type: str,
        provider: str,
        success: bool,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        error: Optional[str],
        changed_fields: List[str] | None
    ) -> None:
        tenant_id = AuditContext.tenant_id.get()
        previous_hash = self.storage.get_last_hash(tenant_id)

        record = AuditRecord.create(
            action=action,
            model=model,
            entity_id=entity_id,
            storage_type=storage_type,
            provider=provider,
            success=success,
            before=before,
            after=after,
            changed_fields=changed_fields,
            error=error,
            context=AuditContext,
            previous_hash=previous_hash,
        )

        self.storage.persist(record)
