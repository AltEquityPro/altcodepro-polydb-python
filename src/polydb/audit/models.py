# src/polydb/audit/models.py

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional
import uuid
import hashlib
import json

from src.polydb.json_safe import json_safe


@dataclass
class AuditRecord:
    audit_id: str
    timestamp: str
    tenant_id: Optional[str]
    actor_id: Optional[str]
    roles: List[str]
    action: str
    model: str
    entity_id: Optional[str]
    storage_type: str
    provider: str
    success: bool

    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    changed_fields: Optional[List[str]]

    trace_id: Optional[str]
    request_id: Optional[str]
    ip_address: Optional[str]
    user_agent: Optional[str]

    error: Optional[str]

    hash: Optional[str] = None
    previous_hash: Optional[str] = None

    @classmethod
    def create(
        cls,
        *,
        action: str,
        model: str,
        entity_id: Optional[str],
        storage_type: str,
        provider: str,
        success: bool,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        changed_fields: Optional[List[str]],
        error: Optional[str],
        context,
        previous_hash: Optional[str] = None,
    ):
        now = datetime.utcnow().isoformat()
        audit_id = str(uuid.uuid4())

        record = cls(
            audit_id=audit_id,
            timestamp=now,
            tenant_id=context.tenant_id.get(),
            actor_id=context.actor_id.get(),
            roles=context.roles.get(),
            action=action,
            model=model,
            entity_id=entity_id,
            storage_type=storage_type,
            provider=provider,
            success=success,
            before=before,
            after=after,
            changed_fields=changed_fields,
            trace_id=context.trace_id.get(),
            request_id=context.request_id.get(),
            ip_address=context.ip_address.get(),
            user_agent=context.user_agent.get(),
            error=error,
            previous_hash=previous_hash,
        )

        record.hash = hashlib.sha256(
            json.dumps(asdict(record), sort_keys=True,default=json_safe).encode()
        ).hexdigest()

        return record
