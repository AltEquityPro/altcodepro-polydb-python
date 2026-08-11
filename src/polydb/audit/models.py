# src/polydb/audit/models.py
"""The audit record and its integrity hash.

The chain used a plain SHA-256 over the record's canonical payload. That
detects accidental corruption and it detects an editor who forgets to
recompute the hash - but it does not detect the adversary an audit log exists
for. Anyone who can write the table can also recompute SHA-256, so they can
rewrite a record, re-hash it, and re-hash every record after it: the chain
still verifies, and the tampering is invisible. The digest was unkeyed, so
"can write the log" implied "can forge the log".

``POLYDB_AUDIT_HMAC_KEY`` turns the digest into HMAC-SHA256. The output is
still 64 hex characters, so it fits the existing ``VARCHAR(64)`` columns and
needs no migration - only the way the value is derived changes.

Records written before a key was configured stay verifiable. See
``AuditStorage.verify_chain`` for the cutover rule that makes that safe rather
than a loophole.
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from ..json_safe import json_safe

logger = logging.getLogger("polydb.audit")

#: Env var holding the audit HMAC key. Absent means legacy unkeyed hashing.
AUDIT_HMAC_KEY_ENV = "POLYDB_AUDIT_HMAC_KEY"

#: Set to 1/true/yes to refuse to write audit records without a key, for
#: deployments that would rather fail than keep a forgeable log.
AUDIT_REQUIRE_HMAC_ENV = "POLYDB_AUDIT_REQUIRE_HMAC"

_warned_no_key = False

#: Distinguishes "caller did not specify a key" from "caller specified None",
#: which verification needs in order to check the legacy variant deliberately.
_UNSET = object()


class AuditKeyMissingError(RuntimeError):
    """No audit HMAC key, and the deployment asked to require one."""


def audit_hmac_key() -> Optional[bytes]:
    """The configured audit key, or ``None``.

    Read per call rather than cached so a key rotated into the environment
    takes effect without a restart, and so tests can set it per case.
    """
    raw = os.getenv(AUDIT_HMAC_KEY_ENV, "").strip()
    return raw.encode("utf-8") if raw else None


def require_hmac() -> bool:
    return os.getenv(AUDIT_REQUIRE_HMAC_ENV, "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _iso(ts: Any) -> str:
    return ts.isoformat() if hasattr(ts, "isoformat") else str(ts)


def canonical_audit_payload(src: Dict[str, Any]) -> str:
    """Deterministic JSON for the hash chain. Identical whether `src` is a
    freshly-built record (asdict) or a row read back from Postgres, so the
    create-time hash and the verify-time recomputed hash match.
    Normalizes [] vs NULL and timestamp formatting; excludes `hash`."""
    payload = {
        "audit_id": src.get("audit_id"),
        "timestamp": _iso(src.get("timestamp")),
        "tenant_id": src.get("tenant_id"),
        "actor_id": src.get("actor_id"),
        "roles": list(src.get("roles") or []),
        "action": src.get("action"),
        "model": src.get("model"),
        "entity_id": src.get("entity_id"),
        "storage_type": src.get("storage_type"),
        "provider": src.get("provider"),
        "success": bool(src.get("success")),
        "before": src.get("before"),
        "after": src.get("after"),
        "changed_fields": list(src.get("changed_fields") or []),
        "trace_id": src.get("trace_id"),
        "request_id": src.get("request_id"),
        "ip_address": src.get("ip_address"),
        "user_agent": src.get("user_agent"),
        "error": src.get("error"),
        "previous_hash": src.get("previous_hash") or "",
    }
    return json.dumps(payload, sort_keys=True, default=json_safe)


def compute_audit_hash(
    src: Dict[str, Any], *, key: Optional[bytes] = _UNSET  # type: ignore[assignment]
) -> str:
    """The record's integrity digest, keyed when a key is available.

    ``key`` defaults to whatever the environment provides; pass it explicitly
    (including ``None``) to compute a specific variant, which is what
    verification does when it has to check a record written under the other
    scheme.
    """
    if key is _UNSET:
        key = audit_hmac_key()
        if key is None:
            _warn_unkeyed_once()

    payload = canonical_audit_payload(src).encode()
    if key is None:
        return hashlib.sha256(payload).hexdigest()
    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def _warn_unkeyed_once() -> None:
    global _warned_no_key
    if _warned_no_key:
        return
    _warned_no_key = True
    logger.warning(
        "Audit records are being hashed without a key (%s is unset). The "
        "chain still detects accidental corruption, but anyone who can write "
        "polydb_audit_log can recompute it, so it does not detect deliberate "
        "tampering. Set %s to seal the log; set %s=1 to refuse to write "
        "without one.",
        AUDIT_HMAC_KEY_ENV,
        AUDIT_HMAC_KEY_ENV,
        AUDIT_REQUIRE_HMAC_ENV,
    )


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
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
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

        record.hash = compute_audit_hash(asdict(record))

        return record
