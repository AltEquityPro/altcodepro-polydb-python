"""The rule that decides whether legacy records are a migration path or a hole.

Records written before ``POLYDB_AUDIT_HMAC_KEY`` was configured carry an
unkeyed SHA-256 and cannot be re-verified with the key. Accepting them
unconditionally would hand an attacker a downgrade: write an unkeyed record,
have it accepted, forge freely.

``verify_chain`` therefore accepts an unkeyed record only *before* the first
keyed one. That gives two properties, both exercised below:

* nothing unkeyed can be appended after the cutover; and
* the legacy prefix cannot be rewritten either, because rewriting record N
  changes ``hash(N)``, which is ``previous_hash`` inside record N+1's payload
  -- and a keyed N+1 cannot be recomputed without the key.

So configuring a key seals the history that already exists, not only what
comes after.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from polydb.audit.AuditStorage import AuditStorage, ChainVerification  # noqa: E402
from polydb.audit.models import AUDIT_HMAC_KEY_ENV, compute_audit_hash  # noqa: E402

KEY = "an-audit-signing-key"


class _FakeSql:
    """Just enough of the sql surface for verify_chain: it only reads."""

    def __init__(self, rows: List[Dict[str, Any]]):
        self.rows = rows

    def execute(self, *_a, **_k):
        return None

    def query_linq(self, _table, _builder):
        return list(self.rows)


def _storage(rows) -> AuditStorage:
    storage = AuditStorage.__new__(AuditStorage)  # skip the factory in __init__
    storage.sql = _FakeSql(rows)
    return storage


def _row(index: int, previous_hash: str, *, key: Optional[bytes], **overrides):
    row: Dict[str, Any] = {
        "audit_id": f"aud-{index}",
        "timestamp": f"2026-01-0{index + 1}T00:00:00",
        "tenant_id": "t1",
        "actor_id": "u1",
        "roles": [],
        "action": "update",
        "model": "Invoice",
        "entity_id": f"inv-{index}",
        "storage_type": "sql",
        "provider": "postgres",
        "success": True,
        "before": None,
        "after": {"n": index},
        "changed_fields": [],
        "trace_id": None,
        "request_id": None,
        "ip_address": None,
        "user_agent": None,
        "error": None,
        "previous_hash": previous_hash,
    }
    row.update(overrides)
    row["hash"] = compute_audit_hash(row, key=key)
    return row


def _chain(schemes: List[Optional[bytes]]) -> List[Dict[str, Any]]:
    """Build a linked chain, each link hashed under the given scheme."""
    rows: List[Dict[str, Any]] = []
    prev = ""
    for i, key in enumerate(schemes):
        row = _row(i, prev, key=key)
        rows.append(row)
        prev = row["hash"]
    return rows


K = KEY.encode()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(AUDIT_HMAC_KEY_ENV, raising=False)


# ── the ordinary cases ─────────────────────────────────────────────────────


def test_an_all_legacy_chain_still_verifies_without_a_key():
    result = _storage(_chain([None, None, None])).verify_chain_detailed()

    assert result.ok
    assert result.legacy_records == 3


def test_an_all_keyed_chain_verifies_with_the_key(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)

    result = _storage(_chain([K, K, K])).verify_chain_detailed()

    assert result.ok
    assert result.legacy_records == 0


def test_a_legacy_prefix_followed_by_keyed_records_verifies(monkeypatch):
    """The upgrade path: existing history, then the key arrives."""
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)

    result = _storage(_chain([None, None, K, K])).verify_chain_detailed()

    assert result.ok
    assert result.legacy_records == 2  # reported, so the tail is visible


def test_an_empty_log_verifies():
    assert _storage([]).verify_chain_detailed().ok


# ── the attacks ────────────────────────────────────────────────────────────


def test_an_unkeyed_record_appended_after_the_cutover_is_refused(monkeypatch):
    """The downgrade attack. Without the cutover rule this would pass, and
    every protection the key buys would be optional at the attacker's
    discretion."""
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)

    result = _storage(_chain([K, K, None])).verify_chain_detailed()

    assert not result.ok
    assert result.reason == "unkeyed_after_cutover"
    assert result.broken_at == 2


def test_editing_a_keyed_record_is_caught(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    rows = _chain([K, K, K])
    rows[1]["action"] = "read"  # hide what really happened

    result = _storage(rows).verify_chain_detailed()

    assert not result.ok
    assert result.reason == "content"
    assert result.audit_id == "aud-1"


def test_editing_and_rehashing_a_keyed_record_is_caught(monkeypatch):
    """The attacker does the obvious thing: recompute the digest. Without the
    key the recomputation is wrong, and the link to the next record breaks."""
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    rows = _chain([K, K, K])
    rows[1]["action"] = "read"
    rows[1]["hash"] = compute_audit_hash(rows[1], key=None)  # no key available

    result = _storage(rows).verify_chain_detailed()

    assert not result.ok


def test_the_legacy_prefix_cannot_be_rewritten_once_a_keyed_record_exists(
    monkeypatch,
):
    """The property that makes accepting legacy records safe.

    The attacker rewrites a pre-key record and recomputes its unkeyed hash --
    which they can, it needs no secret. But that hash is the next record's
    previous_hash, and the next record is keyed, so they cannot re-link it.
    """
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    rows = _chain([None, None, K])

    rows[1]["before"] = {"amount": 0}  # rewrite history
    rows[1]["hash"] = compute_audit_hash(rows[1], key=None)  # freely recomputed

    result = _storage(rows).verify_chain_detailed()

    assert not result.ok
    assert result.reason == "link"  # record 2 no longer points at record 1
    assert result.broken_at == 2


def test_a_rewritten_legacy_record_is_caught_even_before_any_key_exists():
    """Not the same protection - an attacker could re-link the whole chain
    here - but a partial edit that leaves the links stale is still caught."""
    rows = _chain([None, None, None])
    rows[1]["action"] = "read"

    result = _storage(rows).verify_chain_detailed()

    assert not result.ok
    assert result.reason == "content"


def test_a_deleted_record_breaks_the_link(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    rows = _chain([K, K, K])
    del rows[1]

    result = _storage(rows).verify_chain_detailed()

    assert not result.ok
    assert result.reason == "link"


def test_reordering_breaks_the_link(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    rows = _chain([K, K, K])
    rows[1], rows[2] = rows[2], rows[1]

    assert not _storage(rows).verify_chain_detailed().ok


def test_verifying_a_keyed_chain_without_the_key_fails(monkeypatch):
    """Losing the key means losing the ability to verify, not silently
    downgrading to 'looks fine'."""
    rows = _chain([K, K])
    # env has no key at all

    assert not _storage(rows).verify_chain_detailed().ok


# ── the boolean wrapper stays compatible ───────────────────────────────────


def test_verify_chain_still_returns_a_bool(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)

    assert _storage(_chain([K, K])).verify_chain() is True
    assert _storage(_chain([K, K, None])).verify_chain() is False


def test_the_result_is_a_frozen_dataclass():
    result = ChainVerification(ok=True)

    with pytest.raises(Exception):
        result.ok = False  # type: ignore[misc]
