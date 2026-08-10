"""The audit log's integrity hash was unkeyed.

A plain SHA-256 over the record detects accidental corruption, and it detects
an editor who forgets to recompute the hash. It does not detect the adversary
an audit log exists for. SHA-256 needs no secret, so anyone who can write
``polydb_audit_log`` can rewrite a record, recompute its hash, and recompute
every hash after it -- the chain still verifies and the tampering is invisible.
"Can write the log" implied "can forge the log".

``POLYDB_AUDIT_HMAC_KEY`` makes the digest an HMAC. The output is still 64 hex
characters, so it fits the existing VARCHAR(64) columns with no migration.

The interesting design question is what happens to records written before the
key existed. They cannot be re-verified with it, but accepting them
unconditionally would hand an attacker a downgrade: write an unkeyed record,
have it accepted. The cutover rule below is what closes that.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from polydb.audit.models import (  # noqa: E402
    AUDIT_HMAC_KEY_ENV,
    AUDIT_REQUIRE_HMAC_ENV,
    audit_hmac_key,
    canonical_audit_payload,
    compute_audit_hash,
    require_hmac,
)

KEY = "an-audit-signing-key"


def _record(**overrides):
    base = {
        "audit_id": "aud-1",
        "timestamp": "2026-01-01T00:00:00",
        "tenant_id": "t1",
        "actor_id": "u1",
        "roles": ["admin"],
        "action": "delete",
        "model": "Invoice",
        "entity_id": "inv-9",
        "storage_type": "sql",
        "provider": "postgres",
        "success": True,
        "before": {"amount": 100},
        "after": None,
        "changed_fields": [],
        "trace_id": "tr-1",
        "request_id": "rq-1",
        "ip_address": "1.2.3.4",
        "user_agent": "curl",
        "error": None,
        "previous_hash": "",
    }
    base.update(overrides)
    return base


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(AUDIT_HMAC_KEY_ENV, raising=False)
    monkeypatch.delenv(AUDIT_REQUIRE_HMAC_ENV, raising=False)


# ── the forgery the old scheme allowed ─────────────────────────────────────


def test_an_unkeyed_hash_can_be_forged_by_anyone(monkeypatch):
    """Demonstrates the actual weakness rather than asserting a code shape.

    An attacker with write access edits the record and recomputes the digest
    from public information alone -- no secret involved -- and verification
    would accept it.
    """
    tampered = _record(action="read", before=None)  # hide a deletion

    forged = hashlib.sha256(canonical_audit_payload(tampered).encode()).hexdigest()

    assert forged == compute_audit_hash(tampered)


def test_a_keyed_hash_cannot_be_forged_without_the_key(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    tampered = _record(action="read", before=None)

    # The attacker's best effort with public information only.
    forged = hashlib.sha256(canonical_audit_payload(tampered).encode()).hexdigest()

    assert forged != compute_audit_hash(tampered)


def test_the_wrong_key_does_not_verify(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    record = _record()
    genuine = compute_audit_hash(record)

    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, "not-the-key")

    assert compute_audit_hash(record) != genuine


# ── the digest still fits the existing schema ──────────────────────────────


def test_the_digest_is_still_64_hex_characters(monkeypatch):
    """hash and previous_hash are VARCHAR(64); a longer digest would need a
    migration, and a truncated one would quietly weaken the chain."""
    unkeyed = compute_audit_hash(_record())
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)
    keyed = compute_audit_hash(_record())

    for digest in (unkeyed, keyed):
        assert len(digest) == 64
        int(digest, 16)  # hex


def test_content_changes_still_change_the_digest(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)

    assert compute_audit_hash(_record()) != compute_audit_hash(
        _record(action="read")
    )
    assert compute_audit_hash(_record()) != compute_audit_hash(
        _record(previous_hash="deadbeef")
    )


# ── key handling ───────────────────────────────────────────────────────────


def test_no_key_falls_back_to_the_legacy_digest():
    """Upgrading must not break a deployment that has not set a key yet -
    audit writes sit in the data write path, so hard-failing by default would
    take out every write on a rolling upgrade."""
    assert audit_hmac_key() is None
    assert compute_audit_hash(_record()) == hashlib.sha256(
        canonical_audit_payload(_record()).encode()
    ).hexdigest()


def test_a_blank_key_counts_as_no_key(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, "   ")

    assert audit_hmac_key() is None


def test_the_key_is_read_per_call_so_rotation_needs_no_restart(monkeypatch):
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, "first")
    first = compute_audit_hash(_record())

    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, "second")

    assert compute_audit_hash(_record()) != first


@pytest.mark.parametrize("value,expected", [("1", True), ("true", True),
                                            ("yes", True), ("0", False),
                                            ("", False)])
def test_the_require_flag_is_explicit(monkeypatch, value, expected):
    monkeypatch.setenv(AUDIT_REQUIRE_HMAC_ENV, value)

    assert require_hmac() is expected


def test_an_explicit_key_argument_overrides_the_environment(monkeypatch):
    """Verification needs to compute the other scheme's digest deliberately."""
    monkeypatch.setenv(AUDIT_HMAC_KEY_ENV, KEY)

    assert compute_audit_hash(_record(), key=None) == hashlib.sha256(
        canonical_audit_payload(_record()).encode()
    ).hexdigest()


def test_the_canonical_payload_excludes_the_hash_itself():
    """Otherwise the digest would have to cover its own output."""
    payload = json.loads(canonical_audit_payload(_record(hash="x")))

    assert "hash" not in payload
    assert payload["previous_hash"] == ""
