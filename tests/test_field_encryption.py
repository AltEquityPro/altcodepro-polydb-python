"""
tests/test_field_encryption.py
===============================
Unit tests for polydb.security.FieldEncryption: fail-closed key handling,
round-trip encryption, legacy-ciphertext compatibility, and key rotation.

No live database or cloud SDKs required.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Stub out cloud / optional SDK imports so the module import chain succeeds
# in environments without the full dependency set (mirrors
# test_atomic_decrement.py's stubbing approach).
# ---------------------------------------------------------------------------
for _mod in [
    "google", "google.api_core", "google.api_core.exceptions",
    "google.cloud", "google.cloud.pubsub_v1", "google.cloud.storage",
    "google.cloud.firestore", "google.cloud.bigquery",
    "azure", "azure.storage", "azure.storage.blob", "azure.storage.queue",
    "azure.storage.file", "azure.data", "azure.data.tables",
    "boto3", "botocore", "botocore.exceptions",
    "redis", "pymongo",
    "varint", "baseconv",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)

_gcp_exc = sys.modules.get("google.api_core.exceptions") or types.ModuleType("google.api_core.exceptions")
if not hasattr(_gcp_exc, "AlreadyExists"):
    _gcp_exc.AlreadyExists = type("AlreadyExists", (Exception,), {})
    _gcp_exc.NotFound = type("NotFound", (Exception,), {})
sys.modules["google.api_core.exceptions"] = _gcp_exc

cryptography = pytest.importorskip("cryptography")

from polydb.errors import EncryptionConfigError
from polydb.security import FieldEncryption

ENV_KEYS = (
    "POLYDB_ENCRYPTION_KEY",
    "POLYDB_ENCRYPTION_KEYS",
    "POLYDB_ENCRYPTION_KEY_ID",
)


@pytest.fixture(autouse=True)
def _clean_env():
    saved = {k: os.environ.pop(k, None) for k in ENV_KEYS}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _gen_key() -> str:
    return base64.b64encode(os.urandom(32)).decode()


def test_missing_key_fails_closed():
    """Constructing FieldEncryption with no key configured must raise, not
    silently generate an ephemeral in-memory key."""
    with pytest.raises(EncryptionConfigError):
        FieldEncryption()


def test_invalid_key_length_fails_closed():
    os.environ["POLYDB_ENCRYPTION_KEY"] = base64.b64encode(os.urandom(16)).decode()
    with pytest.raises(EncryptionConfigError):
        FieldEncryption()


def test_round_trip_and_key_id_tag():
    os.environ["POLYDB_ENCRYPTION_KEY"] = _gen_key()
    fe = FieldEncryption()

    data = {"ssn": "123-45-6789", "age": 30}
    enc = fe.encrypt_fields(data, ["ssn"])

    assert enc["ssn"] != data["ssn"]
    assert enc["ssn"].startswith("encrypted:v1:")
    assert enc["age"] == 30  # untouched field

    dec = fe.decrypt_fields(enc, ["ssn"])
    assert dec["ssn"] == "123-45-6789"


def test_non_string_value_round_trips_via_json():
    os.environ["POLYDB_ENCRYPTION_KEY"] = _gen_key()
    fe = FieldEncryption()

    data = {"meta": {"a": 1, "b": [1, 2, 3]}}
    enc = fe.encrypt_fields(data, ["meta"])
    dec = fe.decrypt_fields(enc, ["meta"])
    assert dec["meta"] == {"a": 1, "b": [1, 2, 3]}


def test_legacy_ciphertext_without_key_id_still_decrypts():
    """Values encrypted before key-versioning was introduced (format
    'encrypted:<blob>', no key-id segment) must still decrypt under the
    active key."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key_b64 = _gen_key()
    os.environ["POLYDB_ENCRYPTION_KEY"] = key_b64
    fe = FieldEncryption()

    key_bytes = base64.b64decode(key_b64)
    aesgcm = AESGCM(key_bytes)
    nonce = os.urandom(12)
    ct = aesgcm.encrypt(nonce, b"legacy-plaintext", None)
    legacy_blob = "encrypted:" + base64.b64encode(nonce + ct).decode()

    dec = fe.decrypt_fields({"x": legacy_blob}, ["x"])
    assert dec["x"] == "legacy-plaintext"


def test_key_rotation_old_ciphertext_still_decryptable():
    key_v1 = _gen_key()
    os.environ["POLYDB_ENCRYPTION_KEY"] = key_v1
    fe_v1 = FieldEncryption()
    old_enc = fe_v1.encrypt_fields({"ssn": "111-22-3333"}, ["ssn"])
    assert old_enc["ssn"].startswith("encrypted:v1:")

    # Rotate: v1 retired, v2 active
    key_v2 = _gen_key()
    os.environ["POLYDB_ENCRYPTION_KEYS"] = json.dumps({"v1": key_v1})
    os.environ["POLYDB_ENCRYPTION_KEY"] = key_v2
    os.environ["POLYDB_ENCRYPTION_KEY_ID"] = "v2"
    fe_v2 = FieldEncryption()

    # Old data encrypted under the retired key still decrypts.
    dec = fe_v2.decrypt_fields(old_enc, ["ssn"])
    assert dec["ssn"] == "111-22-3333"

    # New encryption uses the new active key id.
    new_enc = fe_v2.encrypt_fields({"ssn": "444-55-6666"}, ["ssn"])
    assert new_enc["ssn"].startswith("encrypted:v2:")


def test_missing_retired_key_raises_instead_of_corrupting_data():
    key_v1 = _gen_key()
    os.environ["POLYDB_ENCRYPTION_KEY"] = key_v1
    fe_v1 = FieldEncryption()
    old_enc = fe_v1.encrypt_fields({"ssn": "999-88-7777"}, ["ssn"])

    # Rotate without carrying the old key forward in POLYDB_ENCRYPTION_KEYS.
    os.environ["POLYDB_ENCRYPTION_KEY"] = _gen_key()
    os.environ["POLYDB_ENCRYPTION_KEY_ID"] = "v2"
    fe_v2 = FieldEncryption()

    with pytest.raises(EncryptionConfigError):
        fe_v2.decrypt_fields(old_enc, ["ssn"])


def test_none_values_are_passed_through():
    os.environ["POLYDB_ENCRYPTION_KEY"] = _gen_key()
    fe = FieldEncryption()
    data = {"ssn": None}
    enc = fe.encrypt_fields(data, ["ssn"])
    assert enc["ssn"] is None
