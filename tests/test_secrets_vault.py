"""
tests/test_secrets_vault.py
============================
Integration tests for the cloud-agnostic secrets adapters
(CloudDatabaseFactory.get_secrets), against a real Vault dev server.

Requires VAULT_ADDR / VAULT_TOKEN (e.g. `vault server -dev`); skipped
otherwise rather than failing CI on environments without one running.
The Azure Key Vault / AWS Secrets Manager / GCP Secret Manager adapters
share the same SecretsAdapter interface and dispatch path -- only Vault
is exercised here since it's the one backend that's free and trivial to
run locally for CI, matching the project's "verify against a real
backend, not a mock" convention used by the Postgres/Mongo/etc. tests.
"""
from __future__ import annotations

import os
import uuid

import pytest

from polydb.cloudDatabaseFactory import CloudDatabaseFactory
from polydb.models import CloudProvider


def _require_vault() -> None:
    if not (os.getenv("VAULT_ADDR") and os.getenv("VAULT_TOKEN")):
        pytest.skip("Vault not configured -- set VAULT_ADDR / VAULT_TOKEN")


@pytest.fixture
def secrets():
    _require_vault()
    factory = CloudDatabaseFactory(provider=CloudProvider.VAULT)
    return factory.get_secrets()


@pytest.fixture
def secret_key():
    return f"test/{uuid.uuid4().hex}/api_key"


def test_set_then_get_round_trips(secrets, secret_key):
    secrets.set_secret(secret_key, "sk_test_abc123")
    assert secrets.get_secret(secret_key) == "sk_test_abc123"


def test_get_missing_key_returns_none_not_raise(secrets):
    assert secrets.get_secret(f"test/{uuid.uuid4().hex}/does-not-exist") is None


def test_set_secret_overwrites_existing_value(secrets, secret_key):
    secrets.set_secret(secret_key, "first")
    secrets.set_secret(secret_key, "second")
    assert secrets.get_secret(secret_key) == "second"


def test_delete_secret_removes_it(secrets, secret_key):
    secrets.set_secret(secret_key, "sk_test_abc123")
    assert secrets.delete_secret(secret_key) is True
    assert secrets.get_secret(secret_key) is None


def test_delete_missing_secret_returns_false_or_true_but_never_raises(secrets):
    # Vault's KV v2 delete is idempotent by design (deleting an absent
    # path is not an error) -- assert it doesn't raise, not a specific
    # boolean, since that's a legitimate implementation choice.
    secrets.delete_secret(f"test/{uuid.uuid4().hex}/never-existed")


def test_list_secrets_finds_keys_under_a_prefix(secrets):
    prefix = f"test/{uuid.uuid4().hex}"
    secrets.set_secret(f"{prefix}/stripe_key", "sk_1")
    secrets.set_secret(f"{prefix}/twilio_key", "sk_2")

    names = secrets.list_secrets(prefix)
    assert set(names) == {"stripe_key", "twilio_key"}


def test_factory_caches_the_same_adapter_instance(secrets):
    factory = CloudDatabaseFactory(provider=CloudProvider.VAULT)
    a = factory.get_secrets()
    b = factory.get_secrets()
    assert a is b
