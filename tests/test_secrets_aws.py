"""
tests/test_secrets_aws.py
==========================
Integration tests for AWSSecretsManagerAdapter (via get_secrets()).

Uses moto's mock_aws rather than LocalStack (unlike test_aws.py's other
adapters) -- moto is already a `test` extra dependency, needs no running
service, and Secrets Manager is well-covered by moto's mocks, so this is
the lower-friction choice specifically for this adapter. If LocalStack
becomes the project-wide standard for AWS integration tests, this should
move to match test_aws.py's convention instead of being the odd one out.
"""
from __future__ import annotations

import uuid

import pytest

pytest.importorskip("moto")

from moto import mock_aws

from polydb.cloudDatabaseFactory import CloudDatabaseFactory
from polydb.models import CloudProvider


@pytest.fixture
def secrets():
    with mock_aws():
        factory = CloudDatabaseFactory(provider=CloudProvider.AWS)
        yield factory.get_secrets()


@pytest.fixture
def secret_key():
    return f"test/{uuid.uuid4().hex}/api_key"


def test_set_then_get_round_trips(secrets, secret_key):
    secrets.set_secret(secret_key, "sk_test_abc123")
    assert secrets.get_secret(secret_key) == "sk_test_abc123"


def test_get_missing_key_returns_none_not_raise(secrets):
    assert secrets.get_secret(f"test/{uuid.uuid4().hex}/does-not-exist") is None


def test_set_secret_on_existing_key_updates_value(secrets, secret_key):
    secrets.set_secret(secret_key, "first")
    secrets.set_secret(secret_key, "second")
    assert secrets.get_secret(secret_key) == "second"


def test_delete_secret_removes_it(secrets, secret_key):
    secrets.set_secret(secret_key, "sk_test_abc123")
    assert secrets.delete_secret(secret_key) is True
    assert secrets.get_secret(secret_key) is None


def test_delete_missing_secret_returns_false(secrets):
    assert secrets.delete_secret(f"test/{uuid.uuid4().hex}/never-existed") is False


def test_list_secrets_finds_keys_under_a_prefix(secrets):
    prefix = f"test/{uuid.uuid4().hex}"
    secrets.set_secret(f"{prefix}/stripe_key", "sk_1")
    secrets.set_secret(f"{prefix}/twilio_key", "sk_2")

    names = secrets.list_secrets(prefix)
    assert set(names) == {f"{prefix}/stripe_key", f"{prefix}/twilio_key"}
