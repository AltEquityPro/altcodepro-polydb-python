"""
tests/test_cloud_factory.py
===========================
Tests for CloudDatabaseFactory auto-detection and adapter vending.

Covers:
  - Provider auto-detection from environment variables (all 7 providers)
  - Explicit CLOUD_PROVIDER env override
  - Invalid CLOUD_PROVIDER falls through to detection
  - get_nosql_kv() returns correct adapter type per provider
  - get_object_storage() returns correct adapter type per provider
  - get_queue() returns correct adapter type per provider
  - get_shared_files() returns correct adapter type per provider
  - get_sql() always returns PostgreSQLAdapter
  - Multiple factories with different providers are independent
  - Factory detection priority order
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter
from polydb.cloudDatabaseFactory import CloudDatabaseFactory
from polydb.models import CloudProvider


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


def _factory(env: dict):
    """Build a CloudDatabaseFactory with a controlled environment."""

    with patch.dict(os.environ, env, clear=False):
        return CloudDatabaseFactory()


def _factory_explicit(provider_value: str):
    """Build with an explicit CloudProvider enum value."""

    return CloudDatabaseFactory(provider=CloudProvider(provider_value))


# ────────────────────────────────────────────────────────────────────────────
# Provider auto-detection
# ────────────────────────────────────────────────────────────────────────────


class TestProviderDetection:
    def _clean_env(self) -> dict:
        """Environment stripped of all provider hint variables."""
        return {
            k: ""
            for k in [
                "CLOUD_PROVIDER",
                "AZURE_STORAGE_CONNECTION_STRING",
                "AWS_ACCESS_KEY_ID",
                "GOOGLE_CLOUD_PROJECT",
                "VERCEL_ENV",
                "MONGODB_URI",
                "POSTGRES_URL",
                "POSTGRES_CONNECTION_STRING",
            ]
        }

    def _detect(self, extra: dict):

        env = {**self._clean_env(), **extra}
        with patch.dict(os.environ, env, clear=False):
            return CloudDatabaseFactory()

    def test_detects_azure(self):

        cf = self._detect({"AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpoints..."})
        assert cf.provider == CloudProvider.AZURE

    def test_detects_aws(self):

        cf = self._detect({"AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE"})
        assert cf.provider == CloudProvider.AWS

    def test_detects_gcp(self):

        cf = self._detect({"GOOGLE_CLOUD_PROJECT": "my-project"})
        assert cf.provider == CloudProvider.GCP

    def test_detects_vercel(self):

        cf = self._detect({"VERCEL_ENV": "production"})
        assert cf.provider == CloudProvider.VERCEL

    def test_detects_mongodb(self):

        cf = self._detect({"MONGODB_URI": "mongodb://localhost/test"})
        assert cf.provider == CloudProvider.MONGODB

    def test_detects_postgresql_via_url(self):

        cf = self._detect({"POSTGRES_URL": "postgresql://localhost/test"})
        assert cf.provider == CloudProvider.POSTGRESQL

    def test_detects_postgresql_via_conn_string(self):

        cf = self._detect({"POSTGRES_CONNECTION_STRING": "postgresql://localhost/test"})
        assert cf.provider == CloudProvider.POSTGRESQL

    def test_defaults_to_postgresql_when_nothing_set(self):

        cf = self._detect({})
        assert cf.provider == CloudProvider.POSTGRESQL

    def test_cloud_provider_env_overrides_detection(self):

        env = {
            **self._clean_env(),
            "CLOUD_PROVIDER": "gcp",
            "AWS_ACCESS_KEY_ID": "key",  # would normally detect AWS
        }

        with patch.dict(os.environ, env, clear=False):
            cf = CloudDatabaseFactory()
        assert cf.provider == CloudProvider.GCP

    def test_invalid_cloud_provider_env_falls_through(self):

        env = {
            **self._clean_env(),
            "CLOUD_PROVIDER": "unicorn",  # invalid
            "MONGODB_URI": "mongodb://localhost",  # should detect this
        }

        with patch.dict(os.environ, env, clear=False):
            cf = CloudDatabaseFactory()
        assert cf.provider == CloudProvider.MONGODB

    def test_detection_priority_azure_before_aws(self):
        """Azure is listed first in detection rules → wins if both present."""

        cf = self._detect(
            {
                "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpoints...",
                "AWS_ACCESS_KEY_ID": "key",
            }
        )
        assert cf.provider == CloudProvider.AZURE

    def test_explicit_provider_ignores_env(self):

        # Even with AWS key in env, explicit provider wins
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "key"}):
            cf = CloudDatabaseFactory(provider=CloudProvider.GCP)
        assert cf.provider == CloudProvider.GCP


# ────────────────────────────────────────────────────────────────────────────
# Adapter type checks — verify correct class is returned per provider
# ────────────────────────────────────────────────────────────────────────────


class TestAdapterTypes:
    """
    These tests import the adapter classes by module path and verify isinstance.
    If an adapter isn't installed (e.g. azure-data-tables not present), the test
    is skipped automatically.
    """

    def _skip_if_import_fails(self, *module_paths: str) -> None:
        for path in module_paths:
            parts = path.rsplit(".", 1)
            try:
                mod = __import__(parts[0], fromlist=[parts[1]])
                getattr(mod, parts[1])
            except (ImportError, AttributeError):
                pytest.skip(f"Dependency not available: {path}")

    # ── get_nosql_kv ─────────────────────────────────────────────────────────

    def test_azure_nosql_kv_type(self, azure_factory):
        self._skip_if_import_fails("azure.data.tables.TableServiceClient")
        adapter = azure_factory.get_nosql_kv()
        assert "AzureTable" in type(adapter).__name__

    def test_aws_nosql_kv_type(self, aws_factory):
        adapter = aws_factory.get_nosql_kv()
        assert "DynamoDB" in type(adapter).__name__

    def test_gcp_nosql_kv_type(self, gcp_factory):
        adapter = gcp_factory.get_nosql_kv()
        assert "Firestore" in type(adapter).__name__

    def test_vercel_nosql_kv_type(self, vercel_factory):
        adapter = vercel_factory.get_nosql_kv()
        assert "Vercel" in type(adapter).__name__

    def test_mongodb_nosql_kv_type(self, mongo_factory):
        adapter = mongo_factory.get_nosql_kv()
        assert "MongoDB" in type(adapter).__name__ or "Mongo" in type(adapter).__name__

    # ── get_object_storage ────────────────────────────────────────────────────

    def test_azure_object_storage_type(self, azure_factory):
        adapter = azure_factory.get_object_storage()
        assert "Blob" in type(adapter).__name__ or "Azure" in type(adapter).__name__

    def test_aws_object_storage_type(self, aws_factory):
        adapter = aws_factory.get_object_storage()
        assert "S3" in type(adapter).__name__

    def test_gcp_object_storage_type(self, gcp_factory):
        adapter = gcp_factory.get_object_storage()
        assert "GCP" in type(adapter).__name__ or "Storage" in type(adapter).__name__

    def test_vercel_object_storage_type(self, vercel_factory):
        adapter = vercel_factory.get_object_storage()
        assert "Blob" in type(adapter).__name__ or "Vercel" in type(adapter).__name__

    def test_pg_object_storage_type(self, pg_factory):
        """PostgreSQL provider falls through to S3Compatible."""
        adapter = pg_factory.get_object_storage()
        assert "S3Compatible" in type(adapter).__name__ or "S3" in type(adapter).__name__

    # ── get_queue ─────────────────────────────────────────────────────────────

    def test_azure_queue_type(self, azure_factory):
        adapter = azure_factory.get_queue()
        assert "Queue" in type(adapter).__name__

    def test_aws_queue_type(self, aws_factory):
        adapter = aws_factory.get_queue()
        assert "SQS" in type(adapter).__name__

    def test_gcp_queue_type(self, gcp_factory):
        adapter = gcp_factory.get_queue()
        assert "PubSub" in type(adapter).__name__ or "Pub" in type(adapter).__name__

    def test_vercel_queue_type(self, vercel_factory):
        adapter = vercel_factory.get_queue()
        assert "Queue" in type(adapter).__name__ or "Vercel" in type(adapter).__name__

    def test_pg_queue_returns_none_or_raises(self, pg_factory):
        """get_queue() on Postgres/Mongo providers has no implementation."""
        # Should either return None or raise; must not return a wrong type
        try:
            result = pg_factory.get_queue()
            assert result is None or "Queue" in type(result).__name__
        except (NotImplementedError, AttributeError):
            pass  # acceptable

    # ── get_shared_files ─────────────────────────────────────────────────────

    def test_aws_shared_files_type(self, aws_factory):
        adapter = aws_factory.get_shared_files()
        assert "EFS" in type(adapter).__name__ or "S3" in type(adapter).__name__

    def test_gcp_shared_files_type(self, gcp_factory):
        adapter = gcp_factory.get_shared_files()
        assert adapter is not None

    def test_pg_shared_files_returns_none(self, pg_factory):
        result = pg_factory.get_shared_files()
        assert result is None

    # ── get_sql always returns PostgreSQL ─────────────────────────────────────

    def test_all_providers_return_postgresql_adapter(
        self, pg_factory, mongo_factory, azure_factory, aws_factory, gcp_factory
    ):

        for factory in [pg_factory, mongo_factory, azure_factory, aws_factory, gcp_factory]:
            adapter = factory.get_sql()
            assert isinstance(adapter, PostgreSQLAdapter)


# ────────────────────────────────────────────────────────────────────────────
# Independence of multiple factory instances
# ────────────────────────────────────────────────────────────────────────────


class TestFactoryIndependence:
    def test_two_factories_have_separate_connections(self, pg_factory, mongo_factory):
        """Different factories must not share adapter instances."""
        sql_a = pg_factory.get_sql()
        sql_b = pg_factory.get_sql()  # same factory — same instance (lazy)
        assert sql_a is sql_b  # lazy init returns same object

        sql_c = mongo_factory.get_sql()
        # mongo_factory builds a separate PostgreSQLAdapter with its own pool
        assert sql_a is not sql_c

    def test_factory_provider_value_logged_correctly(self, pg_factory, mongo_factory):

        assert pg_factory.provider == CloudProvider.POSTGRESQL
        assert mongo_factory.provider == CloudProvider.MONGODB
