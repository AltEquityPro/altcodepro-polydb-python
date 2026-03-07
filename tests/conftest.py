"""
conftest.py — polydb integration test configuration
=====================================================

CLI options (every value can also come from .env.test or the environment):

  --env-file          Path to a dotenv file          (default: tests/.env.test)
  --pg-url            PostgreSQL connection string
  --mongo-url         MongoDB URI
  --azure-conn        Azure Storage connection string
  --aws-key           AWS access key id
  --aws-secret        AWS secret access key
  --aws-region        AWS region
  --aws-endpoint      AWS endpoint URL (LocalStack)
  --gcp-project       GCP project id
  --firestore-host    FIRESTORE_EMULATOR_HOST
  --pubsub-host       PUBSUB_EMULATOR_HOST
  --gcs-host          STORAGE_EMULATOR_HOST
  --vercel-kv-url     Redis URL used as Vercel KV
  --redis-cache-url   Redis URL used as the cache layer
  --run-all           Run all provider suites (default: only providers whose
                      env vars are present)

Markers:
  postgresql   mongodb   azure   aws   gcp   vercel   multi_engine
  slow         (skipped unless --run-slow)

Usage examples
--------------
# run everything declared in .env.test
pytest tests/ --env-file=tests/.env.test -v

# run only PostgreSQL tests
pytest tests/ -m postgresql --pg-url postgresql://polydb:polydb@localhost:5433/polydb_test

# run multi-engine tests only
pytest tests/ -m multi_engine --env-file=tests/.env.test
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Generator, Optional

import pytest
from dotenv import load_dotenv

from polydb.cloudDatabaseFactory import CloudDatabaseFactory
from polydb.models import CloudProvider

# ── Try to import polydb — if the package is not installed, all tests that need
#    it will be skipped via the fixture rather than collected as errors.
try:
    _POLYDB_AVAILABLE = True
except ImportError:
    _POLYDB_AVAILABLE = False

# ────────────────────────────────────────────────────────────────────────────
# CLI options
# ────────────────────────────────────────────────────────────────────────────

def pytest_addoption(parser: pytest.Parser) -> None:
    grp = parser.getgroup("polydb", "polydb integration test options")

    grp.addoption("--env-file",       default=None,          help="Path to dotenv file")
    grp.addoption("--pg-url",         default=None,          help="PostgreSQL DSN")
    grp.addoption("--mongo-url",      default=None,          help="MongoDB URI")
    grp.addoption("--azure-conn",     default=None,          help="Azure Storage connection string")
    grp.addoption("--aws-key",        default=None,          help="AWS_ACCESS_KEY_ID")
    grp.addoption("--aws-secret",     default=None,          help="AWS_SECRET_ACCESS_KEY")
    grp.addoption("--aws-region",     default="us-east-1",   help="AWS region")
    grp.addoption("--aws-endpoint",   default=None,          help="AWS endpoint URL (LocalStack)")
    grp.addoption("--gcp-project",    default=None,          help="GCP project id")
    grp.addoption("--firestore-host", default=None,          help="FIRESTORE_EMULATOR_HOST")
    grp.addoption("--pubsub-host",    default=None,          help="PUBSUB_EMULATOR_HOST")
    grp.addoption("--gcs-host",       default=None,          help="STORAGE_EMULATOR_HOST")
    grp.addoption("--vercel-kv-url",  default=None,          help="Redis URL acting as Vercel KV")
    grp.addoption("--redis-cache-url",default=None,          help="Redis URL for the cache layer")
    grp.addoption("--run-all",        action="store_true",   help="Force-run all provider suites")
    grp.addoption("--run-slow",       action="store_true",   help="Include slow tests")


# ────────────────────────────────────────────────────────────────────────────
# Configuration object shared across the session
# ────────────────────────────────────────────────────────────────────────────

class TestConfig:
    """Resolved configuration for this test run."""

    def __init__(self, opts: Any) -> None:
        # 1. Load dotenv file (CLI > default path)
        env_file = opts.env_file or os.path.join(
            os.path.dirname(__file__), ".env.test"
        )
        if Path(env_file).exists():
            load_dotenv(env_file, override=False)

        # 2. Apply CLI overrides into os.environ so adapters pick them up
        self._apply("POSTGRES_CONNECTION_STRING", opts.pg_url)
        self._apply("MONGODB_URI",                opts.mongo_url)
        self._apply("AZURE_STORAGE_CONNECTION_STRING", opts.azure_conn)
        self._apply("AWS_ACCESS_KEY_ID",          opts.aws_key)
        self._apply("AWS_SECRET_ACCESS_KEY",      opts.aws_secret)
        self._apply("AWS_DEFAULT_REGION",         opts.aws_region)
        self._apply("AWS_ENDPOINT_URL",           opts.aws_endpoint)
        self._apply("GOOGLE_CLOUD_PROJECT",       opts.gcp_project)
        self._apply("FIRESTORE_EMULATOR_HOST",    opts.firestore_host)
        self._apply("PUBSUB_EMULATOR_HOST",       opts.pubsub_host)
        self._apply("STORAGE_EMULATOR_HOST",      opts.gcs_host)
        self._apply("KV_REST_API_URL",            opts.vercel_kv_url)
        self._apply("REDIS_CACHE_URL",            opts.redis_cache_url)

        # 3. Expose resolved values
        self.pg_url      = os.getenv("POSTGRES_CONNECTION_STRING", "")
        self.mongo_url   = os.getenv("MONGODB_URI", "")
        self.azure_conn  = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
        self.aws_key     = os.getenv("AWS_ACCESS_KEY_ID", "")
        self.gcp_project = os.getenv("GOOGLE_CLOUD_PROJECT", "")
        self.fs_host     = os.getenv("FIRESTORE_EMULATOR_HOST", "")
        self.vercel_kv   = os.getenv("KV_REST_API_URL", "") or os.getenv("VERCEL_ENV", "")
        self.redis_cache = os.getenv("REDIS_CACHE_URL", "")
        self.run_all     = opts.run_all

    @staticmethod
    def _apply(env_key: str, cli_val: Optional[str]) -> None:
        if cli_val:
            os.environ[env_key] = cli_val

    # ── availability helpers ─────────────────────────────────────────────────

    def has_postgres(self)  -> bool: return bool(self.pg_url)
    def has_mongo(self)     -> bool: return bool(self.mongo_url)
    def has_azure(self)     -> bool: return bool(self.azure_conn)
    def has_aws(self)       -> bool: return bool(self.aws_key)
    def has_gcp(self)       -> bool: return bool(self.gcp_project and self.fs_host)
    def has_vercel(self)    -> bool: return bool(self.vercel_kv)
    def has_redis(self)     -> bool: return bool(self.redis_cache)


# ────────────────────────────────────────────────────────────────────────────
# Session-scoped fixture: config
# ────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def test_config(request: pytest.FixtureRequest) -> TestConfig:
    return TestConfig(request.config.option)


# ────────────────────────────────────────────────────────────────────────────
# Skip helpers — used inside fixtures
# ────────────────────────────────────────────────────────────────────────────

def _require_polydb() -> None:
    if not _POLYDB_AVAILABLE:
        pytest.skip("polydb package not importable — ensure PYTHONPATH is set")


def _require(condition: bool, label: str) -> None:
    if not condition:
        pytest.skip(f"{label} not configured — set env vars or pass CLI options")


# ────────────────────────────────────────────────────────────────────────────
# Per-provider CloudDatabaseFactory fixtures
# ────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def pg_factory(test_config: TestConfig):
    _require_polydb()
    _require(test_config.has_postgres(), "PostgreSQL")
    return CloudDatabaseFactory(provider=CloudProvider.POSTGRESQL)


@pytest.fixture(scope="session")
def mongo_factory(test_config: TestConfig):
    _require_polydb()
    _require(test_config.has_mongo(), "MongoDB")
    return CloudDatabaseFactory(provider=CloudProvider.MONGODB)


@pytest.fixture(scope="session")
def azure_factory(test_config: TestConfig):
    _require_polydb()
    _require(test_config.has_azure(), "Azure")
    return CloudDatabaseFactory(provider=CloudProvider.AZURE)


@pytest.fixture(scope="session")
def aws_factory(test_config: TestConfig):
    _require_polydb()
    _require(test_config.has_aws(), "AWS")
    return CloudDatabaseFactory(provider=CloudProvider.AWS)


@pytest.fixture(scope="session")
def gcp_factory(test_config: TestConfig):
    _require_polydb()
    _require(test_config.has_gcp(), "GCP/Firestore")
    return CloudDatabaseFactory(provider=CloudProvider.GCP)


@pytest.fixture(scope="session")
def vercel_factory(test_config: TestConfig):
    _require_polydb()
    _require(test_config.has_vercel(), "Vercel")
    return CloudDatabaseFactory(provider=CloudProvider.VERCEL)


# ────────────────────────────────────────────────────────────────────────────
# Per-provider adapter fixtures  (sql / nosql / object_storage / queue / files)
# ────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def pg_sql(pg_factory):
    return pg_factory.get_sql()


@pytest.fixture(scope="session")
def mongo_nosql(mongo_factory):
    return mongo_factory.get_nosql_kv()


@pytest.fixture(scope="session")
def azure_nosql(azure_factory):
    return azure_factory.get_nosql_kv()


@pytest.fixture(scope="session")
def azure_blob(azure_factory):
    return azure_factory.get_object_storage()


@pytest.fixture(scope="session")
def azure_queue(azure_factory):
    return azure_factory.get_queue()


@pytest.fixture(scope="session")
def azure_files(azure_factory):
    return azure_factory.get_shared_files()


@pytest.fixture(scope="session")
def aws_nosql(aws_factory):
    return aws_factory.get_nosql_kv()


@pytest.fixture(scope="session")
def aws_blob(aws_factory):
    return aws_factory.get_object_storage()


@pytest.fixture(scope="session")
def aws_queue(aws_factory):
    return aws_factory.get_queue()


@pytest.fixture(scope="session")
def gcp_nosql(gcp_factory):
    return gcp_factory.get_nosql_kv()


@pytest.fixture(scope="session")
def gcp_blob(gcp_factory):
    return gcp_factory.get_object_storage()


@pytest.fixture(scope="session")
def gcp_queue(gcp_factory):
    return gcp_factory.get_queue()


@pytest.fixture(scope="session")
def gcp_files(gcp_factory):
    return gcp_factory.get_shared_files()


@pytest.fixture(scope="session")
def vercel_nosql(vercel_factory):
    return vercel_factory.get_nosql_kv()


@pytest.fixture(scope="session")
def vercel_blob(vercel_factory):
    return vercel_factory.get_object_storage()


# ────────────────────────────────────────────────────────────────────────────
# PostgreSQL schema bootstrap
# ────────────────────────────────────────────────────────────────────────────

_POSTGRES_DDL = """
CREATE TABLE IF NOT EXISTS polydb_items (
    id          TEXT PRIMARY KEY,
    name        TEXT,
    value       INTEGER,
    meta        JSONB,
    tags        TEXT[],
    tenant_id   TEXT,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS polydb_archive (
    id          TEXT PRIMARY KEY,
    name        TEXT,
    value       INTEGER,
    tenant_id   TEXT,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);
"""

@pytest.fixture(scope="session", autouse=False)
def pg_schema(pg_sql):
    """Ensure test tables exist in PostgreSQL."""
    pg_sql.execute(_POSTGRES_DDL)
    yield
    # Teardown: drop data but keep structure for speed
    pg_sql.execute("DELETE FROM polydb_items")
    pg_sql.execute("DELETE FROM polydb_archive")


# ────────────────────────────────────────────────────────────────────────────
# Helpers used by tests
# ────────────────────────────────────────────────────────────────────────────

def uid() -> str:
    """Short unique id safe for use as both a PK and a dict key."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def make_item():
    """Factory that returns a fresh dict each call."""
    def _factory(**extra) -> dict:
        return {"id": uid(), "name": f"item-{uid()}", "value": 42, **extra}
    return _factory


# ────────────────────────────────────────────────────────────────────────────
# Mark slow tests
# ────────────────────────────────────────────────────────────────────────────

def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "postgresql:    PostgreSQL-specific tests")
    config.addinivalue_line("markers", "mongodb:       MongoDB-specific tests")
    config.addinivalue_line("markers", "azure:         Azure-specific tests")
    config.addinivalue_line("markers", "aws:           AWS-specific tests")
    config.addinivalue_line("markers", "gcp:           GCP/Firestore-specific tests")
    config.addinivalue_line("markers", "vercel:        Vercel KV tests")
    config.addinivalue_line("markers", "multi_engine:  Multi-engine routing tests")
    config.addinivalue_line("markers", "slow:          Slow integration tests")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if not config.getoption("--run-slow", default=False):
        skip_slow = pytest.mark.skip(reason="pass --run-slow to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)