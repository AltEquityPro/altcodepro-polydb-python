"""Tenant provisioning must not splice unvalidated names into DDL.

provision_tenant / deprovision_tenant interpolated ``config.schema_name`` and
``config.database_name`` straight into ``CREATE SCHEMA``, ``CREATE DATABASE``,
``DROP SCHEMA ... CASCADE`` and ``DROP DATABASE`` with no validation at all --
bypassing the allowlist (``validate_table_name``) that the SQL adapter applies
to every other identifier. An onboarding flow deriving those names from
tenant-supplied input could therefore drop another tenant's schema.

Identifiers cannot be parameterised, so the fix is an allowlist.
"""

from __future__ import annotations

import pytest

from polydb.errors import ValidationError
from polydb.multitenancy import (
    IsolationLevel,
    TenantConfig,
    TenantMigrationManager,
    TenantRegistry,
)


class FakeSQL:
    def __init__(self):
        self.executed: list[str] = []

    def execute(self, sql, *args, **kwargs):
        self.executed.append(sql)


class FakeFactory:
    def __init__(self):
        self._sql = FakeSQL()


def _manager():
    factory = FakeFactory()
    return factory, TenantMigrationManager(factory, TenantRegistry())


INJECTIONS = [
    "x; DROP SCHEMA public CASCADE; --",
    "public; DROP DATABASE prod; --",
    'a" OR 1=1 --',
    "a'; --",
    "schema name",          # whitespace
    "1abc",                 # leading digit
    "sch-ema",              # hyphen is not valid unquoted
    "",                     # empty
    "a" * 80,               # over the identifier length limit
]


class TestProvisionRejectsInjection:
    @pytest.mark.parametrize("payload", INJECTIONS)
    def test_schema_name_is_rejected(self, payload):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1",
            isolation_level=IsolationLevel.SEPARATE_SCHEMA,
            schema_name=payload,
        )
        with pytest.raises(ValidationError):
            mgr.provision_tenant(cfg)
        # and crucially, nothing reached the database
        assert factory._sql.executed == []

    @pytest.mark.parametrize("payload", INJECTIONS)
    def test_database_name_is_rejected(self, payload):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1",
            isolation_level=IsolationLevel.SEPARATE_DATABASE,
            database_name=payload,
        )
        with pytest.raises(ValidationError):
            mgr.provision_tenant(cfg)
        assert factory._sql.executed == []


class TestDeprovisionRejectsInjection:
    def test_drop_schema_payload_is_rejected(self):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1",
            isolation_level=IsolationLevel.SEPARATE_SCHEMA,
            schema_name="ok_schema",
        )
        mgr.registry.register(cfg)
        # mutate post-registration, mimicking a poisoned stored config
        cfg.schema_name = "x; DROP SCHEMA public CASCADE; --"
        with pytest.raises(ValidationError):
            mgr.deprovision_tenant("t1")
        assert factory._sql.executed == []

    def test_drop_database_payload_is_rejected(self):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1",
            isolation_level=IsolationLevel.SEPARATE_DATABASE,
            database_name="ok_db",
        )
        mgr.registry.register(cfg)
        cfg.database_name = "prod; DROP DATABASE other; --"
        with pytest.raises(ValidationError):
            mgr.deprovision_tenant("t1")
        assert factory._sql.executed == []


class TestLegitimateNamesStillWork:
    def test_schema_provision_and_deprovision(self):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1",
            isolation_level=IsolationLevel.SEPARATE_SCHEMA,
            schema_name="tenant_acme",
        )
        mgr.provision_tenant(cfg)
        assert factory._sql.executed == [
            "CREATE SCHEMA IF NOT EXISTS tenant_acme;"
        ]

        mgr.deprovision_tenant("t1")
        assert factory._sql.executed[-1] == (
            "DROP SCHEMA IF EXISTS tenant_acme CASCADE;"
        )

    def test_database_provision(self):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1",
            isolation_level=IsolationLevel.SEPARATE_DATABASE,
            database_name="tenant_acme_db",
        )
        mgr.provision_tenant(cfg)
        assert factory._sql.executed == ["CREATE DATABASE tenant_acme_db;"]

    def test_shared_schema_issues_no_ddl(self):
        factory, mgr = _manager()
        cfg = TenantConfig(
            tenant_id="t1", isolation_level=IsolationLevel.SHARED_SCHEMA
        )
        mgr.provision_tenant(cfg)
        assert factory._sql.executed == []
