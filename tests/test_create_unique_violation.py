import pytest

from polydb.base.constraints import UniqueConstraintError
from polydb.databaseFactory import DatabaseFactory


class _Sql:
    def __init__(self, msg):
        self.msg, self.updated = msg, False

    def insert(self, table, data, session_vars=None):
        raise Exception(self.msg)

    def update(self, *a, **k):
        self.updated = True
        return {"id": "1"}


class M:
    __polydb__ = {"storage": "sql", "table": "users"}


def _factory(msg):
    f = DatabaseFactory.__new__(DatabaseFactory)
    sql = _Sql(msg)
    f._inject_audit_fields = lambda d, is_create: d
    f.encryption = None
    f._enable_cache = False
    f._cache = None
    f.metrics = None
    f._audit_write = lambda *a, **k: None
    f._run = lambda fn: fn()
    f._is_sql = lambda *a, **k: True
    f._adapters_for = lambda *a, **k: type("A", (), {"sql": sql})()
    return f, sql


def test_non_key_unique_violation_raises():
    f, sql = _factory("duplicate key value violates unique constraint\nKey (email)=(a@b.c) already exists")
    with pytest.raises(UniqueConstraintError):
        f.create(M, {"email": "a@b.c", "id": "2"})
    assert not sql.updated


def test_primary_key_conflict_still_overwrites():
    f, sql = _factory("duplicate key value violates unique constraint\nKey (id, tenant_id)=(1, t) already exists")
    f.create(M, {"id": "1", "tenant_id": "t", "x": 1})
    assert sql.updated
