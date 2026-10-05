"""DatabaseFactory wiring: encrypted_fields reach the factory, writes are audited, transient failures retry, and the
read-cache key covers the window and session variables. Unit level: no database, fake adapters."""
import types

import pytest

from polydb.databaseFactory import DatabaseFactory, _extract_meta
from polydb.security import FieldEncryption


class _Sql:
    def __init__(self):
        self.rows, self.fail_times, self.calls = {}, 0, 0

    def insert(self, table, data, session_vars=None):
        self.calls += 1
        if self.fail_times:
            self.fail_times -= 1
            raise TimeoutError("connection timed out")
        self.rows[data["id"]] = dict(data)
        return dict(data)

    def select(self, table, query=None, limit=None, offset=None, session_vars=None, fields=None):
        self.calls += 1
        return [dict(r) for r in self.rows.values()]

    def update(self, table, entity_id, data, session_vars=None):
        eid = entity_id["id"] if isinstance(entity_id, dict) else entity_id
        self.rows[eid].update(data)
        return dict(self.rows[eid])

    def delete(self, table, entity_id, session_vars=None):
        self.rows.pop(entity_id["id"] if isinstance(entity_id, dict) else entity_id, None)
        return {"deleted": True}


class _Cache:
    def __init__(self):
        self.store, self.keys = {}, []

    def get(self, model, query):
        self.keys.append(query)
        return self.store.get((model, repr(sorted(query.items()))))

    def set(self, model, query, value, ttl=None):
        self.store[(model, repr(sorted(query.items())))] = value

    def invalidate(self, model, query=None):
        self.store.clear()


class _Audit:
    def __init__(self):
        self.records = []

    def record(self, **kw):
        self.records.append(kw)


def _factory(sql, *, audit=None, cache=None, encryption=None, retries=True):
    f = object.__new__(DatabaseFactory)
    f._enable_retries, f._enable_audit, f._audit = retries, audit is not None, audit
    f._enable_cache, f._cache, f._soft_delete = cache is not None, cache, False
    f.encryption, f.metrics = encryption, None
    f._adapters_for = lambda model, meta, override=None: types.SimpleNamespace(sql=sql, nosql=None)
    return f


class Note:
    __polydb__ = {"storage": "sql", "table": "notes", "encrypted_fields": ["secret"], "cache": True, "cache_ttl": 60}


def test_encrypted_fields_reach_the_model_metadata():
    assert _extract_meta(Note).encrypted_fields == ("secret",)
    assert _extract_meta(type("X", (), {"__polydb__": {"storage": "sql", "table": "t"}})).encrypted_fields == ()


def test_encryption_runs_for_a_model_that_declares_encrypted_fields(monkeypatch):
    monkeypatch.setenv("POLYDB_ENCRYPTION_KEY", __import__("base64").b64encode(b"k" * 32).decode())
    sql = _Sql()
    try:
        enc = FieldEncryption()
    except Exception:
        pytest.skip("FieldEncryption needs a configured key in this environment")
    f = _factory(sql, encryption=enc)
    out = f.create(Note, {"id": "1", "secret": "plain"})
    assert sql.rows["1"]["secret"] != "plain"      # stored encrypted
    assert out["secret"] == "plain"                # returned decrypted


def test_writes_are_audited_and_encrypted_values_are_masked():
    sql, audit = _Sql(), _Audit()
    f = _factory(sql, audit=audit)
    f.create(Note, {"id": "1", "secret": "plain", "title": "a"})
    kinds = [r["action"] for r in audit.records]
    assert kinds == ["create"] and audit.records[0]["success"] is True
    assert audit.records[0]["after"]["secret"] == "[encrypted]" and audit.records[0]["after"]["title"] == "a"


def test_an_audit_failure_never_breaks_the_write():
    class Boom:
        def record(self, **kw):
            raise RuntimeError("audit store down")
    sql = _Sql()
    assert _factory(sql, audit=Boom()).create(Note, {"id": "1"})["id"] == "1"


def test_transient_errors_are_retried_and_constraint_errors_are_not(monkeypatch):
    import polydb.databaseFactory as df
    monkeypatch.setattr(df, "_TRANSIENT_RETRY", lambda fn: __import__("tenacity").retry(
        retry=df.retry_if_exception(df._is_transient), stop=df.stop_after_attempt(3), reraise=True)(fn))
    sql = _Sql()
    sql.fail_times = 2
    _factory(sql).create(Note, {"id": "1"})
    assert sql.calls == 3

    calls = []
    f = _factory(sql)

    def op():
        calls.append(1)
        raise ValueError('duplicate key value violates unique constraint "x"')
    with pytest.raises(ValueError):
        f._run(op)
    assert len(calls) == 1


def test_retries_can_be_turned_off():
    f = _factory(_Sql(), retries=False)
    calls = []

    def op():
        calls.append(1)
        raise TimeoutError("timeout")
    with pytest.raises(TimeoutError):
        f._run(op)
    assert len(calls) == 1


def test_the_read_cache_key_covers_session_variables_and_the_window():
    sql, cache = _Sql(), _Cache()
    sql.rows["1"] = {"id": "1"}
    f = _factory(sql, cache=cache)
    f.read(Note, {"x": 1}, session_vars={"app.tenant_id": "a"})
    f.read(Note, {"x": 1}, session_vars={"app.tenant_id": "b"})   # must NOT be served tenant a's entry
    f.read(Note, {"x": 1}, session_vars={"app.tenant_id": "a"}, limit=5)
    assert sql.calls == 3
    f.read(Note, {"x": 1}, session_vars={"app.tenant_id": "a"})   # same caller, same query: a hit
    assert sql.calls == 3


# ---------------- reported adapter findings ----------------------------------------------------------------------------

def test_nosql_query_page_advances_instead_of_repeating_the_first_page():
    from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter

    class A(NoSQLKVAdapter):
        def _query_raw(self, model, filters, limit, select=None):
            rows = [{"id": str(i)} for i in range(10)]
            return rows[:limit] if limit else rows

    a = A()
    first, token = a.query_page(object, {}, 4)
    second, token2 = a.query_page(object, {}, 4, token)
    third, token3 = a.query_page(object, {}, 4, token2)
    assert [r["id"] for r in first] == ["0", "1", "2", "3"]
    assert [r["id"] for r in second] == ["4", "5", "6", "7"]
    assert [r["id"] for r in third] == ["8", "9"] and token3 is None


def test_dynamodb_record_over_the_limit_is_not_replaced_by_a_stub():
    from polydb.adapters.DynamoDBAdapter import DynamoDBAdapter

    data = {"id": "1", "blob": "x" * 500_000}
    store, key = DynamoDBAdapter._check_overflow(object.__new__(DynamoDBAdapter), data)
    assert store is data and key is None


def test_soft_deleted_rows_are_hidden_on_nosql_even_when_the_attribute_was_never_written():
    class _NoSql:
        def query(self, cls, query=None, limit=None, no_cache=False, **kw):
            return [{"id": "1"}, {"id": "2", "deleted_at": "2026-01-01"}, {"id": "3", "deleted_at": None}]

    class Doc:
        __polydb__ = {"storage": "nosql"}

    f = _factory(None)
    f._soft_delete = True
    f._adapters_for = lambda model, meta, override=None: types.SimpleNamespace(sql=None, nosql=_NoSql())
    assert [r["id"] for r in f.read(Doc, {})] == ["1", "3"]
    assert len(f.read(Doc, {}, include_deleted=True)) == 3


def test_nosql_read_honours_offset():
    seen = {}

    class _NoSql:
        def query(self, cls, query=None, limit=None, no_cache=False, **kw):
            seen["limit"] = limit
            return [{"id": str(i)} for i in range(10)][:limit]

    class Doc:
        __polydb__ = {"storage": "nosql"}

    f = _factory(None)
    f._adapters_for = lambda model, meta, override=None: types.SimpleNamespace(sql=None, nosql=_NoSql())
    out = f.read(Doc, {}, limit=3, offset=4)
    assert [r["id"] for r in out] == ["4", "5", "6"] and seen["limit"] == 7


def test_edited_migration_ddl_is_reapplied_and_unchanged_ddl_is_not():
    import hashlib

    from polydb.schema import MigrationManager

    class _S:
        def __init__(self):
            self.sql_log, self.stored = [], {}

        def execute(self, sql, params=None, fetch_one=False, **kw):
            self.sql_log.append(sql)
            if sql.startswith("SELECT version"):
                v = params[0]
                return {"version": v, "checksum": self.stored[v]} if v in self.stored else None
            if sql.startswith("UPDATE polydb_migrations"):
                self.stored[params[1]] = params[0]
            return None

        def insert(self, table, data):
            self.stored[data["version"]] = data["checksum"]

    s = _S()
    m = object.__new__(MigrationManager)
    m.sql = s
    assert m.apply_migration("v1", "n", "CREATE TABLE a (x int);") is True
    assert m.apply_migration("v1", "n", "CREATE TABLE a (x int);") is False                 # unchanged
    assert m.apply_migration("v1", "n", "CREATE TABLE a (x int); ALTER TABLE a ADD COLUMN IF NOT EXISTS y int;") is True
    assert s.stored["v1"] == hashlib.sha256(b"CREATE TABLE a (x int); ALTER TABLE a ADD COLUMN IF NOT EXISTS y int;").hexdigest()


def test_schema_builder_emits_add_column_statements_for_non_key_columns():
    from polydb.schema import Column, ColumnType, SchemaBuilder

    b = SchemaBuilder()
    b.add_column(Column(name="id", type=ColumnType.VARCHAR, nullable=False, primary_key=True))
    b.add_column(Column(name="title", type=ColumnType.TEXT, nullable=False))
    assert b.to_add_missing_columns("notes") == ["ALTER TABLE notes ADD COLUMN IF NOT EXISTS title TEXT;"]
