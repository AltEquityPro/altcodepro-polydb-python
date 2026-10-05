"""`fields` / `omit` on read: SQL names the columns, Azure Table sends `$select`, every other NoSQL adapter trims after the read.

Unit level (no Azurite / Postgres): fake table client, fake cursor.
"""

import pytest

from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter, project_rows, select_columns


# ---------------- pure helpers -------------------------------------------------------------------------------------------

def test_project_rows_fields_omit_and_fields_wins():
    rows = [{"id": "1", "a": 1, "b": 2, "big": "x" * 10}]
    assert project_rows(rows, ["id", "a"], None) == [{"id": "1", "a": 1}]
    assert project_rows(rows, None, ["big"]) == [{"id": "1", "a": 1, "b": 2}]
    assert project_rows(rows, ["a"], ["a"]) == [{"a": 1}]
    assert project_rows(rows, None, None) is rows


def test_select_columns_from_fields_or_columns_minus_omit():
    cols = ["id", "a", "b", "big"]
    assert select_columns(["a"], None, cols) == ["a"]
    assert select_columns(None, ["big"], cols) == ["id", "a", "b"]
    assert select_columns(None, ["big"], None) is None  # columns unknown: cannot derive a list, fetch whole rows
    assert select_columns(None, None, cols) is None


# ---------------- base adapter: pushdown flag ----------------------------------------------------------------------------

class _Adapter(NoSQLKVAdapter):
    def __init__(self, pushdown):
        super().__init__()
        self.SUPPORTS_SELECT_PUSHDOWN = pushdown
        self.calls = []
    def _query_raw(self, model, filters, limit, select=None):
        self.calls.append(select)
        return [{"id": "1", "a": 1, "b": 2, "big": "zzz"}]


class _M:
    __polydb__ = {"columns": ["id", "a", "b", "big"]}


def test_without_pushdown_rows_are_trimmed_after_the_read():
    a = _Adapter(False)
    assert a.query(_M, {"x": 1}, fields=["id", "a"]) == [{"id": "1", "a": 1}]
    assert a.calls == [None]  # the store was asked for whole rows


def test_with_pushdown_the_select_list_reaches_the_store_and_result_is_still_exact():
    a = _Adapter(True)
    assert a.query(_M, {}, fields=["a"]) == [{"a": 1}]
    assert a.query(_M, {}, omit=["big"]) == [{"id": "1", "a": 1, "b": 2}]
    assert a.calls == [["a"], ["id", "a", "b"]]


def test_omit_on_a_model_without_known_columns_is_client_side_only():
    class _Bare: pass
    a = _Adapter(True)
    assert a.query(_Bare, {}, omit=["big"]) == [{"id": "1", "a": 1, "b": 2}]
    assert a.calls == [None]


def test_no_projection_keeps_the_old_call_shape():
    a = _Adapter(True)
    assert a.query(_M, {}) == [{"id": "1", "a": 1, "b": 2, "big": "zzz"}]
    assert a.calls == [None]


# ---------------- Azure Table: $select -----------------------------------------------------------------------------------

azure = pytest.importorskip("azure.data.tables")
from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "Artifact"
    __qualname__ = "Artifact"
    __polydb__ = {"pk_field": "tenant_id", "rk_field": "id"}


class _Client:
    def __init__(self, entities):
        self.entities, self.kwargs = entities, []
    def query_entities(self, query_filter=None, **kw):
        self.kwargs.append((query_filter, kw)); return list(self.entities)


ENT = {"PartitionKey": "t1", "RowKey": "r1", "__polydb_model__": "Artifact", "__keymap__": "{}", "id": "r1",
       "name": "n", "status": "ok"}


def _adp(client):
    adp = object.__new__(AzureTableStorageAdapter)
    adp._get_table_client = lambda model: client
    return adp


def test_azure_sends_select_with_the_keys_the_row_needs():
    c = _Client([ENT])
    rows = _adp(c).query(_Model, {"tenant_id": "t1"}, fields=["name", "status"])
    (flt, kw), = c.kwargs
    assert flt == "tenant_id eq 't1'"
    assert set(kw["select"]) == {"name", "status", "PartitionKey", "RowKey", "__polydb_model__", "__keymap__"}
    assert rows == [{"name": "n", "status": "ok"}]  # id / tenant_id added by unpack are trimmed back out


def test_azure_omit_becomes_select_of_the_remaining_known_columns():
    class _M2(_Model):
        __polydb__ = {"pk_field": "tenant_id", "rk_field": "id", "columns": ["id", "name", "status", "content"]}
    c = _Client([ENT])
    rows = _adp(c).query(_M2, {}, omit=["content"])
    (_, kw), = c.kwargs
    assert "content" not in kw["select"] and {"id", "name", "status"} <= set(kw["select"])
    assert rows == [{"id": "r1", "name": "n", "status": "ok", "tenant_id": "t1"}]  # omit drops only what was named


def test_azure_no_projection_sends_no_select():
    c = _Client([ENT])
    _adp(c).query(_Model, {})
    assert c.kwargs == [(None, {})]


def test_azure_falls_back_to_whole_rows_when_a_name_would_be_rewritten():
    c = _Client([dict(ENT, f_1x="v")])
    _adp(c).query(_Model, {}, fields=["1x"])  # "1x" is stored as "f_1x": cannot be selected by its own name
    assert c.kwargs == [(None, {})]


# ---------------- Postgres: named columns --------------------------------------------------------------------------------

from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter  # noqa: E402


class _Cur:
    description = [("id",), ("status",)]
    def fetchall(self): return [("1", "ok")]
    def close(self): pass
class _Conn:
    def cursor(self): return _Cur()
    def commit(self): pass
    def rollback(self): pass


def _pg(captured):
    a = object.__new__(PostgreSQLAdapter)
    a._get_connection = lambda: _Conn()
    a._return_connection = lambda c: None
    a._apply_session_vars = lambda c, v: None
    a._timed_execute = lambda cur, sql, params, **k: captured.append((sql, params))
    a._serialize_params = lambda p: p
    a._deserialize_row = lambda r: r
    return a


def test_postgres_select_names_the_columns():
    cap = []
    rows = _pg(cap).select("orders", {"tenant_id": "t"}, limit=5, fields=["id", "status"])
    assert cap[0][0].startswith("SELECT id, status FROM orders WHERE tenant_id = %s")
    assert rows == [{"id": "1", "status": "ok"}]


def test_postgres_select_without_fields_is_unchanged_and_bad_names_are_refused():
    cap = []
    _pg(cap).select("orders", {})
    assert cap[0][0] == "SELECT * FROM orders"
    with pytest.raises(Exception):
        _pg([]).select("orders", {}, fields=["id; DROP TABLE orders"])
