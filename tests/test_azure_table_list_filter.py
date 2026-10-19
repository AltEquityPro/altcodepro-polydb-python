"""A list of plain values in a filter means "equals any of them" (it used to be sent as one JSON string and matched nothing)."""
import pytest

pytest.importorskip("azure.data.tables")

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "artifacts"
    __polydb__ = {}


class _Fake:
    def __init__(self):
        self.calls = []

    def query_entities(self, query_filter=None, **kw):
        self.calls.append(query_filter)
        return []


def _adapter(fake):
    a = object.__new__(AzureTableStorageAdapter)
    a._get_table_client = lambda model: fake
    a.partition_config = None
    return a


def test_a_list_becomes_an_or_group():
    f = _Fake()
    _adapter(f)._query_raw(_Model, {"project_id": "p1", "artifact_definition_id": ["a.x", "b.y", "it's"]}, 50)
    flt = f.calls[0]
    assert "project_id eq 'p1'" in flt
    assert "(artifact_definition_id eq 'a.x' or artifact_definition_id eq 'b.y' or artifact_definition_id eq 'it''s')" in flt
    assert "polydb_json" not in flt


def test_numbers_and_a_single_item_list():
    f = _Fake()
    _adapter(f)._query_raw(_Model, {"version": [1, 2], "status": ["complete"]}, 50)
    assert "(version eq 1 or version eq 2)" in f.calls[0] and "(status eq 'complete')" in f.calls[0]


def test_an_empty_list_matches_nothing_without_querying():
    f = _Fake()
    assert _adapter(f)._query_raw(_Model, {"status": []}, 50) == [] and f.calls == []


def test_a_scalar_filter_is_unchanged():
    f = _Fake()
    _adapter(f)._query_raw(_Model, {"status": "complete", "version": 3, "ok": True}, 50)
    assert f.calls[0] == "status eq 'complete' and version eq 3 and ok eq true"
