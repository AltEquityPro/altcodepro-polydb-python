"""Unit tests for the Azure Table empty-filter contract (no Azurite needed).

Contract: a filter that carried keys but resolved to no conditions (e.g. every
value was None/unresolvable) must match NOTHING — [] for a query, None for
read_one — never a full-table scan that returns a stray context row. Only a
genuinely empty filter dict ({}) means "list all".

This is the bug where read_one("Plan", {tier: None}) returned an unrelated User
row because the empty query_filter scanned the whole table.
"""

import pytest

pytest.importorskip("azure.data.tables")  # adapter needs the SDK to import

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "Plan"


class _FakeTableClient:
    def __init__(self, entities):
        self.entities = entities
        self.calls = []

    def query_entities(self, query_filter=None, **kw):
        self.calls.append(query_filter)
        # Emulate Azure Table: a None filter returns EVERY entity.
        return list(self.entities) if query_filter is None else []


_STRAY = [{"PartitionKey": "t1", "RowKey": "user@x.com",
           "__polydb_model__": "User", "email": "user@x.com"}]


def _adapter(fake):
    adp = object.__new__(AzureTableStorageAdapter)  # bypass live-connection __init__
    adp._get_table_client = lambda model: fake
    return adp


def test_none_valued_filter_matches_nothing():
    fake = _FakeTableClient(_STRAY)
    res = _adapter(fake)._query_raw(_Model, {"tier": None}, 1)
    assert res == []                      # no stray record
    assert fake.calls == []               # and no full-table scan was issued


def test_empty_filter_lists_all():
    fake = _FakeTableClient(_STRAY)
    res = _adapter(fake)._query_raw(_Model, {}, 10)
    assert fake.calls == [None]           # genuine list-all
    assert len(res) == 1


def test_real_filter_queries_normally():
    fake = _FakeTableClient(_STRAY)
    _adapter(fake)._query_raw(_Model, {"tier": "free"}, 1)
    assert fake.calls and "tier eq 'free'" in (fake.calls[0] or "")


def test_query_paged_none_valued_filter_matches_nothing():
    from polydb.models import PageRequest

    fake = _FakeTableClient(_STRAY)
    page = _adapter(fake).query_paged(_Model, PageRequest(filters={"tier": None}, limit=10))
    assert page.items == [] and page.has_more is False
    assert fake.calls == []               # short-circuited before any scan
