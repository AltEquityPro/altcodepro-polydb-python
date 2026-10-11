"""
tests/test_nosql_constraints.py
================================
Unique constraints and equality indexes for key/value NoSQL adapters (base/constraints.py). A bare in-memory
NoSQLKVAdapter subclass stands in for the store; the Azure `create_entity` primitive is exercised through the same
`_put_new_raw` contract (atomic insert-if-absent).
"""

from __future__ import annotations

import threading

import pytest

from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter
from polydb.base.constraints import UniqueConstraintError, normalize_value


class MemKV(NoSQLKVAdapter):
    def __init__(self):
        super().__init__()
        self.rows: dict[tuple, dict] = {}
        self._mu = threading.Lock()
        self.fail_main_write = False

    @staticmethod
    def _t(model):
        return (getattr(model, "__polydb__", {}) or {}).get("table") or model.__name__

    def _put_raw(self, model, pk, rk, data):
        if self.fail_main_write and self._t(model) == "people":
            raise RuntimeError("boom")
        with self._mu:
            self.rows[(self._t(model), str(pk), str(rk))] = dict(data)
        return dict(data)

    def _put_new_raw(self, model, pk, rk, data):  # atomic, like Azure create_entity
        with self._mu:
            key = (self._t(model), str(pk), str(rk))
            if key in self.rows:
                return False
            self.rows[key] = dict(data)
            return True

    def _get_raw(self, model, pk, rk):
        r = self.rows.get((self._t(model), str(pk), str(rk)))
        return dict(r) if r is not None else None

    def _query_raw(self, model, filters, limit, select=None):
        t = self._t(model)
        out = []
        for (tb, pk, rk), r in self.rows.items():
            if tb != t:
                continue
            if all(r.get(k) == v for k, v in (filters or {}).items()):
                out.append(dict(r))
        return out[:limit] if limit else out

    def _delete_raw(self, model, pk, rk, etag):
        with self._mu:
            existed = self.rows.pop((self._t(model), str(pk), str(rk)), None)
        if existed is None:
            raise KeyError("missing")
        return {"deleted": True}

    def claims(self):
        return [k for k in self.rows if k[0].endswith("uq")]

    def index_rows(self):
        return [k for k in self.rows if k[0].endswith("ix")]


class Person:
    __polydb__ = {"storage": "nosql", "table": "people", "pk_field": "org", "rk_field": "id",
                  "unique_fields": ["email"], "index_fields": ["city"]}


class Plain:
    __polydb__ = {"storage": "nosql", "table": "plain", "pk_field": "org", "rk_field": "id"}


@pytest.fixture
def kv():
    return MemKV()


def person(i, email, city="Leeds", org="o1"):
    return {"org": org, "id": i, "email": email, "city": city, "name": f"n{i}"}


def test_a_duplicate_unique_value_is_rejected(kv):
    kv.put(Person, person("1", "a@x.com"))
    with pytest.raises(UniqueConstraintError) as e:
        kv.put(Person, person("2", "a@x.com"))
    assert e.value.field == "email"
    assert kv._get_raw(Person, "o1", "2") is None  # the second row was never written


def test_uniqueness_is_global_not_per_partition(kv):
    kv.put(Person, person("1", "a@x.com", org="o1"))
    with pytest.raises(UniqueConstraintError):
        kv.put(Person, person("1", "a@x.com", org="o2"))


def test_rewriting_the_same_row_is_idempotent(kv):
    kv.put(Person, person("1", "a@x.com"))
    kv.put(Person, person("1", "a@x.com", city="York"))
    assert len(kv.claims()) == 1


def test_changing_a_value_moves_the_claim_and_frees_the_old_value(kv):
    kv.put(Person, person("1", "a@x.com"))
    kv.patch(Person, {"partition_key": "o1", "id": "1"}, {"email": "b@x.com"})
    kv.put(Person, person("2", "a@x.com"))  # the old value is free again
    with pytest.raises(UniqueConstraintError):
        kv.patch(Person, {"partition_key": "o1", "id": "2"}, {"email": "b@x.com"})
    assert kv._get_raw(Person, "o1", "2")["email"] == "a@x.com"  # a rejected patch changed nothing


def test_delete_releases_the_claim_and_index_rows(kv):
    kv.put(Person, person("1", "a@x.com"))
    kv.delete(Person, {"partition_key": "o1", "id": "1"})
    assert kv.claims() == [] and kv.index_rows() == []
    kv.put(Person, person("2", "a@x.com"))


def test_a_failed_entity_write_releases_what_it_claimed(kv):
    kv.fail_main_write = True
    with pytest.raises(RuntimeError):
        kv.put(Person, person("1", "a@x.com"))
    kv.fail_main_write = False
    assert kv.claims() == []
    kv.put(Person, person("2", "a@x.com"))


def test_an_orphaned_claim_is_taken_over(kv):
    # a crash after the claim but before the entity write leaves a claim whose owner does not exist
    side = kv._side_model(Person, "uq")
    from polydb.base.constraints import _digest
    h = _digest("email", "a@x.com")
    kv.rows[(side.__polydb__["table"], f"u{h[:2]}", h)] = {"id": h, "owner_pk": "o1", "owner_rk": "ghost"}
    kv.put(Person, person("1", "a@x.com"))
    assert kv._get_raw(Person, "o1", "1")["email"] == "a@x.com"


def test_concurrent_writers_cannot_both_take_one_value(kv):
    results = []

    def run(i):
        try:
            kv.put(Person, person(str(i), "race@x.com"))
            results.append("ok")
        except UniqueConstraintError:
            results.append("dup")

    ts = [threading.Thread(target=run, args=(i,)) for i in range(12)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert results.count("ok") == 1 and results.count("dup") == 11


def test_equality_query_on_a_unique_field_is_a_point_read(kv):
    kv.put(Person, person("1", "a@x.com"))
    kv.put(Person, person("2", "b@x.com"))
    calls = {"scan": 0}
    real = kv._query_raw
    kv._query_raw = lambda m, f, l, select=None: (calls.__setitem__("scan", calls["scan"] + 1), real(m, f, l))[1]
    rows = kv.query(Person, {"email": "b@x.com"})
    assert [r["id"] for r in rows] == ["2"] and calls["scan"] == 0


def test_equality_query_on_an_indexed_field_uses_the_index(kv):
    for i, c in enumerate(["Leeds", "York", "Leeds", "Leeds"]):
        kv.put(Person, person(str(i), f"{i}@x.com", city=c))
    rows = kv.query(Person, {"city": "Leeds"})
    assert sorted(r["id"] for r in rows) == ["0", "2", "3"]
    assert len(kv.query(Person, {"city": "Leeds"}, limit=2)) == 2
    assert kv.query(Person, {"city": "Leeds", "name": "n2"})[0]["id"] == "2"  # remaining filters still apply
    assert kv.query(Person, {"city": "Nowhere"}) == []


def test_a_stale_index_row_never_returns_a_wrong_row(kv):
    kv.put(Person, person("1", "a@x.com", city="Leeds"))
    # the entity moved on but a crash left its old index row behind
    kv.rows[(kv._t(Person), "o1", "1")]["city"] = "York"
    assert kv.query(Person, {"city": "Leeds"}) == []


def test_a_key_hint_in_the_query_is_honoured(kv):
    kv.put(Person, person("1", "a@x.com", org="o1"))
    assert kv.query(Person, {"email": "a@x.com", "partition_key": "o1"})
    assert kv.query(Person, {"email": "a@x.com", "partition_key": "other"}) == []


def test_models_without_constraints_are_untouched(kv):
    kv.put(Plain, {"org": "o", "id": "1", "email": "a"})
    kv.put(Plain, {"org": "o", "id": "2", "email": "a"})
    assert kv.claims() == [] and kv.index_rows() == []
    assert len(kv.query(Plain, {"email": "a"})) == 2


def test_rebuild_backfills_rows_written_before_the_constraint_existed(kv):
    kv.rows[("people", "o1", "1")] = person("1", "a@x.com", city="Leeds")
    kv.rows[("people", "o1", "2")] = person("2", "a@x.com", city="Leeds")  # a legacy duplicate
    done = kv.rebuild_constraints(Person)
    assert done["claims"] == 1 and done["duplicates"] == 1 and done["indexed"] == 2
    assert len(kv.query(Person, {"city": "Leeds"})) == 2


@pytest.mark.parametrize("v,want", [(None, None), ("", None), ({"a": 1}, None), (True, "true"), (5, "5"), ("x", "x")])
def test_normalize_value(v, want):
    assert normalize_value(v) == want
