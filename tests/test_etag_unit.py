"""ETag exposure and enforcement on the Azure Table adapter -- unit tests, no emulator needed.

The fake table follows the real service/SDK behaviour that matters here: every write changes the entity's etag; a
conditional write/delete is enforced ONLY when a match condition is passed (the SDK ignores a bare `etag=`, which is
exactly how `delete(..., etag=)` used to be silently unconditional); a mismatch is HTTP 412 `ResourceModifiedError`.
"""

import itertools
import threading

import pytest

pytest.importorskip("azure.data.tables")
from polydb.adapters.AzureTableStorageAdapter import _MODEL_FIELD, AzureTableStorageAdapter  # noqa: E402
from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter  # noqa: E402
from polydb.errors import ConcurrencyError, OperationNotSupportedError  # noqa: E402
from polydb.etag import ETAG_KEY, expose_etag  # noqa: E402


def _if_not_modified(match_condition):
    # by name, not identity: other test modules reload azure.*, which makes two distinct copies of the enum
    return getattr(match_condition, "name", None) == "IfNotModified"


class _Model:
    __name__ = "docs"
    __qualname__ = "docs"
    __polydb__ = {"pk_field": "pk", "rk_field": "rk"}


class ResourceModifiedError(Exception):
    status_code = 412


class ResourceNotFoundError(Exception):
    status_code = 404


class _Ent(dict):
    def __init__(self, data, etag):
        super().__init__(data)
        self.metadata = {"etag": etag, "timestamp": None}


class _FakeTable:
    def __init__(self):
        self.store = {}
        self._n = itertools.count(1)
        self.calls = []

    def _tag(self):
        return f'W/"datetime\'{next(self._n):06d}\'"'

    def _write(self, entity, merge):
        key = (entity["PartitionKey"], entity["RowKey"])
        row = dict(self.store[key][0]) if (merge and key in self.store) else {}
        row.update(entity)
        tag = self._tag()
        self.store[key] = (row, tag)
        return {"etag": tag}

    def create_entity(self, entity, **kw):
        self.calls.append("create")
        return self._write(entity, merge=False)

    def upsert_entity(self, entity, **kw):
        self.calls.append("upsert")
        return self._write(entity, merge=True)

    def update_entity(self, entity, mode=None, etag=None, match_condition=None, **kw):
        self.calls.append("update")
        key = (entity["PartitionKey"], entity["RowKey"])
        if key not in self.store:
            raise ResourceNotFoundError("ResourceNotFound")
        if _if_not_modified(match_condition) and self.store[key][1] != etag:
            raise ResourceModifiedError("UpdateConditionNotSatisfied")
        return self._write(entity, merge=True)

    def get_entity(self, pk, rk, **kw):
        if (pk, rk) not in self.store:
            raise ResourceNotFoundError("ResourceNotFound")
        row, tag = self.store[(pk, rk)]
        return _Ent(row, tag)

    def query_entities(self, query_filter=None, select=None, **kw):
        return [_Ent(r, t) for r, t in self.store.values()]

    def delete_entity(self, pk, rk, etag=None, match_condition=None, **kw):
        self.calls.append("delete")
        if (pk, rk) not in self.store:
            raise ResourceNotFoundError("ResourceNotFound")
        if _if_not_modified(match_condition) and self.store[(pk, rk)][1] != etag:
            raise ResourceModifiedError("UpdateConditionNotSatisfied")
        del self.store[(pk, rk)]


def _adapter():
    fake = _FakeTable()
    blobs, deleted = {}, []
    adp = object.__new__(AzureTableStorageAdapter)
    adp.max_size = AzureTableStorageAdapter.AZURE_TABLE_MAX_SIZE
    adp.partition_config = None
    adp._lock = threading.Lock()
    adp.object_storage = None
    adp._blob_service = None
    adp._get_table_client = lambda model: fake
    adp._blob_upload = lambda key, data: blobs.__setitem__(key, data)
    adp._blob_download = lambda key: blobs[key]
    adp._blob_delete = lambda key: deleted.append(key)
    return adp, fake, blobs, deleted


KEY = {"pk": "p", "rk": "r"}


def _seed(adp):
    adp.put(_Model, {"pk": "p", "rk": "r", "status": "queued", "n": 1})


def test_default_reads_are_byte_for_byte_unchanged():
    adp, *_ = _adapter()
    _seed(adp)
    assert ETAG_KEY not in adp._get_raw(_Model, "p", "r")
    assert all(ETAG_KEY not in r for r in adp._query_raw(_Model, {}, None))
    assert ETAG_KEY not in adp.put(_Model, {"pk": "p", "rk": "r2", "n": 2})


def test_etag_is_exposed_on_get_and_query_only_when_requested():
    adp, *_ = _adapter()
    _seed(adp)
    with expose_etag():
        got = adp._get_raw(_Model, "p", "r")
        rows = adp._query_raw(_Model, {}, None)
    assert got[ETAG_KEY].startswith('W/"datetime')
    assert rows[0][ETAG_KEY] == got[ETAG_KEY]


def test_conditional_patch_succeeds_with_the_current_etag_and_chains():
    adp, fake, *_ = _adapter()
    _seed(adp)
    with expose_etag():
        tag1 = adp._get_raw(_Model, "p", "r")[ETAG_KEY]
        out = adp.patch(_Model, KEY, {"status": "running"}, etag=tag1)
    tag2 = out[ETAG_KEY]
    assert tag2 != tag1 and "update" in fake.calls  # a real conditional write, not an upsert
    assert adp._get_raw(_Model, "p", "r")["status"] == "running"
    with expose_etag():
        adp.patch(_Model, KEY, {"status": "done"}, etag=tag2)  # the new tag is usable straight away
    assert adp._get_raw(_Model, "p", "r")["status"] == "done"


def test_stale_etag_is_refused_and_the_row_is_unchanged():
    adp, fake, *_ = _adapter()
    _seed(adp)
    with expose_etag():
        stale = adp._get_raw(_Model, "p", "r")[ETAG_KEY]
    adp.patch(_Model, KEY, {"status": "someone else"})  # another writer got there first
    with pytest.raises(ConcurrencyError) as exc:
        adp.patch(_Model, KEY, {"status": "mine"}, etag=stale)
    assert not exc.value.missing
    assert adp._get_raw(_Model, "p", "r")["status"] == "someone else"


def test_conflict_is_not_retried_and_is_not_a_nosql_error():
    adp, fake, *_ = _adapter()
    _seed(adp)
    adp.patch(_Model, KEY, {"n": 2})
    with pytest.raises(ConcurrencyError):
        adp.patch(_Model, KEY, {"n": 3}, etag='W/"stale"')
    assert fake.calls.count("update") == 1  # the adapter's own retry decorator must not re-run a lost race


def test_conditional_patch_on_a_missing_row_reports_missing():
    adp, *_ = _adapter()
    with pytest.raises(ConcurrencyError) as exc:
        adp.patch(_Model, KEY, {"n": 1}, etag='W/"x"')
    assert exc.value.missing


def test_a_row_read_with_its_etag_can_be_passed_back_without_storing_the_tag():
    adp, fake, *_ = _adapter()
    _seed(adp)
    with expose_etag():
        row = adp._get_raw(_Model, "p", "r")
    row["status"] = "edited"
    adp.patch(_Model, KEY, row, etag=row[ETAG_KEY])
    stored, _ = fake.store[("p", "r")]
    assert ETAG_KEY not in stored and stored["status"] == "edited"


def test_delete_with_a_stale_etag_is_refused_and_keeps_row_and_blob():
    adp, fake, blobs, deleted = _adapter()
    _seed(adp)
    key = ("p", "r")
    tag = fake.store[key][1]
    adp.patch(_Model, KEY, {"n": 9})  # moves the etag on
    fake.store[key][0].update({"_overflow": True, "_blob_key": "overflow/k1.json", _MODEL_FIELD: _Model.__qualname__})
    with pytest.raises(ConcurrencyError):
        adp.delete(_Model, KEY, etag=tag)
    assert key in fake.store and deleted == []  # row AND its blob survive a refused delete


def test_delete_with_the_current_etag_removes_the_row_then_its_blob():
    adp, fake, blobs, deleted = _adapter()
    _seed(adp)
    key = ("p", "r")
    fake.store[key][0].update({"_overflow": True, "_blob_key": "overflow/k1.json", _MODEL_FIELD: _Model.__qualname__})
    tag = fake.store[key][1]
    out = adp.delete(_Model, KEY, etag=tag)
    assert out["deleted"] and key not in fake.store and deleted == ["overflow/k1.json"]


def test_delete_without_etag_behaves_as_before():
    adp, fake, *_ = _adapter()
    _seed(adp)
    assert adp.delete(_Model, KEY)["deleted"] and ("p", "r") not in fake.store


def test_a_backend_without_conditional_writes_refuses_instead_of_ignoring_the_etag():
    class Plain(NoSQLKVAdapter):
        def __init__(self):
            self.max_size = 10**6
            self.partition_config = None

        def _put_raw(self, model, pk, rk, data):
            return data

        def _get_raw(self, model, pk, rk):
            return None

        def _delete_raw(self, model, pk, rk, etag):
            return {"deleted": True}

        def _query_raw(self, model, filters, limit, select=None):
            return []

    p = Plain()
    with pytest.raises(OperationNotSupportedError, match="conditional"):
        p.patch(_Model, KEY, {"a": 1}, etag="x")
    with pytest.raises(OperationNotSupportedError):
        p.delete(_Model, KEY, etag="x")
    assert p.delete(_Model, KEY)["deleted"]  # no etag: unchanged
