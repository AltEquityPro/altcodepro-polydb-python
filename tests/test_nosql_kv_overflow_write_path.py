"""
tests/test_nosql_kv_overflow_write_path.py
============================================
Phase 0 fix: NoSQLKVAdapter.put() never called _check_overflow -- only
patch() did (see base/NoSQLKVAdapter.py). Every adapter that relies on the
base write path for a plain create (MongoDBAdapter, VercelKVAdapter) had a
dead overflow facility on that path: a create() with an oversized payload
went straight to _put_raw() and either failed at the driver (Mongo's 16MB
BSON cap, Vercel KV's value cap) or silently wrote a payload larger than
what a subsequent query()/query_linq() read path expects to rehydrate.

These tests use a bare, in-memory NoSQLKVAdapter subclass (no live cloud
backend) since the bug and its fix live entirely in the base class -- the
same base every real adapter inherits from unless it overrides put(). This
matches CLAUDE.md's own framing: "any new write path [must go] through
_maybe_overflow_to_s3 / _maybe_store_overflow / the per-property check in
_put_raw" -- put() is exactly the write path that was skipping this.
"""

from __future__ import annotations

import pytest

from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter
from polydb.errors import StorageError


class FakeObjectStorage:
    """In-memory stand-in for CloudDatabaseFactory().get_object_storage()."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.put_calls: list[str] = []

    def put(self, key, data, **kwargs):
        self.put_calls.append(key)
        self.store[key] = data
        return key

    def get(self, key):
        return self.store[key]

    def delete(self, key):
        del self.store[key]
        return True

    def list(self, prefix: str = ""):
        return [k for k in self.store if k.startswith(prefix)]


class InMemoryKVAdapter(NoSQLKVAdapter):
    """Minimal concrete NoSQLKVAdapter: a dict keyed by (pk, rk). No cloud SDK."""

    def __init__(self, *, max_size: int = 200):
        super().__init__()
        self.max_size = max_size
        self.object_storage = FakeObjectStorage()
        self._rows: dict[tuple, dict] = {}

    def _put_raw(self, model, pk, rk, data):
        self._rows[(pk, rk)] = dict(data)
        return dict(data)

    def _get_raw(self, model, pk, rk):
        row = self._rows.get((pk, rk))
        return dict(row) if row is not None else None

    def _query_raw(self, model, filters, limit):
        return [dict(r) for r in self._rows.values()]

    def _delete_raw(self, model, pk, rk, etag):
        return self._rows.pop((pk, rk), {"deleted": False})


class Widget:
    __polydb__ = {"pk_field": "tenant_id", "rk_field": "id"}


def _big_payload(rk: str = "w1") -> dict:
    return {"tenant_id": "t1", "id": rk, "blob": "x" * 5000}


def _small_payload(rk: str = "w2") -> dict:
    return {"tenant_id": "t1", "id": rk, "note": "tiny"}


class TestPutOverflowsLikePatch:
    def test_put_overflows_a_payload_over_max_size(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = _big_payload()

        result = adapter.put(Widget, data)

        # Before the fix: put() called _put_raw(model, pk, rk, data) directly,
        # so the stored row would be the full 5KB payload with no reference
        # markers at all. After the fix it must look exactly like patch()'s
        # own overflow shape.
        assert result["_overflow"] is True
        assert result["_blob_key"].startswith("overflow/")
        assert result["_size"] > 200
        assert "_checksum" in result

    def test_put_does_not_overflow_a_small_payload(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = _small_payload()

        result = adapter.put(Widget, data)

        assert "_overflow" not in result
        assert result == data

    def test_overflowed_put_writes_the_full_payload_to_the_object_store(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = _big_payload()

        result = adapter.put(Widget, data)

        assert len(adapter.object_storage.put_calls) == 1
        blob_key = result["_blob_key"]
        assert blob_key in adapter.object_storage.store

    def test_overflowed_put_round_trips_through_query(self):
        """The scan/query read path already retrieves overflow -- confirm a
        put()-written overflow row rehydrates correctly, proving put() and
        the existing read path agree on the reference shape."""
        adapter = InMemoryKVAdapter(max_size=200)
        data = _big_payload()
        adapter.put(Widget, data)

        rows = adapter.query(Widget)

        assert len(rows) == 1
        assert rows[0]["blob"] == data["blob"]
        assert rows[0]["id"] == "w1"

    def test_overflowed_put_detects_a_corrupted_blob_on_read(self):
        """CLAUDE.md: 'a truncated or swapped blob raises rather than
        returning silent garbage' -- prove that still holds for a put()-
        created overflow row now that put() actually creates them."""
        adapter = InMemoryKVAdapter(max_size=200)
        data = _big_payload()
        result = adapter.put(Widget, data)

        # Tamper with the stored blob bytes after the fact.
        blob_key = result["_blob_key"]
        adapter.object_storage.store[blob_key] = (
            b'{"tenant_id": "t1", "id": "w1", "blob": "TAMPERED"}'
        )

        with pytest.raises(StorageError, match="Checksum mismatch"):
            adapter.query(Widget)

    def test_put_and_patch_agree_on_the_overflow_threshold(self):
        """Regression guard: put() and patch() must overflow the identical
        payload identically -- they used to disagree (patch() alone checked)."""
        adapter = InMemoryKVAdapter(max_size=200)

        put_result = adapter.put(Widget, _big_payload(rk="w1"))
        patch_result = adapter.patch(Widget, "w3", _big_payload(rk="w3"), replace=True)

        assert put_result["_overflow"] is True
        assert patch_result["_overflow"] is True


class TestCheckOverflowPreservesScalarFields:
    """A real, reproduced bug: _check_overflow()'s own reference dict used
    to carry ONLY the four internal bookkeeping keys
    (_overflow/_blob_key/_size/_checksum), discarding every other field
    from the row a caller actually persists -- contradicting this
    module's own long-documented claim ("scalar fields are copied onto
    the reference row") that the code never implemented. A row whose
    id/tenant_id never survived onto the persisted reference was
    unfindable by an ordinary id/tenant_id-filtered query even though the
    FULL record was sitting safely in object storage the whole time --
    see AzureTableStorageAdapter's own data-loss bug (test_azure_table_
    put_overflow_data_loss.py) for the live-reported incident this fix
    closes for every adapter, not just Azure."""

    def test_small_scalar_fields_are_copied_onto_the_reference_row(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = {
            "tenant_id": "t1",
            "id": "w1",
            "name": "Widget One",
            "count": 3,
            "active": True,
            "note": None,
            "blob": "x" * 5000,
        }

        reference, blob_key = adapter._check_overflow(data)

        assert reference["_overflow"] is True
        assert reference["tenant_id"] == "t1"
        assert reference["id"] == "w1"
        assert reference["name"] == "Widget One"
        assert reference["count"] == 3
        assert reference["active"] is True
        assert reference["note"] is None
        # The oversized field itself must NOT be copied onto the reference
        # row -- that's the whole point of moving it to blob storage.
        assert "blob" not in reference

    def test_a_large_non_scalar_field_is_excluded_from_the_reference_row(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = {"tenant_id": "t1", "id": "w1", "spec": {"paths": {"x": "y" * 5000}}}

        reference, _ = adapter._check_overflow(data)

        assert "spec" not in reference
        assert reference["tenant_id"] == "t1"
        assert reference["id"] == "w1"

    def test_a_caller_field_never_shadows_the_bookkeeping_keys(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = {
            "tenant_id": "t1",
            "id": "w1",
            "blob": "x" * 5000,
            # A caller-supplied field that happens to collide with one of
            # _check_overflow's own bookkeeping keys must never win.
            "_overflow": "not-a-bookkeeping-value",
        }

        reference, blob_key = adapter._check_overflow(data)

        assert reference["_overflow"] is True
        assert reference["_blob_key"] == blob_key

    def test_scalar_copy_is_capped_at_fifty_fields(self):
        adapter = InMemoryKVAdapter(max_size=200)
        data = {"tenant_id": "t1", "id": "w1", "blob": "x" * 5000}
        for i in range(80):
            data[f"field_{i}"] = i

        reference, _ = adapter._check_overflow(data)

        copied_field_count = sum(1 for k in reference if k.startswith("field_"))
        assert copied_field_count <= adapter._SCALAR_COPY_MAX_FIELDS

    def test_a_row_that_overflowed_is_still_findable_by_an_id_tenant_id_filtered_query(self):
        """The real-world manifestation of the bug this fix closes: the
        exact id/tenant_id-filtered read IntegrationTemplateStore.get()/
        DatabaseFactory.read_one() issue must find an overflowed row."""
        adapter = InMemoryKVAdapter(max_size=200)
        adapter.put(Widget, _big_payload(rk="w1"))

        stored = adapter._get_raw(Widget, "t1", "w1")

        assert stored["id"] == "w1"
        assert stored["tenant_id"] == "t1"
