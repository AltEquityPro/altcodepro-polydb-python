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
