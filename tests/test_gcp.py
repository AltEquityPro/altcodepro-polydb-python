"""
tests/test_gcp.py
=================
Integration tests for all GCP adapters (via emulators).

  FirestoreAdapter    → get_nosql_kv()       (FIRESTORE_EMULATOR_HOST)
  GCPStorageAdapter   → get_object_storage() + get_shared_files()
                         (STORAGE_EMULATOR_HOST via fake-gcs-server)
  PubSubAdapter       → get_queue()           (PUBSUB_EMULATOR_HOST)
"""

from __future__ import annotations

from sqlite3 import DatabaseError
import time

import pytest

from conftest import uid
from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter
from polydb.query import Operator, QueryBuilder

pytestmark = pytest.mark.gcp


# ────────────────────────────────────────────────────────────────────────────
# Sentinel models
# ────────────────────────────────────────────────────────────────────────────


class GcpItem:
    """Maps to a Firestore collection."""


class GcpArchive:
    """Separate Firestore collection."""


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


def gitem(**extra) -> dict:
    return {"id": uid(), "name": f"gcp-{uid()}", "value": 7, **extra}


# ────────────────────────────────────────────────────────────────────────────
# FIRESTORE
# ────────────────────────────────────────────────────────────────────────────


class TestFirestore:
    def test_put_and_query(self, gcp_nosql):
        data = gitem()
        result = gcp_nosql.put(GcpItem, data)
        assert result["id"] == data["id"]
        rows = gcp_nosql.query(GcpItem, {"id": data["id"]})
        assert len(rows) == 1

    def test_put_nested_map(self, gcp_nosql):
        data = gitem(meta={"x": 1, "y": [10, 20]})
        gcp_nosql.put(GcpItem, data)
        rows = gcp_nosql.query(GcpItem, {"id": data["id"]})
        assert rows[0]["meta"]["x"] == 1

    def test_put_list_field(self, gcp_nosql):
        data = gitem(tags=["a", "b", "c"])
        gcp_nosql.put(GcpItem, data)
        rows = gcp_nosql.query(GcpItem, {"id": data["id"]})
        assert rows[0]["tags"] == ["a", "b", "c"]

    def test_patch(self, gcp_nosql):
        row = gcp_nosql.put(GcpItem, gitem(value=1))
        gcp_nosql.patch(GcpItem, row["id"], {"value": 100})
        rows = gcp_nosql.query(GcpItem, {"id": row["id"]})
        assert rows[0]["value"] == 100

    def test_patch_partial_preserves_fields(self, gcp_nosql):
        row = gcp_nosql.put(GcpItem, gitem(name="keep", value=1))
        gcp_nosql.patch(GcpItem, row["id"], {"value": 50})
        rows = gcp_nosql.query(GcpItem, {"id": row["id"]})
        assert rows[0]["name"] == "keep"

    def test_upsert(self, gcp_nosql):
        data = gitem(value=5)
        gcp_nosql.upsert(GcpItem, data)
        data["value"] = 55
        gcp_nosql.upsert(GcpItem, data)
        rows = gcp_nosql.query(GcpItem, {"id": data["id"]})
        assert rows[0]["value"] == 55

    def test_delete(self, gcp_nosql):
        row = gcp_nosql.put(GcpItem, gitem())
        gcp_nosql.delete(GcpItem, row["id"])
        assert gcp_nosql.query(GcpItem, {"id": row["id"]}) == []

    def test_delete_returns_deleted(self, gcp_nosql):
        row = gcp_nosql.put(GcpItem, gitem(name="deleted"))
        deleted = gcp_nosql.delete(GcpItem, row["id"])
        assert deleted["id"] == row["id"]

    def test_delete_nonexistent_raises(self, gcp_nosql):

        with pytest.raises(DatabaseError):
            gcp_nosql.delete(GcpItem, "ghost-id")

    def test_query_limit(self, gcp_nosql):
        for _ in range(5):
            gcp_nosql.put(GcpItem, gitem())
        rows = gcp_nosql.query(GcpItem, limit=2)
        assert len(rows) <= 2

    def test_query_no_cache(self, gcp_nosql):
        gcp_nosql.put(GcpItem, gitem(name="nc"))
        rows = gcp_nosql.query(GcpItem, {"name": "nc"}, no_cache=True)
        assert len(rows) >= 1

    def test_collection_isolation(self, gcp_nosql):
        d1 = gitem()
        d2 = gitem()
        gcp_nosql.put(GcpItem, d1)
        gcp_nosql.put(GcpArchive, d2)
        assert gcp_nosql.query(GcpItem, {"id": d2["id"]}) == []
        assert gcp_nosql.query(GcpArchive, {"id": d1["id"]}) == []

    def test_query_page(self, gcp_nosql):
        tag = uid()
        for _ in range(5):
            gcp_nosql.put(GcpItem, gitem(tenant_id=tag))

        page1, tok = gcp_nosql.query_page(GcpItem, {"tenant_id": tag}, 3)
        assert 1 <= len(page1) <= 3

        if tok:
            page2, _ = gcp_nosql.query_page(GcpItem, {"tenant_id": tag}, 3, tok)
            combined = {r["id"] for r in page1} | {r["id"] for r in page2}
            assert len(combined) >= 4

    def test_linq_where(self, gcp_nosql: NoSQLKVAdapter):
        name = f"linq-{uid()}"
        gcp_nosql.put(GcpItem, gitem(name=name, value=888))
        qb = QueryBuilder().where("name", Operator.EQ, name)
        results = gcp_nosql.query_linq_rows(GcpItem, qb)
        assert any(r["value"] == 888 for r in results)

    def test_linq_count(self, gcp_nosql):
        tag = uid()
        for _ in range(3):
            gcp_nosql.put(GcpItem, gitem(tenant_id=tag))
        qb = QueryBuilder().where("tenant_id", "", tag).count()
        total = gcp_nosql.query_linq(GcpItem, qb)
        assert isinstance(total, int)
        assert total == 3


# ────────────────────────────────────────────────────────────────────────────
# GCS OBJECT STORAGE (fake-gcs-server)
# ────────────────────────────────────────────────────────────────────────────


class TestGCSObjectStorage:
    def test_upload_and_download(self, gcp_blob):
        key = f"test/{uid()}.txt"
        content = b"fake gcs content"
        gcp_blob.upload(key, content)
        assert gcp_blob.download(key) == content

    def test_upload_overwrite(self, gcp_blob):
        key = f"test/{uid()}.txt"
        gcp_blob.upload(key, b"v1")
        gcp_blob.upload(key, b"v2")
        assert gcp_blob.download(key) == b"v2"

    def test_delete(self, gcp_blob):
        key = f"test/{uid()}.txt"
        gcp_blob.upload(key, b"bye")
        gcp_blob.delete(key)
        with pytest.raises(Exception):
            gcp_blob.download(key)

    def test_list(self, gcp_blob):
        prefix = f"gcslist-{uid()}/"
        keys = [f"{prefix}{uid()}.bin" for _ in range(3)]
        for k in keys:
            gcp_blob.upload(k, b"data")
        listed = gcp_blob.list(prefix)
        assert all(k in listed for k in keys)

    def test_large_upload(self, gcp_blob):
        key = f"test/{uid()}.bin"
        content = b"G" * (2 * 1024 * 1024)
        gcp_blob.upload(key, content)
        assert gcp_blob.download(key) == content


# ────────────────────────────────────────────────────────────────────────────
# GCP Shared Files (GCPStorageAdapter re-used as file store)
# ────────────────────────────────────────────────────────────────────────────


class TestGCPFiles:
    def test_upload_and_download(self, gcp_files):
        path = f"files/{uid()}.txt"
        content = b"gcp file content"
        gcp_files.upload(path, content)
        assert gcp_files.download(path) == content

    def test_delete(self, gcp_files):
        path = f"files/{uid()}.txt"
        gcp_files.upload(path, b"temp")
        gcp_files.delete(path)
        with pytest.raises(Exception):
            gcp_files.download(path)


# ────────────────────────────────────────────────────────────────────────────
# PUB/SUB QUEUE
# ────────────────────────────────────────────────────────────────────────────


class TestPubSub:
    def test_send_and_receive(self, gcp_queue):
        body = f"pubsub-{uid()}"
        gcp_queue.send(body)
        time.sleep(0.5)  # emulator may be slightly async
        received = gcp_queue.receive(max_messages=10)
        bodies = [m["body"] if isinstance(m, dict) else m for m in received]
        assert body in bodies

    def test_send_multiple(self, gcp_queue):
        msgs = [f"ps-{uid()}" for _ in range(3)]
        for m in msgs:
            gcp_queue.send(m)
        time.sleep(0.5)
        received = gcp_queue.receive(max_messages=10)
        bodies = [m["body"] if isinstance(m, dict) else m for m in received]
        assert all(m in bodies for m in msgs)

    def test_ack_message(self, gcp_queue):
        gcp_queue.send(f"ack-{uid()}")
        time.sleep(0.2)
        received = gcp_queue.receive(max_messages=1)
        if received:
            msg = received[0]
            if isinstance(msg, dict) and "ack_id" in msg:
                gcp_queue.ack(msg["ack_id"])  # Must not raise
