"""
tests/test_azure.py
===================
Integration tests for all Azure adapters (via Azurite on localhost).

  AzureTableStorageAdapter  → get_nosql_kv()
  AzureBlobStorageAdapter   → get_object_storage()
  AzureQueueAdapter         → get_queue()
  AzureFileStorageAdapter   → get_shared_files()

Azurite connection string is loaded from AZURE_STORAGE_CONNECTION_STRING.
The well-known Azurite dev key is used in .env.test.
"""

from __future__ import annotations

import io
import time

import pytest

from conftest import uid

pytestmark = pytest.mark.azure


# ────────────────────────────────────────────────────────────────────────────
# Helpers / sentinels
# ────────────────────────────────────────────────────────────────────────────

class AzureItem:
    """Sentinel model — adapter uses class name as Table name."""


def entity(**extra) -> dict:
    return {"id": uid(), "name": f"az-{uid()}", "value": 1, **extra}


# ────────────────────────────────────────────────────────────────────────────
# TABLE STORAGE (NoSQL KV)
# ────────────────────────────────────────────────────────────────────────────

class TestAzureTable:
    def test_put_and_query(self, azure_nosql):
        data = entity()
        result = azure_nosql.put(AzureItem, data)
        assert result["id"] == data["id"]

        rows = azure_nosql.query(AzureItem, {"id": data["id"]})
        assert len(rows) == 1

    def test_put_nested_dict(self, azure_nosql):
        data = entity(meta={"k": "v", "n": 5})
        azure_nosql.put(AzureItem, data)
        rows = azure_nosql.query(AzureItem, {"id": data["id"]})
        assert rows[0]["meta"]["k"] == "v"

    def test_patch_updates_field(self, azure_nosql):
        row = azure_nosql.put(AzureItem, entity(value=1))
        azure_nosql.patch(AzureItem, row["id"], {"value": 999})
        rows = azure_nosql.query(AzureItem, {"id": row["id"]})
        assert rows[0]["value"] == 999

    def test_upsert_insert_then_update(self, azure_nosql):
        data = entity(value=10)
        azure_nosql.upsert(AzureItem, data)
        data["value"] = 20
        azure_nosql.upsert(AzureItem, data)
        rows = azure_nosql.query(AzureItem, {"id": data["id"]})
        assert rows[0]["value"] == 20

    def test_delete_removes_entity(self, azure_nosql):
        row = azure_nosql.put(AzureItem, entity())
        azure_nosql.delete(AzureItem, row["id"])
        rows = azure_nosql.query(AzureItem, {"id": row["id"]})
        assert rows == []

    def test_query_page(self, azure_nosql):
        tag = uid()
        for _ in range(5):
            azure_nosql.put(AzureItem, entity(tenant_id=tag))

        page1, tok = azure_nosql.query_page(AzureItem, {"tenant_id": tag}, 3)
        assert len(page1) <= 3

        if tok:
            page2, _ = azure_nosql.query_page(
                AzureItem, {"tenant_id": tag}, 3, tok
            )
            all_ids = {r["id"] for r in page1} | {r["id"] for r in page2}
            assert len(all_ids) >= 4   # at least 4 distinct items across two pages

    def test_query_limit(self, azure_nosql):
        for _ in range(4):
            azure_nosql.put(AzureItem, entity())
        rows = azure_nosql.query(AzureItem, limit=2)
        assert len(rows) <= 2

    def test_put_separate_models_isolated(self, azure_nosql):
        class AzureOther:
            pass

        d1 = entity()
        d2 = entity()
        azure_nosql.put(AzureItem, d1)
        azure_nosql.put(AzureOther, d2)

        in_item  = azure_nosql.query(AzureItem,  {"id": d2["id"]})
        in_other = azure_nosql.query(AzureOther, {"id": d1["id"]})
        assert in_item  == []
        assert in_other == []


# ────────────────────────────────────────────────────────────────────────────
# BLOB STORAGE
# ────────────────────────────────────────────────────────────────────────────

class TestAzureBlob:
    def test_upload_and_download(self, azure_blob):
        key     = f"test/{uid()}.txt"
        content = b"hello azurite blob"
        azure_blob.upload(key, content)
        downloaded = azure_blob.download(key)
        assert downloaded == content

    def test_upload_overwrite(self, azure_blob):
        key = f"test/{uid()}.txt"
        azure_blob.upload(key, b"v1")
        azure_blob.upload(key, b"v2")
        assert azure_blob.download(key) == b"v2"

    def test_delete_blob(self, azure_blob):
        key = f"test/{uid()}.txt"
        azure_blob.upload(key, b"delete-me")
        azure_blob.delete(key)
        with pytest.raises(Exception):
            azure_blob.download(key)

    def test_list_blobs(self, azure_blob):
        prefix = f"listtest-{uid()}/"
        keys = [f"{prefix}{uid()}.bin" for _ in range(3)]
        for k in keys:
            azure_blob.upload(k, b"x")
        listed = azure_blob.list(prefix)
        assert all(k in listed for k in keys)

    def test_upload_large_blob(self, azure_blob):
        key     = f"test/{uid()}.bin"
        content = b"A" * (1024 * 1024)   # 1 MB
        azure_blob.upload(key, content)
        assert azure_blob.download(key) == content


# ────────────────────────────────────────────────────────────────────────────
# QUEUE
# ────────────────────────────────────────────────────────────────────────────

class TestAzureQueue:
    def test_send_and_receive(self, azure_queue):
        msg = f"msg-{uid()}"
        azure_queue.send(msg)
        received = azure_queue.receive(max_messages=10)
        bodies = [m["body"] if isinstance(m, dict) else m for m in received]
        assert msg in bodies

    def test_send_multiple(self, azure_queue):
        msgs = [f"m-{uid()}" for _ in range(3)]
        for m in msgs:
            azure_queue.send(m)
        received = azure_queue.receive(max_messages=10)
        bodies = [m["body"] if isinstance(m, dict) else m for m in received]
        assert all(m in bodies for m in msgs)

    def test_delete_message(self, azure_queue):
        azure_queue.send(f"del-{uid()}")
        received = azure_queue.receive(max_messages=1)
        assert received
        # Delete using the message handle / id
        msg = received[0]
        if isinstance(msg, dict) and "receipt" in msg:
            azure_queue.delete(msg["id"], msg["receipt"])
        # Should not raise


# ────────────────────────────────────────────────────────────────────────────
# FILE STORAGE
# ────────────────────────────────────────────────────────────────────────────
@pytest.mark.skip(reason="Azure File Share not supported by Azurite")
class TestAzureFiles:
    def test_upload_and_download_file(self, azure_files):
        path    = f"share/test/{uid()}.txt"
        content = b"azure file share content"
        azure_files.upload(path, content)
        downloaded = azure_files.download(path)
        assert downloaded == content

    def test_list_files(self, azure_files):
        prefix = f"share/list-{uid()}/"
        files  = [f"{prefix}{uid()}.txt" for _ in range(2)]
        for f in files:
            azure_files.upload(f, b"data")
        listed = azure_files.list(prefix)
        assert any(f in listed for f in files)

    def test_delete_file(self, azure_files):
        path = f"share/del/{uid()}.txt"
        azure_files.upload(path, b"bye")
        azure_files.delete(path)
        with pytest.raises(Exception):
            azure_files.download(path)