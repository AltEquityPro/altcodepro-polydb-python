"""
tests/test_vercel.py
====================
Integration tests for Vercel adapters (via local Redis on port 6380).

  VercelKVAdapter    → get_nosql_kv()
  VercelBlobAdapter  → get_object_storage()
  VercelQueueAdapter → get_queue()
"""

from __future__ import annotations

import pytest

from conftest import uid

pytestmark = pytest.mark.vercel


class VItem:
    pass


def vitem(**extra) -> dict:
    return {"id": uid(), "name": f"v-{uid()}", "value": 3, **extra}


# ────────────────────────────────────────────────────────────────────────────
# Vercel KV (NoSQL)
# ────────────────────────────────────────────────────────────────────────────

class TestVercelKV:
    def test_put_and_query(self, vercel_nosql):
        data = vitem()
        result = vercel_nosql.put(VItem, data)
        assert result["id"] == data["id"]
        rows = vercel_nosql.query(VItem, {"id": data["id"]})
        assert len(rows) == 1

    def test_patch(self, vercel_nosql):
        row = vercel_nosql.put(VItem, vitem(value=1))
        vercel_nosql.patch(VItem, row["id"], {"value": 42})
        rows = vercel_nosql.query(VItem, {"id": row["id"]})
        assert rows[0]["value"] == 42

    def test_upsert(self, vercel_nosql):
        data = vitem(value=10)
        vercel_nosql.upsert(VItem, data)
        data["value"] = 20
        vercel_nosql.upsert(VItem, data)
        rows = vercel_nosql.query(VItem, {"id": data["id"]})
        assert rows[0]["value"] == 20

    def test_delete(self, vercel_nosql):
        row = vercel_nosql.put(VItem, vitem())
        vercel_nosql.delete(VItem, row["id"])
        rows = vercel_nosql.query(VItem, {"id": row["id"]})
        assert rows == []

    def test_query_limit(self, vercel_nosql):
        for _ in range(5):
            vercel_nosql.put(VItem, vitem())
        rows = vercel_nosql.query(VItem, limit=2)
        assert len(rows) <= 2

    def test_query_page(self, vercel_nosql):
        tag = uid()
        for _ in range(5):
            vercel_nosql.put(VItem, vitem(tenant_id=tag))
        page1, tok = vercel_nosql.query_page(VItem, {"tenant_id": tag}, 3)
        assert len(page1) <= 3
        if tok:
            page2, _ = vercel_nosql.query_page(VItem, {"tenant_id": tag}, 3, tok)
            assert {r["id"] for r in page1}.isdisjoint({r["id"] for r in page2})


# ────────────────────────────────────────────────────────────────────────────
# Vercel Blob
# ────────────────────────────────────────────────────────────────────────────

class TestVercelBlob:
    def test_upload_and_download(self, vercel_blob):
        key     = f"{uid()}.txt"
        content = b"vercel blob data"
        vercel_blob.upload(key, content)
        assert vercel_blob.download(key) == content

    def test_overwrite(self, vercel_blob):
        key = f"{uid()}.txt"
        vercel_blob.upload(key, b"v1")
        vercel_blob.upload(key, b"v2")
        assert vercel_blob.download(key) == b"v2"

    def test_delete(self, vercel_blob):
        key = f"{uid()}.txt"
        vercel_blob.upload(key, b"del")
        vercel_blob.delete(key)
        with pytest.raises(Exception):
            vercel_blob.download(key)