"""
tests/test_mongodb.py
=====================
Integration tests for MongoDBAdapter (via local Mongo on port 27018).

Covers:
  - put / query / patch / upsert / delete
  - query with filters, limit, no_cache / cache_ttl pass-through
  - query_page with continuation tokens
  - query_linq (WHERE / ORDER BY / LIMIT / COUNT)
  - partition config (collection routing)
  - etag / optimistic-concurrency (if supported)
  - error paths
"""

from __future__ import annotations

import pytest

from conftest import uid
from polydb.errors import DatabaseError
from polydb.query import Operator, QueryBuilder

pytestmark = pytest.mark.mongodb


# ────────────────────────────────────────────────────────────────────────────
# Sentinel model type — MongoDBAdapter expects a class or string as the
# first arg (used to derive the collection name in most adapters).
# ────────────────────────────────────────────────────────────────────────────


class PolyItem:
    """Sentinel model for testing."""


class PolyArchive:
    """Alternate model to test collection isolation."""


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


def item(**extra) -> dict:
    return {"id": uid(), "name": f"item-{uid()}", "value": 42, **extra}


# ────────────────────────────────────────────────────────────────────────────
# PUT  (insert / create)
# ────────────────────────────────────────────────────────────────────────────


class TestMongoPut:
    def test_put_returns_row(self, mongo_nosql):
        data = item()
        result = mongo_nosql.put(PolyItem, data)
        assert result["id"] == data["id"]
        assert result["name"] == data["name"]

    def test_put_nested_dict(self, mongo_nosql):
        data = item(meta={"score": 7, "labels": ["a", "b"]})
        result = mongo_nosql.put(PolyItem, data)
        assert result["meta"]["score"] == 7
        assert result["meta"]["labels"] == ["a", "b"]

    def test_put_null_field(self, mongo_nosql):
        data = item(deleted_at=None)
        result = mongo_nosql.put(PolyItem, data)
        assert "deleted_at" in result

    def test_put_list_field(self, mongo_nosql):
        data = item(tags=["x", "y"])
        result = mongo_nosql.put(PolyItem, data)
        assert result["tags"] == ["x", "y"]

    def test_put_separate_collections(self, mongo_nosql):
        """PolyItem and PolyArchive must be stored in separate collections."""
        d1 = item(name="in-items")
        d2 = item(name="in-archive")
        mongo_nosql.put(PolyItem, d1)
        mongo_nosql.put(PolyArchive, d2)

        items_res = mongo_nosql.query(PolyItem, {"id": d1["id"]})
        archive_res = mongo_nosql.query(PolyArchive, {"id": d2["id"]})
        cross_items = mongo_nosql.query(PolyItem, {"id": d2["id"]})
        cross_archive = mongo_nosql.query(PolyArchive, {"id": d1["id"]})

        assert len(items_res) == 1
        assert len(archive_res) == 1
        assert cross_items == []
        assert cross_archive == []


# ────────────────────────────────────────────────────────────────────────────
# QUERY
# ────────────────────────────────────────────────────────────────────────────


class TestMongoQuery:
    def test_query_all(self, mongo_nosql):
        mongo_nosql.put(PolyItem, item())
        mongo_nosql.put(PolyItem, item())
        rows = mongo_nosql.query(PolyItem)
        assert len(rows) >= 2

    def test_query_by_field(self, mongo_nosql):
        unique = f"q-{uid()}"
        mongo_nosql.put(PolyItem, item(name=unique))
        rows = mongo_nosql.query(PolyItem, {"name": unique})
        assert len(rows) == 1
        assert rows[0]["name"] == unique

    def test_query_returns_empty(self, mongo_nosql):
        rows = mongo_nosql.query(PolyItem, {"id": "ghost"})
        assert rows == []

    def test_query_limit(self, mongo_nosql):
        for _ in range(5):
            mongo_nosql.put(PolyItem, item())
        rows = mongo_nosql.query(PolyItem, limit=2)
        assert len(rows) == 2

    def test_query_no_cache_flag(self, mongo_nosql):
        """no_cache=True must not raise — adapter should accept the kwarg."""
        mongo_nosql.put(PolyItem, item(name="nocache"))
        rows = mongo_nosql.query(PolyItem, {"name": "nocache"}, no_cache=True)
        assert len(rows) >= 1

    def test_query_nested_field(self, mongo_nosql):
        unique = uid()
        mongo_nosql.put(PolyItem, item(meta={"ref": unique}))
        rows = mongo_nosql.query(PolyItem, {"meta.ref": unique})
        assert len(rows) == 1


# ────────────────────────────────────────────────────────────────────────────
# QUERY PAGE
# ────────────────────────────────────────────────────────────────────────────


class TestMongoQueryPage:
    def test_pages_no_overlap(self, mongo_nosql):
        tag = uid()
        for _ in range(7):
            mongo_nosql.put(PolyItem, item(tenant_id=tag))

        page1, tok1 = mongo_nosql.query_page(PolyItem, {"tenant_id": tag}, 3)
        assert len(page1) == 3
        assert tok1 is not None

        page2, tok2 = mongo_nosql.query_page(PolyItem, {"tenant_id": tag}, 3, tok1)
        assert len(page2) == 3

        page3, tok3 = mongo_nosql.query_page(PolyItem, {"tenant_id": tag}, 3, tok2)
        assert len(page3) == 1
        assert tok3 is None

        ids1 = {r["id"] for r in page1}
        ids2 = {r["id"] for r in page2}
        ids3 = {r["id"] for r in page3}
        assert ids1.isdisjoint(ids2)
        assert ids1.isdisjoint(ids3)
        assert ids2.isdisjoint(ids3)


# ────────────────────────────────────────────────────────────────────────────
# PATCH
# ────────────────────────────────────────────────────────────────────────────


class TestMongoPatch:
    def test_patch_updates_field(self, mongo_nosql):
        row = mongo_nosql.put(PolyItem, item(value=1))
        updated = mongo_nosql.patch(PolyItem, row["id"], {"value": 999})
        assert updated["value"] == 999

    def test_patch_partial_keeps_other_fields(self, mongo_nosql):
        row = mongo_nosql.put(PolyItem, item(name="keep-me", value=5))
        mongo_nosql.patch(PolyItem, row["id"], {"value": 50})
        rows = mongo_nosql.query(PolyItem, {"id": row["id"]})
        assert rows[0]["name"] == "keep-me"

    def test_patch_nested_dict(self, mongo_nosql):
        row = mongo_nosql.put(PolyItem, item(meta={"k": "old"}))
        mongo_nosql.patch(PolyItem, row["id"], {"meta": {"k": "new"}})
        rows = mongo_nosql.query(PolyItem, {"id": row["id"]})
        assert rows[0]["meta"]["k"] == "new"

    def test_patch_replace_mode(self, mongo_nosql):
        """replace=True should overwrite the whole document."""
        row = mongo_nosql.put(PolyItem, item(name="original", value=1))
        replacement = {"id": row["id"], "name": "replaced"}
        mongo_nosql.patch(PolyItem, row["id"], replacement, replace=True)
        rows = mongo_nosql.query(PolyItem, {"id": row["id"]})
        # 'value' should be gone (or None) after replace
        assert rows[0].get("name") == "replaced"


# ────────────────────────────────────────────────────────────────────────────
# UPSERT
# ────────────────────────────────────────────────────────────────────────────


class TestMongoUpsert:
    def test_upsert_creates_new(self, mongo_nosql):
        data = item(name="upsert-new")
        result = mongo_nosql.upsert(PolyItem, data)
        assert result["id"] == data["id"]

    def test_upsert_updates_existing(self, mongo_nosql):
        data = item(value=1)
        mongo_nosql.put(PolyItem, data)
        data["value"] = 2
        result = mongo_nosql.upsert(PolyItem, data)
        assert result["value"] == 2

    def test_upsert_idempotent(self, mongo_nosql):
        data = item()
        mongo_nosql.upsert(PolyItem, data)
        mongo_nosql.upsert(PolyItem, data)
        rows = mongo_nosql.query(PolyItem, {"id": data["id"]})
        assert len(rows) == 1


# ────────────────────────────────────────────────────────────────────────────
# DELETE
# ────────────────────────────────────────────────────────────────────────────


class TestMongoDelete:
    def test_delete_removes_document(self, mongo_nosql):
        row = mongo_nosql.put(PolyItem, item())
        mongo_nosql.delete(PolyItem, row["id"])
        remaining = mongo_nosql.query(PolyItem, {"id": row["id"]})
        assert remaining == []

    def test_delete_returns_deleted_document(self, mongo_nosql):
        row = mongo_nosql.put(PolyItem, item(name="to-delete"))
        deleted = mongo_nosql.delete(PolyItem, row["id"])
        assert deleted["id"] == row["id"]

    def test_delete_nonexistent_raises(self, mongo_nosql):
        with pytest.raises(DatabaseError):
            mongo_nosql.delete(PolyItem, "ghost-id")


# ────────────────────────────────────────────────────────────────────────────
# LINQ
# ────────────────────────────────────────────────────────────────────────────


class TestMongoLinq:
    def _seed(self, mongo_nosql, n: int = 5, tag: str | None = None) -> list[dict]:
        tag = tag or uid()
        rows = []
        for i in range(n):
            rows.append(mongo_nosql.put(PolyItem, item(value=i * 10, tenant_id=tag)))
        return rows

    def test_linq_where_eq(self, mongo_nosql):
        tag = uid()
        self._seed(mongo_nosql, tag=tag)
        mongo_nosql.put(PolyItem, item(name="linq-target", value=777, tenant_id=tag))
        qb = QueryBuilder().where("name", Operator.EQ, "linq-target")
        results = mongo_nosql.query_linq(PolyItem, qb)
        assert any(r["value"] == 777 for r in results)

    def test_linq_count(self, mongo_nosql):
        tag = uid()
        self._seed(mongo_nosql, 4, tag=tag)
        qb = QueryBuilder().where("tenant_id", "", tag).count()
        total = mongo_nosql.query_linq(PolyItem, qb)
        assert isinstance(total, int)
        assert total == 4

    def test_linq_take(self, mongo_nosql):
        for _ in range(6):
            mongo_nosql.put(PolyItem, item())
        qb = QueryBuilder().take(3)
        results = mongo_nosql.query_linq(PolyItem, qb)
        assert len(results) <= 3
