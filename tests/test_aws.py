"""
tests/test_aws.py
=================
Integration tests for all AWS adapters (via LocalStack on port 4566).

  DynamoDBAdapter  → get_nosql_kv()
  S3Adapter        → get_object_storage()
  SQSAdapter       → get_queue()

LocalStack credentials / endpoint are loaded from env:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_ENDPOINT_URL
"""

from __future__ import annotations

import time

import pytest

from conftest import uid
from polydb.errors import DatabaseError
from polydb.query import Operator, QueryBuilder

pytestmark = pytest.mark.aws


# ────────────────────────────────────────────────────────────────────────────
# Sentinel models
# ────────────────────────────────────────────────────────────────────────────


class DynItem:
    """Maps to a DynamoDB table."""


class DynArchive:
    """Separate DynamoDB table."""


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


def ditem(**extra) -> dict:
    return {"id": uid(), "name": f"aws-{uid()}", "value": 1, **extra}


# ────────────────────────────────────────────────────────────────────────────
# DYNAMODB
# ────────────────────────────────────────────────────────────────────────────


class TestDynamoDB:
    def test_put_and_query(self, aws_nosql):
        data = ditem()
        result = aws_nosql.put(DynItem, data)
        assert result["id"] == data["id"]
        rows = aws_nosql.query(DynItem, {"id": data["id"]})
        assert len(rows) == 1

    def test_put_nested_dict(self, aws_nosql):
        data = ditem(meta={"a": 1, "b": [1, 2, 3]})
        aws_nosql.put(DynItem, data)
        rows = aws_nosql.query(DynItem, {"id": data["id"]})
        assert rows[0]["meta"]["a"] == 1

    def test_put_list_field(self, aws_nosql):
        data = ditem(tags=["t1", "t2"])
        aws_nosql.put(DynItem, data)
        rows = aws_nosql.query(DynItem, {"id": data["id"]})
        assert rows[0]["tags"] == ["t1", "t2"]

    def test_patch(self, aws_nosql):
        row = aws_nosql.put(DynItem, ditem(value=5))
        aws_nosql.patch(DynItem, row["id"], {"value": 50})
        rows = aws_nosql.query(DynItem, {"id": row["id"]})
        assert rows[0]["value"] == 50

    def test_patch_partial_keeps_fields(self, aws_nosql):
        row = aws_nosql.put(DynItem, ditem(name="keep", value=1))
        aws_nosql.patch(DynItem, row["id"], {"value": 99})
        rows = aws_nosql.query(DynItem, {"id": row["id"]})
        assert rows[0]["name"] == "keep"

    def test_upsert_insert_then_update(self, aws_nosql):
        data = ditem(value=10)
        aws_nosql.upsert(DynItem, data)
        data["value"] = 20
        aws_nosql.upsert(DynItem, data)
        rows = aws_nosql.query(DynItem, {"id": data["id"]})
        assert rows[0]["value"] == 20

    def test_delete_removes_item(self, aws_nosql):
        row = aws_nosql.put(DynItem, ditem())
        aws_nosql.delete(DynItem, row["id"])
        rows = aws_nosql.query(DynItem, {"id": row["id"]})
        assert rows == []

    def test_delete_returns_deleted(self, aws_nosql):
        row = aws_nosql.put(DynItem, ditem(name="bye"))
        deleted = aws_nosql.delete(DynItem, row["id"])
        assert deleted["id"] == row["id"]

    def test_delete_nonexistent_raises(self, aws_nosql):
        with pytest.raises(DatabaseError):
            aws_nosql.delete(DynItem, "ghost")

    def test_query_limit(self, aws_nosql):
        for _ in range(5):
            aws_nosql.put(DynItem, ditem())
        rows = aws_nosql.query(DynItem, limit=2)
        assert len(rows) <= 2

    def test_query_page(self, aws_nosql):
        tag = uid()
        for _ in range(6):
            aws_nosql.put(DynItem, ditem(tenant_id=tag))
        page1, tok = aws_nosql.query_page(DynItem, {"tenant_id": tag}, 3)
        assert 1 <= len(page1) <= 3
        if tok:
            page2, _ = aws_nosql.query_page(DynItem, {"tenant_id": tag}, 3, tok)
            combined = {r["id"] for r in page1} | {r["id"] for r in page2}
            assert len(combined) >= 4

    def test_separate_tables_isolated(self, aws_nosql):
        d1 = ditem()
        d2 = ditem()
        aws_nosql.put(DynItem, d1)
        aws_nosql.put(DynArchive, d2)
        assert aws_nosql.query(DynItem, {"id": d2["id"]}) == []
        assert aws_nosql.query(DynArchive, {"id": d1["id"]}) == []

    def test_linq_where_eq(self, aws_nosql):
        name = f"linq-{uid()}"
        aws_nosql.put(DynItem, ditem(name=name, value=321))
        qb = QueryBuilder().where("name", Operator.EQ, name)
        results = aws_nosql.query_linq(DynItem, qb)
        assert any(r["value"] == 321 for r in results)

    def test_linq_count(self, aws_nosql):
        tag = uid()
        for _ in range(3):
            aws_nosql.put(DynItem, ditem(tenant_id=tag))
        qb = QueryBuilder().where("tenant_id", "", tag).count()
        total = aws_nosql.query_linq(DynItem, qb)
        assert isinstance(total, int)
        assert total == 3


# ────────────────────────────────────────────────────────────────────────────
# S3
# ────────────────────────────────────────────────────────────────────────────


class TestS3:
    def test_upload_and_download(self, aws_blob):
        key = f"test/{uid()}.txt"
        content = b"hello s3 localstack"
        aws_blob.upload(key, content)
        assert aws_blob.download(key) == content

    def test_upload_overwrite(self, aws_blob):
        key = f"test/{uid()}.txt"
        aws_blob.upload(key, b"v1")
        aws_blob.upload(key, b"v2")
        assert aws_blob.download(key) == b"v2"

    def test_delete_object(self, aws_blob):
        key = f"test/{uid()}.txt"
        aws_blob.upload(key, b"del")
        aws_blob.delete(key)
        with pytest.raises(Exception):
            aws_blob.download(key)

    def test_list_objects(self, aws_blob):
        prefix = f"listtest-{uid()}/"
        keys = [f"{prefix}{uid()}.bin" for _ in range(3)]
        for k in keys:
            aws_blob.upload(k, b"data")
        listed = aws_blob.list(prefix)
        assert all(k in listed for k in keys)

    def test_upload_binary(self, aws_blob):
        key = f"test/{uid()}.bin"
        content = bytes(range(256))
        aws_blob.upload(key, content)
        assert aws_blob.download(key) == content

    def test_upload_large(self, aws_blob):
        key = f"test/{uid()}.bin"
        content = b"X" * (5 * 1024 * 1024)  # 5 MB
        aws_blob.upload(key, content)
        assert aws_blob.download(key) == content


# ────────────────────────────────────────────────────────────────────────────
# SQS
# ────────────────────────────────────────────────────────────────────────────


class TestSQS:
    def test_send_and_receive(self, aws_queue):
        body = f"sqs-{uid()}"
        aws_queue.send(body)
        received = aws_queue.receive(max_messages=10)
        bodies = [m["body"] if isinstance(m, dict) else m for m in received]
        assert body in bodies

    def test_send_multiple_messages(self, aws_queue):
        msgs = [f"m-{uid()}" for _ in range(3)]
        for m in msgs:
            aws_queue.send(m)
        received = aws_queue.receive(max_messages=10)
        bodies = [m["body"] if isinstance(m, dict) else m for m in received]
        assert all(m in bodies for m in msgs)

    def test_delete_message(self, aws_queue):
        aws_queue.send(f"del-{uid()}")
        received = aws_queue.receive(max_messages=1)
        assert received
        msg = received[0]
        if isinstance(msg, dict) and "receipt_handle" in msg:
            aws_queue.delete(msg["receipt_handle"])

    def test_receive_empty_queue(self, aws_queue):
        """Receive on an empty queue must return an empty list, not raise."""
        # Drain first
        aws_queue.receive(max_messages=10)
        result = aws_queue.receive(max_messages=1)
        assert isinstance(result, list)
