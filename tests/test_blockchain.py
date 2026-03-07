import pytest
import uuid

from polydb.adapters.BlockchainKVAdapter import BlockchainKVAdapter
from polydb.adapters.BlockchainBlobAdapter import BlockchainBlobAdapter
from polydb.adapters.BlockchainQueueAdapter import BlockchainQueueAdapter


pytestmark = pytest.mark.blockchain


def uid():
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────
# KV Storage Tests
# ─────────────────────────────────────────────────────────────────────


class TestBlockchainKV:

    def test_put_and_get(self):

        kv = BlockchainKVAdapter()

        data = {"id": uid(), "name": "blockchain-test", "value": 123}

        kv.put("TestModel", data)

        result = kv.get("TestModel", data["id"])

        assert result["id"] == data["id"]
        assert result["name"] == "blockchain-test"

    def test_delete(self):

        kv = BlockchainKVAdapter()

        data = {"id": uid(), "name": "delete-test"}

        kv.put("TestModel", data)

        kv.delete("TestModel", data["id"])

        result = kv.get("TestModel", data["id"])

        assert result is None


# ─────────────────────────────────────────────────────────────────────
# Blob Storage Tests
# ─────────────────────────────────────────────────────────────────────


class TestBlockchainBlob:

    def test_upload_and_download(self):

        blob = BlockchainBlobAdapter()

        content = b"hello blockchain blob"

        cid = blob.put("file1", content)

        downloaded = blob.get(cid)

        assert downloaded == content

    def test_delete(self):

        blob = BlockchainBlobAdapter()

        cid = blob.put("file2", b"data")

        result = blob.delete(cid)

        assert result["deleted"] is True


# ─────────────────────────────────────────────────────────────────────
# Queue Tests
# ─────────────────────────────────────────────────────────────────────


class TestBlockchainQueue:

    def test_send_and_receive(self):

        queue = BlockchainQueueAdapter()

        message = {"id": uid(), "event": "test_event"}

        queue.send(message)

        messages = queue.receive()

        assert any(m["id"] == message["id"] for m in messages)

    def test_delete(self):

        queue = BlockchainQueueAdapter()

        result = queue.delete("dummy")

        assert result["deleted"] is False
