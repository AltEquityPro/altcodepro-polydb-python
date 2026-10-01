"""Unit tests (no Azurite needed) for a real, reproduced data-loss bug.

`NoSQLKVAdapter.put()/patch()` run `_check_overflow()` on the WHOLE record
before calling `_put_raw()`. `AzureTableStorageAdapter` sets `max_size` to
60KB (it has its own per-PROPERTY overflow for strings > 30KB inside
`_put_raw`), so any record above 60KB was replaced by a whole-record stub
`{_overflow, _blob_key, _size, _checksum}`. `_put_raw` then skips every key
starting with "_" when it builds the entity, so the stub's pointer was never
stored and the update's real fields (content_url, version, ...) were never
written -- a silent drop that left only an orphaned blob. Seen live: a
~41KB generated document plus its prompt context pushed an `artifacts` row
over 60KB; `core.db.update` reported success but the re-read row had none
of the fields.
"""

import threading

import pytest

pytest.importorskip("azure.data.tables")

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "artifacts"
    __qualname__ = "artifacts"
    __polydb__ = {"pk_field": "project_slug", "rk_field": "artifact_id"}


class _FakeTableClient:
    def __init__(self):
        self.store = {}

    def upsert_entity(self, entity, **kw):
        key = (entity["PartitionKey"], entity["RowKey"])
        merged = dict(self.store.get(key, {}))
        merged.update(entity)  # azure MERGE semantics
        self.store[key] = merged

    def get_entity(self, pk, rk):
        return dict(self.store[(pk, rk)])


def _adapter():
    fake = _FakeTableClient()
    blobs = {}
    adp = object.__new__(AzureTableStorageAdapter)
    adp.max_size = AzureTableStorageAdapter.AZURE_TABLE_MAX_SIZE
    adp.partition_config = None
    adp._lock = threading.Lock()

    class _ObjStore:  # what the base class' whole-record overflow writes to
        def put(self, key, data):
            blobs[key] = data

        def get(self, key):
            return blobs[key]

    adp.object_storage = _ObjStore()
    adp._blob_service = None
    adp._get_table_client = lambda model: fake
    adp._blob_upload = lambda key, data: blobs.__setitem__(key, data)
    adp._blob_download = lambda key: blobs[key]
    return adp, fake, blobs


def _row(**extra):
    base = {"project_slug": "p-1", "artifact_id": "a-1", "status": "generating", "version": 0}
    base.update(extra)
    return base


def test_patch_of_a_record_over_60kb_keeps_every_field():
    adp, fake, blobs = _adapter()
    adp.put(_Model, _row())

    big = "x" * (45 * 1024)
    adp.patch(
        _Model,
        {"pk": "p-1", "rk": "a-1"},
        {
            "content": big,
            "system_prompt": "y" * (40 * 1024),
            "content_url": "https://example/blob/1",
            "content_blob_key": "blob-1",
            "version": 1,
            "preview_text": "# Title...",
        },
    )

    row = adp._get_raw(_Model, "p-1", "a-1")
    assert row["content"] == big
    assert row["content_url"] == "https://example/blob/1"
    assert row["content_blob_key"] == "blob-1"
    assert row["version"] == 1
    assert row["preview_text"] == "# Title..."
    assert row["status"] == "generating"


def test_later_small_patch_does_not_lose_the_large_fields():
    adp, fake, blobs = _adapter()
    adp.put(_Model, _row())
    big = "z" * (50 * 1024)
    adp.patch(_Model, {"pk": "p-1", "rk": "a-1"}, {"content": big, "system_prompt": "q" * 50000})
    adp.patch(_Model, {"pk": "p-1", "rk": "a-1"}, {"status": "validating"})

    row = adp._get_raw(_Model, "p-1", "a-1")
    assert row["status"] == "validating"
    assert row["content"] == big


def test_no_whole_record_stub_is_ever_written_for_azure():
    adp, fake, blobs = _adapter()
    adp.put(_Model, _row(content="w" * (70 * 1024)))
    stored = fake.store[("p-1", "a-1")]
    assert "_overflow" not in stored and not any(k.startswith("overflow/") for k in blobs)
    assert stored["status"] == "generating"
