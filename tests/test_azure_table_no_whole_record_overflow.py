"""A record over the 60 KB base-class limit must reach _put_raw intact on Azure Table.

Story: a generated 'Product Requirements' document (large content plus a large stored prompt)
failed to save through patch() while every smaller row saved. The base class replaced the whole
record with an overflow stub, and Azure's _put_raw drops '_' keys, so no field was written.
"""

import threading

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter


class _Model:
    __polydb__ = {"storage": "nosql", "pk_field": "tenant_id", "rk_field": "id"}


def _adapter():
    a = AzureTableStorageAdapter.__new__(AzureTableStorageAdapter)
    a._lock = threading.RLock()
    a.max_size = AzureTableStorageAdapter.AZURE_TABLE_MAX_SIZE
    a.partition_config = None

    class _NoObjectStorage:
        def put(self, *_a, **_k):
            raise AssertionError("whole-record overflow must not be used on Azure Table")

    a.object_storage = _NoObjectStorage()
    written = []
    a._get_raw = lambda m, pk, rk: {"id": "x", "status": "generating", "name": "PRD"}
    a._put_raw = lambda m, pk, rk, d: (written.append(d), d)[1]
    return a, written


def test_patch_of_a_record_over_60kb_writes_every_field():
    a, written = _adapter()
    a.patch(
        _Model,
        {"tenant_id": "t", "id": "x"},
        {
            "tenant_id": "t",
            "content": "a" * 40000,
            "system_prompt": "b" * 30000,
            "status": "complete",
        },
    )
    row = written[0]
    assert row["status"] == "complete" and row["name"] == "PRD"
    assert len(row["content"]) == 40000 and len(row["system_prompt"]) == 30000
    assert "_overflow" not in row


def test_put_of_a_record_over_60kb_writes_every_field():
    a, written = _adapter()
    a.put(_Model, {"tenant_id": "t", "id": "y", "content": "a" * 70000})
    assert len(written[0]["content"]) == 70000 and "_overflow" not in written[0]
