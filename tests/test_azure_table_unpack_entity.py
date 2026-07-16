"""Unit tests for AzureTableStorageAdapter._unpack_entity (no Azurite needed).

Contract: PartitionKey/RowKey/__polydb_model__ are Azure Table Storage
internals and must never leak into a returned record. Each gets mapped back
to the domain field it was derived from (the model's declared
partition_key/sort_key — the same fields _get_pk_rk used to write them), and
only fills in that field if it isn't already present from a regular stored
property. Models with no explicit mapping still round-trip cleanly via the
same defaults _get_pk_rk uses (tenant_id / id) instead of erroring.
"""

import pytest

pytest.importorskip("azure.data.tables")  # adapter needs the SDK to import

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _ArtifactComment:
    __name__ = "ArtifactComment"
    __polydb__ = {"partition_key": "artifact_id", "sort_key": "created_at"}


class _Unmapped:
    __name__ = "Unmapped"
    # no __polydb__ at all


def _adapter():
    return object.__new__(AzureTableStorageAdapter)  # bypass live-connection __init__


def test_mapped_model_remaps_pk_rk_and_strips_internals():
    entity = {
        "PartitionKey": "artifact-123",
        "RowKey": "2026-07-13T21_04_53",
        "__polydb_model__": "ArtifactComment",
        "artifact_id": "artifact-123",
        "created_at": "2026-07-13T21:04:53",
        "body": "hello",
    }
    out = _adapter()._unpack_entity(_ArtifactComment, entity)
    assert "PartitionKey" not in out
    assert "RowKey" not in out
    assert "__polydb_model__" not in out
    assert out["artifact_id"] == "artifact-123"
    assert out["created_at"] == "2026-07-13T21:04:53"  # real property wins over RowKey
    assert out["body"] == "hello"


def test_unmapped_model_falls_back_without_erroring():
    entity = {
        "PartitionKey": "t1",
        "RowKey": "row1",
        "__polydb_model__": "Unmapped",
        "email": "user@x.com",
    }
    out = _adapter()._unpack_entity(_Unmapped, entity)
    assert "PartitionKey" not in out
    assert "RowKey" not in out
    assert "__polydb_model__" not in out
    assert out["tenant_id"] == "t1"
    assert out["id"] == "row1"
    assert out["email"] == "user@x.com"


def test_pk_rk_fill_in_only_when_field_missing():
    entity = {
        "PartitionKey": "artifact-999",
        "RowKey": "row-1",
        "__polydb_model__": "ArtifactComment",
        "body": "no explicit artifact_id property",
    }
    out = _adapter()._unpack_entity(_ArtifactComment, entity)
    assert out["artifact_id"] == "artifact-999"
    assert out["created_at"] == "row-1"
