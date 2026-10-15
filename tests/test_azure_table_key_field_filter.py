"""An equality on the model's pk/rk field must be sent as PartitionKey/RowKey, not as a property filter
(a property filter scans the whole table)."""
import pytest

pytest.importorskip("azure.data.tables")

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "artifacts"
    __polydb__ = {"pk_field": "project_slug", "rk_field": "artifact_id"}


class _Fake:
    def __init__(self):
        self.calls = []

    def query_entities(self, query_filter=None, **kw):
        self.calls.append(query_filter)
        return []


def _adapter(fake, partition_config=None):
    a = object.__new__(AzureTableStorageAdapter)
    a._get_table_client = lambda model: fake
    a.partition_config = partition_config
    return a


def test_pk_field_equality_becomes_partition_key():
    f = _Fake()
    _adapter(f)._query_raw(_Model, {"project_id": "p1", "project_slug": "p1", "tenant_id": "t"}, 500)
    flt = f.calls[0]
    assert "PartitionKey eq 'p1'" in flt
    assert "project_slug eq 'p1'" in flt  # exactness filter kept
    assert "project_id eq 'p1'" in flt and "tenant_id eq 't'" in flt


def test_rk_field_equality_becomes_row_key():
    f = _Fake()
    _adapter(f)._query_raw(_Model, {"artifact_id": "a1"}, 1)
    assert f.calls[0] == "RowKey eq 'a1' and artifact_id eq 'a1'"


def test_partition_config_keeps_property_filter():
    f = _Fake()
    _adapter(f, partition_config=object())._query_raw(_Model, {"project_slug": "p1"}, 1)
    assert "PartitionKey" not in f.calls[0]


def test_key_value_is_sanitized_like_the_write_path():
    f = _Fake()
    _adapter(f)._query_raw(_Model, {"project_slug": "a:b"}, 1)
    assert "PartitionKey eq 'a_b'" in f.calls[0]
