"""Unit tests (no Azurite needed) for a real, reproduced production bug:
`AzureTableStorageAdapter.put()`/`patch()` (inherited from the base
`NoSQLKVAdapter`, unoverridden before this fix) called the base class's
own `_check_overflow()` BEFORE `_put_raw()` ever ran. For any payload
over `self.max_size` (`AZURE_TABLE_MAX_SIZE = 60KB`, deliberately low so
THIS adapter's own per-property overflow inside `_put_raw` gets a chance
to run per field), `_check_overflow()` replaced the WHOLE record with a
bare `{"_overflow", "_blob_key", "_size", "_checksum"}` reference dict --
discarding every other field (`id`, `tenant_id`, `name`, `description`,
...) before `_put_raw` ever saw the real data. `_put_raw`'s own
reference-entity loop then ALSO stripped those four leftover keys (its
old `if k.startswith("_"): continue` dropped every underscore-prefixed
key, not just the internal model marker) -- so the row that actually
landed in Azure Table carried nothing but `PartitionKey`/`RowKey`/the
model marker. Unfindable by any `id`/`tenant_id`-filtered query
(`_query_raw` filters on literal property names, which no longer
existed) and un-rehydratable on top of that (no `_overflow` flag ever
persisted for `_retrieve_overflow` to key off).

Reproduced live against a real deployment seeding this package's own
bundled OpenAPI integration-template specs (`altcodepro-universal-
interprter`'s own `seed_data/integration_templates/*.json`) into Azure
Table Storage: every spec over ~60KB (the large majority) landed as a
near-empty row; only the few small enough to never trigger `_check_
overflow` at all survived. See that repo's own CLAUDE.md for the full,
live-reported symptom this closes.

This was a real regression surface opened by 2.5.11's own fix
(`NoSQLKVAdapter.put()` now calls `_check_overflow()`) -- correct for
Mongo/Vercel KV, which have nothing better to fall back to, wrong for
Azure, which already has a superior, per-property overflow mechanism.
"""

import threading

import pytest

pytest.importorskip("azure.data.tables")  # adapter needs the SDK to import

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "integration_templates"


class _FakeTableClient:
    """Mirrors tests/test_azure_table_id_property_persisted.py's own real
    filter-evaluating fake -- needed here too, since `_query_raw` builds a
    real OData-shaped filter string this bug lives in."""

    def __init__(self):
        self.store: dict[tuple, dict] = {}

    def upsert_entity(self, entity, **kw):
        self.store[(entity["PartitionKey"], entity["RowKey"])] = dict(entity)

    def get_entity(self, partition_key, row_key):
        try:
            return dict(self.store[(partition_key, row_key)])
        except KeyError:
            raise Exception("ResourceNotFound")

    def query_entities(self, query_filter=None, **kw):
        if query_filter is None:
            return list(self.store.values())
        conditions = []
        for part in query_filter.split(" and "):
            prop, _, raw_val = part.partition(" eq ")
            if raw_val.startswith("'") and raw_val.endswith("'"):
                val = raw_val[1:-1].replace("''", "'")
            elif raw_val == "null":
                val = None
            elif raw_val in ("true", "false"):
                val = raw_val == "true"
            else:
                val = float(raw_val) if "." in raw_val else int(raw_val)
            conditions.append((prop, val))
        return [
            e for e in self.store.values() if all(e.get(prop) == val for prop, val in conditions)
        ]


class _FakeBlobDownload:
    def __init__(self, data: bytes):
        self._data = data

    def readall(self) -> bytes:
        return self._data


class _FakeBlobClient:
    def __init__(self, store: dict, key: str):
        self._store = store
        self._key = key

    def upload_blob(self, data: bytes, overwrite: bool = True):
        self._store[self._key] = data

    def download_blob(self):
        return _FakeBlobDownload(self._store[self._key])


class _FakeBlobService:
    """Stands in for AzureTableStorageAdapter's own `_blob_service` --
    the per-property overflow path (`_blob_upload`/`_blob_download`
    inside `_put_raw`), NOT the base class's generic `object_storage`
    (see `_FakeObjectStorage` below, which this fix proves is never
    touched by `put()`/`patch()` anymore)."""

    def __init__(self):
        self.blobs: dict[str, bytes] = {}

    def get_blob_client(self, container: str, blob_key: str):
        return _FakeBlobClient(self.blobs, blob_key)


class _FakeObjectStorage:
    """The base `NoSQLKVAdapter._check_overflow()`'s own generic overflow
    store. put()/patch() must never reach it for this adapter -- that IS
    the bug this fix closes -- so every method here fails loudly if
    called at all."""

    def put(self, key, data):
        raise AssertionError(
            "object_storage.put() should never be called by AzureTableStorageAdapter's "
            "put()/patch() -- the base class's whole-record _check_overflow() must stay "
            "bypassed for this adapter"
        )

    def get(self, key):
        raise AssertionError("object_storage.get() should never be called either")


def _adapter(fake_table, fake_blob_service=None, fake_object_storage=None):
    adp = object.__new__(AzureTableStorageAdapter)  # bypass live-connection __init__
    adp._get_table_client = lambda model: fake_table
    adp._blob_service = fake_blob_service
    adp.object_storage = fake_object_storage
    adp.container_name = "overflow-container"
    adp.partition_config = None
    adp.max_size = AzureTableStorageAdapter.AZURE_TABLE_MAX_SIZE
    adp._lock = threading.Lock()
    return adp


def _large_spec(size_kb: int) -> dict:
    """A synthetic OpenAPI-spec-shaped payload whose JSON-serialized form
    comfortably exceeds both AzureTableStorageAdapter's own 30KB
    per-property threshold and the base class's 60KB whole-record one --
    matching the real bundled templates that triggered this bug (bigquery
    .json, chargebee.json, ... all multi-hundred-KB to multi-MB)."""
    return {"openapi": "3.0.0", "paths": {"x" * 10: "y" * (size_kb * 1024)}}


def test_put_with_a_large_payload_preserves_every_scalar_field_not_just_the_overflowed_one():
    fake_table = _FakeTableClient()
    fake_blob_service = _FakeBlobService()
    fake_object_storage = _FakeObjectStorage()
    adapter = _adapter(fake_table, fake_blob_service, fake_object_storage)

    spec = _large_spec(100)
    data = {
        "tenant_id": "platform",
        "id": "bigquery",
        "type": "openapi",
        "name": "BigQuery API",
        "description": "desc",
        "version": "v2",
        "spec": spec,
        "credential_keys": ["Oauth2", "Oauth2c"],
    }

    adapter.put(_Model, data)

    # The actual reported bug: the stored row must keep every scalar field,
    # not collapse to just PartitionKey/RowKey/model marker.
    stored = fake_table.store[("platform", "bigquery")]
    assert stored["id"] == "bigquery"
    assert stored["tenant_id"] == "platform"
    assert stored["name"] == "BigQuery API"
    assert stored["type"] == "openapi"

    # The real-world manifestation: an id/tenant_id-filtered query (what
    # IntegrationTemplateStore.get()/DatabaseFactory.read_one() actually
    # issue) must find the row.
    found = adapter._query_raw(_Model, {"tenant_id": "platform", "id": "bigquery"}, None)
    assert len(found) == 1
    assert found[0]["id"] == "bigquery"

    # And a real read-and-rehydrate round trip through the public query()
    # must restore the overflowed "spec" field intact.
    rows = adapter.query(_Model, {"tenant_id": "platform", "id": "bigquery"})
    assert len(rows) == 1
    assert rows[0]["spec"] == spec
    assert rows[0]["name"] == "BigQuery API"


def test_patch_with_a_large_payload_also_preserves_every_scalar_field():
    fake_table = _FakeTableClient()
    fake_blob_service = _FakeBlobService()
    fake_object_storage = _FakeObjectStorage()
    adapter = _adapter(fake_table, fake_blob_service, fake_object_storage)

    spec = _large_spec(80)
    adapter.patch(
        _Model,
        {"partition_key": "platform", "row_key": "chargebee"},
        {
            "tenant_id": "platform",
            "id": "chargebee",
            "type": "openapi",
            "name": "Chargebee",
            "spec": spec,
            "credential_keys": ["BasicAuth"],
        },
    )

    stored = fake_table.store[("platform", "chargebee")]
    assert stored["id"] == "chargebee"
    assert stored["tenant_id"] == "platform"
    assert stored["name"] == "Chargebee"

    found = adapter._query_raw(_Model, {"tenant_id": "platform", "id": "chargebee"}, None)
    assert len(found) == 1

    rows = adapter.query(_Model, {"tenant_id": "platform", "id": "chargebee"})
    assert rows[0]["spec"] == spec


def test_put_never_calls_the_base_classs_whole_record_check_overflow():
    fake_table = _FakeTableClient()
    fake_blob_service = _FakeBlobService()

    put_calls = []

    class _TrackingObjectStorage:
        def put(self, key, data):
            put_calls.append(key)

        def get(self, key):
            raise AssertionError("should never be reached")

    adapter = _adapter(fake_table, fake_blob_service, _TrackingObjectStorage())

    adapter.put(_Model, {"tenant_id": "platform", "id": "gitlab", "spec": _large_spec(200)})

    assert put_calls == []  # the base _check_overflow() path was never touched
    # the real per-property mechanism was used instead (a blob landed in
    # the per-property blob service, keyed by the documented
    # `<pk>_<rk>-<field>-<md5>.json`-shaped scheme, never `overflow/<md5>.json`)
    assert len(fake_blob_service.blobs) == 1
    (blob_key,) = fake_blob_service.blobs.keys()
    assert blob_key.startswith("platform-gitlab-spec-")
    assert not blob_key.startswith("overflow/")


def test_reference_entity_no_longer_strips_overflow_metadata_keys():
    """A narrower, direct proof of the second half of the fix: _put_raw's
    own reference-entity construction loop used to drop EVERY
    underscore-prefixed key (`if k.startswith("_"): continue`), not just
    the internal model marker. Simulates a caller handing _put_raw an
    already-collapsed whole-record overflow reference (the exact shape
    the base class's own _check_overflow() produces) directly, proving
    those keys now survive to the stored entity -- defense in depth for
    any OTHER caller that might still route one through _put_raw, and
    the same fix that lets a real __keymap__ (sanitized-field-name map)
    survive too."""
    fake_table = _FakeTableClient()
    adapter = _adapter(fake_table)

    overflow_reference = {
        "tenant_id": "platform",
        "id": "docusign",
        "_overflow": True,
        "_blob_key": "overflow/deadbeef.json",
        "_size": 999999,
        "_checksum": "deadbeef",
    }
    adapter._put_raw(_Model, "platform", "docusign", overflow_reference)

    stored = fake_table.store[("platform", "docusign")]
    assert stored["_overflow"] is True
    assert stored["_blob_key"] == "overflow/deadbeef.json"
    assert stored["_size"] == 999999
    assert stored["_checksum"] == "deadbeef"
    # And the ordinary scalar fields the earlier fix (2.5.9/2.5.10) already
    # established must keep working are unaffected by this one.
    assert stored["tenant_id"] == "platform"
    assert stored["id"] == "docusign"
