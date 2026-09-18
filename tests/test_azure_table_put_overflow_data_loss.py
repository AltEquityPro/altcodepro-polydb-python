"""Unit tests (no Azurite needed) for a real, reproduced production bug:
`NoSQLKVAdapter._check_overflow()` (base class, shared by every NoSQL
adapter) replaced an oversized record's WHOLE body with a bare
`{"_overflow", "_blob_key", "_size", "_checksum"}` reference dict before
handing it to `_put_raw` -- discarding every other field (`id`,
`tenant_id`, `name`, `description`, ...) from the row actually persisted,
even though the module's own docs already documented (but never
implemented) "scalar fields are copied onto the reference row". For
`AzureTableStorageAdapter` specifically, `_put_raw`'s own reference-entity
loop then ALSO stripped the four bookkeeping keys `_check_overflow` DID
keep (its old `if k.startswith("_"): continue` dropped every
underscore-prefixed key, not just the internal model marker) -- so the
row that actually landed in Azure Table carried nothing but
`PartitionKey`/`RowKey`/the model marker. Unfindable by any
`id`/`tenant_id`-filtered query (`_query_raw` filters on literal property
names, which no longer existed) and un-rehydratable on top of that (no
`_overflow` flag ever persisted for a later `_retrieve_overflow()` to key
off).

Reproduced live against a real deployment seeding this package's own
bundled OpenAPI integration-template specs (`altcodepro-universal-
interprter`'s own `seed_data/integration_templates/*.json`) into Azure
Table Storage: the four specs whose COMPACT (`json.dumps`) size stayed
under Azure's own 60KB whole-record threshold (`clicksend`, `google-maps`,
`google-search-console`, `notion`) kept working; every other spec, all
over that threshold, became unfindable by its own id/tenant_id-filtered
catalog lookup -- a byte-for-byte match between "which specs are over
60KB" and "which specs went missing" confirmed directly against the real
bundled files, not assumed. See that repo's own CLAUDE.md for the full
incident writeup.

**Fixed with two changes, deliberately keeping `_check_overflow()` in the
write path for every adapter, Azure included** -- an earlier version of
this fix instead made `AzureTableStorageAdapter.put()`/`patch()` skip the
base class's `_check_overflow()` call entirely, which would have silently
removed this adapter's own last-resort guarantee that a record of ANY
size can always be written via whole-record blob overflow, never
rejected. That approach was reverted. The real fix is in
`NoSQLKVAdapter._check_overflow()` itself (base/NoSQLKVAdapter.py): its
reference dict now also carries a best-effort copy of every small scalar
field from the original record (see that method's own docstring for the
exact rule), so the persisted row stays queryable by id/tenant_id even
after its bulk payload has moved to blob storage. `AzureTableStorageAdapter
._put_raw`'s own reference-entity loop now skips only the internal
model-marker key (`_MODEL_FIELD`) instead of every underscore-prefixed
key, so those bookkeeping fields (and a real `__keymap__`, when one
exists) actually reach the persisted entity -- without this second fix,
`_overflow`/`_blob_key` themselves would still be stripped, and a later
read would find the row (thanks to the first fix) but never rehydrate its
real content, silently returning an incomplete record instead of the
full one.
"""

import json
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


class _FakeObjectStorage:
    """The base `NoSQLKVAdapter._check_overflow()`'s own generic
    whole-record overflow store -- `put()`/`patch()` MUST still reach
    this on a large payload (that's the whole point of this fix: never
    skip it), so this fake actually stores and serves blobs instead of
    asserting it's never called."""

    def __init__(self):
        self.blobs: dict[str, bytes] = {}

    def put(self, key, data):
        self.blobs[key] = data

    def get(self, key):
        return self.blobs[key]


def _adapter(fake_table, fake_object_storage=None):
    adp = object.__new__(AzureTableStorageAdapter)  # bypass live-connection __init__
    adp._get_table_client = lambda model: fake_table
    adp._blob_service = None  # per-property overflow path unused by these tests
    adp.object_storage = fake_object_storage or _FakeObjectStorage()
    adp.container_name = "overflow-container"
    adp.partition_config = None
    adp.max_size = AzureTableStorageAdapter.AZURE_TABLE_MAX_SIZE
    adp._lock = threading.Lock()
    return adp


def _large_spec(size_kb: int) -> dict:
    """A synthetic OpenAPI-spec-shaped payload whose JSON-serialized form
    comfortably exceeds the base class's own whole-record overflow
    threshold (self.max_size) -- matching the real bundled templates that
    triggered this bug (bigquery.json, chargebee.json, ... all
    multi-hundred-KB to multi-MB)."""
    return {"openapi": "3.0.0", "paths": {"x" * 10: "y" * (size_kb * 1024)}}


def test_put_with_a_large_payload_still_writes_the_full_record_to_blob():
    """The core guarantee this fix must never remove: _check_overflow()
    stays in the write path, so a record of any size can always be
    written via whole-record blob overflow, never rejected."""
    fake_table = _FakeTableClient()
    fake_object_storage = _FakeObjectStorage()
    adapter = _adapter(fake_table, fake_object_storage)

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

    assert len(fake_object_storage.blobs) == 1
    (blob_key,) = fake_object_storage.blobs.keys()
    assert blob_key.startswith("overflow/")
    stored_blob = json.loads(fake_object_storage.blobs[blob_key])
    assert stored_blob == data  # the FULL original record, untouched


def test_put_with_a_large_payload_preserves_every_scalar_field_not_just_the_overflowed_one():
    fake_table = _FakeTableClient()
    adapter = _adapter(fake_table)

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

    # The actual reported bug: the stored row must keep every small scalar
    # field, not collapse to just PartitionKey/RowKey/model marker/blob ref.
    stored = fake_table.store[("platform", "bigquery")]
    assert stored["id"] == "bigquery"
    assert stored["tenant_id"] == "platform"
    assert stored["name"] == "BigQuery API"
    assert stored["type"] == "openapi"
    assert stored["_overflow"] is True

    # The real-world manifestation: an id/tenant_id-filtered query (what
    # IntegrationTemplateStore.get()/DatabaseFactory.read_one() actually
    # issue) must find the row.
    found = adapter._query_raw(_Model, {"tenant_id": "platform", "id": "bigquery"}, None)
    assert len(found) == 1
    assert found[0]["id"] == "bigquery"

    # And a real read-and-rehydrate round trip through the public query()
    # (inherited from the base class, which calls _retrieve_overflow() on
    # every row) must restore the overflowed "spec" field intact.
    rows = adapter.query(_Model, {"tenant_id": "platform", "id": "bigquery"})
    assert len(rows) == 1
    assert rows[0]["spec"] == spec
    assert rows[0]["name"] == "BigQuery API"


def test_patch_with_a_large_payload_also_preserves_every_scalar_field_and_writes_to_blob():
    fake_table = _FakeTableClient()
    fake_object_storage = _FakeObjectStorage()
    adapter = _adapter(fake_table, fake_object_storage)

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

    assert len(fake_object_storage.blobs) == 1

    stored = fake_table.store[("platform", "chargebee")]
    assert stored["id"] == "chargebee"
    assert stored["tenant_id"] == "platform"
    assert stored["name"] == "Chargebee"

    found = adapter._query_raw(_Model, {"tenant_id": "platform", "id": "chargebee"}, None)
    assert len(found) == 1

    rows = adapter.query(_Model, {"tenant_id": "platform", "id": "chargebee"})
    assert rows[0]["spec"] == spec


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
