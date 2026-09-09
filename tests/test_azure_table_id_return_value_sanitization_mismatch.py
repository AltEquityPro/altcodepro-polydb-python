"""Unit tests (no Azurite needed) for a real, reproduced bug: `_put_raw`
unconditionally overwrote its own RETURN value's "id" field with `safe_rk`
(the Azure-RowKey-sanitized value) even when the caller's own write payload
already supplied a real "id" -- so whenever that caller-supplied id needed
RowKey sanitization (contains a character Azure forbids in a RowKey: ':',
' ', '#', '?', '/', '\\'), the PERSISTED entity's "id" property (written
verbatim from the caller's own `data`, untouched) and the RETURNED value's
"id" (always `safe_rk`) silently disagreed.

This is a distinct bug from the one `test_azure_table_id_property_persisted.py`
already covers and fixed (2.5.9: a create with NO "id" in its own payload
never landed a real "id" property at all) -- that fix's own condition
(`if "id" not in data: data = {**data, "id": safe_rk}`) only ever touches
the WRITE side. The return-value line right below it (`restored["id"] =
safe_rk`) had no matching condition, so it fired even when `data["id"]`
was already present and different from `safe_rk`.

Real, reproduced downstream symptom (altcodepro-blueprint-engine): the
`artifacts` model deliberately builds a colon-joined deterministic id
(`project_id + ':' + definition_id`) with `rk_field` pointing at that same
value. `core.db.create_many`'s own CEL-built row data already supplies
this "id" explicitly. The persisted entity's "id" property therefore kept
the real, colon-joined value -- but every returned row's own "id" (what a
workflow's `steps.<create_step>.created.map(c, ...c.id...)` reads to
enqueue a follow-up job) came back with the colon sanitized to an
underscore instead. The follow-up job's own `core.db.get(id=<that wrong,
sanitized value>)` then built an OData `id eq '<sanitized>'` filter that
matched nothing -- every real row existed, but was unreachable by the very
id its own creation step just handed out.
"""

import pytest

pytest.importorskip("azure.data.tables")  # adapter needs the SDK to import

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "artifacts"


class _FakeTableClient:
    """The same real filter-evaluating fake
    test_azure_table_id_property_persisted.py already establishes -- reused
    verbatim rather than re-derived, since this bug lives in the identical
    `_query_raw` OData-filter mechanism."""

    def __init__(self):
        self.store: dict[tuple, dict] = {}

    def upsert_entity(self, entity, **kw):
        self.store[(entity["PartitionKey"], entity["RowKey"])] = dict(entity)

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


def _adapter(fake):
    adp = object.__new__(AzureTableStorageAdapter)  # bypass live-connection __init__
    adp._get_table_client = lambda model: fake
    adp._blob_service = None
    adp.object_storage = None
    return adp


def test_a_caller_supplied_id_needing_rowkey_sanitization_is_returned_unsanitized():
    """The actual reported bug, reproduced with the real shape:
    project_id + ':' + definition_id, used as both "id" and the rk_field
    value -- ':' is not a legal Azure RowKey character."""
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    caller_id = "485533b9-8d0f-4e5f-92da-670cedfe92f5:backend.backend_schema"
    data = {
        "id": caller_id,
        "artifact_id": caller_id,
        "project_slug": "community-platform-187",
        "tenant_id": "altcodepro",
        "name": "Backend Schema",
    }
    created = adapter._put_raw(_Model, "community-platform-187", caller_id, data)

    # The load-bearing assertion: the RETURN value's "id" must equal what
    # the caller actually supplied -- never the sanitized RowKey. Before
    # the fix this was "485533b9-...-670cedfe92f5_backend.backend_schema"
    # (colon silently replaced with underscore).
    assert created["id"] == caller_id

    # And it must agree with what's genuinely persisted -- the real
    # invariant this bug violated (two different strings for the same
    # row's own "id").
    safe_rk = adapter._sanitize_pk_rk(caller_id)
    stored = fake.store[("community-platform-187", safe_rk)]
    assert stored["id"] == caller_id == created["id"]

    # The actual downstream failure this caused: a follow-up step (e.g.
    # start_artifact_generation's own load_artifact) doing
    # core.db.get(id=<the id create_many just returned>) must find the
    # row. Reproduced BROKEN before this fix -- querying by the correct,
    # returned id found nothing, because the stored property and the
    # returned value disagreed.
    found = adapter._query_raw(_Model, {"id": created["id"], "tenant_id": "altcodepro"}, None)
    assert len(found) == 1
    assert found[0]["name"] == "Backend Schema"


def test_a_caller_supplied_id_needing_no_sanitization_is_unaffected():
    """Negative control: when the caller's own id is already RowKey-safe,
    safe_rk == the caller's id anyway, so this bug was invisible -- exactly
    why it slipped past every existing test until now."""
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    data = {"id": "caller-chosen-id", "tenant_id": "t1", "name": "x"}
    created = adapter._put_raw(_Model, "t1", "caller-chosen-id", data)

    assert created["id"] == "caller-chosen-id"
    stored = fake.store[("t1", "caller-chosen-id")]
    assert stored["id"] == "caller-chosen-id"


def test_no_id_in_payload_still_synthesizes_safe_rk_onto_both_sides():
    """The 2.5.9 case must still work identically -- this fix only adds a
    condition around the return-value line, it must not touch the
    no-id-supplied path at all."""
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    data = {"identity_key": "email:x@example.com", "tenant_id": "altcodepro"}
    created = adapter._put_raw(_Model, "email:x@example.com", "email:x@example.com", data)

    assert created["id"] == "email_x@example.com"
    stored = fake.store[("email_x@example.com", "email_x@example.com")]
    assert stored["id"] == created["id"] == "email_x@example.com"
