"""Unit tests (no Azurite needed) for a real, reproduced bug: `_put_raw`
never persisted a real "id" property on the entity unless the caller's own
write payload already had one -- only synthesized it onto that one call's
own RETURN value (`restored["id"] = safe_rk`). A later id-addressed lookup
(`read_one(model, {"id": X, ...})`, the "before" read every
`DatabaseFactory.update()`/`patch()` call makes) builds a real OData
`id eq 'X'` filter against whatever property is actually named "id" on the
stored entity -- which, for any model whose caller doesn't explicitly set
"id" at create time (default pk_field/rk_field or a custom deterministic
mapping alike), never existed. `before` therefore always came back None,
cascading into `DatabaseFactory.update()`'s own pk/rk recovery logic
finding nothing to recover from and falling through to a literal
`str(None) == "None"` PartitionKey -- the same class of bug this file's own
2.5.7 changelog entry already documents for a related codepath.

Reproduces the bug on the OLD code shape directly (a create whose own
payload has no "id" never lands a real "id" property), then proves the fix
closes it (a create with no "id" DOES land a real, filterable "id"
property equal to the physical RowKey, and a create that ALREADY supplies
its own "id" is left completely untouched).
"""

import pytest

pytest.importorskip("azure.data.tables")  # adapter needs the SDK to import

from polydb.adapters.AzureTableStorageAdapter import AzureTableStorageAdapter  # noqa: E402


class _Model:
    __name__ = "users"


class _FakeTableClient:
    """A minimal, real filter-evaluating fake -- unlike
    test_azure_table_empty_filter.py's own fake (which only needs to tell
    a None filter apart from a non-None one), this one has to actually
    match `_query_raw`'s real OData-shaped filter string against stored
    entities, since that's the exact mechanism this bug lives in."""

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


def test_a_create_with_no_id_in_its_own_payload_still_lands_a_real_filterable_id_property():
    """The actual bug: users.identity_key-style deterministic addressing
    (pk_field=rk_field="identity_key", "id" never in the create payload)."""
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    data = {
        "identity_key": "email:sireesh.psvs@gmail.com",
        "tenant_id": "altcodepro",
        "email": "sireesh.psvs@gmail.com",
        "auth_method": "password",
    }
    created = adapter._put_raw(
        _Model, "email:sireesh.psvs@gmail.com", "email:sireesh.psvs@gmail.com", data
    )

    returned_id = created["id"]
    assert returned_id == "email_sireesh.psvs@gmail.com"  # sanitized (colon -> underscore)

    # The load-bearing assertion: the STORED entity itself (not just this
    # one call's own return value) now has a real "id" property.
    stored = fake.store[("email_sireesh.psvs@gmail.com", "email_sireesh.psvs@gmail.com")]
    assert stored["id"] == returned_id

    # And the ordinary id-addressed lookup every update()/patch() "before"
    # read performs now actually finds the row -- reproduced BROKEN before
    # this fix (would have returned []).
    found = adapter._query_raw(_Model, {"id": returned_id, "tenant_id": "altcodepro"}, None)
    assert len(found) == 1
    assert found[0]["email"] == "sireesh.psvs@gmail.com"


def test_a_create_that_already_supplies_its_own_id_is_left_untouched():
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    data = {"id": "caller-chosen-id", "tenant_id": "t1", "name": "x"}
    created = adapter._put_raw(_Model, "t1", "caller-chosen-id", data)

    assert created["id"] == "caller-chosen-id"
    stored = fake.store[("t1", "caller-chosen-id")]
    assert stored["id"] == "caller-chosen-id"


def test_two_different_identities_never_collide_and_both_are_independently_findable():
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    for email in ("sireesh.psvs@gmail.com", "pangaluri2010@gmail.com"):
        key = f"email:{email}"
        adapter._put_raw(
            _Model,
            key,
            key,
            {
                "identity_key": key,
                "tenant_id": "altcodepro",
                "email": email,
            },
        )

    assert len(fake.store) == 2
    for email in ("sireesh.psvs@gmail.com", "pangaluri2010@gmail.com"):
        rows = adapter._query_raw(_Model, {"email": email}, None)
        assert len(rows) == 1 and rows[0]["email"] == email


def test_repeat_put_for_the_same_identity_key_upserts_the_same_row_not_a_duplicate():
    """The actual, originally-reported symptom this whole chain of fixes
    exists to close: a second signup attempt for the same email must
    collapse onto the SAME physical row, never mint a new one."""
    fake = _FakeTableClient()
    adapter = _adapter(fake)

    key = "email:sireesh.psvs@gmail.com"
    adapter._put_raw(
        _Model,
        key,
        key,
        {
            "identity_key": key,
            "tenant_id": "altcodepro",
            "email": "sireesh.psvs@gmail.com",
            "display_name": "Sireesh",
        },
    )
    adapter._put_raw(
        _Model,
        key,
        key,
        {
            "identity_key": key,
            "tenant_id": "altcodepro",
            "email": "sireesh.psvs@gmail.com",
            "display_name": "Sireesh Updated",
        },
    )

    assert len(fake.store) == 1
    only_row = next(iter(fake.store.values()))
    assert only_row["display_name"] == "Sireesh Updated"
