"""PostgreSQLAdapter._deserialize_row's best-effort JSON-shaped-string
decode -- real, load-bearing behavior a real caller depends on, plus the
honest trade-off it can't avoid.

History: this heuristic (`json.loads()` any string column value that
starts/ends with `{}`/`[]`, regardless of the column's own declared
type) was briefly REMOVED in 2.5.16 after a real, reproduced bug --
universal-interprter's `prompt_template_store.py` stores an email
template's `content` as a plain TEXT column holding a JSON-encoded
string, and reading it back silently coerced it from `str` to `dict`,
crashing `prompt.render` -> `text.template`'s `template.replace(...)`
with `'dict' object has no attribute 'replace'`. That removal was itself
a real regression, reverted in 2.5.17: universal-interprter's own
`observability.py` (`Observability.query()`) writes a `logs.emit`
payload as `json.dumps(...)` into a plain TEXT column (`payload`, never
JSONB) and its own docstring explicitly promises "logs.query's own
`payload` field gets a real dict back" -- a real, already-shipped
contract this heuristic is the only thing that ever fulfilled, since
that read path is a raw `sql_adapter.execute()` call with no explicit
parsing of its own (unlike `query_log.py`'s own `fields` column, the
identical shape, which defends itself with an `isinstance(..., str)`
guard + its own `json.loads()` rather than relying on this method).

The real, honest trade-off (unavoidable at this layer, see
_deserialize_row's own docstring): a TEXT column can never be told apart
from a JSONB one purely by string-sniffing its OWN value's shape, so a
caller that must keep a TEXT column's real string content literal
despite it looking JSON-shaped (like `prompt_template_store.py`'s
`content`) owns that coercion at ITS OWN layer -- see that module's own
`_coerce_content_to_str` in universal-interprter for the concrete
pattern. No live Postgres connection is needed for any of this --
`_deserialize_row` is a pure dict-in/dict-out method.
"""

from __future__ import annotations

from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter


def _adapter() -> PostgreSQLAdapter:
    # __init__ eagerly opens a real connection pool (no lazy-connect mode
    # exists) -- _deserialize_row touches no instance state at all, so a
    # bare, uninitialized instance is enough to exercise it without a
    # live Postgres.
    return object.__new__(PostgreSQLAdapter)


class TestDeserializeRowDecodesJsonShapedStrings:
    def test_a_json_object_shaped_string_is_parsed_into_a_dict(self):
        # The real, load-bearing case: a caller (e.g. observability.py's
        # own `payload` column) wrote `json.dumps(...)` into a plain TEXT
        # column and expects a real dict back on read.
        adapter = _adapter()
        row = {"id": "1", "payload": '{"event": "otp_sent", "channel": "email"}'}

        result = adapter._deserialize_row(dict(row))

        assert result["payload"] == {"event": "otp_sent", "channel": "email"}

    def test_a_json_array_shaped_string_is_parsed_into_a_list(self):
        adapter = _adapter()
        row = {"id": "1", "fields": '["email", "status"]'}

        result = adapter._deserialize_row(dict(row))

        assert result["fields"] == ["email", "status"]

    def test_a_string_that_merely_looks_json_shaped_but_isnt_valid_json_is_left_alone(self):
        adapter = _adapter()
        row = {"id": "1", "notes": "{not actually valid json}"}

        result = adapter._deserialize_row(dict(row))

        assert result["notes"] == row["notes"]
        assert isinstance(result["notes"], str)

    def test_an_ordinary_scalar_string_is_untouched(self):
        adapter = _adapter()
        row = {"id": "1", "name": "plain text, not json at all"}

        result = adapter._deserialize_row(dict(row))

        assert result == row

    def test_a_real_dict_value_already_parsed_by_the_driver_passes_through_unchanged(self):
        # This is what a genuine json/jsonb column already looks like by
        # the time _deserialize_row runs -- psycopg2's own global
        # typecasters (OIDs 114/3802) already turned it into a native
        # dict, so there is nothing left for this method to do.
        adapter = _adapter()
        row = {"id": "1", "metadata": {"subject": "hi"}}

        result = adapter._deserialize_row(dict(row))

        assert result == row
        assert isinstance(result["metadata"], dict)

    def test_none_and_non_string_values_are_left_alone(self):
        adapter = _adapter()
        row = {"id": "1", "deleted_at": None, "count": 3, "active": True}

        result = adapter._deserialize_row(dict(row))

        assert result == row

    def test_the_honest_trade_off_a_json_shaped_string_that_is_meant_to_stay_literal_text_still_gets_coerced(
        self,
    ):
        # The real cost this heuristic imposes, documented rather than
        # hidden: a caller owning a TEXT column whose real content
        # happens to be JSON-shaped (e.g. prompt_template_store.py's own
        # `content`) must defend against this itself -- this adapter has
        # no way to know that a given TEXT value should stay a literal
        # string instead of being decoded.
        adapter = _adapter()
        row = {"id": "1", "content": '{"subject": "hi", "html": "<p>{{code}}</p>"}'}

        result = adapter._deserialize_row(dict(row))

        assert isinstance(result["content"], dict)  # not the literal string it was written as
