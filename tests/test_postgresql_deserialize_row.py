"""PostgreSQLAdapter._deserialize_row must never guess-parse a plain
TEXT/VARCHAR column's own string content just because it happens to look
JSON-shaped.

Real, reproduced production bug: `_deserialize_row` used to run every
returned row through a heuristic -- any string value that merely
started/ended with `{}`/`[]` got run through a bare `json.loads()`,
regardless of the column's own declared type. universal-interprter's
`prompt_template_store.py` stores an email template's `content` as a
plain TEXT column holding a JSON-encoded string (`{"subject": ...,
"html": ..., "text": ...}`) -- reading it back silently turned the real
`str` into a `dict`, and the next step in the pipeline
(`actions/core_prompt.py`'s `prompt.render` -> `core_text.py`'s
`text_template`, which calls `template.replace(...)`) crashed with
`'dict' object has no attribute 'replace'`.

A real `json`/`jsonb` column never needed this heuristic: psycopg2
registers global typecasters for those two OIDs (114/3802) automatically
at import time, so a genuine JSON/JSONB column already comes back as a
native dict/list by the time `_deserialize_row` ever sees it --
`isinstance(v, str)` is already False for those. The heuristic was
therefore pure downside: redundant for the one case it was meant to
help, and silently destructive for every ordinary TEXT/VARCHAR column
whose real content happens to resemble JSON. No live Postgres connection
is needed for any of this -- `_deserialize_row` is a pure dict-in/
dict-out method.
"""

from __future__ import annotations

from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter


def _adapter() -> PostgreSQLAdapter:
    # __init__ eagerly opens a real connection pool (no lazy-connect mode
    # exists) -- _deserialize_row touches no instance state at all, so a
    # bare, uninitialized instance is enough to exercise it without a
    # live Postgres.
    return object.__new__(PostgreSQLAdapter)


class TestDeserializeRowNeverGuessParsesTextColumns:
    def test_a_json_shaped_string_in_a_text_column_stays_a_string(self):
        adapter = _adapter()
        row = {
            "id": "1",
            "content": '{"subject": "hi", "html": "<p>{{code}}</p>", "text": "code: {{code}}"}',
        }

        result = adapter._deserialize_row(dict(row))

        assert result["content"] == row["content"]
        assert isinstance(result["content"], str)

    def test_a_json_array_shaped_string_in_a_text_column_stays_a_string(self):
        adapter = _adapter()
        row = {"id": "1", "notes": "[not, actually, json, just, looks, like, it]"}

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

    def test_psycopg2_registers_json_and_jsonb_typecasters_globally(self):
        # The direct proof the heuristic was never needed for a real
        # json/jsonb column in the first place -- confirmed against the
        # pinned driver, not assumed from psycopg2's own docs.
        import psycopg2.extensions as ext

        assert ext.string_types.get(114) is not None  # json
        assert ext.string_types.get(3802) is not None  # jsonb
