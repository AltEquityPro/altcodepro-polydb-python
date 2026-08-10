"""A filter *value* must never be able to become a query operator.

MongoDB reads a dict on the right-hand side of a field as an operator
expression, so ``{"password": {"$gt": ""}}`` stops being "password equals X"
and becomes "every document that has a password" — filter/auth bypass and
full-collection extraction, with no injection into the *keys* required.

Two layers are covered here:
  * QueryBuilder.to_nosql_filter — rejects dict values outright (nothing goes
    through the builder with a legitimate dict).
  * MongoDBAdapter._query_raw / query_page — callers may hand the adapter a
    raw filter dict without touching QueryBuilder at all, so dict values are
    wrapped as ``{"$eq": ...}`` there, which is the literal comparison Mongo
    would do for a subdocument anyway.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from polydb.adapters.MongoDBAdapter import MongoDBAdapter, _as_literal
from polydb.errors import ValidationError
from polydb.query import Operator, QueryBuilder


class PolyItem:
    """Sentinel model — only used to derive a collection name."""


OPERATOR_PAYLOADS = [
    {"$gt": ""},
    {"$ne": None},
    {"$regex": ".*"},
    {"$exists": True},
    {"$where": "1 == 1"},
    {"$nin": []},
]


# ── Layer 1: QueryBuilder ────────────────────────────────────────────────


class TestQueryBuilderRejectsOperatorValues:
    @pytest.mark.parametrize("payload", OPERATOR_PAYLOADS)
    def test_eq_with_dict_value_is_rejected(self, payload):
        qb = QueryBuilder().where("password", Operator.EQ, payload)
        with pytest.raises(ValidationError):
            qb.to_nosql_filter()

    @pytest.mark.parametrize(
        "operator",
        [Operator.GT, Operator.GTE, Operator.LT, Operator.LTE, Operator.CONTAINS],
    )
    def test_other_operators_reject_dict_values_too(self, operator):
        qb = QueryBuilder().where("age", operator, {"$ne": None})
        with pytest.raises(ValidationError):
            qb.to_nosql_filter()

    def test_dict_hidden_inside_an_in_list_is_rejected(self):
        qb = QueryBuilder().where("id", Operator.IN, ["a", {"$ne": None}])
        with pytest.raises(ValidationError):
            qb.to_nosql_filter()

    def test_scalar_filters_still_work(self):
        qb = (
            QueryBuilder()
            .where("name", Operator.EQ, "alice")
            .where("age", Operator.GTE, 21)
            .where("bio", Operator.CONTAINS, "engineer")
        )
        assert qb.to_nosql_filter() == {
            "name": "alice",
            "age__gte": 21,
            "bio__contains": "engineer",
        }

    def test_in_lists_are_still_legitimate(self):
        qb = QueryBuilder().where("status", Operator.IN, ["active", "trial"])
        assert qb.to_nosql_filter() == {"status__in": ["active", "trial"]}


# ── Layer 2: MongoDBAdapter ──────────────────────────────────────────────


def _adapter_with_fake_collection():
    """Build an adapter without touching __init__ (which dials a server)."""
    adapter = MongoDBAdapter.__new__(MongoDBAdapter)
    adapter._client = None  # __del__ reads this

    cursor = MagicMock()
    cursor.__iter__.return_value = iter([])
    cursor.sort.return_value = cursor
    cursor.limit.return_value = cursor

    collection = MagicMock()
    collection.find.return_value = cursor
    adapter._get_collection = lambda model: collection  # type: ignore[method-assign]
    return adapter, collection


class TestMongoAdapterNeutralisesOperatorValues:
    @pytest.mark.parametrize("payload", OPERATOR_PAYLOADS)
    def test_equality_value_cannot_become_an_operator(self, payload):
        adapter, collection = _adapter_with_fake_collection()

        adapter._query_raw(PolyItem, {"password": payload}, None)

        sent = collection.find.call_args.args[0]
        assert sent == {"password": {"$eq": payload}}
        # The payload survives only as an operand, never as the operator doc.
        assert set(sent["password"]) == {"$eq"}

    @pytest.mark.parametrize("payload", OPERATOR_PAYLOADS)
    def test_id_shortcut_cannot_become_an_operator(self, payload):
        adapter, collection = _adapter_with_fake_collection()

        adapter._query_raw(PolyItem, {"id": payload}, None)

        assert collection.find.call_args.args[0] == {"_pk": {"$eq": payload}}

    def test_query_page_sanitizes_the_caller_query(self):
        adapter, collection = _adapter_with_fake_collection()

        adapter.query_page(PolyItem, {"password": {"$ne": None}}, 10)

        assert collection.find.call_args.args[0] == {"password": {"$eq": {"$ne": None}}}

    def test_query_page_still_applies_its_continuation_cursor(self):
        adapter, collection = _adapter_with_fake_collection()

        adapter.query_page(PolyItem, {"name": "alice"}, 10, continuation_token="pk-5")

        assert collection.find.call_args.args[0] == {
            "name": "alice",
            "_pk": {"$gt": "pk-5"},
        }

    def test_scalar_and_suffix_filters_are_unchanged(self):
        adapter, collection = _adapter_with_fake_collection()

        adapter._query_raw(
            PolyItem,
            {
                "name": "alice",
                "age__gte": 21,
                "status__in": ["active", "trial"],
                "bio__contains": "a.b",
            },
            None,
        )

        assert collection.find.call_args.args[0] == {
            "name": "alice",
            "age": {"$gte": 21},
            "status": {"$in": ["active", "trial"]},
            "bio": {"$regex": r"a\.b", "$options": "i"},
        }

    def test_subdocument_equality_still_matches_the_same_documents(self):
        """{"$eq": doc} is exactly Mongo's literal subdocument comparison."""
        assert _as_literal({"city": "NY"}) == {"$eq": {"city": "NY"}}
        assert _as_literal("alice") == "alice"
        assert _as_literal(["a", "b"]) == ["a", "b"]
