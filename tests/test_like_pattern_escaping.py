"""CONTAINS/STARTS_WITH/ENDS_WITH must match a literal substring.

The value is bound as a parameter, so there is no SQL injection here — but it
used to be wrapped in wildcards unescaped, so a ``%`` or ``_`` in the value was
still interpreted by the pattern matcher. That subverts the intended semantics
(``STARTS_WITH("%")`` matches everything) and a leading ``%`` turns an
index-friendly prefix scan into a full table scan.
"""

from __future__ import annotations

import pytest

from polydb.query import Operator, QueryBuilder


ESCAPE_CLAUSE = "LIKE %s ESCAPE '\\'"


class TestMetacharactersAreEscaped:
    @pytest.mark.parametrize(
        "operator,value,expected",
        [
            (Operator.CONTAINS, "100%", r"%100\%%"),
            (Operator.STARTS_WITH, "%", r"\%%"),
            (Operator.ENDS_WITH, "%", r"%\%"),
            (Operator.CONTAINS, "a_b", r"%a\_b%"),
            (Operator.CONTAINS, "back\\slash", r"%back\\slash%"),
            (Operator.STARTS_WITH, "%_\\", r"\%\_\\%"),
        ],
    )
    def test_wildcards_in_the_value_are_neutralised(self, operator, value, expected):
        clause, params = QueryBuilder().where("name", operator, value).to_sql_where()

        assert clause == f"name {ESCAPE_CLAUSE}"
        assert params == [expected]

    def test_leading_wildcard_cannot_be_forced_on_a_prefix_scan(self):
        _, params = (
            QueryBuilder().where("name", Operator.STARTS_WITH, "%").to_sql_where()
        )

        assert not params[0].startswith("%")


class TestLegitimateValuesStillWork:
    @pytest.mark.parametrize(
        "operator,expected",
        [
            (Operator.CONTAINS, "%alice%"),
            (Operator.STARTS_WITH, "alice%"),
            (Operator.ENDS_WITH, "%alice"),
        ],
    )
    def test_plain_values_are_unchanged(self, operator, expected):
        clause, params = QueryBuilder().where("name", operator, "alice").to_sql_where()

        assert clause == f"name {ESCAPE_CLAUSE}"
        assert params == [expected]

    def test_other_operators_are_untouched(self):
        clause, params = (
            QueryBuilder()
            .where("name", Operator.EQ, "alice")
            .where("age", Operator.GTE, 21)
            .where("status", Operator.IN, ["active", "trial"])
            .to_sql_where()
        )

        assert clause == "name = %s AND age >= %s AND status IN (%s,%s)"
        assert params == ["alice", 21, "active", "trial"]
