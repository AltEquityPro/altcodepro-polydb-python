"""
tests/test_advanced_query_injection.py
========================================
Unit tests for polydb.advanced_query.AdvancedQueryBuilder: identifier/HAVING
validation that closes the SQL-injection gap in to_sql(), and confirmation
that legitimate queries still build correctly.

No live database or cloud SDKs required.
"""

from __future__ import annotations

import sys
import types

import pytest

for _mod in [
    "google", "google.api_core", "google.api_core.exceptions",
    "google.cloud", "google.cloud.pubsub_v1", "google.cloud.storage",
    "google.cloud.firestore", "google.cloud.bigquery",
    "azure", "azure.storage", "azure.storage.blob", "azure.storage.queue",
    "azure.storage.file", "azure.data", "azure.data.tables",
    "boto3", "botocore", "botocore.exceptions",
    "redis", "pymongo",
    "varint", "baseconv",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)

_gcp_exc = sys.modules.get("google.api_core.exceptions") or types.ModuleType("google.api_core.exceptions")
if not hasattr(_gcp_exc, "AlreadyExists"):
    _gcp_exc.AlreadyExists = type("AlreadyExists", (Exception,), {})
    _gcp_exc.NotFound = type("NotFound", (Exception,), {})
sys.modules["google.api_core.exceptions"] = _gcp_exc

from polydb.errors import ValidationError
from polydb.advanced_query import AdvancedQueryBuilder, AggregateFunction, JoinType, QueryHelper


# ---------------------------------------------------------------------------
# Legitimate usage keeps working
# ---------------------------------------------------------------------------

def test_legitimate_query_builds_expected_sql():
    qb = (
        AdvancedQueryBuilder(table="orders")
        .join("users", "orders.user_id", "users.id", JoinType.LEFT, alias="u")
        .aggregate(AggregateFunction.COUNT, "orders.id", "order_count")
        .group_by("u.id")
        .having("COUNT(orders.id) > 5")
    )
    sql, params = qb.to_sql()
    assert sql == (
        "SELECT u.id, COUNT(orders.id) AS order_count FROM orders "
        "LEFT JOIN users AS u ON orders.user_id = users.id "
        "GROUP BY u.id HAVING COUNT(orders.id) > 5"
    )
    assert params == []


def test_query_helper_count_by_field():
    qb = QueryHelper.count_by_field("events", "*", "event_type")
    sql, _ = qb.to_sql()
    assert sql == "SELECT event_type, COUNT(*) AS count FROM events GROUP BY event_type"


def test_query_helper_join_with_filter():
    qb = QueryHelper.join_with_filter("orders", "users", "user_id")
    sql, _ = qb.to_sql()
    assert sql == "SELECT * FROM orders INNER JOIN users ON orders.user_id = users.user_id"


def test_having_with_and_or_chain():
    qb = AdvancedQueryBuilder(table="orders").having("COUNT(id) > 5 AND SUM(amount) < 1000")
    sql, _ = qb.to_sql()
    assert "HAVING COUNT(id) > 5 AND SUM(amount) < 1000" in sql


# ---------------------------------------------------------------------------
# Injection payloads are rejected
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", [
    "orders; DROP TABLE users; --",
    "orders' OR '1'='1",
    "orders/**/UNION/**/SELECT",
])
def test_malicious_table_name_rejected(table):
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table=table)


def test_malicious_join_table_rejected():
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders").join("users; DROP TABLE x; --", "a", "b")


def test_malicious_join_on_clause_rejected():
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders").join(
            "users", "orders.id = 1 OR 1=1; --", "users.id"
        )


def test_malicious_group_by_rejected():
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders").group_by("id); DROP TABLE users; --")


def test_malicious_aggregate_field_rejected():
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders").aggregate(
            AggregateFunction.COUNT, "id); DROP TABLE users; --", "x"
        )


def test_malicious_aggregate_alias_rejected():
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders").aggregate(
            AggregateFunction.COUNT, "id", "x; DROP TABLE users; --"
        )


@pytest.mark.parametrize("condition", [
    "1=1; DROP TABLE users; --",
    "id) OR 1=1--",
    "1=1 UNION SELECT password FROM users",
    "COUNT(id) > (SELECT COUNT(*) FROM users)",  # subqueries not allowed
    "name = 'admin'",  # string literals not allowed
])
def test_malicious_having_condition_rejected(condition):
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders").having(condition)


def test_malicious_having_rejected_via_constructor():
    with pytest.raises(ValidationError):
        AdvancedQueryBuilder(table="orders", having_conditions=["1=1; DROP TABLE users; --"])
