"""
tests/test_atomic_decrement.py
==============================
Unit tests for PostgreSQLAdapter.atomic_decrement_if_sufficient and savepoint helpers.

All DB calls are mocked — no live Postgres required.
Cloud-SDK imports (google, azure, boto3, etc.) are stubbed via sys.modules so
this test file runs without any cloud packages installed.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch, call
import pytest

# ---------------------------------------------------------------------------
# Stub out cloud / optional SDK imports so the module import chain succeeds
# in environments without the full dependency set.
# ---------------------------------------------------------------------------
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

# Stub specific exception classes needed by some adapters
_gcp_exc = sys.modules.get("google.api_core.exceptions") or types.ModuleType("google.api_core.exceptions")
if not hasattr(_gcp_exc, "AlreadyExists"):
    _gcp_exc.AlreadyExists = type("AlreadyExists", (Exception,), {})
    _gcp_exc.NotFound = type("NotFound", (Exception,), {})
sys.modules["google.api_core.exceptions"] = _gcp_exc

from polydb.errors import InsufficientBalanceError, DatabaseError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_adapter():
    """Return a PostgreSQLAdapter with a mocked pool so __init__ doesn't connect."""
    with patch("polydb.adapters.PostgreSQLAdapter.PostgreSQLAdapter._initialize_pool"):
        from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter
        adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
        adapter._pool = MagicMock()
        adapter._lock = MagicMock()
        import logging
        adapter.logger = logging.getLogger("test")
        return adapter


def _make_conn(rows, col_names=None):
    """Build a fake psycopg2 connection that returns *rows* from fetchone()."""
    if col_names is None:
        col_names = ["id", "tenant_id", "balance"]

    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = rows
    cursor.description = [(c,) for c in col_names]

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ---------------------------------------------------------------------------
# atomic_decrement_if_sufficient — success path
# ---------------------------------------------------------------------------

def test_atomic_decrement_success():
    adapter = _make_adapter()
    row = ("wallet-1", "tenant-a", 20)  # balance after decrement
    conn, cursor = _make_conn(row)
    adapter._get_connection = MagicMock(return_value=conn)
    adapter._return_connection = MagicMock()

    result = adapter.atomic_decrement_if_sufficient(
        table="credit_wallet",
        balance_field="balance",
        amount=80,
        where_field="tenant_id",
        where_value="tenant-a",
    )

    assert result["balance"] == 20
    assert result["tenant_id"] == "tenant-a"
    conn.commit.assert_called_once()
    adapter._return_connection.assert_called_once_with(conn)


# ---------------------------------------------------------------------------
# atomic_decrement_if_sufficient — insufficient balance (no rows returned)
# ---------------------------------------------------------------------------

def test_atomic_decrement_insufficient_balance():
    adapter = _make_adapter()
    conn, cursor = _make_conn(None)  # fetchone returns None → no rows updated
    adapter._get_connection = MagicMock(return_value=conn)
    adapter._return_connection = MagicMock()

    with pytest.raises(InsufficientBalanceError):
        adapter.atomic_decrement_if_sufficient(
            table="credit_wallet",
            balance_field="balance",
            amount=200,
            where_field="tenant_id",
            where_value="tenant-a",
        )

    conn.rollback.assert_called_once()
    adapter._return_connection.assert_called_once_with(conn)


# ---------------------------------------------------------------------------
# atomic_decrement_if_sufficient — uses provided tx, no own_conn management
# ---------------------------------------------------------------------------

def test_atomic_decrement_with_tx():
    adapter = _make_adapter()
    row = ("wallet-2", "tenant-b", 10)
    tx_conn, cursor = _make_conn(row)

    # Should NOT call _get_connection when tx is provided
    adapter._get_connection = MagicMock()
    adapter._return_connection = MagicMock()

    result = adapter.atomic_decrement_if_sufficient(
        table="credit_wallet",
        balance_field="balance",
        amount=50,
        where_field="tenant_id",
        where_value="tenant-b",
        tx=tx_conn,
    )

    assert result["balance"] == 10
    adapter._get_connection.assert_not_called()
    # No commit/return_connection on externally managed tx
    tx_conn.commit.assert_not_called()
    adapter._return_connection.assert_not_called()


# ---------------------------------------------------------------------------
# atomic_decrement_if_sufficient — SQL error raises DatabaseError
# ---------------------------------------------------------------------------

def test_atomic_decrement_db_error():
    adapter = _make_adapter()
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.execute.side_effect = Exception("connection reset")
    conn.cursor.return_value = cursor
    adapter._get_connection = MagicMock(return_value=conn)
    adapter._return_connection = MagicMock()

    with pytest.raises(DatabaseError):
        adapter.atomic_decrement_if_sufficient(
            table="credit_wallet",
            balance_field="balance",
            amount=10,
            where_field="tenant_id",
            where_value="tenant-a",
        )


# ---------------------------------------------------------------------------
# Savepoint helpers
# ---------------------------------------------------------------------------

def test_begin_savepoint():
    adapter = _make_adapter()
    tx = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    tx.cursor.return_value = cursor

    adapter.begin_savepoint("sp1", tx)
    cursor.execute.assert_called_once_with("SAVEPOINT sp1")


def test_rollback_to_savepoint():
    adapter = _make_adapter()
    tx = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    tx.cursor.return_value = cursor

    adapter.rollback_to_savepoint("sp1", tx)
    cursor.execute.assert_called_once_with("ROLLBACK TO SAVEPOINT sp1")


def test_release_savepoint():
    adapter = _make_adapter()
    tx = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    tx.cursor.return_value = cursor

    adapter.release_savepoint("sp1", tx)
    cursor.execute.assert_called_once_with("RELEASE SAVEPOINT sp1")
