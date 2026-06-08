"""
Tests for polydb.observability — structured logging and slow query detection.
All tests are unit tests (no live DB required).
"""
from __future__ import annotations

import json
import logging
import os
import time
from unittest.mock import MagicMock, patch, call

import pytest

from polydb.observability.logging import (
    configure_logging,
    set_polydb_log_context,
    _ctx_tenant_id,
    _ctx_model,
    _ctx_operation,
    _ctx_duration_ms,
    _PolyDBJsonFormatter,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(message: str = "hello", level: int = logging.DEBUG) -> logging.LogRecord:
    record = logging.LogRecord(
        name="polydb.test",
        level=level,
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )
    return record


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class TestPolyDBJsonFormatter:
    def setup_method(self):
        # Clear context vars between tests
        _ctx_tenant_id.set(None)
        _ctx_model.set(None)
        _ctx_operation.set(None)
        _ctx_duration_ms.set(None)

    def test_basic_fields_present(self):
        formatter = _PolyDBJsonFormatter()
        output = formatter.format(_make_record("test message"))
        data = json.loads(output)
        assert data["message"] == "test message"
        assert "timestamp" in data
        assert "level" in data
        assert "logger" in data

    def test_context_fields_injected(self):
        set_polydb_log_context(
            tenant_id="acme",
            model="User",
            operation="select",
            duration_ms=42.5,
        )
        formatter = _PolyDBJsonFormatter()
        output = formatter.format(_make_record("ctx test"))
        data = json.loads(output)
        assert data["tenant_id"] == "acme"
        assert data["model"] == "User"
        assert data["operation"] == "select"
        assert data["duration_ms"] == 42.5

    def test_missing_context_fields_omitted(self):
        formatter = _PolyDBJsonFormatter()
        output = formatter.format(_make_record("no ctx"))
        data = json.loads(output)
        assert "tenant_id" not in data
        assert "model" not in data

    def test_extra_record_fields_included(self):
        formatter = _PolyDBJsonFormatter()
        record = _make_record("with extra")
        record.operation = "insert"
        record.table = "users"
        record.duration_ms = 12.3
        output = formatter.format(record)
        data = json.loads(output)
        assert data["operation"] == "insert"
        assert data["table"] == "users"
        assert data["duration_ms"] == 12.3


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    def test_sets_json_formatter(self):
        configure_logging("DEBUG")
        polydb_logger = logging.getLogger("polydb")
        assert len(polydb_logger.handlers) >= 1
        handler = polydb_logger.handlers[0]
        assert isinstance(handler.formatter, _PolyDBJsonFormatter)

    def test_respects_env_var(self):
        with patch.dict(os.environ, {"POLYDB_LOG_LEVEL": "WARNING"}):
            configure_logging()
        polydb_logger = logging.getLogger("polydb")
        assert polydb_logger.level == logging.WARNING


# ---------------------------------------------------------------------------
# PostgreSQLAdapter._timed_execute
# ---------------------------------------------------------------------------


class TestTimedExecute:
    """Unit tests for the timing / slow-query warning logic."""

    def _make_adapter(self, slow_ms: float = 1000.0):
        """Create a PostgreSQLAdapter without a live DB by mocking pool init."""
        with patch(
            "polydb.adapters.PostgreSQLAdapter.PostgreSQLAdapter._initialize_pool"
        ):
            from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter

            adapter = PostgreSQLAdapter.__new__(PostgreSQLAdapter)
            adapter._pool = None
            adapter._lock = __import__("threading").Lock()
            adapter._slow_query_ms = slow_ms
            import polydb.utils as u
            adapter.logger = u.setup_logger("polydb.test.adapter")
            return adapter

    def test_returns_elapsed_ms(self):
        adapter = self._make_adapter()
        cursor = MagicMock()

        elapsed = adapter._timed_execute(cursor, "SELECT 1", [], operation="select", table="t")
        assert elapsed >= 0
        cursor.execute.assert_called_once_with("SELECT 1", [])

    def test_slow_query_warning_emitted(self):
        adapter = self._make_adapter(slow_ms=0.0)  # everything is "slow"
        cursor = MagicMock()

        with patch.object(adapter.logger, "warning") as mock_warn:
            adapter._timed_execute(cursor, "SELECT 1", [], operation="select", table="users")

        mock_warn.assert_called_once()
        call_args = mock_warn.call_args[0]
        assert "Slow query" in call_args[0]

    def test_no_warning_for_fast_query(self):
        adapter = self._make_adapter(slow_ms=99999.0)  # nothing triggers
        cursor = MagicMock()

        with patch.object(adapter.logger, "warning") as mock_warn:
            adapter._timed_execute(cursor, "SELECT 1", [], operation="select", table="users")

        mock_warn.assert_not_called()
