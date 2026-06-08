"""
Structured JSON logging for PolyDB.

Usage::

    from polydb.observability.logging import configure_logging, set_polydb_log_context

    configure_logging()
    set_polydb_log_context(tenant_id="acme", model="User", operation="select")
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import time
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Context variables
# ---------------------------------------------------------------------------

_ctx_tenant_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "polydb_tenant_id", default=None
)
_ctx_model: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "polydb_model", default=None
)
_ctx_operation: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "polydb_operation", default=None
)
_ctx_duration_ms: contextvars.ContextVar[Optional[float]] = contextvars.ContextVar(
    "polydb_duration_ms", default=None
)


def set_polydb_log_context(
    *,
    tenant_id: Optional[str] = None,
    model: Optional[str] = None,
    operation: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> None:
    """Set per-request context fields that are injected into every log record."""
    if tenant_id is not None:
        _ctx_tenant_id.set(tenant_id)
    if model is not None:
        _ctx_model.set(model)
    if operation is not None:
        _ctx_operation.set(operation)
    if duration_ms is not None:
        _ctx_duration_ms.set(duration_ms)


# ---------------------------------------------------------------------------
# JSON Formatter
# ---------------------------------------------------------------------------


class _PolyDBJsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject context vars
        tenant_id = _ctx_tenant_id.get()
        if tenant_id is not None:
            payload["tenant_id"] = tenant_id

        model = _ctx_model.get()
        if model is not None:
            payload["model"] = model

        operation = _ctx_operation.get()
        if operation is not None:
            payload["operation"] = operation

        duration_ms = _ctx_duration_ms.get()
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        # Attach any extra fields the caller passed directly to the record
        for key in ("operation", "table", "duration_ms", "tenant_id", "model"):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------


def configure_logging(level: Optional[str] = None) -> None:
    """
    Configure root / polydb logger to emit structured JSON.

    Reads ``POLYDB_LOG_LEVEL`` env var (default ``INFO``).
    """
    resolved_level = level or os.getenv("POLYDB_LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, resolved_level, logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(_PolyDBJsonFormatter())

    polydb_logger = logging.getLogger("polydb")
    polydb_logger.setLevel(numeric_level)

    # Replace existing handlers to avoid duplicates
    polydb_logger.handlers = [handler]
    polydb_logger.propagate = False
