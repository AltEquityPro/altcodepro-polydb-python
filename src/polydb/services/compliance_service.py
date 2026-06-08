"""
polydb.services.compliance_service
====================================
Compliance audit log writer with retry queue and CRITICAL-level alerting.

Fix M14: audit failures were silently swallowed (except Exception: logger.warning).
This implementation:
  - Emits a compliance.audit_failure metric counter on first failure
  - Enqueues a retry in an async queue
  - After MAX_RETRIES consecutive failures logs at CRITICAL and calls the alert fn
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

logger = logging.getLogger("polydb.services.compliance")


@dataclass
class AuditEvent:
    event_type: str
    tenant_id: str
    actor_id: str
    resource_type: str
    resource_id: str
    action: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    attempt: int = 0
    created_at: float = field(default_factory=time.time)


WriterFn = Callable[[AuditEvent], Awaitable[None]]
MetricsFn = Callable[[str, Dict[str, str]], None]
AlertFn = Callable[[AuditEvent], Awaitable[None]]


class ComplianceService:
    """Audit log writer with async retry queue and escalation."""

    MAX_RETRIES = 3
    RETRY_DELAY_S = 2.0

    def __init__(
        self,
        *,
        metrics_fn: Optional[MetricsFn] = None,
        alert_fn: Optional[AlertFn] = None,
    ) -> None:
        self._metrics = metrics_fn
        self._alert = alert_fn
        self._retry_queue: asyncio.Queue[AuditEvent] = asyncio.Queue(maxsize=10_000)
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None

    async def log(self, event: AuditEvent, *, writer: WriterFn) -> None:
        """Write audit event. On failure, enqueue for retry."""
        try:
            await writer(event)
        except Exception as exc:
            self._record_failure(event, exc, step="initial")
            event.attempt = 1
            try:
                self._retry_queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.critical(
                    "compliance.audit_failure retry queue full — event dropped: "
                    "event_type=%s tenant_id=%s",
                    event.event_type, event.tenant_id,
                )

    async def start(self, *, writer: WriterFn) -> None:
        """Start the background retry worker. Call once at service startup."""
        self._running = True
        self._worker_task = asyncio.create_task(
            self._drain_retry_queue(writer=writer),
            name="compliance_retry_worker",
        )

    def stop(self) -> None:
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()

    async def _drain_retry_queue(self, *, writer: WriterFn) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._retry_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            if event.attempt >= self.MAX_RETRIES:
                logger.critical(
                    "compliance.audit_failure UNRECOVERABLE after %d retries: "
                    "event_type=%s tenant_id=%s resource=%s/%s",
                    event.attempt,
                    event.event_type,
                    event.tenant_id,
                    event.resource_type,
                    event.resource_id,
                )
                if self._alert:
                    try:
                        await self._alert(event)
                    except Exception as alert_exc:
                        logger.error("Alert function failed: %s", alert_exc)
                continue

            await asyncio.sleep(self.RETRY_DELAY_S)
            try:
                await writer(event)
            except Exception as exc:
                self._record_failure(event, exc, step=f"retry_{event.attempt}")
                event.attempt += 1
                try:
                    self._retry_queue.put_nowait(event)
                except asyncio.QueueFull:
                    logger.critical(
                        "compliance.audit_failure retry queue full on re-enqueue — event lost: "
                        "event_type=%s tenant_id=%s",
                        event.event_type, event.tenant_id,
                    )

    def _record_failure(self, event: AuditEvent, exc: Exception, step: str) -> None:
        logger.error(
            "compliance.audit_failure step=%s event_type=%s tenant_id=%s resource=%s/%s error=%s",
            step, event.event_type, event.tenant_id,
            event.resource_type, event.resource_id, exc,
        )
        if self._metrics:
            try:
                self._metrics(
                    "compliance.audit_failure",
                    {"event_type": event.event_type, "tenant_id": event.tenant_id, "step": step},
                )
            except Exception:
                pass
