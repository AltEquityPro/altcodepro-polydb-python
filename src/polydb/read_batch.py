# src/polydb/read_batch.py
"""Coalesce the SQL reads several threads make at the same moment into ONE pipelined round trip on ONE connection.

The problem this solves: a request that needs N independent reads used to pay N network round trips in a row (each query waits
for the previous answer), or -- when run on N threads -- held N Postgres connections at once and could drain the connection pool
or the server's connection slots. Postgres runs one statement at a time per connection, but with psycopg 3 *pipeline mode* a
client can send many statements without waiting for each answer, so N reads cost about one round trip and one connection.

How it plugs in: a caller (the engine's `coll.parallel`) creates a `ReadBatch` for the tasks it is about to run and makes it the
current batch for those threads. `PostgreSQLAdapter.select()` -- the single place every SQL read goes through -- checks for a
current batch and, if there is one, hands its query to the batch instead of executing it, then waits for its own rows. Everything
above `select()` (permission checks, tenant scoping, audit, caches, decryption) runs untouched on each task's own thread; only the
final "send this SELECT to the database" step is shared.

A batch flushes as soon as every still-running task is waiting on it, or after `window_s` (so a task busy with non-database work
cannot hold the others up). Each query still runs in its own read-only transaction with its own `session_vars`
(`set_config(..., true)`), so row-level security sees exactly the tenant it would have seen on a connection of its own.
"""
from __future__ import annotations

import contextvars
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional


@dataclass
class SelectSpec:
    """One SELECT to run, already validated by `PostgreSQLAdapter.select()`."""

    table: str
    query: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    fields: Optional[List[str]] = None
    session_vars: Optional[Dict[str, str]] = None
    # A `QueryBuilder` (LINQ-style query: projection, ORDER BY, operators, count). When set, `query`/`limit`/`offset`/`fields` are
    # not used; the result is the row list, or the integer for a count-only builder.
    builder: Optional[Any] = None


class _Pending:
    __slots__ = ("adapter", "spec", "event", "rows", "error")

    def __init__(self, adapter: Any, spec: SelectSpec):
        self.adapter = adapter
        self.spec = spec
        self.event = threading.Event()
        self.rows: Any = None
        self.error: Optional[BaseException] = None


_current: contextvars.ContextVar[Optional["ReadBatch"]] = contextvars.ContextVar("polydb_read_batch", default=None)


def current_batch() -> Optional["ReadBatch"]:
    return _current.get()


@contextmanager
def use_read_batch(batch: Optional["ReadBatch"]) -> Iterator[None]:
    """Make `batch` the current batch for this context (and for contexts copied from it afterwards)."""
    token = _current.set(batch)
    try:
        yield
    finally:
        _current.reset(token)


class ReadBatch:
    """Collects SELECTs submitted by up to `parties` threads and runs them together.

    Each party must call `task_done()` exactly once when it has finished (success or failure), so the batch knows nobody else is
    coming and can flush what is waiting."""

    def __init__(self, parties: int, window_s: Optional[float] = None):
        self._active = max(0, int(parties))
        self._window = (
            float(window_s) if window_s is not None else float(os.getenv("POLYDB_READ_BATCH_WINDOW_MS", "5")) / 1000.0
        )
        self._lock = threading.Lock()
        self._queue: List[_Pending] = []
        self.flushes = 0          # how many pipelined round trips were made (for tests / diagnostics)
        self.statements = 0       # how many SELECTs went through the batch

    # -- called from a task's thread, inside PostgreSQLAdapter.select() ------------------------------------------------------
    def submit(self, adapter: Any, spec: SelectSpec) -> Any:
        item = _Pending(adapter, spec)
        with self._lock:
            self._queue.append(item)
            everyone_is_waiting = len(self._queue) >= self._active
        if everyone_is_waiting:
            self._flush()
        elif not item.event.wait(self._window):
            self._flush()  # the window elapsed: send whatever has queued up
        item.event.wait()  # another thread may be executing our query right now
        if item.error is not None:
            raise item.error
        return item.rows if item.rows is not None else []

    def task_done(self) -> None:
        with self._lock:
            self._active -= 1
            waiting = bool(self._queue) and len(self._queue) >= self._active
        if waiting:
            self._flush()

    # -- internals ------------------------------------------------------------------------------------------------------------
    def _flush(self) -> None:
        with self._lock:
            batch, self._queue = self._queue, []
        if not batch:
            return
        groups: Dict[int, List[_Pending]] = {}
        for it in batch:
            groups.setdefault(id(it.adapter), []).append(it)
        try:
            for items in groups.values():
                try:
                    outcomes = items[0].adapter.select_many([i.spec for i in items])
                except BaseException as exc:  # noqa: BLE001 -- every waiter must be released
                    outcomes = [exc] * len(items)
                self.flushes += 1
                self.statements += len(items)
                for it, out in zip(items, outcomes):
                    if isinstance(out, BaseException):
                        it.error = out
                    else:
                        it.rows = out
        finally:
            for it in batch:
                it.event.set()
