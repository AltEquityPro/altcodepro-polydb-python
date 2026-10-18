# src/polydb/write_behind.py
"""Write-behind queue: telemetry-style inserts (audit log, event log, query log) written by ONE background thread, in batches.

Why: the engine writes an audit row, an event row and a query-log row for every action it runs. Written inline, each of those is its
own transaction on a pooled connection -- two network round trips apiece, taken on whatever thread is running the action. A request
that runs 40 steps paid ~120 extra transactions before it could answer, and a `coll.parallel` group did it from several threads at
once, each holding its own connection. Here the caller only appends the row to an in-memory queue and moves on; one writer thread
groups what has queued up per table and sends each group as ONE pipelined transaction on ONE connection
(`PostgreSQLAdapter.insert_many`).

Trade-off, stated plainly: a row still queued when the process is killed hard (SIGKILL / OOM) is lost; a normal exit flushes
(`atexit`). A full queue falls back to writing inline, so nothing is dropped under load. Only for rows nobody reads back in the
same request -- never for data a caller depends on.
"""
from __future__ import annotations

import atexit
import logging
import os
import queue
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_QUEUE_MAX = int(os.getenv("POLYDB_WRITE_BEHIND_QUEUE_MAX", "20000"))
_BATCH_MAX = int(os.getenv("POLYDB_WRITE_BEHIND_BATCH_MAX", "200"))
_LINGER_S = float(os.getenv("POLYDB_WRITE_BEHIND_LINGER_MS", "25")) / 1000.0

_Item = Tuple[Any, str, Dict[str, Any], Optional[Dict[str, str]]]  # (sql adapter, table, row, session_vars)


class WriteBehind:
    def __init__(self) -> None:
        self._queue: "queue.Queue[_Item]" = queue.Queue(maxsize=_QUEUE_MAX)
        self._thread: Optional[threading.Thread] = None
        self._start_lock = threading.Lock()
        self.batches = 0     # pipelined transactions written (diagnostics / tests)
        self.rows = 0        # rows written through the queue
        atexit.register(self.flush)

    # ------------------------------------------------------------------------------------------------------------------
    def submit(self, adapter: Any, table: str, row: Dict[str, Any], session_vars: Optional[Dict[str, str]] = None) -> None:
        self._ensure_thread()
        try:
            self._queue.put_nowait((adapter, table, row, session_vars))
        except queue.Full:
            # Never drop a row: when the queue is full the caller writes it inline, exactly as it would without write-behind.
            adapter.insert(table, row, session_vars=session_vars)

    def flush(self, timeout: float = 10.0) -> None:
        """Block until every queued row has been written (or `timeout` seconds pass)."""
        if self._thread is None:
            return
        deadline = time.monotonic() + timeout
        while (not self._queue.empty() or self._queue.unfinished_tasks) and time.monotonic() < deadline:
            time.sleep(0.005)

    # ------------------------------------------------------------------------------------------------------------------
    def _ensure_thread(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        with self._start_lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, name="polydb-write-behind", daemon=True)
                self._thread.start()

    def _run(self) -> None:
        while True:
            first = self._queue.get()
            batch: List[_Item] = [first]
            deadline = time.monotonic() + _LINGER_S
            while len(batch) < _BATCH_MAX:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._queue.get(timeout=remaining))
                except queue.Empty:
                    break
            try:
                self._write(batch)
            except Exception:  # noqa: BLE001 -- best-effort, the writer thread must never die
                logger.warning("write-behind: batch write failed", exc_info=True)
            finally:
                for _ in batch:
                    self._queue.task_done()

    def _write(self, batch: List[_Item]) -> None:
        groups: Dict[Tuple[int, str, Tuple[Tuple[str, str], ...]], List[_Item]] = {}
        for item in batch:
            adapter, table, _row, session_vars = item
            key = (id(adapter), table, tuple(sorted((session_vars or {}).items())))
            groups.setdefault(key, []).append(item)
        for items in groups.values():
            adapter, table, _row, session_vars = items[0]
            rows = [i[2] for i in items]
            try:
                adapter.insert_many(table, rows, session_vars=session_vars)
                self.batches += 1
                self.rows += len(rows)
            except Exception:  # noqa: BLE001 -- one bad row must not cost the rest: retry them one by one
                logger.warning("write-behind: pipelined insert into %s failed; retrying row by row", table, exc_info=True)
                for r in rows:
                    try:
                        adapter.insert(table, r, session_vars=session_vars)
                        self.rows += 1
                    except Exception:  # noqa: BLE001
                        logger.warning("write-behind: dropped one row for %s", table, exc_info=True)
