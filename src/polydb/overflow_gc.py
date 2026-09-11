"""
src/polydb/overflow_gc.py

Garbage collection for orphaned NoSQL KV overflow blobs.

Every `NoSQLKVAdapter` spills an oversized record's JSON to the paired
object store under a content-addressed key (``overflow/<md5>.json``) and
keeps only a small reference row (``_overflow``/``_blob_key``/``_size``/
``_checksum``) in the KV store itself (see
``NoSQLKVAdapter._check_overflow``). An update to an already-overflowed
record writes a *new* blob (a new md5 for the new content) and leaves the
old one behind with nothing referencing it any more -- ``delete()`` only
ever removes the reference row, never the blob it pointed at. Nothing in
this codebase has ever swept those orphans, so overflow storage grows
without bound for the life of a deployment (see CLAUDE.md's "Overflow
blobs are never garbage-collected").

This module closes that gap with a conservative two-pass mark-and-sweep:
a blob under the given prefix that isn't referenced by any live row is
only a *candidate* the first time it's seen -- it's recorded, with the
wall-clock time it was first seen, in a small JSON state blob written
back into the same object store (``<prefix>_gc_state.json``). A candidate
is only actually deleted once ``grace_seconds`` have elapsed since that
first sighting *and* it is still unreferenced on this run. This gives a
real grace window without needing last-modified timestamps from the
object store, which the ``ObjectStorageAdapter`` protocol does not
expose on any backend.

A blob that becomes referenced again between two sweeps (a genuine race
with a concurrent write, not merely a slow one -- content-addressed keys
mean a live row can only ever reference the blob matching its own
current content) drops out of the candidate list rather than being
deleted, so a sweep can never delete a blob a live row still points at.

PolyDB doesn't own a model registry (see databaseFactory.py's own "dumb
storage layer" design stance) -- the caller supplies the list of models
to scan, e.g.:

    from polydb.overflow_gc import sweep_overflow_blobs
    from polydb.cloudDatabaseFactory import CloudDatabaseFactory

    factory = CloudDatabaseFactory()
    report = sweep_overflow_blobs(
        factory.get_object_storage(),
        factory.get_nosql_kv(),
        models=[Order, Widget],
    )

Run on a schedule (daily is plenty) -- a blob only gets deleted on the
*second* sweep that finds it orphaned, at least `grace_seconds` after the
first.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple

from .json_safe import json_safe

if TYPE_CHECKING:
    from .base.NoSQLKVAdapter import NoSQLKVAdapter
    from .base.ObjectStorageAdapter import ObjectStorageAdapter


DEFAULT_PREFIX = "overflow/"
DEFAULT_GRACE_SECONDS = 24 * 60 * 60  # 24h


@dataclass
class GCReport:
    """Result of one sweep_overflow_blobs() call."""

    scanned_blobs: int = 0
    referenced_blobs: int = 0
    new_candidates: List[str] = field(default_factory=list)
    still_pending: List[str] = field(default_factory=list)
    deleted: List[str] = field(default_factory=list)
    errors: List[Tuple[str, str]] = field(default_factory=list)


def _state_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/_gc_state.json"


def _load_state(object_storage: "ObjectStorageAdapter", prefix: str) -> dict:
    try:
        raw = object_storage.get(_state_key(prefix))
    except Exception:
        return {}
    if not raw:
        return {}
    try:
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except Exception:
        return {}


def _save_state(object_storage: "ObjectStorageAdapter", prefix: str, state: dict) -> None:
    object_storage.put(_state_key(prefix), json.dumps(state, default=json_safe).encode())


def _referenced_blob_keys(adapter: "NoSQLKVAdapter", models: Iterable[type]) -> set:
    """Every _blob_key a live row currently points at, across the given models."""
    referenced: set = set()
    for model in models:
        for row in adapter._query_raw(model, {}, None):
            blob_key = row.get("_blob_key")
            if blob_key:
                referenced.add(blob_key)
    return referenced


def sweep_overflow_blobs(
    object_storage: "ObjectStorageAdapter",
    adapter: "NoSQLKVAdapter",
    models: Iterable[type],
    *,
    prefix: str = DEFAULT_PREFIX,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
    dry_run: bool = False,
    now: Optional[float] = None,
) -> GCReport:
    """
    Run one GC pass over `prefix` in `object_storage`, treating every
    `_blob_key` referenced by a live row of `models` (read through
    `adapter`) as live. Anything else is orphaned; an orphan is only
    deleted once it has been orphaned for at least `grace_seconds` across
    two separate sweeps. `dry_run=True` reports what *would* be deleted
    without deleting anything or advancing the grace clock.
    """
    now = time.time() if now is None else now
    report = GCReport()

    state_key_name = _state_key(prefix)
    all_keys = [k for k in object_storage.list(prefix) if k != state_key_name]
    report.scanned_blobs = len(all_keys)

    referenced = _referenced_blob_keys(adapter, models)
    report.referenced_blobs = len(referenced)

    state = _load_state(object_storage, prefix)
    next_state: dict = {}

    for key in all_keys:
        if key in referenced:
            continue  # live -- never a candidate, never written into next_state

        first_seen = state.get(key)
        if first_seen is None:
            # First sweep to see this one orphaned: start its clock, don't delete yet.
            next_state[key] = now
            report.new_candidates.append(key)
            continue

        if now - first_seen < grace_seconds:
            next_state[key] = first_seen
            report.still_pending.append(key)
            continue

        # Orphaned on this sweep AND the previous one, past the grace window.
        if dry_run:
            report.deleted.append(key)  # what WOULD be deleted
            next_state[key] = first_seen
            continue

        try:
            object_storage.delete(key)
            report.deleted.append(key)
        except Exception as e:
            report.errors.append((key, str(e)))
            next_state[key] = first_seen  # retry on the next sweep

    if not dry_run:
        _save_state(object_storage, prefix, next_state)

    return report
