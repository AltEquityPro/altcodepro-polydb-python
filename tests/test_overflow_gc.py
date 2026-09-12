"""
tests/test_overflow_gc.py
==========================
Phase 0 fix: overflow blobs were never garbage collected (CLAUDE.md known
gap #16). Content-addressed keys mean an update to an already-overflowed
record writes a *new* blob and orphans the old one; delete() only ever
removed the reference row, never the blob. Covers polydb.overflow_gc's
two-pass mark-and-sweep: a live blob is never touched, a newly orphaned
blob is only *recorded* (not deleted), and a blob orphaned across two
sweeps separated by at least `grace_seconds` is deleted on the second.
"""

from __future__ import annotations

import json

import pytest

from polydb.overflow_gc import GCReport, sweep_overflow_blobs


class FakeObjectStorage:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def put(self, key, data, **kwargs):
        self.store[key] = data if isinstance(data, bytes) else data.encode()
        return key

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        del self.store[key]

    def list(self, prefix: str = ""):
        return [k for k in self.store if k.startswith(prefix)]


class FakeAdapter:
    """Stands in for a NoSQLKVAdapter for _query_raw() purposes only."""

    def __init__(self, rows: list[dict]):
        self.rows = rows

    def _query_raw(self, model, filters, limit):
        return self.rows


class Widget:
    pass


def test_a_referenced_blob_is_never_a_candidate():
    os_ = FakeObjectStorage()
    os_.store["overflow/live.json"] = b"{}"
    adapter = FakeAdapter(rows=[{"_blob_key": "overflow/live.json"}])

    report = sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0)

    assert report.scanned_blobs == 1
    assert report.referenced_blobs == 1
    assert report.new_candidates == []
    assert report.deleted == []
    assert "overflow/live.json" in os_.store


def test_a_newly_orphaned_blob_is_recorded_but_not_deleted():
    os_ = FakeObjectStorage()
    os_.store["overflow/orphan.json"] = b"{}"
    adapter = FakeAdapter(rows=[])

    report = sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0)

    assert report.new_candidates == ["overflow/orphan.json"]
    assert report.deleted == []
    assert "overflow/orphan.json" in os_.store  # still there


def test_an_orphan_inside_the_grace_window_is_not_yet_deleted():
    os_ = FakeObjectStorage()
    os_.store["overflow/orphan.json"] = b"{}"
    adapter = FakeAdapter(rows=[])

    sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0, grace_seconds=3600)
    report = sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0 + 60, grace_seconds=3600)

    assert report.still_pending == ["overflow/orphan.json"]
    assert report.deleted == []
    assert "overflow/orphan.json" in os_.store


def test_an_orphan_past_the_grace_window_is_deleted_on_the_second_sweep():
    os_ = FakeObjectStorage()
    os_.store["overflow/orphan.json"] = b"{}"
    adapter = FakeAdapter(rows=[])

    sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0, grace_seconds=3600)
    report = sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0 + 7200, grace_seconds=3600)

    assert report.deleted == ["overflow/orphan.json"]
    assert "overflow/orphan.json" not in os_.store


def test_a_blob_that_becomes_referenced_again_before_the_grace_window_survives():
    """A row can only ever reference the blob matching its own current
    content (content-addressed keys) -- but a concurrent overwrite between
    two sweeps could still re-reference the same key. The sweep must not
    delete it in that case."""
    os_ = FakeObjectStorage()
    os_.store["overflow/reref.json"] = b"{}"
    adapter = FakeAdapter(rows=[])

    sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0, grace_seconds=3600)

    # Something re-referenced it before the second sweep.
    adapter.rows = [{"_blob_key": "overflow/reref.json"}]
    report = sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0 + 7200, grace_seconds=3600)

    assert report.deleted == []
    assert "overflow/reref.json" in os_.store


def test_dry_run_reports_without_deleting_or_advancing_state():
    os_ = FakeObjectStorage()
    os_.store["overflow/orphan.json"] = b"{}"
    adapter = FakeAdapter(rows=[])

    sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0, grace_seconds=3600)
    report = sweep_overflow_blobs(
        os_, adapter, [Widget], now=1000.0 + 7200, grace_seconds=3600, dry_run=True
    )

    assert report.deleted == ["overflow/orphan.json"]  # "would delete"
    assert "overflow/orphan.json" in os_.store  # but it's still there


def test_multiple_models_are_all_scanned_for_references():
    os_ = FakeObjectStorage()
    os_.store["overflow/a.json"] = b"{}"
    os_.store["overflow/b.json"] = b"{}"

    class ModelA:
        pass

    class ModelB:
        pass

    rows_by_model = {
        ModelA: [{"_blob_key": "overflow/a.json"}],
        ModelB: [{"_blob_key": "overflow/b.json"}],
    }

    class MultiModelAdapter:
        def _query_raw(self, model, filters, limit):
            return rows_by_model[model]

    report = sweep_overflow_blobs(os_, MultiModelAdapter(), [ModelA, ModelB], now=1000.0)

    assert report.referenced_blobs == 2
    assert report.deleted == []


def test_the_gc_state_blob_itself_is_never_treated_as_an_orphan_candidate():
    os_ = FakeObjectStorage()
    os_.store["overflow/orphan.json"] = b"{}"
    adapter = FakeAdapter(rows=[])

    sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0)

    # The state file the first sweep wrote must not itself show up as a
    # scanned candidate on the next sweep.
    report = sweep_overflow_blobs(os_, adapter, [Widget], now=1000.0 + 10)
    assert "overflow/_gc_state.json" not in report.new_candidates
    assert "overflow/_gc_state.json" not in report.still_pending
