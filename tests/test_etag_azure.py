"""ETag behaviour end to end through DatabaseFactory against a REAL Azure Table + Blob service (Azurite).

Run with: AZURE_STORAGE_CONNECTION_STRING=<azurite connection string> pytest tests/test_etag_azure.py -m azure
"""

from __future__ import annotations

import threading

import pytest
from conftest import uid

from polydb import ConcurrencyError, DatabaseFactory
from polydb.databaseFactory import EngineConfig

pytestmark = pytest.mark.azure


@pytest.fixture(autouse=True)
def _needs_the_real_azure_sdk():
    """Other test modules in this suite replace or reload `azure.*` in sys.modules (stub modules, deleting and re-importing
    packages). After that, nothing that uses the real SDK works in the same process -- the existing
    tests/test_azure.py::test_put_separate_models_isolated fails the same way on an untouched checkout. Skip instead of
    failing, and say how to run it: `pytest tests/test_etag_azure.py -m azure` (a process of its own)."""
    try:
        import azure.core
        import azure.data.tables as tables
        import azure.data.tables._serialize as serialize
        import azure.storage.blob as blob

        clean = (
            hasattr(tables, "TableServiceClient")
            and hasattr(blob, "BlobServiceClient")
            and serialize.MatchConditions is azure.core.MatchConditions
        )
    except Exception:  # noqa: BLE001
        clean = False
    if not clean:
        pytest.skip(
            "another test module stubbed or reloaded the azure SDK in this process; "
            "run alone: pytest tests/test_etag_azure.py -m azure"
        )


class EtagDoc:
    __name__ = "etagdoc"
    __polydb__ = {"storage": "nosql", "table": "etagdoc", "pk_field": "pk", "rk_field": "rk"}


@pytest.fixture()
def db(azure_factory):
    return DatabaseFactory(
        engines=[EngineConfig(name="etag", cloud_factory=azure_factory, is_default_nosql=True)],
        enable_audit=False,
        enable_cache=False,
    )


def _row(db, **extra):
    pk, rk = f"p-{uid()}", "r"
    db.create(EtagDoc, {"pk": pk, "rk": rk, "n": 0, "status": "new", **extra})
    return {"pk": pk, "rk": rk}


def test_reads_expose_etag_only_when_asked(db):
    key = _row(db)
    assert "_etag" not in db.read_one(EtagDoc, key)
    got = db.read_one(EtagDoc, key, with_etag=True)
    assert got["_etag"] and got["status"] == "new"
    assert "_etag" in db.read(EtagDoc, {"pk": key["pk"]}, with_etag=True)[0]
    assert "_etag" in db.read(EtagDoc, {"pk": key["pk"]}, fields=["status"], with_etag=True)[0]  # survives projection
    assert "_etag" not in db.read(EtagDoc, {"pk": key["pk"]})[0]


def test_compare_and_swap_update_and_chaining(db):
    key = _row(db)
    t1 = db.read_one(EtagDoc, key, with_etag=True)["_etag"]
    r2 = db.update(EtagDoc, key, {"status": "running"}, etag=t1, with_etag=True)
    t2 = r2["_etag"]
    assert t2 and t2 != t1
    db.update(EtagDoc, key, {"status": "done"}, etag=t2)
    assert db.read_one(EtagDoc, key)["status"] == "done"
    with pytest.raises(ConcurrencyError):
        db.update(EtagDoc, key, {"status": "lost"}, etag=t1)  # stale
    assert db.read_one(EtagDoc, key)["status"] == "done"


def test_two_writers_from_the_same_version_exactly_one_wins(db):
    key = _row(db)
    tag = db.read_one(EtagDoc, key, with_etag=True)["_etag"]
    wins, losses = [], []
    barrier = threading.Barrier(6)

    def writer(i):
        barrier.wait()
        try:
            db.update(EtagDoc, key, {"winner": i}, etag=tag)
            wins.append(i)
        except ConcurrencyError:
            losses.append(i)

    ts = [threading.Thread(target=writer, args=(i,)) for i in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(wins) == 1 and len(losses) == 5
    assert db.read_one(EtagDoc, key)["winner"] == wins[0]


def test_a_row_read_with_its_etag_round_trips_through_update(db):
    key = _row(db)
    row = db.read_one(EtagDoc, key, with_etag=True)
    row["status"] = "edited"
    db.update(EtagDoc, key, row, etag=row["_etag"])
    fresh = db.read_one(EtagDoc, key)
    assert fresh["status"] == "edited" and "_etag" not in fresh


def test_conditional_delete(db):
    key = _row(db)
    tag = db.read_one(EtagDoc, key, with_etag=True)["_etag"]
    db.update(EtagDoc, key, {"n": 1})  # version moves on
    with pytest.raises(ConcurrencyError):
        db.delete(EtagDoc, key, etag=tag, hard=True)
    assert db.read_one(EtagDoc, key) is not None
    fresh = db.read_one(EtagDoc, key, with_etag=True)["_etag"]
    db.delete(EtagDoc, key, etag=fresh, hard=True)
    assert db.read_one(EtagDoc, key) is None


def test_conditional_update_of_a_row_with_a_large_overflowed_property(db):
    key = _row(db)
    big = "z" * 200_000
    tag = db.read_one(EtagDoc, key, with_etag=True)["_etag"]
    db.update(EtagDoc, key, {"blob": big}, etag=tag)
    assert db.read_one(EtagDoc, key)["blob"] == big
    with pytest.raises(ConcurrencyError):
        db.update(EtagDoc, key, {"blob": big + "x"}, etag=tag)
    assert db.read_one(EtagDoc, key)["blob"] == big
