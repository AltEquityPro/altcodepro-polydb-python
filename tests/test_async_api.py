"""
tests/test_async_api.py
========================
Real, end-to-end proof that `polydb.aio.AsyncPolyDB`/`AsyncDatabaseFactory`
(closing CLAUDE.md's own known-gap #5, "No async API") genuinely work
against a real Postgres backend -- not just that the wrapper's own
thread-hopping logic looks right in isolation. Every call below is a real
`await`, on a real running event loop, hitting the real `polydb_items`
table `pg_schema` already provisions for `test_postgresql.py`.

Deliberately does NOT reuse `test_multi_engine.py`'s own `_patch_factory`
helper (`db._meta`/`db._model_type`) -- that helper references methods
`DatabaseFactory` doesn't actually have (meta extraction is the
module-level `_extract_meta()` function, not an instance method); using
a plain model class with a real `__polydb__` dict is both the documented,
real mechanism and avoids depending on that helper's own bug.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import uid

from polydb.aio import AsyncDatabaseFactory, AsyncPolyDB
from polydb.databaseFactory import DatabaseFactory, EngineConfig

pytestmark = pytest.mark.postgresql


class Item:
    __polydb__ = {"storage": "sql", "table": "polydb_items"}


def _row(**extra) -> dict:
    return {"id": uid(), "name": "async-item", "value": 1, **extra}


@pytest.fixture
def sync_db(pg_factory) -> DatabaseFactory:
    engine = EngineConfig(
        name="primary", cloud_factory=pg_factory, sql_models={"Item"}, is_default_sql=True
    )
    return DatabaseFactory(engines=[engine], enable_audit=False, enable_cache=False)


@pytest.mark.asyncio
async def test_async_database_factory_create_read_update_delete_roundtrip(sync_db, pg_schema):
    adb = AsyncDatabaseFactory(sync=sync_db)
    row = _row()

    created = await adb.create(Item, row)
    assert created["id"] == row["id"]

    fetched = await adb.read_one(Item, {"id": row["id"]})
    assert fetched["name"] == "async-item"

    updated = await adb.update(Item, {"id": row["id"]}, {"value": 2})
    assert updated["value"] == 2

    deleted = await adb.delete(Item, {"id": row["id"]})
    assert deleted

    assert await adb.read_one(Item, {"id": row["id"]}) is None


@pytest.mark.asyncio
async def test_async_database_factory_constructs_its_own_sync_instance_when_no_sync_is_given(
    pg_factory, pg_schema
):
    engine = EngineConfig(
        name="primary", cloud_factory=pg_factory, sql_models={"Item"}, is_default_sql=True
    )
    adb = AsyncDatabaseFactory(engines=[engine], enable_audit=False, enable_cache=False)
    row = _row()
    created = await adb.create(Item, row)
    assert created["id"] == row["id"]
    assert isinstance(adb.sync, DatabaseFactory)


def test_async_database_factory_refuses_both_sync_and_constructor_args(pg_factory):
    engine = EngineConfig(
        name="primary", cloud_factory=pg_factory, sql_models={"Item"}, is_default_sql=True
    )
    real_sync = DatabaseFactory(engines=[engine], enable_audit=False, enable_cache=False)
    with pytest.raises(TypeError, match="not both"):
        AsyncDatabaseFactory(sync=real_sync, engines=[engine])


@pytest.mark.asyncio
async def test_async_polydb_wraps_the_facade_and_forwards_create_read(pg_factory, pg_schema):
    # soft_delete=False: PolyDB defaults to True (unlike DatabaseFactory's
    # own False default), which stamps a `deleted_by` column
    # `_POSTGRES_DDL`'s bare `polydb_items` table doesn't carry -- not an
    # async-wrapper concern, so sidestepped rather than worked around.
    apoly = AsyncPolyDB(
        cloud_factory=pg_factory, enable_audit=False, enable_cache=False, soft_delete=False
    )
    row = _row()

    created = await apoly.create(Item, row)
    assert created["id"] == row["id"]

    rows = await apoly.read(Item, {"id": row["id"]})
    assert len(rows) == 1
    assert rows[0]["id"] == row["id"]

    await apoly.delete(Item, {"id": row["id"]})


@pytest.mark.asyncio
async def test_async_polydb_wraps_an_already_constructed_sync_polydb(pg_factory, pg_schema):
    from polydb.PolyDB import PolyDB

    sync_poly = PolyDB(
        cloud_factory=pg_factory, enable_audit=False, enable_cache=False, soft_delete=False
    )
    apoly = AsyncPolyDB(sync=sync_poly)
    assert apoly.sync is sync_poly

    row = _row()
    await apoly.create(Item, row)
    fetched = await apoly.read(Item, {"id": row["id"]})
    assert fetched[0]["name"] == "async-item"
    await apoly.delete(Item, {"id": row["id"]})


@pytest.mark.asyncio
async def test_non_callable_attributes_pass_through_synchronously_not_wrapped(sync_db):
    from polydb.security import DataMasking

    adb = AsyncDatabaseFactory(sync=sync_db)
    # `masking` is a plain DataMasking instance attribute on
    # DatabaseFactory, not a method -- must be readable directly, never
    # an unawaited coroutine.
    assert isinstance(adb.masking, DataMasking)


@pytest.mark.asyncio
async def test_calls_genuinely_run_off_the_event_loop_thread(sync_db, pg_schema):
    """Proves this isn't secretly synchronous under the hood: the
    underlying sync call observes a DIFFERENT thread than the one
    running this test coroutine."""
    import threading

    adb = AsyncDatabaseFactory(sync=sync_db)
    test_thread = threading.current_thread()
    observed_threads: list[threading.Thread] = []

    original_create = sync_db.create

    def _spy_create(*args, **kwargs):
        observed_threads.append(threading.current_thread())
        return original_create(*args, **kwargs)

    sync_db.create = _spy_create
    try:
        await adb.create(Item, _row())
    finally:
        sync_db.create = original_create

    assert len(observed_threads) == 1
    assert observed_threads[0] is not test_thread


@pytest.mark.asyncio
async def test_dedicated_executor_is_used_when_max_workers_is_given_and_closes_cleanly(
    sync_db, pg_schema
):
    adb = AsyncDatabaseFactory(sync=sync_db, max_workers=2)
    try:
        row = _row()
        await adb.create(Item, row)
        assert (await adb.read_one(Item, {"id": row["id"]}))["id"] == row["id"]
        await adb.delete(Item, {"id": row["id"]})
    finally:
        adb.close()
    # Closed executor refuses new work -- confirms close() genuinely shut it down.
    with pytest.raises(RuntimeError):
        await adb.create(Item, _row())


@pytest.mark.asyncio
async def test_async_context_manager_closes_the_dedicated_executor_on_exit(sync_db, pg_schema):
    async with AsyncDatabaseFactory(sync=sync_db, max_workers=2) as adb:
        row = _row()
        await adb.create(Item, row)
        await adb.delete(Item, {"id": row["id"]})
    assert adb._executor._shutdown  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_concurrent_awaits_genuinely_run_in_parallel_not_serialized(sync_db, pg_schema):
    """A real timing proof, not just a functional one: N concurrent
    `create()` calls, each sleeping briefly on its own worker thread
    before the real DB write, complete in roughly one sleep's worth of
    wall-clock time, not N sleeps -- proving asyncio.to_thread genuinely
    overlaps them rather than serializing behind a single lock."""
    import time

    adb = AsyncDatabaseFactory(sync=sync_db, max_workers=8)
    delay = 0.2
    n = 5
    original_create = sync_db.create

    def _slow_create(*args, **kwargs):
        time.sleep(delay)
        return original_create(*args, **kwargs)

    sync_db.create = _slow_create
    try:
        start = asyncio.get_event_loop().time()
        await asyncio.gather(*[adb.create(Item, _row()) for _ in range(n)])
        elapsed = asyncio.get_event_loop().time() - start
    finally:
        sync_db.create = original_create
        adb.close()

    assert elapsed < delay * n  # would be >= delay*n if serialized
