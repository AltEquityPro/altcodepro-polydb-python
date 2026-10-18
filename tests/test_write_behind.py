"""Write-behind (telemetry rows): queued, then written by one thread in pipelined batches on one connection."""
import threading
import time
import uuid

import psycopg
import pytest

from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter
from polydb.databaseFactory import DatabaseFactory
from polydb.errors import DatabaseError
from polydb.write_behind import WriteBehind

pytestmark = pytest.mark.postgresql

TABLE = f"wb_events_{uuid.uuid4().hex[:8]}"


class _Model:
    __polydb__ = {"storage": "sql", "table": TABLE}


@pytest.fixture(scope="module")
def admin_dsn(test_config):
    if not test_config.pg_url:
        pytest.skip("no PostgreSQL URL (--pg-url)")
    with psycopg.connect(test_config.pg_url, autocommit=True) as c:
        c.execute(f"CREATE TABLE {TABLE} (id text PRIMARY KEY, n int, note text, created_at timestamptz, updated_at timestamptz)")
    yield test_config.pg_url
    with psycopg.connect(test_config.pg_url, autocommit=True) as c:
        c.execute(f"DROP TABLE IF EXISTS {TABLE}")


@pytest.fixture
def adapter(admin_dsn, monkeypatch):
    monkeypatch.setenv("POSTGRES_MAX_CONNECTIONS", "4")
    monkeypatch.setenv("POSTGRES_POOL_IDLE_KEEP", "0")
    sep = "&" if "?" in admin_dsn else "?"
    a = PostgreSQLAdapter(connection_string=f"{admin_dsn}{sep}application_name=wb-test")
    yield a
    a._pool.closeall()


def _count(admin_dsn):
    with psycopg.connect(admin_dsn, autocommit=True) as c:
        return c.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]


def _clear(admin_dsn):
    with psycopg.connect(admin_dsn, autocommit=True) as c:
        c.execute(f"DELETE FROM {TABLE}")


def test_insert_many_is_one_transaction_and_all_or_nothing(adapter, admin_dsn):
    _clear(admin_dsn)
    assert adapter.insert_many(TABLE, [{"id": f"a{i}", "n": i} for i in range(50)]) == 50
    assert _count(admin_dsn) == 50
    with pytest.raises(DatabaseError):
        adapter.insert_many(TABLE, [{"id": "ok1", "n": 1}, {"id": "a1", "n": 2}, {"id": "ok2", "n": 3}])  # a1 already exists
    assert _count(admin_dsn) == 50          # nothing from the failed batch was kept
    # rows with different column sets go in together
    adapter.insert_many(TABLE, [{"id": "m1", "n": 1}, {"id": "m2", "note": "x"}])
    assert _count(admin_dsn) == 52


def test_threads_queue_rows_instantly_and_one_writer_batches_them_on_one_connection(adapter, admin_dsn):
    _clear(admin_dsn)
    wb = WriteBehind()
    peak, stop = {"n": 0}, threading.Event()

    def watch():
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            while not stop.is_set():
                k = c.execute(
                    "select count(*) from pg_stat_activity where application_name='wb-test' and state <> 'idle' and pid <> pg_backend_pid()"
                ).fetchone()[0]
                peak["n"] = max(peak["n"], k)
                time.sleep(0.002)

    w = threading.Thread(target=watch)
    w.start()
    t0 = time.perf_counter()

    def producer(k):
        for i in range(40):
            wb.submit(adapter, TABLE, {"id": f"p{k}-{i}", "n": i})

    ts = [threading.Thread(target=producer, args=(k,)) for k in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    queued_in = time.perf_counter() - t0
    wb.flush()
    stop.set()
    w.join()

    assert _count(admin_dsn) == 320 and wb.rows == 320
    assert wb.batches < 320 / 4                 # far fewer transactions than rows
    assert peak["n"] <= 1, f"{peak['n']} connections were busy at once"
    assert queued_in < 1.0                      # producers never waited on the database


def test_a_bad_row_loses_only_itself(adapter, admin_dsn):
    _clear(admin_dsn)
    wb = WriteBehind()
    wb.submit(adapter, TABLE, {"id": "dup", "n": 1})
    wb.flush()
    for i in range(5):
        wb.submit(adapter, TABLE, {"id": f"good{i}", "n": i})
    wb.submit(adapter, TABLE, {"id": "dup", "n": 2})            # violates the primary key
    for i in range(5, 9):
        wb.submit(adapter, TABLE, {"id": f"good{i}", "n": i})
    wb.flush()
    assert _count(admin_dsn) == 10                              # dup + nine good rows; only the duplicate was dropped


def test_database_factory_create_background_queues_and_flushes(adapter, admin_dsn, pg_factory):
    _clear(admin_dsn)
    db = DatabaseFactory(cloud_factory=pg_factory, enable_audit=False, enable_cache=False)
    row = db.create(_Model, {"id": "bg1", "n": 1}, background=True)
    assert row["id"] == "bg1" and "created_at" in row            # the row as it will be stored
    for i in range(30):
        db.create(_Model, {"id": f"bg{i + 2}", "n": i}, background=True)
    db.flush_background_writes()
    assert _count(admin_dsn) == 31
    db.create(_Model, {"id": "inline", "n": 0})                  # background=False still writes inline
    assert _count(admin_dsn) == 32
