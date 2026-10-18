"""ReadBatch / PostgreSQLAdapter.select_many: many SELECTs, one pipelined round trip, one connection, tenant isolation intact."""
import threading
import time
import uuid

import psycopg
import pytest

from polydb.adapters.PostgreSQLAdapter import PostgreSQLAdapter
from polydb.errors import DatabaseError
from polydb.read_batch import ReadBatch, SelectSpec, use_read_batch

pytestmark = pytest.mark.postgresql

SUFFIX = uuid.uuid4().hex[:8]
T_ROWS = f"rb_rows_{SUFFIX}"
T_OTHER = f"rb_other_{SUFFIX}"
ROLE = f"rb_app_{SUFFIX}"
PASSWORD = "rbpass"


@pytest.fixture(scope="module")
def admin_dsn(test_config):
    if not test_config.pg_url:
        pytest.skip("no PostgreSQL URL (--pg-url)")
    return test_config.pg_url


@pytest.fixture(scope="module")
def app_dsn(admin_dsn):
    """A NON-superuser role (row-level security does not apply to superusers) over two small tables, one protected by RLS."""
    from urllib.parse import urlparse, urlunparse

    with psycopg.connect(admin_dsn, autocommit=True) as c:
        c.execute(f"DROP TABLE IF EXISTS {T_ROWS}, {T_OTHER}")
        c.execute(f"CREATE TABLE {T_ROWS} (id int, tenant_id text, name text)")
        c.execute(f"CREATE TABLE {T_OTHER} (id int, note text)")
        c.execute(
            f"INSERT INTO {T_ROWS} SELECT g, CASE WHEN g % 2 = 0 THEN 'A' ELSE 'B' END, 'row' || g FROM generate_series(1, 10) g"
        )
        c.execute(f"INSERT INTO {T_OTHER} VALUES (1, 'x'), (2, 'y')")
        c.execute(f"ALTER TABLE {T_ROWS} ENABLE ROW LEVEL SECURITY")
        c.execute(f"ALTER TABLE {T_ROWS} FORCE ROW LEVEL SECURITY")
        c.execute(f"CREATE POLICY p ON {T_ROWS} USING (tenant_id = current_setting('app.tenant_id', true))")
        c.execute(f"DROP ROLE IF EXISTS {ROLE}")
        c.execute(f"CREATE ROLE {ROLE} LOGIN PASSWORD '{PASSWORD}'")
        c.execute(f"GRANT SELECT ON {T_ROWS}, {T_OTHER} TO {ROLE}")
    u = urlparse(admin_dsn)
    netloc = f"{ROLE}:{PASSWORD}@{u.hostname}" + (f":{u.port}" if u.port else "")
    yield urlunparse(u._replace(netloc=netloc, query="application_name=rb-test"))
    with psycopg.connect(admin_dsn, autocommit=True) as c:
        c.execute(f"DROP TABLE IF EXISTS {T_ROWS}, {T_OTHER}")
        c.execute(f"DROP ROLE IF EXISTS {ROLE}")


@pytest.fixture
def adapter(app_dsn, monkeypatch):
    monkeypatch.setenv("POSTGRES_MAX_CONNECTIONS", "6")
    monkeypatch.setenv("POSTGRES_POOL_IDLE_KEEP", "0")
    a = PostgreSQLAdapter(connection_string=app_dsn)
    yield a
    a.reset_pool()
    a._pool.closeall()


def _ids(rows):
    return sorted(r["id"] for r in rows)


def test_select_many_returns_each_spec_its_own_rows_and_keeps_tenant_isolation(adapter):
    specs = [
        SelectSpec(T_ROWS, {}, session_vars={"app.tenant_id": "A"}),
        SelectSpec(T_ROWS, {}, session_vars={"app.tenant_id": "B"}),
        SelectSpec(T_OTHER, {"id": 2}),
        SelectSpec(T_ROWS, {"id": [1, 2, 3, 4]}, fields=["id"], session_vars={"app.tenant_id": "A"}),
        SelectSpec(T_ROWS, {}, limit=2, session_vars={"app.tenant_id": "nobody"}),
    ]
    out = adapter.select_many(specs)
    assert _ids(out[0]) == [2, 4, 6, 8, 10]          # tenant A only
    assert _ids(out[1]) == [1, 3, 5, 7, 9]           # tenant B only, on the very same connection and pipeline
    assert out[2] == [{"id": 2, "note": "y"}]
    assert _ids(out[3]) == [2, 4] and set(out[3][0]) == {"id"}
    assert out[4] == []                              # an unknown tenant sees nothing
    # nothing leaked onto the connection the pipeline used
    with adapter._pool._pool.connection() as c:
        assert c.execute("select current_setting('app.tenant_id', true)").fetchone()[0] in (None, "")


def test_a_failing_query_fails_alone_and_the_ones_after_it_still_run(adapter):
    out = adapter.select_many([
        SelectSpec(T_OTHER, {}),
        SelectSpec("rb_missing_table_" + SUFFIX, {}),
        SelectSpec(T_ROWS, {}, session_vars={"app.tenant_id": "A"}),
        SelectSpec(T_OTHER, {"id": 1}),
    ])
    assert _ids(out[0]) == [1, 2]
    assert isinstance(out[1], DatabaseError)
    assert _ids(out[2]) == [2, 4, 6, 8, 10]
    assert out[3] == [{"id": 1, "note": "x"}]


def test_a_session_var_outside_the_allow_list_is_refused_for_that_spec_only(adapter):
    out = adapter.select_many([SelectSpec(T_OTHER, {}, session_vars={"search_path": "x"}), SelectSpec(T_OTHER, {"id": 1})])
    assert isinstance(out[0], DatabaseError) and "allow-list" in str(out[0])
    assert out[1] == [{"id": 1, "note": "x"}]


def test_threads_inside_a_batch_share_one_round_trip_and_one_connection(adapter, app_dsn, admin_dsn):
    n = 12
    batch = ReadBatch(parties=n)
    results, errors = {}, []

    def task(i):
        try:
            with use_read_batch(batch):
                results[i] = adapter.select(T_OTHER, {"id": 1 + (i % 2)})
        except Exception as e:  # pragma: no cover
            errors.append(e)
        finally:
            batch.task_done()

    peak = {"n": 0}
    stop = threading.Event()

    def watch():
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            while not stop.is_set():
                k = c.execute(
                    "select count(*) from pg_stat_activity where application_name='rb-test' and pid <> pg_backend_pid()"
                ).fetchone()[0]
                peak["n"] = max(peak["n"], k)
                time.sleep(0.002)

    w = threading.Thread(target=watch)
    w.start()
    threads = [threading.Thread(target=task, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    w.join()

    assert not errors
    assert all(results[i] == [{"id": 1 + (i % 2), "note": "x" if i % 2 == 0 else "y"}] for i in range(n))
    assert batch.statements == n
    assert batch.flushes <= 3                      # all twelve reads went out in (very nearly) one pipelined round trip
    assert peak["n"] <= 2, f"{peak['n']} connections were open at once (expected one for the whole batch)"


def test_a_batch_flushes_when_a_party_finishes_without_reading(adapter):
    batch = ReadBatch(parties=2, window_s=5.0)  # a long window: only the 'everyone else is done' rule can release the waiter
    out = {}

    def reader():
        with use_read_batch(batch):
            out["rows"] = adapter.select(T_OTHER, {"id": 1})
        batch.task_done()

    t = threading.Thread(target=reader)
    t0 = time.monotonic()
    t.start()
    time.sleep(0.1)
    batch.task_done()  # the second party had nothing to read
    t.join(timeout=3)
    assert out["rows"] == [{"id": 1, "note": "x"}] and time.monotonic() - t0 < 3


def test_select_outside_a_batch_and_with_an_explicit_transaction_are_unaffected(adapter):
    assert _ids(adapter.select(T_OTHER, {})) == [1, 2]
    tx = adapter.begin_transaction()
    try:
        with use_read_batch(ReadBatch(parties=1)) as _:
            assert _ids(adapter.select(T_OTHER, {}, tx=tx)) == [1, 2]   # a caller's own transaction is never batched
    finally:
        adapter.rollback(tx)


def test_linq_queries_are_batched_too_with_their_own_tenant(adapter):
    from polydb.query import Operator, QueryBuilder

    rows_q = QueryBuilder().where("id", Operator.GT, 4).order_by("id", descending=True)
    count_q = QueryBuilder().count()
    out = adapter.select_many([
        SelectSpec(T_ROWS, builder=rows_q, session_vars={"app.tenant_id": "A"}),
        SelectSpec(T_ROWS, builder=count_q, session_vars={"app.tenant_id": "B"}),
    ])
    assert [r["id"] for r in out[0]] == [10, 8, 6]
    assert out[1] == 5


def test_query_linq_inside_a_batch_takes_part_in_it(adapter):
    from polydb.query import Operator, QueryBuilder

    batch = ReadBatch(parties=2)
    got = {}

    def a():
        with use_read_batch(batch):
            got["a"] = adapter.query_linq(T_ROWS, QueryBuilder().where("id", Operator.LT, 3).order_by("id"),
                                           session_vars={"app.tenant_id": "A"})
        batch.task_done()

    def b():
        with use_read_batch(batch):
            got["b"] = adapter.select(T_OTHER, {})
        batch.task_done()

    ts = [threading.Thread(target=a), threading.Thread(target=b)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert [r["id"] for r in got["a"]] == [2] and _ids(got["b"]) == [1, 2]
    assert batch.flushes == 1 and batch.statements == 2
