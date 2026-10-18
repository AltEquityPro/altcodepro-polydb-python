"""LazyPool (psycopg 3): opens nothing until needed, waits instead of failing when exhausted, keeps at most `keep` idle."""
import os
import threading
import time

import pytest

from polydb.adapters.PostgreSQLAdapter import LazyPool, PoolError

pytestmark = pytest.mark.postgresql


@pytest.fixture
def dsn(test_config):
    if not test_config.pg_url:
        pytest.skip("no PostgreSQL URL (--pg-url)")
    return test_config.pg_url


def _server_conns(dsn):
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as c:
        return c.execute(
            "select count(*) from pg_stat_activity where application_name = 'polydb-lazy-test' and pid <> pg_backend_pid()"
        ).fetchone()[0]


def _pool(dsn, **kw):
    sep = "&" if "?" in dsn else "?"
    return LazyPool(f"{dsn}{sep}application_name=polydb-lazy-test", **kw)


def test_opens_nothing_at_start_when_keep_is_zero(dsn):
    pool = _pool(dsn, max_size=4, keep=0)
    try:
        time.sleep(0.3)
        assert _server_conns(dsn) == 0
        c = pool.getconn()
        assert _server_conns(dsn) == 1
        pool.putconn(c)
    finally:
        pool.closeall()


def test_keeps_only_one_idle_connection_by_default(dsn):
    pool = _pool(dsn, max_size=5, keep=1, max_idle=0.5)
    try:
        conns = [pool.getconn() for _ in range(4)]
        assert _server_conns(dsn) >= 4
        for c in conns:
            pool.putconn(c)
        time.sleep(2.0)  # the extra idle ones are closed after max_idle
        assert _server_conns(dsn) == 1
    finally:
        pool.closeall()


def test_waits_for_a_free_connection_instead_of_failing_at_once(dsn):
    pool = _pool(dsn, max_size=2, keep=0, acquire_timeout=3)
    try:
        a, b = pool.getconn(), pool.getconn()
        threading.Timer(0.3, lambda: pool.putconn(a)).start()
        t0 = time.monotonic()
        c = pool.getconn()
        assert 0.2 < time.monotonic() - t0 < 2.5
        pool.putconn(b)
        pool.putconn(c)
    finally:
        pool.closeall()


def test_times_out_with_a_clear_message_when_nothing_is_returned(dsn):
    pool = _pool(dsn, max_size=1, keep=0, acquire_timeout=0.3)
    try:
        held = pool.getconn()
        with pytest.raises(PoolError, match="connection pool exhausted: waited 0.3s for one of 1"):
            pool.getconn()
        pool.putconn(held)
        assert pool.getconn() is not None  # the slot came back
    finally:
        pool.closeall()


def test_a_connection_returned_mid_transaction_is_rolled_back(dsn):
    pool = _pool(dsn, max_size=1, keep=0)
    try:
        c = pool.getconn()
        c.execute("create temp table lazy_t(i int)")
        pool.putconn(c)  # still inside the implicit transaction
        c2 = pool.getconn()
        assert c2.info.transaction_status.name == "IDLE"
        pool.putconn(c2)
    finally:
        pool.closeall()
