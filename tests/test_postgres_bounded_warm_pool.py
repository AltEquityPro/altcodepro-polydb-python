"""BoundedWarmPool: waits instead of failing when exhausted, and keeps returned connections warm."""
import threading
import time

import psycopg2.extensions
import psycopg2.pool
import pytest

from polydb.adapters.PostgreSQLAdapter import BoundedWarmPool


class _Info:
    transaction_status = psycopg2.extensions.TRANSACTION_STATUS_IDLE


class FakeConn:
    opened = 0

    def __init__(self):
        FakeConn.opened += 1
        self.closed = 0
        self.info = _Info()

    def close(self):
        self.closed = 1

    def rollback(self):
        pass


@pytest.fixture(autouse=True)
def fake_connect(monkeypatch):
    FakeConn.opened = 0
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: FakeConn())


def _pool(**kw):
    return BoundedWarmPool(1, kw.pop("maxconn", 3), dsn="fake", **kw)


def test_waits_for_a_free_connection_instead_of_failing_at_once():
    pool = _pool(maxconn=2, acquire_timeout=3)
    a, b = pool.getconn(), pool.getconn()
    threading.Timer(0.3, lambda: pool.putconn(a)).start()
    t0 = time.monotonic()
    c = pool.getconn()  # stock psycopg2 raises PoolError("connection pool exhausted") right here
    assert 0.2 < time.monotonic() - t0 < 2.0
    assert c is a
    pool.putconn(b); pool.putconn(c)


def test_times_out_with_a_clear_message_when_nothing_is_returned():
    pool = _pool(maxconn=1, acquire_timeout=0.2)
    held = pool.getconn()
    with pytest.raises(psycopg2.pool.PoolError, match="connection pool exhausted: waited 0.2s for one of 1"):
        pool.getconn()
    pool.putconn(held)
    assert pool.getconn() is held  # the permit came back


def test_a_failed_getconn_does_not_leak_a_permit(monkeypatch):
    pool = _pool(maxconn=1, acquire_timeout=0.2)
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db down")))
    pool._pool.clear()  # force a real connect on the next getconn
    for _ in range(3):
        with pytest.raises(Exception, match="db down"):
            pool.getconn()
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: FakeConn())
    assert pool.getconn() is not None  # would time out if a permit had leaked


def test_returned_connections_stay_open_up_to_keepconn():
    pool = _pool(maxconn=5, keepconn=3)
    conns = [pool.getconn() for _ in range(5)]
    for c in conns:
        pool.putconn(c)
    assert sum(1 for c in conns if not c.closed) == 3 and len(pool._pool) == 3
    opened = FakeConn.opened
    again = [pool.getconn() for _ in range(3)]
    assert FakeConn.opened == opened  # reused, nothing re-opened


def test_stock_minconn_behaviour_when_keepconn_is_not_given():
    pool = _pool(maxconn=4)
    conns = [pool.getconn() for _ in range(3)]
    for c in conns:
        pool.putconn(c)
    assert sum(1 for c in conns if not c.closed) == 1


def test_concurrent_burst_larger_than_the_pool_all_succeed():
    pool = _pool(maxconn=3, keepconn=3, acquire_timeout=5)
    errors, done = [], []

    def work():
        try:
            c = pool.getconn(); time.sleep(0.02); pool.putconn(c); done.append(1)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=work) for _ in range(30)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errors and len(done) == 30
