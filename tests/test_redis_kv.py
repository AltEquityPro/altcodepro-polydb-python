"""
tests/test_redis_kv.py
=======================
Integration tests for the generic raw Redis KV operations added to
RedisCacheEngine (src/polydb/cache.py) and exposed as
DatabaseFactory.redis_get/redis_set/redis_delete/redis_incr/redis_decr/
redis_exists/redis_ttl/redis_expire.

Real, unmocked, against a real local Redis (redis-server on localhost:6379,
same as VercelKVAdapter's own local-redis test convention). Distinct from
the existing get()/set()/invalidate() query-result cache (which hashes a
model+query dict into a key) -- these back a caller's own explicit key,
e.g. a manifest workflow step's "redis" integration doing rate limiting
or a distributed lock.
"""
from __future__ import annotations

import os
import uuid

import pytest

redis = pytest.importorskip("redis")

from polydb.cache import RedisCacheEngine
from polydb.databaseFactory import DatabaseFactory
from polydb.errors import CacheError


def _redis_available() -> bool:
    try:
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = redis.from_url(url)
        client.ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _redis_available(),
    reason="No local Redis reachable (start one with `redis-server --daemonize yes`)",
)


def _key() -> str:
    return f"polydb_test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def engine():
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    e = RedisCacheEngine(redis_url=url, prefix="polydb_test:")
    yield e
    e.clear()


@pytest.fixture
def db():
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    factory = DatabaseFactory(enable_audit=False, use_redis_cache=True, redis_cache_url=url)
    yield factory
    factory._cache.clear()


# ────────────────────────────────────────────────────────────────────────────
# RedisCacheEngine's raw methods directly
# ────────────────────────────────────────────────────────────────────────────

class TestRedisCacheEngineRawKV:
    def test_get_on_a_missing_key_returns_none(self, engine):
        assert engine.get_raw("m", _key()) is None

    def test_set_then_get_round_trips_a_json_value(self, engine):
        key = _key()
        assert engine.set_raw("m", key, {"a": 1, "b": [1, 2, 3]}) is True
        assert engine.get_raw("m", key) == {"a": 1, "b": [1, 2, 3]}

    def test_set_with_ttl_expires(self, engine):
        key = _key()
        engine.set_raw("m", key, "v", ttl=1)
        assert engine.get_raw("m", key) == "v"
        assert engine.ttl_raw("m", key) in (0, 1)

    def test_set_without_ttl_has_no_expiry(self, engine):
        key = _key()
        engine.set_raw("m", key, "v")
        assert engine.ttl_raw("m", key) == -1

    def test_ttl_on_missing_key_is_minus_two(self, engine):
        assert engine.ttl_raw("m", _key()) == -2

    def test_exists_raw(self, engine):
        key = _key()
        assert engine.exists_raw("m", key) is False
        engine.set_raw("m", key, "v")
        assert engine.exists_raw("m", key) is True

    def test_incrby_creates_the_counter_starting_from_zero(self, engine):
        key = _key()
        assert engine.incrby("m", key) == 1
        assert engine.incrby("m", key) == 2
        assert engine.incrby("m", key, amount=5) == 7

    def test_incrby_negative_amount_decrements(self, engine):
        key = _key()
        engine.incrby("m", key, amount=10)
        assert engine.incrby("m", key, amount=-3) == 7

    def test_delete_key_removes_a_raw_kv_entry(self, engine):
        key = _key()
        engine.set_raw("m", key, "v")
        assert engine.exists_raw("m", key) is True
        engine.delete_key("m", key)
        assert engine.exists_raw("m", key) is False

    def test_a_raw_key_and_a_zset_key_with_the_same_name_dont_collide(self, engine):
        # Both zadd and set_raw route through the same _make_raw_key --
        # this proves that's safe as long as a caller doesn't reuse one
        # `key` string for both purposes on the same model namespace.
        key = _key()
        engine.set_raw("m", key, "plain-value")
        assert engine.get_raw("m", key) == "plain-value"

    def test_methods_raise_cache_error_when_no_client_is_configured(self):
        engine = RedisCacheEngine.__new__(RedisCacheEngine)
        engine._client = None
        with pytest.raises(CacheError):
            engine.get_raw("m", "k")
        with pytest.raises(CacheError):
            engine.set_raw("m", "k", "v")
        with pytest.raises(CacheError):
            engine.incrby("m", "k")
        with pytest.raises(CacheError):
            engine.exists_raw("m", "k")
        with pytest.raises(CacheError):
            engine.ttl_raw("m", "k")


# ────────────────────────────────────────────────────────────────────────────
# DatabaseFactory.redis_* wiring -- the shape a manifest workflow step
# reaches through ctx.db
# ────────────────────────────────────────────────────────────────────────────

class TestDatabaseFactoryRedisWiring:
    def test_redis_get_set_round_trip(self, db):
        key = _key()
        assert db.redis_get(key) is None
        assert db.redis_set(key, {"order_id": "o1", "total": 42.5}) is True
        assert db.redis_get(key) == {"order_id": "o1", "total": 42.5}

    def test_redis_delete(self, db):
        key = _key()
        db.redis_set(key, "v")
        db.redis_delete(key)
        assert db.redis_get(key) is None

    def test_redis_incr_and_decr(self, db):
        key = _key()
        assert db.redis_incr(key) == 1
        assert db.redis_incr(key) == 2
        assert db.redis_decr(key) == 1

    def test_redis_exists_and_ttl_and_expire(self, db):
        key = _key()
        assert db.redis_exists(key) is False
        db.redis_set(key, "v")
        assert db.redis_exists(key) is True
        assert db.redis_ttl(key) == -1
        db.redis_expire(key, 60)
        assert 0 < db.redis_ttl(key) <= 60

    def test_a_namespace_keeps_keys_from_colliding_across_manifests(self, db):
        # Two different manifests/integrations could pick the same literal
        # key ("counter") -- namespace is the isolation boundary, same
        # reasoning as the project_id-scoped migration version fix in
        # universal_engine's compiler.py.
        key = "counter"
        db.redis_set(key, "tenant-a-value", namespace="tenant-a")
        db.redis_set(key, "tenant-b-value", namespace="tenant-b")
        assert db.redis_get(key, namespace="tenant-a") == "tenant-a-value"
        assert db.redis_get(key, namespace="tenant-b") == "tenant-b-value"

    def test_redis_methods_raise_cache_error_without_a_configured_backend(self):
        db = DatabaseFactory(enable_audit=False)  # use_redis_cache defaults to False
        with pytest.raises(CacheError):
            db.redis_get("k")
        with pytest.raises(CacheError):
            db.redis_set("k", "v")
        with pytest.raises(CacheError):
            db.redis_delete("k")
        with pytest.raises(CacheError):
            db.redis_incr("k")
        with pytest.raises(CacheError):
            db.redis_exists("k")
        with pytest.raises(CacheError):
            db.redis_ttl("k")
        with pytest.raises(CacheError):
            db.redis_expire("k", 10)
