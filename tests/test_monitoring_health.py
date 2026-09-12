# Phase 1 (CI hygiene pass): a real, found-while-adding-flake8-to-CI bug in
# HealthCheck.check_cache_health() (src/polydb/monitoring.py) -- it wrote a
# test value into the cache, read it back into `retrieved`, and then never
# actually compared the two. set()/get() not raising is not the same thing
# as the cache round-tripping the value correctly, so a cache silently
# returning stale/empty/wrong data still reported {"status": "healthy"} as
# long as neither call raised an exception. Reproduced here with a fake
# cache that "succeeds" but returns the wrong value, proving the pre-fix
# code would have called that healthy.
from __future__ import annotations

from polydb.monitoring import HealthCheck


class _FakeCache:
    def __init__(self, *, broken: bool = False):
        self._broken = broken
        self.set_calls: list[tuple] = []

    def set(self, key, namespace, value, ttl):
        self.set_calls.append((key, namespace, value, ttl))

    def get(self, key, namespace):
        if self._broken:
            # A cache that "works" (no exception) but silently returns
            # stale/wrong data -- the exact failure mode round-trip
            # verification exists to catch.
            return {"test": False}
        return {"test": True}


class _FakeFactory:
    def __init__(self, cache):
        self._cache = cache
        self._sql = None
        self._provider_name = "postgresql"


def test_check_cache_health_reports_healthy_on_a_real_round_trip():
    factory = _FakeFactory(_FakeCache(broken=False))
    result = HealthCheck(factory).check_cache_health()
    assert result["status"] == "healthy"
    assert "latency_ms" in result


def test_check_cache_health_reports_unhealthy_when_the_round_trip_returns_the_wrong_value():
    factory = _FakeFactory(_FakeCache(broken=True))
    result = HealthCheck(factory).check_cache_health()
    assert result["status"] == "unhealthy"
    assert "mismatch" in result["error"]


def test_check_cache_health_reports_disabled_when_no_cache_is_configured():
    factory = _FakeFactory(None)
    result = HealthCheck(factory).check_cache_health()
    assert result["status"] == "disabled"
