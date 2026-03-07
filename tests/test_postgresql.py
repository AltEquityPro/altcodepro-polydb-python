"""
tests/test_postgresql.py
========================
Integration tests for PostgreSQLAdapter (via local Postgres on port 5433).

Covers:
  - insert / select / update / upsert / delete (RETURNING *)
  - select with IN / IS NULL / LIMIT / OFFSET
  - select_page with continuation tokens
  - JSONB column round-trip
  - TEXT[] column round-trip
  - query_linq  (WHERE / ORDER BY / LIMIT / COUNT / DISTINCT / GROUP BY)
  - execute raw SQL (fetch=True / fetch_one=True / DDL)
  - transactions  (commit / rollback)
  - distributed advisory lock  (pg_advisory_lock)
  - connection-pool behaviour under concurrent load
  - error paths (bad table, missing row)
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

import pytest

from conftest import uid
from polydb.errors import DatabaseError
from polydb.query import Operator, QueryBuilder

pytestmark = pytest.mark.postgresql


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

TABLE = "polydb_items"
ARCHIVE = "polydb_archive"


def fresh(name: str = "test", value: int = 1, **extra) -> dict:
    return {"id": uid(), "name": name, "value": value, **extra}


# ────────────────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_table(pg_sql, pg_schema):
    """Wipe test data before each test (not after, so failures are inspectable)."""
    pg_sql.execute(f"DELETE FROM {TABLE}")
    pg_sql.execute(f"DELETE FROM {ARCHIVE}")
    yield


# ────────────────────────────────────────────────────────────────────────────
# INSERT
# ────────────────────────────────────────────────────────────────────────────

class TestInsert:
    def test_insert_returns_row(self, pg_sql):
        data = fresh("alice", 10)
        result = pg_sql.insert(TABLE, data)

        assert result["id"] == data["id"]
        assert result["name"] == "alice"
        assert result["value"] == 10

    def test_insert_jsonb_column(self, pg_sql):
        data = fresh(meta={"score": 99, "tags": ["a", "b"]})
        result = pg_sql.insert(TABLE, data)

        assert isinstance(result["meta"], dict)
        assert result["meta"]["score"] == 99
        assert result["meta"]["tags"] == ["a", "b"]

    def test_insert_text_array_column(self, pg_sql):
        data = fresh(tags=["x", "y", "z"])
        result = pg_sql.insert(TABLE, data)

        assert result["tags"] == ["x", "y", "z"]

    def test_insert_null_optional_fields(self, pg_sql):
        data = fresh()               # no meta, no tags
        result = pg_sql.insert(TABLE, data)

        assert result["meta"] is None
        assert result["tags"] is None

    def test_insert_duplicate_pk_raises(self, pg_sql):
        
        data = fresh()
        pg_sql.insert(TABLE, data)
        with pytest.raises(DatabaseError):
            pg_sql.insert(TABLE, data)   # same id → PK violation


# ────────────────────────────────────────────────────────────────────────────
# SELECT
# ────────────────────────────────────────────────────────────────────────────

class TestSelect:
    def test_select_all(self, pg_sql):
        pg_sql.insert(TABLE, fresh("a"))
        pg_sql.insert(TABLE, fresh("b"))
        rows = pg_sql.select(TABLE)
        assert len(rows) >= 2

    def test_select_by_exact_field(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh("charlie", 77))
        results = pg_sql.select(TABLE, {"name": "charlie"})
        assert any(r["id"] == row["id"] for r in results)

    def test_select_in_clause(self, pg_sql):
        r1 = pg_sql.insert(TABLE, fresh("d1", 1))
        r2 = pg_sql.insert(TABLE, fresh("d2", 2))
        r3 = pg_sql.insert(TABLE, fresh("d3", 3))
        found = pg_sql.select(TABLE, {"id": [r1["id"], r2["id"]]})
        ids_found = {r["id"] for r in found}
        assert r1["id"] in ids_found
        assert r2["id"] in ids_found
        assert r3["id"] not in ids_found

    def test_select_is_null(self, pg_sql):
        r_null = pg_sql.insert(TABLE, fresh())             # deleted_at = NULL
        r_del  = pg_sql.insert(TABLE, fresh(deleted_at="2024-01-01T00:00:00"))
        results = pg_sql.select(TABLE, {"deleted_at": None})
        ids = {r["id"] for r in results}
        assert r_null["id"] in ids
        assert r_del["id"] not in ids

    def test_select_limit(self, pg_sql):
        for _ in range(5):
            pg_sql.insert(TABLE, fresh())
        results = pg_sql.select(TABLE, limit=3)
        assert len(results) == 3

    def test_select_offset(self, pg_sql):
        for _ in range(6):
            pg_sql.insert(TABLE, fresh())
        all_rows  = pg_sql.select(TABLE)
        with_off  = pg_sql.select(TABLE, offset=3)
        assert len(with_off) == len(all_rows) - 3

    def test_select_returns_empty_list(self, pg_sql):
        results = pg_sql.select(TABLE, {"id": "does-not-exist"})
        assert results == []


# ────────────────────────────────────────────────────────────────────────────
# SELECT PAGE
# ────────────────────────────────────────────────────────────────────────────

class TestSelectPage:
    def test_pagination_basic(self, pg_sql):
        for i in range(7):
            pg_sql.insert(TABLE, fresh(f"page-{i}", i))

        page1, token1 = pg_sql.select_page(TABLE, {}, page_size=3)
        assert len(page1) == 3
        assert token1 is not None

        page2, token2 = pg_sql.select_page(TABLE, {}, page_size=3, continuation_token=token1)
        assert len(page2) == 3
        assert token2 is not None

        page3, token3 = pg_sql.select_page(TABLE, {}, page_size=3, continuation_token=token2)
        assert len(page3) == 1        # 7 items, 3+3+1
        assert token3 is None

    def test_pagination_no_overlap(self, pg_sql):
        for i in range(6):
            pg_sql.insert(TABLE, fresh(f"pg-{i}"))

        page1, tok = pg_sql.select_page(TABLE, {}, page_size=3)
        page2, _   = pg_sql.select_page(TABLE, {}, page_size=3, continuation_token=tok)

        ids1 = {r["id"] for r in page1}
        ids2 = {r["id"] for r in page2}
        assert ids1.isdisjoint(ids2)


# ────────────────────────────────────────────────────────────────────────────
# UPDATE
# ────────────────────────────────────────────────────────────────────────────

class TestUpdate:
    def test_update_by_id(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh("before", 1))
        updated = pg_sql.update(TABLE, row["id"], {"name": "after", "value": 99})
        assert updated["name"] == "after"
        assert updated["value"] == 99

    def test_update_by_dict_key(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh("lookup", 5))
        updated = pg_sql.update(TABLE, {"id": row["id"]}, {"value": 50})
        assert updated["value"] == 50

    def test_update_jsonb_field(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh(meta={"k": "v1"}))
        updated = pg_sql.update(TABLE, row["id"], {"meta": {"k": "v2", "new": True}})
        assert updated["meta"]["k"] == "v2"
        assert updated["meta"]["new"] is True

    def test_update_nonexistent_raises(self, pg_sql):
        
        with pytest.raises(DatabaseError):
            pg_sql.update(TABLE, "nonexistent-id", {"name": "x"})

    def test_update_null_field(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh(deleted_at="2024-01-01T00:00:00"))
        updated = pg_sql.update(TABLE, row["id"], {"deleted_at": None})
        assert updated["deleted_at"] is None


# ────────────────────────────────────────────────────────────────────────────
# UPSERT
# ────────────────────────────────────────────────────────────────────────────

class TestUpsert:
    def test_upsert_insert_new(self, pg_sql):
        data = fresh("upsert-new", 10)
        result = pg_sql.upsert(TABLE, data)
        assert result["id"] == data["id"]
        assert result["name"] == "upsert-new"

    def test_upsert_update_existing(self, pg_sql):
        data = fresh("upsert-existing", 10)
        pg_sql.insert(TABLE, data)
        data["value"] = 999
        result = pg_sql.upsert(TABLE, data)
        assert result["value"] == 999

    def test_upsert_idempotent(self, pg_sql):
        data = fresh("idem", 1)
        pg_sql.upsert(TABLE, data)
        pg_sql.upsert(TABLE, data)
        rows = pg_sql.select(TABLE, {"id": data["id"]})
        assert len(rows) == 1


# ────────────────────────────────────────────────────────────────────────────
# DELETE
# ────────────────────────────────────────────────────────────────────────────

class TestDelete:
    def test_delete_by_id(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh())
        deleted = pg_sql.delete(TABLE, row["id"])
        assert deleted["id"] == row["id"]
        remaining = pg_sql.select(TABLE, {"id": row["id"]})
        assert remaining == []

    def test_delete_by_dict_key(self, pg_sql):
        row = pg_sql.insert(TABLE, fresh())
        pg_sql.delete(TABLE, {"id": row["id"]})
        assert pg_sql.select(TABLE, {"id": row["id"]}) == []

    def test_delete_nonexistent_raises(self, pg_sql):
        
        with pytest.raises(DatabaseError):
            pg_sql.delete(TABLE, "ghost-row")


# ────────────────────────────────────────────────────────────────────────────
# LINQ
# ────────────────────────────────────────────────────────────────────────────

class TestLinq:
    def _seed(self, pg_sql, n: int = 5) -> list[dict]:
        rows = []
        for i in range(n):
            rows.append(pg_sql.insert(TABLE, fresh(f"linq-{i}", i * 10)))
        return rows

    def test_linq_where_eq(self, pg_sql):
        
        self._seed(pg_sql)
        pg_sql.insert(TABLE, fresh("target", 777))
        qb = QueryBuilder().where("name", Operator.EQ, "target")
        results = pg_sql.query_linq(TABLE, qb)
        assert len(results) == 1
        assert results[0]["value"] == 777

    def test_linq_where_gt(self, pg_sql):
        
        self._seed(pg_sql, 5)                # values 0,10,20,30,40
        qb = QueryBuilder().where("value", Operator.GT, 20)
        results = pg_sql.query_linq(TABLE, qb)
        assert all(r["value"] > 20 for r in results)
        assert len(results) == 2             # 30, 40

    def test_linq_order_by_asc(self, pg_sql):
        
        self._seed(pg_sql, 4)
        qb = QueryBuilder().order_by("value", descending=False)
        results = pg_sql.query_linq(TABLE, qb)
        values = [r["value"] for r in results]
        assert values == sorted(values)

    def test_linq_order_by_desc(self, pg_sql):
        
        self._seed(pg_sql, 4)
        qb = QueryBuilder().order_by("value", descending=True)
        results = pg_sql.query_linq(TABLE, qb)
        values = [r["value"] for r in results]
        assert values == sorted(values, reverse=True)

    def test_linq_take(self, pg_sql):
        
        self._seed(pg_sql, 6)
        qb = QueryBuilder().take(3)
        results = pg_sql.query_linq(TABLE, qb)
        assert len(results) == 3

    def test_linq_skip(self, pg_sql):
        
        self._seed(pg_sql, 5)
        all_rows = pg_sql.select(TABLE)
        qb = QueryBuilder().skip(2)
        results = pg_sql.query_linq(TABLE, qb)
        assert len(results) == len(all_rows) - 2

    def test_linq_count(self, pg_sql):
        
        self._seed(pg_sql, 4)
        qb = QueryBuilder().count()
        total = pg_sql.query_linq(TABLE, qb)
        assert isinstance(total, int)
        assert total == 4

    def test_linq_distinct(self, pg_sql):
        
        # Insert duplicates on the 'name' column
        for _ in range(3):
            pg_sql.insert(TABLE, fresh("dup-name", 1))
        qb = QueryBuilder().select_fields(["name"]).distinct() # type: ignore
        results = pg_sql.query_linq(TABLE, qb)
        names = [r["name"] for r in results]
        assert len(names) == len(set(names))

    def test_linq_where_like(self, pg_sql):
        
        pg_sql.insert(TABLE, fresh("hello-world", 1))
        pg_sql.insert(TABLE, fresh("goodbye", 2))
        qb = QueryBuilder().where("name", Operator.IN, "%hello%")
        results = pg_sql.query_linq(TABLE, qb)
        assert all("hello" in r["name"] for r in results)


# ────────────────────────────────────────────────────────────────────────────
# EXECUTE RAW SQL
# ────────────────────────────────────────────────────────────────────────────

class TestExecute:
    def test_execute_ddl(self, pg_sql):
        # Should not raise
        pg_sql.execute(
            "CREATE TABLE IF NOT EXISTS _polydb_exec_test (id TEXT PRIMARY KEY)"
        )
        pg_sql.execute("DROP TABLE IF EXISTS _polydb_exec_test")

    def test_execute_fetch(self, pg_sql):
        pg_sql.insert(TABLE, fresh("exec-fetch", 55))
        rows = pg_sql.execute(
            f"SELECT * FROM {TABLE} WHERE name = %s", ["exec-fetch"], fetch=True
        )
        assert len(rows) == 1
        assert rows[0]["value"] == 55

    def test_execute_fetch_one(self, pg_sql):
        pg_sql.insert(TABLE, fresh("exec-one", 66))
        row = pg_sql.execute(
            f"SELECT * FROM {TABLE} WHERE name = %s", ["exec-one"], fetch_one=True
        )
        assert row is not None
        assert row["value"] == 66

    def test_execute_no_fetch_returns_none(self, pg_sql):
        result = pg_sql.execute(
            f"INSERT INTO {TABLE} (id, name, value) VALUES (%s, %s, %s)",
            [uid(), "exec-none", 0],
        )
        assert result is None

    def test_execute_fetch_one_missing_returns_none(self, pg_sql):
        row = pg_sql.execute(
            f"SELECT * FROM {TABLE} WHERE id = %s", ["definitely-missing"], fetch_one=True
        )
        assert row is None


# ────────────────────────────────────────────────────────────────────────────
# TRANSACTIONS
# ────────────────────────────────────────────────────────────────────────────

class TestTransactions:
    def test_commit(self, pg_sql):
        tx = pg_sql.begin_transaction()
        data = fresh("tx-commit", 1)
        pg_sql.insert(TABLE, data, tx=tx)
        pg_sql.commit(tx)

        rows = pg_sql.select(TABLE, {"id": data["id"]})
        assert len(rows) == 1

    def test_rollback(self, pg_sql):
        tx = pg_sql.begin_transaction()
        data = fresh("tx-rollback", 2)
        pg_sql.insert(TABLE, data, tx=tx)
        pg_sql.rollback(tx)

        rows = pg_sql.select(TABLE, {"id": data["id"]})
        assert rows == []

    def test_multiple_ops_in_transaction(self, pg_sql):
        tx = pg_sql.begin_transaction()
        r1 = pg_sql.insert(TABLE, fresh("txm-1", 1), tx=tx)
        r2 = pg_sql.insert(TABLE, fresh("txm-2", 2), tx=tx)
        pg_sql.update(TABLE, r1["id"], {"value": 100}, tx=tx)
        pg_sql.commit(tx)

        final1 = pg_sql.select(TABLE, {"id": r1["id"]})[0]
        final2 = pg_sql.select(TABLE, {"id": r2["id"]})[0]
        assert final1["value"] == 100
        assert final2["value"] == 2

    def test_select_within_transaction_sees_own_writes(self, pg_sql):
        tx = pg_sql.begin_transaction()
        data = fresh("tx-vis", 99)
        pg_sql.insert(TABLE, data, tx=tx)
        rows = pg_sql.select(TABLE, {"id": data["id"]}, tx=tx)
        assert len(rows) == 1          # visible within same tx
        pg_sql.rollback(tx)


# ────────────────────────────────────────────────────────────────────────────
# DISTRIBUTED LOCK
# ────────────────────────────────────────────────────────────────────────────

class TestDistributedLock:
    def test_lock_acquired_and_released(self, pg_sql):
        executed = []
        with pg_sql.distributed_lock("test-lock-basic"):
            executed.append(1)
        assert executed == [1]

    def test_lock_is_exclusive(self, pg_sql):
        """
        Two threads contend for the same lock.
        The second must wait until the first releases.
        Ordering of critical sections must be strict.
        """
        log: list[str] = []
        errors: list[Exception] = []

        def worker(tag: str, delay: float) -> None:
            try:
                with pg_sql.distributed_lock("test-exclusive-lock"):
                    log.append(f"{tag}-enter")
                    time.sleep(delay)
                    log.append(f"{tag}-exit")
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=("T1", 0.2))
        t2 = threading.Thread(target=worker, args=("T2", 0.05))
        t1.start()
        time.sleep(0.05)   # give T1 time to grab lock
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert not errors, errors
        # The enter/exit pairs must not interleave
        t1_enter = log.index("T1-enter")
        t1_exit  = log.index("T1-exit")
        t2_enter = log.index("T2-enter")
        assert t1_exit < t2_enter, f"Interleaved: {log}"

    def test_lock_released_on_exception(self, pg_sql):
        

        try:
            with pg_sql.distributed_lock("test-exc-lock"):
                raise ValueError("boom")
        except ValueError:
            pass

        # Must be able to acquire again immediately
        acquired = False
        with pg_sql.distributed_lock("test-exc-lock"):
            acquired = True
        assert acquired


# ────────────────────────────────────────────────────────────────────────────
# CONNECTION POOL concurrency
# ────────────────────────────────────────────────────────────────────────────

class TestConnectionPool:
    @pytest.mark.slow
    def test_concurrent_inserts(self, pg_sql):
        errors: list[Exception] = []
        results: list[dict] = []
        lock = threading.Lock()

        def insert_one() -> None:
            try:
                row = pg_sql.insert(TABLE, fresh(f"concurrent-{uid()}"))
                with lock:
                    results.append(row)
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=insert_one) for _ in range(15)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, errors
        assert len(results) == 15

    @pytest.mark.slow
    def test_pool_exhaustion_recovers(self, pg_sql):
        """Hold pool connections briefly then release; subsequent ops must succeed."""
        def hold_conn() -> None:
            tx = pg_sql.begin_transaction()
            time.sleep(0.3)
            pg_sql.rollback(tx)

        threads = [threading.Thread(target=hold_conn) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # Must still work
        row = pg_sql.insert(TABLE, fresh("after-pool-stress"))
        assert row["name"] == "after-pool-stress"