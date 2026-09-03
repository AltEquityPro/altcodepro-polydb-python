"""
tests/test_session_vars.py
===========================
Integration tests for the generic Postgres `session_vars` mechanism:
PostgreSQLAdapter.insert/select/update/delete/execute's `session_vars`
keyword-only parameter, and DatabaseFactory.create/read/read_one/update/
delete threading it through to the adapter.

This is polydb's own plumbing proof, not an RLS test -- polydb stays
tenant-unaware on purpose (see databaseFactory.py's module docstring), so
there is no RLS policy anywhere in this file, and `app.tenant_id` here is
just the one allow-listed session-var *name*, never interpreted as
meaning "tenant" by any code in this file. What's proven here:

  - `SET LOCAL`-equivalent scoping (via set_config(..., is_local=True))
    genuinely applies only inside the one transaction it was set for, and
    reverts automatically at COMMIT -- queried directly via
    current_setting(), not assumed from the SQL standard.
  - A pooled connection carries NO leaked session var into its next,
    unrelated checkout -- the exact "naive implementation" risk
    universal_engine's own README names as the reason this was deferred
    until this generic mechanism existed. Proven deterministically with a
    size-1 connection pool, not just "probably the same connection".
  - The allow-list rejects a session variable name that isn't
    `app.tenant_id` -- this parameter is keyword-only, code-controlled,
    never reachable from request input, but the allow-list is a second,
    independent guard against a future caller (or a bug) asking this
    adapter to SET an arbitrary Postgres GUC.
  - A caller-supplied transaction (tx=...) still honors session_vars --
    the documented design decision that this is NOT a no-op just because
    the caller, not this method, owns the connection.
  - DatabaseFactory.create/read/read_one/update/delete all actually pass
    session_vars through to the adapter, not just accept and drop it.
"""

from __future__ import annotations

import pytest

from conftest import uid
from polydb.adapters.PostgreSQLAdapter import ALLOWED_SESSION_VARS, PostgreSQLAdapter
from polydb.databaseFactory import DatabaseFactory
from polydb.errors import DatabaseError

pytestmark = pytest.mark.postgresql

TABLE = "polydb_items"


def fresh(**extra) -> dict:
    return {"id": uid(), "name": "session-vars-test", "value": 1, **extra}


class SqlModel:
    __polydb__ = {"storage": "sql", "table": TABLE}


@pytest.fixture(autouse=True)
def clean_table(pg_sql, pg_schema):
    pg_sql.execute(f"DELETE FROM {TABLE}")
    yield


def _current_setting(pg_sql, *, tx=None) -> str | None:
    row = pg_sql.execute(
        "SELECT current_setting('app.tenant_id', true) AS v", tx=tx, fetch_one=True
    )
    val = row["v"]
    return val or None  # Postgres returns '' for an unset custom GUC via `true`, not NULL


# ────────────────────────────────────────────────────────────────────────────
# Allow-list
# ────────────────────────────────────────────────────────────────────────────


class TestAllowList:
    def test_allow_list_contains_exactly_app_tenant_id(self):
        # Locks the current, deliberately narrow default -- a future addition
        # should be a conscious edit to PostgreSQLAdapter.py, not something
        # that silently grew via an unrelated change.
        assert ALLOWED_SESSION_VARS == frozenset({"app.tenant_id"})

    def test_disallowed_session_var_name_is_rejected(self, pg_sql):
        with pytest.raises(DatabaseError, match="not in the allow-list"):
            pg_sql.select(TABLE, {}, session_vars={"statement_timeout": "1"})

    def test_disallowed_name_never_reaches_postgres(self, pg_sql):
        # Rejected before any SET/set_config runs -- not merely rejected by
        # Postgres after the fact. A real row is never touched.
        with pytest.raises(DatabaseError):
            pg_sql.insert(TABLE, fresh(), session_vars={"role": "postgres"})
        assert pg_sql.select(TABLE, {}) == []

    def test_none_and_empty_session_vars_are_a_no_op(self, pg_sql):
        # Every existing caller passes nothing at all -- confirm the
        # parameter is genuinely optional, not just defaulted.
        row = pg_sql.insert(TABLE, fresh(), session_vars=None)
        assert row["id"]
        row2 = pg_sql.insert(TABLE, fresh(), session_vars={})
        assert row2["id"]


# ────────────────────────────────────────────────────────────────────────────
# Scoping: one transaction only, reverts at commit
# ────────────────────────────────────────────────────────────────────────────


class TestTransactionScoping:
    def test_session_var_visible_inside_the_same_caller_owned_transaction(self, pg_sql):
        tx = pg_sql.begin_transaction()
        try:
            pg_sql.select(TABLE, {}, tx=tx, session_vars={"app.tenant_id": "tenant-in-tx"})
            assert _current_setting(pg_sql, tx=tx) == "tenant-in-tx"
        finally:
            pg_sql.rollback(tx)

    def test_session_var_reverts_after_commit_own_connection_path(self, pg_sql):
        # own_conn=True path (no tx supplied): insert() acquires its own
        # pooled connection, sets the var, runs, commits, returns the
        # connection to the pool. A fresh call afterward must see it unset.
        pg_sql.insert(TABLE, fresh(), session_vars={"app.tenant_id": "tenant-own-conn"})
        assert _current_setting(pg_sql) is None

    def test_caller_supplied_tx_still_honors_session_vars(self, pg_sql):
        """Design decision: session_vars applies whether or not the caller
        owns the transaction. A caller managing its own tx (universal_engine's
        idempotency.py reserve()/batches.py cancel()/pause()/resume(), which
        run several raw-SQL statements against one already-open tx) needs
        'set this Postgres session var for this transaction' to mean
        exactly that -- making it a no-op when tx is supplied would
        silently break precisely those multi-statement-same-tx callers,
        which is why this is NOT the caller's own responsibility to issue
        separately."""
        tx = pg_sql.begin_transaction()
        try:
            row = pg_sql.insert(
                TABLE, fresh(), tx=tx, session_vars={"app.tenant_id": "tenant-caller-tx"}
            )
            # A second statement on the SAME tx, no session_vars passed this
            # time -- still sees the value set by the first call, because it
            # was scoped to the transaction, not to that one call.
            assert _current_setting(pg_sql, tx=tx) == "tenant-caller-tx"
            pg_sql.commit(tx)
        except Exception:
            pg_sql.rollback(tx)
            raise
        # Reverted now that the caller's own transaction has committed.
        assert _current_setting(pg_sql) is None
        assert pg_sql.select(TABLE, {"id": row["id"]}) != []

    def test_execute_raw_sql_honors_session_vars_within_its_own_transaction(self, pg_sql):
        tx = pg_sql.begin_transaction()
        try:
            pg_sql.execute(
                "SELECT 1", tx=tx, session_vars={"app.tenant_id": "tenant-execute"}
            )
            val = pg_sql.execute(
                "SELECT current_setting('app.tenant_id', true) AS v", tx=tx, fetch_one=True
            )["v"]
            assert val == "tenant-execute"
        finally:
            pg_sql.rollback(tx)
        assert _current_setting(pg_sql) is None


# ────────────────────────────────────────────────────────────────────────────
# The load-bearing proof: no leak across a pooled connection's reuse
# ────────────────────────────────────────────────────────────────────────────


class TestPooledConnectionDoesNotLeak:
    def test_session_var_does_not_survive_into_the_next_checkout(self, pg_sql, monkeypatch):
        """The exact risk README's own RLS paragraph names as the reason a
        naive implementation is dangerous: a pooled connection reusing a
        leaked session variable across an unrelated *later* query would let
        one caller's SET LOCAL app.tenant_id silently apply to a completely
        different caller's next, unrelated query on the same physical
        connection. Proven deterministically with a size-1 pool (so
        checkout #2 is *guaranteed* to be the identical physical connection
        checkout #1 used), not merely "probably the same connection" from
        pool reuse odds.
        """
        monkeypatch.setenv("POSTGRES_MIN_CONNECTIONS", "1")
        monkeypatch.setenv("POSTGRES_MAX_CONNECTIONS", "1")
        adapter = PostgreSQLAdapter(connection_string=pg_sql.connection_string)
        try:
            conn1 = adapter._get_connection()
            adapter._apply_session_vars(conn1, {"app.tenant_id": "leaky-tenant"})
            with conn1.cursor() as cur:
                cur.execute("SELECT current_setting('app.tenant_id', true) AS v")
                assert cur.fetchone()[0] == "leaky-tenant"
            conn1.commit()  # ends the transaction the SET LOCAL was scoped to
            adapter._return_connection(conn1)

            conn2 = adapter._get_connection()
            assert conn2 is conn1, "pool size 1 must hand back the identical connection"
            with conn2.cursor() as cur:
                cur.execute("SELECT current_setting('app.tenant_id', true) AS v")
                leaked = cur.fetchone()[0]
            conn2.commit()
            adapter._return_connection(conn2)

            assert not leaked, (
                f"session var leaked across pooled-connection reuse: {leaked!r}"
            )
        finally:
            adapter.reset_pool()

    def test_session_var_does_not_leak_through_high_level_calls_either(self, pg_sql, monkeypatch):
        """Same proof through the public insert()/select() API instead of the
        private connection accessors -- a caller using this adapter the
        normal way (own_conn=True on every call, no tx) never sees another
        caller's session_vars either."""
        monkeypatch.setenv("POSTGRES_MIN_CONNECTIONS", "1")
        monkeypatch.setenv("POSTGRES_MAX_CONNECTIONS", "1")
        adapter = PostgreSQLAdapter(connection_string=pg_sql.connection_string)
        try:
            adapter.insert(TABLE, fresh(), session_vars={"app.tenant_id": "tenant-a"})
            row = adapter.execute(
                "SELECT current_setting('app.tenant_id', true) AS v", fetch_one=True
            )
            assert not row["v"], f"tenant-a's session var leaked into an unrelated call: {row['v']!r}"
        finally:
            adapter.reset_pool()


# ────────────────────────────────────────────────────────────────────────────
# DatabaseFactory threads session_vars through to the adapter
# ────────────────────────────────────────────────────────────────────────────


class TestDatabaseFactoryThreadsSessionVars:
    def test_create_read_read_one_update_delete_all_forward_session_vars(
        self, pg_factory, monkeypatch
    ):
        db = DatabaseFactory(cloud_factory=pg_factory, enable_audit=False, enable_cache=False)

        recorded: list[tuple[str, dict | None]] = []
        original_insert = PostgreSQLAdapter.insert
        original_select = PostgreSQLAdapter.select
        original_update = PostgreSQLAdapter.update
        original_delete = PostgreSQLAdapter.delete

        def spy_insert(self, table, data, tx=None, *, session_vars=None):
            recorded.append(("insert", session_vars))
            return original_insert(self, table, data, tx, session_vars=session_vars)

        def spy_select(self, table, query=None, limit=None, offset=None, tx=None, *, session_vars=None):
            recorded.append(("select", session_vars))
            return original_select(
                self, table, query, limit=limit, offset=offset, tx=tx, session_vars=session_vars
            )

        def spy_update(self, table, entity_id, data, tx=None, *, session_vars=None):
            recorded.append(("update", session_vars))
            return original_update(self, table, entity_id, data, tx, session_vars=session_vars)

        def spy_delete(self, table, entity_id, tx=None, *, session_vars=None):
            recorded.append(("delete", session_vars))
            return original_delete(self, table, entity_id, tx, session_vars=session_vars)

        monkeypatch.setattr(PostgreSQLAdapter, "insert", spy_insert)
        monkeypatch.setattr(PostgreSQLAdapter, "select", spy_select)
        monkeypatch.setattr(PostgreSQLAdapter, "update", spy_update)
        monkeypatch.setattr(PostgreSQLAdapter, "delete", spy_delete)

        sv = {"app.tenant_id": "tenant-plumbing"}
        created = db.create(SqlModel, fresh(), session_vars=sv)
        db.read(SqlModel, {"id": created["id"]}, session_vars=sv)
        db.read_one(SqlModel, {"id": created["id"]}, session_vars=sv)
        db.update(SqlModel, created["id"], {"value": 2}, session_vars=sv)
        db.delete(SqlModel, created["id"], session_vars=sv)

        kinds = [kind for kind, _ in recorded]
        assert "insert" in kinds
        assert "select" in kinds  # both read() and read_one() go through select()
        assert "update" in kinds
        assert "delete" in kinds
        assert all(sv_seen == sv for _, sv_seen in recorded), recorded

    def test_omitted_session_vars_is_byte_for_byte_unaffected(self, pg_factory):
        """Every existing caller/test calls create/read/read_one/update/
        delete with no session_vars at all -- confirm that path is
        completely unaffected (None flows through, no SET/set_config ever
        runs, per _apply_session_vars' own early return)."""
        db = DatabaseFactory(cloud_factory=pg_factory, enable_audit=False, enable_cache=False)
        created = db.create(SqlModel, fresh())
        found = db.read_one(SqlModel, {"id": created["id"]})
        assert found["id"] == created["id"]
        db.update(SqlModel, created["id"], {"value": 5})
        again = db.read_one(SqlModel, {"id": created["id"]})
        assert again["value"] == 5
        db.delete(SqlModel, created["id"])
        assert db.read_one(SqlModel, {"id": created["id"]}) is None
