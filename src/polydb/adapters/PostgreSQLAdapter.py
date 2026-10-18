# src/polydb/adapters/postgres.py
import base64
import hashlib
import json
import os
import threading
import time
import weakref
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import psycopg
import psycopg.pq
import psycopg_pool
from psycopg import sql as pg_sql
from psycopg.types.json import Jsonb as Json

from ..errors import ConnectionError, DatabaseError, InsufficientBalanceError
from ..query import Operator, QueryBuilder
from ..read_batch import SelectSpec, current_batch
from ..retry import retry
from ..types import JsonDict, Lookup
from ..utils import validate_column_name, validate_table_name

# Postgres session variables this adapter is willing to SET (scoped to one
# transaction, via set_config(..., is_local=True)) on a caller's behalf.
# `session_vars` is a keyword-only parameter no request body/query string
# ever reaches directly -- every caller is trusted engine code, never
# attacker input -- but this allow-list is a second, independent guard
# against a future caller (or a bug in one) asking this adapter to SET an
# arbitrary Postgres GUC (search_path, role, statement_timeout, ...) it was
# never built to accept. polydb stays tenant-unaware on purpose (see
# databaseFactory.py's own module docstring) -- this mechanism is generic
# ("set these session vars for one transaction"), not tenant-specific; the
# one entry below is simply the one real caller (universal_engine's RLS
# policies) needs today. Add a name here only when a real caller needs it.
ALLOWED_SESSION_VARS = frozenset({"app.tenant_id"})



class PoolError(Exception):
    """Raised when no connection frees up within the acquire timeout."""


def _close_pool_quietly(pool) -> None:
    # A pool that is dropped without closeall() (a test, a process shutting down) must not raise from __del__ -- psycopg_pool's own
    # finalizer does, because its worker threads outlive the object.
    try:
        pool.close(timeout=1.0)
    except Exception:
        pass


class LazyPool:
    """A small, lazy, bounded connection pool on psycopg 3 (`psycopg_pool.ConnectionPool`).

    Why not the old eager pool: every process used to open `POSTGRES_MIN_CONNECTIONS` connections the moment it started and keep
    them for its whole life. With a handful of processes (api plus one worker per lane) and a database that only allows a few dozen
    clients, the idle reservations alone filled the server ("remaining connection slots are reserved ...") and a new revision or
    worker could not even start.

    This pool opens connections ONLY when a request needs one, keeps at most `keep` (default 1) idle for reuse, and closes every
    other idle connection after `max_idle` seconds. `max_size` is a hard ceiling per process. A request that finds every connection
    in use waits up to `acquire_timeout` seconds instead of failing at once."""

    def __init__(self, dsn: str, *, max_size: int, keep: int = 1, acquire_timeout: float = 10.0,
                 max_idle: float = 30.0, max_lifetime: float = 1800.0):
        self.maxconn = max_size
        self._timeout = float(acquire_timeout)
        keep = max(0, min(keep, max_size))
        self._pool = psycopg_pool.ConnectionPool(
            conninfo=dsn,
            min_size=keep,
            max_size=max_size,
            timeout=self._timeout,
            max_idle=max_idle,
            max_lifetime=max_lifetime,
            reconnect_timeout=30.0,
            # prepare_threshold=None: no server-side prepared statements (they pin per-connection state and break behind a
            # transaction-mode pooler); autocommit off keeps the engine's explicit commit/rollback contract.
            kwargs={"autocommit": False, "prepare_threshold": None},
            open=False,
            name="polydb-pg",
        )
        self._pool.open()
        weakref.finalize(self, _close_pool_quietly, self._pool)

    def getconn(self, key=None):
        try:
            return self._pool.getconn(timeout=self._timeout)
        except psycopg_pool.PoolTimeout as e:
            raise PoolError(
                f"connection pool exhausted: waited {self._timeout:g}s for one of {self.maxconn} connections"
            ) from e

    def putconn(self, conn=None, key=None, close=False):
        if conn is None:
            return
        if close:
            try:
                conn.close()
            except Exception:
                pass
        self._pool.putconn(conn)

    def closeall(self):
        self._pool.close()

    def used_count(self) -> int:
        stats = self._pool.get_stats()
        return max(0, int(stats.get("pool_size", 0)) - int(stats.get("pool_available", 0)))


class PostgreSQLAdapter:
    """PostgreSQL with full LINQ support, connection pooling, JSON/JSONB support"""

    def __init__(self, connection_string: Optional[str] = None):
        from ..utils import setup_logger

        self.logger = setup_logger(__name__)
        self._slow_query_ms: float = float(os.getenv("POLYDB_SLOW_QUERY_MS", "1000"))
        self.connection_string = connection_string or os.getenv(
            "POSTGRES_CONNECTION_STRING",
            os.getenv("POSTGRES_URL", ""),
        )
        if not self.connection_string:
            raise ConnectionError("POSTGRES_CONNECTION_STRING or POSTGRES_URL must be set")
        self._pool = None
        self._lock = threading.Lock()
        self._initialize_pool()

    # ---------------------------------------------------------------------
    # CONNECTION HYGIENE HELPERS
    # ---------------------------------------------------------------------

    def _is_idle(self, conn) -> bool:
        try:
            return conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        except Exception:
            return False

    def _drain_transaction(self, conn) -> None:
        if self._is_idle(conn):
            return
        try:
            conn.rollback()
        except Exception:
            pass

    def _ping_connection(self, conn) -> bool:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            try:
                conn.rollback()
            except Exception:
                pass
            return True
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False

    def _initialize_pool(self):
        try:
            from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

            dsn = self.connection_string
            if "postgresql://" in dsn:
                parsed = urlparse(dsn)
                query = parse_qs(parsed.query)
                query.setdefault("connect_timeout", ["30"])
                query.setdefault("keepalives", ["1"])
                query.setdefault("keepalives_idle", ["30"])
                query.setdefault("keepalives_interval", ["10"])
                query.setdefault("keepalives_count", ["5"])
                # connect_timeout only bounds the initial handshake; keepalives
                # only detect a dead *idle* connection. Neither bounds a query
                # that's already in flight and stalls mid-response (packet
                # loss / a silently-dropped connection somewhere on the path
                # to a remote DB) -- that shows up as libpq's own "could not
                # receive data from server: Operation timed out / SSL SYSCALL
                # error", and without a bound the client can block on that
                # recv() for a very long time (Python can't interrupt a
                # blocking C-level syscall, so even Ctrl+C doesn't land until
                # it returns). tcp_user_timeout bounds unacknowledged data at
                # the OS/TCP level; statement_timeout bounds how long Postgres
                # itself will run a query before cancelling it server-side.
                query.setdefault(
                    "tcp_user_timeout",
                    [os.getenv("POSTGRES_TCP_USER_TIMEOUT_MS", "20000")],
                )
                query.setdefault(
                    "options",
                    [f"-c statement_timeout={os.getenv('POSTGRES_STATEMENT_TIMEOUT_MS', '20000')}"],
                )

                # quote_via=quote, not the default quote_plus: libpq's URI
                # parser percent-decodes query values but treats "+" as a
                # literal plus, so form-encoding the space in
                # "-c statement_timeout=..." makes the server see the option
                # name as "+statement_timeout" and reject the connection.
                new_query = urlencode(query, doseq=True, quote_via=quote)
                parsed = parsed._replace(query=new_query)
                dsn = urlunparse(parsed)

            _maxconn = int(os.getenv("POSTGRES_MAX_CONNECTIONS", "10"))
            with self._lock:
                if not self._pool:
                    # One probe connection up front so a bad DSN / full server fails here, loudly, exactly as the old eager pool
                    # did -- and is closed again at once (the pool itself opens connections only on demand).
                    with psycopg.connect(dsn, connect_timeout=int(os.getenv("POSTGRES_CONNECT_TIMEOUT_S", "30"))) as probe:
                        probe.execute("SELECT 1")
                    self._pool = LazyPool(
                        dsn,
                        max_size=_maxconn,
                        keep=int(os.getenv("POSTGRES_POOL_IDLE_KEEP", "1")),
                        acquire_timeout=float(os.getenv("POSTGRES_POOL_ACQUIRE_TIMEOUT_S", "10")),
                        max_idle=float(os.getenv("POSTGRES_POOL_MAX_IDLE_S", "30")),
                    )
                    self.logger.info(
                        "PostgreSQL pool initialized (lazy): idle-keep=%s max=%s",
                        os.getenv("POSTGRES_POOL_IDLE_KEEP", "1"),
                        _maxconn,
                    )
        except Exception as e:
            raise ConnectionError(f"Failed to initialize PostgreSQL pool: {str(e)}")

    def _log_pool_utilization(self) -> None:
        if not self._pool:
            return
        try:
            maxconn = self._pool.maxconn
            used_count = self._pool.used_count()
            pct = (used_count / maxconn * 100) if maxconn else 0
            if pct > 80:
                self.logger.warning(
                    "PostgreSQL pool utilization: %d/%d connections in use (%.0f%%)",
                    used_count,
                    maxconn,
                    pct,
                )
        except Exception:
            pass

    def _get_connection(self) -> Any:
        if not self._pool:
            self._initialize_pool()

        try:
            conn = self._pool.getconn()  # type: ignore

            if conn.closed:
                self.logger.warning("Closed connection detected from pool, closing and retrying")
                self._pool.putconn(conn, close=True)  # type: ignore
                conn = self._pool.getconn()  # type: ignore
                self._drain_transaction(conn)

            self._log_pool_utilization()
            return conn

        except Exception as e:
            self.logger.error(f"Failed to acquire connection from pool: {e}")
            raise ConnectionError(f"Could not obtain database connection: {e}") from e

    def _return_connection(self, conn: Any):
        if self._pool and conn:
            self._drain_transaction(conn)
            self._pool.putconn(conn)

    # ---------------------------------------------------------------------
    # QUERY TIMING HELPER
    # ---------------------------------------------------------------------

    def _timed_execute(
        self,
        cursor: Any,
        sql: str,
        params: Any,
        *,
        operation: str = "execute",
        table: str = "",
    ) -> float:
        t0 = time.perf_counter()
        # psycopg 3 parses `%` as a placeholder whenever a params sequence is given, even an empty one (psycopg2 did not):
        # no parameters means None, so raw SQL / DDL containing a literal `%` or several statements still runs.
        cursor.execute(sql, params if params else None)
        duration_ms = (time.perf_counter() - t0) * 1000.0

        self.logger.debug(
            "SQL executed",
            extra={"operation": operation, "table": table, "duration_ms": round(duration_ms, 3)},
        )

        if duration_ms > self._slow_query_ms:
            self.logger.warning(
                "Slow query detected: operation=%s table=%s duration_ms=%.1f threshold=%.1f",
                operation,
                table,
                duration_ms,
                self._slow_query_ms,
            )

        return duration_ms

    # ---------------------------------------------------------------------
    # TRANSACTIONS
    # ---------------------------------------------------------------------

    def reset_pool(self):
        # `self._lock` is a plain threading.Lock (not reentrant) --
        # _initialize_pool() below acquires it itself, so it must NOT
        # still be held when we call it, or this self-deadlocks (found by
        # actually calling reset_pool() for the first time anywhere in
        # this codebase, in tests/test_session_vars.py's pooled-connection
        # leak test -- no prior caller ever exercised this method). The
        # tiny window here where self._pool is briefly None before
        # _initialize_pool() re-acquires the lock and sets it back is
        # safe: that method's own `if not self._pool` check under its own
        # lock is exactly what already protects concurrent callers of it
        # (e.g. _get_connection()'s own lazy re-init), so it's not a new
        # exposure.
        with self._lock:
            if self._pool:
                try:
                    self._pool.closeall()
                except Exception:
                    pass
                self._pool = None
        self._initialize_pool()

    def begin_transaction(self) -> Any:
        conn = self._get_connection()
        self._drain_transaction(conn)
        conn.autocommit = False
        return conn

    def _apply_session_vars(self, conn: Any, session_vars: Optional[Dict[str, str]]) -> None:
        """SET LOCAL-equivalent for a fixed, allow-listed set of Postgres
        session variables, scoped to whatever transaction `conn` is
        currently in -- reverts automatically at COMMIT/ROLLBACK, never
        outlives the transaction it was set for. This is the fix for the
        gap README documents: `SET LOCAL <var> = <value>` only means
        anything on the exact connection/transaction the real query after
        it runs on, so it has to be issued here, immediately before the
        query, on the same `conn` -- never as a separate call the caller
        could race against a pool checkin/checkout.

        Uses `set_config(name, value, is_local)` rather than a literal
        `SET LOCAL {key} = %s` string: `SET` itself has no bind-parameter
        syntax in Postgres (the value has no placeholder form), but
        `set_config` is an ordinary SQL function, so both the name and the
        value are real query parameters -- nothing about either is ever
        f-string'd into the SQL text. `is_local=True` is what gives this
        `SET LOCAL`'s exact semantics (reverts at the end of the current
        transaction) rather than `SET`'s session-lifetime one -- the
        session-lifetime form is exactly the pooled-connection leak this
        whole mechanism exists to avoid (see tests/test_session_vars.py's
        leak-across-checkout proof).
        """
        if not session_vars:
            return
        with conn.cursor() as cur:
            for key, value in session_vars.items():
                if key not in ALLOWED_SESSION_VARS:
                    raise DatabaseError(
                        f"session_vars: {key!r} is not in the allow-list of Postgres "
                        f"session variables this adapter will set"
                    )
                cur.execute("SELECT set_config(%s, %s, true)", [key, str(value)])

    def commit(self, tx: Any):
        if tx:
            tx.commit()
            self._return_connection(tx)

    def rollback(self, tx: Any):
        if tx:
            tx.rollback()
            self._return_connection(tx)

    # ---------------------------------------------------------------------
    # JSON HELPERS
    # ---------------------------------------------------------------------

    def _json_safe(self, obj: Any):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return str(obj)
        if isinstance(obj, dict):
            return {k: self._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._json_safe(v) for v in obj]
        return obj

    def _serialize_value(self, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, dict):
            return Json(self._json_safe(v))
        if isinstance(v, (list, tuple)):
            # A bare list/tuple of scalars is a real Postgres array column
            # (e.g. TEXT[]) -- psycopg adapts a plain Python list to that
            # natively, so it must pass through untouched here, exactly
            # like _serialize_param already does for query parameters
            # (below). Only a list/tuple actually containing dicts is
            # genuinely JSON-shaped (no Postgres array type holds JSON
            # objects as elements), so that case alone still gets
            # Json()-wrapped for a JSONB column. Wrapping every list
            # unconditionally (the pre-fix behavior) sent a JSON string
            # literal like '["x","y"]' to any TEXT[] column, which
            # Postgres correctly refuses ("malformed array literal") --
            # this insert/update/upsert path silently could never write
            # a real array column at all.
            seq = list(v)
            if any(isinstance(x, dict) for x in seq):
                return Json(self._json_safe(seq))
            return seq
        if isinstance(v, (datetime, date)):
            return v
        if isinstance(v, Decimal):
            return float(v)
        return v

    def _serialize_param(self, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, dict):
            return Json(self._json_safe(v))
        if isinstance(v, (list, tuple)):
            seq = list(v)
            if any(isinstance(x, dict) for x in seq):
                return Json(self._json_safe(seq))
            return seq
        if isinstance(v, (datetime, date)):
            return v
        if isinstance(v, Decimal):
            return float(v)
        return v

    def _serialize_params(self, params: List[Any]) -> List[Any]:
        return [self._serialize_param(p) for p in params]

    def _deserialize_row(self, row: JsonDict) -> JsonDict:
        """Best-effort convenience decode: any string column value that
        LOOKS like JSON (starts/ends with `{}`/`[]`) is `json.loads`'d,
        regardless of the column's own declared type. This is a real,
        deliberate, LOAD-BEARING convenience a real caller depends on --
        e.g. universal-interprter's `observability.py`'s `Observability.
        query()` writes a `logs.emit` payload as `json.dumps(...)` into a
        plain TEXT column (`payload`, never JSONB -- see that module's
        own comment) and its own docstring explicitly promises "logs.
        query's own `payload` field gets a real dict back"; removing this
        heuristic (briefly tried, then reverted -- see this repo's own
        2.5.16/2.5.17 CLAUDE.md history) silently broke that real,
        already-shipped contract. `query_log.py`'s own `fields` column is
        the identical shape, just defended a second way: it independently
        `json.loads`'s its own value with an `isinstance(..., str)` guard
        rather than relying on this method at all -- proof this repo has
        real callers on both sides (some lean on this adapter-level
        decode, some defend for themselves).

        **The real, honest trade-off, unavoidable at this layer**: a
        genuinely non-JSON TEXT/VARCHAR string whose own real content
        merely LOOKS JSON-shaped (e.g. a prompt template's own `content`
        column holding a literal JSON-encoded email template -- see
        universal-interprter's `prompt_template_store.py`) gets coerced
        from `str` to `dict`/`list` here too, since this method has no
        way to distinguish "real app data serialized as JSON text" from
        "a legitimately JSON-shaped string that must stay a string" --
        both are simply "a string that starts with `{` and ends with
        `}`." This adapter deliberately does NOT try to solve that
        ambiguity generically (declaring the column JSONB instead sidesteps
        it entirely -- psycopg registers JSON/JSONB loaders
        for OIDs 114/3802 automatically at import time, so a genuine
        `json`/`jsonb` column never even reaches this heuristic, `isinstance
        (v, str)` already `False` by the time this method sees it); a
        caller that must keep a TEXT column's string content literal
        despite it being JSON-shaped owns that coercion at ITS OWN layer
        instead (see `prompt_template_store.py`'s own `_coerce_content_to_str`
        for the concrete pattern: re-`json.dumps()` a `content` value that
        came back non-`str`, restoring the real string every caller of
        that store is promised). This adapter stays a "dumb storage
        layer" in the sense that it never MODELS what a column's real
        semantic type should be -- it does still apply this one generic,
        best-effort convenience decode uniformly, exactly as it always
        has.
        """
        for k, v in list(row.items()):
            if isinstance(v, str):
                s = v.strip()
                if (s.startswith("{") and s.endswith("}")) or (
                    s.startswith("[") and s.endswith("]")
                ):
                    try:
                        row[k] = json.loads(s)
                    except Exception:
                        pass
        return row

    # ---------------------------------------------------------------------
    # INSERT
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def insert(
        self,
        table: str,
        data: JsonDict,
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            # Applied regardless of who owns `conn` -- see this method's
            # own `session_vars` parameter doc on why a caller-supplied tx
            # still honors it (databaseFactory.py's create() is own_conn;
            # universal_engine's idempotency.py/batches.py call insert-
            # shaped methods on their own already-open tx and need this
            # exact same call to take effect on it).
            self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING *"
            values = [self._serialize_value(v) for v in data.values()]
            self._timed_execute(cursor, query, values, operation="insert", table=table)
            result_row = cursor.fetchone()
            columns_list = [desc[0] for desc in cursor.description]
            result = dict(zip(columns_list, result_row))
            if own_conn:
                conn.commit()
            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Insert failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # INSERT MANY (pipelined)
    # ---------------------------------------------------------------------

    def insert_many(
        self,
        table: str,
        rows: List[JsonDict],
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> int:
        """Insert many rows in ONE transaction and ONE network round trip (psycopg 3 `executemany` runs in pipeline mode).

        All-or-nothing: if any row fails the whole batch is rolled back and a DatabaseError is raised (the caller -- see
        `write_behind.WriteBehind` -- then retries the rows one by one). Rows may have different column sets; they are grouped by
        column set, and every group is sent inside the same transaction. No RETURNING: this is for rows nobody reads back."""
        table = validate_table_name(table)
        if not rows:
            return 0
        groups: Dict[Tuple[str, ...], List[JsonDict]] = {}
        for row in rows:
            cols = tuple(row.keys())
            for c in cols:
                validate_column_name(c)
            groups.setdefault(cols, []).append(row)

        conn = self._get_connection()
        try:
            self._apply_session_vars(conn, session_vars)
            t0 = time.perf_counter()
            with conn.cursor() as cursor:
                for cols, group in groups.items():
                    placeholders = ", ".join(["%s"] * len(cols))
                    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
                    cursor.executemany(sql, [[self._serialize_value(r[c]) for c in cols] for r in group])
            conn.commit()
            self._log_batch(len(rows), (time.perf_counter() - t0) * 1000.0)
            return len(rows)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            raise DatabaseError(f"Insert many failed: {str(e)}")
        finally:
            self._return_connection(conn)

    # ---------------------------------------------------------------------
    # SELECT
    # ---------------------------------------------------------------------

    @staticmethod
    def _build_select(
        table: str,
        query: Optional[Lookup],
        limit: Optional[int],
        offset: Optional[int],
        fields: Optional[List[str]],
    ) -> Tuple[str, List[Any]]:
        """The SQL text and bind parameters of a plain `select()` -- a pure function, shared by the single and the pipelined path."""
        sql = f"SELECT {', '.join(fields)} FROM {table}" if fields else f"SELECT * FROM {table}"
        params: List[Any] = []
        if query:
            where_parts: List[str] = []
            for k, v in query.items():
                validate_column_name(k)
                if v is None:
                    where_parts.append(f"{k} IS NULL")
                elif isinstance(v, (list, tuple)):
                    placeholders = ",".join(["%s"] * len(v))
                    where_parts.append(f"{k} IN ({placeholders})")
                    params.extend(list(v))
                else:
                    where_parts.append(f"{k} = %s")
                    params.append(v)
            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        if offset:
            sql += " OFFSET %s"
            params.append(offset)
        return sql, params

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def select(
        self,
        table: str,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
        fields: Optional[List[str]] = None,
    ) -> List[JsonDict]:
        table = validate_table_name(table)
        if fields:
            for f in fields:
                validate_column_name(f)

        # Inside a ReadBatch (the engine's coll.parallel) this query is queued and sent together with its siblings in one
        # pipelined round trip on one connection -- see polydb/read_batch.py. A caller-supplied transaction is never batched.
        batch = current_batch() if tx is None else None
        if batch is not None:
            return batch.submit(
                self,
                SelectSpec(table=table, query=query, limit=limit, offset=offset, fields=fields, session_vars=session_vars),
            )

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            sql, params = self._build_select(table, query, limit, offset, fields)

            self._timed_execute(
                cursor, sql, self._serialize_params(params), operation="select", table=table
            )
            columns = [desc[0] for desc in cursor.description]
            results = [self._deserialize_row(dict(zip(columns, row))) for row in cursor.fetchall()]
            cursor.close()
            if own_conn:
                conn.commit()
            return results
        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"Select failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    def select_many(self, specs: List[SelectSpec]) -> List[Any]:
        """Run several SELECTs in ONE round trip on ONE connection (psycopg 3 pipeline mode).

        Returns one entry per spec, in order: the rows (list of dicts), or a `DatabaseError` for a query that failed. A failing
        query never poisons the others: Postgres skips every statement after an error until the next sync point, so the specs
        that were skipped are simply sent again in a second pipeline (one extra round trip per failure, nothing in the common
        case).

        Every query keeps the guarantees a connection of its own would give it: it runs in its own READ ONLY transaction, with its
        own `session_vars` applied via `set_config(name, value, true)` (transaction-local, so row-level security sees exactly the
        tenant that spec asked for and nothing leaks to the next spec or the next borrower of the connection)."""
        results: List[Any] = [None] * len(specs)
        if not specs:
            return results

        built: List[Optional[Tuple[str, List[Any]]]] = []
        for i, sp in enumerate(specs):
            try:
                table = validate_table_name(sp.table)
                if sp.fields:
                    for f in sp.fields:
                        validate_column_name(f)
                if sp.builder is not None:
                    sql, params = self._build_linq(table, sp.builder)
                else:
                    sql, params = self._build_select(table, sp.query, sp.limit, sp.offset, sp.fields)
                for k in (sp.session_vars or {}):
                    if k not in ALLOWED_SESSION_VARS:
                        raise DatabaseError(
                            f"session_vars: {k!r} is not in the allow-list of Postgres "
                            f"session variables this adapter will set"
                        )
                built.append((sql, self._serialize_params(params)))
            except Exception as e:  # a malformed spec fails alone
                built.append(None)
                results[i] = e if isinstance(e, DatabaseError) else DatabaseError(f"Select failed: {e}")

        pending = [i for i in range(len(specs)) if built[i] is not None]
        if not pending:
            return results

        conn = self._get_connection()
        t0 = time.perf_counter()
        broken = False
        try:
            conn.autocommit = True
            while pending:
                cursors: Dict[int, Any] = {}
                pipeline_error: Optional[BaseException] = None
                try:
                    with conn.pipeline():
                        for i in pending:
                            sql, params = built[i]  # type: ignore[misc]
                            conn.execute("ROLLBACK")  # no-op when idle; clears a transaction the previous failure left aborted
                            conn.execute("BEGIN READ ONLY")
                            for k, v in (specs[i].session_vars or {}).items():
                                conn.execute("SELECT set_config(%s, %s, true)", [k, str(v)])
                            cur = conn.cursor()
                            cur.execute(sql, params)
                            conn.execute("COMMIT")
                            cursors[i] = cur
                except Exception as e:  # the first failing statement; everything after it was skipped by the server
                    pipeline_error = e

                skipped: List[int] = []
                failed_seen = False
                for i in pending:
                    if failed_seen:
                        skipped.append(i)
                        continue
                    cur = cursors.get(i)
                    try:
                        if specs[i].builder is not None and specs[i].builder.count_only:
                            results[i] = cur.fetchone()[0]  # type: ignore[union-attr]
                        else:
                            columns = [d[0] for d in cur.description]  # type: ignore[union-attr]
                            results[i] = [self._deserialize_row(dict(zip(columns, r))) for r in cur.fetchall()]  # type: ignore[union-attr]
                    except Exception as e:
                        failed_seen = True
                        cause = pipeline_error if pipeline_error is not None else e
                        results[i] = DatabaseError(f"Select failed: {str(cause)}")
                if not failed_seen and pipeline_error is not None:
                    # the pipeline failed after every result was already in (e.g. COMMIT of the last spec): nothing is lost
                    pass
                pending = skipped
        except Exception:
            broken = True
            raise
        finally:
            try:
                if not conn.closed:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    conn.autocommit = False
            except Exception:
                broken = True
            if broken:
                self._pool.putconn(conn, close=True)  # type: ignore[union-attr]
            else:
                self._return_connection(conn)
            self._log_batch(len(specs), (time.perf_counter() - t0) * 1000.0)
        return results

    def _log_batch(self, n: int, duration_ms: float) -> None:
        self.logger.debug("SQL pipeline executed: statements=%d duration_ms=%.1f", n, duration_ms)
        if duration_ms > self._slow_query_ms:
            self.logger.warning(
                "Slow query detected: operation=select_many statements=%d duration_ms=%.1f threshold=%.1f",
                n, duration_ms, self._slow_query_ms,
            )

    # ---------------------------------------------------------------------
    # SELECT PAGE
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def select_page(
        self,
        table: str,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None,
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> Tuple[List[JsonDict], Optional[str]]:
        offset = int(continuation_token) if continuation_token else 0
        results = self.select(
            table, query, limit=page_size + 1, offset=offset, tx=tx, session_vars=session_vars
        )
        has_more = len(results) > page_size
        if has_more:
            results = results[:page_size]
        next_token = str(offset + page_size) if has_more else None
        return results, next_token

    # ---------------------------------------------------------------------
    # UPDATE
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def update(
        self,
        table: str,
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
            params: List[Any] = [self._serialize_value(v) for v in data.values()]

            if isinstance(entity_id, dict):
                where_parts: List[str] = []
                for k, v in entity_id.items():
                    validate_column_name(k)
                    if v is None:
                        where_parts.append(f"{k} IS NULL")
                    else:
                        where_parts.append(f"{k} = %s")
                        params.append(self._serialize_param(v))
                where_clause = " AND ".join(where_parts)
            else:
                where_clause = "id = %s"
                params.append(entity_id)

            query = f"UPDATE {table} SET {set_clause} WHERE {where_clause} RETURNING *"
            self._timed_execute(cursor, query, params, operation="update", table=table)
            result_row = cursor.fetchone()
            if not result_row:
                raise DatabaseError("No rows updated")
            columns = [desc[0] for desc in cursor.description]
            result = dict(zip(columns, result_row))
            if own_conn:
                conn.commit()
            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Update failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # UPSERT
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def upsert(
        self,
        table: str,
        data: JsonDict,
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            conflict_columns = ["id"] if "id" in data else list(data.keys())[:1]
            update_fields = [k for k in data.keys() if k not in conflict_columns]

            if update_fields:
                update_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_fields])
                on_conflict = f"DO UPDATE SET {update_clause}"
            else:
                on_conflict = "DO NOTHING"

            query = f"""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(conflict_columns)})
                {on_conflict}
                RETURNING *
            """
            values = [self._serialize_value(v) for v in data.values()]
            self._timed_execute(cursor, query, values, operation="upsert", table=table)
            result_row = cursor.fetchone()
            if not result_row:
                if "id" in conflict_columns and "id" in data:
                    cursor.execute(f"SELECT * FROM {table} WHERE id = %s", [data["id"]])
                    result_row = cursor.fetchone()
                    if not result_row:
                        raise DatabaseError("Upsert did nothing and existing row not found")
                else:
                    raise DatabaseError("Upsert did nothing and cannot determine existing row")

            columns_list = [desc[0] for desc in cursor.description]
            result = dict(zip(columns_list, result_row))
            if own_conn:
                conn.commit()
            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Upsert failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # DELETE
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def delete(
        self,
        table: str,
        entity_id: Union[Any, Lookup],
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            params: List[Any] = []
            if isinstance(entity_id, dict):
                where_parts: List[str] = []
                for k, v in entity_id.items():
                    validate_column_name(k)
                    if v is None:
                        where_parts.append(f"{k} IS NULL")
                    else:
                        where_parts.append(f"{k} = %s")
                        params.append(self._serialize_param(v))
                where_clause = " AND ".join(where_parts)
            else:
                where_clause = "id = %s"
                params.append(entity_id)

            query = f"DELETE FROM {table} WHERE {where_clause} RETURNING *"
            self._timed_execute(cursor, query, params, operation="delete", table=table)
            result_row = cursor.fetchone()
            if not result_row:
                raise DatabaseError("No rows deleted")
            columns = [desc[0] for desc in cursor.description]
            result = dict(zip(columns, result_row))
            if own_conn:
                conn.commit()
            cursor.close()
            return self._deserialize_row(result)
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Delete failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # LINQ QUERY
    # ---------------------------------------------------------------------

    @staticmethod
    def _build_linq(table: str, builder: QueryBuilder) -> Tuple[str, List[Any]]:
        """The SQL text and bind parameters of a `query_linq()` -- a pure function, shared by the single and the pipelined path."""
        if builder.count_only:
            sql = f"SELECT COUNT(*) FROM {table}"
        elif builder.selected_fields:
            for f in builder.selected_fields:
                validate_column_name(f)
            fields = ", ".join(builder.selected_fields)
            if builder.distinct_flag:
                sql = f"SELECT DISTINCT {fields} FROM {table}"
            else:
                sql = f"SELECT {fields} FROM {table}"
        else:
            sql = f"SELECT * FROM {table}"

        params: List[Any] = []
        where_clause, where_params = builder.to_sql_where()
        if where_clause:
            sql += f" WHERE {where_clause}"
            params.extend(where_params)

        if builder.group_by_fields:
            for f in builder.group_by_fields:
                validate_column_name(f)
            sql += f" GROUP BY {', '.join(builder.group_by_fields)}"

        if builder.order_by_fields:
            order_parts = []
            for field, desc in builder.order_by_fields:
                validate_column_name(field)
                direction = "DESC" if desc else "ASC"
                order_parts.append(f"{field} {direction}")
            sql += f" ORDER BY {', '.join(order_parts)}"

        if builder.take_count is not None:
            sql += " LIMIT %s"
            params.append(builder.take_count)

        if builder.skip_count:
            sql += " OFFSET %s"
            params.append(builder.skip_count)
        return sql, params

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def query_linq(
        self,
        table: str,
        builder: QueryBuilder,
        tx: Optional[Any] = None,
        *,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> Union[List[JsonDict], int]:
        table = validate_table_name(table)
        batch = current_batch() if tx is None else None
        if batch is not None:
            return batch.submit(self, SelectSpec(table=table, builder=builder, session_vars=session_vars))
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            if own_conn:
                self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            sql, params = self._build_linq(table, builder)

            self._timed_execute(
                cursor, sql, self._serialize_params(params), operation="query_linq", table=table
            )

            if builder.count_only:
                result = cursor.fetchone()[0]
            else:
                columns = [desc[0] for desc in cursor.description]
                result = [
                    self._deserialize_row(dict(zip(columns, row))) for row in cursor.fetchall()
                ]

            cursor.close()
            if own_conn:
                conn.commit()
            return result
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"LINQ query failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # PAGED QUERY (generic PageRequest / PageResult)
    # ---------------------------------------------------------------------

    @property
    def capabilities(self):
        from ..models import BackendCapabilities

        return BackendCapabilities(
            server_order=True,
            server_filter=True,
            native_cursor=False,
            supports_count=True,
        )

    def query_paged(self, table: str, request, tx: Optional[Any] = None):
        """Server-side ORDER BY + LIMIT/OFFSET with opaque offset cursor."""
        from ..models import PageResult

        offset = 0
        if request.cursor:
            try:
                cd = json.loads(base64.b64decode(request.cursor.encode()).decode())
                offset = cd.get("offset", 0)
            except Exception:
                offset = 0

        builder = QueryBuilder()
        for k, v in (request.filters or {}).items():
            builder.where(k, Operator.EQ, v)
        if request.order_by:
            builder.order_by(request.order_by, descending=request.order_desc)
        builder.skip(offset)
        builder.take(request.limit + 1)
        if request.fields:
            builder.select_fields(request.fields)

        results = self.query_linq(table, builder, tx=tx)
        if isinstance(results, int):
            return PageResult(items=[], has_more=False)

        has_more = len(results) > request.limit
        if has_more:
            results = results[: request.limit]

        next_cursor = None
        if has_more:
            cursor_data = {
                "offset": offset + request.limit,
                "order_by": request.order_by,
                "desc": request.order_desc,
            }
            next_cursor = base64.b64encode(json.dumps(cursor_data).encode()).decode()

        return PageResult(items=results, next_cursor=next_cursor, has_more=has_more)

    # ---------------------------------------------------------------------
    # EXECUTE RAW SQL
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def execute(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        tx: Optional[Any] = None,
        *,
        fetch: bool = False,
        fetch_one: bool = False,
        session_vars: Optional[Dict[str, str]] = None,
    ) -> Union[None, JsonDict, List[JsonDict]]:
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        cursor = None
        try:
            # execute() is the raw-SQL escape hatch a few callers already
            # use for a query with no generic-CRUD equivalent (an atomic
            # claim, a bulk UPDATE, an ORDER BY) -- those are exactly the
            # callers RLS-protected engine-infrastructure tables need this
            # for (see universal_engine's idempotency.py/batches.py),
            # since they run several statements against the same `tx` and
            # only need the session var set once per transaction, same as
            # every other method above.
            self._apply_session_vars(conn, session_vars)
            cursor = conn.cursor()
            self.logger.debug("Executing raw SQL (%d params)", len(params or []))
            exec_params = self._serialize_params(params or [])
            self._timed_execute(cursor, sql, exec_params, operation="execute", table="")

            if fetch_one:
                row = cursor.fetchone()
                result = None
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    result = self._deserialize_row(dict(zip(columns, row)))
                if own_conn:
                    conn.commit()
                return result

            if fetch:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                results = [self._deserialize_row(dict(zip(columns, r))) for r in rows]
                if own_conn:
                    conn.commit()
                return results

            if own_conn:
                conn.commit()
            return None
        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"Execute failed: {str(e)}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # ATOMIC OPERATIONS
    # ---------------------------------------------------------------------

    def atomic_decrement_if_sufficient(
        self,
        table: str,
        balance_field: str,
        amount: Any,
        where_field: str,
        where_value: Any,
        *,
        tx: Optional[Any] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        validate_column_name(balance_field)
        validate_column_name(where_field)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()
            sql = (
                f"UPDATE {table} "
                f"SET {balance_field} = {balance_field} - %s "
                f"WHERE {where_field} = %s AND {balance_field} >= %s "
                f"RETURNING *"
            )
            cursor.execute(sql, [amount, where_value, amount])
            result_row = cursor.fetchone()

            if not result_row:
                if own_conn:
                    conn.rollback()
                raise InsufficientBalanceError(
                    f"Insufficient balance in {table}.{balance_field} for {where_field}={where_value!r}"
                )

            columns = [desc[0] for desc in cursor.description]
            result = dict(zip(columns, result_row))
            if own_conn:
                conn.commit()
            cursor.close()
            return self._deserialize_row(result)
        except InsufficientBalanceError:
            raise
        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"atomic_decrement_if_sufficient failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    def atomic_set_if(
        self,
        table: str,
        id_field: str,
        id_value: Any,
        field: str,
        value: Any,
        expected: Any,
        *,
        tx: Optional[Any] = None,
    ) -> Optional[JsonDict]:
        table = validate_table_name(table)
        validate_column_name(field)
        validate_column_name(id_field)

        conn = tx
        own_conn = not conn
        if own_conn:
            conn = self._get_connection()
        try:
            cursor = conn.cursor()
            sql = (
                f"UPDATE {table} SET {field} = %s "
                f"WHERE {id_field} = %s AND {field} = %s RETURNING *"
            )
            cursor.execute(sql, [value, id_value, expected])
            row = cursor.fetchone()
            if own_conn:
                conn.commit()
            cursor.close()
            if row is None:
                return None
            cols = [d[0] for d in cursor.description]
            return self._deserialize_row(dict(zip(cols, row)))
        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"atomic_set_if failed: {e}") from e
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    def atomic_max(
        self,
        table: str,
        id_field: str,
        id_value: Any,
        field: str,
        value: Any,
        *,
        tx: Optional[Any] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        validate_column_name(field)
        validate_column_name(id_field)

        conn = tx
        own_conn = not conn
        if own_conn:
            conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {table} SET {field} = GREATEST({field}, %s) WHERE {id_field} = %s RETURNING *",
                [value, id_value],
            )
            row = cursor.fetchone()
            if own_conn:
                conn.commit()
            cursor.close()
            if row is None:
                raise DatabaseError(f"atomic_max: row not found {id_field}={id_value!r}")
            cols = [d[0] for d in cursor.description]
            return self._deserialize_row(dict(zip(cols, row)))
        except DatabaseError:
            raise
        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"atomic_max failed: {e}") from e
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    def atomic_min(
        self,
        table: str,
        id_field: str,
        id_value: Any,
        field: str,
        value: Any,
        *,
        tx: Optional[Any] = None,
    ) -> JsonDict:
        table = validate_table_name(table)
        validate_column_name(field)
        validate_column_name(id_field)

        conn = tx
        own_conn = not conn
        if own_conn:
            conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {table} SET {field} = LEAST({field}, %s) WHERE {id_field} = %s RETURNING *",
                [value, id_value],
            )
            row = cursor.fetchone()
            if own_conn:
                conn.commit()
            cursor.close()
            if row is None:
                raise DatabaseError(f"atomic_min: row not found {id_field}={id_value!r}")
            cols = [d[0] for d in cursor.description]
            return self._deserialize_row(dict(zip(cols, row)))
        except DatabaseError:
            raise
        except Exception as e:
            if own_conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise DatabaseError(f"atomic_min failed: {e}") from e
        finally:
            if own_conn and conn:
                self._return_connection(conn)

    # ---------------------------------------------------------------------
    # SAVEPOINTS
    # ---------------------------------------------------------------------

    def begin_savepoint(self, name: str, tx: Any) -> None:
        try:
            with tx.cursor() as cur:
                cur.execute(pg_sql.SQL("SAVEPOINT {}").format(pg_sql.Identifier(name)))
        except Exception as e:
            raise DatabaseError(f"begin_savepoint({name!r}) failed: {str(e)}")

    def rollback_to_savepoint(self, name: str, tx: Any) -> None:
        try:
            with tx.cursor() as cur:
                cur.execute(pg_sql.SQL("ROLLBACK TO SAVEPOINT {}").format(pg_sql.Identifier(name)))
        except Exception as e:
            raise DatabaseError(f"rollback_to_savepoint({name!r}) failed: {str(e)}")

    def release_savepoint(self, name: str, tx: Any) -> None:
        try:
            with tx.cursor() as cur:
                cur.execute(pg_sql.SQL("RELEASE SAVEPOINT {}").format(pg_sql.Identifier(name)))
        except Exception as e:
            raise DatabaseError(f"release_savepoint({name!r}) failed: {str(e)}")

    # ---------------------------------------------------------------------
    # DISTRIBUTED LOCK
    # ---------------------------------------------------------------------

    @contextmanager
    def distributed_lock(self, lock_name: str) -> Iterator[None]:
        conn = None
        cursor = None
        lock_id = int(hashlib.sha256(lock_name.encode()).hexdigest(), 16) % (2**63)

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT pg_advisory_lock(%s);", (lock_id,))
                self.logger.debug("Acquired distributed lock: %s", lock_name)
            except Exception as e:
                raise DatabaseError(f"Distributed lock acquire failed: {e}") from e

            try:
                yield
            finally:
                try:
                    cursor.execute("SELECT pg_advisory_unlock(%s);", (lock_id,))
                    self.logger.debug("Released distributed lock: %s", lock_name)
                except Exception as e:
                    self.logger.exception(
                        "Distributed lock release failed for %s: %s", lock_name, e
                    )
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.commit()
                except Exception:
                    pass
                self._return_connection(conn)
