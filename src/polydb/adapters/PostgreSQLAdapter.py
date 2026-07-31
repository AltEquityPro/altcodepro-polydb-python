# src/polydb/adapters/postgres.py
import os
import threading
import time
from typing import Any, Iterator, List, Optional, Tuple, Union
import hashlib
from contextlib import contextmanager
import json
import base64
from decimal import Decimal
from datetime import datetime, date

import psycopg2.extensions
from psycopg2 import sql as pg_sql
from psycopg2.extras import Json

from ..errors import DatabaseError, ConnectionError, InsufficientBalanceError
from ..retry import retry
from ..utils import validate_table_name, validate_column_name
from ..query import QueryBuilder, Operator
from ..types import JsonDict, Lookup


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
            return conn.info.transaction_status == psycopg2.extensions.TRANSACTION_STATUS_IDLE
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
            import psycopg2.pool
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

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

                new_query = urlencode(query, doseq=True)
                parsed = parsed._replace(query=new_query)
                dsn = urlunparse(parsed)

            _maxconn = int(os.getenv("POSTGRES_MAX_CONNECTIONS", "100"))
            with self._lock:
                if not self._pool:
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=int(os.getenv("POSTGRES_MIN_CONNECTIONS", "5")),
                        maxconn=_maxconn,
                        dsn=dsn,
                    )
                    self.logger.info(
                        "PostgreSQL pool initialized: min=%s max=%s",
                        os.getenv("POSTGRES_MIN_CONNECTIONS", "5"),
                        _maxconn,
                    )
        except Exception as e:
            raise ConnectionError(f"Failed to initialize PostgreSQL pool: {str(e)}")

    def _log_pool_utilization(self) -> None:
        if not self._pool:
            return
        try:
            maxconn = int(os.getenv("POSTGRES_MAX_CONNECTIONS", "100"))
            used_count = len(getattr(self._pool, "_used", {}))
            pct = (used_count / maxconn * 100) if maxconn else 0
            if pct > 80:
                self.logger.warning(
                    "PostgreSQL pool utilization: %d/%d connections in use (%.0f%%)",
                    used_count, maxconn, pct,
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
        cursor.execute(sql, params)
        duration_ms = (time.perf_counter() - t0) * 1000.0

        self.logger.debug(
            "SQL executed",
            extra={"operation": operation, "table": table, "duration_ms": round(duration_ms, 3)},
        )

        if duration_ms > self._slow_query_ms:
            self.logger.warning(
                "Slow query detected: operation=%s table=%s duration_ms=%.1f threshold=%.1f",
                operation, table, duration_ms, self._slow_query_ms,
            )

        return duration_ms

    # ---------------------------------------------------------------------
    # TRANSACTIONS
    # ---------------------------------------------------------------------

    def reset_pool(self):
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
        if isinstance(v, (dict, list, tuple)):
            return Json(self._json_safe(v))
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
    def insert(self, table: str, data: JsonDict, tx: Optional[Any] = None) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
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
    # SELECT
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def select(
        self,
        table: str,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tx: Optional[Any] = None,
    ) -> List[JsonDict]:
        table = validate_table_name(table)
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()
            sql = f"SELECT * FROM {table}"
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
    ) -> Tuple[List[JsonDict], Optional[str]]:
        offset = int(continuation_token) if continuation_token else 0
        results = self.select(table, query, limit=page_size + 1, offset=offset, tx=tx)
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
    def upsert(self, table: str, data: JsonDict, tx: Optional[Any] = None) -> JsonDict:
        table = validate_table_name(table)
        for k in data.keys():
            validate_column_name(k)

        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
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
        self, table: str, entity_id: Union[Any, Lookup], tx: Optional[Any] = None
    ) -> JsonDict:
        table = validate_table_name(table)
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
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

    @retry(max_attempts=3, delay=1.0, exceptions=(DatabaseError,))
    def query_linq(
        self, table: str, builder: QueryBuilder, tx: Optional[Any] = None
    ) -> Union[List[JsonDict], int]:
        table = validate_table_name(table)
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        try:
            cursor = conn.cursor()
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
            results = results[:request.limit]

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
    ) -> Union[None, JsonDict, List[JsonDict]]:
        conn = tx
        own_conn = False
        if not conn:
            conn = self._get_connection()
            own_conn = True

        cursor = None
        try:
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
