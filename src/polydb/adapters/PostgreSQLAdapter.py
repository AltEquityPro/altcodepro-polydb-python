# src/polydb/adapters/postgres.py
import datetime
from decimal import Decimal
import os
import threading
from typing import Any, Iterator, List, Optional, Tuple, Union
import hashlib
from contextlib import contextmanager
import json
from datetime import datetime, date
from psycopg2.extras import Json

from ..errors import DatabaseError, ConnectionError
from ..retry import retry
from ..utils import validate_table_name, validate_column_name
from ..query import QueryBuilder
from ..types import JsonDict, Lookup


class PostgreSQLAdapter:
    """PostgreSQL with full LINQ support, connection pooling, JSON/JSONB support"""

    def __init__(self, connection_string: Optional[str] = None):
        from ..utils import setup_logger

        self.logger = setup_logger(__name__)
        self.connection_string = connection_string or os.getenv(
            "POSTGRES_CONNECTION_STRING",
            os.getenv("POSTGRES_URL", ""),
        )
        if not self.connection_string:
            raise ConnectionError("POSTGRES_CONNECTION_STRING or POSTGRES_URL must be set")
        self._pool = None
        self._lock = threading.Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        try:
            import psycopg2.pool

            with self._lock:
                if not self._pool:
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=int(os.getenv("POSTGRES_MIN_CONNECTIONS", "2")),
                        maxconn=int(os.getenv("POSTGRES_MAX_CONNECTIONS", "20")),
                        dsn=self.connection_string,
                    )
                    self.logger.info("PostgreSQL pool initialized")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize PostgreSQL pool: {str(e)}")

    def _get_connection(self) -> Any:
        if not self._pool:
            self._initialize_pool()
        return self._pool.getconn()  # type: ignore

    def _return_connection(self, conn: Any):
        if self._pool and conn:
            self._pool.putconn(conn)

    # ---------------------------------------------------------------------
    # TRANSACTIONS
    # ---------------------------------------------------------------------

    def begin_transaction(self) -> Any:
        """Begin a transaction and return the connection handle."""
        conn = self._get_connection()
        conn.autocommit = False
        return conn

    def commit(self, tx: Any):
        """Commit the transaction using the provided connection."""
        if tx:
            tx.commit()
            self._return_connection(tx)

    def rollback(self, tx: Any):
        """Rollback the transaction using the provided connection."""
        if tx:
            tx.rollback()
            self._return_connection(tx)

    # ---------------------------------------------------------------------
    # JSON HELPERS
    # ---------------------------------------------------------------------
    def _json_safe(self, obj: Any):
        """
        Ensure JSON serialization never fails.
        Used only for Json() wrapping.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, date):
            return str(obj)
        if isinstance(obj, dict):
            return {k: self._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._json_safe(v) for v in obj]
        return obj

    def _serialize_value(self, v: Any) -> Any:
        """
        Make all outgoing values safe for psycopg2.

        Rules:
        - dict -> Json()
        - list -> leave as list (so TEXT[] works)
        - datetime/date -> pass as native (psycopg2 handles it)
        - Decimal -> convert to float
        - everything else -> pass as-is
        """

        if v is None:
            return None

        # Dict -> JSON/JSONB
        if isinstance(v, dict):
            return Json(self._json_safe(v))

        # List:
        # DO NOT wrap in Json() automatically.
        # If column is JSONB, Postgres will still accept Json(list).
        # But for TEXT[] columns we must send Python list.
        if isinstance(v, list):
            return v

        # Datetime / date
        if isinstance(v, (datetime, date)):
            return v  # psycopg2 handles natively

        # Decimal
        if isinstance(v, Decimal):
            return float(v)

        return v

    def _serialize_params(self, params: List[Any]) -> List[Any]:
        return [self._serialize_value(p) for p in params]

    def _deserialize_row(self, row: JsonDict) -> JsonDict:
        """
        Postgres JSON/JSONB often comes back as dict already depending on driver config.
        If it comes as a string, try json.loads safely.
        """
        for k, v in list(row.items()):
            if isinstance(v, str):
                s = v.strip()
                # quick cheap check to avoid parsing normal strings
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
            cursor.execute(query, values)

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

                    # IMPORTANT: None must be "IS NULL" not "= %s"
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

            cursor.execute(sql, self._serialize_params(params))
            columns = [desc[0] for desc in cursor.description]
            results = [self._deserialize_row(dict(zip(columns, row))) for row in cursor.fetchall()]
            cursor.close()

            return results
        except Exception as e:
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
                        params.append(self._serialize_value(v))
                where_clause = " AND ".join(where_parts)
            else:
                where_clause = "id = %s"
                params.append(entity_id)

            query = f"UPDATE {table} SET {set_clause} WHERE {where_clause} RETURNING *"
            cursor.execute(query, params)

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
            cursor.execute(query, values)

            result_row = cursor.fetchone()
            if not result_row:
                # DO NOTHING case: fetch existing row (best effort)
                # If conflict is on id, we can read it back.
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
                        params.append(self._serialize_value(v))
                where_clause = " AND ".join(where_parts)
            else:
                where_clause = "id = %s"
                params.append(entity_id)

            query = f"DELETE FROM {table} WHERE {where_clause} RETURNING *"
            cursor.execute(query, params)
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

            # ------------------------------------------------
            # SELECT clause
            # ------------------------------------------------

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

            # ------------------------------------------------
            # WHERE
            # ------------------------------------------------

            where_clause, where_params = builder.to_sql_where()

            if where_clause:
                sql += f" WHERE {where_clause}"
                params.extend(where_params)

            # ------------------------------------------------
            # GROUP BY
            # ------------------------------------------------

            if builder.group_by_fields:

                for f in builder.group_by_fields:
                    validate_column_name(f)

                sql += f" GROUP BY {', '.join(builder.group_by_fields)}"

            # ------------------------------------------------
            # ORDER BY
            # ------------------------------------------------

            if builder.order_by_fields:

                order_parts = []

                for field, desc in builder.order_by_fields:

                    validate_column_name(field)

                    direction = "DESC" if desc else "ASC"

                    order_parts.append(f"{field} {direction}")

                sql += f" ORDER BY {', '.join(order_parts)}"

            # ------------------------------------------------
            # LIMIT
            # ------------------------------------------------

            if builder.take_count is not None:
                sql += " LIMIT %s"
                params.append(builder.take_count)

            # ------------------------------------------------
            # OFFSET
            # ------------------------------------------------

            if builder.skip_count:
                sql += " OFFSET %s"
                params.append(builder.skip_count)

            # ------------------------------------------------
            # EXECUTE
            # ------------------------------------------------

            cursor.execute(sql, self._serialize_params(params))

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
            self.logger.debug("Executing raw SQL: %s", sql)

            exec_params = self._serialize_params(params or [])
            cursor.execute(sql, exec_params)

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
    # DISTRIBUTED LOCK
    # ---------------------------------------------------------------------

    @contextmanager
    def distributed_lock(self, lock_name: str) -> Iterator[None]:
        """
        PostgreSQL advisory lock (session scoped).
        - Always unlock before returning the pooled connection.
        - Never wrap exceptions raised by the user block.
        """
        conn = None
        cursor = None
        lock_id = int(hashlib.sha256(lock_name.encode()).hexdigest(), 16) % (2**63)

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Acquire lock (DB op) — if this fails, it's a DatabaseError
            try:
                cursor.execute("SELECT pg_advisory_lock(%s);", (lock_id,))
                self.logger.debug("Acquired distributed lock: %s", lock_name)
            except Exception as e:
                raise DatabaseError(f"Distributed lock acquire failed: {e}") from e

            try:
                # User code runs here. If it raises, it MUST propagate unchanged.
                yield
            finally:
                # Release lock (DB op). Always attempted.
                try:
                    cursor.execute("SELECT pg_advisory_unlock(%s);", (lock_id,))
                    self.logger.debug("Released distributed lock: %s", lock_name)
                except Exception as e:
                    # IMPORTANT: don't mask user exceptions.
                    # If unlock fails, raise DatabaseError only if user block didn't already fail.
                    # Easiest safe behavior: just log and continue.
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
