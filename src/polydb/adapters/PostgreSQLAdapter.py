# src/polydb/adapters/postgres.py
import os
import threading
from typing import Any, List, Optional, Tuple, Union
import hashlib
from contextlib import contextmanager

from ..errors import DatabaseError, ConnectionError
from ..retry import retry
from ..utils import validate_table_name, validate_column_name
from ..query import QueryBuilder
from ..types import JsonDict, Lookup


class PostgreSQLAdapter:
    """PostgreSQL with full LINQ support, connection pooling"""

    def __init__(self):
        from ..utils import setup_logger

        self.logger = setup_logger(__name__)
        self.connection_string = os.getenv(
            "POSTGRES_CONNECTION_STRING",
            os.getenv("POSTGRES_URL", "postgresql://user:password@localhost:5432/database"),
        )
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

    def begin_transaction(self) -> Any:
        """Begin a transaction and return the connection handle."""
        conn = self._get_connection()
        conn.autocommit = False  # Ensure transaction mode
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

            cursor.execute(query, list(data.values()))
            result_row = cursor.fetchone()
            columns_list = [desc[0] for desc in cursor.description]
            result = dict(zip(columns_list, result_row))

            if own_conn:
                conn.commit()

            cursor.close()
            return result
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Insert failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

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
            params = []

            if query:
                where_parts = []
                for k, v in query.items():
                    validate_column_name(k)
                    if isinstance(v, (list, tuple)):
                        placeholders = ",".join(["%s"] * len(v))
                        where_parts.append(f"{k} IN ({placeholders})")
                        params.extend(v)
                    else:
                        where_parts.append(f"{k} = %s")
                        params.append(v)

                if where_parts:
                    sql += " WHERE " + " AND ".join(where_parts)

            if limit:
                sql += f" LIMIT %s"
                params.append(limit)
            if offset:
                sql += f" OFFSET %s"
                params.append(offset)

            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()

            return results
        except Exception as e:
            raise DatabaseError(f"Select failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

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
            params = list(data.values())

            if isinstance(entity_id, dict):
                where_parts = []
                for k, v in entity_id.items():
                    validate_column_name(k)
                    where_parts.append(f"{k} = %s")
                    params.append(v)
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
            return result
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Update failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

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
            update_clause = ", ".join(
                [f"{k} = EXCLUDED.{k}" for k in data.keys() if k not in conflict_columns]
            )

            query = f"""
                INSERT INTO {table} ({columns}) 
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(conflict_columns)}) 
                DO UPDATE SET {update_clause}
                RETURNING *
            """

            cursor.execute(query, list(data.values()))
            result_row = cursor.fetchone()
            columns_list = [desc[0] for desc in cursor.description]
            result = dict(zip(columns_list, result_row))

            if own_conn:
                conn.commit()

            cursor.close()
            return result
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Upsert failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

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

            params = []
            if isinstance(entity_id, dict):
                where_parts = []
                for k, v in entity_id.items():
                    validate_column_name(k)
                    where_parts.append(f"{k} = %s")
                    params.append(v)
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
            return result
        except Exception as e:
            if own_conn:
                conn.rollback()
            raise DatabaseError(f"Delete failed: {str(e)}")
        finally:
            if own_conn and conn:
                self._return_connection(conn)

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
            elif builder.select_fields:
                for f in builder.select_fields:
                    validate_column_name(f)
                fields = ", ".join(builder.select_fields)
                if builder.distinct:
                    sql = f"SELECT DISTINCT {fields} FROM {table}"
                else:
                    sql = f"SELECT {fields} FROM {table}"
            else:
                sql = f"SELECT * FROM {table}"

            params = []

            # WHERE
            where_clause, where_params = builder.to_sql_where()
            if where_clause:
                sql += f" WHERE {where_clause}"
                params.extend(where_params)

            # GROUP BY
            if builder.group_by_fields:
                for f in builder.group_by_fields:
                    validate_column_name(f)
                sql += f" GROUP BY {', '.join(builder.group_by_fields)}"

            # ORDER BY
            if builder.order_by_fields:
                order_parts = []
                for field, desc in builder.order_by_fields:
                    validate_column_name(field)
                    direction = "DESC" if desc else "ASC"
                    order_parts.append(f"{field} {direction}")
                sql += f" ORDER BY {', '.join(order_parts)}"

            # LIMIT / OFFSET
            if builder.take_count:
                sql += f" LIMIT %s"
                params.append(builder.take_count)

            if builder.skip_count:
                sql += f" OFFSET %s"
                params.append(builder.skip_count)

            cursor.execute(sql, params)

            if builder.count_only:
                result = cursor.fetchone()[0]
            else:
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                result = results

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

    def __del__(self):
        if self._pool:
            try:
                self._pool.closeall()
            except:
                pass

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
            cursor.execute(sql, params or [])

            # Fetch results (if any) BEFORE commit (fine either way)
            if fetch_one:
                row = cursor.fetchone()
                result = None
                if row:
                    columns = [desc[0] for desc in cursor.description]
                    result = dict(zip(columns, row))
                if own_conn:
                    conn.commit()
                return result

            if fetch:
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, r)) for r in rows]
                if own_conn:
                    conn.commit()
                return results

            # Non-fetch execution (DDL/DML)
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

    @contextmanager
    def distributed_lock(self, lock_name: str):
        """
        PostgreSQL advisory lock.
        Cluster-wide distributed lock.

        Safe across:
        - Multiple pods
        - Multiple containers
        - Multiple instances
        """

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Convert lock_name to 64-bit hash
            lock_id = int(hashlib.sha256(lock_name.encode()).hexdigest(), 16) % (2**63)

            cursor.execute("SELECT pg_advisory_lock(%s);", (lock_id,))
            self.logger.debug(f"Acquired distributed lock: {lock_name}")

            yield

            cursor.execute("SELECT pg_advisory_unlock(%s);", (lock_id,))
            self.logger.debug(f"Released distributed lock: {lock_name}")

            cursor.close()
            conn.commit()

        except Exception as e:
            if conn:
                conn.rollback()
            raise DatabaseError(f"Distributed lock failed: {str(e)}")

        finally:
            if conn:
                self._return_connection(conn)