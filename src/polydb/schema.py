# src/polydb/schema.py
"""
Schema management and migrations
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from .errors import ValidationError
from .utils import validate_column_name, validate_table_name


def _render_default(value: Any) -> str:
    """Render a column DEFAULT as a literal that cannot escape its context.

    A string default used to be interpolated as ``DEFAULT '{value}'``, so a
    single quote in it closed the literal and the rest of the value was parsed
    as SQL - the whole statement is DDL, which cannot be parameterised, so the
    literal has to be made safe here. Quotes are doubled (the SQL-standard
    escape); numbers and booleans keep rendering bare as before.
    """
    if isinstance(value, bool) or isinstance(value, (int, float, Decimal)):
        return f"DEFAULT {value}"

    text = str(value)
    if "\x00" in text:
        raise ValidationError("Invalid column default: NUL characters are not allowed")
    escaped = text.replace("'", "''")
    return f"DEFAULT '{escaped}'"


class ColumnType(Enum):
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    JSONB = "JSONB"
    UUID = "UUID"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"


@dataclass
class Column:
    name: str
    type: ColumnType
    nullable: bool = True
    default: Optional[Any] = None
    primary_key: bool = False
    unique: bool = False
    max_length: Optional[int] = None


@dataclass
class Index:
    name: str
    columns: List[str]
    unique: bool = False
    # Postgres index access method -- "btree" (the default, and the only
    # one Postgres actually permits combined with UNIQUE) plus the real
    # non-default methods a caller may need for a specific column shape:
    # "gin"/"gist" (full-text search, JSONB containment, array overlap),
    # "hash" (equality-only, smaller than btree for that one case), "brin"
    # (large, naturally-ordered, append-mostly columns -- cheap to
    # maintain relative to btree at real scale). Never validated against
    # Postgres's own real per-type support here -- that's a live DDL
    # concern the caller's own database will enforce; this dataclass only
    # carries the caller's choice through to the generated statement.
    using: str = "btree"


class SchemaBuilder:
    """Build SQL schema definitions"""

    def __init__(self):
        self.columns: List[Column] = []
        self.indexes: List[Index] = []
        self.primary_keys: List[str] = []

    def add_column(self, column: Column) -> "SchemaBuilder":
        self.columns.append(column)
        if column.primary_key:
            self.primary_keys.append(column.name)
        return self

    def add_index(self, index: Index) -> "SchemaBuilder":
        self.indexes.append(index)
        return self

    def to_create_table(self, table_name: str) -> str:
        """Generate CREATE TABLE statement"""
        validate_table_name(table_name)
        col_defs = []

        for col in self.columns:
            parts = [validate_column_name(col.name)]

            # Type
            if col.type == ColumnType.VARCHAR and col.max_length:
                parts.append(f"VARCHAR({col.max_length})")
            else:
                parts.append(col.type.value)

            # Nullable
            if not col.nullable:
                parts.append("NOT NULL")

            # Default
            if col.default is not None:
                parts.append(_render_default(col.default))

            # Unique
            if col.unique:
                parts.append("UNIQUE")

            col_defs.append(" ".join(parts))

        # Primary key
        if self.primary_keys:
            pk_cols = [validate_column_name(c) for c in self.primary_keys]
            col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        sql += ",\n".join(f"  {col}" for col in col_defs)
        sql += "\n);"

        return sql

    def to_add_missing_columns(self, table_name: str) -> List[str]:
        """`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for every non-key column. `CREATE TABLE IF NOT EXISTS` never adds a
        column that was declared after the table was first created; appending these to the same migration makes an edited
        column list reach existing tables. Added columns are always nullable (existing rows have no value) and never
        UNIQUE; a declared default is kept."""
        validate_table_name(table_name)
        keys = set(self.primary_keys or [])
        out: List[str] = []
        for col in self.columns:
            if col.name in keys or col.primary_key:
                continue
            parts = [validate_column_name(col.name)]
            if col.type == ColumnType.VARCHAR and col.max_length:
                parts.append(f"VARCHAR({col.max_length})")
            else:
                parts.append(col.type.value)
            if col.default is not None:
                parts.append(_render_default(col.default))
            out.append(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {' '.join(parts)};")
        return out

    def to_create_indexes(self, table_name: str) -> List[str]:
        """Generate CREATE INDEX statements"""
        statements = []

        validate_table_name(table_name)

        for idx in self.indexes:
            unique = "UNIQUE " if idx.unique else ""
            # "btree" omitted outright rather than spelled out as
            # "USING btree" -- it's Postgres's own implicit default, and
            # every index this codebase generated before `using` existed
            # rendered without a USING clause at all; emitting it only for
            # a non-default method keeps every pre-existing caller's own
            # generated SQL text byte-for-byte unchanged.
            using = f"USING {idx.using} " if idx.using != "btree" else ""
            cols = ", ".join(validate_column_name(c) for c in idx.columns)
            sql = (
                f"CREATE {unique}INDEX IF NOT EXISTS {validate_table_name(idx.name)} "
                f"ON {table_name}{' ' if using else ''}{using}({cols});"
            )
            statements.append(sql)

        return statements


class MigrationManager:
    """Database migration management"""

    def __init__(self, sql_adapter):
        self.sql = sql_adapter
        self._ensure_migrations_table()

    def _ensure_migrations_table(self):
        """Create migrations tracking table"""
        schema = """
        CREATE TABLE IF NOT EXISTS polydb_migrations (
            id SERIAL PRIMARY KEY,
            version VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP DEFAULT NOW(),
            rollback_sql TEXT,
            checksum VARCHAR(64)
        );
        """
        self.sql.execute(schema)

    def apply_migration(
        self, version: str, name: str, up_sql: str, down_sql: Optional[str] = None
    ) -> bool:
        """Apply a migration"""
        import hashlib

        # Check if already applied
        existing = self.sql.execute(
            "SELECT version, checksum FROM polydb_migrations WHERE version = %s", [version], fetch_one=True
        )

        # Calculate checksum
        checksum = hashlib.sha256(up_sql.encode()).hexdigest()

        if existing:
            stored = existing.get("checksum") if isinstance(existing, dict) else None
            if not stored or stored == checksum:
                return False
            # The DDL under this version was edited (a column added, an index changed). Re-apply it -- the statements
            # this codebase generates are idempotent (IF NOT EXISTS) -- and remember the new checksum. A failure here
            # must not stop a deployment from starting: keep the old checksum so the next start tries again.
            try:
                self.sql.execute(up_sql)
                self.sql.execute(
                    "UPDATE polydb_migrations SET checksum = %s WHERE version = %s", [checksum, version]
                )
                return True
            except Exception as exc:  # noqa: BLE001
                import logging

                logging.getLogger(__name__).warning("Re-applying edited migration %s failed (kept old): %s", version, exc)
                return False

        try:
            # Execute migration
            self.sql.execute(up_sql)

            # Record migration
            self.sql.insert(
                "polydb_migrations",
                {"version": version, "name": name, "rollback_sql": down_sql, "checksum": checksum},
            )

            return True
        except Exception as e:
            raise Exception(f"Migration {version} failed: {str(e)}")

    def rollback_migration(self, version: str) -> bool:
        """Rollback a migration"""
        migration = self.sql.execute(
            "SELECT rollback_sql FROM polydb_migrations WHERE version = %s",
            [version],
            fetch_one=True,
        )

        if not migration or not migration.get("rollback_sql"):
            raise Exception(f"No rollback available for {version}")

        try:
            # Execute rollback
            self.sql.execute(migration["rollback_sql"])

            # Remove from migrations
            self.sql.execute("DELETE FROM polydb_migrations WHERE version = %s", [version])

            return True
        except Exception as e:
            raise Exception(f"Rollback {version} failed: {str(e)}")

    def get_applied_migrations(self) -> List[Dict[str, Any]]:
        """Get list of applied migrations"""
        return self.sql.execute("SELECT * FROM polydb_migrations ORDER BY applied_at", fetch=True)
