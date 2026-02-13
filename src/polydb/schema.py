# src/polydb/schema.py
"""
Schema management and migrations
"""
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import json
from datetime import datetime


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


class SchemaBuilder:
    """Build SQL schema definitions"""
    
    def __init__(self):
        self.columns: List[Column] = []
        self.indexes: List[Index] = []
        self.primary_keys: List[str] = []
    
    def add_column(self, column: Column) -> 'SchemaBuilder':
        self.columns.append(column)
        if column.primary_key:
            self.primary_keys.append(column.name)
        return self
    
    def add_index(self, index: Index) -> 'SchemaBuilder':
        self.indexes.append(index)
        return self
    
    def to_create_table(self, table_name: str) -> str:
        """Generate CREATE TABLE statement"""
        col_defs = []
        
        for col in self.columns:
            parts = [col.name]
            
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
                if isinstance(col.default, str):
                    parts.append(f"DEFAULT '{col.default}'")
                else:
                    parts.append(f"DEFAULT {col.default}")
            
            # Unique
            if col.unique:
                parts.append("UNIQUE")
            
            col_defs.append(" ".join(parts))
        
        # Primary key
        if self.primary_keys:
            col_defs.append(f"PRIMARY KEY ({', '.join(self.primary_keys)})")
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        sql += ",\n".join(f"  {col}" for col in col_defs)
        sql += "\n);"
        
        return sql
    
    def to_create_indexes(self, table_name: str) -> List[str]:
        """Generate CREATE INDEX statements"""
        statements = []
        
        for idx in self.indexes:
            unique = "UNIQUE " if idx.unique else ""
            cols = ", ".join(idx.columns)
            sql = f"CREATE {unique}INDEX IF NOT EXISTS {idx.name} ON {table_name}({cols});"
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
        self,
        version: str,
        name: str,
        up_sql: str,
        down_sql: Optional[str] = None
    ) -> bool:
        """Apply a migration"""
        import hashlib
        
        # Check if already applied
        existing = self.sql.execute(
            "SELECT version FROM polydb_migrations WHERE version = %s",
            [version],
            fetch_one=True
        )
        
        if existing:
            return False
        
        # Calculate checksum
        checksum = hashlib.sha256(up_sql.encode()).hexdigest()
        
        try:
            # Execute migration
            self.sql.execute(up_sql)
            
            # Record migration
            self.sql.insert('polydb_migrations', {
                'version': version,
                'name': name,
                'rollback_sql': down_sql,
                'checksum': checksum
            })
            
            return True
        except Exception as e:
            raise Exception(f"Migration {version} failed: {str(e)}")
    
    def rollback_migration(self, version: str) -> bool:
        """Rollback a migration"""
        migration = self.sql.execute(
            "SELECT rollback_sql FROM polydb_migrations WHERE version = %s",
            [version],
            fetch_one=True
        )
        
        if not migration or not migration.get('rollback_sql'):
            raise Exception(f"No rollback available for {version}")
        
        try:
            # Execute rollback
            self.sql.execute(migration['rollback_sql'])
            
            # Remove from migrations
            self.sql.execute(
                "DELETE FROM polydb_migrations WHERE version = %s",
                [version]
            )
            
            return True
        except Exception as e:
            raise Exception(f"Rollback {version} failed: {str(e)}")
    
    def get_applied_migrations(self) -> List[Dict[str, Any]]:
        """Get list of applied migrations"""
        return self.sql.execute(
            "SELECT * FROM polydb_migrations ORDER BY applied_at",
            fetch=True
        )