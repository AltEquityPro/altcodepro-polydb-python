"""SchemaBuilder splices identifiers and defaults into DDL, so both need guards.

``to_create_table`` / ``to_create_indexes`` interpolated the table name, column
names, index names and PRIMARY KEY columns straight into the statement, and
rendered a string default as ``DEFAULT '{value}'``. DDL cannot be
parameterised, so a caller deriving any of these from user input could append
arbitrary statements — a single quote in a default is enough to break out of
the literal.

Identifiers reuse the allowlist utils.validate_table_name /
validate_column_name that the SQL adapter already applies everywhere else;
string defaults get their quotes doubled (the SQL-standard escape).
"""

from __future__ import annotations

import pytest

from polydb.errors import ValidationError
from polydb.schema import Column, ColumnType, Index, SchemaBuilder


IDENTIFIER_INJECTIONS = [
    "users; DROP TABLE secrets; --",
    "users (x INT); DROP TABLE t; --",
    'users" OR 1=1 --',
    "users'--",
    "user name",
    "",
]


def _builder() -> SchemaBuilder:
    return SchemaBuilder().add_column(Column(name="id", type=ColumnType.UUID))


class TestIdentifiersAreValidated:
    @pytest.mark.parametrize("payload", IDENTIFIER_INJECTIONS)
    def test_table_name_is_rejected(self, payload):
        with pytest.raises(ValidationError):
            _builder().to_create_table(payload)

    @pytest.mark.parametrize("payload", IDENTIFIER_INJECTIONS)
    def test_column_name_is_rejected(self, payload):
        builder = SchemaBuilder().add_column(Column(name=payload, type=ColumnType.TEXT))
        with pytest.raises(ValidationError):
            builder.to_create_table("users")

    def test_primary_key_column_is_rejected(self):
        builder = SchemaBuilder().add_column(
            Column(name="id", type=ColumnType.UUID, primary_key=True)
        )
        builder.primary_keys = ["id); DROP TABLE users; --"]
        with pytest.raises(ValidationError):
            builder.to_create_table("users")

    @pytest.mark.parametrize("payload", IDENTIFIER_INJECTIONS)
    def test_index_name_is_rejected(self, payload):
        builder = _builder().add_index(Index(name=payload, columns=["id"]))
        with pytest.raises(ValidationError):
            builder.to_create_indexes("users")

    def test_index_column_is_rejected(self):
        builder = _builder().add_index(
            Index(name="idx_users_id", columns=["id); DROP TABLE users; --"])
        )
        with pytest.raises(ValidationError):
            builder.to_create_indexes("users")

    @pytest.mark.parametrize("payload", IDENTIFIER_INJECTIONS)
    def test_index_table_name_is_rejected(self, payload):
        builder = _builder().add_index(Index(name="idx_users_id", columns=["id"]))
        with pytest.raises(ValidationError):
            builder.to_create_indexes(payload)


class TestStringDefaultsCannotEscapeTheLiteral:
    def test_quote_in_default_does_not_break_out(self):
        builder = SchemaBuilder().add_column(
            Column(
                name="role",
                type=ColumnType.VARCHAR,
                max_length=32,
                default="x'); DROP TABLE users; --",
            )
        )
        sql = builder.to_create_table("users")

        # The payload stays entirely inside one balanced literal - the quote is
        # doubled, so nothing after it is parsed as SQL.
        literal = sql.split("DEFAULT ", 1)[1].rsplit("\n);", 1)[0]
        assert literal == "'x''); DROP TABLE users; --'"
        assert sql.count("'") % 2 == 0
        assert sql.rstrip().endswith(");")

    def test_nul_byte_in_default_is_rejected(self):
        builder = SchemaBuilder().add_column(
            Column(name="role", type=ColumnType.TEXT, default="a\x00b")
        )
        with pytest.raises(ValidationError):
            builder.to_create_table("users")


class TestLegitimateSchemasStillBuild:
    def test_create_table_is_unchanged_for_valid_input(self):
        builder = (
            SchemaBuilder()
            .add_column(Column(name="id", type=ColumnType.UUID, primary_key=True))
            .add_column(
                Column(
                    name="email",
                    type=ColumnType.VARCHAR,
                    max_length=255,
                    nullable=False,
                    unique=True,
                )
            )
            .add_column(
                Column(name="role", type=ColumnType.TEXT, default="member")
            )
            .add_column(Column(name="score", type=ColumnType.INTEGER, default=0))
            .add_column(Column(name="active", type=ColumnType.BOOLEAN, default=True))
        )

        sql = builder.to_create_table("users")

        assert sql == (
            "CREATE TABLE IF NOT EXISTS users (\n"
            "  id UUID,\n"
            "  email VARCHAR(255) NOT NULL UNIQUE,\n"
            "  role TEXT DEFAULT 'member',\n"
            "  score INTEGER DEFAULT 0,\n"
            "  active BOOLEAN DEFAULT True,\n"
            "  PRIMARY KEY (id)\n"
            ");"
        )

    def test_create_indexes_is_unchanged_for_valid_input(self):
        builder = _builder().add_index(
            Index(name="idx_users_email", columns=["email", "id"], unique=True)
        )

        assert builder.to_create_indexes("users") == [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email, id);"
        ]
