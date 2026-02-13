"""
REAL POLYDB INTEGRATION TEST
Provider-agnostic
"""

import uuid
import os

from polydb.databaseFactory import DatabaseFactory
from polydb.decorators import polydb_model
from polydb.types import ModelMeta


# ---------------------------------
# TEST MODELS
# ---------------------------------

@polydb_model
class TestUser:
    __polydb__ = ModelMeta(
        storage="nosql",
        collection="polydb_test_users",
    )


@polydb_model
class TestSQL:
    __polydb__ = ModelMeta(
        storage="sql",
        table="polydb_test_sql",
    )


# ---------------------------------
# MAIN TEST
# ---------------------------------

def main():

    db = DatabaseFactory()

    test_id = str(uuid.uuid4())[:8]

    print("Provider:", db._cloud_factory.provider.value)

    # -------------------
    # NoSQL
    # -------------------
    print("\n--- NOSQL TEST ---")

    db.create(TestUser, {
        "id": test_id,
        "name": "PolyDB"
    })

    rows = db.read(TestUser, {"id": test_id})
    assert rows

    print("Read OK")

    db.delete(TestUser, test_id)
    print("Delete OK")

    # -------------------
    # SQL
    # -------------------
    print("\n--- SQL TEST ---")

    # You must create this table manually once:
    # CREATE TABLE polydb_test_sql (id TEXT PRIMARY KEY, name TEXT);

    db.create(TestSQL, {
        "id": test_id,
        "name": "PolyDB SQL"
    })

    rows = db.read(TestSQL, {"id": test_id})
    assert rows

    print("SQL Read OK")

    db.delete(TestSQL, test_id)
    print("SQL Delete OK")

    print("\nALL TESTS PASSED 🎉")


if __name__ == "__main__":
    main()
