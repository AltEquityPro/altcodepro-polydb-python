# examples/complete_usage.py
"""
Complete PolyDB usage example with all features
"""

from polydb import (
    DatabaseFactory,
    polydb_model,
    QueryBuilder,
    Operator,
    AuditContext,
)


# 1. Define models
@polydb_model
class User:
    __polydb__ = {
        "storage": "sql",
        "table": "users",
        "cache": True,
        "cache_ttl": 600,
    }


@polydb_model
class Product:
    __polydb__ = {
        "storage": "nosql",
        "collection": "products",
        "pk_field": "category",
        "rk_field": "product_id",
        "cache": True,
        "cache_ttl": 300,
    }


# 2. Initialize factory
db = DatabaseFactory(
    enable_audit=True,
    enable_cache=True,
    soft_delete=True,
)


# 3. Set audit context (per request)
AuditContext.set(
    actor_id="user_123",
    roles=["admin", "editor"],
    tenant_id="tenant_abc",
    trace_id="trace_xyz",
    request_id="req_456",
    ip_address="192.168.1.1",
    user_agent="Mozilla/5.0",
)


# 4. CRUD operations with auto-audit and tenant injection
user = db.create(User, {
    "name": "John Doe",
    "email": "john@example.com",
    "role": "admin",
})
# Auto-injected: tenant_id, created_at, created_by, updated_at, updated_by


# 5. LINQ-style queries
query = (
    QueryBuilder()
    .where("role", Operator.EQ, "admin")
    .where("created_at", Operator.GT, "2025-01-01")
    .order_by("created_at", descending=True)
    .skip(0)
    .take(10)
    .select("id", "name", "email")
)

admins = db.query_linq(User, query)


# 6. Count queries
count_query = (
    QueryBuilder()
    .where("role", Operator.EQ, "admin")
    .count()
)

admin_count = db.query_linq(User, count_query)


# 7. Distinct queries
distinct_query = (
    QueryBuilder()
    .select("role")
    .distinct_on()
)

roles = db.query_linq(User, distinct_query)


# 8. Update with field-level audit
updated_user = db.update(User, user["id"], {
    "email": "newemail@example.com",
})
# Audit log shows: changed_fields = ["email", "updated_at", "updated_by"]


# 9. Soft delete (default)
db.delete(User, user["id"])
# Sets deleted_at, deleted_by instead of hard delete


# 10. Hard delete
db.delete(User, user["id"], hard=True)


# 11. Read with cache
users = db.read(User, {"role": "admin"}, limit=10)
# Cached for 600 seconds (from model cache_ttl)

# 13. NoSQL with overflow storage (automatic)
large_product = db.create(Product, {
    "category": "electronics",
    "product_id": "prod_123",
    "name": "Large Dataset Product",
    "data": {"key": "value"} * 100000,  # >1MB # type: ignore
})
# Automatically stored in blob storage, transparent retrieval


# 14. LINQ with complex filters
complex_query = (
    QueryBuilder()
    .where("category", Operator.EQ, "electronics")
    .where("price", Operator.GTE, 100)
    .where("price", Operator.LTE, 1000)
    .where("tags", Operator.CONTAINS, "featured")
    .order_by("price", descending=False)
    .take(20)
)

products = db.query_linq(Product, complex_query)


# 15. Upsert (insert or update)
product = db.upsert(Product, {
    "category": "electronics",
    "product_id": "prod_123",
    "name": "Updated Product",
    "price": 299.99,
})


# 16. Include deleted records
all_users = db.read(User, {}, include_deleted=True)


# 17. Verify audit chain integrity
from polydb.audit.AuditStorage import AuditStorage

audit = AuditStorage()
is_valid = audit.verify_chain(tenant_id="tenant_abc")
print(f"Audit chain valid: {is_valid}")


# 18. Cache invalidation
from polydb.cache import CacheStrategy
def bulk_insert(cursor):
    cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
    cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Bob", "bob@example.com"))



# 20. Read one
user = db.read_one(User, {"email": "john@example.com"})


# 21. Complex LINQ with grouping (SQL)
group_query = (
    QueryBuilder()
    .select("role")
    .where("created_at", Operator.GT, "2025-01-01")
    .group_by("role")
)

grouped = db.query_linq(User, group_query)


print("✅ All operations completed with full audit trail")