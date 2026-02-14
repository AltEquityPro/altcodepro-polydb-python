# PolyDB v2.2.0 - Enterprise Database Abstraction Layer

**Production-ready, cloud-independent database abstraction with full LINQ support, field-level audit, cache, and overflow storage**

## Features

✅ **LINQ-Style Queries** - All SQL clauses: WHERE, ORDER BY, SELECT, GROUP BY, DISTINCT, COUNT  
✅ **Multi-Cloud** - Azure, AWS, GCP, Vercel, MongoDB, PostgreSQL  
✅ **Automatic Overflow** - Large NoSQL records → Object Storage (transparent)  
✅ **Enterprise Audit** - Cryptographic hash chain, field-level changes, strict ordering  
✅ **Cache Engine** - In-memory with TTL, automatic invalidation  
✅ **Soft Delete** - Optional logical deletion with audit trail  
✅ **Auto-Inject** - Tenant ID, audit fields (created_at, updated_by, etc.)  
✅ **Thread-Safe** - Connection pooling, distributed-safe hash chaining  
✅ **Retry Logic** - Exponential backoff, configurable  
✅ **Type-Safe** - Protocol-based adapters, full type hints  

## Quick Start

```python
from polydb import DatabaseFactory, polydb_model, QueryBuilder, Operator, AuditContext

# 1. Define model
@polydb_model
class User:
    __polydb__ = {
        "storage": "sql",
        "table": "users",
        "cache": True,
        "cache_ttl": 600,
    }

# 2. Initialize
db = DatabaseFactory(
    enable_audit=True,
    enable_cache=True,
    soft_delete=True,
)

# 3. Set context (per-request)
AuditContext.set(
    actor_id="user_123",
    roles=["admin"],
    tenant_id="tenant_abc",
)

# 4. CRUD with auto-audit
user = db.create(User, {"name": "John", "email": "john@example.com"})
# Auto-injects: tenant_id, created_at, created_by, updated_at, updated_by

# 5. LINQ queries
query = (
    QueryBuilder()
    .where("role", Operator.EQ, "admin")
    .where("age", Operator.GTE, 18)
    .order_by("created_at", descending=True)
    .skip(10)
    .take(20)
    .select("id", "name", "email")
)

admins = db.query_linq(User, query)
```

## LINQ Operations

### Filters
```python
builder.where("field", Operator.EQ, value)      # ==
builder.where("field", Operator.NE, value)      # !=
builder.where("field", Operator.GT, value)      # >
builder.where("field", Operator.GTE, value)     # >=
builder.where("field", Operator.LT, value)      # <
builder.where("field", Operator.LTE, value)     # <=
builder.where("field", Operator.IN, [1,2,3])    # IN
builder.where("field", Operator.CONTAINS, "text") # LIKE
builder.where("field", Operator.STARTS_WITH, "prefix")
builder.where("field", Operator.ENDS_WITH, "suffix")
```

### Ordering & Pagination
```python
builder.order_by("field", descending=True)
builder.skip(10)
builder.take(20)
```

### Projection
```python
builder.select("id", "name", "email")
```

### Aggregation
```python
builder.count()
builder.distinct_on()
builder.group_by("role", "department")
```

## NoSQL Overflow Storage

Records >1MB automatically stored in object storage:

```python
@polydb_model
class Product:
    __polydb__ = {
        "storage": "nosql",
        "collection": "products",
    }

# Large data automatically overflows
product = db.create(Product, {
    "data": {"huge": "payload"} * 100000
})

# Transparent retrieval
retrieved = db.read_one(Product, {"id": product["id"]})
# User never knows it came from blob storage
```

## Enterprise Audit Trail

### Automatic Tracking
- **Who**: actor_id, roles
- **What**: action, model, entity_id, changed_fields
- **When**: timestamp (microsecond precision)
- **Where**: tenant_id, ip_address, user_agent
- **Context**: trace_id, request_id
- **Integrity**: cryptographic hash chain

### Field-Level Changes
```python
db.update(User, user_id, {"email": "new@example.com"})
# Audit log shows: changed_fields = ["email", "updated_at", "updated_by"]
```

### Verify Chain
```python
from polydb.audit.storage import AuditStorage

audit = AuditStorage()
is_valid = audit.verify_chain(tenant_id="tenant_abc")
```

## Cache Engine

```python
# Auto-cache from model metadata
@polydb_model
class User:
    __polydb__ = {
        "storage": "sql",
        "table": "users",
        "cache": True,
        "cache_ttl": 600,  # 10 minutes
    }

# Cached read
users = db.read(User, {"role": "admin"})

# Bypass cache
users = db.read(User, {"role": "admin"}, no_cache=True)

# Manual invalidation
from polydb.cache import RedisCacheEngine
cache = RedisCacheEngine()
cache.invalidate("User")
cache.clear()
```

## Soft Delete

```python
db = DatabaseFactory(soft_delete=True)

# Soft delete (sets deleted_at, deleted_by)
db.delete(User, user_id)

# Hard delete
db.delete(User, user_id, hard=True)

# Include deleted in queries
all_users = db.read(User, {}, include_deleted=True)
```

## Pagination

```python
page1, next_token = db.read_page(User, {"role": "admin"}, page_size=50)
page2, token2 = db.read_page(User, {"role": "admin"}, page_size=50, continuation_token=next_token)
```

## Multi-Tenant

```python
# Auto-inject tenant_id from context
AuditContext.set(tenant_id="tenant_123", actor_id="user_456", roles=["admin"])

user = db.create(User, {"name": "John"})
# Result: {"name": "John", "tenant_id": "tenant_123", "created_by": "user_456", ...}

# Filter by tenant (automatic)
users = db.read(User, {})  # Only returns tenant_123 records
```

## Environment Variables

```bash
# Provider selection (optional, auto-detected)
CLOUD_PROVIDER=aws|azure|gcp|vercel|mongodb|postgresql

# PostgreSQL
POSTGRES_CONNECTION_STRING=postgresql://user:pass@host:5432/db
POSTGRES_MIN_CONNECTIONS=2
POSTGRES_MAX_CONNECTIONS=20

# AWS
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
DYNAMODB_TABLE_NAME=...
S3_BUCKET_NAME=...

# Azure
AZURE_STORAGE_CONNECTION_STRING=...
AZURE_TABLE_NAME=...
AZURE_CONTAINER_NAME=...

# GCP
GOOGLE_CLOUD_PROJECT=...
FIRESTORE_COLLECTION=...
GCS_BUCKET_NAME=...

# MongoDB
MONGODB_URI=mongodb://...
MONGODB_DATABASE=...
MONGODB_COLLECTION=...

# Vercel
KV_URL=...
KV_REST_API_TOKEN=...
BLOB_READ_WRITE_TOKEN=...
```

## Model Metadata

```python
@polydb_model
class Entity:
    __polydb__ = {
        # Required
        "storage": "sql" | "nosql",
        
        # SQL
        "table": "table_name",
        
        # NoSQL
        "collection": "collection_name",
        "pk_field": "partition_key_field",
        "rk_field": "row_key_field",
        
        # Cache
        "cache": True,
        "cache_ttl": 300,  # seconds
        
        # Optional
        "provider": "aws",  # Override auto-detection
    }
```

## Thread Safety

- Connection pooling with locks
- Distributed-safe audit hash chaining
- Cache with thread-safe operations
- Retry logic with exponential backoff

## Performance

- **SQL**: Connection pooling (min=2, max=20)
- **NoSQL**: Client reuse, overflow storage
- **Cache**: In-memory with automatic invalidation
- **Retry**: Configurable backoff (0.5s-6s)

## Production Checklist

✅ Set `CLOUD_PROVIDER` explicitly  
✅ Configure connection pool sizes  
✅ Enable audit (`enable_audit=True`)  
✅ Set cache TTL per model  
✅ Use soft delete for compliance  
✅ Set audit context per request  
✅ Monitor audit chain integrity  
✅ Configure retry attempts  

## License

MIT