

# AltCodePro PolyDB

**AltCodePro PolyDB** is a production-ready **multi-cloud database abstraction layer for Python**.

It provides a **single unified API** for SQL, NoSQL, object storage, queues, and blockchain storage across major platforms including **AWS, Azure, GCP, Vercel, MongoDB, PostgreSQL, and Web3 networks**.

PolyDB enables developers to build **cloud-portable applications** without vendor lock-in.

It is designed for modern distributed systems that require:

* multi-cloud portability
* hybrid SQL + NoSQL architectures
* distributed storage routing
* event-driven queues
* object storage abstraction
* blockchain storage integration
* enterprise-grade reliability

---

# Architecture

PolyDB unifies multiple storage layers behind a single API.

```
                    PolyDB
                       │
        ┌──────────────┼──────────────┐
        │              │              │
     SQL DB         NoSQL KV        Queue
   PostgreSQL     Dynamo/Azure    SQS/PubSub
        │              │              │
        └─────── Object Storage ─────┘
                   S3 / Blob
                        │
                    Blockchain
                  (KV / Blob / Queue)
```

---

# Project Structure

```
polydb/
 ├ adapters/
 │   ├ aws/
 │   ├ azure/
 │   ├ gcp/
 │   ├ vercel/
 │   └ blockchain/
 │
 ├ core/
 │   ├ database_factory.py
 │   ├ query.py
 │   ├ batch.py
 │
 ├ security/
 ├ monitoring/
 ├ cache/
 └ multitenancy/
```

---

# Key Features

## Multi-Cloud Support

Works across multiple cloud providers with automatic environment detection.

| Provider   | NoSQL             | Object Storage | Queue       |
| ---------- | ----------------- | -------------- | ----------- |
| AWS        | DynamoDB          | S3             | SQS         |
| Azure      | Table Storage     | Blob Storage   | Azure Queue |
| GCP        | Firestore         | GCS            | Pub/Sub     |
| Vercel     | KV (Redis)        | Blob           | Queue       |
| MongoDB    | MongoDB           | —              | —           |
| PostgreSQL | SQL               | S3-compatible  | —           |
| Blockchain | Smart Contract KV | IPFS           | Event Queue |

---

# ❤️ Support the Project

PolyDB is **free and open source**.

If you find this project useful, you can support continued development.

### Sponsor

[![Sponsor](https://img.shields.io/badge/Sponsor-AltCodePro-red?logo=githubsponsors)](https://github.com/sponsors)

Or support the project through AltCodePro:

[https://chatgpt.com/g/g-68d6efde65e0819192b9e130fb56b26b-altcodepro-gpt-idea-to-mvps-investor-docs](https://chatgpt.com/g/g-68d6efde65e0819192b9e130fb56b26b-altcodepro-gpt-idea-to-mvps-investor-docs)

Your support helps improve:

* multi-cloud database adapters
* blockchain storage integrations
* enterprise scalability features
* developer tooling and documentation

## Hybrid SQL + NoSQL

PolyDB allows applications to use **SQL and NoSQL together** without changing code.

Example architecture:

```
Application
     │
PolyDB DatabaseFactory
     │
 ┌──────────────┬──────────────┐
 │              │              │
SQL          NoSQL           Storage
(Postgres)   (DynamoDB)     (S3)
```

Example:

```python
from polydb import DatabaseFactory

db = DatabaseFactory()

db.create(User, {"name": "Alice"})
db.read(User, {"email": "alice@email.com"})
db.update(User, 1, {"status": "active"})
db.delete(User, 1)
```

---

## Automatic Cloud Provider Detection

PolyDB automatically detects the cloud provider based on environment variables.

| Environment Variable            | Provider   |
| ------------------------------- | ---------- |
| AZURE_STORAGE_CONNECTION_STRING | Azure      |
| AWS_ACCESS_KEY_ID               | AWS        |
| GOOGLE_CLOUD_PROJECT            | GCP        |
| VERCEL_ENV                      | Vercel     |
| MONGODB_URI                     | MongoDB    |
| POSTGRES_URL                    | PostgreSQL |

You can also manually set:

```
CLOUD_PROVIDER=aws
```

---

# Object Storage

PolyDB provides a unified API for cloud storage systems.

Supported backends:

* AWS S3
* Azure Blob Storage
* Google Cloud Storage
* Vercel Blob
* IPFS (Blockchain)

Example:

```python
storage = factory.get_object_storage()

storage.upload("file.txt", b"hello world")
storage.download("file.txt")
storage.delete("file.txt")
```

---

# Queue Abstraction

PolyDB supports distributed message queues.

Supported systems:

* AWS SQS
* Azure Queue
* Google Pub/Sub
* Vercel Queue
* Blockchain event queues

Example:

```python
queue = factory.get_queue()

queue.send_message("jobs", {"task": "process"})
msg = queue.receive_message("jobs")
queue.delete_message("jobs", msg)
```

---

# Blockchain Storage

PolyDB includes experimental blockchain adapters.

Capabilities:

* Smart contract key-value storage
* IPFS blob storage
* Event-based queue systems

Supported networks:

* Ethereum
* Polygon
* BNB Chain
* Avalanche
* Local Ganache

Example:

```python
factory = CloudDatabaseFactory(provider="blockchain")

kv = factory.get_nosql_kv()
blob = factory.get_object_storage()
queue = factory.get_queue()
```

---

# Advanced Query Engine

PolyDB includes a LINQ-style query builder.

Supports:

* filtering
* ordering
* pagination
* projections
* count queries

Example:

```python
qb = QueryBuilder().where("age", Operator.GT, 18)

results = db.query_linq(User, qb)
```

---

# Batch Operations

High-performance bulk operations.

```python
db.batch.bulk_insert(User, users)

db.batch.bulk_update(User, updates)

db.batch.bulk_delete(User, ids)
```

---

# Multi-Tenancy

PolyDB supports SaaS-style tenant isolation.

Isolation strategies:

* shared schema
* separate schema
* separate database

Components:

* TenantRegistry
* TenantContext
* TenantIsolationEnforcer

---

# Caching

Built-in distributed caching using Redis.

Features:

* TTL caching
* LFU tracking
* cache warming
* cache invalidation

---

# Monitoring & Observability

PolyDB provides built-in metrics collection.

Tracks:

* query latency
* slow queries
* error rates
* cache hit rates

Prometheus export supported.

---

# Security

PolyDB includes enterprise-grade security features.

* field-level encryption
* row-level security
* audit logging
* tenant isolation
* data masking

---

# Installation

Install from PyPI:

```
pip install altcodepro-polydb-python
```

Optional dependencies:

```
pip install redis boto3 google-cloud-storage web3 ipfshttpclient
```

---

# Example

```python
from polydb import DatabaseFactory, polydb_model

@polydb_model
class User:
    __polydb__ = {
        "storage": "nosql"
    }

db = DatabaseFactory()

db.create(User, {"name": "Alice"})
users = db.read(User, {"name": "Alice"})
```

---

# Why PolyDB

PolyDB replaces multiple infrastructure SDKs with one API.

| Traditional Approach           | PolyDB                  |
| ------------------------------ | ----------------------- |
| Multiple cloud SDKs            | One unified API         |
| Provider lock-in               | Multi-cloud portability |
| Separate queue/storage clients | Unified infrastructure  |
| Complex data routing           | Automatic routing       |

---

# Roadmap

Planned features:

* GraphQL API layer
* distributed transactions
* vector database support
* AI retrieval pipelines
* edge database adapters

---

# License

MIT License

---

# Links

PyPI
[https://pypi.org/project/altcodepro-polydb-python/](https://pypi.org/project/altcodepro-polydb-python/)

Website
[https://chatgpt.com/g/g-68d6efde65e0819192b9e130fb56b26b-altcodepro-gpt-idea-to-mvps-investor-docs](https://chatgpt.com/g/g-68d6efde65e0819192b9e130fb56b26b-altcodepro-gpt-idea-to-mvps-investor-docs)