# CLAUDE.md — altcodepro-polydb-python (PolyDB)

Guidance for Claude Code when working in this repository.

## What this project is

**PolyDB** is an open-source (MIT), production-oriented **multi-cloud storage abstraction layer for
Python**. It gives an application one API for **SQL, NoSQL key-value, object storage, shared files,
and queues** and lets the backing provider (AWS / Azure / GCP / Vercel / MongoDB / PostgreSQL /
blockchain) be a configuration choice rather than a code change. It also hides each backend's
per-record size ceiling by transparently spilling oversized payloads to object storage and
rehydrating them on read — see [Transparent large-payload overflow](#transparent-large-payload-overflow-headline-feature).

- Package name: `altcodepro-polydb-python`, import name `polydb`
- Source layout: `src/polydb/` (setuptools `package-dir = {"" = "src"}`), ships `py.typed`
- Python: `requires-python >= 3.11`
- Repo: https://github.com/altcodepro/polydb-python
- ~14k LOC across 59 modules; integration tests in [tests/](tests/) run against emulators
  (Azurite / LocalStack / GCP emulator / Redis / Postgres / Mongo) via
  [docker-compose.test.yml](docker-compose.test.yml)

Design stance, stated in [databaseFactory.py](src/polydb/databaseFactory.py): PolyDB is the *dumb
storage layer*. Business logic, tenant enforcement and model-registry validation belong in the
caller ("UDL"). `PolyDB` the facade class does layer on tenancy/RLS helpers, but the
`DatabaseFactory` underneath deliberately does not.

## Layering

```
PolyDB (facade)                    src/polydb/PolyDB.py
  ├─ DatabaseFactory               CRUD, routing, cache, audit, encryption, retries
  │    └─ EngineConfig[]           multi-engine routing (sql_models / nosql_models per engine)
  └─ CloudDatabaseFactory          provider detection + adapter construction/caching
       └─ adapters/*               concrete provider clients
            └─ base/*              abstract contracts (NoSQLKV, ObjectStorage, Queue, SharedFiles)
```

Key entry points:

| File | Role |
| --- | --- |
| [PolyDB.py](src/polydb/PolyDB.py) | All-in-one facade: CRUD, query, blob, queue, file, cache, metrics, security, tenancy |
| [databaseFactory.py](src/polydb/databaseFactory.py) | Storage layer: multi-engine routing, soft delete, audit fields, retries |
| [cloudDatabaseFactory.py](src/polydb/cloudDatabaseFactory.py) | Provider detection (`CLOUD_PROVIDER` env or credential sniffing) + adapter cache |
| [models.py](src/polydb/models.py) | `CloudProvider` enum and one typed `*Config` per adapter |
| [types.py](src/polydb/types.py) | `ModelMeta` + `SQLAdapter` / `NoSQLKVAdapter` Protocols |

Models are plain classes carrying a `__polydb__` dict (`storage`, `table`/`collection`, `pk_field`,
`rk_field`, `provider`, `cache`, `cache_ttl`); `_extract_meta()` turns that into `ModelMeta`, which
drives SQL-vs-NoSQL routing.

## Features on offer

**Providers** (`CloudProvider`): `azure`, `aws`, `gcp`, `vercel`, `mongodb`, `s3_compatible`,
`postgresql`, `blockchain`.

| Capability | AWS | Azure | GCP | Vercel | Blockchain | Other |
| --- | --- | --- | --- | --- | --- | --- |
| SQL | PostgreSQL | PostgreSQL | PostgreSQL | PostgreSQL | PostgreSQL | PostgreSQL |
| NoSQL KV | DynamoDB | Table Storage | Firestore | Vercel KV | Contract KV | MongoDB / Cosmos-Mongo |
| Object storage | S3 | Blob | GCS | Vercel Blob | IPFS | S3-compatible (MinIO etc.) |
| Shared files | EFS | Azure Files | Filestore | Vercel File | IPFS file | — |
| Queue | SQS | Storage Queue | Pub/Sub | Vercel Queue | Event queue | — |

**SQL is provider-independent.** `CloudDatabaseFactory.get_sql()` always returns
[`PostgreSQLAdapter`](src/polydb/adapters/PostgreSQLAdapter.py) regardless of `CloudProvider` — it
is driven purely by a connection string (`PostgreSQLConfig.connection_string`, else
`POSTGRES_CONNECTION_STRING` / `POSTGRES_URL`). The same adapter therefore serves RDS / Aurora
PostgreSQL, Azure Database for PostgreSQL, Cloud SQL, Neon, Supabase, Vercel Postgres and
self-hosted alike, with pooling, LINQ, JSONB, distributed locks and transactions intact. Only the
NoSQL / storage / queue rows actually branch on provider, so a Postgres-backed app is portable
across clouds with no code and no config change beyond the DSN.

### Transparent large-payload overflow (headline feature)

Every backing store has a hard per-record ceiling — Azure Table 1MB entity / 64KB property,
DynamoDB 400KB item, Firestore 1MB document. PolyDB hides those ceilings: **callers write whatever
size record they have and read it back whole, with no size branching in application code.**

Mechanism — when a payload exceeds the adapter's `max_size`, the adapter writes the full JSON to
the paired object store, keeps a small reference row in the KV store, and rehydrates it on every
read path (`_get_raw`, `query`, `query_linq`, paged reads) before returning to the caller. The
reference carries `_overflow`, `_blob_key`, `_size` and an MD5 `_checksum` that is verified on
retrieval, so a truncated or swapped blob raises rather than returning silent garbage. Scalar
fields are copied onto the reference row (best effort, first 50) so filtering and indexing still
work against overflowed records.

| Adapter | Threshold | Overflow store | Granularity |
| --- | --- | --- | --- |
| [AzureTableStorageAdapter](src/polydb/adapters/AzureTableStorageAdapter.py) | 30KB per property (`MAX_PROPERTY_CHARS`) | Azure Blob (`AZURE_CONTAINER_NAME`, default `overflow`) | **per property** — only oversized columns move out |
| [DynamoDBAdapter](src/polydb/adapters/DynamoDBAdapter.py) | 400KB item (`DYNAMODB_MAX_SIZE`) | S3 (`bucket_name`) | whole item |
| [FirestoreAdapter](src/polydb/adapters/FirestoreAdapter.py) | 1MB doc (`FIRESTORE_MAX_SIZE`) | GCS bucket | whole document |
| [NoSQLKVAdapter](src/polydb/base/NoSQLKVAdapter.py) base | 1MB (`max_size`) | `CloudDatabaseFactory().get_object_storage()` | whole record, `overflow/<md5>.json` |

Azure's is the most refined: it overflows *individual properties* rather than the whole entity, so
a row with one huge JSON column keeps every other column queryable in the table, and
`_restore_overflow_properties` splices the blob contents back into that field on read. Blob keys are
content-addressed (`<pk>_<rk>/<field>/<md5>.json`), making rewrites idempotent.

When touching any NoSQL adapter, **the overflow round-trip is the invariant to preserve**: any new
read path must funnel through the adapter's `_resolve_overflow` / `_restore_overflow_properties`,
and any new write path through `_maybe_overflow_to_s3` / `_maybe_store_overflow` / the per-property
check in `_put_raw`. A read path that forgets it returns reference stubs to the caller instead of
data.

**Cross-cutting:**

- **Query** — LINQ-style [`QueryBuilder`](src/polydb/query.py) (`where/order_by/skip/take/select/
  group_by/distinct/count`) compiling to parameterized SQL *and* NoSQL filters; escaped `LIKE`
  patterns and mapping-value rejection guard against injection.
  [`AdvancedQueryBuilder`](src/polydb/advanced_query.py) adds joins, aggregates, `HAVING`.
- **Paging** — cursor paging via `PageRequest`/`PageResult` and `BackendCapabilities`.
- **Multi-engine routing** — several `EngineConfig`s at once, per-model or per-call
  (`EngineOverride`) selection, with `is_default_sql` / `is_default_nosql`.
- **Cache** — [`RedisCacheEngine`](src/polydb/cache.py) with TTL, stats, invalidation strategies,
  `CacheWarmer`, plus zset helpers used for rate-limit style workloads.
- **Audit** — [audit/](src/polydb/audit/) hash-chained, HMAC-signed records (`POLYDB_AUDIT_HMAC_KEY`),
  canonical payload serialization, `verify_chain()` / `verify_chain_detailed()` tamper detection,
  `AuditContext` for actor propagation.
- **Security** — [security.py](src/polydb/security.py): AES-256-GCM `FieldEncryption` with key ids
  and rotation (`POLYDB_ENCRYPTION_KEY`, `POLYDB_ENCRYPTION_KEYS`, `POLYDB_ENCRYPTION_KEY_ID`; keys
  are never generated in-process), `DataMasking`, `RowLevelSecurity` with tenant/role/ownership/
  sensitivity/time policies.
- **Multitenancy** — [multitenancy.py](src/polydb/multitenancy.py): registry, contextvar tenant
  scope, isolation levels (shared table / schema / database), quota manager, provision/deprovision.
- **Observability** — [monitoring.py](src/polydb/monitoring.py) metrics collector with aggregation,
  Prometheus export and health checks; [observability/logging.py](src/polydb/observability/logging.py)
  structured JSON logging with request context.
- **Reliability** — tenacity-based retries with a non-retryable classifier
  ([retry.py](src/polydb/retry.py)), unique-violation parsing, soft delete, `_inject_audit_fields`.
- **Batch & schema** — [batch.py](src/polydb/batch.py) bulk insert/update/delete + transactions;
  [schema.py](src/polydb/schema.py) `SchemaBuilder` DDL and a `MigrationManager` with rollback.
- **Validation** — [validation.py](src/polydb/validation.py) model/schema validation of `__polydb__`.

## Conventions

- Adapter files are `PascalCase.py` matching the class; core modules are lowercase. Keep it.
- Every adapter takes a typed `*Config` from `models.py`; add new config classes there, then wire
  the branch in the matching `CloudDatabaseFactory.get_*` method.
- Provider SDKs are imported **inside** the factory branch, not at module top level, so a user who
  installed only one extra doesn't pay for the rest. Preserve that.
- Never interpolate identifiers into SQL — use `psycopg2.sql` composition and the
  `validate_table_name` / `validate_column_name` helpers in [utils.py](src/polydb/utils.py).
- Formatting: black + isort, line length 100 (`pyproject.toml`).

## Working in this repo

```bash
uv sync                            # or: pip install -e ".[all,dev,test]"
docker compose -f docker-compose.test.yml up -d
pytest -m postgresql               # markers: postgresql mongodb azure aws gcp vercel multi_engine slow
black src tests && isort src tests && flake8 src
python -m build                    # dist/ artifacts; twine upload dist/*
```

See [BUILD_GUIDE.md](BUILD_GUIDE.md) and [Readme_Integration_Tests.md](Readme_Integration_Tests.md).

### Release checklist (do this before every commit that changes behaviour)

1. Update this file if the change alters architecture, features, or conventions.
2. Bump `version` in [pyproject.toml](pyproject.toml) **and** `__version__` in
   [src/polydb/__init__.py](src/polydb/__init__.py) — they must match.
3. Run black/isort and the relevant test markers.

## Recent changes

- **2.5.6** — Real, per-backend `extend`/`delay`/`cancel` queue operations
  ([QueueAdapter.py](src/polydb/base/QueueAdapter.py) base contract), implemented honestly per
  adapter rather than faked uniformly — each backend only gets the operations its own real API
  supports:
  - **SQS** ([SQSAdapter.py](src/polydb/adapters/SQSAdapter.py)): `extend` via
    `change_message_visibility`; `delay` via `send_message`'s own `DelaySeconds` (capped at SQS's
    real 0–900s ceiling). No `cancel` — a delayed SQS message has no `ReceiptHandle` to cancel
    with until it's actually received.
  - **Azure Queue** ([AzureQueueAdapter.py](src/polydb/adapters/AzureQueueAdapter.py)): all three
    real — `extend` via `update_message`, `delay` via `send_message`'s own `visibility_timeout`,
    `cancel` via the existing `delete()` (the delay call's own returned receipt is what makes a
    still-invisible message cancellable).
  - **GCP Pub/Sub** ([GCPPubSubAdapter.py](src/polydb/adapters/GCPPubSubAdapter.py)): `extend`
    only, via `modify_ack_deadline` (capped at Pub/Sub's real 600s `MAX_ACK_DEADLINE_SECONDS`). No
    `delay`/`cancel` — Pub/Sub has no delayed-publish primitive.
  - **RabbitMQ** ([RabbitMQAdapter.py](src/polydb/adapters/RabbitMQAdapter.py)): `delay`/`cancel`
    via a real TTL + dead-letter-exchange pattern (`_ensure_delay_queue` declares a
    `{queue}.delay.{seconds}` queue with `x-message-ttl`/`x-dead-letter-exchange=""`/
    `x-dead-letter-routing-key={queue}`, so the delayed message dead-letters back into the real
    queue once its TTL expires). No `extend` — AMQP has no renewable per-message visibility timer.
    `cancel(message_id, queue_name, *, delay_seconds=...)` is deliberately WIDER than the base
    `QueueAdapter` contract (an extra required keyword) because finding the right delay queue to
    scan needs to know the original delay; it drains up to `MAX_CANCEL_SCAN` (10,000) messages via
    bounded `basic_get`, acking the one matching `message_id` (dropping it) and nacking
    (`requeue=True`) everything else to preserve their own scheduling.
  - Every adapter that doesn't support an operation still raises `NotImplementedError`, named,
    never silently no-ops or fakes success.
  - `databaseFactory.py` gained a module-level, config-driven cap on `receive_queue`'s own
    `max_messages` — `QUEUE_RECEIVE_MAX_MESSAGES_CAP` (env var `POLYDB_QUEUE_RECEIVE_MAX_MESSAGES`,
    default 1000), clamped via `min(max_messages, QUEUE_RECEIVE_MAX_MESSAGES_CAP)` before the real
    adapter call — previously unbounded, relying purely on real queue depth. Also added
    `extend_queue`/`delay_queue`/`cancel_queue` wrapper methods on `DatabaseFactory`, following the
    exact existing `get_queue(adapter_name).method(...)` pattern every other queue method already
    uses; `cancel_queue` accepts `**kwargs` to conditionally forward RabbitMQ's own `delay_seconds`.
  - Also syncs `__version__` with `pyproject.toml` (2.5.6).

- **2.5.5** — Azure queue `DEFAULT_VISIBILITY_TIMEOUT` raised from 300s to 3600s and made
  overridable via `POLYDB_QUEUE_VISIBILITY_TIMEOUT`
  ([AzureQueueAdapter.py](src/polydb/adapters/AzureQueueAdapter.py)). The old 300s was shorter than
  the platform's 3600s durable-task timeout, so any task running past five minutes was redelivered
  while still executing, producing concurrent duplicate runs of the same job id (upstream
  de-duplication cannot help — a redelivery carries the same run id). Trade-off accepted: a hard
  worker crash now leaves a message invisible for up to an hour. Also syncs `__version__` with
  `pyproject.toml`, which had drifted (2.2.3 vs 2.5.4).

## Known gaps (test coverage excluded)

Ordered roughly by impact. None of these are in-flight; treat as a backlog.

1. **`PolyDB` is not exported from the package root.** [__init__.py](src/polydb/__init__.py)
   exports the two factories but not the facade the docs call "the primary developer-facing
   entrypoint" — users must `from polydb.PolyDB import PolyDB`. Also unexported: `QueryHelper`,
   `AdvancedQueryBuilder`, `EngineConfig`, `EngineOverride`, `TenantConfig`, `SchemaBuilder`,
   `MetricsCollector`, `FieldEncryption`, `PageRequest`/`PageResult`.
2. **`decorators.py` is a 1040-line orphaned duplicate of `databaseFactory.py`.** Nothing imports
   it; it defines a second, older `DatabaseFactory`/`EngineConfig`. Delete it — it is a live trap
   for anyone grepping for `class DatabaseFactory`.
3. **Packaging: the optional-extras design is defeated by the core `dependencies` list.** boto3,
   five azure-* packages, four google-cloud-* packages, pymongo, web3 and ipfshttpclient are all
   *required*, so `pip install altcodepro-polydb-python` pulls every cloud SDK and the extras are
   decorative. `build` and `twine` are also listed as runtime dependencies. Core should be
   psycopg2-binary + tenacity + python-dotenv (+ redis).
4. **Python-version metadata is inconsistent.** `requires-python = ">=3.11"` vs classifiers
   advertising 3.8–3.10, `[tool.mypy] python_version = "3.8"`, and black `target-version` py38+.
   Pick 3.11 everywhere.
5. **`ModelRegistry` ([registry.py](src/polydb/registry.py)) is dead code** — defined, documented,
   never imported. Either wire it into `_extract_meta()` (it is the only path that supports
   `register_dynamic()` schema-driven models) or drop it.
6. **`CloudDatabaseFactory` caches adapters by name only.** `get_nosql_kv(partition_config=X,
   name="kv")` followed by `get_nosql_kv(partition_config=Y, name="kv")` silently returns the
   first adapter with partition config X. Cache key should include the partition config.
7. **`get_nosql_kv` falls through to MongoDB for unmatched providers.** With provider
   `POSTGRESQL` or `S3_COMPATIBLE` it constructs a `MongoDBAdapter` with an empty URI, which fails
   obscurely later. Harmless for the common SQL-only case (those apps only call `get_sql()`), but a
   `POSTGRESQL` app that adds a KV model gets a Mongo connection error instead of
   `UnsupportedStorageTypeError`. (`table_name` in the Azure branch is also assigned and never
   used.)
8. **No CI.** No `.github/workflows` — no lint, type-check, test, or publish automation, and no
   dependency/secret scanning on a repo that ships credential-handling code.
9. **No async API.** Everything is synchronous (`ComplianceService` is the lone `async def`), which
   rules out FastAPI/asyncio callers except via thread pools. A documented stance ("sync only, wrap
   in `run_in_executor`") would at least set expectations.
10. **Two competing pytest configs.** Both `pytest.ini` and `[tool.pytest.ini_options]` exist with
    different `addopts`; `pytest.ini` wins, so the coverage flags in `pyproject.toml` never apply.
11. **Docs drift.** [README.md](README.md)'s "Project Structure" describes `adapters/aws/`,
    `core/`, `security/` package directories that do not exist, and BUILD_GUIDE.md lists
    `database.py` / `factory.py`. Neither documents the `PolyDB` facade or the env-var contract
    (`POLYDB_ENCRYPTION_KEY*`, `POLYDB_AUDIT_HMAC_KEY`, `POLYDB_SLOW_QUERY_MS`,
    `POLYDB_QUEUE_VISIBILITY_TIMEOUT`, `REDIS_CACHE_URL`, `CLOUD_PROVIDER`).
12. **Open-source hygiene.** MIT LICENSE is present, but there is no CONTRIBUTING.md, CHANGELOG.md,
    SECURITY.md, issue/PR templates, or code of conduct, and no published API reference.
13. **Typo in extra name:** `bolckchain` should be `blockchain` (rename, keeping the old key as an
    alias for one release).
14. **Overflow is not uniform across adapters.** Azure Table, DynamoDB and Firestore each
    implement it independently; **MongoDB, Vercel KV and Blockchain KV have no overflow at all**, so
    a document over Mongo's 16MB BSON limit or past Vercel KV's value cap fails at the driver
    instead of spilling to blob. The base-class `_check_overflow` / `_retrieve_overflow` exist to be
    that shared path — wiring the remaining three to it (or documenting them as size-limited) would
    make "users don't worry about size" true everywhere.
15. **The base class's `put()` never calls `_check_overflow`.** `NoSQLKVAdapter.put()` goes straight
    to `_put_raw`; only `patch()` checks. Every adapter that relies on the base write path (Vercel
    KV, MongoDB) therefore has a dead overflow facility, and `self.max_size` is unused on the write
    side for Azure too — Azure branches on its own hard-coded `MAX_PROPERTY_CHARS = 30 * 1024`
    instead, while `AZURE_TABLE_MAX_SIZE = 60 * 1024` is set on `self.max_size` and never consulted.
    Two different thresholds, neither of them the one in the comment (which says "1MB").
16. **Overflow blobs are never garbage-collected.** Content-addressed keys mean an update writes a
    new blob and orphans the old one; `delete` removes the reference row but the blob stays. There
    is no sweeper and no lifecycle-policy guidance, so overflow storage grows without bound.
17. **Repo hygiene:** `combine_code.py`, `extract_architecture.py`, `architecture/`, `token.txt`
    and a checked-in `dist/` are dev scratch in the project root. `.env`/`token.txt` are correctly
    gitignored and untracked — keep it that way.
