# CLAUDE.md — altcodepro-polydb-python (PolyDB)

Guidance for Claude Code when working in this repository. This file is periodically re-verified
against the actual source (not the other way around) — if code and this file disagree, trust the
code and fix this file.

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
- Repo: https://github.com/AltEquityPro/altcodepro-polydb-python
- ~15.7k LOC across 68 `.py` files (6 `__init__.py`) under `src/polydb/`; integration tests in
  [tests/](tests/) run against emulators (Azurite / LocalStack / GCP emulator / Redis / Postgres /
  Mongo) via [docker-compose.test.yml](docker-compose.test.yml)

**Version drift, live right now**: `pyproject.toml`'s `version` is `2.5.11` but
`src/polydb/__init__.py`'s `__version__` is `2.5.14` — they don't match. This violates the Release
checklist below and will fail `.github/workflows/publish.yml`'s own version-match gate on the next
push to `main` that touches `pyproject.toml`. Reconcile them (bump `pyproject.toml` to `2.5.14`,
or whichever is actually correct) before relying on either number.

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
| [models.py](src/polydb/models.py) | `CloudProvider` enum, one typed `*Config` per adapter, plus `BackendCapabilities`/`PageRequest`/`PageResult` |
| [types.py](src/polydb/types.py) | `ModelMeta` + `SQLAdapter` / `NoSQLKVAdapter` Protocols |
| [aio.py](src/polydb/aio.py) | `AsyncPolyDB` / `AsyncDatabaseFactory` -- thread-pool-backed async wrappers over the sync `PolyDB`/`DatabaseFactory` |

Models are plain classes carrying a `__polydb__` dict (`storage`, `table`/`collection`, `pk_field`,
`rk_field`, `provider`, `cache`, `cache_ttl`); `_extract_meta()` turns that into `ModelMeta`, which
drives SQL-vs-NoSQL routing.

`from polydb import ...` (package root, `src/polydb/__init__.py`) exports: `PolyDB`, `AsyncPolyDB`,
`AsyncDatabaseFactory`, `DatabaseFactory`, `CloudDatabaseFactory`, `CloudProvider`,
`PartitionConfig`, `MongoConfig`, `CosmosMongoConfig`, `QueryBuilder`, `Operator`, `AuditContext`,
`CacheEngine`, `sweep_overflow_blobs`/`GCReport`, and the error classes. **Not** exported (import
from their own modules instead): `QueryHelper`, `AdvancedQueryBuilder`, `EngineConfig`,
`EngineOverride`, `TenantConfig`, `SchemaBuilder`, `MetricsCollector`, `FieldEncryption`,
`PageRequest`/`PageResult`, `BackendCapabilities`.

There is also a `src/polydb/services/` package (`compliance_service.py`'s `ComplianceService`,
`security_service.py`'s `SecurityService`) that nothing in `src/polydb` or `tests/` imports outside
`services/__init__.py` itself — real, sizable, but completely unwired and untested. See Known gaps.

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
across clouds with no code and no config change beyond the DSN. `get_nosql_kv()` branches AZURE →
`AzureTableStorageAdapter`, AWS → `DynamoDBAdapter`, GCP → `FirestoreAdapter`, VERCEL →
`VercelKVAdapter`, BLOCKCHAIN → `BlockchainKVAdapter`, MONGODB → `MongoDBAdapter`; every other
provider (POSTGRESQL, S3_COMPATIBLE, VAULT, KAFKA, RABBITMQ) raises `UnsupportedStorageTypeError`
naming the provider and pointing at `get_sql()` for the Postgres case.

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
| [BlockchainKVAdapter](src/polydb/adapters/BlockchainKVAdapter.py) | 8KB (`BLOCKCHAIN_MAX_SIZE`, extends the base) | same `get_object_storage()` path | whole record — a far lower ceiling than the base default given on-chain storage's real per-byte cost |

Azure's is the most refined: it overflows *individual properties* rather than the whole entity, so
a row with one huge JSON column keeps every other column queryable in the table, and
`_restore_overflow_properties` splices the blob contents back into that field on read. Blob keys are
content-addressed (`<pk>_<rk>/<field>/<md5>.json`), making rewrites idempotent. Its per-property
threshold is a hardcoded constant, independent of `self.max_size` — see Known gaps.

MongoDB and Vercel KV take the base `NoSQLKVAdapter` row as-is (no override) — both `put()` and
`patch()` funnel through `_check_overflow`, so this is a real, uniform guarantee on every write
path, not just `patch()`. Overflow blobs are content-addressed and never rewritten in place, only
ever written fresh and orphaned by later updates — [`overflow_gc.py`](src/polydb/overflow_gc.py)'s
`sweep_overflow_blobs()` reclaims them (mark-and-sweep, with a grace window).

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
- Never interpolate a raw, uncontrolled identifier into SQL. The actual, dominant pattern in
  `PostgreSQLAdapter.py`'s `insert`/`select`/`update`/`delete`/`query_linq` is: validate the
  table/column name through `validate_table_name`/`validate_column_name` in
  [utils.py](src/polydb/utils.py) first, then f-string-interpolate the now-validated identifier.
  `psycopg2.sql.Identifier` composition is used only for the `SAVEPOINT`/`ROLLBACK`/`RELEASE`
  statements, not as the general mechanism — don't assume it's used everywhere.
- Formatting: black + isort, line length 100 (`pyproject.toml`).

## Working in this repo

```bash
uv sync                            # or: pip install -e ".[all,dev,test]"
docker compose -f docker-compose.test.yml up -d
pytest -m postgresql               # markers: postgresql mongodb azure aws gcp vercel multi_engine slow
black src tests && isort src tests && flake8 src
python -m build                    # dist/ artifacts; only for local inspection now --
                                    # publishing itself is automated, see below
```

**Publishing is automated**: [`.github/workflows/publish.yml`](.github/workflows/publish.yml)
builds and publishes to PyPI automatically whenever `pyproject.toml`'s own `version` changes on a
push to `main` (via PyPI Trusted Publishing/OIDC -- no token secret in this repo), gated on
`__version__` matching `version` first. Bump `version` + `__version__` together (Release checklist
below), push, and the publish happens on its own -- no manual `python -m build`/`twine upload`
needed anymore. (See the version-drift note above the Release checklist is currently violated.)

See [BUILD_GUIDE.md](BUILD_GUIDE.md) and [Readme_Integration_Tests.md](Readme_Integration_Tests.md)
— both have known drift from the real source tree, see Known gaps.

### Release checklist (do this before every commit that changes behaviour)

1. Update this file if the change alters architecture, features, or conventions.
2. Bump `version` in [pyproject.toml](pyproject.toml) **and** `__version__` in
   [src/polydb/__init__.py](src/polydb/__init__.py) — they must match.
3. Run black/isort and the relevant test markers.

## Recent changes

- **2.5.14** — `PolyDB`/`AsyncPolyDB`/`AsyncDatabaseFactory` exported from the package root. New
  [`aio.py`](src/polydb/aio.py): `AsyncPolyDB`/`AsyncDatabaseFactory` wrap the unchanged sync
  `PolyDB`/`DatabaseFactory` via `asyncio.to_thread`/`ThreadPoolExecutor` (no native async rewrite
  of the adapters — see Known gaps). Fixed `PostgreSQLAdapter._serialize_value` to match
  `_serialize_param`'s list/tuple/dict handling — it previously `Json()`-wrapped every list/tuple
  unconditionally, which broke writes to real Postgres array (`TEXT[]`) columns. New
  [`.github/workflows/publish.yml`](.github/workflows/publish.yml) (see "Working in this repo").
- **2.5.13** — CI's `security` job's first real triage: `pip-audit --skip-editable` made blocking
  (no `|| true`); new [`.gitleaksignore`](.gitleaksignore) allowlists two reviewed findings
  (Azurite's documented default emulator key, an entropy false-positive in `VaultAdapter.py`).
- **2.5.12** — Added `.github/workflows/ci.yml` (`lint` blocking on black/isort/flake8, `test`
  against Postgres/Mongo/Redis containers). Deleted unused `decorators.py`. Fixed
  `VercelQueueAdapter` to extend `QueueAdapter(ABC)` (it previously stood outside the hierarchy, so
  unsupported ops raised a bare `AttributeError` instead of a named `NotImplementedError`). Fixed
  `HealthCheck.check_cache_health()` to actually compare the round-tripped value instead of only
  checking that `set()`/`get()` didn't raise.
- **2.5.11** — `NoSQLKVAdapter.put()` now calls `_check_overflow()` (previously only `patch()`
  did — `put()` on Mongo/Vercel KV had a dead overflow guarantee). `BlockchainKVAdapter` now
  extends `NoSQLKVAdapter` with its own 8KB ceiling. `CloudDatabaseFactory.get_nosql_kv()`'s
  adapter cache key now includes `partition_config` (previously keyed by name alone, silently
  dropping a second caller's config); unmatched providers now raise `UnsupportedStorageTypeError`
  instead of falling through to a broken `MongoDBAdapter("", "")`. New
  [`overflow_gc.py`](src/polydb/overflow_gc.py) sweeps orphaned overflow blobs.
- **2.5.9** — Fixed `AzureTableStorageAdapter._put_raw()` to persist a real `"id"` property on
  create when the caller's payload doesn't supply one (previously only synthesized onto the return
  value, never written) — the missing property broke every later `id`-filtered lookup and caused
  phantom `PartitionKey="None"` rows.
- **2.5.8** — Fixed `_get_table_name()`'s fallback chain to also check `__polydb__["table"]` (it
  only checked `collection`/`collection_name`), fixing shared-table collisions for
  compiler-synthesized models that use the `table` key.
- **2.5.7** — Fixed `DatabaseFactory.update()`/`delete()` to fall back to `NoSQLKVAdapter`'s real
  default (`tenant_id`/`id`) when a model declares no `pk_field`/`rk_field`, instead of leaving the
  key `None` and silently operating on a phantom `PartitionKey="None"` row.
- **2.5.6** — Real, per-backend `extend`/`delay`/`cancel` queue operations
  ([QueueAdapter.py](src/polydb/base/QueueAdapter.py) base contract) — each backend gets only the
  operations its real API supports (SQS: extend + delay; Azure Queue: all three; GCP Pub/Sub:
  extend only; RabbitMQ: delay + cancel via TTL + dead-letter-exchange). Unsupported ops raise a
  named `NotImplementedError`, never a silent no-op. Added a config-driven cap on `receive_queue`'s
  `max_messages` (`POLYDB_QUEUE_RECEIVE_MAX_MESSAGES`, default 1000).
- **2.5.5** — Azure queue `DEFAULT_VISIBILITY_TIMEOUT` raised 300s → 3600s, overridable via
  `POLYDB_QUEUE_VISIBILITY_TIMEOUT` (300s was shorter than the platform's own durable-task timeout,
  causing duplicate concurrent redeliveries of a still-running task).

## Known gaps (test coverage excluded)

Ordered roughly by impact. None of these are in-flight; treat as a backlog.

1. **Partial package-root export.** See the export list under "Layering" above — `QueryHelper`,
   `AdvancedQueryBuilder`, `EngineConfig`, `EngineOverride`, `TenantConfig`, `SchemaBuilder`,
   `MetricsCollector`, `FieldEncryption`, `PageRequest`/`PageResult`, `BackendCapabilities` are
   still only importable from their own modules.
2. **Python-version metadata is inconsistent.** `requires-python = ">=3.11"` vs classifiers
   advertising 3.8–3.10, `[tool.mypy] python_version = "3.8"`, and black `target-version` py38+.
   Pick 3.11 everywhere. (This is also why `.github/workflows/ci.yml`'s own mypy step is
   report-only rather than blocking.)
3. **`ModelRegistry` ([registry.py](src/polydb/registry.py)) is dead code** — defined, documented,
   never imported. Either wire it into `_extract_meta()` (it is the only path that supports
   `register_dynamic()` schema-driven models) or drop it.
4. **`src/polydb/services/` is dead code too** — `ComplianceService`/`SecurityService`, real and
   sizable, never imported outside `services/__init__.py`, never tested, never wired into
   `DatabaseFactory`/`PolyDB`. Either integrate it or remove it.
5. **Two competing pytest configs.** Both `pytest.ini` and `[tool.pytest.ini_options]` exist with
   different `addopts`; `pytest.ini` wins, so the coverage flags in `pyproject.toml` never apply.
6. **Docs drift.** [README.md](README.md)'s "Project Structure" describes `adapters/aws/`, `core/`,
   `security/`, `monitoring/`, `cache/`, `multitenancy/` package directories that do not exist.
   [BUILD_GUIDE.md](BUILD_GUIDE.md) lists `database.py`/`decorators.py` (deleted in 2.5.12)/
   `factory.py`. Neither documents the `PolyDB` facade or the env-var contract
   (`POLYDB_ENCRYPTION_KEY*`, `POLYDB_AUDIT_HMAC_KEY`, `POLYDB_SLOW_QUERY_MS`,
   `POLYDB_QUEUE_VISIBILITY_TIMEOUT`, `REDIS_CACHE_URL`, `CLOUD_PROVIDER`). Also:
   `Readme_Integration_Tests.md` documents copying `tests/.env.test` for the local emulator ports,
   but that file doesn't exist in the repo — `.github/workflows/ci.yml`'s `test` job sets the
   equivalent env vars directly instead of depending on it.
7. **Open-source hygiene.** MIT LICENSE is present, but there is no CONTRIBUTING.md, CHANGELOG.md,
   SECURITY.md, issue/PR templates, or code of conduct, and no published API reference.
8. **Azure Table's overflow threshold still doesn't consult `self.max_size`.** Azure branches on
   its own hard-coded `MAX_PROPERTY_CHARS = 30 * 1024` instead; `AZURE_TABLE_MAX_SIZE = 60 * 1024`
   is set on `self.max_size` and never read. Two different thresholds, neither the one in the base
   class's own comment (which says "1MB"). This is Azure's per-property granularity being on a
   separate code path from the base class by design, just not yet unified on one configured value.
9. **Repo hygiene:** `combine_code.py`, `extract_architecture.py`, and `architecture/` are still
   dev scratch in the project root (`token.txt` and a checked-in `dist/` are no longer present —
   already cleaned up since this was last a gap).
10. **`tests/test_multi_engine.py::TestSingleEngine` is broken test-suite drift, excluded from CI.**
    Its own `_patch_factory` helper calls `db._meta(...)`/`db._model_type(...)` — neither exists on
    `DatabaseFactory`; meta extraction is the module-level `_extract_meta()` function, not an
    instance method, so every test in that class fails with a plain `AttributeError`. Pre-existing,
    not something CI introduced; `.github/workflows/ci.yml`'s `test` job excludes the
    `multi_engine` marker entirely until this is fixed for real.
