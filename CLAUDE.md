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
| [aio.py](src/polydb/aio.py) | `AsyncPolyDB` / `AsyncDatabaseFactory` -- thread-pool-backed async wrappers over the sync `PolyDB`/`DatabaseFactory` |

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
| [BlockchainKVAdapter](src/polydb/adapters/BlockchainKVAdapter.py) | 8KB (`BLOCKCHAIN_MAX_SIZE`, extends the base) | same `get_object_storage()` path | whole record — a far lower ceiling than the base default given on-chain storage's real per-byte cost |

Azure's is the most refined: it overflows *individual properties* rather than the whole entity, so
a row with one huge JSON column keeps every other column queryable in the table, and
`_restore_overflow_properties` splices the blob contents back into that field on read. Blob keys are
content-addressed (`<pk>_<rk>/<field>/<md5>.json`), making rewrites idempotent.

MongoDB and Vercel KV take the base `NoSQLKVAdapter` row as-is (no override) — since 2.5.11 both
`put()` and `patch()` funnel through `_check_overflow`, so this is a real, uniform guarantee on
every write path, not just `patch()`. Overflow blobs are content-addressed and never rewritten in
place, only ever written fresh and orphaned by later updates — see `overflow_gc.py` (2.5.11) for
the sweeper that reclaims them.

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
python -m build                    # dist/ artifacts; only for local inspection now --
                                    # publishing itself is automated, see below
```

**Publishing is automated** (2.5.14): [`.github/workflows/publish.yml`](.github/workflows/publish.yml)
builds and publishes to PyPI automatically whenever `pyproject.toml`'s own `version` changes on a
push to `main` (via PyPI Trusted Publishing/OIDC -- no token secret in this repo). Bump `version` +
`__version__` together (Release checklist above), push, and the publish happens on its own -- no
manual `python -m build`/`twine upload` needed anymore.

See [BUILD_GUIDE.md](BUILD_GUIDE.md) and [Readme_Integration_Tests.md](Readme_Integration_Tests.md).

### Release checklist (do this before every commit that changes behaviour)

1. Update this file if the change alters architecture, features, or conventions.
2. Bump `version` in [pyproject.toml](pyproject.toml) **and** `__version__` in
   [src/polydb/__init__.py](src/polydb/__init__.py) — they must match.
3. Run black/isort and the relevant test markers.

## Recent changes

- **2.5.14** — Three items, closing known-gaps #1 and #5, plus a real reproduced adapter bug found
  while testing #5:
  - **`PolyDB` exported from the package root**, closing known-gap #1 — `from polydb import PolyDB`
    now works (previously `from polydb.PolyDB import PolyDB` only). `AsyncPolyDB`/
    `AsyncDatabaseFactory` (next bullet) are exported alongside it. The rest of known-gap #1's own
    list (`QueryHelper`, `AdvancedQueryBuilder`, `EngineConfig`, `EngineOverride`, `TenantConfig`,
    `SchemaBuilder`, `MetricsCollector`, `FieldEncryption`, `PageRequest`/`PageResult`) is still
    unexported — narrower, deliberate scope this round, not a claim the whole gap is closed.
  - **Async API, closing known-gap #5** — new [`aio.py`](src/polydb/aio.py): `AsyncPolyDB`/
    `AsyncDatabaseFactory` wrap the real, unchanged, synchronous `PolyDB`/`DatabaseFactory` and run
    every one of their methods via `asyncio.to_thread` (or a dedicated `ThreadPoolExecutor` when
    `max_workers` is given), returning an awaitable — the same "wrap a blocking DB-API driver in a
    thread pool" pattern `encode/databases` and Starlette's own docs recommend, not a novel trick.
    **The documented stance, stated plainly in that module's own top comment**: a genuinely native
    async rewrite (asyncpg/aioboto3/motor/etc. replacing psycopg2/boto3/pymongo/pika/kafka-python
    across every adapter) would mean a from-scratch, parallel adapter layer roughly doubling this
    package's own maintenance surface — real, much larger, separate future work this round
    deliberately does NOT attempt. What this round DOES deliver: any FastAPI/asyncio caller can use
    PolyDB today without blocking the event loop, via `AsyncPolyDB`/`AsyncDatabaseFactory`, zero
    changes to the existing sync `PolyDB`/`DatabaseFactory`/adapters (every existing synchronous
    caller is completely unaffected). One generic `_AsyncProxy` base class covers both wrapper
    types via `__getattr__` (cached per-attribute) rather than hand-duplicating ~65 method
    signatures from `PolyDB` + ~30 from `DatabaseFactory`; non-callable attributes pass through
    synchronously (never wrapped/awaited). Proven end to end against real Postgres in
    `tests/test_async_api.py` (10 tests) — not just that the wrapper's own dispatch logic looks
    right in isolation: a real `create`/`read`/`update`/`delete` round trip through both wrapper
    types, wrapping an already-constructed sync instance vs. building one internally, non-callable
    pass-through, a real thread-identity proof that calls genuinely run off the event-loop's own
    thread, a dedicated-executor lifecycle proof (`close()`/`async with`), and a real wall-clock
    timing proof that N concurrent awaited calls (each with an injected `time.sleep`) complete in
    roughly one sleep's worth of time, not N sleeps -- proving genuine overlap, not serialization
    behind a hidden lock.
  - **Real, reproduced bug found and fixed while writing `tests/test_async_api.py`'s own fixtures**
    (unrelated to async itself -- surfaced by routine use of the existing `tests/test_postgresql.py`
    fixtures, `git stash` confirms it pre-dates this round): `PostgreSQLAdapter._serialize_value`
    (used by `insert`/`update`/`upsert`) unconditionally `Json()`-wrapped every `list`/`tuple`
    value, turning a plain Python list meant for a real Postgres `TEXT[]` column into a JSON string
    literal (`'["x","y"]'`) that Postgres correctly refuses ("malformed array literal") -- meaning
    `insert`/`update`/`upsert` could never actually write a real array column at all, only JSONB.
    The sibling `_serialize_param` (used for query parameters, a few lines below in the same file)
    already had the correct logic -- pass a bare list of scalars through untouched (psycopg2 adapts
    it to a real Postgres array natively), only `Json()`-wrap a list that itself contains dicts (no
    Postgres array type holds JSON objects as elements) -- `_serialize_value` just never matched it.
    Fixed by aligning `_serialize_value` with `_serialize_param`'s exact logic. Verified via
    `tests/test_postgresql.py::TestInsert::test_insert_text_array_column`, which existed already
    and was failing before this fix (confirmed via `git stash`), now passes; the full `pytest -m
    postgresql` suite (69 tests including the 10 new async ones) is green.
  - New [`.github/workflows/publish.yml`](.github/workflows/publish.yml) automates PyPI publishing
    on every version bump, closing the maintainer's own manual "bump pyproject.toml, then
    build+twine-upload by hand" workflow. Triggers on any push to `main` touching `pyproject.toml`;
    compares this checkout's own `version` against PyPI's currently-published version (via the
    public, unauthenticated `https://pypi.org/pypi/altcodepro-polydb-python/json` endpoint) rather
    than requiring a git tag -- **no git tags exist in this repo's history**, so a tag-based trigger
    would need a second, separate new habit; this trigger matches the human workflow that already
    exists today, just automated. Uses **PyPI Trusted Publishing (OIDC)** --
    `pypa/gh-action-pypi-publish@release/v1` with `permissions: id-token: write` and an `environment:
    pypi` -- so **no `PYPI_API_TOKEN` secret is stored in this repo at all**; the one-time,
    human-only setup step (documented in the workflow file's own top comment, since no CI job can
    do it) is adding this repo as a trusted publisher on pypi.org under the project's own
    Publishing settings. Also gates on `src/polydb/__init__.py`'s `__version__` matching
    `pyproject.toml`'s `version` before ever attempting a build -- this repo's own documented
    "Release checklist" requirement, enforced as a real CI check instead of trusted-by-convention.
    A version-unchanged push (a docs fix, a test-only change) is a correct, fast no-op; re-running
    for an already-published version is idempotent, never double-publishes.

- **2.5.13** — The `security` CI job's first-ever triage pass, closing the "report-only until a
  first pass is triaged" caveat 2.5.12 deliberately left open (see that entry's own `security`
  bullet below):
  - **pip-audit**: `pip-audit --skip-editable` is now genuinely blocking (`|| true` removed). A
    local `pip-audit` run inside this sandbox's own pre-existing, long-lived Python environment
    had shown 27 "vulnerabilities" across httplib2/idna/pip/setuptools/urllib3/wheel — investigated
    one by one rather than blindly flipped to blocking: `httplib2`/`pip`/`setuptools`/`wheel` all
    traced to `/usr/lib/python3/dist-packages`, apt-installed system tooling (`python3-launchpadlib`
    et al.) with zero relationship to this repo's own dependency tree — confirmed via `pip show
    --files`/`Required-by` that nothing this repo (or its `[all]` extra) actually depends on pulls
    in `httplib2` at all. `idna`/`urllib3` were real, but stale copies left in a shared
    `~/.local/site-packages` from unrelated earlier work in that same long-lived environment, not
    what this repo's own `pyproject.toml` floors resolve to fresh. Proven by re-running the exact
    same audit inside a brand-new, throwaway venv (`python -m venv` + `pip install --upgrade pip
    setuptools wheel` + `pip install -e ".[all,dev,test]"` + `pip-audit`) — genuinely
    **zero findings**, confirming this repo's own dependency floors were never actually vulnerable;
    the CI job now also runs `pip install --upgrade pip setuptools wheel` before the audit as cheap
    insurance against a stale runner image repeating the same false alarm.
  - **gitleaks**: new [`.gitleaksignore`](.gitleaksignore) allowlists the only two findings a real,
    full-git-history `gitleaks detect` run turned up, each individually investigated before being
    added (never a blanket suppression): Azurite's own well-known, publicly-documented default
    local-emulator storage account key, which `Readme_Integration_Tests.md`'s own connection-string
    example is supposed to show verbatim (not a credential for any real account), and a plain
    entropy false-positive in `VaultAdapter.py` on the source text `mount_point=self.mount_point`
    (a variable reference, not a secret). `gitleaks/gitleaks-action@v2` itself had no
    `continue-on-error` set even before this pass — it was already failing the job on any
    unallowlisted finding, so "report-only" undersold its actual behavior; this pass is what
    finally reviewed what it would find and gave it a real, intentional pass/fail baseline instead
    of accidentally-blocking-on-whatever-it-happens-to-flag.
  - Both scans verified clean against the current tree with the fixes above in place before calling
    this closed — not asserted from the workflow YAML alone.

- **2.5.12** — Phase 1, first two tier-2 gaps closed:
  - **CI**, closing known-gap 6: `.github/workflows/ci.yml` — `lint` (black/isort blocking, flake8
    blocking, mypy report-only pending the python-version-metadata cleanup in known-gap 4),
    `security` (pip-audit + gitleaks, both report-only until a first pass is triaged), `test`
    (Postgres/Mongo/Redis service containers, `pytest -m "postgresql or mongodb or vercel"`).
    `multi_engine` deliberately excluded — see known-gap 14. Added `.flake8` (flake8 has no native
    `pyproject.toml` support; `max-line-length = 100` + `E203`/`W503`/`E501` ignored to match
    black's own settings and its own line-wrapping authority).
  - **`decorators.py` deleted**, closing known-gap 2 — confirmed nothing imported it first.
  - The repo-wide black/isort/flake8 pass needed to make the new `lint` job start green (not a
    known-broken placeholder) surfaced two real bugs, both fixed and covered by new tests:
    - `VercelQueueAdapter` never extended `QueueAdapter(ABC)` the way every sibling adapter
      (SQS/Azure/RabbitMQ) does, even though `send`/`receive`/`delete` already satisfy every
      abstract method it requires — an unused `from ..base.QueueAdapter import QueueAdapter` was
      the flake8 finding that surfaced it. Meant `nack`/`purge`/`declare`/`status`/`extend`/
      `delay`/`cancel` fell through to a plain `AttributeError` instead of the base's own
      intentional, named `NotImplementedError`, and the class silently opted out of
      `isinstance(adapter, QueueAdapter)` checks. Fixed by extending the base and calling
      `super().__init__()`; `tests/test_vercel.py::TestVercelQueue` (new) proves both.
    - `HealthCheck.check_cache_health()` ([monitoring.py](src/polydb/monitoring.py)) wrote a test
      value into the cache, read it back into `retrieved`, and never actually compared the two —
      `set()`/`get()` not raising is not the same thing as the cache round-tripping the value
      correctly, so a cache silently returning stale/empty/wrong data still reported `{"status":
      "healthy"}`. Fixed by comparing `retrieved` against what was written and reporting
      `unhealthy` with a clear mismatch message on a miss. `tests/test_monitoring_health.py` (new)
      reproduces the pre-fix blind spot with a fake cache that "succeeds" but returns the wrong
      value.
  - Every other flake8 finding (unused imports, a handful of unused local variables that were
    genuinely never read anywhere — not wired-up-but-dead audit scaffolding worth investing in
    right now, forward-referenced return-type annotations flagged `F821` under this repo's own
    deliberate "don't import cloud SDKs at module level" convention, two stray `f""` strings with
    no placeholder, some trailing whitespace inside a SQL literal) was mechanical cleanup with zero
    behavior change, confirmed via a full before/after test-suite diff against this exact commit
    (`git stash`/`pop`) — byte-for-byte identical pass/fail/skip/error counts either side.

- **2.5.11** — Phase 0 hardening: four reproduced gaps in the NoSQL KV layer, all closed together
  since they share the same overflow/adapter-vending code paths.
  - [`NoSQLKVAdapter.put()`](src/polydb/base/NoSQLKVAdapter.py) now calls `_check_overflow()` before
    `_put_raw()`, exactly like `patch()` already did. Previously a plain `create()`-shaped write
    went straight to `_put_raw()` with no size check at all — the base class's own overflow
    facility existed but only `patch()` ever reached it, so any adapter that relies on the base
    write path (`MongoDBAdapter`, `VercelKVAdapter`) had a dead overflow guarantee on `put()`
    specifically, even though the exact same payload overflowed correctly through `patch()`.
    Reproduced and closed in `tests/test_nosql_kv_overflow_write_path.py` (an in-memory
    `NoSQLKVAdapter` subclass proves `put()` and `patch()` now overflow the identical payload
    identically, that the overflowed row round-trips through `query()`, and that a tampered blob
    still raises on checksum mismatch).
  - [`BlockchainKVAdapter`](src/polydb/adapters/BlockchainKVAdapter.py) previously stood entirely
    outside the `NoSQLKVAdapter` hierarchy with zero size guard — an oversized record was sent
    on-chain unmodified, both far more expensive per byte than any off-chain store and, on most EVM
    chains, likely to be rejected outright past the call-data size ceiling before this adapter's own
    logic ever ran. It now extends `NoSQLKVAdapter` (gaining `_check_overflow`/`_retrieve_overflow`
    for free; its own `put`/`get`/`delete`/`query` stay overridden exactly as before, since none of
    them use the base class's pk/rk addressing) with its own much lower ceiling,
    `BLOCKCHAIN_MAX_SIZE = 8 * 1024`, reflecting on-chain storage's real cost profile. Verified in
    `tests/test_blockchain_overflow_guard.py` without a live chain — `object.__new__` bypasses the
    real `__init__`'s Web3/RPC/account setup (none of which the overflow guard depends on) and wires
    in only what `put()`/`get()` actually touch, proving the oversized payload never reaches
    `contract.functions.put()` and that `get()` rehydrates and checksum-verifies correctly.
  - [`CloudDatabaseFactory.get_nosql_kv()`](src/polydb/cloudDatabaseFactory.py)'s adapter cache was
    keyed by `name` alone: a second call with a different `partition_config` under the same `name`
    silently returned the first adapter, the second caller's partition config thrown away with no
    error. The cache key now incorporates a stable string identity of `partition_config`
    (mirroring the existing `object::`/`secrets::` composite-key pattern used by
    `get_object_storage()`/`get_secrets()` in the same file). Also dropped the Azure branch's dead
    `table_name = cfg.table_name` assignment (the adapter's constructor has never accepted a
    `table_name` parameter — table resolution is per-model via `_get_table_name()`, see 2.5.8).
  - `get_nosql_kv()`'s provider branch used to fall through to a bare `else: MongoDBAdapter("", "")`
    for any unmatched `CloudProvider` (`POSTGRESQL`, `S3_COMPATIBLE`, `VAULT`, `KAFKA`, `RABBITMQ`)
    — a Mongo client pointed at an empty URI that failed obscurely on first real use instead of
    here, at construction, with a message naming the actual problem. `MONGODB` is now its own
    explicit `elif` branch and every other provider raises `UnsupportedStorageTypeError` naming the
    provider and pointing at `get_sql()` for the Postgres-only case. Both fixes verified in
    `tests/test_cloud_factory.py`'s new `TestNoSQLKVAdapterCache`/`TestNoSQLKVProviderFallback`
    classes (constructed without any live backend — `VercelKVAdapter` connects lazily, so cache
    identity is provable with no Redis running).
  - New: [`overflow_gc.py`](src/polydb/overflow_gc.py)'s `sweep_overflow_blobs()` closes the
    "overflow blobs are never garbage-collected" gap — content-addressed keys mean an update to an
    already-overflowed record orphans the old blob, and `delete()` only ever removed the reference
    row, never the blob it pointed at, so overflow storage grew without bound for the life of a
    deployment. A conservative two-pass mark-and-sweep: a blob unreferenced by any live row (across
    caller-supplied models, scanned via the adapter's own `_query_raw`) is only a *candidate* the
    first time it's seen, recorded with the wall-clock time in a small state blob written back into
    the same object store (`<prefix>_gc_state.json`); it's only actually deleted once
    `grace_seconds` have elapsed since that first sighting *and* it's still unreferenced on a later
    sweep — giving a real grace window without needing last-modified timestamps, which
    `ObjectStorageAdapter` exposes on no backend. A blob re-referenced between two sweeps (a
    concurrent write racing the sweep) drops out of the candidate list instead of being deleted.
    Covered end to end in `tests/test_overflow_gc.py` with a fake object store and adapter,
    including the re-referenced-between-sweeps and dry-run cases.

- **2.5.9** — Fixed a real, reproduced bug in
  [`AzureTableStorageAdapter._put_raw()`](src/polydb/adapters/AzureTableStorageAdapter.py): a
  create whose own write payload had no `"id"` key never landed a real, queryable `"id"` property
  on the stored entity — `_put_raw()` only ever *synthesized* one onto that one call's own return
  value (`restored["id"] = safe_rk`), never persisted it. This silently broke every later
  id-addressed lookup for such a row: `DatabaseFactory.update()`/`patch()`'s own "before" read
  (`read_one(model, {"id": X, ...})`, the ordinary path every `core.db.update` call in the
  reference engine goes through) builds a real OData `id eq 'X'` filter against whatever property
  is actually named `"id"` on the entity — and since that property never existed, the filter
  matched nothing, `before` came back `None`, and `update()`'s own pk/rk recovery logic (already
  hardened once in 2.5.7 for the "no declared pk_field" case) had nothing left to recover from,
  falling all the way through to a literal `str(None) == "None"` PartitionKey — the identical
  phantom-row symptom 2.5.7 fixed for a different trigger, now reproduced for a genuinely new one.
  This affects **any** model whose caller doesn't explicitly set `"id"` at create time, not just
  one with a custom `pk_field`/`rk_field` mapping — a downstream, real-world case:
  `altcodepro-blueprint-engine`'s own `users` model was just given a deterministic
  `pk_field=rk_field="identity_key"` (to fix a separate, previously-reported duplicate-row bug —
  the same real email/phone now upserts onto one physical row instead of minting a fresh one every
  signup), and its own `signup_password`/`verify_otp_email`/`verify_otp_phone` workflows'
  `link_default_sub`/`link_default_sub_if_new` steps (`core.db.update(model='users', id=
  steps.create_user.id, ...)`) hit exactly this gap the very first time they ran against a real
  deployment — confirmed directly against a real Azure Table dump: every real user row had a
  second, phantom sibling at `PartitionKey="None"`. Fixed by having `_put_raw()` stamp
  `data["id"] = safe_rk` onto the payload it actually persists (mirroring `_query_raw()`'s own
  read-side synthesis, `out["id"] = ent_dict["RowKey"]` when absent, but on the WRITE side, which
  that function alone could never fix) whenever the caller's own data doesn't already supply one —
  a create that already sets `"id"` itself is left completely untouched. Reproduced before
  trusting the fix, per this repo's own standard: a real, filter-evaluating fake `TableClient`
  (`tests/test_azure_table_id_property_persisted.py`) confirms the pre-fix code never lands a real
  `"id"` property (a `KeyError` on the stored entity, not just a failed downstream lookup) and that
  a subsequent `read_one`-shaped `id`-filtered query always returns nothing; the fix closes both,
  proves two different real users' rows never collide, and proves a *repeat* `_put_raw()` call for
  the identical deterministic key genuinely upserts the one physical row rather than minting a
  second — the actual, originally-reported symptom this whole chain of fixes (2.5.7 → 2.5.8 →
  2.5.9) exists to close.

- **2.5.8** — Fixed a real, reproduced shared-Azure-table bug in
  [`AzureTableStorageAdapter._get_table_name()`](src/polydb/adapters/AzureTableStorageAdapter.py):
  the method's fallback chain checked, in order, `model.__udl_definition__.x_metadata`'s
  `collection_name`, then `model.__polydb__`'s `collection`/`collection_name` — but **never**
  `model.__polydb__["table"]`, even though this repo's own CLAUDE.md (and every prior changelog
  entry) documents `__polydb__` as carrying `storage`, `table`/`collection`, `pk_field`, `rk_field`,
  ... as the two interchangeable naming keys. `altcodepro-universal-interprter`'s own
  `compiler.py:build_model_registry()` stamps every synthesized model's metadata using the key
  `"table"` (`"table": model.name.lower()`), never `"collection"` — so on Azure Table Storage,
  every single NoSQL model compiled from a manifest fell through this method's entire chain and
  landed on the same hardcoded `AZURE_TABLE_NAME` env var default (`"defaulttable"` when unset).
  The practical effect: an app with dozens of distinct NoSQL models (e.g. `users`,
  `subscriptions`, `sessions`, ...) had every one of their rows physically colliding into one
  single shared Azure table, relying entirely on PartitionKey/RowKey uniqueness across unrelated
  models to avoid overwriting each other — a much more severe, silent failure mode than the
  already-fixed 2.5.7 PK/RK bug, and a distinct, independently-layered problem (which physical
  *table* a model's rows live in, vs. which *PartitionKey/RowKey* they use within that table).
  Fixed by adding `polydb_meta.get("table")` as a third fallback alongside `collection`/
  `collection_name` (checked in the same `polydb_meta` branch, after the two existing keys, before
  the env-var default) — so a compiler-synthesized model carrying `__polydb__["table"]` now
  resolves to its own distinct, correctly-named physical table instead of falling through to the
  shared default. Reproduced before trusting the fix: a bare adapter instance's `_get_table_name()`
  called against two model stand-ins carrying only `__polydb__["table"]` (`"users"`/
  `"subscriptions"`, no `collection`/`collection_name`/`__udl_definition__` at all — exactly the
  shape `build_model_registry()` produces) both resolved to the single literal `"defaulttable"`
  before this fix and to their own correct, distinct table names (`"users"`/`"subscriptions"`)
  after it. Backwards compatible: any model already relying on `collection`/`collection_name`
  (or `__udl_definition__.x_metadata.collection_name`) is completely unaffected, since those two
  checks still run first and unconditionally win over `table` when present.

- **2.5.7** — Fixed a real, reproduced bug in `DatabaseFactory.update()`/`delete()`'s NoSQL
  physical-key recovery ([databaseFactory.py](src/polydb/databaseFactory.py)): for the
  overwhelmingly common case where a model declares **no** explicit `pk_field`/`rk_field`
  (`meta.pk_field is None`), both methods' own PartitionKey recovery gave up entirely instead of
  falling back to `NoSQLKVAdapter`'s own real default (`"tenant_id"`/`"id"`, per
  `_pk_rk_field_names`'s own docstring) — leaving `pkey` as `None`. That `None` then reached
  `NoSQLKVAdapter.patch()`/`.delete()`'s own dict-shaped `entity_id` branch (which only recognizes
  `partition_key`/`pk`, never `tenant_id`), so `pk` stayed `None` there too, got `str()`-coerced to
  the literal string `"None"` by `AzureTableStorageAdapter._sanitize_pk_rk`, and both methods
  silently operated on a **phantom row** at `PartitionKey="None"` instead of the real one —
  `update()` wrote a brand-new, non-merged row (losing every other field, and returning
  `{"tenant_id": "None", ...}` as a literal string to the caller) while the real row was never
  found (`existing = self._get_raw(model, None, rk)` → `ResourceNotFound`) and therefore never
  updated at all; `delete()` had the identical shape of bug and would delete nothing. Fixed by
  resolving against `meta.pk_field or "tenant_id"` / `meta.rk_field or "id"` (matching the
  adapter's own real default exactly, never treating an unset `pk_field` as "nothing to recover")
  and by also checking the `entity_id` dict itself (the caller's own already-resolved value, e.g.
  `core_db.py`'s `db_update` passing `{"id": ..., "tenant_id": ctx.tenant_id}`) before falling back
  to a re-read of `before` — more direct and equally authoritative. Reproduced end to end before
  trusting the fix, per this repo's own standard: a fake Azure-shaped NoSQL adapter (stripping
  `PartitionKey`/`RowKey` and remapping to `tenant_id`/`id` on every read, exactly like
  `AzureTableStorageAdapter._unpack_entity`) confirmed the pre-fix code produces the exact reported
  symptom (`update()`'s own return value literally `{"tenant_id": "None", ...}`, a second phantom
  row at `PartitionKey="None"` alongside the real one, the real row never receiving the patched
  field) and that the fix removes it entirely (single row, correctly merged, `delete()` removes the
  real row with none left behind). This is the root cause of a downstream engine-level symptom: a
  `universal-interprter` workflow's `core.db.update` on a default-tier (no declared `pk_field`)
  NoSQL model — e.g. `link_default_sub` setting `default_subscription_id` on a freshly-created
  `users` row during signup — silently updated a phantom row instead of the real one, so a
  subsequent `core.db.read` of that same row (e.g. `login_password`'s `find_user`) never saw the
  field that was supposedly just written.

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

1. **(2.5.14, partially closed)** `PolyDB`/`AsyncPolyDB`/`AsyncDatabaseFactory` are now exported
   from the package root (`from polydb import PolyDB`). Still unexported: `QueryHelper`,
   `AdvancedQueryBuilder`, `EngineConfig`, `EngineOverride`, `TenantConfig`, `SchemaBuilder`,
   `MetricsCollector`, `FieldEncryption`, `PageRequest`/`PageResult`.
2. **Packaging: the optional-extras design is defeated by the core `dependencies` list.** boto3,
   five azure-* packages, four google-cloud-* packages, pymongo, web3 and ipfshttpclient are all
   *required*, so `pip install altcodepro-polydb-python` pulls every cloud SDK and the extras are
   decorative. `build` and `twine` are also listed as runtime dependencies. Core should be
   psycopg2-binary + tenacity + python-dotenv (+ redis).
3. **Python-version metadata is inconsistent.** `requires-python = ">=3.11"` vs classifiers
   advertising 3.8–3.10, `[tool.mypy] python_version = "3.8"`, and black `target-version` py38+.
   Pick 3.11 everywhere. (This is also why `.github/workflows/ci.yml`'s own mypy step is
   report-only rather than blocking, and black itself warns on every run in a 3.11 environment —
   see that workflow's own comments.)
4. **`ModelRegistry` ([registry.py](src/polydb/registry.py)) is dead code** — defined, documented,
   never imported. Either wire it into `_extract_meta()` (it is the only path that supports
   `register_dynamic()` schema-driven models) or drop it.
5. **(2.5.14, closed)** `aio.py`'s `AsyncPolyDB`/`AsyncDatabaseFactory` give FastAPI/asyncio callers
   a real, tested, thread-pool-backed async API today. The documented stance IS "sync core stays
   sync, wrap in a thread pool" -- stated in `aio.py`'s own top comment, not left implicit. A
   genuinely native async rewrite (asyncpg/aioboto3/motor/etc. across every adapter) remains real,
   separate, much larger future work, explicitly not attempted by this round.
6. **Two competing pytest configs.** Both `pytest.ini` and `[tool.pytest.ini_options]` exist with
   different `addopts`; `pytest.ini` wins, so the coverage flags in `pyproject.toml` never apply.
7. **Docs drift.** [README.md](README.md)'s "Project Structure" describes `adapters/aws/`,
   `core/`, `security/` package directories that do not exist, and BUILD_GUIDE.md lists
   `database.py` / `factory.py`. Neither documents the `PolyDB` facade or the env-var contract
   (`POLYDB_ENCRYPTION_KEY*`, `POLYDB_AUDIT_HMAC_KEY`, `POLYDB_SLOW_QUERY_MS`,
   `POLYDB_QUEUE_VISIBILITY_TIMEOUT`, `REDIS_CACHE_URL`, `CLOUD_PROVIDER`). Also: `Readme_Integration_
   Tests.md` documents copying `tests/.env.test` for the local emulator ports, but that file doesn't
   actually exist in the repo — `.github/workflows/ci.yml`'s own `test` job sets the equivalent env
   vars directly instead of depending on it.
8. **Open-source hygiene.** MIT LICENSE is present, but there is no CONTRIBUTING.md, CHANGELOG.md,
   SECURITY.md, issue/PR templates, or code of conduct, and no published API reference.
9. **Typo in extra name:** `bolckchain` should be `blockchain` (rename, keeping the old key as an
   alias for one release).
10. **Azure Table's overflow threshold still doesn't consult `self.max_size`.** Azure branches on
    its own hard-coded `MAX_PROPERTY_CHARS = 30 * 1024` instead; `AZURE_TABLE_MAX_SIZE = 60 * 1024`
    is set on `self.max_size` and never read. Two different thresholds, neither of them the one in
    the base class's own comment (which says "1MB"). Narrower than the fixed 2.5.11 gap (`put()`
    skipping overflow entirely) — this one is Azure's per-property granularity being on a separate
    code path from the base class by design (see the overflow table above), just not yet unified
    on a single configured threshold.
11. **Repo hygiene:** `combine_code.py`, `extract_architecture.py`, `architecture/`, `token.txt`
    and a checked-in `dist/` are dev scratch in the project root. `.env`/`token.txt` are correctly
    gitignored and untracked — keep it that way.
12. **`tests/test_multi_engine.py::TestSingleEngine` is broken test-suite drift, excluded from CI.**
    Its own `_patch_factory` helper calls `db._meta(...)`/`db._model_type(...)` — neither exists on
    `DatabaseFactory` today; meta extraction is the module-level `_extract_meta()` function
    (confirmed by reading `databaseFactory.py` directly), not an instance method, so every test in
    that class fails with a plain `AttributeError`. Pre-existing (confirmed via a before/after
    `git stash` diff against 2.5.12's own CI-adding changeset — byte-for-byte identical failure
    either side), not something CI introduced; `.github/workflows/ci.yml`'s own `test` job excludes
    the `multi_engine` marker entirely until this is fixed for real, rather than shipping a gate
    that's red from day one on an unrelated bug.
