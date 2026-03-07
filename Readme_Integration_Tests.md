# polydb Integration Tests — Local Mac Setup

Full integration tests for every `CloudDatabaseFactory` provider without
touching real cloud accounts.  Everything runs against local emulators via
Docker Desktop.

---

## 1. Prerequisites

| Tool | Install |
|------|---------|
| Docker Desktop (Mac) | https://www.docker.com/products/docker-desktop/ |
| Python 3.11+ | `brew install python@3.11` |
| pip packages | see below |

```bash
pip install -r requirements-test.txt
```

**requirements-test.txt** (add to your repo):
```
pytest>=8
pytest-xdist         # optional parallel runs
python-dotenv
psycopg2-binary
pymongo
azure-data-tables
azure-storage-blob
azure-storage-queue
azure-storage-file-share
boto3
google-cloud-firestore
google-cloud-storage
google-cloud-pubsub
redis
tenacity
```

---

## 2. Start emulators

```bash
cd tests/
docker compose -f docker-compose.test.yml up -d
```

Wait ~15 seconds for all health checks to pass:

```bash
docker compose -f docker-compose.test.yml ps
```

All services should show `healthy`.

### What each service emulates

| Service | Port(s) | Emulates |
|---------|---------|---------|
| `postgres` | 5433 | PostgreSQL — primary SQL engine |
| `mongo` | 27018 | MongoDB — default NoSQL |
| `azurite` | 10000–10002 | Azure Blob / Queue / Table / File |
| `localstack` | 4566 | AWS DynamoDB + S3 + SQS |
| `firestore` | 8080 | GCP Firestore |
| `pubsub` | 8085 | GCP Pub/Sub |
| `fake-gcs` | 4443 | GCP Cloud Storage |
| `redis-vercel` | 6380 | Vercel KV (Redis-compatible) |
| `redis-cache` | 6381 | Redis cache layer |

---

## 3. Configure environment

Copy and optionally edit the test env file:

```bash
cp tests/.env.test .env.test
```

The defaults already match the docker-compose ports.  You only need to edit
if you changed port mappings or are pointing at a real service.

---

## 4. Run the tests

### All tests (auto-skip providers not configured)
```bash
pytest tests/ --env-file=.env.test
```

### Single provider
```bash
# PostgreSQL only
pytest tests/test_postgresql.py --env-file=.env.test

# MongoDB only
pytest tests/test_mongodb.py --env-file=.env.test

# Azure only
pytest tests/test_azure.py --env-file=.env.test

# AWS only
pytest tests/test_aws.py --env-file=.env.test

# GCP only
pytest tests/test_gcp.py --env-file=.env.test

# Vercel only
pytest tests/test_vercel.py --env-file=.env.test

# Multi-engine routing
pytest tests/test_multi_engine.py --env-file=.env.test

# CloudDatabaseFactory detection
pytest tests/test_cloud_factory.py --env-file=.env.test
```

### Pass connection strings on the CLI instead of .env
```bash
pytest tests/test_postgresql.py \
  --pg-url "postgresql://polydb:polydb@localhost:5433/polydb_test"

pytest tests/test_aws.py \
  --aws-key test \
  --aws-secret test \
  --aws-region us-east-1 \
  --aws-endpoint http://localhost:4566
```

### Run slow tests (concurrent pool, stress)
```bash
pytest tests/ --env-file=.env.test --run-slow
```

### Marker filtering
```bash
pytest tests/ -m "postgresql or multi_engine" --env-file=.env.test
pytest tests/ -m "not slow" --env-file=.env.test
```

### Parallel (requires pytest-xdist)
```bash
pytest tests/ --env-file=.env.test -n 4
```

---

## 5. PostgreSQL schema

The `pg_schema` session fixture in `conftest.py` creates all required tables
automatically.  It runs once per session and cleans up data (not structure)
between tests via the `clean_table` autouse fixture in `test_postgresql.py`.

If you want to inspect the schema manually:
```bash
psql postgresql://polydb:polydb@localhost:5433/polydb_test
\dt
```

---

## 6. Tear down

```bash
docker compose -f docker-compose.test.yml down -v
```

The `-v` flag removes volumes so the next `up` starts fresh.

---

## 7. Test file overview

```
tests/
├── conftest.py             # CLI options, fixtures, skip helpers, schema DDL
├── .env.test               # Default emulator connection strings
├── docker-compose.test.yml # All 9 local emulator services
│
├── test_cloud_factory.py   # CloudDatabaseFactory auto-detection + adapter types
├── test_postgresql.py      # PostgreSQLAdapter — full CRUD, LINQ, tx, advisory locks
├── test_mongodb.py         # MongoDBAdapter — full CRUD, LINQ, pagination
├── test_azure.py           # Azure Table + Blob + Queue + File (Azurite)
├── test_aws.py             # DynamoDB + S3 + SQS (LocalStack)
├── test_gcp.py             # Firestore + GCS + Pub/Sub (emulators)
├── test_vercel.py          # Vercel KV + Blob (Redis)
└── test_multi_engine.py    # DatabaseFactory multi-engine routing (all combos)
```

---

## 8. CI / GitHub Actions example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: polydb
          POSTGRES_PASSWORD: polydb
          POSTGRES_DB: polydb_test
        ports: ["5433:5432"]
        options: --health-cmd="pg_isready -U polydb" --health-interval=3s --health-retries=10

      mongo:
        image: mongo:7
        ports: ["27018:27017"]

      azurite:
        image: mcr.microsoft.com/azure-storage/azurite
        ports: ["10000:10000", "10001:10001", "10002:10002"]

      localstack:
        image: localstack/localstack:3
        env:
          SERVICES: dynamodb,s3,sqs
        ports: ["4566:4566"]

      redis-vercel:
        image: redis:7-alpine
        ports: ["6380:6379"]

      redis-cache:
        image: redis:7-alpine
        ports: ["6381:6379"]

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[test]"
      - run: pytest tests/ --env-file=tests/.env.test --run-slow -v
        env:
          POSTGRES_CONNECTION_STRING: postgresql://polydb:polydb@localhost:5433/polydb_test
          MONGODB_URI: mongodb://localhost:27018/polydb_test
          AZURE_STORAGE_CONNECTION_STRING: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
          AWS_DEFAULT_REGION: us-east-1
          AWS_ENDPOINT_URL: http://localhost:4566
          KV_REST_API_URL: redis://localhost:6380
          REDIS_CACHE_URL: redis://localhost:6381
```

---

## 9. Troubleshooting

**LocalStack not starting**
```bash
docker logs <localstack-container-id>
# Common fix: increase Docker memory to ≥4 GB in Docker Desktop settings
```

**Firestore emulator OOM**
The `gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators` image is
large (~2 GB).  Pull it once:
```bash
docker pull gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
```

**Azurite Table API 404**
Ensure you're on Azurite ≥3.28.  The Table API was added in 3.x:
```bash
docker pull mcr.microsoft.com/azure-storage/azurite:latest
```

**Tests skip even with services running**
Check that the env vars are actually set:
```bash
set -a && source .env.test && set +a
echo $POSTGRES_CONNECTION_STRING
pytest tests/ -v
```

**psycopg2 install fails on Apple Silicon**
```bash
brew install postgresql
pip install psycopg2-binary
# or use psycopg[binary] for pg 3.x
```