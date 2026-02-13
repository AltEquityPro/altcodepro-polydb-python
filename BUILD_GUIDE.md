# Build & Installation Guide

## Package Structure
```
src
├──polydb/
├── pyproject.toml          # Main configuration (PEP 621)
├── setup.py                # Backwards compatibility
├── MANIFEST.in             # Package files
├── LICENSE                 # MIT License
├── requirements*.txt       # Dependency lists
└── polydb/                 # Source code
    ├── __init__.py
    ├── database.py
    ├── decorators.py
    ├── errors.py
    ├── factory.py
    ├── models.py
    ├── registry.py
    ├── retry.py
    ├── types.py
    ├── utils.py
    ├── base/
    └── adapters/
```

## Build for PyPI

```bash
# Install build tools
pip install build twine

# Build package
python -m build

# This creates:
# - dist/altcodepro_polydb_python-2.0.0-py3-none-any.whl
# - dist/altcodepro-polydb-python-2.0.0.tar.gz

# Test upload to TestPyPI
twine upload --repository testpypi dist/*

# Upload to PyPI
twine upload dist/*
```

## Installation Options

### Minimal (PostgreSQL only)
```bash
pip install altcodepro-polydb-python
```
**Dependencies**: `psycopg2-binary>=2.9.9`

### Generic Stack (Cheapest)
```bash
pip install altcodepro-polydb-python[generic]
```
**Dependencies**: PostgreSQL, MongoDB, RabbitMQ, S3-compatible

### AWS
```bash
pip install altcodepro-polydb-python[aws]
```
**Dependencies**: boto3 (DynamoDB, S3, SQS)

### Azure
```bash
pip install altcodepro-polydb-python[azure]
```
**Dependencies**: Azure Storage SDK (Blob, Table, Queue, Files)

### GCP
```bash
pip install altcodepro-polydb-python[gcp]
```
**Dependencies**: Google Cloud SDK (Storage, Firestore, Pub/Sub)

### Multiple Providers
```bash
pip install altcodepro-polydb-python[aws,azure,gcp]
```

### All Providers
```bash
pip install altcodepro-polydb-python[all]
```

### Development
```bash
pip install altcodepro-polydb-python[dev]
```
**Includes**: pytest, black, flake8, mypy, isort

## Direct Installation from Requirements

```bash
# Generic stack
pip install -r requirements-generic.txt

# AWS
pip install -r requirements-aws.txt

# Azure
pip install -r requirements-azure.txt

# GCP
pip install -r requirements-gcp.txt

# Development
pip install -r requirements-dev.txt
```

## Development Setup

```bash
# Clone repository
git clone https://github.com/altcodepro/polydb-python
cd polydb-python

# Install in editable mode with dev dependencies
pip install -e .[dev]

# Run tests
pytest

# Format code
black polydb/
isort polydb/

# Type checking
mypy polydb/

# Linting
flake8 polydb/
```

## Version 2.0.0 Changes

All critical issues fixed:
- ✅ Connection pooling (PostgreSQL)
- ✅ Client reuse (all providers)
- ✅ Column validation
- ✅ Transaction API fix
- ✅ Thread safety
- ✅ Timeout handling
- ✅ Health checks
- ✅ Metrics hooks

## Environment Variables

```bash
# Explicit provider (recommended)
CLOUD_PROVIDER=postgresql  # or aws, azure, gcp, vercel

# PostgreSQL
POSTGRES_CONNECTION_STRING=postgresql://user:pass@host:5432/db
POSTGRES_MIN_CONNECTIONS=2
POSTGRES_MAX_CONNECTIONS=10

# MongoDB
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=mydb
MONGODB_COLLECTION=mycollection

# AWS
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
DYNAMODB_TABLE_NAME=mytable
S3_BUCKET_NAME=mybucket

# Azure
AZURE_STORAGE_CONNECTION_STRING=...
AZURE_TABLE_NAME=mytable
AZURE_CONTAINER_NAME=mycontainer

# GCP
GOOGLE_CLOUD_PROJECT=my-project
FIRESTORE_COLLECTION=mycollection
GCS_BUCKET_NAME=mybucket
```
