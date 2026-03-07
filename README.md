# AltCodePro PolyDB

**AltCodePro PolyDB** is a production-ready **multi-cloud database abstraction layer for Python**.

It provides a **single unified API** for SQL and NoSQL databases across major cloud providers including **AWS, Azure, GCP, Vercel, MongoDB, and PostgreSQL**.

PolyDB is designed for modern cloud applications that require:

- multi-cloud portability
- hybrid SQL + NoSQL architectures
- distributed storage routing
- enterprise-grade reliability

---

## Key Features

### Multi-Cloud Support

Works across multiple cloud providers with automatic provider detection.

| Provider | NoSQL | Object Storage | Queue |
|--------|------|---------------|------|
| AWS | DynamoDB | S3 | SQS |
| Azure | Table Storage | Blob Storage | Azure Queue |
| GCP | Firestore | GCS | Pub/Sub |
| Vercel | KV (Redis) | Blob | Queue |
| MongoDB | MongoDB | — | — |
| PostgreSQL | SQL | S3-compatible | — |

---

### Hybrid SQL + NoSQL

Use both SQL and NoSQL databases simultaneously.

Example architecture:
