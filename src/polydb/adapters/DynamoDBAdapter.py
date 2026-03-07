# src/polydb/adapters/DynamoDBAdapter.py
from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
from polydb.errors import DatabaseError
from typing import Any, Dict, List, Optional, Tuple

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from boto3.session import Session
from ..base.NoSQLKVAdapter import NoSQLKVAdapter
from ..errors import ConnectionError, NoSQLError
from ..json_safe import json_safe
from ..models import PartitionConfig
from ..retry import retry
from ..types import JsonDict


class DynamoDBAdapter(NoSQLKVAdapter):
    """
    Production-grade DynamoDB adapter with optional S3 overflow.

    Goals (matches your adapter test style)
    - stored row keeps "id" == pk
    - query({"id": ...}) works
    - patch() merges (handled by NoSQLKVAdapter.patch)
    - delete() returns {"id": <pk>} and raises sqlite3.DatabaseError on missing
    - query_page() uses DynamoDB LastEvaluatedKey token (stable)
    - LocalStack support (endpoint_url) + auto create table/bucket in test/dev
    """

    DYNAMODB_MAX_SIZE = 400 * 1024  # 400KB DynamoDB item limit

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        table_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        super().__init__(partition_config)

        self.max_size = self.DYNAMODB_MAX_SIZE
        self.default_table = table_name or os.getenv("DYNAMODB_TABLE_NAME", "polydb")
        self.bucket_name = bucket_name or os.getenv("S3_OVERFLOW_BUCKET", "dynamodb-overflow")

        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        # LocalStack support:
        # - Many setups export AWS_ENDPOINT_URL=http://localhost:4566
        # - Some export LOCALSTACK_ENDPOINT_URL, or infer from LOCALSTACK_HOST
        self.endpoint_url = (
            endpoint_url
            or os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("LOCALSTACK_ENDPOINT_URL")
            or (
                f"http://{os.getenv('LOCALSTACK_HOST')}:4566"
                if os.getenv("LOCALSTACK_HOST")
                else None
            )
        )

        self._dynamodb: Any = None
        self._s3 = None
        self._lock = threading.Lock()

        self._initialize_clients()

    # ---------------------------------------------------------------------
    # Init / Helpers
    # ---------------------------------------------------------------------

    def _initialize_clients(self) -> None:
        try:
            with self._lock:
                if self._dynamodb and self._s3:
                    return

                session = Session(region_name=self.region)

                # LocalStack often needs dummy keys but boto3 doesn’t require them explicitly
                self._dynamodb = session.resource(
                    "dynamodb",
                    endpoint_url=self.endpoint_url,
                )
                self._s3 = session.client(
                    "s3",
                    endpoint_url=self.endpoint_url,
                )

                self.logger.info(
                    f"Initialized DynamoDB/S3 clients (region={self.region}, endpoint={self.endpoint_url or 'aws'})"
                )
        except Exception as e:
            raise ConnectionError(f"DynamoDB init failed: {e}")

    def _table_name(self, model: type) -> str:
        meta = getattr(model, "__polydb__", {}) or {}
        return meta.get("table") or meta.get("collection") or model.__name__.lower()

    def _get_table(self, model: type):
        if not self._dynamodb:
            self._initialize_clients()
        if not self._dynamodb:
            raise ConnectionError("DynamoDB resource not initialized")
        table_name = self._table_name(model)
        table = self._dynamodb.Table(table_name)
        self._ensure_table_exists(table_name)
        return table

    def _ensure_table_exists(self, table_name: str) -> None:
        """
        Ensure PK/SK table exists. Safe in prod (no-op if exists).
        Required for LocalStack integration tests.
        """
        if not self._dynamodb:
            return

        try:
            self._dynamodb.meta.client.describe_table(TableName=table_name)
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("ResourceNotFoundException", "ValidationException"):
                # ValidationException can happen on LocalStack when table doesn't exist yet
                # but describe throws something odd; fall through to create attempt.
                pass

        try:
            self._dynamodb.meta.client.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            # Wait until active (works in AWS + LocalStack)
            self._dynamodb.meta.client.get_waiter("table_exists").wait(TableName=table_name)
            self.logger.info(f"Created DynamoDB table: {table_name}")
        except ClientError as e:
            # If another process created it meanwhile
            if e.response.get("Error", {}).get("Code") in ("ResourceInUseException",):
                return
            raise

    def _ensure_bucket_exists(self) -> None:
        """Ensure overflow bucket exists (safe no-op in prod; needed in LocalStack)."""
        if not self._s3:
            return
        try:
            self._s3.head_bucket(Bucket=self.bucket_name)
            return
        except Exception:
            pass

        try:
            # us-east-1 does not require LocationConstraint; other regions do.
            if self.region == "us-east-1":
                self._s3.create_bucket(Bucket=self.bucket_name)
            else:
                self._s3.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            self.logger.info(f"Created S3 overflow bucket: {self.bucket_name}")
        except Exception:
            # If already exists or emulator differences
            pass

    def _encode_token(self, lek: Dict[str, Any]) -> str:
        raw = json.dumps(lek, default=json_safe).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8")

    def _decode_token(self, tok: str) -> Dict[str, Any]:
        raw = base64.urlsafe_b64decode(tok.encode("utf-8"))
        return json.loads(raw.decode("utf-8"))

    def _blob_key(self, model: type, pk: str, rk: str, checksum: str) -> str:
        return f"overflow/{self._table_name(model)}/{pk}/{rk}/{checksum}.json"

    def _maybe_overflow_to_s3(
        self, model: type, pk: str, rk: str, payload: JsonDict
    ) -> Optional[JsonDict]:
        data_bytes = json.dumps(payload, default=json_safe).encode("utf-8")
        if len(data_bytes) <= self.DYNAMODB_MAX_SIZE:
            return None

        if not self._s3:
            raise NoSQLError("DynamoDB item exceeds 400KB and S3 client is unavailable")

        self._ensure_bucket_exists()

        checksum = hashlib.md5(data_bytes).hexdigest()
        blob_key = self._blob_key(model, pk, rk, checksum)

        self._s3.put_object(Bucket=self.bucket_name, Key=blob_key, Body=data_bytes)

        ref: JsonDict = {
            "PK": pk,
            "SK": rk,
            "id": pk,  # ✅ for tests and querying
            "_pk": pk,
            "_rk": rk,
            "_overflow": True,
            "_blob_key": blob_key,
            "_size": len(data_bytes),
            "_checksum": checksum,
        }

        # Best-effort keep scalar fields for filtering
        kept = 0
        for k, v in payload.items():
            if k in ref:
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                ref[k] = v
                kept += 1
            if kept >= 50:
                break

        self.logger.info(f"Stored DynamoDB overflow to S3: {blob_key} ({len(data_bytes)} bytes)")
        return ref

    def _resolve_overflow(self, item: JsonDict) -> JsonDict:
        if not item.get("_overflow"):
            return item

        if not self._s3:
            raise NoSQLError("Overflow item present but S3 client unavailable")

        blob_key = item.get("_blob_key")
        checksum = item.get("_checksum")
        if not blob_key:
            raise NoSQLError("Overflow item missing _blob_key")

        resp = self._s3.get_object(Bucket=self.bucket_name, Key=blob_key)
        blob_data = resp["Body"].read()

        actual = hashlib.md5(blob_data).hexdigest()
        if checksum and actual != checksum:
            raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual}")

        restored = json.loads(blob_data.decode("utf-8"))
        return restored

    # ---------------------------------------------------------------------
    # Required NoSQLKVAdapter hooks
    # ---------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            table = self._get_table(model)

            payload: JsonDict = dict(data or {})
            payload["PK"] = pk
            payload["SK"] = rk

            # Ensure "id" field exists for tests/querying
            payload.setdefault("id", pk)
            payload.setdefault("_pk", pk)
            payload.setdefault("_rk", rk)

            ref = self._maybe_overflow_to_s3(model, pk, rk, payload)
            table.put_item(Item=ref if ref is not None else payload)

            # ✅ match pattern: put returns {"id": pk}
            return {"id": pk}

        except Exception as e:
            raise NoSQLError(f"DynamoDB put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            table = self._get_table(model)

            resp = table.get_item(Key={"PK": pk, "SK": rk})
            item = resp.get("Item")
            if not item:
                return None

            item.setdefault("id", pk)
            return self._resolve_overflow(item)

        except Exception as e:
            raise NoSQLError(f"DynamoDB get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        """
        Prefer Query when PK can be derived. Otherwise Scan.
        Supports filters like {"id": "..."} and arbitrary Attr equality.
        """
        try:
            table = self._get_table(model)
            filters = filters or {}

            # Treat 'id' as PK by convention (since we store PK=pk=id in NoSQLKVAdapter by default)
            pk_value = filters.get("PK") or filters.get("partition_key") or filters.get("id")

            if pk_value is not None:
                key_cond = Key("PK").eq(str(pk_value))
                if "SK" in filters:
                    key_cond = key_cond & Key("SK").eq(str(filters["SK"]))

                kwargs: Dict[str, Any] = {"KeyConditionExpression": key_cond}

                # Other filters as FilterExpression
                other = {
                    k: v for k, v in filters.items() if k not in ("PK", "SK", "partition_key", "id")
                }
                if other:
                    expr = None
                    for k, v in other.items():
                        part = Attr(k).eq(v)
                        expr = part if expr is None else (expr & part)
                    kwargs["FilterExpression"] = expr

                if limit:
                    kwargs["Limit"] = limit

                resp = table.query(**kwargs)
                items = resp.get("Items", [])
            else:
                # Scan
                kwargs = {}
                if filters:
                    expr = None
                    for k, v in filters.items():
                        part = Attr(k).eq(v)
                        expr = part if expr is None else (expr & part)
                    kwargs["FilterExpression"] = expr
                if limit:
                    kwargs["Limit"] = limit

                resp = table.scan(**kwargs)
                items = resp.get("Items", [])

            out: List[JsonDict] = []
            for it in items:
                it.setdefault("id", it.get("_pk") or it.get("PK"))
                out.append(self._resolve_overflow(it))
            return out

        except Exception as e:
            raise NoSQLError(f"DynamoDB query failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        """
        - if missing => raise sqlite3.DatabaseError (matches Firestore style tests)
        - if overflow => delete S3 object best-effort
        - return {"id": pk}
        """
        try:
            table = self._get_table(model)

            resp = table.get_item(Key={"PK": pk, "SK": rk})
            item = resp.get("Item")
            if not item:
                raise DatabaseError(f"Item {pk}/{rk} does not exist")

            if item.get("_overflow") and self._s3:
                blob_key = item.get("_blob_key")
                if blob_key:
                    try:
                        self._s3.delete_object(Bucket=self.bucket_name, Key=blob_key)
                    except Exception:
                        pass

            table.delete_item(Key={"PK": pk, "SK": rk})
            return {"id": pk}

        except DatabaseError:
            raise
        except Exception as e:
            raise NoSQLError(f"DynamoDB delete failed: {e}")

    # ---------------------------------------------------------------------
    # Provider-specific pagination (stable, not offset-based)
    # ---------------------------------------------------------------------
    def query_page(
        self,
        model: type,
        query: Dict[str, Any],
        page_size: int,
        continuation_token: Optional[str] = None,
    ) -> Tuple[List[JsonDict], Optional[str]]:
        """
        DynamoDB pagination.

        Behaviour:
        - Uses Query when PK/id is available
        - Uses Scan otherwise
        - Handles FilterExpression correctly when scanning by collecting
        results until page_size items are returned
        - continuation_token = base64(json(LastEvaluatedKey))
        """
        try:
            table = self._get_table(model)
            query = query or {}

            start_key = self._decode_token(continuation_token) if continuation_token else None

            pk_value = query.get("PK") or query.get("partition_key") or query.get("id")

            items: List[JsonDict] = []
            last_key = start_key

            if pk_value is not None:

                key_cond = Key("PK").eq(str(pk_value))

                if "SK" in query:
                    key_cond = key_cond & Key("SK").eq(str(query["SK"]))

                kwargs: Dict[str, Any] = {
                    "KeyConditionExpression": key_cond,
                    "Limit": page_size,
                }

                if last_key:
                    kwargs["ExclusiveStartKey"] = last_key

                other_filters = {
                    k: v for k, v in query.items() if k not in ("PK", "SK", "partition_key", "id")
                }

                if other_filters:
                    expr = None
                    for k, v in other_filters.items():
                        part = Attr(k).eq(v)
                        expr = part if expr is None else expr & part
                    kwargs["FilterExpression"] = expr

                resp = table.query(**kwargs)
                items = resp.get("Items", [])
                last_key = resp.get("LastEvaluatedKey")

            # ------------------------------------------------------------------
            # SCAN path (collect until page_size)
            # ------------------------------------------------------------------
            else:

                filter_expr = None
                if query:
                    for k, v in query.items():
                        part = Attr(k).eq(v)
                        filter_expr = part if filter_expr is None else filter_expr & part

                while len(items) < page_size:

                    kwargs: Dict[str, Any] = {"Limit": page_size}

                    if last_key:
                        kwargs["ExclusiveStartKey"] = last_key

                    if filter_expr is not None:
                        kwargs["FilterExpression"] = filter_expr

                    resp = table.scan(**kwargs)

                    batch = resp.get("Items", [])
                    items.extend(batch)

                    last_key = resp.get("LastEvaluatedKey")

                    if not last_key:
                        break

                items = items[:page_size]

            # ------------------------------------------------------------------
            # Normalize output
            # ------------------------------------------------------------------
            out: List[JsonDict] = []

            for it in items:
                it.setdefault("id", it.get("_pk") or it.get("PK"))
                out.append(self._resolve_overflow(it))

            next_tok = self._encode_token(last_key) if last_key else None

            return out, next_tok

        except Exception as e:
            raise NoSQLError(f"DynamoDB query_page failed: {e}")
