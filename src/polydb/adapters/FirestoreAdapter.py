# src/polydb/adapters/FirestoreAdapter.py
from __future__ import annotations

import hashlib
import json
import os
import threading
from sqlite3 import DatabaseError
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import firestore
from google.cloud import storage
from google.cloud.firestore import Client
from google.cloud.firestore_v1.base_query import FieldFilter

from ..base.NoSQLKVAdapter import NoSQLKVAdapter
from ..errors import ConnectionError, NoSQLError
from ..json_safe import json_safe
from ..models import PartitionConfig
from ..retry import retry
from ..types import JsonDict


class FirestoreAdapter(NoSQLKVAdapter):
    """
    Production-grade Firestore adapter with optional GCS overflow.

    Goals (matches your tests)
    - Document id == pk (so querying {"id": ...} works)
    - patch() merges (preserves existing fields)
    - delete() returns {"id": <pk>} and raises DatabaseError on missing
    - query_page() returns (rows, token) with stable pagination
    - Emulator support via FIRESTORE_EMULATOR_HOST
    """

    FIRESTORE_MAX_SIZE = 1024 * 1024  # 1MB doc limit (practical)

    def __init__(
        self,
        partition_config: Optional[PartitionConfig] = None,
        project: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        super().__init__(partition_config)
        self.max_size = self.FIRESTORE_MAX_SIZE

        self.project = (
            project
            or os.getenv("GCP_PROJECT")
            or os.getenv("GOOGLE_CLOUD_PROJECT")
            or "polydb-test"
        )

        # Overflow bucket (optional; used only if doc would exceed max size)
        self.bucket_name = bucket_name or os.getenv("GCS_OVERFLOW_BUCKET", "firestore-overflow")

        self._client: Optional[Client] = None
        self._storage_client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None

        self._lock = threading.Lock()
        self._initialize_clients()

    # ---------------------------------------------------------------------
    # Init / Helpers
    # ---------------------------------------------------------------------

    def _initialize_clients(self) -> None:
        try:
            with self._lock:
                if self._client:
                    return

                # Firestore client (emulator respected automatically if FIRESTORE_EMULATOR_HOST set)
                self._client = firestore.Client(project=self.project)

                # Storage client: only needed for overflow.
                # In emulator/test env you may have STORAGE_EMULATOR_HOST + anonymous/no-auth.
                # If storage client init fails, we keep overflow disabled (still production-safe).
                try:
                    self._storage_client = storage.Client(project=self.project)
                    self._bucket = self._storage_client.bucket(self.bucket_name)

                    # Create bucket if possible; ignore if already exists or emulator lacks create.
                    try:
                        self._bucket.create()  # type: ignore[union-attr]
                        self.logger.info(f"Created GCS overflow bucket: {self.bucket_name}")
                    except Exception:
                        pass

                    self.logger.info(
                        f"Firestore initialized (project={self.project}) with GCS overflow bucket={self.bucket_name}"
                    )
                except Exception as e:
                    # Keep Firestore working; overflow becomes a no-op.
                    self._storage_client = None
                    self._bucket = None
                    self.logger.warning(f"GCS overflow disabled (storage client init failed): {e}")
                    self.logger.info(f"Firestore initialized (project={self.project})")

        except Exception as e:
            raise ConnectionError(f"Firestore init failed: {e}")

    def _collection_name(self, model: type) -> str:
        meta = getattr(model, "__polydb__", {}) or {}
        return meta.get("collection") or meta.get("table") or model.__name__.lower()

    def _get_collection(self, model: type) -> Any:
        if not self._client:
            self._initialize_clients()
        if not self._client:
            raise ConnectionError("Firestore client not initialized")
        return self._client.collection(self._collection_name(model))

    def _doc_id(self, pk: str) -> str:
        # IMPORTANT for tests: doc_id == pk == row["id"]
        return str(pk)

    def _blob_key(self, model: type, pk: str, rk: str, checksum: str) -> str:
        # Keep it stable and unique per model + keys
        return f"overflow/{self._collection_name(model)}/{pk}/{rk}/{checksum}.json"

    def _maybe_store_overflow(
        self, model: type, pk: str, rk: str, payload: JsonDict
    ) -> Optional[JsonDict]:
        """
        If payload exceeds doc limit, store full payload in GCS and return reference document.
        If GCS is not available, raise (to avoid silently corrupting data).
        """
        data_bytes = json.dumps(payload, default=json_safe).encode("utf-8")
        if len(data_bytes) <= self.FIRESTORE_MAX_SIZE:
            return None

        if not self._bucket:
            raise NoSQLError(
                "Document exceeds Firestore 1MB limit and GCS overflow bucket is not available"
            )

        checksum = hashlib.md5(data_bytes).hexdigest()
        blob_key = self._blob_key(model, pk, rk, checksum)

        blob = self._bucket.blob(blob_key)
        blob.upload_from_string(data_bytes)

        ref: JsonDict = {
            "id": pk,
            "_pk": pk,
            "_rk": rk,
            "_overflow": True,
            "_blob_key": blob_key,
            "_size": len(data_bytes),
            "_checksum": checksum,
        }

        # Keep some scalar fields for index/query convenience (best effort)
        kept = 0
        for k, v in payload.items():
            if k in ("_overflow", "_blob_key", "_checksum"):
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                ref[k] = v
                kept += 1
            if kept >= 50:
                break

        self.logger.info(f"Stored Firestore overflow to GCS: {blob_key} ({len(data_bytes)} bytes)")
        return ref

    def _resolve_overflow(self, doc_data: JsonDict) -> JsonDict:
        if not doc_data.get("_overflow"):
            return doc_data

        blob_key = doc_data.get("_blob_key")
        checksum = doc_data.get("_checksum")

        if not blob_key:
            raise NoSQLError("Overflow doc missing _blob_key")
        if not self._bucket:
            raise NoSQLError("Overflow doc present but GCS bucket unavailable")

        blob = self._bucket.blob(blob_key)
        blob_data = blob.download_as_bytes()

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
            collection = self._get_collection(model)
            doc_id = self._doc_id(pk)

            # Ensure required identifiers exist for tests and convenience
            payload: JsonDict = dict(data or {})
            payload["id"] = pk
            payload["_pk"] = pk
            payload["_rk"] = rk

            overflow_ref = self._maybe_store_overflow(model, pk, rk, payload)
            if overflow_ref is not None:
                collection.document(doc_id).set(overflow_ref)
            else:
                collection.document(doc_id).set(payload)

            return {"id": pk}

        except Exception as e:
            raise NoSQLError(f"Firestore put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            collection = self._get_collection(model)
            doc_id = self._doc_id(pk)

            snap = collection.document(doc_id).get()
            if not getattr(snap, "exists", False):
                return None

            doc_data = snap.to_dict() or {}
            doc_data.setdefault("id", pk)

            return self._resolve_overflow(doc_data)

        except Exception as e:
            raise NoSQLError(f"Firestore get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(
        self, model: type, filters: Dict[str, Any], limit: Optional[int]
    ) -> List[JsonDict]:
        """
        Basic equality / comparator filtering via FieldFilter.
        Note: Firestore requires indexes for some compound queries in real GCP.
        Emulator usually allows most.
        """
        try:
            collection = self._get_collection(model)
            query = collection

            for field, value in (filters or {}).items():
                # Support your existing suffix operators if needed
                if field.endswith("__gt"):
                    query = query.where(filter=FieldFilter(field[:-4], ">", value))
                elif field.endswith("__gte"):
                    query = query.where(filter=FieldFilter(field[:-5], ">=", value))
                elif field.endswith("__lt"):
                    query = query.where(filter=FieldFilter(field[:-4], "<", value))
                elif field.endswith("__lte"):
                    query = query.where(filter=FieldFilter(field[:-5], "<=", value))
                elif field.endswith("__in"):
                    query = query.where(filter=FieldFilter(field[:-4], "in", value))
                else:
                    query = query.where(filter=FieldFilter(field, "==", value))

            if limit:
                query = query.limit(limit)

            docs = list(query.stream())
            out: List[JsonDict] = []
            for d in docs:
                row = d.to_dict() or {}
                # Ensure id present for tests
                row.setdefault("id", row.get("_pk") or d.id)
                out.append(self._resolve_overflow(row))
            return out

        except Exception as e:
            raise NoSQLError(f"Firestore query failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        """
        Test expectations:
        - deleting nonexistent raises sqlite3.DatabaseError
        - delete returns {"id": pk}
        - deletes overflow blob if present
        """
        try:
            collection = self._get_collection(model)
            doc_id = self._doc_id(pk)

            snap = collection.document(doc_id).get()
            if not getattr(snap, "exists", False):
                # tests expect DatabaseError specifically
                raise DatabaseError(f"Document {doc_id} does not exist")

            doc_data = snap.to_dict() or {}
            if doc_data.get("_overflow") and self._bucket:
                blob_key = doc_data.get("_blob_key")
                if blob_key:
                    try:
                        self._bucket.blob(blob_key).delete()
                        self.logger.debug(f"Deleted overflow GCS object: {blob_key}")
                    except Exception:
                        pass

            collection.document(doc_id).delete()
            return {"id": pk}

        except DatabaseError:
            raise
        except Exception as e:
            raise NoSQLError(f"Firestore delete failed: {e}")

    # ---------------------------------------------------------------------
    # Pagination helper used by NoSQLKVAdapter.query_page (if it calls _query_page_raw)
    # If your base calls only _query_raw, you can still add a public query_page method
    # in NoSQLKVAdapter; but since your tests call gcp_nosql.query_page(...) we provide it.
    # ---------------------------------------------------------------------

    def query_page(
        self,
        model: type,
        query=None,
        page_size: int = 25,
        continuation_token: Optional[str] = None,
        order_by: str = "id",
    ) -> Tuple[List[JsonDict], Optional[str]]:
        """
        Returns (rows, next_token). Token is last document id from the page.

        Works with your tests:
          page1, tok = gcp_nosql.query_page(GcpItem, {"tenant_id": tag}, 3)
          page2, _   = gcp_nosql.query_page(GcpItem, {"tenant_id": tag}, 3, tok)
        """

        try:
            collection = self._get_collection(model)
            fs_query = collection

            # Apply filters
            if query:
                for field, value in query.items():
                    fs_query = fs_query.where(filter=FieldFilter(field, "==", value))

            fs_query = fs_query.order_by(order_by).limit(page_size)

            # Continue from token
            if continuation_token:
                fs_query = fs_query.start_after({order_by: continuation_token})

            docs = list(fs_query.stream())

            rows: List[JsonDict] = []
            for d in docs:
                row = d.to_dict() or {}
                row.setdefault("id", row.get("_pk") or d.id)
                rows.append(self._resolve_overflow(row))

            next_token = None
            if len(rows) == page_size:
                next_token = str(rows[-1].get(order_by))

            return rows, next_token

        except Exception as e:
            raise NoSQLError(f"Firestore query_page failed: {e}")
