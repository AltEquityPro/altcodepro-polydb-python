# src/polydb/adapters/FirestoreAdapter.py
from __future__ import annotations

import hashlib
import json
import os
import threading
from sqlite3 import DatabaseError
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from ..base.NoSQLKVAdapter import NoSQLKVAdapter
from ..errors import ConnectionError, NoSQLError
from ..json_safe import json_safe
from ..models import PartitionConfig
from ..retry import retry
from ..types import JsonDict

if TYPE_CHECKING:  # type-checker only — not imported at runtime
    from google.cloud import storage
    from google.cloud.firestore import Client


class FirestoreAdapter(NoSQLKVAdapter):
    """
    Production-grade Firestore adapter with optional GCS overflow.

    Goals (matches your tests)
    - stored row keeps "id" == rk (the row key -- see NoSQLKVAdapter._get_pk_rk,
      which derives rk from data.get(rk_field, ...) with rk_field defaulting
      to "id"), so querying {"id": ...} matches the record's own id
    - patch() merges (preserves existing fields)
    - delete() returns {"id": <rk>} and raises DatabaseError on missing
    - query_page() returns (rows, token) with stable pagination
    - Emulator support via FIRESTORE_EMULATOR_HOST

    NOTE: the Firestore *document id* is still `pk` alone (see _doc_id) --
    this adapter does not compose pk+rk into the physical document key the
    way VercelKVAdapter/DynamoDBAdapter do. That is a pre-existing, separate
    design constraint (two rows sharing the same pk collide onto one
    document) left as-is here since fixing it is a bigger structural change
    this pass didn't verify against a live Firestore/emulator.

    NOTE: google-cloud-firestore / google-cloud-storage are imported lazily
    inside the methods that use them, so installing them is only required when
    this adapter is actually used.
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
        from google.cloud import firestore, storage  # lazy: GCP only

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
        # Document id is the partition key alone (not composed with rk).
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
            "id": rk,
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

            # "id" is the row key (see NoSQLKVAdapter._get_pk_rk, which derives
            # rk from data.get(rk_field, ...) with rk_field defaulting to "id").
            # It is already present in `payload` via dict(data) above whenever
            # the caller supplied one -- only fall back to `rk`, never `pk`,
            # which would silently collapse every row's "id" to its partition
            # key and break id-addressed lookups (the same corruption
            # previously present here and in VercelKVAdapter/DynamoDBAdapter).
            payload: JsonDict = dict(data or {})
            payload.setdefault("id", rk)
            payload["_pk"] = pk
            payload["_rk"] = rk

            overflow_ref = self._maybe_store_overflow(model, pk, rk, payload)
            if overflow_ref is not None:
                collection.document(doc_id).set(overflow_ref)
            else:
                collection.document(doc_id).set(payload)

            # Return the full stored record, not just {"id": pk}.
            return overflow_ref if overflow_ref is not None else payload

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
            doc_data.setdefault("id", rk)

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
        from google.cloud.firestore_v1.base_query import FieldFilter  # lazy: GCP only

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
                # Fall back to the row key, not the partition key, when "id"
                # is absent from the stored document (legacy/incomplete data).
                row.setdefault("id", row.get("_rk") or d.id)
                out.append(self._resolve_overflow(row))
            return out

        except Exception as e:
            raise NoSQLError(f"Firestore query failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        """
        Test expectations:
        - deleting nonexistent raises sqlite3.DatabaseError
        - delete returns {"id": rk}
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
            return {"id": rk}

        except DatabaseError:
            raise
        except Exception as e:
            raise NoSQLError(f"Firestore delete failed: {e}")

    # ---------------------------------------------------------------------
    # Pagination
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
        """
        from google.cloud.firestore_v1.base_query import FieldFilter  # lazy: GCP only

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
                row.setdefault("id", row.get("_rk") or d.id)
                rows.append(self._resolve_overflow(row))

            next_token = None
            if len(rows) == page_size:
                next_token = str(rows[-1].get(order_by))

            return rows, next_token

        except Exception as e:
            raise NoSQLError(f"Firestore query_page failed: {e}")
