# src/polydb/adapters/GCPStorageAdapter.py

import mimetypes
import os
import threading
from typing import Any, Dict, List, Optional

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry


class GCPStorageAdapter(ObjectStorageAdapter):
    """
    Production-grade Google Cloud Storage adapter.

    - One client, many buckets (resolved + cached per call)
    - Automatic bucket creation, emulator support (fake-gcs-server)
    - put/get/delete are symmetric: a blob is stored at `key` and fetched at `key`
    - per-call `container_name` overrides the bucket (generic name kept for
      cross-provider parity)
    """

    def __init__(self, project_id: str, endpoint: Optional[str], bucket_name: Optional[str] = None):
        super().__init__()

        self.bucket_name: str = bucket_name or os.getenv("GCS_BUCKET_NAME", "default")
        self.project_id: str = project_id or os.getenv("GOOGLE_CLOUD_PROJECT", "polydb-test")
        self._endpoint: Optional[str] = endpoint or os.getenv("GCS_ENDPOINT")

        self._client = None
        self._buckets: Dict[str, Any] = {}
        self._lock = threading.Lock()

        self._initialize_client()

    # ------------------------------------------------------------------
    # Client / bucket resolution
    # ------------------------------------------------------------------
    def _initialize_client(self) -> None:
        """Initialize the shared GCS client once (thread-safe)."""
        from google.cloud import storage  # lazy: only required for this provider

        try:
            with self._lock:
                if self._client:
                    return

                if self._endpoint:
                    self.logger.info(f"Using GCS emulator: {self._endpoint}")
                    self._client = storage.Client(
                        project=self.project_id,
                        client_options={"api_endpoint": self._endpoint},
                    )
                else:
                    self._client = storage.Client(project=self.project_id)

                self.logger.info(f"GCS client initialized (project={self.project_id})")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize GCS: {str(e)}")

    def _get_bucket(self, container_name: Optional[str] = None):
        """Resolve (and cache) a bucket, auto-creating it."""
        if self._client is None:
            raise ConnectionError("GCS client not initialized")

        name = container_name or self.bucket_name

        cached = self._buckets.get(name)
        if cached is not None:
            return cached

        with self._lock:
            cached = self._buckets.get(name)  # re-check under lock
            if cached is not None:
                return cached

            bucket = self._client.bucket(name)
            try:
                if not bucket.exists():
                    bucket = self._client.create_bucket(name)
                    self.logger.info(f"Created GCS bucket: {name}")
            except Exception:
                # fake-gcs-server does not support bucket.exists()
                pass

            self._buckets[name] = bucket
            return bucket

    # ------------------------------------------------------------------
    # Put
    # ------------------------------------------------------------------
    def put(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        optimize: bool = True,
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
        container_name: Optional[str] = None,
    ) -> str:
        if optimize and media_type:
            data = self._optimize_media(data, media_type)
        return self._put_raw(
            key=key,
            data=data,
            fileName=fileName,
            media_type=media_type,
            metadata=metadata,
            container_name=container_name,
        )

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
        container_name: Optional[str] = None,
    ) -> str:
        """Upload object. Stored at `key` so get/delete find it by the same key."""
        try:
            bucket = self._get_bucket(container_name)
            name = container_name or self.bucket_name

            # filename is metadata only — it must NOT alter the blob key
            filename = fileName or os.path.basename(key) or key
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            blob = bucket.blob(key)

            safe_metadata = {k: str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename
            blob.metadata = safe_metadata

            blob.upload_from_string(data, content_type=media_type or "application/octet-stream")
            blob.patch()  # persist metadata

            self.logger.debug(f"GCS uploaded blob: {name}/{key}, type={media_type}")
            return f"https://storage.googleapis.com/{name}/{key}"

        except Exception as e:
            raise StorageError(f"GCS put failed: {str(e)}")

    # ------------------------------------------------------------------
    # Get / Delete / List
    # ------------------------------------------------------------------
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str, container_name: Optional[str] = None) -> Optional[bytes]:
        try:
            blob = self._get_bucket(container_name).blob(key)
            if not blob.exists():
                return None
            data = blob.download_as_bytes()
            self.logger.debug(f"GCS downloaded blob: {key}")
            return data
        except Exception as e:
            raise StorageError(f"GCS get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str, container_name: Optional[str] = None) -> bool:
        try:
            blob = self._get_bucket(container_name).blob(key)
            if not blob.exists():
                return False
            blob.delete()
            self.logger.debug(f"GCS deleted blob: {key}")
            return True
        except Exception as e:
            raise StorageError(f"GCS delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "", container_name: Optional[str] = None) -> List[str]:
        try:
            blobs = self._get_bucket(container_name).list_blobs(prefix=prefix)
            results = [blob.name for blob in blobs]
            self.logger.debug(f"GCS listed {len(results)} blobs (prefix={prefix})")
            return results
        except Exception as e:
            raise StorageError(f"GCS list failed: {str(e)}")
