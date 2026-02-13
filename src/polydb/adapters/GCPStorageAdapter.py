# src/polydb/adapters/GCPStorageAdapter.py
import os
import threading
from typing import List, Optional, cast
from google.cloud.firestore import DocumentSnapshot
from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry


class GCPStorageAdapter(ObjectStorageAdapter):
    """GCP Cloud Storage with client reuse"""

    def __init__(self):
        super().__init__()
        self.bucket_name = os.getenv("GCS_BUCKET_NAME", "default")
        self._client = None
        self._bucket = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize GCS client once"""
        try:
            from google.cloud import storage

            with self._lock:
                if not self._client:
                    self._client = storage.Client()
                    self._bucket = self._client.bucket(self.bucket_name)
                    self.logger.info("Initialized GCS client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize GCS: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(self, key: str, data: bytes) -> str:
        """Store object"""
        try:
            if self._bucket:
                blob = self._bucket.blob(key)
                blob.upload_from_string(data)
                self.logger.debug(f"Uploaded blob: {key}")
            return key
        except Exception as e:
            raise StorageError(f"GCS put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Get object"""
        try:
            if self._bucket:
                blob = self._bucket.blob(key)
                return blob.download_as_bytes()
            return None
        except Exception as e:
            raise StorageError(f"GCS get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object"""
        try:
            if self._bucket:
                blob = self._bucket.blob(key)
                blob.delete()
                return True
            return False
        except Exception as e:
            raise StorageError(f"GCS delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        try:
            if self._bucket:
                blobs = self._bucket.list_blobs(prefix=prefix)
                return [blob.name for blob in blobs]
            return []
        except Exception as e:
            raise StorageError(f"GCS list failed: {str(e)}")


