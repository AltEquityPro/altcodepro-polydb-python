# src/polydb/adapters/GCPStorageAdapter.py

import os
import threading
from typing import List, Optional

from google.cloud import storage
from google.api_core.exceptions import NotFound

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry


class GCPStorageAdapter(ObjectStorageAdapter):
    """
    Production-grade Google Cloud Storage adapter.

    Features
    --------
    - Thread-safe client initialization
    - Automatic bucket creation
    - Emulator support (fake-gcs-server)
    - Retry support
    - Structured logging
    """

    def __init__(self, bucket_name: Optional[str] = None):
        super().__init__()

        self.bucket_name: str = bucket_name or os.getenv("GCS_BUCKET_NAME", "default")
        self.project_id: str = os.getenv("GOOGLE_CLOUD_PROJECT", "polydb-test")

        # Emulator support
        self._endpoint: Optional[str] = os.getenv("GCS_ENDPOINT")

        self._client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None

        self._lock = threading.Lock()

        self._initialize_client()

    # ------------------------------------------------------------------
    # Client initialization
    # ------------------------------------------------------------------

    def _initialize_client(self) -> None:
        """Initialize GCS client once (thread-safe)"""
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

                self._bucket = self._client.bucket(self.bucket_name)

                # Ensure bucket exists
                try:
                    if not self._bucket.exists():
                        self._bucket = self._client.create_bucket(self.bucket_name)
                        self.logger.info(f"Created GCS bucket: {self.bucket_name}")
                except Exception:
                    # fake-gcs-server does not support bucket.exists()
                    pass

                self.logger.info(
                    f"GCS initialized (bucket={self.bucket_name}, project={self.project_id})"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize GCS: {str(e)}")

    # ------------------------------------------------------------------
    # Put object
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(self, key: str, data: bytes) -> str:
        """Upload object to GCS"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blob = self._bucket.blob(key)

            blob.upload_from_string(data)

            self.logger.debug(f"GCS uploaded blob: {key}")

            return key

        except Exception as e:
            raise StorageError(f"GCS put failed: {str(e)}")

    # ------------------------------------------------------------------
    # Get object
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> Optional[bytes]:
        """Download object from GCS"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blob = self._bucket.blob(key)

            if not blob.exists():
                return None

            data = blob.download_as_bytes()

            self.logger.debug(f"GCS downloaded blob: {key}")

            return data

        except NotFound:
            return None

        except Exception as e:
            raise StorageError(f"GCS get failed: {str(e)}")

    # ------------------------------------------------------------------
    # Delete object
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object from GCS"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blob = self._bucket.blob(key)

            if not blob.exists():
                return False

            blob.delete()

            self.logger.debug(f"GCS deleted blob: {key}")

            return True

        except NotFound:
            return False

        except Exception as e:
            raise StorageError(f"GCS delete failed: {str(e)}")

    # ------------------------------------------------------------------
    # List objects
    # ------------------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with optional prefix"""
        try:
            if not self._bucket:
                raise ConnectionError("GCS bucket not initialized")

            blobs = self._bucket.list_blobs(prefix=prefix)

            results = [blob.name for blob in blobs]

            self.logger.debug(f"GCS listed {len(results)} blobs (prefix={prefix})")

            return results

        except Exception as e:
            raise StorageError(f"GCS list failed: {str(e)}")
