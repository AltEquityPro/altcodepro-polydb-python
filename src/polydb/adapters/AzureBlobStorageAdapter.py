# src/polydb/adapters/AzureBlobStorageAdapter.py

from polydb.base.ObjectStorageAdapter import ObjectStorageAdapter
from polydb.errors import ConnectionError, StorageError
from polydb.retry import retry
import os
import threading
from typing import List

class AzureBlobStorageAdapter(ObjectStorageAdapter):
    """Azure Blob Storage with client reuse"""

    def __init__(self):
        super().__init__()
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        self.container_name = os.getenv("AZURE_CONTAINER_NAME", "default") or ""
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Azure Blob Storage client once"""
        try:
            from azure.storage.blob import BlobServiceClient

            with self._lock:
                if not self._client:
                    self._client = BlobServiceClient.from_connection_string(self.connection_string)
                    self.logger.info("Initialized Azure Blob Storage client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Blob Storage: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(self, key: str, data: bytes) -> str:
        """Store blob"""
        try:
            if self._client:
                blob_client = self._client.get_blob_client(self.container_name, key)
                blob_client.upload_blob(data, overwrite=True)
                self.logger.debug(f"Uploaded blob: {key}")
            return key
        except Exception as e:
            raise StorageError(f"Azure Blob put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Get blob"""
        try:
            if self._client:
                blob_client = self._client.get_blob_client(self.container_name, key)
                return blob_client.download_blob().readall()
            return None
        except Exception as e:
            raise StorageError(f"Azure Blob get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete blob"""
        try:
            if self._client:
                blob_client = self._client.get_blob_client(self.container_name, key)
                blob_client.delete_blob()
                return True
            return False
        except Exception as e:
            raise StorageError(f"Azure Blob delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List blobs with prefix"""
        try:
            if self._client:
                container_client = self._client.get_container_client(self.container_name)
                return [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]
            return []
        except Exception as e:
            raise StorageError(f"Azure Blob list failed: {str(e)}")