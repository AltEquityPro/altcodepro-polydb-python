# src/polydb/adapters/AzureBlobStorageAdapter.py

import os
import threading
from typing import List, Optional

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import ConnectionError, StorageError
from ..retry import retry


class AzureBlobStorageAdapter(ObjectStorageAdapter):
    """
    Production-grade Azure Blob Storage adapter.

    Features
    - Thread-safe client initialization
    - Container auto-creation
    - Retry support
    - Structured logging
    - Connection reuse
    """

    def __init__(self, connection_string: str = "", container_name: str = ""):
        super().__init__()

        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = container_name or os.getenv("AZURE_CONTAINER_NAME", "polydb")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")

        self._client: Optional[BlobServiceClient] = None
        self._container: Optional[ContainerClient] = None
        self._lock = threading.Lock()

        self._initialize_client()

    def _initialize_client(self) -> None:
        """Initialize Azure Blob client and container"""
        try:
            with self._lock:
                if self._client is not None:
                    return
                if not self.connection_string:
                    raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")
                self._client = BlobServiceClient.from_connection_string(self.connection_string)

                self._container = self._client.get_container_client(self.container_name)

                try:
                    self._container.create_container()
                    self.logger.info(f"Created container: {self.container_name}")
                except ResourceExistsError:
                    pass

                self.logger.info(
                    f"Azure Blob Storage initialized (container={self.container_name})"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Blob Storage: {e}")

    def _require_container(self) -> ContainerClient:
        """Ensure container exists"""
        if self._container is None:
            raise ConnectionError("Azure Blob Storage client is not initialized")
        return self._container

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(self, key: str, data: bytes) -> str:
        """Upload blob"""
        try:
            container = self._require_container()

            blob_client = container.get_blob_client(key)
            blob_client.upload_blob(data, overwrite=True)

            self.logger.debug(f"Uploaded blob key={key}")

            return key

        except Exception as e:
            raise StorageError(f"Azure Blob put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Download blob"""
        try:
            container = self._require_container()

            blob_client = container.get_blob_client(key)

            downloader = blob_client.download_blob()
            data = downloader.readall()

            self.logger.debug(f"Downloaded blob key={key}")

            return data

        except ResourceNotFoundError:
            return None
        except Exception as e:
            raise StorageError(f"Azure Blob get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete blob"""
        try:
            container = self._require_container()

            blob_client = container.get_blob_client(key)
            blob_client.delete_blob(delete_snapshots="include")

            self.logger.debug(f"Deleted blob key={key}")

            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise StorageError(f"Azure Blob delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List blobs"""
        try:
            container = self._require_container()

            blobs = container.list_blobs(name_starts_with=prefix)
            results = [blob.name for blob in blobs]

            self.logger.debug(f"Listed {len(results)} blobs prefix={prefix}")

            return results

        except Exception as e:
            raise StorageError(f"Azure Blob list failed: {e}")
