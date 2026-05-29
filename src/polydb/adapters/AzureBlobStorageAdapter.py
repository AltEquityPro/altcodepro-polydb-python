# src/polydb/adapters/AzureBlobStorageAdapter.py

import os
import threading
import mimetypes
from typing import Any, Dict, List, Optional
from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import ConnectionError, StorageError
from ..retry import retry


class AzureBlobStorageAdapter(ObjectStorageAdapter):
    """
    Production-grade Azure Blob Storage adapter.

    - One BlobServiceClient, many containers (resolved + cached per call)
    - Container auto-creation
    - Thread-safe, retryable, structured logging
    - put/get/delete are symmetric: a blob is stored at `key` and fetched at `key`
    """

    def __init__(self, connection_string: str = "", container_name: str = ""):
        super().__init__()

        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = container_name or os.getenv("AZURE_CONTAINER_NAME", "polydb")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING is not configured")

        self._client = None
        self._containers = {}
        self._lock = threading.Lock()

        self._initialize_client()

    # ------------------------------------------------------------------
    # CLIENT / CONTAINER RESOLUTION
    # ------------------------------------------------------------------
    def _initialize_client(self) -> None:
        """Initialize the shared Azure Blob service client."""
        from azure.storage.blob import BlobServiceClient

        try:
            with self._lock:
                if self._client is None and self.connection_string is not None:
                    self._client = BlobServiceClient.from_connection_string(self.connection_string)
                    self.logger.info("Azure Blob Storage client initialized")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure Blob Storage: {e}")

    def _get_container(self, container_name: Optional[str] = None):
        """Resolve (and cache) a ContainerClient, auto-creating the container."""
        from azure.core.exceptions import ResourceExistsError

        if self._client is None:
            raise ConnectionError("Azure Blob Storage client is not initialized")

        name = container_name or self.container_name

        cached = self._containers.get(name)
        if cached is not None:
            return cached

        with self._lock:
            cached = self._containers.get(name)  # re-check under lock
            if cached is not None:
                return cached

            container = self._client.get_container_client(name)
            try:
                container.create_container()
                self.logger.info(f"Created container: {name}")
            except ResourceExistsError:
                pass

            self._containers[name] = container
            return container

    # ------------------------------------------------------------------
    # PUT
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
        """Upload blob. Stored at `key` so get/delete can find it by the same key."""
        from azure.storage.blob import ContentSettings

        try:
            container = self._get_container(container_name)

            # filename is metadata only — it must NOT alter the blob key
            filename = fileName or os.path.basename(key) or key
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            blob_client = container.get_blob_client(key)
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type=media_type or "application/octet-stream"
                ),
                metadata={**(metadata or {}), "filename": filename},
            )

            self.logger.debug(
                f"Uploaded blob key={key} container={container.container_name} type={media_type}"
            )
            return blob_client.url

        except Exception as e:
            raise StorageError(f"Azure Blob put failed: {e}")

    # ------------------------------------------------------------------
    # GET / DELETE / LIST
    # ------------------------------------------------------------------
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str, container_name: Optional[str] = None) -> bytes | None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            container = self._get_container(container_name)
            data = container.get_blob_client(key).download_blob().readall()
            self.logger.debug(f"Downloaded blob key={key}")
            return data
        except ResourceNotFoundError:
            return None
        except Exception as e:
            raise StorageError(f"Azure Blob get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str, container_name: Optional[str] = None) -> bool:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            container = self._get_container(container_name)
            container.get_blob_client(key).delete_blob(delete_snapshots="include")
            self.logger.debug(f"Deleted blob key={key}")
            return True
        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise StorageError(f"Azure Blob delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "", container_name: Optional[str] = None) -> List[str]:
        try:
            container = self._get_container(container_name)
            results = [b.name for b in container.list_blobs(name_starts_with=prefix)]
            self.logger.debug(f"Listed {len(results)} blobs prefix={prefix}")
            return results
        except Exception as e:
            raise StorageError(f"Azure Blob list failed: {e}")
