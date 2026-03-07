# src/polydb/adapters/AzureFileStorageAdapter.py

from __future__ import annotations

import os
import threading
from typing import List, Optional

from azure.storage.fileshare import ShareServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from ..base.SharedFilesAdapter import SharedFilesAdapter
from ..errors import ConnectionError, StorageError
from ..retry import retry


class AzureFileStorageAdapter(SharedFilesAdapter):
    """
    Production-grade Azure File Storage adapter.

    Fixes:
    - Adds upload/download methods expected by tests
    - Ensures share exists
    - Ensures directory structure exists
    - Correct Azure file creation before upload
    - Keeps write/read compatibility
    """

    def __init__(self, connection_string: str = "", share_name: str = ""):
        super().__init__()

        self.connection_string = (
            connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        )

        self.share_name = share_name or os.getenv("AZURE_SHARE_NAME", "polydb")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING not configured")

        self._client: Optional[ShareServiceClient] = None
        self._share = None
        self._lock = threading.Lock()

        self._initialize_client()

    # --------------------------------------------------
    # Client initialization
    # --------------------------------------------------

    def _initialize_client(self):
        try:
            with self._lock:
                if self._client:
                    return

                self._client = ShareServiceClient.from_connection_string(self.connection_string)

                self._share = self._client.get_share_client(self.share_name)

                try:
                    self._share.create_share()
                except ResourceExistsError:
                    pass

                self.logger.info("Initialized Azure File Storage client")

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure File Storage: {str(e)}")

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def _split_path(self, path: str):
        if "/" not in path:
            return "", path

        directory, filename = path.rsplit("/", 1)
        return directory, filename

    def _ensure_directory(self, directory: str):
        if not directory and self._share:
            return self._share.get_directory_client("")
        if not self._share:
            raise ConnectionError("Azure File Storage share not initialized")
        dir_client = self._share.get_directory_client(directory)

        try:
            dir_client.create_directory()
        except ResourceExistsError:
            pass

        return dir_client

    # --------------------------------------------------
    # Core operations
    # --------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def upload(self, path: str, data: bytes) -> str:
        """Upload file"""
        try:
            directory, filename = self._split_path(path)

            dir_client = self._ensure_directory(directory)
            file_client = dir_client.get_file_client(filename)

            file_client.create_file(len(data))
            file_client.upload_file(data)

            return path

        except Exception as e:
            raise StorageError(f"Azure File upload failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def download(self, path: str) -> bytes:
        """Download file"""
        try:
            directory, filename = self._split_path(path)
            if not self._share:
                raise ConnectionError("Azure File Storage share not initialized")
            dir_client = self._share.get_directory_client(directory or "")
            file_client = dir_client.get_file_client(filename)

            return file_client.download_file().readall()

        except ResourceNotFoundError:
            raise StorageError(f"File not found: {path}")
        except Exception as e:
            raise StorageError(f"Azure File download failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, path: str) -> bool:
        """Delete file"""
        try:
            directory, filename = self._split_path(path)
            if not self._share:
                raise ConnectionError("Azure File Storage share not initialized")
            dir_client = self._share.get_directory_client(directory or "")
            file_client = dir_client.get_file_client(filename)

            file_client.delete_file()
            return True

        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise StorageError(f"Azure File delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, directory: str = "") -> List[str]:
        """List files"""
        try:
            if not self._share:
                raise ConnectionError("Azure File Storage share not initialized")
            dir_client = self._share.get_directory_client(directory or "")

            results: List[str] = []

            for item in dir_client.list_directories_and_files():
                results.append(item.name)

            return results

        except Exception as e:
            raise StorageError(f"Azure File list failed: {str(e)}")

    # --------------------------------------------------
    # Backward compatibility
    # --------------------------------------------------

    def write(self, path: str, data: bytes) -> bool:
        """Alias for upload"""
        self.upload(path, data)
        return True

    def read(self, path: str) -> bytes | None:
        """Alias for download"""
        try:
            return self.download(path)
        except StorageError:
            return None
