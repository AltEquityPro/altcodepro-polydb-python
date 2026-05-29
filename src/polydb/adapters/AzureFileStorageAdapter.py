# src/polydb/adapters/AzureFileStorageAdapter.py

from __future__ import annotations

import os
import threading
from typing import Any, Dict, List, Optional

from ..base.SharedFilesAdapter import SharedFilesAdapter
from ..errors import ConnectionError, StorageError
from ..retry import retry


class AzureFileStorageAdapter(SharedFilesAdapter):
    """
    Production-grade Azure File Storage adapter.

    - One service client, many shares (resolved + cached per call)
    - Auto-creates share + nested directory structure
    - per-call `share_name` overrides the configured share on every method
    """

    def __init__(self, connection_string: str = "", share_name: str = ""):
        super().__init__()

        self.connection_string = (
            connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        )
        self.share_name = share_name or os.getenv("AZURE_SHARE_NAME", "polydb")

        if not self.connection_string:
            raise ConnectionError("AZURE_STORAGE_CONNECTION_STRING not configured")

        self._client = None
        self._shares: Dict[str, Any] = {}
        self._lock = threading.Lock()

        self._initialize_client()

    # --------------------------------------------------
    # Client / share resolution
    # --------------------------------------------------
    def _initialize_client(self):
        from azure.storage.fileshare import ShareServiceClient

        try:
            with self._lock:
                if self._client:
                    return
                self._client = ShareServiceClient.from_connection_string(self.connection_string)
                self.logger.info("Initialized Azure File Storage client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure File Storage: {str(e)}")

    def _get_share(self, share_name: Optional[str] = None):
        """Resolve (and cache) a ShareClient, auto-creating the share."""
        if self._client is None:
            raise ConnectionError("Azure File Storage client is not initialized")

        name = share_name or self.share_name

        cached = self._shares.get(name)
        if cached is not None:
            return cached

        from azure.core.exceptions import ResourceExistsError

        with self._lock:
            cached = self._shares.get(name)  # re-check under lock
            if cached is not None:
                return cached

            share = self._client.get_share_client(name)
            try:
                share.create_share()
                self.logger.info(f"Created share: {name}")
            except ResourceExistsError:
                pass

            self._shares[name] = share
            return share

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    def _split_path(self, path: str):
        if "/" not in path:
            return "", path
        directory, filename = path.rsplit("/", 1)
        return directory, filename

    def _ensure_directory(self, share, directory: str):
        """Create each directory level (Azure requires parents to exist)."""
        if not directory:
            return share.get_directory_client("")

        from azure.core.exceptions import ResourceExistsError

        current = ""
        dir_client = share.get_directory_client("")
        for part in (p for p in directory.split("/") if p):
            current = f"{current}/{part}" if current else part
            dir_client = share.get_directory_client(current)
            try:
                dir_client.create_directory()
            except ResourceExistsError:
                pass
        return dir_client

    # --------------------------------------------------
    # Core operations
    # --------------------------------------------------
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def upload(self, path: str, data: bytes, share_name: Optional[str] = None) -> str:
        """Upload file"""
        try:
            share = self._get_share(share_name)
            directory, filename = self._split_path(path)
            dir_client = self._ensure_directory(share, directory)
            file_client = dir_client.get_file_client(filename)

            file_client.create_file(len(data))
            file_client.upload_file(data)
            return path
        except Exception as e:
            raise StorageError(f"Azure File upload failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def download(self, path: str, share_name: Optional[str] = None) -> bytes:
        """Download file"""
        from azure.core.exceptions import ResourceNotFoundError

        try:
            share = self._get_share(share_name)
            directory, filename = self._split_path(path)
            dir_client = share.get_directory_client(directory or "")
            file_client = dir_client.get_file_client(filename)
            return file_client.download_file().readall()
        except ResourceNotFoundError:
            raise StorageError(f"File not found: {path}")
        except Exception as e:
            raise StorageError(f"Azure File download failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, path: str, share_name: Optional[str] = None) -> bool:
        """Delete file"""
        from azure.core.exceptions import ResourceNotFoundError

        try:
            share = self._get_share(share_name)
            directory, filename = self._split_path(path)
            dir_client = share.get_directory_client(directory or "")
            file_client = dir_client.get_file_client(filename)
            file_client.delete_file()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as e:
            raise StorageError(f"Azure File delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, directory: str = "", share_name: Optional[str] = None) -> List[str]:
        """List files"""
        try:
            share = self._get_share(share_name)
            dir_client = share.get_directory_client(directory or "")
            return [item.name for item in dir_client.list_directories_and_files()]
        except Exception as e:
            raise StorageError(f"Azure File list failed: {str(e)}")

    # --------------------------------------------------
    # Backward compatibility (base interface)
    # --------------------------------------------------
    def write(self, path: str, data: bytes, share_name: Optional[str] = None) -> bool:
        """Alias for upload"""
        self.upload(path, data, share_name=share_name)
        return True

    def read(self, path: str, share_name: Optional[str] = None) -> bytes | None:
        """Alias for download"""
        try:
            return self.download(path, share_name=share_name)
        except StorageError:
            return None
