# src/polydb/adapters/AzureFileStorageAdapter.py

from polydb.base.SharedFilesAdapter import SharedFilesAdapter
from polydb.errors import ConnectionError, StorageError
from polydb.retry import retry
import os
import threading
from typing import List

class AzureFileStorageAdapter(SharedFilesAdapter):
    """Azure File Storage with client reuse"""

    def __init__(self):
        super().__init__()
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        self.share_name = os.getenv("AZURE_SHARE_NAME", "default") or ""
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Azure File Storage client once"""
        try:
            from azure.storage.fileshare import ShareServiceClient

            with self._lock:
                if not self._client:
                    self._client = ShareServiceClient.from_connection_string(self.connection_string)
                    self.logger.info("Initialized Azure File Storage client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Azure File Storage: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def write(self, path: str, data: bytes) -> bool:
        """Write file"""
        try:
            if self._client:
                file_client = self._client.get_share_client(self.share_name).get_file_client(path)
                file_client.upload_file(data)
                return True
            return False
        except Exception as e:
            raise StorageError(f"Azure File write failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def read(self, path: str) -> bytes | None:
        """Read file"""
        try:
            if self._client:
                file_client = self._client.get_share_client(self.share_name).get_file_client(path)
                return file_client.download_file().readall()
            return None
        except Exception as e:
            raise StorageError(f"Azure File read failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, path: str) -> bool:
        """Delete file"""
        try:
            if self._client:
                file_client = self._client.get_share_client(self.share_name).get_file_client(path)
                file_client.delete_file()
                return True
            return False
        except Exception as e:
            raise StorageError(f"Azure File delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, directory: str = "/") -> List[str]:
        """List files in directory"""
        try:
            if self._client:
                dir_client = self._client.get_share_client(self.share_name).get_directory_client(
                    directory
                )
                return [item.name for item in dir_client.list_directories_and_files()]
            return []
        except Exception as e:
            raise StorageError(f"Azure File list failed: {str(e)}")