# src/polydb/adapters/EFSAdapter.py

from polydb.base.SharedFilesAdapter import SharedFilesAdapter
from polydb.errors import StorageError
import os
from typing import List

class EFSAdapter(SharedFilesAdapter):
    """AWS EFS (mounted filesystem)"""

    def __init__(self):
        super().__init__()
        self.mount_point = os.getenv("EFS_MOUNT_POINT", "/mnt/efs")

    def write(self, path: str, data: bytes) -> bool:
        """Write file"""
        try:
            full_path = os.path.join(self.mount_point, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(data)
            return True
        except Exception as e:
            raise StorageError(f"EFS write failed: {str(e)}")

    def read(self, path: str) -> bytes:
        """Read file"""
        try:
            full_path = os.path.join(self.mount_point, path)
            with open(full_path, "rb") as f:
                return f.read()
        except Exception as e:
            raise StorageError(f"EFS read failed: {str(e)}")

    def delete(self, path: str) -> bool:
        """Delete file"""
        try:
            full_path = os.path.join(self.mount_point, path)
            os.remove(full_path)
            return True
        except Exception as e:
            raise StorageError(f"EFS delete failed: {str(e)}")

    def list(self, directory: str = "/") -> List[str]:
        """List files in directory"""
        try:
            full_path = os.path.join(self.mount_point, directory)
            return os.listdir(full_path)
        except Exception as e:
            raise StorageError(f"EFS list failed: {str(e)}")