# src/polydb/adapters/GCPFilestoreAdapter.py

import os
from typing import List, Optional

from ..base.SharedFilesAdapter import SharedFilesAdapter
from ..errors import StorageError, ConnectionError


class FilestoreAdapter(SharedFilesAdapter):
    """
    GCP Filestore — managed NFS mounted onto the host at a path (e.g. /mnt/filestore).
    Like EFS, it's a POSIX directory once mounted, so ops are plain os/open calls.

    - per-call `share_name` selects a sub-root directory under the mount point
    - paths are confined to the resolved root (no traversal escapes)
    """

    def __init__(self, mount_point: str = ""):
        super().__init__()
        self.mount_point = mount_point or os.getenv("FILESTORE_MOUNT_POINT", "/mnt/filestore")
        if not self.mount_point:
            raise ConnectionError("Filestore mount_point is required")

    def _resolve(self, path: str, share_name: Optional[str] = None) -> str:
        base = os.path.join(self.mount_point, share_name) if share_name else self.mount_point
        base_abs = os.path.abspath(base)
        rel = (path or "").lstrip("/\\")
        full_abs = os.path.abspath(os.path.join(base_abs, rel))
        if full_abs != base_abs and not full_abs.startswith(base_abs + os.sep):
            raise StorageError(f"Path escapes storage root: {path}")
        return full_abs

    def write(self, path: str, data: bytes, share_name: Optional[str] = None) -> bool:
        try:
            full_path = self._resolve(path, share_name)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(data)
            return True
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Filestore write failed: {str(e)}")

    def read(self, path: str, share_name: Optional[str] = None) -> bytes | None:
        try:
            with open(self._resolve(path, share_name), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Filestore read failed: {str(e)}")

    def delete(self, path: str, share_name: Optional[str] = None) -> bool:
        try:
            os.remove(self._resolve(path, share_name))
            return True
        except FileNotFoundError:
            return False
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Filestore delete failed: {str(e)}")

    def list(self, directory: str = "", share_name: Optional[str] = None) -> List[str]:
        try:
            full_path = self._resolve(directory, share_name)
            if not os.path.isdir(full_path):
                return []
            return os.listdir(full_path)
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Filestore list failed: {str(e)}")
