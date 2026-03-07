import os
import requests
from typing import List
from pathlib import Path

from ..errors import StorageError
from ..retry import retry
from ..utils import setup_logger


class VercelBlobAdapter:
    """
    Vercel Blob Storage adapter.

    If BLOB_READ_WRITE_TOKEN is missing, falls back to
    local filesystem storage for testing.
    """

    def __init__(self, token: str = "", timeout: int = 10):

        self.logger = setup_logger(self.__class__.__name__)

        self.token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
        self.timeout = timeout or int(os.getenv("VERCEL_BLOB_TIMEOUT", "10"))

        # Local fallback storage for tests
        self.local_dir = Path(os.getenv("VERCEL_BLOB_LOCAL_DIR", "/tmp/vercel_blob"))
        self.local_dir.mkdir(parents=True, exist_ok=True)

        self.local_mode = not bool(self.token)

    # ---------------------------------------------------------
    # PUT
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def put(self, key: str, data: bytes) -> str:
        try:

            # LOCAL MODE (tests)
            if self.local_mode:
                path = self.local_dir / key
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                return str(path)

            # REAL VERCEL
            response = requests.put(
                f"https://blob.vercel-storage.com/{key}",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "x-content-type": "application/octet-stream",
                },
                data=data,
                timeout=self.timeout,
            )

            response.raise_for_status()

            result = response.json()

            return result.get("url") or result.get("pathname") or key

        except Exception as e:
            raise StorageError(f"Vercel Blob put failed: {e}")

    # ---------------------------------------------------------
    # GET
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes:
        try:

            if self.local_mode:
                path = self.local_dir / key
                if not path.exists():
                    raise StorageError("Object not found")
                return path.read_bytes()

            response = requests.get(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            response.raise_for_status()

            return response.content

        except Exception as e:
            raise StorageError(f"Vercel Blob get failed: {e}")

    # ---------------------------------------------------------
    # DELETE
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        try:

            if self.local_mode:
                path = self.local_dir / key
                if path.exists():
                    path.unlink()
                return True

            response = requests.delete(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            response.raise_for_status()

            return True

        except Exception as e:
            raise StorageError(f"Vercel Blob delete failed: {e}")

    # ---------------------------------------------------------
    # LIST
    # ---------------------------------------------------------

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        try:

            if self.local_mode:
                results = []
                for p in self.local_dir.rglob("*"):
                    if p.is_file():
                        rel = str(p.relative_to(self.local_dir))
                        if rel.startswith(prefix):
                            results.append(rel)
                return results

            response = requests.get(
                f"https://blob.vercel-storage.com/?prefix={prefix}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            response.raise_for_status()

            blobs = response.json().get("blobs", [])

            return [b.get("pathname") for b in blobs]

        except Exception as e:
            raise StorageError(f"Vercel Blob list failed: {e}")

    # ---------------------------------------------------------
    # PUBLIC API (aliases used in tests)
    # ---------------------------------------------------------

    def upload(self, key: str, data: bytes) -> str:
        return self.put(key, data)

    def download(self, key: str) -> bytes:
        return self.get(key)