import os
import mimetypes
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError
from ..retry import retry


class VercelBlobAdapter(ObjectStorageAdapter):
    """
    Vercel Blob Storage adapter.

    If BLOB_READ_WRITE_TOKEN is missing, falls back to
    local filesystem storage for testing.
    """

    def __init__(self, token: str = "", timeout: int = 10):
        super().__init__()

        self.token = token or os.getenv("BLOB_READ_WRITE_TOKEN")
        self.timeout = timeout or int(os.getenv("VERCEL_BLOB_TIMEOUT", "10"))

        # Local fallback storage for tests
        self.local_dir = Path(os.getenv("VERCEL_BLOB_LOCAL_DIR", "/tmp/vercel_blob"))
        self.local_dir.mkdir(parents=True, exist_ok=True)

        self.local_mode = not bool(self.token)

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload object to Vercel Blob or local fallback and return URL/path"""
        try:
            # --------------------------------------------------
            # Resolve filename
            # --------------------------------------------------
            filename = fileName or os.path.basename(key)

            # --------------------------------------------------
            # Ensure extension from media_type
            # --------------------------------------------------
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            # --------------------------------------------------
            # Final blob key
            # --------------------------------------------------
            blob_key = f"{key.rstrip('/')}/{filename}" if fileName else key

            # --------------------------------------------------
            # Metadata (string-safe)
            # --------------------------------------------------
            safe_metadata = {str(k): str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename
            if media_type:
                safe_metadata["contentType"] = media_type

            # --------------------------------------------------
            # LOCAL MODE
            # --------------------------------------------------
            if self.local_mode:
                path = self.local_dir / blob_key
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)

                meta_path = path.with_suffix(path.suffix + ".metadata.json")
                try:
                    import json

                    meta_path.write_text(json.dumps(safe_metadata, ensure_ascii=False, indent=2))
                except Exception:
                    pass

                self.logger.debug(f"Stored local Vercel blob: {blob_key}")
                return str(path)

            # --------------------------------------------------
            # REAL VERCEL
            # --------------------------------------------------
            headers = {
                "Authorization": f"Bearer {self.token}",
                "x-content-type": media_type or "application/octet-stream",
            }

            for k, v in safe_metadata.items():
                headers[f"x-metadata-{k}"] = v

            response = requests.put(
                f"https://blob.vercel-storage.com/{blob_key}",
                headers=headers,
                data=data,
                timeout=self.timeout,
            )
            response.raise_for_status()

            result = response.json()

            self.logger.debug(f"Uploaded Vercel blob: {blob_key}, type={media_type}")

            return (
                result.get("url") or result.get("downloadUrl") or result.get("pathname") or blob_key
            )

        except Exception as e:
            raise StorageError(f"Vercel Blob put failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        try:
            if self.local_mode:
                path = self.local_dir / key
                if not path.exists():
                    return None
                return path.read_bytes()

            response = requests.get(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.content

        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Vercel Blob get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        try:
            if self.local_mode:
                path = self.local_dir / key
                if path.exists():
                    path.unlink()
                meta_path = path.with_suffix(path.suffix + ".metadata.json")
                if meta_path.exists():
                    meta_path.unlink()
                return True

            response = requests.delete(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )

            if response.status_code == 404:
                return False

            response.raise_for_status()
            return True

        except Exception as e:
            raise StorageError(f"Vercel Blob delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        try:
            if self.local_mode:
                results: List[str] = []
                for p in self.local_dir.rglob("*"):
                    if p.is_file() and not p.name.endswith(".metadata.json"):
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
            return [b.get("pathname") for b in blobs if b.get("pathname")]

        except Exception as e:
            raise StorageError(f"Vercel Blob list failed: {e}")
