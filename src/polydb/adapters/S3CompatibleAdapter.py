# src/polydb/adapters/S3CompatibleAdapter.py
import mimetypes
import os
import threading
from typing import Any, Dict, List, Optional
from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry


class S3CompatibleAdapter(ObjectStorageAdapter):
    """S3-compatible storage (MinIO, DigitalOcean Spaces) with client reuse"""

    def __init__(self):
        super().__init__()
        self.endpoint = os.getenv("S3_ENDPOINT_URL")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "default")
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize S3-compatible client once"""
        try:
            import boto3

            with self._lock:
                if not self._client:
                    self._client = boto3.client(
                        "s3",
                        endpoint_url=self.endpoint,
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key=self.secret_key,
                    )
                    self.logger.info("Initialized S3-compatible client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize S3-compatible client: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(
        self,
        key: str,
        data: bytes,
        fileName: str = "",
        media_type: Optional[str] = None,
        metadata: Dict[str, Any] | None = None,
    ) -> str:
        """Upload object to S3-compatible storage with metadata and media type"""
        try:
            if not self._client:
                self._initialize_client()

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
            # Final key
            # --------------------------------------------------
            blob_key = f"{key.rstrip('/')}/{filename}" if fileName else key

            # --------------------------------------------------
            # Metadata (string only)
            # --------------------------------------------------
            safe_metadata = {k: str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename

            # --------------------------------------------------
            # Upload
            # --------------------------------------------------
            self._client.put_object(  # type: ignore
                Bucket=self.bucket_name,
                Key=blob_key,
                Body=data,
                ContentType=media_type or "application/octet-stream",
                Metadata=safe_metadata,
            )

            self.logger.debug(f"S3 uploaded: {blob_key}, type={media_type}")

            # --------------------------------------------------
            # Return URL
            # --------------------------------------------------
            if self.endpoint:
                # MinIO / Spaces / custom endpoint
                url = f"{self.endpoint.rstrip('/')}/{self.bucket_name}/{blob_key}"
            else:
                # AWS S3 default
                url = f"https://{self.bucket_name}.s3.amazonaws.com/{blob_key}"

            return url

        except Exception as e:
            raise StorageError(f"S3-compatible put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Get object"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                response = self._client.get_object(Bucket=self.bucket_name, Key=key)
                return response["Body"].read()
            return None
        except Exception as e:
            raise StorageError(f"S3-compatible get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                self._client.delete_object(Bucket=self.bucket_name, Key=key)
                return True
            return False
        except Exception as e:
            raise StorageError(f"S3-compatible delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                response = self._client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
                return [obj["Key"] for obj in response.get("Contents", [])]
            return []
        except Exception as e:
            raise StorageError(f"S3-compatible list failed: {str(e)}")
