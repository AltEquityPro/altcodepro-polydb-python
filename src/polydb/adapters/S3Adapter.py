# src/polydb/adapters/S3Adapter.py

"""
S3 adapter (AWS + LocalStack compatible)
"""

from __future__ import annotations

import mimetypes
import os
import threading
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry


class S3Adapter(ObjectStorageAdapter):
    """AWS S3 adapter with client reuse and automatic bucket creation"""

    def __init__(self, bucket_name: str = "", region: str = "", endpoint_url: str = ""):
        super().__init__()

        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "polydb-test")

        self.region = (
            region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
        )

        self.endpoint_url = endpoint_url or os.getenv("AWS_ENDPOINT_URL")

        self._client: Any = None
        self._lock = threading.Lock()

        self._initialize_client()

    # ---------------------------------------------------------
    # Client initialization
    # ---------------------------------------------------------

    def _initialize_client(self):
        """Initialize S3 client once"""
        try:
            with self._lock:
                if self._client:
                    return

                self._client = boto3.client(
                    "s3",
                    region_name=self.region,
                    endpoint_url=self.endpoint_url,
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                )

                self._ensure_bucket_exists()

                self.logger.info(
                    f"Initialized S3 client (region={self.region}, endpoint={self.endpoint_url or 'aws'})"
                )

        except Exception as e:
            raise ConnectionError(f"Failed to initialize S3 client: {e}")

    # ---------------------------------------------------------
    # Bucket management
    # ---------------------------------------------------------

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist (safe for AWS + LocalStack)"""
        if not self._client:
            return

        try:
            self._client.head_bucket(Bucket=self.bucket_name)
            return
        except ClientError:
            pass

        try:
            if self.region == "us-east-1":
                self._client.create_bucket(Bucket=self.bucket_name)
            else:
                self._client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )

            self.logger.info(f"Created S3 bucket: {self.bucket_name}")

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise StorageError(f"S3 bucket creation failed: {e}")

    # ---------------------------------------------------------
    # Core operations
    # ---------------------------------------------------------

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
            if self.endpoint_url:
                # MinIO / Spaces / custom endpoint
                url = f"{self.endpoint_url.rstrip('/')}/{self.bucket_name}/{blob_key}"
            else:
                # AWS S3 default
                url = f"https://{self.bucket_name}.s3.amazonaws.com/{blob_key}"

            return url

        except Exception as e:
            raise StorageError(f"S3-compatible put failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes | None:
        """Download object"""
        try:
            if not self._client:
                self._initialize_client()

            response = self._client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )

            return response["Body"].read()

        except self._client.exceptions.NoSuchKey:  # type: ignore
            return None
        except Exception as e:
            raise StorageError(f"S3 get failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        """Delete object"""
        try:
            if not self._client:
                self._initialize_client()

            self._client.delete_object(
                Bucket=self.bucket_name,
                Key=key,
            )

            return True

        except Exception as e:
            raise StorageError(f"S3 delete failed: {e}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        try:
            if not self._client:
                self._initialize_client()

            response = self._client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
            )

            contents = response.get("Contents", [])

            return [obj["Key"] for obj in contents]

        except Exception as e:
            raise StorageError(f"S3 list failed: {e}")
