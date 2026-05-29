# src/polydb/adapters/S3CompatibleAdapter.py
import mimetypes
import os
import threading
from typing import Any, Dict, List, Optional

from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry


class S3CompatibleAdapter(ObjectStorageAdapter):
    """S3-compatible storage (AWS S3, MinIO, DigitalOcean Spaces) with client reuse.

    - put/get/delete are symmetric: a blob is stored at `key` and fetched at `key`
    - per-call `container_name` overrides the bucket (generic name kept for
      cross-provider parity with the Azure adapter)
    - get() returns None when the object does not exist
    """

    def __init__(self, bucket_name: str = ""):
        super().__init__()
        self.endpoint = os.getenv("S3_ENDPOINT_URL")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "default")
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize S3-compatible client once."""
        try:
            import boto3  # lazy: boto3 is only required for this provider

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

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _bucket(self, container_name: Optional[str] = None) -> str:
        return container_name or self.bucket_name

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        # inspect botocore ClientError without importing botocore at module load
        resp = getattr(exc, "response", None)
        if isinstance(resp, dict):
            code = str(resp.get("Error", {}).get("Code", ""))
            return code in {"NoSuchKey", "404"}
        return False

    def _url(self, bucket: str, key: str) -> str:
        if self.endpoint:  # MinIO / Spaces / custom endpoint (path-style)
            return f"{self.endpoint.rstrip('/')}/{bucket}/{key}"
        return f"https://{bucket}.s3.amazonaws.com/{key}"  # AWS default (virtual-host)

    # ------------------------------------------------------------------
    # put
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
        """Upload object. Stored at `key` so get/delete find it by the same key."""
        try:
            if not self._client:
                self._initialize_client()

            bucket = self._bucket(container_name)

            # filename is metadata only — it must NOT alter the object key
            filename = fileName or os.path.basename(key) or key
            if media_type:
                ext = mimetypes.guess_extension(media_type) or ""
                if ext and not filename.lower().endswith(ext):
                    filename += ext

            safe_metadata = {k: str(v) for k, v in (metadata or {}).items()}
            safe_metadata["filename"] = filename

            self._client.put_object(  # type: ignore
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType=media_type or "application/octet-stream",
                Metadata=safe_metadata,
            )

            self.logger.debug(f"S3 uploaded: {bucket}/{key}, type={media_type}")
            return self._url(bucket, key)

        except Exception as e:
            raise StorageError(f"S3-compatible put failed: {str(e)}")

    # ------------------------------------------------------------------
    # get / delete / list
    # ------------------------------------------------------------------
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str, container_name: Optional[str] = None) -> bytes | None:
        try:
            if not self._client:
                self._initialize_client()
            response = self._client.get_object(  # type: ignore
                Bucket=self._bucket(container_name), Key=key
            )
            return response["Body"].read()
        except Exception as e:
            if self._is_not_found(e):
                return None
            raise StorageError(f"S3-compatible get failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str, container_name: Optional[str] = None) -> bool:
        try:
            if not self._client:
                self._initialize_client()
            self._client.delete_object(Bucket=self._bucket(container_name), Key=key)  # type: ignore
            return True
        except Exception as e:
            if self._is_not_found(e):
                return False
            raise StorageError(f"S3-compatible delete failed: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "", container_name: Optional[str] = None) -> List[str]:
        try:
            if not self._client:
                self._initialize_client()
            paginator = self._client.get_paginator("list_objects_v2")  # type: ignore
            results: List[str] = []
            for page in paginator.paginate(Bucket=self._bucket(container_name), Prefix=prefix):
                results.extend(obj["Key"] for obj in page.get("Contents", []))
            self.logger.debug(f"S3 listed {len(results)} objects prefix={prefix}")
            return results
        except Exception as e:
            raise StorageError(f"S3-compatible list failed: {str(e)}")
