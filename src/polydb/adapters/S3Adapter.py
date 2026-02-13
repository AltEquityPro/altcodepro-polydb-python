# src/polydb/adapters/S3Adapter.py
"""
S3 adapter
"""

import os
import threading
from typing import List, Optional
from ..base.ObjectStorageAdapter import ObjectStorageAdapter
from ..errors import StorageError, ConnectionError
from ..retry import retry
class S3Adapter(ObjectStorageAdapter):
    """AWS S3 with client reuse"""

    def __init__(self):
        super().__init__()
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "default")
        self._client = None
        self._lock = threading.Lock()
        self._initialize_client()

    def _initialize_client(self):
        """Initialize S3 client once"""
        try:
            import boto3

            with self._lock:
                if not self._client:
                    self._client = boto3.client("s3")
                    self.logger.info("Initialized S3 client")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize S3 client: {str(e)}")

    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _put_raw(self, key: str, data: bytes) -> str:
        """Store object"""
        try:
            if not self._client:
                self._initialize_client()
            if self._client:
                self._client.put_object(Bucket=self.bucket_name, Key=key, Body=data)
                self.logger.debug(f"Uploaded to S3: {key}")
            return key
        except Exception as e:
            raise StorageError(f"S3 put failed: {str(e)}")

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
            raise StorageError(f"S3 get failed: {str(e)}")

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
            raise StorageError(f"S3 delete failed: {str(e)}")

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
            raise StorageError(f"S3 list failed: {str(e)}")


