from ..errors import StorageError
from ..utils import setup_logger
from abc import ABC, abstractmethod
from typing import List, Optional


class ObjectStorageAdapter(ABC):
    """Base class for Object Storage with automatic optimization"""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    def put(
        self, key: str, data: bytes, optimize: bool = True, media_type: Optional[str] = None
    ) -> str:
        """Store object with optional optimization"""
        if optimize and media_type:
            data = self._optimize_media(data, media_type)
        return self._put_raw(key, data)

    def _optimize_media(self, data: bytes, media_type: str) -> bytes:
        """Optimize images and videos - placeholder for implementation"""
        return data

    @abstractmethod
    def _put_raw(self, key: str, data: bytes) -> str:
        """Provider-specific put"""
        pass

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Get object"""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete object"""
        pass

    @abstractmethod
    def list(self, prefix: str = "") -> List[str]:
        """List objects with prefix"""
        pass

    def upload(self, key: str, data: bytes, **kwargs) -> str:
        """
        Alias for put().
        Accepts kwargs so callers can pass optimize/media_type without breaking.
        """
        return self.put(key, data, **kwargs)

    def download(self, key: str) -> bytes:
        """
        Alias for get() but guarantees bytes or raises.
        This matches how your tests expect download() to behave.
        """
        if not key:
            raise StorageError("Key cannot be empty")

        data = self.get(key)
        if data is None:
            raise StorageError(f"Object not found: {key}")
        return data
