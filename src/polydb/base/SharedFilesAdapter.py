from polydb.utils import setup_logger


from abc import ABC, abstractmethod
from typing import List


class SharedFilesAdapter(ABC):
    """Base class for Shared File Storage"""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def write(self, path: str, data: bytes) -> bool:
        """Write file"""
        pass

    @abstractmethod
    def read(self, path: str) -> bytes:
        """Read file"""
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        """Delete file"""
        pass

    @abstractmethod
    def list(self, directory: str = "/") -> List[str]:
        """List files in directory"""
        pass