from ..utils import setup_logger
from abc import ABC, abstractmethod
from typing import Optional


class SecretsAdapter(ABC):
    """Base class for cloud secret managers (Key Vault / Secrets Manager /
    Secret Manager / Vault) -- same shape as the other base adapters
    (ObjectStorageAdapter, QueueAdapter, ...), dispatched the same way via
    CloudDatabaseFactory.get_secrets()."""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def get_secret(self, key: str) -> Optional[str]:
        """Return the current value of a secret, or None if it doesn't exist."""
        raise NotImplementedError

    @abstractmethod
    def set_secret(self, key: str, value: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_secret(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def list_secrets(self, prefix: str = "") -> list[str]:
        raise NotImplementedError
