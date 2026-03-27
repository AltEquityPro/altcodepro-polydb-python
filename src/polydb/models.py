# src/polydb/models.py
"""
Data models and configurations
"""

from dataclasses import dataclass
from typing import Optional, List, Callable, Any
from enum import Enum


class CloudProvider(Enum):
    """Supported cloud providers"""

    AZURE = "azure"
    AWS = "aws"
    GCP = "gcp"
    VERCEL = "vercel"
    MONGODB = "mongodb"
    S3_COMPATIBLE = "s3_compatible"
    POSTGRESQL = "postgresql"
    BLOCKCHAIN = "blockchain"


@dataclass
class PartitionConfig:
    """Configuration for partition and row keys"""

    partition_key_template: str = "default_{id}"
    row_key_template: Optional[str] = None
    composite_keys: Optional[List[str]] = None
    auto_generate: bool = True


@dataclass
class QueryOptions:
    """LINQ-style query options"""

    filter_func: Optional[Callable] = None
    order_by: Optional[str] = None
    skip: int = 0
    take: Optional[int] = None
    select_fields: Optional[List[str]] = None
    count_only: bool = False


# ============================================================
# CONFIG CLASSES (TYPED)
# ============================================================


class StorageConfig:
    def __init__(self, provider: CloudProvider, name: str = "default"):
        self.provider = provider
        self.name = name


class AzureStorageConfig(StorageConfig):
    def __init__(
        self,
        name: str = "default",
        connection_string: Optional[str] = None,
        container: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.container = container


class S3StorageConfig(StorageConfig):
    def __init__(
        self,
        name: str = "default",
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        super().__init__(CloudProvider.S3_COMPATIBLE, name)
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket


class GCPStorageConfig(StorageConfig):
    def __init__(self, name: str = "default", bucket: Optional[str] = None):
        super().__init__(CloudProvider.GCP, name)
        self.bucket = bucket


class VercelStorageConfig(StorageConfig):
    def __init__(self, name: str = "default", token: Optional[str] = None, timeout: int = 10):
        super().__init__(CloudProvider.VERCEL, name)
        self.token = token
        self.timeout = timeout


class BlockchainStorageConfig(StorageConfig):
    def __init__(self, name: str = "default", ipfs_url: Optional[str] = None):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.ipfs_url = ipfs_url
