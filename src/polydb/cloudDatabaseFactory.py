from __future__ import annotations

import os
import threading
from typing import Dict, List, Optional

from .adapters.PostgreSQLAdapter import PostgreSQLAdapter
from .adapters.AzureBlobStorageAdapter import AzureBlobStorageAdapter
from .adapters.BlockchainBlobAdapter import BlockchainBlobAdapter
from .adapters.GCPStorageAdapter import GCPStorageAdapter
from .adapters.S3CompatibleAdapter import S3CompatibleAdapter
from .adapters.VercelBlobAdapter import VercelBlobAdapter

from .models import (
    AzureStorageConfig,
    BlockchainStorageConfig,
    CloudProvider,
    GCPStorageConfig,
    PartitionConfig,
    StorageConfig,
    VercelStorageConfig,
)
from .utils import setup_logger


# ============================================================
# FACTORY
# ============================================================


class CloudDatabaseFactory:
    """
    Multi-cloud factory (simple + production ready)

    Supports:
    - Typed configs (preferred)
    - Env fallback
    - Multiple named connections
    """

    def __init__(
        self,
        provider: Optional[CloudProvider] = None,
        storage_configs: Optional[List[StorageConfig]] = None,
    ):
        self.logger = setup_logger(__name__)

        self.provider = provider or self._detect_provider()

        # map name -> config
        self.configs: Dict[str, StorageConfig] = {}
        if storage_configs:
            for cfg in storage_configs:
                self.configs[cfg.name] = cfg

        self.instances: Dict[
            str,
            PostgreSQLAdapter
            | AzureBlobStorageAdapter
            | S3CompatibleAdapter
            | GCPStorageAdapter
            | VercelBlobAdapter
            | BlockchainBlobAdapter,
        ] = {}
        self._lock = threading.Lock()

        self.logger.info(f"Factory initialized (default provider={self.provider.value})")

    # --------------------------------------------------------
    # Provider detection (env fallback)
    # --------------------------------------------------------
    def _detect_provider(self) -> CloudProvider:
        explicit = os.getenv("CLOUD_PROVIDER")
        if explicit:
            try:
                return CloudProvider(explicit.lower())
            except Exception:
                self.logger.warning(f"Invalid CLOUD_PROVIDER: {explicit}")

        if os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
            return CloudProvider.AZURE
        if os.getenv("AWS_ACCESS_KEY_ID"):
            return CloudProvider.AWS
        if os.getenv("GOOGLE_CLOUD_PROJECT"):
            return CloudProvider.GCP
        if os.getenv("VERCEL_ENV"):
            return CloudProvider.VERCEL

        return CloudProvider.POSTGRESQL

    # --------------------------------------------------------
    # OBJECT STORAGE
    # --------------------------------------------------------
    def get_object_storage(
        self, name: str = "default"
    ) -> (
        PostgreSQLAdapter
        | AzureBlobStorageAdapter
        | S3CompatibleAdapter
        | GCPStorageAdapter
        | VercelBlobAdapter
        | BlockchainBlobAdapter
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            provider = cfg.provider

            # ---------------- AZURE ----------------
            if provider == CloudProvider.AZURE:
                from .adapters.AzureBlobStorageAdapter import AzureBlobStorageAdapter

                connection_string = None
                container = None

                if isinstance(cfg, AzureStorageConfig):
                    connection_string = cfg.connection_string
                    container = cfg.container

                instance = AzureBlobStorageAdapter(
                    connection_string=connection_string or "",
                    container_name=container or "",
                )

            # ---------------- AWS / S3 ----------------
            elif provider in (CloudProvider.AWS, CloudProvider.S3_COMPATIBLE):
                from .adapters.S3CompatibleAdapter import S3CompatibleAdapter

                instance = S3CompatibleAdapter()

            # ---------------- GCP ----------------
            elif provider == CloudProvider.GCP:
                from .adapters.GCPStorageAdapter import GCPStorageAdapter

                bucket = None
                if isinstance(cfg, GCPStorageConfig):
                    bucket = cfg.bucket

                instance = GCPStorageAdapter(bucket_name=bucket)

            # ---------------- VERCEL ----------------
            elif provider == CloudProvider.VERCEL:
                from .adapters.VercelBlobAdapter import VercelBlobAdapter

                token = None
                timeout = 10

                if isinstance(cfg, VercelStorageConfig):
                    token = cfg.token
                    timeout = cfg.timeout

                instance = VercelBlobAdapter(token=token or "", timeout=timeout)

            # ---------------- BLOCKCHAIN ----------------
            elif provider == CloudProvider.BLOCKCHAIN:
                from .adapters.BlockchainBlobAdapter import BlockchainBlobAdapter

                ipfs_url = None
                if isinstance(cfg, BlockchainStorageConfig):
                    ipfs_url = cfg.ipfs_url

                instance = BlockchainBlobAdapter(ipfs_url=ipfs_url)

            # ---------------- DEFAULT ----------------
            else:
                from .adapters.S3CompatibleAdapter import S3CompatibleAdapter

                self.logger.warning(f"Fallback to S3-compatible for provider={provider}")
                instance = S3CompatibleAdapter()

            self.instances[name] = instance
            return instance

    # --------------------------------------------------------
    # KV / NoSQL
    # --------------------------------------------------------
    def get_nosql_kv(
        self,
        partition_config: Optional[PartitionConfig] = None,
    ):
        if self.provider == CloudProvider.AZURE:
            from .adapters.AzureTableStorageAdapter import AzureTableStorageAdapter

            return AzureTableStorageAdapter(partition_config)

        if self.provider == CloudProvider.AWS:
            from .adapters.DynamoDBAdapter import DynamoDBAdapter

            return DynamoDBAdapter(partition_config)

        if self.provider == CloudProvider.GCP:
            from .adapters.FirestoreAdapter import FirestoreAdapter

            return FirestoreAdapter(partition_config)

        if self.provider == CloudProvider.VERCEL:
            from .adapters.VercelKVAdapter import VercelKVAdapter

            return VercelKVAdapter(partition_config)

        if self.provider == CloudProvider.BLOCKCHAIN:
            from .adapters.BlockchainKVAdapter import BlockchainKVAdapter

            return BlockchainKVAdapter()

        from .adapters.MongoDBAdapter import MongoDBAdapter

        return MongoDBAdapter(partition_config)

    # --------------------------------------------------------
    # SQL
    # --------------------------------------------------------
    def get_sql(self):
        from .adapters.PostgreSQLAdapter import PostgreSQLAdapter

        with self._lock:
            if "sql" not in self.instances:
                self.instances["sql"] = PostgreSQLAdapter()
            return self.instances["sql"]
