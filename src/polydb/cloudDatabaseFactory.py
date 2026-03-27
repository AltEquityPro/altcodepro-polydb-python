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
    AzureFileConfig,
    AzureQueueConfig,
    AzureStorageConfig,
    AzureTableConfig,
    BlockchainKVConfig,
    BlockchainQueueConfig,
    BlockchainStorageConfig,
    CloudProvider,
    DynamoDBConfig,
    EFSFileConfig,
    FirestoreConfig,
    GCPFileConfig,
    GCPPubSubConfig,
    GCPStorageConfig,
    MongoConfig,
    PartitionConfig,
    PostgreSQLConfig,
    SQSAdapterConfig,
    StorageConfig,
    VercelKVConfig,
    VercelQueueConfig,
    VercelStorageConfig,
)
from .utils import setup_logger
from .adapters.AzureQueueAdapter import AzureQueueAdapter
from .adapters.GCPPubSubAdapter import GCPPubSubAdapter
from .adapters.VercelQueueAdapter import VercelQueueAdapter
from .adapters.BlockchainQueueAdapter import BlockchainQueueAdapter
from .adapters.SQSAdapter import SQSAdapter


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

        self.instances = {}
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
        self, name: str = "azure"
    ) -> (
        AzureBlobStorageAdapter
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
                project_id = ""
                endpoint = None
                if isinstance(cfg, GCPStorageConfig):
                    bucket = cfg.bucket
                    project_id = cfg.project_id
                    endpoint = cfg.endpoint

                instance = GCPStorageAdapter(
                    project_id=project_id, endpoint=endpoint, bucket_name=bucket
                )

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
    # SQL
    # --------------------------------------------------------
    def get_sql(self, name: str = "sql"):
        from .adapters.PostgreSQLAdapter import PostgreSQLAdapter

        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            connection_string = None

            if isinstance(cfg, PostgreSQLConfig):
                connection_string = cfg.connection_string

            instance = PostgreSQLAdapter(connection_string=connection_string)

            self.instances[name] = instance
            return instance

    def get_nosql_kv(
        self,
        partition_config: Optional[PartitionConfig] = None,
        name: str = "kv",
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            # ---------------- AZURE TABLE ----------------
            if cfg.provider == CloudProvider.AZURE:
                from .adapters.AzureTableStorageAdapter import AzureTableStorageAdapter

                connection_string = ""
                table_name = ""
                container_name = ""

                if isinstance(cfg, AzureTableConfig):
                    connection_string = cfg.connection_string or ""
                    table_name = cfg.table_name
                    container_name = cfg.container_name

                instance = AzureTableStorageAdapter(
                    partition_config=partition_config,
                    connection_string=connection_string,
                    table_name=table_name,
                    container_name=container_name,
                )

            # ---------------- AWS DYNAMODB ----------------
            elif cfg.provider == CloudProvider.AWS:
                from .adapters.DynamoDBAdapter import DynamoDBAdapter

                table_name = None
                bucket_name = None
                region = None
                endpoint_url = None

                if isinstance(cfg, DynamoDBConfig):
                    table_name = cfg.table_name
                    bucket_name = cfg.bucket_name
                    region = cfg.region
                    endpoint_url = cfg.endpoint_url

                instance = DynamoDBAdapter(
                    partition_config=partition_config,
                    table_name=table_name,
                    bucket_name=bucket_name,
                    region=region,
                    endpoint_url=endpoint_url,
                )

            # ---------------- GCP FIRESTORE ----------------
            elif cfg.provider == CloudProvider.GCP:
                from .adapters.FirestoreAdapter import FirestoreAdapter

                project = None
                bucket_name = None

                if isinstance(cfg, FirestoreConfig):
                    project = cfg.project
                    bucket_name = cfg.bucket_name

                instance = FirestoreAdapter(
                    partition_config=partition_config,
                    project=project,
                    bucket_name=bucket_name,
                )

            # ---------------- VERCEL KV ----------------
            elif cfg.provider == CloudProvider.VERCEL:
                from .adapters.VercelKVAdapter import VercelKVAdapter

                kv_url = ""
                kv_token = ""
                timeout = 10

                if isinstance(cfg, VercelKVConfig):
                    kv_url = cfg.kv_url
                    kv_token = cfg.kv_token
                    timeout = cfg.timeout

                instance = VercelKVAdapter(
                    partition_config=partition_config,
                    kv_url=kv_url,
                    kv_token=kv_token,
                    timeout=timeout,
                )

            # ---------------- BLOCKCHAIN KV ----------------
            elif cfg.provider == CloudProvider.BLOCKCHAIN:
                from .adapters.BlockchainKVAdapter import BlockchainKVAdapter

                chain = None
                rpc_url = None
                private_key = None
                contract_address = None
                contract_abi = None

                if isinstance(cfg, BlockchainKVConfig):
                    chain = cfg.chain
                    rpc_url = cfg.rpc_url
                    private_key = cfg.private_key
                    contract_address = cfg.contract_address
                    contract_abi = cfg.contract_abi

                instance = BlockchainKVAdapter(
                    chain=chain,
                    rpc_url=rpc_url,
                    private_key=private_key,
                    contract_address=contract_address,
                    contract_abi=contract_abi,
                )

            # ---------------- MONGODB ----------------
            else:
                from .adapters.MongoDBAdapter import MongoDBAdapter

                mongo_uri = ""
                db_name = ""

                if isinstance(cfg, MongoConfig):
                    mongo_uri = cfg.mongo_uri
                    db_name = cfg.db_name

                instance = MongoDBAdapter(
                    partition_config=partition_config,
                    mongo_uri=mongo_uri,
                    db_name=db_name,
                )

            self.instances[name] = instance
            return instance

    def get_queue(
        self, name="azure_queue"
    ) -> (
        AzureQueueAdapter
        | SQSAdapter
        | GCPPubSubAdapter
        | VercelQueueAdapter
        | BlockchainQueueAdapter
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            if cfg.provider == CloudProvider.AZURE:

                connection_string = None
                if isinstance(cfg, AzureQueueConfig):
                    connection_string = cfg.connection_string
                instance = AzureQueueAdapter(connection_string or "")

            elif cfg.provider == CloudProvider.AWS:
                from .adapters.SQSAdapter import SQSAdapter

                queue_name = None
                region = None
                endpoint_url = None
                if isinstance(cfg, SQSAdapterConfig):
                    queue_name = cfg.queue_name
                    region = cfg.region
                    endpoint_url = cfg.endpoint_url
                instance = SQSAdapter(
                    queue_name=queue_name or "",
                    region=region or "",
                    endpoint_url=endpoint_url or "",
                )

            elif cfg.provider == CloudProvider.GCP:

                topic = None
                project_id = ""
                subscription = None
                if isinstance(cfg, GCPPubSubConfig):
                    topic = cfg.topic
                    project_id = cfg.project_id
                    subscription = cfg.subscription
                instance = GCPPubSubAdapter(
                    project_id=project_id, topic=topic, subscription=subscription
                )

            elif cfg.provider == CloudProvider.VERCEL:

                url = ""
                token = ""
                if isinstance(cfg, VercelQueueConfig):
                    url = cfg.url
                    token = cfg.token
                instance = VercelQueueAdapter(url or "", token or "")

            elif cfg.provider == CloudProvider.BLOCKCHAIN:

                rpc_url = ""
                private_key = ""
                contract_address = ""
                contract_abi = []
                if isinstance(cfg, BlockchainQueueConfig):
                    rpc_url = cfg.rpc_url
                    private_key = cfg.private_key
                    private_key = cfg.private_key
                    contract_abi = cfg.contract_abi
                instance = BlockchainQueueAdapter(
                    rpc_url=rpc_url,
                    private_key=private_key,
                    contract_address=contract_address,
                    contract_abi=contract_abi,
                )

            else:
                raise NotImplementedError(
                    f"Queue adapter is not supported for {self.provider.value}"
                )

            self.instances["queue"] = instance
            return instance

    def get_files(self, name: str = "files"):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            # ---------------- AZURE FILE STORAGE ----------------
            if cfg.provider == CloudProvider.AZURE:
                from .adapters.AzureFileStorageAdapter import AzureFileStorageAdapter

                connection_string = ""
                share_name = ""

                if isinstance(cfg, AzureFileConfig):
                    connection_string = cfg.connection_string or ""
                    share_name = cfg.share_name or ""

                instance = AzureFileStorageAdapter(
                    connection_string=connection_string,
                    share_name=share_name,
                )

            # ---------------- AWS EFS ----------------
            elif cfg.provider == CloudProvider.AWS:
                from .adapters.EFSAdapter import EFSAdapter

                mount_point = None
                if isinstance(cfg, EFSFileConfig):
                    mount_point = cfg.mount_path

                instance = EFSAdapter(
                    mount_point=mount_point or os.getenv("EFS_MOUNT_POINT", "/mnt/efs")
                )

            # ---------------- GCP STORAGE (FILES VIA BUCKET) ----------------
            elif cfg.provider == CloudProvider.GCP:
                from .adapters.GCPStorageAdapter import GCPStorageAdapter

                project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
                endpoint = os.getenv("GCP_STORAGE_ENDPOINT")  # optional (for emulator)
                bucket = None

                if isinstance(cfg, GCPFileConfig):
                    bucket = cfg.bucket

                instance = GCPStorageAdapter(
                    project_id=project_id,
                    endpoint=endpoint,
                    bucket_name=bucket,
                )

            # ---------------- VERCEL (fallback to blob) ----------------
            elif cfg.provider == CloudProvider.VERCEL:
                # Vercel has no file system → reuse blob adapter
                instance = self.get_object_storage(name)

            # ---------------- BLOCKCHAIN ----------------
            elif cfg.provider == CloudProvider.BLOCKCHAIN:
                raise NotImplementedError("File storage not supported for blockchain")

            # ---------------- DEFAULT ----------------
            else:
                raise NotImplementedError(f"File storage not supported for {self.provider.value}")

            self.instances[name] = instance
            return instance
