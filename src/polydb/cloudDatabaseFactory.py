from __future__ import annotations

import os
import threading
from typing import Dict, List, Optional

from .base import SharedFilesAdapter

# Deliberately NOT importing adapter classes at module level (they used to
# be, redundantly -- every branch below already does its own lazy, local
# `from .adapters.XAdapter import XAdapter` at the point of use, and
# `from __future__ import annotations` above means the return-type hints
# don't need a real import either). The top-level imports served no
# purpose except forcing every cloud SDK to be installed just to
# `import polydb` at all -- found by actually trying a minimal install.
from .models import (
    AWSSecretsManagerConfig,
    AzureFileConfig,
    AzureKeyVaultConfig,
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
    GCPSecretManagerConfig,
    GCPStorageConfig,
    KafkaQueueConfig,
    MongoConfig,
    PartitionConfig,
    PostgreSQLConfig,
    RabbitMQConfig,
    SQSAdapterConfig,
    StorageConfig,
    VaultConfig,
    VercelKVConfig,
    VercelQueueConfig,
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
        # README documents MONGODB_URI as a detection signal; the check was
        # missing here entirely, so it silently fell through to Postgres
        # instead (found by running the package's own test suite).
        if os.getenv("MONGODB_URI"):
            return CloudProvider.MONGODB

        return CloudProvider.POSTGRESQL

    # --------------------------------------------------------
    # OBJECT STORAGE
    # --------------------------------------------------------
    def get_object_storage(
        self,
        name: str = "azure",
        container_name: Optional[str] = None,
    ) -> (
        AzureBlobStorageAdapter
        | S3CompatibleAdapter
        | GCPStorageAdapter
        | VercelBlobAdapter
        | BlockchainBlobAdapter
    ):
        with self._lock:
            # cache per (name, container) so different containers don't collide
            cache_key = f"object::{name}::{container_name or ''}"
            if cache_key in self.instances:
                return self.instances[cache_key]

            cfg = self.configs.get(name) or StorageConfig(provider=self.provider, name=name)
            provider = cfg.provider

            # ---------------- AZURE ----------------
            if provider == CloudProvider.AZURE:
                from .adapters.AzureBlobStorageAdapter import AzureBlobStorageAdapter

                connection_string = None
                container = None
                if isinstance(cfg, AzureStorageConfig):
                    connection_string = cfg.connection_string
                    container = cfg.container

                container = container_name or container  # per-call overrides config/env

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
                bucket = container_name or bucket  # container_name doubles as bucket override
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

                ipfs_url = cfg.ipfs_url if isinstance(cfg, BlockchainStorageConfig) else None
                instance = BlockchainBlobAdapter(ipfs_url=ipfs_url)

            # ---------------- DEFAULT ----------------
            else:
                from .adapters.S3CompatibleAdapter import S3CompatibleAdapter

                self.logger.warning(f"Fallback to S3-compatible for provider={provider}")
                instance = S3CompatibleAdapter()

            self.instances[cache_key] = instance
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
                container_name = ""

                if isinstance(cfg, AzureTableConfig):
                    connection_string = cfg.connection_string or ""
                    table_name = cfg.table_name
                    container_name = cfg.container_name

                instance = AzureTableStorageAdapter(
                    partition_config=partition_config,
                    connection_string=connection_string,
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
        | KafkaQueueAdapter
        | RabbitMQAdapter
    ):
        with self._lock:
            if name in self.instances:
                return self.instances[name]

            cfg = self.configs.get(name)
            if not cfg:
                cfg = StorageConfig(provider=self.provider, name=name)

            if cfg.provider == CloudProvider.AZURE:
                from .adapters.AzureQueueAdapter import AzureQueueAdapter

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
                from .adapters.GCPPubSubAdapter import GCPPubSubAdapter

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
                from .adapters.VercelQueueAdapter import VercelQueueAdapter

                url = ""
                token = ""
                if isinstance(cfg, VercelQueueConfig):
                    url = cfg.url
                    token = cfg.token
                instance = VercelQueueAdapter(url or "", token or "")

            elif cfg.provider == CloudProvider.BLOCKCHAIN:
                from .adapters.BlockchainQueueAdapter import BlockchainQueueAdapter

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

            elif cfg.provider == CloudProvider.KAFKA:
                from .adapters.KafkaQueueAdapter import KafkaQueueAdapter

                bootstrap_servers = ""
                group_id = ""
                security_protocol = ""
                sasl_mechanism = None
                sasl_plain_username = None
                sasl_plain_password = None
                ssl_cafile = None
                if isinstance(cfg, KafkaQueueConfig):
                    bootstrap_servers = cfg.bootstrap_servers
                    group_id = cfg.group_id
                    security_protocol = cfg.security_protocol
                    sasl_mechanism = cfg.sasl_mechanism
                    sasl_plain_username = cfg.sasl_plain_username
                    sasl_plain_password = cfg.sasl_plain_password
                    ssl_cafile = cfg.ssl_cafile
                instance = KafkaQueueAdapter(
                    bootstrap_servers=bootstrap_servers or "",
                    group_id=group_id or "",
                    security_protocol=security_protocol or "",
                    sasl_mechanism=sasl_mechanism or "",
                    sasl_plain_username=sasl_plain_username or "",
                    sasl_plain_password=sasl_plain_password or "",
                    ssl_cafile=ssl_cafile or "",
                )

            elif cfg.provider == CloudProvider.RABBITMQ:
                from .adapters.RabbitMQAdapter import RabbitMQAdapter

                url = ""
                host = ""
                port = 0
                username = ""
                password = ""
                virtual_host = ""
                if isinstance(cfg, RabbitMQConfig):
                    url = cfg.url
                    host = cfg.host
                    port = cfg.port
                    username = cfg.username
                    password = cfg.password
                    virtual_host = cfg.virtual_host
                instance = RabbitMQAdapter(
                    url=url or "",
                    host=host or "",
                    port=port or 0,
                    username=username or "",
                    password=password or "",
                    virtual_host=virtual_host or "",
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

            cfg = self.configs.get(name) or StorageConfig(provider=self.provider, name=name)

            if cfg.provider == CloudProvider.AZURE:
                from .adapters.AzureFileStorageAdapter import AzureFileStorageAdapter

                connection_string = ""
                share_name = ""
                if isinstance(cfg, AzureFileConfig):
                    connection_string = cfg.connection_string or ""
                    share_name = cfg.share_name or ""
                instance = AzureFileStorageAdapter(
                    connection_string=connection_string, share_name=share_name
                )

            elif cfg.provider == CloudProvider.AWS:
                from .adapters.EFSAdapter import EFSAdapter

                mount_point = cfg.mount_path if isinstance(cfg, EFSFileConfig) else None
                instance = EFSAdapter(mount_point=mount_point or "")

            elif cfg.provider == CloudProvider.GCP:
                from .adapters.GCPFilestoreAdapter import FilestoreAdapter

                mount_point = getattr(cfg, "mount_path", None)
                instance = FilestoreAdapter(mount_point=mount_point or "")

            elif cfg.provider == CloudProvider.VERCEL:
                from .adapters.VercelFileAdapter import VercelFileAdapter

                instance = VercelFileAdapter()

            elif cfg.provider == CloudProvider.BLOCKCHAIN:
                from .adapters.BlockchainFileAdapter import BlockchainFileAdapter

                instance = BlockchainFileAdapter(
                    chain=getattr(cfg, "chain", None),
                    rpc_url=getattr(cfg, "rpc_url", None),
                    private_key=getattr(cfg, "private_key", None),
                    contract_address=getattr(cfg, "contract_address", None),
                    contract_abi=getattr(cfg, "contract_abi", None),
                    ipfs_url=getattr(cfg, "ipfs_url", None),
                )
            else:
                raise NotImplementedError(f"File storage not supported for {cfg.provider.value}")

            self.instances[name] = instance
            return instance

    # --------------------------------------------------------
    # SECRETS
    # --------------------------------------------------------
    def get_secrets(self, name: str = "secrets"):
        """Cloud-agnostic secrets: Azure Key Vault / AWS Secrets Manager /
        GCP Secret Manager / HashiCorp Vault (self-hosted), dispatched the
        same way as every other adapter type. Unlike the data adapters,
        this deliberately does NOT fall back to self.provider by default --
        a factory configured for provider=postgresql or provider=mongodb
        has no secrets manager of its own, so an unconfigured/unrecognized
        provider here falls back to Vault (self-hosted, cloud-agnostic)
        rather than raising."""
        with self._lock:
            cache_key = f"secrets::{name}"
            if cache_key in self.instances:
                return self.instances[cache_key]

            cfg = self.configs.get(name)
            provider = cfg.provider if cfg else self.provider

            if provider == CloudProvider.AZURE:
                from .adapters.AzureKeyVaultAdapter import AzureKeyVaultAdapter

                vault_url = cfg.vault_url if isinstance(cfg, AzureKeyVaultConfig) else None
                instance = AzureKeyVaultAdapter(vault_url=vault_url or "")

            elif provider == CloudProvider.AWS:
                from .adapters.AWSSecretsManagerAdapter import AWSSecretsManagerAdapter

                region = cfg.region if isinstance(cfg, AWSSecretsManagerConfig) else None
                instance = AWSSecretsManagerAdapter(region=region or "")

            elif provider == CloudProvider.GCP:
                from .adapters.GCPSecretManagerAdapter import GCPSecretManagerAdapter

                project_id = cfg.project_id if isinstance(cfg, GCPSecretManagerConfig) else None
                instance = GCPSecretManagerAdapter(project_id=project_id or "")

            else:
                from .adapters.VaultAdapter import VaultAdapter

                url, token, mount_point = None, None, "secret"
                if isinstance(cfg, VaultConfig):
                    url, token, mount_point = cfg.url, cfg.token, cfg.mount_point
                instance = VaultAdapter(
                    url=url or "", token=token or "", mount_point=mount_point
                )

            self.instances[cache_key] = instance
            return instance
