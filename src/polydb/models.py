# src/polydb/models.py
"""
Data models and configurations
"""

from dataclasses import dataclass, field
from typing import Optional, List, Callable, Any, Dict
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
    VAULT = "vault"
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"


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


@dataclass
class BackendCapabilities:
    """Describes what a storage backend can push down natively."""

    server_order: bool = False
    server_filter: bool = False
    native_cursor: bool = False
    supports_count: bool = True


@dataclass
class PageRequest:
    """Unified pagination request passed to query_paged()."""

    filters: Optional[Dict[str, Any]] = None
    order_by: Optional[str] = None
    order_desc: bool = False
    limit: int = 50
    cursor: Optional[str] = None
    fields: Optional[List[str]] = None


@dataclass
class PageResult:
    """Unified pagination result returned by query_paged()."""

    items: List[Any] = field(default_factory=list)
    next_cursor: Optional[str] = None
    has_more: bool = False
    total: Optional[int] = None


# ============================================================
# CONFIG CLASSES (TYPED)
# ============================================================


class StorageConfig:
    def __init__(self, provider: CloudProvider, name: str = "default"):
        self.provider = provider
        self.name = name


class AzureKeyVaultConfig(StorageConfig):
    def __init__(self, name: str = "secrets", vault_url: Optional[str] = None):
        super().__init__(CloudProvider.AZURE, name)
        self.vault_url = vault_url


class AWSSecretsManagerConfig(StorageConfig):
    def __init__(self, name: str = "secrets", region: Optional[str] = None):
        super().__init__(CloudProvider.AWS, name)
        self.region = region


class GCPSecretManagerConfig(StorageConfig):
    def __init__(self, name: str = "secrets", project_id: Optional[str] = None):
        super().__init__(CloudProvider.GCP, name)
        self.project_id = project_id


class VaultConfig(StorageConfig):
    def __init__(
        self,
        name: str = "secrets",
        url: Optional[str] = None,
        token: Optional[str] = None,
        mount_point: str = "secret",
    ):
        super().__init__(CloudProvider.VAULT, name)
        self.url = url
        self.token = token
        self.mount_point = mount_point


class AzureStorageConfig(StorageConfig):
    def __init__(
        self,
        name: str = "azure",
        connection_string: Optional[str] = None,
        container: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.container = container


class AzureQueueConfig(StorageConfig):
    def __init__(
        self,
        name: str = "azure_queue",
        connection_string: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string


class S3StorageConfig(StorageConfig):
    def __init__(
        self,
        name: str = "s3",
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


class SQSAdapterConfig(StorageConfig):
    def __init__(
        self,
        name: str = "sqs_adapter",
        queue_name: str = "",
        region: str = "",
        endpoint_url: str = "",
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.queue_name = queue_name
        self.region = region
        self.endpoint_url = endpoint_url


class GCPStorageConfig(StorageConfig):
    def __init__(self, name: str = "gcp", project_id="", endpoint="", bucket: Optional[str] = None):
        super().__init__(CloudProvider.GCP, name)
        self.bucket = bucket
        self.project_id = project_id
        self.endpoint = endpoint


class GCPPubSubConfig(StorageConfig):
    def __init__(
        self, name: str = "gcp_queue", project_id="", subscription="", topic: Optional[str] = None
    ):
        super().__init__(CloudProvider.GCP, name)
        self.topic = topic
        self.project_id = project_id
        self.subscription = subscription


class VercelStorageConfig(StorageConfig):
    def __init__(self, name: str = "vercel", token: Optional[str] = None, timeout: int = 10):
        super().__init__(CloudProvider.VERCEL, name)
        self.token = token
        self.timeout = timeout


class VercelQueueConfig(StorageConfig):
    def __init__(self, name: str = "vercel_queue", token: Optional[str] = None, url: str = ""):
        super().__init__(CloudProvider.VERCEL, name)
        self.token = token
        self.url = url


class KafkaQueueConfig(StorageConfig):
    def __init__(
        self,
        name: str = "kafka_queue",
        bootstrap_servers: str = "",
        group_id: str = "",
        security_protocol: str = "",
        sasl_mechanism: Optional[str] = None,
        sasl_plain_username: Optional[str] = None,
        sasl_plain_password: Optional[str] = None,
        ssl_cafile: Optional[str] = None,
    ):
        super().__init__(CloudProvider.KAFKA, name)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.ssl_cafile = ssl_cafile


class RabbitMQConfig(StorageConfig):
    def __init__(
        self,
        name: str = "rabbitmq",
        url: str = "",
        host: str = "",
        port: int = 0,
        username: str = "",
        password: str = "",
        virtual_host: str = "",
    ):
        super().__init__(CloudProvider.RABBITMQ, name)
        self.url = url
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host


class BlockchainStorageConfig(StorageConfig):
    def __init__(self, name: str = "blockchain", ipfs_url: Optional[str] = None):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.ipfs_url = ipfs_url


class BlockchainQueueConfig(StorageConfig):
    def __init__(
        self,
        name: str = "blockchain_queue",
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.rpc_url = rpc_url
        self.private_key = private_key
        self.contract_address = contract_address
        self.contract_abi = contract_abi


class FileStorageConfig(StorageConfig):
    def __init__(self, provider: CloudProvider, name: str = "files"):
        super().__init__(provider, name)


class AzureFileConfig(FileStorageConfig):
    def __init__(
        self,
        name: str = "azure_files",
        connection_string: Optional[str] = None,
        share_name: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.share_name = share_name


class GCPFileConfig(FileStorageConfig):
    def __init__(
        self,
        name: str = "gcp_files",
        bucket: Optional[str] = None,
    ):
        super().__init__(CloudProvider.GCP, name)
        self.bucket = bucket


class EFSFileConfig(FileStorageConfig):
    def __init__(
        self,
        name: str = "efs",
        mount_path: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AWS, name)
        self.mount_path = mount_path


class PostgreSQLConfig(StorageConfig):
    def __init__(
        self,
        name: str = "sql",
        connection_string: Optional[str] = None,
    ):
        super().__init__(CloudProvider.POSTGRESQL, name)
        self.connection_string = connection_string


class AzureTableConfig(StorageConfig):
    def __init__(
        self,
        name: str = "azure_kv",
        connection_string: Optional[str] = None,
        table_name: str = "",
        container_name: str = "",
    ):
        super().__init__(CloudProvider.AZURE, name)
        self.connection_string = connection_string
        self.table_name = table_name
        self.container_name = container_name


class DynamoDBConfig(StorageConfig):
    def __init__(
        self,
        name: str = "dynamodb",
        table_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        super().__init__(CloudProvider.AWS, name)
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.region = region
        self.endpoint_url = endpoint_url


class FirestoreConfig(StorageConfig):
    def __init__(
        self,
        name: str = "firestore",
        project: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        super().__init__(CloudProvider.GCP, name)
        self.project = project
        self.bucket_name = bucket_name


class VercelKVConfig(StorageConfig):
    def __init__(
        self,
        name: str = "vercel_kv",
        kv_url: str = "",
        kv_token: str = "",
        timeout: int = 10,
    ):
        super().__init__(CloudProvider.VERCEL, name)
        self.kv_url = kv_url
        self.kv_token = kv_token
        self.timeout = timeout


class BlockchainKVConfig(StorageConfig):
    def __init__(
        self,
        name: str = "blockchain_kv",
        chain: Optional[str] = None,
        rpc_url: Optional[str] = None,
        private_key: Optional[str] = None,
        contract_address: Optional[str] = None,
        contract_abi: Optional[list] = None,
    ):
        super().__init__(CloudProvider.BLOCKCHAIN, name)
        self.chain = chain
        self.rpc_url = rpc_url
        self.private_key = private_key
        self.contract_address = contract_address
        self.contract_abi = contract_abi


class MongoConfig(StorageConfig):
    def __init__(
        self,
        name: str = "mongo",
        mongo_uri: str = "",
        db_name: str = "",
    ):
        super().__init__(CloudProvider.MONGODB, name)
        self.mongo_uri = mongo_uri
        self.db_name = db_name


class CosmosMongoConfig(MongoConfig):
    """Azure Cosmos DB via the **MongoDB API**.

    Cosmos-for-Mongo speaks the MongoDB wire protocol, so it needs no adapter of
    its own — it rides the existing ``MongoDBAdapter`` (and thus the whole polydb
    stack) unchanged. This gives the best of both: Azure-native, serverless /
    autoscale, managed billing + monitoring **and** portability — the same app
    runs on MongoDB Atlas by swapping the connection string.

    Pass a full ``connection_string`` (recommended — works for both RU and vCore),
    or ``account`` + ``key`` to build the standard Cosmos-for-Mongo (RU) URI.
    """

    def __init__(
        self,
        name: str = "cosmos",
        *,
        connection_string: str = "",
        account: str = "",
        key: str = "",
        db_name: str = "",
        port: int = 10255,
    ):
        uri = connection_string
        if not uri and account and key:
            uri = (
                f"mongodb://{account}:{key}@{account}.mongo.cosmos.azure.com:{port}/"
                f"?ssl=true&replicaSet=globaldb&retrywrites=false"
                f"&maxIdleTimeMS=120000&appName=@{account}@"
            )
        super().__init__(name=name, mongo_uri=uri, db_name=db_name)
        self.is_cosmos = True
        self.account = account
