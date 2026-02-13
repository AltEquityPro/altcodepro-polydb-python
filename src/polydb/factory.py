# src/polydb/factory.py
import os
import threading
from typing import Optional

from .models import CloudProvider, PartitionConfig
from .utils import setup_logger


class CloudDatabaseFactory:
    """Cloud-independent factory with auto-detection"""
    
    def __init__(self, provider: Optional[CloudProvider] = None):
        self.logger = setup_logger(__name__)
        self.provider = provider or self._detect_provider()
        self.connections = {}
        self._lock = threading.Lock()
        
        self.logger.info(f"CloudDatabaseFactory provider: {self.provider.value}")
    
    def _detect_provider(self) -> CloudProvider:
        explicit = os.getenv('CLOUD_PROVIDER')
        if explicit:
            try:
                return CloudProvider(explicit.lower())
            except ValueError:
                self.logger.warning(f"Invalid CLOUD_PROVIDER: {explicit}")
        
        rules = [
            ('AZURE_STORAGE_CONNECTION_STRING', CloudProvider.AZURE),
            ('AWS_ACCESS_KEY_ID', CloudProvider.AWS),
            ('GOOGLE_CLOUD_PROJECT', CloudProvider.GCP),
            ('VERCEL_ENV', CloudProvider.VERCEL),
            ('MONGODB_URI', CloudProvider.MONGODB),
            ('POSTGRES_URL', CloudProvider.POSTGRESQL),
            ('POSTGRES_CONNECTION_STRING', CloudProvider.POSTGRESQL),
        ]
        
        for env_var, provider in rules:
            if os.getenv(env_var):
                return provider
        
        self.logger.warning("No provider detected, defaulting to PostgreSQL")
        return CloudProvider.POSTGRESQL
    
    def get_nosql_kv(self, partition_config: Optional[PartitionConfig] = None):
        if self.provider == CloudProvider.AZURE:
            from .adapters.AzureTableStorageAdapter import AzureTableStorageAdapter
            return AzureTableStorageAdapter(partition_config)
        elif self.provider == CloudProvider.AWS:
            from .adapters.DynamoDBAdapter import DynamoDBAdapter
            return DynamoDBAdapter(partition_config)
        elif self.provider == CloudProvider.GCP:
            from .adapters.FirestoreAdapter import FirestoreAdapter
            return FirestoreAdapter(partition_config)
        elif self.provider == CloudProvider.VERCEL:
            from .adapters.VercelKVAdapter import VercelKVAdapter
            return VercelKVAdapter(partition_config)
        else:
            from .adapters.MongoDBAdapter import MongoDBAdapter
            return MongoDBAdapter(partition_config)
    
    def get_object_storage(self):
        if self.provider == CloudProvider.AZURE:
            from .adapters.AzureBlobStorageAdapter import AzureBlobStorageAdapter
            return AzureBlobStorageAdapter()
        elif self.provider == CloudProvider.AWS:
            from .adapters.S3Adapter import S3Adapter
            return S3Adapter()
        elif self.provider == CloudProvider.GCP:
            from .adapters.GCPStorageAdapter import GCPStorageAdapter
            return GCPStorageAdapter()
        elif self.provider == CloudProvider.VERCEL:
            from .adapters.VercelKVAdapter import VercelBlobAdapter
            return VercelBlobAdapter()
        else:
            from .adapters.S3CompatibleAdapter import S3CompatibleAdapter
            return S3CompatibleAdapter()
    
    def get_queue(self):
        if self.provider == CloudProvider.AZURE:
            from .adapters.AzureQueueAdapter import AzureQueueAdapter
            return AzureQueueAdapter()
        elif self.provider == CloudProvider.AWS:
            from .adapters.SQSAdapter import SQSAdapter
            return SQSAdapter()
        elif self.provider == CloudProvider.GCP:
            from .adapters.PubSubAdapter import PubSubAdapter
            return PubSubAdapter()
        elif self.provider == CloudProvider.VERCEL:
            from .adapters.VercelKVAdapter import VercelQueueAdapter
            return VercelQueueAdapter()
    
    def get_shared_files(self):
        if self.provider == CloudProvider.AZURE:
            from .adapters.AzureFileStorageAdapter import AzureFileStorageAdapter
            return AzureFileStorageAdapter()
        elif self.provider == CloudProvider.AWS:
            from .adapters.EFSAdapter import EFSAdapter
            return EFSAdapter()
        elif self.provider == CloudProvider.GCP:
            from .adapters.GCPStorageAdapter import GCPStorageAdapter
            return GCPStorageAdapter()
    
    def get_sql(self):
        from .adapters.PostgreSQLAdapter import PostgreSQLAdapter
        return PostgreSQLAdapter()