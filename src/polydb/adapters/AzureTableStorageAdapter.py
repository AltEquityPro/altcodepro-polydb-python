# src/polydb/adapters/AzureTableStorageAdapter.py
import os
import threading
from typing import Any, Dict, List, Optional
from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter
from src.polydb.json_safe import json_safe
from ..errors import NoSQLError, ConnectionError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig


class AzureTableStorageAdapter(NoSQLKVAdapter):
    """Azure Table Storage with Azure Blob overflow (limit: 1MB per entity)"""
    
    AZURE_TABLE_MAX_SIZE = 1024 * 1024  # 1MB
    
    def __init__(self, partition_config: Optional[PartitionConfig] = None):
        super().__init__(partition_config)
        self.max_size = self.AZURE_TABLE_MAX_SIZE
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or ""
        self.table_name = os.getenv("AZURE_TABLE_NAME", "defaulttable") or ""
        self.container_name = os.getenv("AZURE_CONTAINER_NAME", "overflow") or ""
        self._client = None
        self._table_client = None
        self._blob_service = None
        self._client_lock = threading.Lock()
        self._initialize_client()
    
    def _initialize_client(self):
        try:
            from azure.data.tables import TableServiceClient
            from azure.storage.blob import BlobServiceClient
            
            with self._client_lock:
                if not self._client:
                    self._client = TableServiceClient.from_connection_string(self.connection_string)
                    self._table_client = self._client.get_table_client(self.table_name)
                    self._blob_service = BlobServiceClient.from_connection_string(self.connection_string)
                    try:
                        self._blob_service.create_container(self.container_name)
                    except:
                        pass  # Already exists
                    
                    self.logger.info("Azure Table Storage initialized with Blob overflow")
        except Exception as e:
            raise ConnectionError(f"Azure Table init failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            import json
            import hashlib
            
            data_copy = dict(data)
            data_copy['PartitionKey'] = pk
            data_copy['RowKey'] = rk
            
            # Check size
            data_bytes = json.dumps(data_copy,default=json_safe).encode()
            data_size = len(data_bytes)
            
            if data_size > self.AZURE_TABLE_MAX_SIZE:
                # Store in Blob
                blob_id = hashlib.md5(data_bytes).hexdigest()
                blob_key = f"overflow/{pk}/{rk}/{blob_id}.json"
                
                if self._blob_service:
                    blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
                    blob_client.upload_blob(data_bytes, overwrite=True)
                    self.logger.info(f"Stored overflow to Blob: {blob_key} ({data_size} bytes)")
                
                # Store reference in table
                reference_data = {
                    'PartitionKey': pk,
                    'RowKey': rk,
                    '_overflow': True,
                    '_blob_key': blob_key,
                    '_size': data_size,
                    '_checksum': blob_id,
                }
                
                if self._table_client:
                    self._table_client.upsert_entity(reference_data)
            else:
                # Store directly in table
                if self._table_client:
                    self._table_client.upsert_entity(data_copy)
            
            return {'PartitionKey': pk, 'RowKey': rk}
        except Exception as e:
            raise NoSQLError(f"Azure Table put failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            import json
            import hashlib
            
            if not self._table_client:
                return None
            
            entity = self._table_client.get_entity(pk, rk)
            entity_dict = dict(entity)
            
            # Check if overflow
            if entity_dict.get('_overflow'):
                blob_key = entity_dict.get('_blob_key')
                checksum = entity_dict.get('_checksum')
                
                if blob_key and self._blob_service:
                    blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
                    blob_data = blob_client.download_blob().readall()
                    
                    # Verify checksum
                    actual_checksum = hashlib.md5(blob_data).hexdigest()
                    if actual_checksum != checksum:
                        raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual_checksum}")
                    
                    retrieved = json.loads(blob_data.decode())
                    self.logger.debug(f"Retrieved overflow from Blob: {blob_key}")
                    return retrieved
            
            return entity_dict
        except Exception as e:
            if "ResourceNotFound" in str(e):
                return None
            raise NoSQLError(f"Azure Table get failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(self, model: type, filters: Dict[str, Any], limit: Optional[int]) -> List[JsonDict]:
        try:
            if not self._table_client:
                return []
            
            query_filter = None
            if filters:
                filter_parts = []
                for k, v in filters.items():
                    if isinstance(v, str):
                        filter_parts.append(f"{k} eq '{v}'")
                    elif isinstance(v, bool):
                        filter_parts.append(f"{k} eq {str(v).lower()}")
                    else:
                        filter_parts.append(f"{k} eq {v}")
                
                query_filter = " and ".join(filter_parts)
            
            entities = self._table_client.query_entities(
                query_filter=query_filter, # type: ignore
                results_per_page=limit
            )
            
            return [dict(entity) for entity in entities]
        except Exception as e:
            raise NoSQLError(f"Azure Table query failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            if not self._table_client:
                return {'deleted': False}
            
            # Check if overflow before deleting
            try:
                entity = self._table_client.get_entity(pk, rk)
                entity_dict = dict(entity)
                
                if entity_dict.get('_overflow'):
                    blob_key = entity_dict.get('_blob_key')
                    if blob_key and self._blob_service:
                        blob_client = self._blob_service.get_blob_client(self.container_name, blob_key)
                        blob_client.delete_blob()
                        self.logger.debug(f"Deleted overflow blob: {blob_key}")
            except:
                pass  # Entity might not exist or no overflow
            
            # Delete table entity
            self._table_client.delete_entity(pk, rk, etag=etag)
            return {'deleted': True, 'PartitionKey': pk, 'RowKey': rk}
        except Exception as e:
            raise NoSQLError(f"Azure Table delete failed: {str(e)}")

