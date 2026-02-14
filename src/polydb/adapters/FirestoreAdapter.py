# src/polydb/adapters/FirestoreAdapter.py
import os
import threading
from typing import Any, Dict, List, Optional
from google.cloud import firestore
from google.cloud import storage
from google.cloud.firestore import Client
from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter
from ..json_safe import json_safe

from ..errors import NoSQLError, ConnectionError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig


class FirestoreAdapter(NoSQLKVAdapter):
    """Firestore with GCS overflow (limit: 1MB per document)"""
    
    FIRESTORE_MAX_SIZE = 1024 * 1024  # 1MB
    
    def __init__(self, partition_config: Optional[PartitionConfig] = None):
        super().__init__(partition_config)
        self.max_size = self.FIRESTORE_MAX_SIZE
        self.bucket_name = os.getenv("GCS_OVERFLOW_BUCKET", "firestore-overflow")
        self._client: Optional[Client] = None
        self._storage_client = None
        self._bucket = None
        self._client_lock = threading.Lock()
        self._initialize_client()
    
    def _initialize_client(self):
        try:
            with self._client_lock:
                if not self._client:
                    self._client = firestore.Client()
                    self._storage_client = storage.Client()
                    self._bucket = self._storage_client.bucket(self.bucket_name)
                    
                    # Ensure bucket exists
                    try:
                        self._bucket.create()
                    except:
                        pass  # Already exists
                    
                    self.logger.info("Firestore initialized with GCS overflow")
        except Exception as e:
            raise ConnectionError(f"Firestore init failed: {str(e)}")
    
    def _get_collection(self, model: type) -> Any:
        if not self._client:
            self._initialize_client()
        
        meta = getattr(model, '__polydb__', {})
        collection_name = meta.get('collection') or meta.get('table') or model.__name__.lower()
        return self._client.collection(collection_name) # type: ignore
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            import json
            import hashlib
            
            doc_id = f"{pk}_{rk}"
            data_copy = dict(data)
            data_copy['_pk'] = pk
            data_copy['_rk'] = rk
            
            # Check size
            data_bytes = json.dumps(data_copy,default=json_safe).encode()
            data_size = len(data_bytes)
            
            if data_size > self.FIRESTORE_MAX_SIZE:
                # Store in GCS
                blob_id = hashlib.md5(data_bytes).hexdigest()
                blob_key = f"overflow/{pk}/{rk}/{blob_id}.json"
                
                if self._bucket:
                    blob = self._bucket.blob(blob_key)
                    blob.upload_from_string(data_bytes)
                    self.logger.info(f"Stored overflow to GCS: {blob_key} ({data_size} bytes)")
                
                # Store reference in Firestore
                reference_data = {
                    '_pk': pk,
                    '_rk': rk,
                    '_overflow': True,
                    '_blob_key': blob_key,
                    '_size': data_size,
                    '_checksum': blob_id,
                }
                
                collection = self._get_collection(model)
                collection.document(doc_id).set(reference_data)
            else:
                # Store directly in Firestore
                collection = self._get_collection(model)
                collection.document(doc_id).set(data_copy)
            
            return {'_pk': pk, '_rk': rk, 'id': doc_id}
        except Exception as e:
            raise NoSQLError(f"Firestore put failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            import json
            import hashlib
            
            doc_id = f"{pk}_{rk}"
            collection = self._get_collection(model)
            doc = collection.document(doc_id).get()
            
            if not doc.exists:
                return None
            
            doc_data = doc.to_dict()
            
            # Check if overflow
            if doc_data.get('_overflow'):
                blob_key = doc_data.get('_blob_key')
                checksum = doc_data.get('_checksum')
                
                if blob_key and self._bucket:
                    blob = self._bucket.blob(blob_key)
                    blob_data = blob.download_as_bytes()
                    
                    # Verify checksum
                    actual_checksum = hashlib.md5(blob_data).hexdigest()
                    if actual_checksum != checksum:
                        raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual_checksum}")
                    
                    retrieved = json.loads(blob_data.decode())
                    self.logger.debug(f"Retrieved overflow from GCS: {blob_key}")
                    return retrieved
            
            return doc_data
        except Exception as e:
            raise NoSQLError(f"Firestore get failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(self, model: type, filters: Dict[str, Any], limit: Optional[int]) -> List[JsonDict]:
        try:
            collection = self._get_collection(model)
            query = collection
            
            for field, value in filters.items():
                if field.endswith('__gt'):
                    query = query.where(field[:-4], '>', value)
                elif field.endswith('__gte'):
                    query = query.where(field[:-5], '>=', value)
                elif field.endswith('__lt'):
                    query = query.where(field[:-4], '<', value)
                elif field.endswith('__lte'):
                    query = query.where(field[:-5], '<=', value)
                elif field.endswith('__in'):
                    query = query.where(field[:-4], 'in', value)
                else:
                    query = query.where(field, '==', value)
            
            if limit:
                query = query.limit(limit)
            
            docs = query.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            raise NoSQLError(f"Firestore query failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            doc_id = f"{pk}_{rk}"
            collection = self._get_collection(model)
            
            # Check if overflow before deleting
            try:
                doc = collection.document(doc_id).get()
                if doc.exists:
                    doc_data = doc.to_dict()
                    
                    if doc_data.get('_overflow'):
                        blob_key = doc_data.get('_blob_key')
                        if blob_key and self._bucket:
                            blob = self._bucket.blob(blob_key)
                            blob.delete()
                            self.logger.debug(f"Deleted overflow GCS object: {blob_key}")
            except:
                pass  # Doc might not exist or no overflow
            
            # Delete Firestore document
            collection.document(doc_id).delete()
            return {'deleted': True, 'id': doc_id}
        except Exception as e:
            raise NoSQLError(f"Firestore delete failed: {str(e)}")