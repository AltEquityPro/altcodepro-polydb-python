# src/polydb/adapters/VercelKVAdapter.py
import os
import threading
from typing import Any, Dict, List, Optional
from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter
from ..errors import NoSQLError, StorageError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig


class VercelKVAdapter(NoSQLKVAdapter):
    """Vercel KV (Redis) with Vercel Blob overflow (limit: 100KB per key)"""
    
    VERCEL_KV_MAX_SIZE = 100 * 1024  # 100KB
    
    def __init__(self, partition_config: Optional[PartitionConfig] = None):
        super().__init__(partition_config)
        self.max_size = self.VERCEL_KV_MAX_SIZE
        self.kv_url = os.getenv('KV_URL')
        self.kv_token = os.getenv('KV_REST_API_TOKEN')
        self.blob_token = os.getenv('BLOB_READ_WRITE_TOKEN')
        self.timeout = int(os.getenv('VERCEL_KV_TIMEOUT', '10'))
        self._client_lock = threading.Lock()
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            import requests
            import json
            import hashlib
            
            key = f"{pk}:{rk}"
            data_copy = dict(data)
            data_copy['_pk'] = pk
            data_copy['_rk'] = rk
            
            # Check size
            data_bytes = json.dumps(data_copy).encode()
            data_size = len(data_bytes)
            
            if data_size > self.VERCEL_KV_MAX_SIZE:
                # Store in Vercel Blob
                blob_id = hashlib.md5(data_bytes).hexdigest()
                blob_key = f"overflow/{pk}/{rk}/{blob_id}.json"
                
                blob_response = requests.put(
                    f"https://blob.vercel-storage.com/{blob_key}",
                    headers={
                        "Authorization": f"Bearer {self.blob_token}",
                        "x-content-type": "application/json",
                    },
                    data=data_bytes,
                    timeout=self.timeout,
                )
                blob_response.raise_for_status()
                self.logger.info(f"Stored overflow to Blob: {blob_key} ({data_size} bytes)")
                
                # Store reference in KV
                reference_data = {
                    '_pk': pk,
                    '_rk': rk,
                    '_overflow': True,
                    '_blob_key': blob_key,
                    '_size': data_size,
                    '_checksum': blob_id,
                }
                
                response = requests.post(
                    f"{self.kv_url}/set/{key}",
                    headers={'Authorization': f'Bearer {self.kv_token}'},
                    json={'value': json.dumps(reference_data)},
                    timeout=self.timeout
                )
                response.raise_for_status()
            else:
                # Store directly in KV
                response = requests.post(
                    f"{self.kv_url}/set/{key}",
                    headers={'Authorization': f'Bearer {self.kv_token}'},
                    json={'value': json.dumps(data_copy)},
                    timeout=self.timeout
                )
                response.raise_for_status()
            
            return {'key': key, '_pk': pk, '_rk': rk}
        except Exception as e:
            raise NoSQLError(f"Vercel KV put failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            import requests
            import json
            import hashlib
            
            key = f"{pk}:{rk}"
            
            response = requests.get(
                f"{self.kv_url}/get/{key}",
                headers={'Authorization': f'Bearer {self.kv_token}'},
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                return None
            
            result = response.json().get('result')
            if not result:
                return None
            
            kv_data = json.loads(result)
            
            # Check if overflow
            if kv_data.get('_overflow'):
                blob_key = kv_data.get('_blob_key')
                checksum = kv_data.get('_checksum')
                
                if blob_key:
                    blob_response = requests.get(
                        f"https://blob.vercel-storage.com/{blob_key}",
                        headers={"Authorization": f"Bearer {self.blob_token}"},
                        timeout=self.timeout,
                    )
                    blob_response.raise_for_status()
                    blob_data = blob_response.content
                    
                    # Verify checksum
                    actual_checksum = hashlib.md5(blob_data).hexdigest()
                    if actual_checksum != checksum:
                        raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual_checksum}")
                    
                    retrieved = json.loads(blob_data.decode())
                    self.logger.debug(f"Retrieved overflow from Blob: {blob_key}")
                    return retrieved
            
            return kv_data
        except Exception as e:
            raise NoSQLError(f"Vercel KV get failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(self, model: type, filters: Dict[str, Any], limit: Optional[int]) -> List[JsonDict]:
        try:
            import requests
            import json
            
            # Vercel KV doesn't support native queries, use SCAN pattern
            pattern = "*"
            if filters.get('_pk'):
                pattern = f"{filters['_pk']}:*"
            
            response = requests.get(
                f"{self.kv_url}/keys/{pattern}",
                headers={'Authorization': f'Bearer {self.kv_token}'},
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                return []
            
            keys = response.json().get('result', [])
            
            # Fetch all matching keys
            results = []
            for key in keys:
                if limit and len(results) >= limit:
                    break
                
                get_response = requests.get(
                    f"{self.kv_url}/get/{key}",
                    headers={'Authorization': f'Bearer {self.kv_token}'},
                    timeout=self.timeout
                )
                
                if get_response.status_code == 200:
                    result = get_response.json().get('result')
                    if result:
                        data = json.loads(result)
                        
                        # Apply filters
                        match = True
                        for k, v in filters.items():
                            if k.startswith('_'):
                                continue
                            if data.get(k) != v:
                                match = False
                                break
                        
                        if match:
                            results.append(data)
            
            return results
        except Exception as e:
            raise NoSQLError(f"Vercel KV query failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            import requests
            import json
            
            key = f"{pk}:{rk}"
            
            # Check if overflow before deleting
            try:
                get_response = requests.get(
                    f"{self.kv_url}/get/{key}",
                    headers={'Authorization': f'Bearer {self.kv_token}'},
                    timeout=self.timeout
                )
                
                if get_response.status_code == 200:
                    result = get_response.json().get('result')
                    if result:
                        kv_data = json.loads(result)
                        
                        if kv_data.get('_overflow'):
                            blob_key = kv_data.get('_blob_key')
                            if blob_key:
                                blob_response = requests.delete(
                                    f"https://blob.vercel-storage.com/{blob_key}",
                                    headers={"Authorization": f"Bearer {self.blob_token}"},
                                    timeout=self.timeout,
                                )
                                blob_response.raise_for_status()
                                self.logger.debug(f"Deleted overflow Blob: {blob_key}")
            except:
                pass  # Key might not exist or no overflow
            
            # Delete KV key
            response = requests.delete(
                f"{self.kv_url}/del/{key}",
                headers={'Authorization': f'Bearer {self.kv_token}'},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            return {'deleted': True, 'key': key}
        except Exception as e:
            raise NoSQLError(f"Vercel KV delete failed: {str(e)}")


class VercelBlobAdapter:
    """Vercel Blob Storage"""
    
    def __init__(self):
        from ..utils import setup_logger
        self.logger = setup_logger(__name__)
        self.blob_token = os.getenv("BLOB_READ_WRITE_TOKEN")
        self.timeout = int(os.getenv("VERCEL_BLOB_TIMEOUT", "10"))
    
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def put(self, key: str, data: bytes) -> str:
        try:
            import requests
            
            response = requests.put(
                f"https://blob.vercel-storage.com/{key}",
                headers={
                    "Authorization": f"Bearer {self.blob_token}",
                    "x-content-type": "application/octet-stream",
                },
                data=data,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()["url"]
        except Exception as e:
            raise StorageError(f"Vercel Blob put failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def get(self, key: str) -> bytes:
        try:
            import requests
            
            response = requests.get(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.blob_token}"},
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.content
        except Exception as e:
            raise StorageError(f"Vercel Blob get failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def delete(self, key: str) -> bool:
        try:
            import requests
            
            response = requests.delete(
                f"https://blob.vercel-storage.com/{key}",
                headers={"Authorization": f"Bearer {self.blob_token}"},
                timeout=self.timeout,
            )
            response.raise_for_status()
            return True
        except Exception as e:
            raise StorageError(f"Vercel Blob delete failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def list(self, prefix: str = "") -> List[str]:
        try:
            import requests
            
            response = requests.get(
                f"https://blob.vercel-storage.com/?prefix={prefix}",
                headers={"Authorization": f"Bearer {self.blob_token}"},
                timeout=self.timeout,
            )
            response.raise_for_status()
            return [blob["pathname"] for blob in response.json()["blobs"]]
        except Exception as e:
            raise StorageError(f"Vercel Blob list failed: {str(e)}")


class VercelQueueAdapter:
    """Vercel Queue adapter"""
    
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        raise NotImplementedError("Vercel Queue not yet available")
    
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        raise NotImplementedError("Vercel Queue not yet available")
    
    def delete(self, message_id: str, queue_name: str = "default") -> bool:
        raise NotImplementedError("Vercel Queue not yet available")