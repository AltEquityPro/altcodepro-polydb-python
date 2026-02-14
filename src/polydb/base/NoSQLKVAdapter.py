# src/polydb/adapters/NoSQLKVAdapter.py
from __future__ import annotations

import hashlib
import json
import threading
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from ..json_safe import json_safe

from ..errors import NoSQLError, StorageError
from ..retry import retry
from ..query import QueryBuilder, Operator
from ..types import JsonDict, Lookup

if TYPE_CHECKING:
    from ..models import PartitionConfig


class NoSQLKVAdapter:
    """Base with auto-overflow and LINQ support"""
    
    def __init__(self, partition_config: Optional[PartitionConfig] = None):
        from ..utils import setup_logger
        self.logger = setup_logger(self.__class__.__name__)
        self.partition_config = partition_config
        self.object_storage = None
        self._lock = threading.Lock()
        self.max_size = 1024 * 1024  # 1MB
    
    def _get_pk_rk(self, model: type, data: JsonDict) -> Tuple[str, str]:
        """Extract PK/RK from model metadata"""
        meta = getattr(model, '__polydb__', {})
        pk_field = meta.get('pk_field', 'id')
        rk_field = meta.get('rk_field')
        
        if self.partition_config:
            try:
                pk = self.partition_config.partition_key_template.format(**data)
            except KeyError:
                pk = f"default_{data.get(pk_field, hashlib.md5(json.dumps(data, sort_keys=True,default=json_safe).encode()).hexdigest()[:8])}"
        else:
            pk = str(data.get(pk_field, 'default'))
        
        if rk_field and rk_field in data:
            rk = str(data[rk_field])
        elif self.partition_config and self.partition_config.row_key_template:
            try:
                rk = self.partition_config.row_key_template.format(**data)
            except KeyError:
                rk = hashlib.md5(json.dumps(data, sort_keys=True,default=json_safe).encode()).hexdigest()
        else:
            rk = data.get('id', hashlib.md5(json.dumps(data, sort_keys=True,default=json_safe).encode()).hexdigest())
        
        return str(pk), str(rk)
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _check_overflow(self, data: JsonDict) -> Tuple[JsonDict, Optional[str]]:
        """Check size and store in blob if needed"""
        data_bytes = json.dumps(data,default=json_safe).encode()
        data_size = len(data_bytes)
        
        if data_size > self.max_size:
            with self._lock:
                if not self.object_storage:
                    from ..factory import CloudDatabaseFactory
                    factory = CloudDatabaseFactory()
                    self.object_storage = factory.get_object_storage()
            
            blob_id = hashlib.md5(data_bytes).hexdigest()
            blob_key = f"overflow/{blob_id}.json"
            
            try:
                self.object_storage.put(blob_key, data_bytes)
            except Exception as e:
                raise StorageError(f"Overflow storage failed: {str(e)}")
            
            return {
                "_overflow": True,
                "_blob_key": blob_key,
                "_size": data_size,
                "_checksum": blob_id
            }, blob_key
        
        return data, None
    
    @retry(max_attempts=3, delay=1.0, exceptions=(StorageError,))
    def _retrieve_overflow(self, data: JsonDict) -> JsonDict:
        """Retrieve from blob if overflow"""
        if not data.get("_overflow"):
            return data
        
        with self._lock:
            if not self.object_storage:
                from ..factory import CloudDatabaseFactory
                factory = CloudDatabaseFactory()
                self.object_storage = factory.get_object_storage()
        
        try:
            blob_data = self.object_storage.get(data["_blob_key"])
            retrieved = json.loads(blob_data.decode())
            
            # Verify checksum
            checksum = hashlib.md5(blob_data).hexdigest()
            if checksum != data.get("_checksum"):
                raise StorageError("Checksum mismatch on overflow retrieval")
            
            return retrieved
        except Exception as e:
            raise StorageError(f"Overflow retrieval failed: {str(e)}")
    
    def _apply_filters(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply filters in-memory for NoSQL"""
        if not builder.filters:
            return results
        
        filtered = []
        for item in results:
            match = True
            for f in builder.filters:
                value = item.get(f.field)
                
                if f.operator == Operator.EQ and value != f.value:
                    match = False
                elif f.operator == Operator.NE and value == f.value:
                    match = False
                elif f.operator == Operator.GT and not (value and value > f.value):
                    match = False
                elif f.operator == Operator.GTE and not (value and value >= f.value):
                    match = False
                elif f.operator == Operator.LT and not (value and value < f.value):
                    match = False
                elif f.operator == Operator.LTE and not (value and value <= f.value):
                    match = False
                elif f.operator == Operator.IN and value not in f.value:
                    match = False
                elif f.operator == Operator.NOT_IN and value in f.value:
                    match = False
                elif f.operator == Operator.CONTAINS and (not value or f.value not in str(value)):
                    match = False
                elif f.operator == Operator.STARTS_WITH and (not value or not str(value).startswith(f.value)):
                    match = False
                elif f.operator == Operator.ENDS_WITH and (not value or not str(value).endswith(f.value)):
                    match = False
                
                if not match:
                    break
            
            if match:
                filtered.append(item)
        
        return filtered
    
    def _apply_ordering(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply ordering"""
        if not builder.order_by_fields:
            return results
        
        for field, desc in reversed(builder.order_by_fields):
            results = sorted(
                results,
                key=lambda x: x.get(field, ''),
                reverse=desc
            )
        
        return results
    
    def _apply_pagination(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply skip/take"""
        if builder.skip_count:
            results = results[builder.skip_count:]
        
        if builder.take_count:
            results = results[:builder.take_count]
        
        return results
    
    def _apply_projection(self, results: List[JsonDict], builder: QueryBuilder) -> List[JsonDict]:
        """Apply field selection"""
        if not builder.select_fields:
            return results
        
        return [
            {k: v for k, v in item.items() if k in builder.select_fields}
            for item in results
        ]
    
    # Abstract methods to implement
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        raise NotImplementedError
    
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        raise NotImplementedError
    
    def _query_raw(self, model: type, filters: Dict[str, Any], limit: Optional[int]) -> List[JsonDict]:
        raise NotImplementedError
    
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        raise NotImplementedError
    
    # Protocol implementation
    def put(self, model: type, data: JsonDict) -> JsonDict:
        pk, rk = self._get_pk_rk(model, data)
        store_data, _ = self._check_overflow(data)
        return self._put_raw(model, pk, rk, store_data)
    
    def query(
        self,
        model: type,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        no_cache: bool = False,
        cache_ttl: Optional[int] = None
    ) -> List[JsonDict]:
        results = self._query_raw(model, query or {}, limit)
        return [self._retrieve_overflow(r) for r in results]
    
    def query_page(
        self,
        model: type,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None
    ) -> Tuple[List[JsonDict], Optional[str]]:
        # Basic implementation - override per provider
        offset = int(continuation_token) if continuation_token else 0
        results = self.query(model, query, limit=page_size + 1)
        
        has_more = len(results) > page_size
        if has_more:
            results = results[:page_size]
        
        next_token = str(offset + page_size) if has_more else None
        return results, next_token
    
    def patch(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False
    ) -> JsonDict:
        if isinstance(entity_id, dict):
            pk = entity_id.get('partition_key') or entity_id.get('pk')
            rk = entity_id.get('row_key') or entity_id.get('rk') or entity_id.get('id')
        else:
            pk, rk = self._get_pk_rk(model, {'id': entity_id})
        
        if not replace:
            existing = self._get_raw(model, pk, rk) # type: ignore
            if existing:
                existing = self._retrieve_overflow(existing)
                existing.update(data)
                data = existing
        
        store_data, _ = self._check_overflow(data)
        return self._put_raw(model, pk, rk, store_data) # type: ignore
    
    def upsert(self, model: type, data: JsonDict, *, replace: bool = False) -> JsonDict:
        return self.put(model, data)
    
    def delete(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None
    ) -> JsonDict:
        if isinstance(entity_id, dict):
            pk = entity_id.get('partition_key') or entity_id.get('pk')
            rk = entity_id.get('row_key') or entity_id.get('rk') or entity_id.get('id')
        else:
            pk, rk = self._get_pk_rk(model, {'id': entity_id})
        
        return self._delete_raw(model, pk, rk, etag) # type: ignore
    
    def query_linq(self, model: type, builder: QueryBuilder) -> Union[List[JsonDict], int]:
        """LINQ-style query"""
        results = self._query_raw(model, {}, None)
        results = [self._retrieve_overflow(r) for r in results]
        
        results = self._apply_filters(results, builder)
        
        if builder.count_only:
            return len(results)
        
        results = self._apply_ordering(results, builder)
        results = self._apply_pagination(results, builder)
        results = self._apply_projection(results, builder)
        
        if builder.distinct:
            seen = set()
            unique = []
            for r in results:
                key = json.dumps(r, sort_keys=True,default=json_safe)
                if key not in seen:
                    seen.add(key)
                    unique.append(r)
            results = unique
        
        return results