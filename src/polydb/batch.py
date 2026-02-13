# src/polydb/batch.py
"""
Batch operations for high-throughput scenarios
"""
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from .types import JsonDict


@dataclass
class BatchResult:
    succeeded: List[JsonDict]
    failed: List[Dict[str, Any]]  # {data, error}
    total: int
    success_count: int
    error_count: int


class BatchOperations:
    """Batch CRUD operations"""
    
    def __init__(self, factory):
        self.factory = factory
    
    def bulk_insert(
        self,
        model: Union[type, str],
        records: List[JsonDict],
        *,
        chunk_size: int = 100,
        fail_fast: bool = False
    ) -> BatchResult:
        """Bulk insert with chunking"""
        succeeded = []
        failed = []
        
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            
            for record in chunk:
                try:
                    result = self.factory.create(model, record)
                    succeeded.append(result)
                except Exception as e:
                    failed.append({
                        'data': record,
                        'error': str(e)
                    })
                    
                    if fail_fast:
                        break
        
        return BatchResult(
            succeeded=succeeded,
            failed=failed,
            total=len(records),
            success_count=len(succeeded),
            error_count=len(failed)
        )
    
    def bulk_update(
        self,
        model: Union[type, str],
        updates: List[Dict[str, Any]],  # {entity_id, data}
        *,
        chunk_size: int = 100,
        fail_fast: bool = False
    ) -> BatchResult:
        """Bulk update"""
        succeeded = []
        failed = []
        
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]
            
            for update in chunk:
                try:
                    result = self.factory.update(
                        model,
                        update['entity_id'],
                        update['data']
                    )
                    succeeded.append(result)
                except Exception as e:
                    failed.append({
                        'data': update,
                        'error': str(e)
                    })
                    
                    if fail_fast:
                        break
        
        return BatchResult(
            succeeded=succeeded,
            failed=failed,
            total=len(updates),
            success_count=len(succeeded),
            error_count=len(failed)
        )
    
    def bulk_delete(
        self,
        model: Union[type, str],
        entity_ids: List[Any],
        *,
        chunk_size: int = 100,
        fail_fast: bool = False,
        hard: bool = False
    ) -> BatchResult:
        """Bulk delete"""
        succeeded = []
        failed = []
        
        for i in range(0, len(entity_ids), chunk_size):
            chunk = entity_ids[i:i + chunk_size]
            
            for entity_id in chunk:
                try:
                    result = self.factory.delete(model, entity_id, hard=hard)
                    succeeded.append(result)
                except Exception as e:
                    failed.append({
                        'data': {'entity_id': entity_id},
                        'error': str(e)
                    })
                    
                    if fail_fast:
                        break
        
        return BatchResult(
            succeeded=succeeded,
            failed=failed,
            total=len(entity_ids),
            success_count=len(succeeded),
            error_count=len(failed)
        )


class TransactionManager:
    """Transaction support for SQL operations"""
    
    def __init__(self, sql_adapter):
        self.sql = sql_adapter
    
    def __enter__(self):
        self.conn = self.sql._get_connection()
        self.conn.autocommit = False
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        
        self.sql._return_connection(self.conn)
        return False
    
    def execute(self, sql: str, params: Optional[List[Any]] = None):
        """Execute within transaction"""
        cursor = self.conn.cursor()
        cursor.execute(sql, params or [])
        cursor.close()