# src/polydb/adapters/DynamoDBAdapter.py
import os
import threading
from typing import Any, Dict, List, Optional
import boto3
from boto3.dynamodb.conditions import Key, Attr

from polydb.base.NoSQLKVAdapter import NoSQLKVAdapter

from ..errors import NoSQLError, ConnectionError
from ..retry import retry
from ..types import JsonDict
from ..models import PartitionConfig


class DynamoDBAdapter(NoSQLKVAdapter):
    """DynamoDB with S3 overflow (limit: 400KB per item)"""
    
    DYNAMODB_MAX_SIZE = 400 * 1024  # 400KB
    
    def __init__(self, partition_config: Optional[PartitionConfig] = None):
        super().__init__(partition_config)
        self.max_size = self.DYNAMODB_MAX_SIZE
        self.table_name = os.getenv("DYNAMODB_TABLE_NAME", "default")
        self.bucket_name = os.getenv("S3_OVERFLOW_BUCKET", "dynamodb-overflow")
        self._resource = None
        self._s3_client = None
        self._client_lock = threading.Lock()
        self._initialize()
    
    def _initialize(self):
        try:
            with self._client_lock:
                if not self._resource:
                    self._resource = boto3.resource('dynamodb')
                    self._s3_client = boto3.client('s3')
                    
                    # Ensure bucket exists
                    try:
                        self._s3_client.create_bucket(Bucket=self.bucket_name)
                    except:
                        pass  # Already exists
                    
                    self.logger.info("DynamoDB initialized with S3 overflow")
        except Exception as e:
            raise ConnectionError(f"DynamoDB init failed: {str(e)}")
    
    def _get_table(self, model: type):
        if not self._resource:
            self._initialize()
        
        meta = getattr(model, '__polydb__', {})
        table_name = meta.get('table') or self.table_name or model.__name__.lower()
        return self._resource.Table(table_name) # type: ignore
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _put_raw(self, model: type, pk: str, rk: str, data: JsonDict) -> JsonDict:
        try:
            import json
            import hashlib
            
            data_copy = dict(data)
            data_copy['PK'] = pk
            data_copy['SK'] = rk
            
            # Check size
            data_bytes = json.dumps(data_copy,default=json_safe).encode()
            data_size = len(data_bytes)
            
            if data_size > self.DYNAMODB_MAX_SIZE:
                # Store in S3
                blob_id = hashlib.md5(data_bytes).hexdigest()
                blob_key = f"overflow/{pk}/{rk}/{blob_id}.json"
                
                if self._s3_client:
                    self._s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=blob_key,
                        Body=data_bytes
                    )
                    self.logger.info(f"Stored overflow to S3: {blob_key} ({data_size} bytes)")
                
                # Store reference in DynamoDB
                reference_data = {
                    'PK': pk,
                    'SK': rk,
                    '_overflow': True,
                    '_blob_key': blob_key,
                    '_size': data_size,
                    '_checksum': blob_id,
                }
                
                table = self._get_table(model)
                table.put_item(Item=reference_data)
            else:
                # Store directly in DynamoDB
                table = self._get_table(model)
                table.put_item(Item=data_copy)
            
            return {'PK': pk, 'SK': rk}
        except Exception as e:
            raise NoSQLError(f"DynamoDB put failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _get_raw(self, model: type, pk: str, rk: str) -> Optional[JsonDict]:
        try:
            import json
            import hashlib
            
            table = self._get_table(model)
            response = table.get_item(Key={'PK': pk, 'SK': rk})
            
            if 'Item' not in response:
                return None
            
            item = response['Item']
            
            # Check if overflow
            if item.get('_overflow'):
                blob_key = item.get('_blob_key')
                checksum = item.get('_checksum')
                
                if blob_key and self._s3_client:
                    s3_response = self._s3_client.get_object(
                        Bucket=self.bucket_name,
                        Key=blob_key
                    )
                    blob_data = s3_response['Body'].read()
                    
                    # Verify checksum
                    actual_checksum = hashlib.md5(blob_data).hexdigest()
                    if actual_checksum != checksum:
                        raise NoSQLError(f"Checksum mismatch: expected {checksum}, got {actual_checksum}")
                    
                    retrieved = json.loads(blob_data.decode())
                    self.logger.debug(f"Retrieved overflow from S3: {blob_key}")
                    return retrieved
            
            return item
        except Exception as e:
            raise NoSQLError(f"DynamoDB get failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _query_raw(self, model: type, filters: Dict[str, Any], limit: Optional[int]) -> List[JsonDict]:
        try:
            table = self._get_table(model)
            
            # If PK in filters, use query, else scan
            if 'PK' in filters or 'partition_key' in filters:
                pk_value = filters.get('PK') or filters.get('partition_key')
                key_condition = Key('PK').eq(pk_value)
                
                if 'SK' in filters:
                    key_condition = key_condition & Key('SK').eq(filters['SK'])
                
                kwargs = {'KeyConditionExpression': key_condition}
                
                # Other filters as FilterExpression
                other_filters = {k: v for k, v in filters.items() if k not in ['PK', 'SK', 'partition_key']}
                if other_filters:
                    filter_expr = None
                    for k, v in other_filters.items():
                        expr = Attr(k).eq(v)
                        filter_expr = expr if filter_expr is None else filter_expr & expr
                    kwargs['FilterExpression'] = filter_expr # type: ignore
                
                if limit:
                    kwargs['Limit'] = limit # type: ignore
                
                response = table.query(**kwargs)
            else:
                # Scan with filters
                kwargs = {}
                if filters:
                    filter_expr = None
                    for k, v in filters.items():
                        expr = Attr(k).eq(v)
                        filter_expr = expr if filter_expr is None else filter_expr & expr
                    kwargs['FilterExpression'] = filter_expr
                
                if limit:
                    kwargs['Limit'] = limit
                
                response = table.scan(**kwargs)
            
            return response.get('Items', [])
        except Exception as e:
            raise NoSQLError(f"DynamoDB query failed: {str(e)}")
    
    @retry(max_attempts=3, delay=1.0, exceptions=(NoSQLError,))
    def _delete_raw(self, model: type, pk: str, rk: str, etag: Optional[str]) -> JsonDict:
        try:
            table = self._get_table(model)
            
            # Check if overflow before deleting
            try:
                response = table.get_item(Key={'PK': pk, 'SK': rk})
                if 'Item' in response:
                    item = response['Item']
                    
                    if item.get('_overflow'):
                        blob_key = item.get('_blob_key')
                        if blob_key and self._s3_client:
                            self._s3_client.delete_object(
                                Bucket=self.bucket_name,
                                Key=blob_key
                            )
                            self.logger.debug(f"Deleted overflow S3 object: {blob_key}")
            except:
                pass  # Item might not exist or no overflow
            
            # Delete DynamoDB item
            table.delete_item(Key={'PK': pk, 'SK': rk})
            return {'deleted': True, 'PK': pk, 'SK': rk}
        except Exception as e:
            raise NoSQLError(f"DynamoDB delete failed: {str(e)}")