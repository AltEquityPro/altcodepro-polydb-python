# src/polydb/types.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union, runtime_checkable, Literal

from .query import QueryBuilder

StorageType = Literal["sql", "nosql"]

JsonDict = Dict[str, Any]
Lookup = Dict[str, Any]


@dataclass(frozen=True)
class ModelMeta:
    """Model metadata"""
    storage: StorageType
    table: Optional[str] = None
    collection: Optional[str] = None
    pk_field: Optional[str] = None
    rk_field: Optional[str] = None
    provider: Optional[str] = None
    cache: bool = False
    cache_ttl: Optional[int] = None


@runtime_checkable
class SQLAdapter(Protocol):
    """SQL adapter contract"""
    
    def insert(self, table: str, data: JsonDict) -> JsonDict: ...
    
    def select(
        self,
        table: str,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[JsonDict]: ...
    
    def select_page(
        self,
        table: str,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None
    ) -> Tuple[List[JsonDict], Optional[str]]: ...
    
    def update(
        self,
        table: str,
        entity_id: Union[Any, Lookup],
        data: JsonDict
    ) -> JsonDict: ...
    
    def upsert(self, table: str, data: JsonDict) -> JsonDict: ...
    
    def delete(
        self,
        table: str,
        entity_id: Union[Any, Lookup]
    ) -> JsonDict: ...
    
    def query_linq(
        self,
        table: str,
        builder: QueryBuilder
    ) -> Union[List[JsonDict], int]: ...
    
    def execute(self, sql: str, params: Optional[List[Any]] = None) -> None: ...


@runtime_checkable
class NoSQLKVAdapter(Protocol):
    """NoSQL KV adapter contract"""
    
    def put(self, model: type, data: JsonDict) -> JsonDict: ...
    
    def query(
        self,
        model: type,
        query: Optional[Lookup] = None,
        limit: Optional[int] = None,
        no_cache: bool = False,
        cache_ttl: Optional[int] = None
    ) -> List[JsonDict]: ...
    
    def query_page(
        self,
        model: type,
        query: Lookup,
        page_size: int,
        continuation_token: Optional[str] = None
    ) -> Tuple[List[JsonDict], Optional[str]]: ...
    
    def patch(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        data: JsonDict,
        *,
        etag: Optional[str] = None,
        replace: bool = False
    ) -> JsonDict: ...
    
    def upsert(
        self,
        model: type,
        data: JsonDict,
        *,
        replace: bool = False
    ) -> JsonDict: ...
    
    def delete(
        self,
        model: type,
        entity_id: Union[Any, Lookup],
        *,
        etag: Optional[str] = None
    ) -> JsonDict: ...
    
    def query_linq(
        self,
        model: type,
        builder: QueryBuilder
    ) -> Union[List[JsonDict], int]: ...