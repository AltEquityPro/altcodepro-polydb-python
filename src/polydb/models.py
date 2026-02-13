# src/polydb/models.py
"""
Data models and configurations
"""

from dataclasses import dataclass
from typing import Optional, List, Callable, Any
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