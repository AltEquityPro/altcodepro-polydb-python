# src/polydb/cache_advanced.py
"""
Advanced caching with Redis support and strategies
"""
from typing import Any, Dict, Optional, List
import json
import hashlib
import threading
from enum import Enum
import redis

class CacheStrategy(Enum):
    """Cache invalidation strategies"""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    TTL = "ttl"  # Time To Live
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"


class RedisCacheEngine:
    """Redis-based distributed cache"""
    
    def __init__(
        self,
        redis_url: Optional[str] = None,
        prefix: str = "polydb:",
        default_ttl: int = 3600
    ):
        self.prefix = prefix
        self.default_ttl = default_ttl
        self._client = None
        self._lock = threading.Lock()
        self.redis_url = redis_url
        self._initialize()
    
    def _initialize(self):
        """Initialize Redis connection"""
        try:
            with self._lock:
                if not self._client:
                    if self.redis_url:
                        self._client = redis.from_url(self.redis_url)
                    else:
                        import os
                        redis_host = os.getenv('REDIS_HOST', 'localhost')
                        redis_port = int(os.getenv('REDIS_PORT', '6379'))
                        redis_db = int(os.getenv('REDIS_DB', '0'))
                        
                        self._client = redis.Redis(
                            host=redis_host,
                            port=redis_port,
                            db=redis_db,
                            decode_responses=True
                        )
        except ImportError:
            raise ImportError("Redis not installed. Install with: pip install redis")
    
    def _make_key(self, model: str, query: Dict[str, Any]) -> str:
        """Generate cache key"""
        query_str = json.dumps(query, sort_keys=True)
        query_hash = hashlib.md5(query_str.encode()).hexdigest()
        return f"{self.prefix}{model}:{query_hash}"
    
    def get(self, model: str, query: Dict[str, Any]) -> Optional[Any]:
        """Get from cache"""
        if not self._client:
            return None
        
        key = self._make_key(model, query)
        
        try:
            data = self._client.get(key)
            if data:
                # Increment access count for LFU
                self._client.incr(f"{key}:access_count")
                return json.loads(data)
            return None
        except Exception:
            return None
    
    def set(
        self,
        model: str,
        query: Dict[str, Any],
        value: Any,
        ttl: Optional[int] = None
    ):
        """Set cache with TTL"""
        if not self._client:
            return
        
        key = self._make_key(model, query)
        ttl = ttl or self.default_ttl
        
        try:
            data = json.dumps(value)
            self._client.setex(key, ttl, data)
            
            # Initialize access count
            self._client.set(f"{key}:access_count", 0, ex=ttl)
        except Exception:
            pass
    
    def invalidate(
        self,
        model: str,
        query: Optional[Dict[str, Any]] = None
    ):
        """Invalidate cache"""
        if not self._client:
            return
        
        if query:
            key = self._make_key(model, query)
            self._client.delete(key, f"{key}:access_count")
        else:
            # Invalidate all for model
            pattern = f"{self.prefix}{model}:*"
            keys = self._client.keys(pattern)
            if keys:
                self._client.delete(*keys)
    
    def clear(self):
        """Clear entire cache"""
        if not self._client:
            return
        
        pattern = f"{self.prefix}*"
        keys = self._client.keys(pattern)
        if keys:
            self._client.delete(*keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self._client:
            return {}
        
        try:
            info = self._client.info('stats')
            return {
                'hits': info.get('keyspace_hits', 0),
                'misses': info.get('keyspace_misses', 0),
                'hit_rate': info.get('keyspace_hits', 0) / 
                           max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1)
            }
        except Exception:
            return {}


class CacheWarmer:
    """Pre-populate cache with frequently accessed data"""
    
    def __init__(self, factory, cache_engine):
        self.factory = factory
        self.cache = cache_engine
    
    def warm_model(
        self,
        model,
        queries: List[Dict[str, Any]],
        ttl: Optional[int] = None
    ):
        """Warm cache for specific queries"""
        for query in queries:
            try:
                results = self.factory.read(model, query=query)
                self.cache.set(
                    model.__name__ if isinstance(model, type) else model,
                    query,
                    results,
                    ttl
                )
            except Exception:
                pass
    
    def warm_popular_queries(
        self,
        model,
        limit: int = 10,
        ttl: Optional[int] = None
    ):
        """Warm cache with most popular queries"""
        # This would need query log analysis
        # Placeholder implementation
        pass


class CacheInvalidationStrategy:
    """Manages cache invalidation strategies"""
    
    def __init__(self, cache_engine, strategy: CacheStrategy = CacheStrategy.TTL):
        self.cache = cache_engine
        self.strategy = strategy
    
    def invalidate_on_write(self, model: str, data: Dict[str, Any]):
        """Invalidate cache on write operations"""
        if self.strategy in [CacheStrategy.WRITE_THROUGH, CacheStrategy.WRITE_BACK]:
            self.cache.invalidate(model)
    
    def invalidate_related(self, model: str, relationships: List[str]):
        """Invalidate related models"""
        for related in relationships:
            self.cache.invalidate(related)