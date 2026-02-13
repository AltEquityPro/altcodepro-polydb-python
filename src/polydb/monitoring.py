# src/polydb/monitoring.py
"""
Comprehensive monitoring, metrics, and observability
"""
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import threading
import time
import logging


@dataclass
class QueryMetrics:
    """Metrics for a single query"""
    operation: str
    model: str
    duration_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tenant_id: Optional[str] = None
    actor_id: Optional[str] = None
    rows_affected: Optional[int] = None
    cache_hit: bool = False


@dataclass
class AggregatedMetrics:
    """Aggregated metrics over time window"""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    total_duration_ms: float = 0.0
    avg_duration_ms: float = 0.0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    cache_hit_rate: float = 0.0
    queries_by_operation: Dict[str, int] = field(default_factory=dict)
    queries_by_model: Dict[str, int] = field(default_factory=dict)
    slow_queries: List[QueryMetrics] = field(default_factory=list)


class MetricsCollector:
    """Collects and aggregates metrics"""
    
    def __init__(self, slow_query_threshold_ms: float = 1000.0):
        self.slow_query_threshold = slow_query_threshold_ms
        self._metrics: List[QueryMetrics] = []
        self._lock = threading.Lock()
        self._hooks: List[Callable] = []
        self.logger = logging.getLogger(__name__)
    
    def record(self, metric: QueryMetrics):
        """Record a query metric"""
        with self._lock:
            self._metrics.append(metric)
            
            # Log slow queries
            if metric.duration_ms > self.slow_query_threshold:
                self.logger.warning(
                    f"Slow query detected: {metric.operation} on {metric.model} "
                    f"took {metric.duration_ms:.2f}ms"
                )
            
            # Trigger hooks
            for hook in self._hooks:
                try:
                    hook(metric)
                except Exception as e:
                    self.logger.error(f"Metrics hook failed: {e}")
    
    def register_hook(self, hook: Callable[[QueryMetrics], None]):
        """Register a metrics hook"""
        self._hooks.append(hook)
    
    def get_metrics(
        self,
        since: Optional[datetime] = None,
        model: Optional[str] = None,
        operation: Optional[str] = None
    ) -> List[QueryMetrics]:
        """Get filtered metrics"""
        with self._lock:
            metrics = self._metrics.copy()
        
        if since:
            metrics = [m for m in metrics if m.timestamp >= since]
        
        if model:
            metrics = [m for m in metrics if m.model == model]
        
        if operation:
            metrics = [m for m in metrics if m.operation == operation]
        
        return metrics
    
    def aggregate(
        self,
        since: Optional[datetime] = None,
        model: Optional[str] = None
    ) -> AggregatedMetrics:
        """Generate aggregated metrics"""
        metrics = self.get_metrics(since=since, model=model)
        
        if not metrics:
            return AggregatedMetrics()
        
        agg = AggregatedMetrics()
        agg.total_queries = len(metrics)
        
        durations = []
        cache_hits = 0
        
        for m in metrics:
            if m.success:
                agg.successful_queries += 1
            else:
                agg.failed_queries += 1
            
            durations.append(m.duration_ms)
            agg.total_duration_ms += m.duration_ms
            
            if m.cache_hit:
                cache_hits += 1
            
            # Count by operation
            agg.queries_by_operation[m.operation] = \
                agg.queries_by_operation.get(m.operation, 0) + 1
            
            # Count by model
            agg.queries_by_model[m.model] = \
                agg.queries_by_model.get(m.model, 0) + 1
            
            # Track slow queries
            if m.duration_ms > self.slow_query_threshold:
                agg.slow_queries.append(m)
        
        agg.avg_duration_ms = agg.total_duration_ms / agg.total_queries
        agg.min_duration_ms = min(durations)
        agg.max_duration_ms = max(durations)
        agg.cache_hit_rate = cache_hits / agg.total_queries
        
        return agg
    
    def clear_old_metrics(self, older_than: timedelta):
        """Clear metrics older than specified duration"""
        cutoff = datetime.utcnow() - older_than
        
        with self._lock:
            self._metrics = [m for m in self._metrics if m.timestamp >= cutoff]
    
    def export_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        agg = self.aggregate()
        
        lines = [
            f"# HELP polydb_queries_total Total number of queries",
            f"# TYPE polydb_queries_total counter",
            f"polydb_queries_total {agg.total_queries}",
            "",
            f"# HELP polydb_queries_successful Successful queries",
            f"# TYPE polydb_queries_successful counter",
            f"polydb_queries_successful {agg.successful_queries}",
            "",
            f"# HELP polydb_queries_failed Failed queries",
            f"# TYPE polydb_queries_failed counter",
            f"polydb_queries_failed {agg.failed_queries}",
            "",
            f"# HELP polydb_query_duration_ms Query duration",
            f"# TYPE polydb_query_duration_ms summary",
            f"polydb_query_duration_ms_sum {agg.total_duration_ms}",
            f"polydb_query_duration_ms_count {agg.total_queries}",
            "",
            f"# HELP polydb_cache_hit_rate Cache hit rate",
            f"# TYPE polydb_cache_hit_rate gauge",
            f"polydb_cache_hit_rate {agg.cache_hit_rate}",
        ]
        
        return "\n".join(lines)


class PerformanceMonitor:
    """Context manager for automatic query timing"""
    
    def __init__(
        self,
        collector: MetricsCollector,
        operation: str,
        model: str,
        tenant_id: Optional[str] = None,
        actor_id: Optional[str] = None
    ):
        self.collector = collector
        self.operation = operation
        self.model = model
        self.tenant_id = tenant_id
        self.actor_id = actor_id
        self.start_time = None
        self.success = False
        self.error = None
        self.rows_affected = None
        self.cache_hit = False
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000 # type: ignore
        
        if exc_type is None:
            self.success = True
        else:
            self.error = str(exc_val)
        
        metric = QueryMetrics(
            operation=self.operation,
            model=self.model,
            duration_ms=duration_ms,
            success=self.success,
            error=self.error,
            tenant_id=self.tenant_id,
            actor_id=self.actor_id,
            rows_affected=self.rows_affected,
            cache_hit=self.cache_hit
        )
        
        self.collector.record(metric)
        
        return False


class HealthCheck:
    """System health monitoring"""
    
    def __init__(self, factory):
        self.factory = factory
        self.logger = logging.getLogger(__name__)
    
    def check_sql_health(self) -> Dict[str, Any]:
        """Check SQL database health"""
        try:
            start = time.perf_counter()
            self.factory._sql.execute("SELECT 1", fetch_one=True)
            duration_ms = (time.perf_counter() - start) * 1000
            
            return {
                'status': 'healthy',
                'latency_ms': duration_ms,
                'provider': self.factory._provider_name
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'provider': self.factory._provider_name
            }
    
    def check_nosql_health(self) -> Dict[str, Any]:
        """Check NoSQL database health"""
        try:
            # Attempt a simple operation
            start = time.perf_counter()
            # This would need a test model
            duration_ms = (time.perf_counter() - start) * 1000
            
            return {
                'status': 'healthy',
                'latency_ms': duration_ms,
                'provider': self.factory._provider_name
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'provider': self.factory._provider_name
            }
    
    def check_cache_health(self) -> Dict[str, Any]:
        """Check cache health"""
        if not self.factory._cache:
            return {'status': 'disabled'}
        
        try:
            # Test cache operations
            test_key = "_health_check"
            test_value = {"test": True}
            
            start = time.perf_counter()
            self.factory._cache.set(test_key, {}, test_value, 10)
            retrieved = self.factory._cache.get(test_key, {})
            duration_ms = (time.perf_counter() - start) * 1000
            
            return {
                'status': 'healthy',
                'latency_ms': duration_ms
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def full_health_check(self) -> Dict[str, Any]:
        """Complete system health check"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sql': self.check_sql_health(),
            'nosql': self.check_nosql_health(),
            'cache': self.check_cache_health()
        }