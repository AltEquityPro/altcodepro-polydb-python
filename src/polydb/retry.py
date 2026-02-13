# src/polydb/retry.py
"""
Retry logic with exponential backoff and metrics hooks
"""

import time
import logging
from functools import wraps
from typing import Callable, Optional, Tuple, Type


# Metrics hooks for enterprise monitoring
class MetricsHooks:
    """Metrics hooks that users can override for monitoring"""
    
    @staticmethod
    def on_query_start(operation: str, **kwargs):
        """Called when query starts"""
        pass
    
    @staticmethod
    def on_query_end(operation: str, duration: float, success: bool, **kwargs):
        """Called when query ends"""
        pass
    
    @staticmethod
    def on_error(operation: str, error: Exception, **kwargs):
        """Called when error occurs"""
        pass


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0, 
        exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            
            logger = logging.getLogger(__name__)
            
            while attempt < max_attempts:
                start_time = time.time()
                try:
                    MetricsHooks.on_query_start(func.__name__, args=args, kwargs=kwargs)
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    MetricsHooks.on_query_end(func.__name__, duration, True)
                    return result
                except exceptions as e:
                    attempt += 1
                    duration = time.time() - start_time
                    MetricsHooks.on_query_end(func.__name__, duration, False)
                    MetricsHooks.on_error(func.__name__, e)
                    
                    if attempt >= max_attempts:
                        raise
                    
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            
        return wrapper
    return decorator