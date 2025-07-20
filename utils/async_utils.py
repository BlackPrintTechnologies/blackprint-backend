import asyncio
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Global thread pool for async operations
_thread_pool = None
_pool_lock = threading.Lock()

def get_thread_pool():
    """Get or create a thread pool for async operations"""
    global _thread_pool
    if _thread_pool is None:
        with _pool_lock:
            if _thread_pool is None:
                _thread_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="async_api")
                logger.info("Created thread pool for async API operations")
    return _thread_pool

def async_route(func):
    """
    Decorator to make Flask-RESTful methods async
    Properly handles coroutines and returns actual results
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Create a new event loop for this request
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Run the async function and get the result
                result = loop.run_until_complete(func(*args, **kwargs))
                return result
            finally:
                # Clean up the loop
                loop.close()
                
        except Exception as e:
            logger.error(f"Error in async route {func.__name__}: {e}")
            raise
    
    return wrapper

def async_route_with_cache(prefix: str, ttl: Optional[int] = None):
    """
    Combined decorator for async routes with caching
    Handles both async execution and Redis caching with proper request-based cache keys
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Import here to avoid circular imports
                from utils.redis_client import get_redis_client
                from flask import request
                import hashlib
                import json
                
                redis_client = get_redis_client()
                
                # Generate cache key if Redis is available
                cache_key = None
                if redis_client.is_connected():
                    # Create a comprehensive cache key including request data
                    cache_data = {
                        'function': func.__name__,
                        'args': [str(arg) for arg in args[1:] if not callable(arg)],  # Skip 'self'
                        'kwargs': {k: str(v) for k, v in kwargs.items() if not callable(v)},
                        'url_args': dict(request.args) if request.args else {},
                        'json_data': request.get_json(silent=True) or {},
                        'method': request.method,
                        'endpoint': request.endpoint
                    }
                    
                    # Create a hash from all request data
                    key_string = json.dumps(cache_data, sort_keys=True, default=str)
                    cache_key = hashlib.sha256(key_string.encode()).hexdigest()[:16]
                    
                    # Try to get from cache
                    cached_result = redis_client.get(prefix, cache_key)
                    if cached_result is not None:
                        logger.debug(f"Cache hit for {prefix}:{cache_key}")
                        return cached_result
                
                # Create a new event loop for this request
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    # Run the async function and get the result
                    result = loop.run_until_complete(func(*args, **kwargs))
                    
                    # Cache the result if Redis is available
                    if redis_client.is_connected() and cache_key:
                        redis_client.set(prefix, cache_key, result, ttl)
                        logger.debug(f"Cached result for {prefix}:{cache_key}")
                    
                    return result
                finally:
                    # Clean up the loop
                    loop.close()
                    
            except Exception as e:
                logger.error(f"Error in async cached route {func.__name__}: {e}")
                raise
        
        return wrapper
    return decorator

async def run_sync_in_executor(sync_func, *args, **kwargs):
    """
    Run a synchronous function in thread pool executor
    """
    loop = asyncio.get_event_loop()
    executor = get_thread_pool()
    
    # Create a wrapper for the sync function with its arguments
    def sync_wrapper():
        return sync_func(*args, **kwargs)
    
    return await loop.run_in_executor(executor, sync_wrapper)

async def run_controller_method(controller, method_name, *args, **kwargs):
    """
    Run a controller method asynchronously
    """
    method = getattr(controller, method_name)
    return await run_sync_in_executor(method, *args, **kwargs)

def setup_async_environment():
    """Setup the async environment for the Flask app"""
    # Ensure we have an event loop policy that works with threads
    if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    logger.info("Async environment setup complete") 

def invalidate_cache_pattern(prefix: str, pattern: str = None):
    """
    Utility function to invalidate cache entries
    """
    try:
        from utils.redis_client import get_redis_client
        
        redis_client = get_redis_client()
        if redis_client.is_connected():
            if pattern:
                # Clear specific pattern (implement if needed)
                logger.debug(f"Cache invalidation for pattern {pattern} in {prefix}")
            else:
                # Clear entire namespace
                cleared = redis_client.clear_namespace(prefix)
                logger.info(f"Invalidated {cleared} cache entries in namespace: {prefix}")
                return cleared
    except Exception as e:
        logger.error(f"Error invalidating cache: {e}")
    return 0 