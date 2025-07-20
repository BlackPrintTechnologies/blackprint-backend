import functools
import logging
import hashlib
import json
from typing import Optional, Any, Callable
from utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)

def cache_response(prefix: str, ttl: Optional[int] = None, key_generator: Optional[Callable] = None):
    """
    Decorator for caching API responses
    
    Args:
        prefix: Cache namespace (e.g., 'property', 'demographic', 'user')
        ttl: Time to live in seconds
        key_generator: Custom function to generate cache key from function args
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            redis_client = get_redis_client()
            
            # Skip caching if Redis is not available
            if not redis_client.is_connected():
                logger.debug(f"Redis not available, executing {func.__name__} without cache")
                return func(*args, **kwargs)
            
            # Generate cache key
            if key_generator:
                cache_key = key_generator(*args, **kwargs)
            else:
                cache_key = _default_key_generator(func.__name__, *args, **kwargs)
            
            # Try to get from cache
            cached_result = redis_client.get(prefix, cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {prefix}:{cache_key}")
                return cached_result
            
            # Execute function and cache result
            logger.debug(f"Cache miss for {prefix}:{cache_key}, executing function")
            result = func(*args, **kwargs)
            
            # Cache the result
            redis_client.set(prefix, cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator

def cache_async_response(prefix: str, ttl: Optional[int] = None, key_generator: Optional[Callable] = None):
    """
    Decorator for caching async API responses
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            redis_client = get_redis_client()
            
            # Skip caching if Redis is not available
            if not redis_client.is_connected():
                logger.debug(f"Redis not available, executing {func.__name__} without cache")
                return await func(*args, **kwargs)
            
            # Generate cache key
            if key_generator:
                cache_key = key_generator(*args, **kwargs)
            else:
                cache_key = _default_key_generator(func.__name__, *args, **kwargs)
            
            # Try to get from cache
            cached_result = redis_client.get(prefix, cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {prefix}:{cache_key}")
                return cached_result
            
            # Execute function and cache result
            logger.debug(f"Cache miss for {prefix}:{cache_key}, executing function")
            result = await func(*args, **kwargs)
            
            # Cache the result
            redis_client.set(prefix, cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator

def invalidate_cache(prefix: str, key_pattern: Optional[str] = None):
    """
    Decorator to invalidate cache after certain operations
    
    Args:
        prefix: Cache namespace to invalidate
        key_pattern: Specific key pattern to invalidate (if None, clears entire namespace)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            # Invalidate cache after successful execution
            redis_client = get_redis_client()
            if redis_client.is_connected():
                if key_pattern:
                    # Invalidate specific pattern (to be implemented if needed)
                    logger.debug(f"Cache invalidation for pattern {key_pattern} not implemented")
                else:
                    # Clear entire namespace
                    cleared = redis_client.clear_namespace(prefix)
                    logger.debug(f"Invalidated {cleared} cache entries in namespace: {prefix}")
            
            return result
        
        return wrapper
    return decorator

def _default_key_generator(func_name: str, *args, **kwargs) -> str:
    """
    Default cache key generator
    Creates a hash from function name and arguments
    """
    # Create a string representation of all arguments
    key_data = {
        'function': func_name,
        'args': [str(arg) for arg in args[1:] if not callable(arg)],  # Skip 'self' parameter
        'kwargs': {k: str(v) for k, v in kwargs.items() if not callable(v)}
    }
    
    key_string = json.dumps(key_data, sort_keys=True)
    return hashlib.sha256(key_string.encode()).hexdigest()[:16]

# Specialized cache decorators for common use cases
def cache_property_data(ttl: int = 3600):
    """Cache property-related data for 1 hour by default"""
    return cache_response('property', ttl)

def cache_demographic_data(ttl: int = 7200):
    """Cache demographic data for 2 hours by default (changes less frequently)"""
    return cache_response('demographic', ttl)

def cache_user_data(ttl: int = 1800):
    """Cache user data for 30 minutes by default"""
    return cache_response('user', ttl)

def cache_search_results(ttl: int = 3600):
    """Cache search results for 1 hour by default"""
    return cache_response('search', ttl)

def cache_layer_data(ttl: int = 14400):
    """Cache layer data for 4 hours by default (most stable data)"""
    return cache_response('layer', ttl)

# Custom key generators for specific use cases
def property_key_generator(*args, **kwargs) -> str:
    """Generate cache key for property-related operations"""
    if len(args) > 1:
        # Assuming pattern: method(self, user_id, fid, ...)
        user_id = args[1] if len(args) > 1 else kwargs.get('user_id', 'unknown')
        fid = args[2] if len(args) > 2 else kwargs.get('fid', 'unknown')
        return f"user_{user_id}_fid_{fid}"
    return _default_key_generator('property', *args, **kwargs)

def demographic_key_generator(*args, **kwargs) -> str:
    """Generate cache key for demographic data"""
    if len(args) > 1:
        fid = args[1] if len(args) > 1 else kwargs.get('fid', 'unknown')
        user_id = args[2] if len(args) > 2 else kwargs.get('user_id', 'unknown')
        return f"fid_{fid}_user_{user_id}"
    return _default_key_generator('demographic', *args, **kwargs)

def market_info_key_generator(*args, **kwargs) -> str:
    """Generate cache key for market info data"""
    # Extract market IDs from arguments
    spot2_id = kwargs.get('spot2_id', args[1] if len(args) > 1 else 'none')
    inmuebles24_id = kwargs.get('inmuebles24_id', args[2] if len(args) > 2 else 'none')
    propiedades_id = kwargs.get('propiedades_id', args[3] if len(args) > 3 else 'none')
    
    return f"spot2_{spot2_id}_inmuebles_{inmuebles24_id}_prop_{propiedades_id}" 