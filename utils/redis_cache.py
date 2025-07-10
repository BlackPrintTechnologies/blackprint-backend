import redis
import json
import hashlib
import logging
from functools import wraps
import os

logger = logging.getLogger(__name__)

# Load configuration from app.json
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

class RedisCache:
    def __init__(self):
        try:
            self.redis_client = redis.Redis(
                host=config.get('REDIS_HOST', 'localhost'),
                port=int(config.get('REDIS_PORT', 6379)),
                db=int(config.get('REDIS_DB', 0)),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.redis_client.ping()
            logger.info("✅ Redis connection established successfully")
            # Test basic operations
            test_key = "test_connection"
            self.redis_client.set(test_key, "test_value", ex=10)
            test_value = self.redis_client.get(test_key)
            if test_value == "test_value":
                logger.info("✅ Redis read/write test successful")
            else:
                logger.warning("⚠️ Redis read/write test failed")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Caching will be disabled.")
            self.redis_client = None
    
    def set(self, key, value, ttl=3600):
        """Set a value in Redis with TTL"""
        if not self.redis_client:
            return False
        try:
            # Handle different value types
            if isinstance(value, (dict, list, tuple)):
                value = json.dumps(value)
            elif not isinstance(value, (str, int, float, bytes)):
                value = json.dumps(value)
            self.redis_client.setex(key, ttl, value)
            logger.info(f"💾 Redis CACHE SET: {key} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.error(f"Redis SET error: {e}")
            return False
    
    def get(self, key):
        """Get a value from Redis"""
        if not self.redis_client:
            return None
        try:
            value = self.redis_client.get(key)
            if value:
                logger.info(f"✅ Redis CACHE HIT: {key}")
                # Try to parse as JSON, if fails return as string
                try:
                    parsed_value = json.loads(value)
                    # If it's a tuple (response, status_code), return as tuple
                    if isinstance(parsed_value, list) and len(parsed_value) == 2:
                        return (parsed_value[0], parsed_value[1])
                    return parsed_value
                except:
                    return value
            else:
                logger.info(f"❌ Redis CACHE MISS: {key}")
                return None
        except Exception as e:
            logger.error(f"Redis GET error: {e}")
            return None
    
    def delete(self, key):
        """Delete a key from Redis"""
        if not self.redis_client:
            return False
        try:
            result = self.redis_client.delete(key)
            logger.info(f"Cache DELETE: {key}")
            return result > 0
        except Exception as e:
            logger.error(f"Redis DELETE error: {e}")
            return False
    
    def exists(self, key):
        """Check if key exists in Redis"""
        if not self.redis_client:
            return False
        try:
            return self.redis_client.exists(key) > 0
        except Exception as e:
            logger.error(f"Redis EXISTS error: {e}")
            return False
    
    def flush_all(self):
        """Clear all cache"""
        if not self.redis_client:
            return False
        try:
            self.redis_client.flushdb()
            logger.info("Cache FLUSH ALL")
            return True
        except Exception as e:
            logger.error(f"Redis FLUSH error: {e}")
            return False

# Global Redis instance
redis_cache = RedisCache()

def cache_response(prefix, ttl=3600):
    """Decorator to cache API responses"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            key_parts = [prefix]
            key_parts.extend([str(arg) for arg in args])
            key_parts.extend([f"{k}:{v}" for k, v in sorted(kwargs.items())])
            cache_key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            
            # Try to get from cache
            cached_result = redis_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            redis_cache.set(cache_key, result, ttl)
            return result
        return wrapper
    return decorator

def generate_cache_key(prefix, *args, **kwargs):
    """Generate a cache key from prefix and arguments"""
    key_parts = [prefix]
    key_parts.extend([str(arg) for arg in args])
    key_parts.extend([f"{k}:{v}" for k, v in sorted(kwargs.items())])
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()

def invalidate_cache_pattern(pattern):
    """Invalidate all cache keys matching a pattern"""
    if not redis_cache.redis_client:
        return 0
    try:
        keys = redis_cache.redis_client.keys(pattern)
        if keys:
            redis_cache.redis_client.delete(*keys)
            logger.info(f"Cache INVALIDATED: {len(keys)} keys matching pattern '{pattern}'")
            return len(keys)
        return 0
    except Exception as e:
        logger.error(f"Cache invalidation error: {e}")
        return 0

def invalidate_property_cache(fid=None):
    """Invalidate property-related cache"""
    if fid:
        # Invalidate specific property cache
        pattern = f"*property_search*{fid}*"
        invalidate_cache_pattern(pattern)
        pattern = f"*demographic*{fid}*"
        invalidate_cache_pattern(pattern)
        pattern = f"*commercial_growth*{fid}*"
        invalidate_cache_pattern(pattern)
    else:
        # Invalidate all property cache
        invalidate_cache_pattern("*property*")
        invalidate_cache_pattern("*demographic*")
        invalidate_cache_pattern("*commercial_growth*") 