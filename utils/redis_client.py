import redis
import json
import pickle
import logging
import os
import time
from typing import Optional, Any, Union
from contextlib import contextmanager
import hashlib

logger = logging.getLogger(__name__)

class RedisClient:
    """
    Redis client optimized for backend caching with error handling and connection management
    """
    
    def __init__(self):
        # Load configuration from app.json
        config = self._load_config()
        
        self.host = config.get('REDIS_HOST', 'localhost')
        self.port = int(config.get('REDIS_PORT', 6379))
        self.db = int(config.get('REDIS_DB', 0))
        self.password = config.get('REDIS_PASSWORD', None) or None
        self.socket_timeout = int(config.get('REDIS_SOCKET_TIMEOUT', 5))
        self.socket_connect_timeout = int(config.get('REDIS_CONNECT_TIMEOUT', 5))
        self.max_connections = int(config.get('REDIS_MAX_CONNECTIONS', 50))
        
        # Default TTL values (in seconds)
        self.default_ttl = int(config.get('REDIS_DEFAULT_TTL', 3600))  # 1 hour
        self.short_ttl = int(config.get('REDIS_SHORT_TTL', 300))      # 5 minutes
        self.long_ttl = int(config.get('REDIS_LONG_TTL', 86400))     # 24 hours
        
        # Connection pool
        self.connection_pool = None
        self.redis_client = None
        self._initialize_connection()
    
    def _load_config(self):
        """Load configuration from app.json"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'app.json')
            with open(config_path, 'r') as config_file:
                return json.load(config_file)
        except Exception as e:
            logger.warning(f"Failed to load app.json, using environment variables: {e}")
            # Fallback to environment variables
            return {
                'REDIS_HOST': os.getenv('REDIS_HOST', 'localhost'),
                'REDIS_PORT': os.getenv('REDIS_PORT', '6379'),
                'REDIS_DB': os.getenv('REDIS_DB', '0'),
                'REDIS_PASSWORD': os.getenv('REDIS_PASSWORD', ''),
                'REDIS_DEFAULT_TTL': os.getenv('REDIS_DEFAULT_TTL', '3600'),
                'REDIS_SHORT_TTL': os.getenv('REDIS_SHORT_TTL', '300'),
                'REDIS_LONG_TTL': os.getenv('REDIS_LONG_TTL', '86400'),
                'REDIS_SOCKET_TIMEOUT': os.getenv('REDIS_SOCKET_TIMEOUT', '5'),
                'REDIS_CONNECT_TIMEOUT': os.getenv('REDIS_CONNECT_TIMEOUT', '5'),
                'REDIS_MAX_CONNECTIONS': os.getenv('REDIS_MAX_CONNECTIONS', '50')
            }
    
    def _initialize_connection(self):
        """Initialize Redis connection pool"""
        try:
            self.connection_pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                max_connections=self.max_connections,
                decode_responses=False,  # We handle encoding/decoding manually
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"✅ Redis connected successfully at {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.redis_client = None
    
    def is_connected(self) -> bool:
        """Check if Redis is connected and available"""
        try:
            if self.redis_client:
                self.redis_client.ping()
                return True
        except Exception:
            pass
        return False
    
    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value for Redis storage"""
        try:
            if isinstance(value, (str, int, float)):
                return str(value).encode('utf-8')
            else:
                return pickle.dumps(value)
        except Exception as e:
            logger.error(f"❌ Serialization failed: {e}")
            raise
    
    def _deserialize_value(self, value: bytes) -> Any:
        """Deserialize value from Redis"""
        try:
            # Try pickle first (for complex objects)
            try:
                return pickle.loads(value)
            except:
                # Fallback to string decoding
                return value.decode('utf-8')
        except Exception as e:
            logger.error(f"❌ Deserialization failed: {e}")
            return None
    
    def _make_key(self, prefix: str, key: str) -> str:
        """Create a namespaced cache key"""
        return f"backend_cache:{prefix}:{key}"
    
    def _make_hash_key(self, data: Union[str, dict]) -> str:
        """Create a hash key from data for consistent caching"""
        if isinstance(data, dict):
            data = json.dumps(data, sort_keys=True)
        return hashlib.sha256(str(data).encode()).hexdigest()[:16]
    
    def set(self, prefix: str, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value in Redis cache
        
        Args:
            prefix: Cache namespace (e.g., 'property', 'user', 'demographic')
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (default: 1 hour)
        """
        if not self.is_connected():
            logger.warning("⚠️ Redis not connected, skipping cache set")
            return False
        
        try:
            redis_key = self._make_key(prefix, key)
            serialized_value = self._serialize_value(value)
            
            if ttl is None:
                ttl = self.default_ttl
            
            result = self.redis_client.setex(redis_key, ttl, serialized_value)
            
            if result:
                logger.debug(f"✅ Cached {prefix}:{key} (TTL: {ttl}s)")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to set cache {prefix}:{key} - {e}")
            return False
    
    def get(self, prefix: str, key: str) -> Optional[Any]:
        """
        Get a value from Redis cache
        
        Args:
            prefix: Cache namespace
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        if not self.is_connected():
            logger.warning("⚠️ Redis not connected, skipping cache get")
            return None
        
        try:
            redis_key = self._make_key(prefix, key)
            cached_data = self.redis_client.get(redis_key)
            
            if cached_data:
                logger.debug(f"✅ Cache hit for {prefix}:{key}")
                return self._deserialize_value(cached_data)
            else:
                logger.debug(f"❌ Cache miss for {prefix}:{key}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to get cache {prefix}:{key} - {e}")
            return None
    
    def delete(self, prefix: str, key: str) -> bool:
        """Delete a cache entry"""
        if not self.is_connected():
            return False
        
        try:
            redis_key = self._make_key(prefix, key)
            result = self.redis_client.delete(redis_key)
            logger.debug(f"🗑️ Deleted cache {prefix}:{key}")
            return result > 0
        except Exception as e:
            logger.error(f"❌ Failed to delete cache {prefix}:{key} - {e}")
            return False
    
    def clear_namespace(self, prefix: str) -> int:
        """Clear all cache entries in a namespace"""
        if not self.is_connected():
            return 0
        
        try:
            pattern = f"backend_cache:{prefix}:*"
            keys = self.redis_client.keys(pattern)
            if keys:
                result = self.redis_client.delete(*keys)
                logger.info(f"🗑️ Cleared {result} keys from namespace: {prefix}")
                return result
            return 0
        except Exception as e:
            logger.error(f"❌ Failed to clear namespace {prefix} - {e}")
            return 0
    
    def get_stats(self) -> dict:
        """Get Redis cache statistics"""
        if not self.is_connected():
            return {'status': 'disconnected'}
        
        try:
            info = self.redis_client.info()
            return {
                'status': 'connected',
                'used_memory': info.get('used_memory_human', 'N/A'),
                'used_memory_peak': info.get('used_memory_peak_human', 'N/A'),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'hit_rate': round(
                    info.get('keyspace_hits', 0) / 
                    max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1) * 100, 2
                ),
                'evicted_keys': info.get('evicted_keys', 0),
                'expired_keys': info.get('expired_keys', 0)
            }
        except Exception as e:
            logger.error(f"❌ Failed to get cache stats - {e}")
            return {'status': 'error', 'error': str(e)}
    
    def health_check(self) -> dict:
        """Perform Redis health check"""
        try:
            start_time = time.time()
            self.redis_client.ping()
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            
            return {
                'status': 'healthy',
                'response_time_ms': round(response_time, 2),
                'connection_pool_created_connections': self.connection_pool.created_connections,
                'connection_pool_available_connections': len(self.connection_pool._available_connections)
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }

# Global Redis client instance
_redis_client = None

def get_redis_client() -> RedisClient:
    """Get the global Redis client instance"""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client

# Convenience functions for easy caching
def cache_set(prefix: str, key: str, value: Any, ttl: Optional[int] = None) -> bool:
    """Set cache value"""
    return get_redis_client().set(prefix, key, value, ttl)

def cache_get(prefix: str, key: str) -> Optional[Any]:
    """Get cache value"""
    return get_redis_client().get(prefix, key)

def cache_delete(prefix: str, key: str) -> bool:
    """Delete cache value"""
    return get_redis_client().delete(prefix, key)

def cache_clear_namespace(prefix: str) -> int:
    """Clear all cache entries in namespace"""
    return get_redis_client().clear_namespace(prefix) 