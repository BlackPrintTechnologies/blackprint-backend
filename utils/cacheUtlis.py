import redis
import json
from functools import wraps
from flask import request
import hashlib

# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

class RedisCache:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = redis.Redis(
                host=config['REDIS_HOST'],
                port=config['REDIS_PORT'],
                db=config['REDIS_DB'],
                decode_responses=True
            )
        return cls._instance

    @staticmethod
    def generate_cache_key(prefix, *args, **kwargs):
        """Generate a unique cache key based on the function arguments"""
        key_parts = [prefix]
        key_parts.extend([str(arg) for arg in args])
        key_parts.extend([f"{k}:{v}" for k, v in sorted(kwargs.items())])
        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()

def cache_response(prefix, expiration=None):
    """
    Decorator to cache API responses
    :param prefix: Prefix for the cache key
    :param expiration: Cache expiration time in seconds
    """
    if expiration is None:
        expiration = config.get('CACHE_EXPIRATION', 3600)

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get cache instance
            cache = RedisCache.get_instance()
            
            # Generate cache key
            cache_key = RedisCache.generate_cache_key(
                prefix,
                request.path,
                request.args.to_dict(),
                request.get_json(silent=True)
            )

            # Try to get cached response
            cached_response = cache.get(cache_key)
            if cached_response:
                return json.loads(cached_response)

            # If not cached, execute the function
            response = f(*args, **kwargs)
            
            # Cache the response
            cache.setex(
                cache_key,
                expiration,
                json.dumps(response)
            )
            
            return response
        return decorated_function
    return decorator