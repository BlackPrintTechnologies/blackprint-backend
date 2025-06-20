import threading
import logging

logger = logging.getLogger(__name__)

# In-memory caches for various responses
_caches = {
    'property': {},
    'user_property': {},
    'demographic': {},
    'market_info': {}
}

_locks = {
    'property': threading.Lock(),
    'user_property': threading.Lock(),
    'demographic': threading.Lock(),
    'market_info': threading.Lock()
}

def set_in_cache(cache_name, key, value):
    """
    Sets a value in the specified cache.
    """
    if cache_name not in _caches:
        logger.warning(f"Cache '{cache_name}' not found.")
        return

    with _locks[cache_name]:
        logger.info(f"Caching response for key: {key} in cache: {cache_name}")
        _caches[cache_name][key] = value

def get_from_cache(cache_name, key):
    """
    Retrieves a value from the specified cache.
    Returns None if the key is not found.
    """
    if cache_name not in _caches:
        logger.warning(f"Cache '{cache_name}' not found.")
        return None

    with _locks[cache_name]:
        if key in _caches[cache_name]:
            logger.info(f"Cache hit for key: {key} in cache: {cache_name}")
            return _caches[cache_name][key]
        else:
            logger.info(f"Cache miss for key: {key} in cache: {cache_name}")
            return None 