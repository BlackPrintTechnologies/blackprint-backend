import json
import os
from functools import wraps
from flask import request, jsonify
import hashlib
import psycopg2
from psycopg2 import sql, pool
from logsmanager.logging_config import setup_logging
import logging

# Initialize logging
setup_logging()

# Retrieve the logger
logger = logging.getLogger(__name__)

# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# PostgreSQL Connection Pool
class PostgresCache:
    _pool = None

    @classmethod
    def get_connection(cls):
        """Get a database connection from the pool."""
        if cls._pool is None:
            logging.info("Initializing PostgreSQL connection pool...")
            try:
                cls._pool = pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=config['DB_HOST'],
                    port=config['DB_PORT'],
                    dbname=config['STAGING_DB_NAME'],  # Use staging DB if needed: config['STAGING_DB_NAME']
                    user=config['DB_USERNAME'],
                    password=config['DB_PASSWORD']
                )
                cls._initialize_cache_table()
                logging.info("PostgreSQL connection pool created successfully.")
            except Exception as e:
                logging.error(f"Error initializing PostgreSQL pool: {e}")
                raise e
        logging.info("Getting database connection from pool.")        
        return cls._pool.getconn()

    @classmethod
    def release_connection(cls, conn):
        """Release a database connection back to the pool."""
        if cls._pool:
            logging.info("Releasing database connection back to the pool.")
            cls._pool.putconn(conn)

    @classmethod
    def _initialize_cache_table(cls):
        """Initialize the cache table if it doesn't exist."""
        conn = cls.get_connection()
        try:
            with conn.cursor() as cursor:
                logging.info("Checking if cache table exists in the database...")

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        expiration TIMESTAMP
                    )
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_cache_expiration ON cache (expiration)
                """)
                conn.commit()
                logging.info("Cache table verified/created successfully.")
        except Exception as e:
            logging.error(f"Error initializing cache table: {e}")
        finally:
            cls.release_connection(conn)

    @staticmethod
    def generate_cache_key(prefix, *args, **kwargs):
        """Generate a unique cache key based on function arguments."""
        key_parts = [prefix]
        key_parts.extend([str(arg) for arg in args])
        key_parts.extend([f"{k}:{v}" for k, v in sorted(kwargs.items())])
        key_string = ":".join(key_parts)
        cache_key = hashlib.md5(key_string.encode()).hexdigest()
        logging.info(f"Generated cache key: {cache_key}")
        return cache_key

def cache_response(prefix, expiration=None):
    """
    Decorator to cache API responses using PostgreSQL.
    :param prefix: Prefix for the cache key.
    :param expiration: Cache expiration time in seconds.
    """
    if expiration is None:
        expiration = config.get('CACHE_EXPIRATION', 3600)  # Default to 1 hour

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logging.info(f"Checking cache for function '{f.__name__}' with prefix '{prefix}'...")

            conn = PostgresCache.get_connection()
            try:
                # Generate cache key
                cache_key = PostgresCache.generate_cache_key(
                    prefix,
                    request.path,
                    request.args.to_dict(),
                    request.get_json(silent=True) or {}  # Ensure it's not None
                )

                with conn.cursor() as cursor:
                    # Try to get cached response
                    cursor.execute("""
                        SELECT value FROM cache
                        WHERE key = %s AND expiration > NOW()
                    """, (cache_key,))
                    result = cursor.fetchone()

                    if result:
                        logging.info(f"Cache hit for key: {cache_key}. Returning cached response.")
                        cached_data = json.loads(result[0])
                        # Return only the response data, not the status code
                        return cached_data[0] if isinstance(cached_data, list) else cached_data

                    logging.info(f"Cache miss for key: {cache_key}. Executing function '{f.__name__}'...")
                    # If not cached, execute the function
                    response = f(*args, **kwargs)
                    
                    # If response is a tuple (response, status_code), only cache the response
                    cache_data = response[0] if isinstance(response, tuple) else response

                    # Cache the response
                    cursor.execute("""
                        INSERT INTO cache (key, value, expiration)
                        VALUES (%s, %s, NOW() + INTERVAL '%s seconds')
                        ON CONFLICT (key) DO UPDATE SET
                            value = EXCLUDED.value,
                            expiration = EXCLUDED.expiration
                    """, (cache_key, json.dumps(cache_data), expiration))
                    conn.commit()
                    logging.info(f"Response cached for key: {cache_key}, expires in {expiration} seconds.")

                return response
            except Exception as e:
                logging.error(f"Error handling cache: {e}")
                return jsonify({"error": "Internal Server Error"}), 500
            finally:
                PostgresCache.release_connection(conn)

        return decorated_function
    return decorator
