import json
import psycopg2
from psycopg2 import pool
from threading import Lock
import time

# Load configuration from app.json
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']

class DatabaseBase:
    """Base class for Database setup and pooling"""
    def __init__(self, db_config, db_name="Database"):
        self.db_config = db_config
        self.pool = None
        self.db_name = db_name
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize connection pool with retries"""
        retries = 5
        for attempt in range(retries):
            try:
                self.pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    **self.db_config
                )
                print(f"✅ {self.db_name} Connected Successfully!")
                break
            except Exception as e:
                print(f"❌ {self.db_name} Connection Failed. Retry {attempt + 1}/{retries}. Error: {e}")
                time.sleep(3)  # Wait before retrying
        else:
            print(f"🚨 {self.db_name} failed to connect after {retries} attempts. Exiting...")
            exit(1)  # Terminate the application if DB is unavailable

    def connect(self):
        """Get a connection from the pool"""
        return self.pool.getconn()

    # def release_connection(self, conn):
    def disconnect(self, conn):
        """Release the connection back to the pool"""
        self.pool.putconn(conn)

    def close_all_connections(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            print(f"🔒 {self.db_name} connections closed.")


class PostgreSQLDatabase(DatabaseBase):
    """Singleton class for PostgreSQL"""
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(PostgreSQLDatabase, cls).__new__(cls)
                # Explicitly initialize the instance with the required arguments
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            super().__init__(
                {
                    'host': config['DB_HOST'],
                    'database': config['DB_NAME'],
                    'user': config['DB_USERNAME'],
                    'password': config['DB_PASSWORD'],
                    'port': config['DB_PORT']
                },
                db_name="PostgreSQL"
            )
            self._initialized = True


class RedshiftDatabase(DatabaseBase):
    """Singleton class for Redshift"""
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(RedshiftDatabase, cls).__new__(cls)
                # Explicitly initialize the instance with the required arguments
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            super().__init__(
                {
                    'host': config['AWS_HOST'],
                    'database': config['AWS_REDSHIFT_DATABASE'],
                    'user': config['AWS_USERNAME'],
                    'password': config['AWS_PASSWORD'],
                    'port': config['AWS_PORT']
                },
                db_name="Redshift"
            )
            self._initialized = True


# Create global instances for PostgreSQL and Redshift
postgres_pool = PostgreSQLDatabase()
redshift_pool = RedshiftDatabase()