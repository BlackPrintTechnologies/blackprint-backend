import json
import psycopg2
from psycopg2 import pool
from threading import Lock
import time
import logging

# Load configuration from app.json
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']

# Set up logging
logging.basicConfig(level=logging.INFO)

class Database:
    _instance = None
    _lock = Lock()
    _pool = None

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Database, cls).__new__(cls)
                cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.db_config = {
            'host': config['DB_HOST'],
            'database': config['DB_NAME'],
            'user': config['DB_USERNAME'],
            'password': config['DB_PASSWORD'],
            'port': config['DB_PORT'],
            'connect_timeout': 10  # Timeout after 10 seconds if connection isn't established
        }
        self._pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, **self.db_config
        )

    def log_pool_status(self):
        with self._lock:
            in_use = len(self._pool._used)
            available = self._pool.maxconn - in_use
            logging.info(f"Pool status: In-use: {in_use}, Available: {available}")

    def connect(self, db_name=None):
        if db_name:
            self.db_config['database'] = db_name
        st = time.time()
        
        retries = 3
        while retries > 0:
            try:
                connection = self._pool.getconn()
                logging.info(f"Time taken to connect to db: {time.time() - st} seconds")
                return connection
            except psycopg2.OperationalError as e:
                logging.error(f"Connection attempt failed: {e}")
                retries -= 1
                if retries == 0:
                    raise
                logging.info("Retrying in 5 seconds...")
                time.sleep(5)

    def disconnect(self, connection):
        if connection:
            self._pool.putconn(connection)


class RedshiftDatabase:
    _instance = None
    _lock = Lock()
    _pool = None

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(RedshiftDatabase, cls).__new__(cls)
                cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.db_config = {
            'host': config['AWS_HOST'],
            'database': config['AWS_REDSHIFT_DATABASE'],
            'user': config['AWS_USERNAME'],
            'password': config['AWS_PASSWORD'],
            'port': config['AWS_PORT'],
            'connect_timeout': 10  # Timeout after 10 seconds if connection isn't established
        }
        self._pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, **self.db_config
        )

    def log_pool_status(self):
        with self._lock:
            in_use = len(self._pool._used)
            available = self._pool.maxconn - in_use
            logging.info(f"Redshift Pool status: In-use: {in_use}, Available: {available}")

    def connect(self, db_name=None):
        if db_name:
            self.db_config['database'] = db_name
        st = time.time()

        retries = 3
        while retries > 0:
            try:
                connection = self._pool.getconn()
                logging.info(f"Time taken to connect to Redshift db: {time.time() - st} seconds")
                return connection
            except psycopg2.OperationalError as e:
                logging.error(f"Connection attempt failed: {e}")
                retries -= 1
                if retries == 0:
                    raise
                logging.info("Retrying in 5 seconds...")
                time.sleep(5)

    def disconnect(self, connection):
        if connection:
            self._pool.putconn(connection)
