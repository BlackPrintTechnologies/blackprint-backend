import json
import psycopg2
from threading import Lock

# Load configuration from app.json
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']

class Database:
    _instance = None
    _lock = Lock()

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
            'port': config['DB_PORT']
        }
        self.connection = None

    def connect(self, db_name=None):
        if self.connection is None:
            if db_name:
                self.db_config['database'] = db_name
            self.connection = psycopg2.connect(**self.db_config)
        return self.connection

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

class Database:
    _instance = None
    _lock = Lock()

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
            'port': config['DB_PORT']
        }
        self.connection = None

    def connect(self, db_name=None):
        if self.connection is None:
            if db_name:
                self.db_config['database'] = db_name
            self.connection = psycopg2.connect(**self.db_config)
        return self.connection

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

class RedshiftDatabase:
    _instance = None
    _lock = Lock()

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
            'port': config['AWS_PORT']
        }
        self.connection = None

    def connect(self, db_name=None):
        if self.connection is None:
            if db_name:
                self.db_config['database'] = db_name
            self.connection = psycopg2.connect(**self.db_config)
        return self.connection

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None