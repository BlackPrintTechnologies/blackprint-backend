# migration_setup.py
from flask import Flask 
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import json

# Load configuration from app.json
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Initialize Flask app
app = Flask(__name__)

# Determine the environment (default to 'production')
# Configure the database URI based on the environment
db_name = config['DB_NAME']
app.config['SQLALCHEMY_DATABASE_URI'] = (
    f"postgresql://{config['DB_USERNAME']}:{config['DB_PASSWORD']}@"
    f"{config['DB_HOST']}:{config['DB_PORT']}/{db_name}"
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize SQLAlchemy and Flask-Migrate
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Import models (to ensure they are registered with SQLAlchemy)
from models.user import BPUser
from models.prop_cache import BPPropCache
from models.saved_searches import BPSavedSearches
from models.users_questionare import BPUsersQuestionare
from models.groups import Group
from models.bp_user_property import BPUserProperty