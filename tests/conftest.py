import pytest
from flask import Flask
from flask_restful import Api
from flask.testing import FlaskClient
from migration_setup import app, db
from module.group.routes import Group, GroupProperty  # Import your routes

@pytest.fixture(scope="module")
def test_client():
    """Fixture to create a test client for Flask app"""
    app.config["TESTING"] = True
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"  # Use in-memory database for testing
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # Register the routes for testing
    api = Api(app)
    api.add_resource(Group, '/group', '/group/<int:grp_id>')
    api.add_resource(GroupProperty, '/group/property', '/group/property/<int:grp_id>')

    with app.test_client() as testing_client:
        with app.app_context():
            db.create_all()  # Create tables
        yield testing_client  # Provide the client to tests

        # Cleanup after tests
        with app.app_context():
            db.drop_all()

@pytest.fixture(scope="module")
def auth_token(test_client):
    """Fixture to create a test user and obtain an authentication token"""
    # Step 1: Create a test user
    signup_data = {
        "email": "testuser@example.com",
        "password": "testpassword",
        "name": "Test User"
    }
    test_client.post('/user/signup', json=signup_data)

    # Step 2: Log in the test user and obtain the token
    login_data = {
        "email": "testuser@example.com",
        "password": "testpassword"
    }
    response = test_client.post('/user/signin', json=login_data)
    token = response.json.get('data', {}).get('token')

    if not token:
        pytest.fail("Failed to obtain authentication token")

    yield token  # Provide the token to tests