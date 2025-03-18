import pytest
from migration_setup import db
from models.user import BPUser

@pytest.fixture(scope="module")
def new_user():
    """Fixture to create a new BPUser instance"""
    user = BPUser(
        bp_name="Test User",
        bp_company="Test Company",
        bp_industry="Technology",
        bp_email="testuser@example.com",
        bp_password="securepassword",
        bp_status=1,
        bp_is_onboarded=0,
        bp_user_verified=0
    )
    return user

def test_new_user(new_user):
    """Test if user instance is created correctly"""
    assert new_user.bp_name == "Test User"
    assert new_user.bp_company == "Test Company"
    assert new_user.bp_industry == "Technology"
    assert new_user.bp_email == "testuser@example.com"
    assert new_user.bp_password == "securepassword"
    assert new_user.bp_status == 1
    assert new_user.bp_is_onboarded == 0
    assert new_user.bp_user_verified == 0

@pytest.fixture(scope="module")
def setup_database(test_client):
    """Fixture to create a test database and clean up after tests"""
    db.create_all()  # Create tables
    user = BPUser(
        bp_name="DB Test User",
        bp_company="DB Test Company",
        bp_industry="Finance",
        bp_email="dbuser@example.com",
        bp_password="testpassword",
        bp_status=1,
        bp_is_onboarded=1,
        bp_user_verified=1
    )
    db.session.add(user)
    db.session.commit()

    yield db

    db.session.remove()
    db.drop_all()

def test_database_user(setup_database):
    """Test if the user is correctly inserted and retrieved from the database"""
    user = BPUser.query.filter_by(bp_email="dbuser@example.com").first()
    assert user is not None
    assert user.bp_name == "DB Test User"
    assert user.bp_company == "DB Test Company"
    assert user.bp_industry == "Finance"
    assert user.bp_status == 1
    assert user.bp_is_onboarded == 1
    assert user.bp_user_verified == 1
