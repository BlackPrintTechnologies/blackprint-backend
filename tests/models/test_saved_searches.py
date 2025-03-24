import pytest
from migration_setup import db
from models.saved_searches import BPSavedSearches
from datetime import datetime
import json

@pytest.fixture(scope="module")
def saved_search():
    """Fixture to create a sample saved search entry"""
    return BPSavedSearches(
        user_id=1,
        search_name="Test Search",
        search_query="SELECT * FROM test_table",
        search_value={"filter": "active"},
        search_response={"data": ["result1", "result2"]}
    )

def test_create_saved_search(test_client, saved_search):
    """Test creating a saved search entry in the database"""
    with test_client.application.app_context():
        db.session.add(saved_search)
        db.session.commit()

        # Fetch from DB
        search_entry = BPSavedSearches.query.first()
        assert search_entry is not None
        assert search_entry.user_id == 1
        assert search_entry.search_name == "Test Search"
        assert search_entry.search_query == "SELECT * FROM test_table"
        assert search_entry.search_value == {"filter": "active"}
        assert search_entry.search_response == {"data": ["result1", "result2"]}

def test_created_at_timestamp(test_client, saved_search):
    """Test if created_at timestamp is set correctly"""
    with test_client.application.app_context():
        db.session.add(saved_search)
        db.session.commit()

        search_entry = BPSavedSearches.query.first()
        assert isinstance(search_entry.created_at, datetime)

def test_search_status_default(test_client, saved_search):
    """Test if search_status default value is 1"""
    with test_client.application.app_context():
        db.session.add(saved_search)
        db.session.commit()

        search_entry = BPSavedSearches.query.first()
        assert search_entry.search_status == 1  # Default value should be 1

def test_update_saved_search(test_client, saved_search):
    """Test updating a saved search entry and check updated_at changes"""
    with test_client.application.app_context():
        db.session.add(saved_search)
        db.session.commit()

        search_entry = BPSavedSearches.query.first()
        old_updated_at = search_entry.updated_at

        # Update the search entry
        search_entry.search_query = "SELECT * FROM new_table"
        db.session.commit()

        updated_entry = BPSavedSearches.query.first()
        assert updated_entry.search_query == "SELECT * FROM new_table"
        assert updated_entry.updated_at > old_updated_at  # Ensure timestamp updates
