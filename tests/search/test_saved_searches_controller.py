import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from utils.responseUtils import Response
from module.search.controller import SavedSearchesController

# Fixture to mock the Database class
@pytest.fixture
def mock_db():
    with patch('module.search.controller.Database') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance

# Test SavedSearchesController
class TestSavedSearchesController:
    def test_get_saved_searches(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock fetched data with correctly formatted timestamps
        mock_cursor.fetchall.return_value = [
            {
                "id": 1,
                "user_id": 1,
                "search_name": "Test Search",
                "created_at": "2023-01-01T00:00:00",  # Ensure ISO format
                "updated_at": "2023-01-01T00:00:00"
            }
        ]

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.get_saved_searches(user_id=1)

        # Debugging: Print the response
        print("Response from get_saved_searches:", response)

        # Assert the response
        assert status_code == 200
        assert response["message"] == "Success"
        assert response["data"][0]["id"] == 1
        assert response["data"][0]["search_name"] == "Test Search"
        assert response["data"][0]["created_at"] == "2023-01-01T00:00:00"
        assert response["data"][0]["updated_at"] == "2023-01-01T00:00:00"

    def test_create_saved_search(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate ID return for the inserted row
        mock_cursor.fetchone.return_value = {"id": 1}

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.create_saved_search(
            user_id=1,
            search_name="Test Search",
            search_query="query",
            search_value="value",
            search_response="response"
        )

        # Debugging: Print the response
        print("Response from create_saved_search:", response)

        # Assert the response
        assert status_code == 201
        assert response["message"] == "Created"
        assert response["data"]["id"] == 1

    def test_update_saved_search(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.update_saved_search(
            id=1,
            search_name="Updated Search"
        )

        # Debugging: Print the response
        print("Response from update_saved_search:", response)

        # Assert the response
        assert status_code == 200
        assert response["message"] == "Saved search updated successfully"

    def test_delete_saved_search(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.delete_saved_search(id=1)

        # Debugging: Print the response
        print("Response from delete_saved_search:", response)

        # Assert the response
        assert status_code == 200
        assert response["message"] == "Saved search deleted successfully"

    def test_get_saved_searches_error(self, mock_db):
        # Mock the database to raise an exception
        mock_db.connect.side_effect = Exception("Database error")

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.get_saved_searches(user_id=1)

        # Debugging: Print the response
        print("Response from get_saved_searches (error):", response)

        # Assert the response
        assert status_code == 500
        assert response["message"] == "Internal Server Error"

    def test_create_saved_search_error(self, mock_db):
        # Mock the database to raise an exception
        mock_db.connect.side_effect = Exception("Database error")

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.create_saved_search(
            user_id=1,
            search_name="Test Search",
            search_query="query",
            search_value="value",
            search_response="response"
        )

        # Debugging: Print the response
        print("Response from create_saved_search (error):", response)

        # Assert the response
        assert status_code == 500
        assert response["message"] == "Internal Server Error"

    def test_update_saved_search_error(self, mock_db):
        # Mock the database to raise an exception
        mock_db.connect.side_effect = Exception("Database error")

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.update_saved_search(
            id=1,
            search_name="Updated Search"
        )

        # Debugging: Print the response
        print("Response from update_saved_search (error):", response)

        # Assert the response
        assert status_code == 500
        assert response["message"] == "Internal Server Error"

    def test_delete_saved_search_error(self, mock_db):
        # Mock the database to raise an exception
        mock_db.connect.side_effect = Exception("Database error")

        # Call the method
        controller = SavedSearchesController()
        response, status_code = controller.delete_saved_search(id=1)

        # Debugging: Print the response
        print("Response from delete_saved_search (error):", response)

        # Assert the response
        assert status_code == 500
        assert response["message"] == "Internal Server Error"
