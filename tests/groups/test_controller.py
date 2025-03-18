import pytest
from unittest.mock import patch, MagicMock
from module.group.controller import GroupsController
from utils.responseUtils import Response
from utils.dbUtils import Database

# Fixture to mock the Database class
@pytest.fixture
def mock_db():
    with patch('utils.dbUtils.Database') as mock:
        mock.return_value.connect.return_value = MagicMock()
        yield mock

# Test GroupsController
class TestGroupsController:
    def test_get_groups(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchall.return_value = [
            {"grp_id": 1, "grp_name": "Test Group", "created_at": "2023-01-01T00:00:00", "updated_at": "2023-01-01T00:00:00"}
        ]

        # Call the method
        controller = GroupsController()
        response = controller.get_groups(grp_id=1, user_id=1)

        # Assert the response
        expected_response = Response.success(data=[{"grp_id": 1, "grp_name": "Test Group", "created_at": "2023-01-01T00:00:00", "updated_at": "2023-01-01T00:00:00"}])
        assert response == expected_response

    def test_create_group(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchone.return_value = {"grp_id": 1}

        # Call the method
        controller = GroupsController()
        response = controller.create_group(user_id=1, grp_name="Test Group", property_ids=[1, 2])

        # Assert the response
        expected_response = Response.created(data={"grp_id": 1})
        assert response == expected_response

    def test_update_group(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Call the method
        controller = GroupsController()
        response = controller.update_group(grp_id=1, grp_name="Updated Group", property_ids=[1, 2])

        # Assert the response
        expected_response = Response.success(message="Group updated successfully")
        assert response == expected_response

    def test_delete_group(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Call the method
        controller = GroupsController()
        response = controller.delete_group(grp_id=1)

        # Assert the response
        expected_response = Response.success(message="Group deleted successfully")
        assert response == expected_response

    def test_add_property_to_group(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Call the method
        controller = GroupsController()
        response = controller.add_property_to_group(grp_id=1, property_id=2)

        # Assert the response
        expected_response = Response.success(message="Property added to group successfully")
        assert response == expected_response

    def test_update_property_for_group(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.fetchone.return_value = {"property_ids": [1, 2]}

        # Call the method
        controller = GroupsController()
        response = controller.update_property_for_group(grp_id=1, property_id=[1, 2])

        # Assert the response
        expected_response = Response.success(data={"property_ids": [1, 2]}, message="Property updated successfully")
        assert response == expected_response

    def test_remove_property_from_group(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = mock_db.return_value.connect.return_value
        mock_cursor = mock_conn.cursor.return_value

        # Call the method
        controller = GroupsController()
        response = controller.remove_property_from_group(grp_id=1, property_id=2)

        # Assert the response
        expected_response = Response.success(message="Property removed from group successfully")
        assert response == expected_response