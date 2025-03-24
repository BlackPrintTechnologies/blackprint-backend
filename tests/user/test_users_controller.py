import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from decimal import Decimal
from module.user.controller import UsersController


@pytest.fixture
def mock_db():
    """Fixture to mock the Database class."""
    with patch('module.user.controller.Database') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance


class TestUsersController:

    def test_get_users(self, mock_db):
        """Test fetching users."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock query result to return a list of tuples
        mock_cursor.fetchall.return_value = [
            (1, "Test User", "test@example.com", datetime(2023, 1, 1, 0, 0, 0), Decimal(1), Decimal(0))
        ]

        # Call the method
        controller = UsersController()
        response, status_code = controller.get_users()

        print("Response from get_users:", response)

        # Assertions
        assert status_code == 200
        assert response["message"] == "Success"
        assert isinstance(response["data"], list)  # Ensure it's a list
        assert response["data"][0]["bp_user_id"] == 1

    def test_create_user(self, mock_db):
        """Test creating a new user."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate returning the new user ID
        mock_cursor.fetchone.return_value = (1,)

        # Call the method
        controller = UsersController()
        response, status_code = controller.create_user(
            bp_name="Test User",
            bp_company="Test Company",
            bp_industry="Test Industry",
            bp_email="test@example.com",
            bp_password="password",
            bp_status=1
        )

        print("Response from create_user:", response)

        # Assertions
        assert status_code == 201
        assert response["message"] == "Created"
        assert response["data"]["bp_user_id"] == 1

    def test_update_user(self, mock_db):
        """Test updating a user."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate successful update
        mock_cursor.rowcount = 1

        # Call the method
        controller = UsersController()
        response, status_code = controller.update_user(
            id=1,
            bp_name="Updated User"
        )

        print("Response from update_user:", response)

        # Assertions
        assert status_code == 200
        assert response["message"] == "User updated successfully"

    def test_delete_user(self, mock_db):
        """Test deleting a user."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate successful delete
        mock_cursor.rowcount = 1

        # Call the method
        controller = UsersController()
        response, status_code = controller.delete_user(id=1)

        print("Response from delete_user:", response)

        # Assertions
        assert status_code == 200
        assert response["message"] == "User deleted successfully"

    def test_verify_user(self, mock_db):
        """Test verifying a user."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate user lookup
        mock_cursor.fetchone.return_value = ("test@example.com",)

        # Mock token verification function
        with patch('module.user.controller.get_user_id_from_token', return_value="test@example.com"):
            # Call the method
            controller = UsersController()
            response, status_code = controller.verify_user(bp_email="test@example.com", token="valid_token")

            print("Response from verify_user:", response)

            # Assertions
            assert status_code == 200
            assert response["message"] == "User Verified"

    def test_send_user_verification_email(self, mock_db):
        """Test sending a verification email."""
        with patch('module.user.controller.send_email', return_value="Email sent successfully"):
            # Call the method
            controller = UsersController()
            response, status_code = controller.send_user_verification_email(bp_email="test@example.com")

            print("Response from send_user_verification_email:", response)

            # Assertions
            assert status_code == 200
            assert response["message"] == "Email sent successfully"
