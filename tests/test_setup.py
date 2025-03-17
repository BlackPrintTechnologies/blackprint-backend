# To test the setup is working perfectly or not
def test_conftest_fixture(test_client):
    """Test if test_client fixture is working"""
    response = test_client.get("/")  # Assuming your app has a root route
    assert response.status_code in [200, 404]  # If 404, no root route is defined, but Flask is running
