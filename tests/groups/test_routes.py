import pytest
from module.group.routes import Group, GroupProperty
from utils.responseUtils import Response
from unittest.mock import patch


        
def test_get_group(test_client, auth_token):
    # Make a GET request to the /group/1 endpoint with the authentication token
    headers = {'Authorization': f'Bearer {auth_token}'}
    response = test_client.get('/group/1', headers=headers)

    # Assert the response
    assert response.status_code == 200
    assert response.json == {"status": "success", "data": [{"grp_id": 1, "grp_name": "Test Group"}]}

def test_create_group(test_client, auth_token):
    # Make a POST request to the /group endpoint with the authentication token
    headers = {'Authorization': f'Bearer {auth_token}'}
    group_data = {
        "grp_name": "Test Group",
        "property_ids": [1, 2]
    }
    response = test_client.post('/group', json=group_data, headers=headers)

    # Assert the response
    assert response.status_code == 201
    assert response.json == {"status": "success", "data": {"grp_id": 1}}

def test_update_group(test_client, auth_token):
    # Make a PUT request to the /group endpoint with the authentication token
    headers = {'Authorization': f'Bearer {auth_token}'}
    update_data = {
        "grp_id": 1,
        "grp_name": "Updated Group",
        "property_ids": [1, 2]
    }
    response = test_client.put('/group', json=update_data, headers=headers)

    # Assert the response
    assert response.status_code == 200
    assert response.json == {"status": "success", "message": "Group updated successfully"}

def test_delete_group(test_client, auth_token):
    # Make a DELETE request to the /group endpoint with the authentication token
    headers = {'Authorization': f'Bearer {auth_token}'}
    delete_data = {"grp_id": 1}
    response = test_client.delete('/group', json=delete_data, headers=headers)

    # Assert the response
    assert response.status_code == 200
    assert response.json == {"status": "success", "message": "Group deleted successfully"}