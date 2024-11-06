from flask_restful import Resource, reqparse
from flask import request, jsonify
from utils.responseUtils import Response
from module.search.controller import SavedSearchesController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate

# Initialize SavedSearchesController
saved_searches_controller = SavedSearchesController()

# {
#     "search_name" : "test",
#     "search_query" : {
#         "id": 1,
#         "size": 1,
#         "carpet_area": 2
#     },
#     "search_value" : null,
#     "search_response" : null
# }

class SavedSearches(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('user_id', type=int, required=False, help='User ID is required')
    create_parser.add_argument('search_name', type=str, required=True, help='Search name is required')
    create_parser.add_argument('search_query', type=str, required=True, help='Search query is required')
    create_parser.add_argument('search_value', type=dict, required=False, help='Search value is required')
    create_parser.add_argument('search_response', type=dict, required=False, help='Search response is required')

    update_parser = reqparse.RequestParser()
    update_parser.add_argument('id', type=int, required=False)
    update_parser.add_argument('search_name', type=str, required=False)
    update_parser.add_argument('search_query', type=str, required=False)
    update_parser.add_argument('search_value', type=dict, required=False)
    update_parser.add_argument('search_response', type=dict, required=False)
    update_parser.add_argument('search_status', type=int, required=False)

    @authenticate
    def get(self, current_user):
        # data = self.update_parser.parse_args()
        # search_id = data.get('id')
        response = saved_searches_controller.get_saved_searches(id=None, user_id=current_user)
        return response

    @authenticate
    def post(self, current_user):
        data = self.create_parser.parse_args()
        user_id = current_user
        search_name = data.get('search_name')
        search_query = data.get('search_query')
        search_value = data.get('search_value')
        search_response = data.get('search_response')

        response = saved_searches_controller.create_saved_search(
            user_id=user_id,
            search_name=search_name,
            search_query=search_query,
            search_value=search_value,
            search_response=search_response
        )
        return response

    @authenticate
    def put(self, current_user):
        data = self.update_parser.parse_args()
        search_id = data.get('id')
        search_name = data.get('search_name')
        search_query = data.get('search_query')
        search_value = data.get('search_value')
        search_response = data.get('search_response')
        search_status = data.get('search_status')

        response = saved_searches_controller.update_saved_search(
            id=search_id,
            search_name=search_name,
            search_query=search_query,
            search_value=search_value,
            search_response=search_response,
            search_status=search_status
        )
        return response

    @authenticate
    def delete(self, current_user):
        data = self.update_parser.parse_args()
        search_id = data.get('id')
        response = saved_searches_controller.delete_saved_search(id=search_id)
        return response