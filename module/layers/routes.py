from flask_restful import Resource, reqparse
from flask import request, jsonify
from utils.responseUtils import Response
from module.layers.controller import BrandController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate

# Initialize SavedSearchesController
brand_controller = BrandController()

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

class Brands(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('polygon', type=str, required=False, help='User ID is required')

    def get(self):
        data = self.create_parser.parse_args()
        polygon = data.get('polygon')
        response = brand_controller.get_brands(requested_polygon=polygon)
        return response
