from flask_restful import Resource, reqparse
from flask import request, jsonify
from utils.responseUtils import Response
from module.layers.controller import BrandController, TrafficController  # Assuming SavedSearchesController is in search_controller.py
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
    create_parser.add_argument('radius', type=str, required=False, help='User ID is required')
    create_parser.add_argument('fid', type=str, required=False, help='User ID is required')

    def post(self):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        response = brand_controller.get_brands(radius, fid)
        return response

class Traffic(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('radius', type=str, required=False, help='User ID is required')
    create_parser.add_argument('fid', type=str, required=False, help='User ID is required')

    
    def post(self):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        traffic_controller = TrafficController()
        response = traffic_controller.get_mobility_data_within_buffer(fid,radius)
        return response