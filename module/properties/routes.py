from flask_restful import Resource, reqparse
from flask import request, jsonify
from utils.responseUtils import Response
from module.properties.controller import PropertyController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate



class Property(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=True, help='fid is required', location='args')
    create_parser.add_argument('lat', type=str, required=False, help='property_id is required', location='args')
    create_parser.add_argument('lng', type=str, required=False, help='property_id is required', location='args')

    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        lat = data.get('lat')
        lng = data.get('lng')
        pc = PropertyController()
        response = pc.get_properties(current_user, fid, lat, lng)
        return response
    
class PropertyDemographic(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='args')
    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        fid = data.get('fid')

        pc = PropertyController()
        print("fid=====>", fid)
        response = pc.get_property_demographic(fid, current_user)
        return response