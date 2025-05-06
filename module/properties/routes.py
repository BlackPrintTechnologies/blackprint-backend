from flask_restful import Resource, reqparse
from flask import request, jsonify,send_file
from utils.responseUtils import Response
from module.properties.controller import PropertyController, UserPropertyController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate
from utils.streetViewUtils import get_street_view_image

#Route for the Street View image proxy endpoint
class StreetViewImage(Resource):
    # @authenticate do we realy need to authenticate this 
    def get(self):#, current_user):
        pano_id = request.args.get("pano_id")
        heading = request.args.get("heading")
        fov = request.args.get("fov", default=90)
        size = request.args.get("size", default="600x300")

        if not pano_id or not heading:
            return Response.bad_request(message="Missing parameters")

        image_data = get_street_view_image(pano_id, heading, fov, size)
        if not image_data:
            return Response.internal_server_error(message="Failed to fetch image")

        return send_file(image_data, mimetype='image/jpeg')

class Property(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='args')
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
    
class RequestInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required')
    @authenticate
    def post(self, current_user):
        upc = UserPropertyController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        response = upc.request_info_for_property(fid, current_user)
        return response
    
#route for get requested property
class RequestedProperties(Resource):
    @authenticate
    def get(self, current_user):
        upc = UserPropertyController()
        response = upc.get_requested_properties(current_user)
        return response