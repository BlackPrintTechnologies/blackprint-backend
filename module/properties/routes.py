from flask_restful import Resource, reqparse
from flask import request, jsonify,send_file
from utils.responseUtils import Response
from module.properties.controller import PropertyController, UserPropertyController
from utils.commonUtil import authenticate
from utils.streetViewUtils import get_street_view_image
from utils.async_utils import async_route, run_sync_in_executor, run_controller_method, async_route_with_cache
from utils.cache_decorators import cache_async_response, cache_response
import json
import asyncio
# Removed unused prefetch imports
from utils.normalization_utils import normalize_fid, normalize_market_id
import logging
logger = logging.getLogger(__name__)

#Route for the Street View image proxy endpoint
class StreetViewImage(Resource):
    # @authenticate do we realy need to authenticate this 
    @async_route_with_cache(prefix="streetview", ttl=300)
    async def get(self):#, current_user):
        pano_id = request.args.get("pano_id")
        heading = request.args.get("heading")
        fov = request.args.get("fov", default=90)
        size = request.args.get("size", default="600x300")

        if not pano_id or not heading:
            return Response.bad_request(message="Missing parameters")

        # Run street view image fetch asynchronously
        image_data = await run_sync_in_executor(get_street_view_image, pano_id, heading, fov, size)
        if not image_data:
            return Response.internal_server_error(message="Failed to fetch image")

        return send_file(image_data, mimetype='image/jpeg')

class Property(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='json')
    create_parser.add_argument('lat', type=str, required=False, help='property_id is required', location='json')
    create_parser.add_argument('lng', type=str, required=False, help='property_id is required', location='json')
    create_parser.add_argument("show_all_keys", type=bool, required=False, default=True, help="Show all keys in response", location='json')

    @authenticate
    @async_route_with_cache(prefix="property", ttl=3600)
    async def post(self, current_user):
        # Accept all JSON data for flexible filter support
        data = request.get_json(force=True)
        parser_data = self.create_parser.parse_args()
        # Support grouped filter JSON (e.g., {"property": {...}, "zoning": {...}})
        filters = {}
        for group in ["property", "zoning", "demographics", "points_of_interest"]:
            if group in data and isinstance(data[group], dict):
                filters.update(data[group])
        # If no groups, fallback to flat structure
        if not filters:
            filters = data
        filter_keys = [
            'availability', 'property_type', 'plot_min', 'plot_max', 'construction_min', 'construction_max',
            'geometry', 'price_type', 'price_min', 'price_max',
            'city', 'id_municipality'
        ]
        filters['show_all_keys'] = parser_data.get('show_all_keys', True)
        print("Filters:", filters)
        if any(key in filters for key in filter_keys):
            pc = PropertyController()
            response = await run_controller_method(pc, 'filter_properties', filters)
            return response
        else:
            print("No filters provided, using fid, lat, lng")
            fid = filters.get('fid')
            lat = filters.get('lat')
            lng = filters.get('lng')
            norm_fid = normalize_fid(fid)
            pc = PropertyController()
            response = await run_controller_method(pc, 'get_properties', current_user, fid, lat, lng)
            return response
    
class PropertyDemographic(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='args')
    @authenticate
    @async_route_with_cache(prefix="demographic", ttl=3600)
    async def get(self, current_user):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        norm_fid = normalize_fid(fid)

        pc = PropertyController()
        response = await run_controller_method(pc, 'get_property_demographic', norm_fid, current_user)
        return response
    

    
class UserProperty(Resource):
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('prop_status', type=str, required=False,location='args')
    get_parser.add_argument('fid', type=int, required=False,  location='args')
    update_parser = reqparse.RequestParser()
    update_parser.add_argument('fid', type=str, required=False, help='fid is required')
    update_parser.add_argument('prop_status', type=str, required=False, help='status is required')

    @authenticate
    @cache_response(prefix="user_property", ttl=300)  # 5 minutes for user-specific data
    def get(self, current_user):
        data = self.get_parser.parse_args()
        fid = data.get('fid')
        prop_status = data.get('prop_status')
        norm_fid = normalize_fid(fid)

        upc = UserPropertyController()
        response = upc.get_user_properties(current_user, norm_fid, prop_status)
        return response

    @authenticate
    @async_route
    async def put(self, current_user):
        data = self.update_parser.parse_args()
        fid = data.get('fid')
        prop_status = data.get('prop_status')
        norm_fid = normalize_fid(fid)
        upc = UserPropertyController()
        response = await run_controller_method(upc, 'update_property_status', current_user, norm_fid, prop_status)
        return response

#route for get requested property
class RequestedProperties(Resource):
    @authenticate
    @cache_response(prefix="user_requests", ttl=300)  # 5 minutes for user requests
    def get(self, current_user):
        upc = UserPropertyController()
        response = upc.get_requested_properties(current_user)
        return response
    

class UpdateRequestInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required')
    create_parser.add_argument('request_status', type=int, required=False, help='status is required')

    @authenticate
    @async_route
    async def post(self, current_user):
        upc = UserPropertyController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        request_status = data.get('request_status')
        response = await run_controller_method(upc, 'update_property_request_status', fid, current_user, request_status)
        return response

class PropertyTraffic(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=int, required=True, help='fid is required')

    @cache_response(prefix="traffic", ttl=14400)  # 4 hours cache for traffic data
    def post(self):
        args = self.parser.parse_args()
        fid = args['fid']
        pc = PropertyController()
        return pc.get_property_traffic(fid)
    

class PropertyMarketInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('spot2_id', type=str, required=False, help='spot2_id is required', location='args')
    create_parser.add_argument('inmuebles24_id', type=str, required=False, help='inmuebles24_id is required', location='args')
    create_parser.add_argument('propiedades_id', type=str, required=False, help='propiedades_id is required', location='args')

    @authenticate
    @cache_response(prefix="market_info", ttl=3600)  # 1 hour cache for market info
    def get(self, current_user):
        data = self.create_parser.parse_args()
        spot2_id = normalize_market_id(data.get('spot2_id'))
        inmuebles24_id = normalize_market_id(data.get('inmuebles24_id'))
        propiedades_id = normalize_market_id(data.get("propiedades_id"))

        pc = PropertyController()
        response = pc.get_property_market_info(spot2_id, inmuebles24_id, propiedades_id)
        return response

class PropertyDetailsBundle(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=str, required=True, help='fid is required', location='args')

    @authenticate
    @async_route_with_cache(prefix="property_details", ttl=3600)
    async def get(self, current_user):
        args = self.parser.parse_args()
        fid = args['fid']
        pc = PropertyController()
        return await run_controller_method(pc, 'get_property_details_bundle', current_user, fid)

class PropertyCommercialGrowth(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=str, required=True, help='fid is required', location='args')
    
    @authenticate
    @async_route_with_cache(prefix="commercial_growth", ttl=14400)
    async def get(self,current_user):
        args = self.parser.parse_args()
        fid = args['fid']
        norm_fid = normalize_fid(fid)
        pc = PropertyController()
        response = await run_controller_method(pc, 'get_property_commercial_growth', norm_fid)
        return response

class PropertyFilter(Resource):
    @async_route_with_cache(prefix="property_filter", ttl=3600)
    async def post(self):
        data = request.get_json(force=True)
        pc = PropertyController()
        response = await run_controller_method(pc, 'filter_properties', data)
        return response

class AdvancedMunicipalitySearch(Resource):
    # Remove parser, use request.get_json()
    parser = reqparse.RequestParser()
    parser.add_argument('search_key_type', type=str, required=True )
    parser.add_argument('search_value', type=str, required=True )
    parser.add_argument('municipality_nm', type=str, required=False)

    @authenticate
    @async_route_with_cache(prefix="municipality_search", ttl=1800)
    async def post(self, current_user):
        data = self.parser.parse_args()
        search_key_type = data.get('search_key_type')
        search_value = data.get('search_value')
        municipality_nm = data.get('municipality_nm')
        pc = PropertyController()
        return await run_controller_method(pc, 'advanced_municipality_search', search_key_type, search_value, municipality_nm)

# At the end of the file, add the resource to the API (example, actual registration may vary)
# from your main app or blueprint registration, add:
# api.add_resource(AdvancedMunicipalitySearch, '/properties/municipality_search')