from flask_restful import Resource, reqparse
from flask import request, jsonify,send_file
from utils.responseUtils import Response
from module.properties.controller import PropertyController, UserPropertyController
from utils.commonUtil import authenticate
from utils.streetViewUtils import get_street_view_image
import hashlib
import json
from utils.app_cache import get_from_cache, set_in_cache

from utils.normalization_utils import normalize_fid, normalize_market_id
import logging
logger = logging.getLogger(__name__)

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
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='json')
    create_parser.add_argument('lat', type=str, required=False, help='property_id is required', location='json')
    create_parser.add_argument('lng', type=str, required=False, help='property_id is required', location='json')
    create_parser.add_argument('config_city', type=str, required=False, default='mexico', help='City for property data', location='json')
    create_parser.add_argument("show_all_keys", type=bool, required=False, default=True, help="Show all keys in response", location='json')

    @authenticate
    def post(self, current_user):
        # Accept all JSON data for flexible filter support
        data = request.get_json(force=True)
        parser_data = self.create_parser.parse_args()
        city = parser_data.get('config_city', 'mexico')
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
            # Add filter-based caching
            filter_key_raw = f"user={current_user}|filters={json.dumps(filters, sort_keys=True)}"
            filter_cache_key = hashlib.sha256(filter_key_raw.encode()).hexdigest()
            # cached_response = get_from_cache('property', filter_cache_key)
            # if cached_response:
            #     return cached_response
            pc = PropertyController()
            response = pc.filter_properties(filters, city=city)
            
            # Only cache successful responses (currently commented out but fixed for future use)
            # if isinstance(response, dict) and response.get('message', '').lower() == 'success':
            #     set_in_cache('property', filter_cache_key, response)
            # elif isinstance(response, tuple) and len(response) == 2:
            #     response_data, status_code = response
            #     if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
            #         set_in_cache('property', filter_cache_key, response)
            
            return response
        else:
            print("No filters provided, using fid, lat, lng")
            fid = filters.get('fid')
            lat = filters.get('lat')
            lng = filters.get('lng')
            show_all_keys = filters.get('show_all_keys', True)
            norm_fid = normalize_fid(fid)
            norm_lat = str(lat) if lat is not None else None
            norm_lng = str(lng) if lng is not None else None
            cache_key_raw = f"user={current_user}|fid={norm_fid}|lat={norm_lat}|lng={norm_lng}|city={city}"
            cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
            cached_response = get_from_cache('property', cache_key)
            if cached_response:
                return cached_response
            pc = PropertyController()
            response = pc.get_properties(current_user, fid, lat, lng, city=city, show_all_keys=show_all_keys)
            
            # Only cache successful responses
            if isinstance(response, dict) and response.get('message', '').lower() == 'success':
                set_in_cache('property', cache_key, response)
            elif isinstance(response, tuple) and len(response) == 2:
                response_data, status_code = response
                if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
                    set_in_cache('property', cache_key, response)
            
            return response
    
class PropertyDemographic(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='args')
    create_parser.add_argument('config_city', type=str, required=False, default='mexico', help='City for property data', location='args')
    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        city = data.get('config_city', 'mexico')
        norm_fid = normalize_fid(fid)
        cache_key = f"user={current_user}|fid={norm_fid}|city={city}"
        
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            return cached_response

        pc = PropertyController()
        response = pc.get_property_demographic(norm_fid, current_user, city=city)
        
        # Only cache successful responses
        if isinstance(response, dict) and response.get('message', '').lower() == 'success':
            set_in_cache('demographic', cache_key, response)
        elif isinstance(response, tuple) and len(response) == 2:
            response_data, status_code = response
            if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
                set_in_cache('demographic', cache_key, response)
        
        return response
    

    
class UserProperty(Resource):
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('prop_status', type=str, required=False,location='args')
    get_parser.add_argument('fid', type=int, required=False,  location='args')
    get_parser.add_argument('config_city', type=str, default="mexico", required=False, help='config_city is required', location='args')
    update_parser = reqparse.RequestParser()
    update_parser.add_argument('fid', type=str, required=False, help='fid is required')
    update_parser.add_argument('prop_status', type=str, required=False, help='status is required')
    update_parser.add_argument('config_city', type=str, default="mexico", required=False, help='config_city is required')

    @authenticate
    def get(self, current_user):
        data = self.get_parser.parse_args()
        fid = data.get('fid')
        prop_status = data.get('prop_status')
        config_city = data.get('config_city', 'mexico')
        norm_fid = normalize_fid(fid)
        cache_key = f"user={current_user}|fid={norm_fid}|prop_status={prop_status}|config_city={config_city}"
        
        cached_response = get_from_cache('user_property', cache_key)
        if cached_response:
            return cached_response

        upc = UserPropertyController()
        response = upc.get_user_properties(current_user, norm_fid,  prop_status, config_city=config_city)
        
        # Only cache successful responses
        if isinstance(response, dict) and response.get('message', '').lower() == 'success':
            set_in_cache('user_property', cache_key, response)
        elif isinstance(response, tuple) and len(response) == 2:
            response_data, status_code = response
            if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
                set_in_cache('user_property', cache_key, response)
        
        return response

    @authenticate
    def put(self, current_user):
        data = self.update_parser.parse_args()
        fid = data.get('fid')
        prop_status = data.get('prop_status')
        config_city = data.get('config_city', 'mexico')
        norm_fid = normalize_fid(fid)
        upc = UserPropertyController()
        response = upc.update_property_status(current_user, norm_fid, prop_status, config_city=config_city)
        return response

#route for get requested property
class RequestedProperties(Resource):
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('config_city', type=str, default="mexico", required=False, help='config_city is required', location='args')
    @authenticate
    def get(self, current_user):
        upc = UserPropertyController()
        config_city = self.get_parser.parse_args().get('config_city', 'mexico')
        response = upc.get_requested_properties(current_user, config_city=config_city)
        return response
    

class UpdateRequestInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required')
    create_parser.add_argument('request_status', type=int, required=False, help='status is required')
    create_parser.add_argument('config_city', type=str, default="mexico", required=False, help='config_city is required')

    @authenticate
    def post(self, current_user):
        upc = UserPropertyController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        request_status = data.get('request_status')
        confg_city = data.get('config_city', 'mexico')
        response = upc.update_property_request_status(fid, current_user, request_status, confg_city)
        return response

class PropertyTraffic(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=int, required=True, help='fid is required')
    parser.add_argument('config_city', type=str, default='mexico', required=False, help='City for property traffic data')

    def post(self):
        args = self.parser.parse_args()
        fid = args['fid']
        pc = PropertyController()
        return pc.get_property_traffic(fid, config_city=args.get('config_city', 'mexico'))
    

class PropertyMarketInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('spot2_id', type=str, required=False, help='spot2_id is required', location='args')
    create_parser.add_argument('inmuebles24_id', type=str, required=False, help='inmuebles24_id is required', location='args')
    create_parser.add_argument('propiedades_id', type=str, required=False, help='propiedades_id is required', location='args')
    create_parser.add_argument('config_city', type=str, required=False, default='mexico', help='City for property data', location='args')

    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        spot2_id = data.get('spot2_id')
        inmuebles24_id = data.get('inmuebles24_id')
        propiedades_id = data.get('propiedades_id')
        city = data.get('config_city', 'mexico')
        
        cache_key_raw = f"user={current_user}|spot2_id={spot2_id}|inmuebles24_id={inmuebles24_id}|propiedades_id={propiedades_id}|city={city}"
        cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
        
        cached_response = get_from_cache('market_info', cache_key)
        if cached_response:
            return cached_response

        pc = PropertyController()
        response = pc.get_property_market_info(spot2_id, inmuebles24_id, propiedades_id, city=city)

        # Only cache successful responses
        if isinstance(response, dict) and response.get('message', '').lower() == 'success':
            set_in_cache('market_info', cache_key, response)
        elif isinstance(response, tuple) and len(response) == 2:
            response_data, status_code = response
            if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
                set_in_cache('market_info', cache_key, response)
        
        return response

class PropertyDetailsBundle(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=str, required=True, help='fid is required', location='args')

    @authenticate
    def get(self, current_user):
        args = self.parser.parse_args()
        fid = args['fid']
        pc = PropertyController()
        return pc.get_property_details_bundle(current_user, fid)

class PropertyCommercialGrowth(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=str, required=True, help='fid is required', location='args')
    
    @authenticate
    def get(self,current_user):
        args = self.parser.parse_args()
        fid = args['fid']
        norm_fid = normalize_fid(fid)
        pc = PropertyController()
        response = pc.get_property_commercial_growth(norm_fid)
        return response

class PropertyFilter(Resource):
    def post(self):
        data = request.get_json(force=True)
        pc = PropertyController()
        response = pc.filter_properties(data)
        return response

class AdvancedMunicipalitySearch(Resource):
    # Remove parser, use request.get_json()
    parser = reqparse.RequestParser()
    parser.add_argument('search_key_type', type=str, required=True )
    parser.add_argument('search_value', type=str, required=True )
    parser.add_argument('municipality_nm', type=str, required=False)

    @authenticate
    def post(self, current_user):
        data = self.parser.parse_args()
        search_key_type = data.get('search_key_type')
        search_value = data.get('search_value')
        municipality_nm = data.get('municipality_nm')
        pc = PropertyController()
        return pc.advanced_municipality_search(search_key_type, search_value, municipality_nm)

# At the end of the file, add the resource to the API (example, actual registration may vary)
# from your main app or blueprint registration, add:
# api.add_resource(AdvancedMunicipalitySearch, '/properties/municipality_search')