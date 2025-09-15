from flask_restful import Resource, reqparse
from flask import request, jsonify,send_file
from utils.responseUtils import Response
from module.properties.controller import PropertyController, UserPropertyController
from utils.commonUtil import authenticate
from utils.streetViewUtils import get_street_view_image
import hashlib
import json
from utils.app_cache import get_from_cache, set_in_cache
from module.properties.prefetch import (
    prefetch_fid_response, 
    prefetch_userproperty_response,
    prefetch_demographic_response,
    prefetch_marketinfo_response
)
from utils.normalization_utils import normalize_fid, normalize_market_id
import logging
logger = logging.getLogger(__name__)

def create_commercial_growth_structure():
    """
    Creates the commercial growth data structure dynamically.
    This eliminates the need for repetitive hardcoded dictionaries.
    """
    # Define the years and their corresponding growth years
    years_data = {
        "2010": None,  # No growth calculation for 2010
        "2015": "2015",
        "2017": "2017", 
        "2020": "2020",
        "2023": "2023"
    }
    
    # Define the geographic levels
    geo_levels = ["block", "alcaldia", "colonia"]
    
    # Define business categories
    categories = [
        "EAT_AND_DRINK",
        "HEALTH_AND_MEDICAL", 
        "BEAUTY_AND_SPA",
        "FINANCIAL_SERVICE",
        "ARTS_AND_ENTERTAINMENT",
        "ACTIVE_LIFE",
        "RETAIL",
        "PETS",
        "ATTRACTIONS_AND_ACTIVITIES",
        "EDUCATION",
        "OTHERS"
    ]
    
    # Create the base structure for each geographic level
    def create_geo_structure():
        structure = {}
        for year, growth_year in years_data.items():
            structure[f"total_businesses_{year}"] = None
            if growth_year:
                structure[f"economic_growth_{growth_year}"] = None
        return structure
    
    # Create the main data structure
    data = {
        "commercial_growth": {
            geo_level: create_geo_structure()
            for geo_level in geo_levels
        },
        "categories": {
            category: {
                geo_level: create_geo_structure()
                for geo_level in geo_levels
            }
            for category in categories
        }
    }
    
    return data

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
            'city', 'id_municipality',
            # New filters for QRO market data
            'operation_type', 'dimension_min', 'dimension_max'
        ]
        filters['show_all_keys'] = parser_data.get('show_all_keys', True)
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
            fid = filters.get('fid')
            lat = filters.get('lat')
            lng = filters.get('lng')
            norm_fid = normalize_fid(fid)
            norm_lat = str(lat) if lat is not None else None
            norm_lng = str(lng) if lng is not None else None
            # Create primary cache key
            cache_key_raw = f"user={current_user}|fid={norm_fid}|lat={norm_lat}|lng={norm_lng}|city={city}"
            cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
            logger.info(f"[CACHE KEY] Primary cache key: {cache_key_raw} -> {cache_key}")
            
            # For FID-only requests, check FID cache first for potential lat/lng cached response
            if fid and not lat and not lng:
                fid_cache_key_raw = f"user={current_user}|fid={norm_fid}|city={city}"
                fid_cache_key = hashlib.sha256(fid_cache_key_raw.encode()).hexdigest()
                logger.info(f"[CACHE KEY] FID cache key: {fid_cache_key_raw} -> {fid_cache_key}")
                cached_response = get_from_cache('property', fid_cache_key)
                if cached_response:
                    logger.info(f"[CACHE HIT] Found cached response for FID {norm_fid}")
                    return cached_response
            
            # Check primary cache key
            cached_response = get_from_cache('property', cache_key)
            if cached_response:
                logger.info(f"[CACHE HIT] Found cached response for primary key")
                return cached_response
                
            pc = PropertyController()
            response = pc.get_properties(current_user, fid, lat, lng, city=city)
            
            # Only cache successful responses
            if isinstance(response, dict) and response.get('message', '').lower() == 'success':
                set_in_cache('property', cache_key, response)
                # For lat/lng requests, also cache with FID key for future FID requests
                if lat and lng and not fid:
                    try:
                        response_fid = None
                        if 'data' in response and isinstance(response['data'], list) and len(response['data']) > 0:
                            # Check if FID is directly in the first item
                            first_item = response['data'][0]
                            if 'fid' in first_item:
                                response_fid = first_item.get('fid')
                            elif 'property_details' in first_item and 'fid' in first_item['property_details']:
                                response_fid = first_item['property_details'].get('fid')
                        elif 'data' in response and isinstance(response['data'], dict):
                            # Check if FID is directly in data or in property_details
                            if 'fid' in response['data']:
                                response_fid = response['data'].get('fid')
                            elif 'property_details' in response['data'] and 'fid' in response['data']['property_details']:
                                response_fid = response['data']['property_details'].get('fid')
                        if response_fid:
                            norm_response_fid = normalize_fid(response_fid)
                            fid_cache_key_raw = f"user={current_user}|fid={norm_response_fid}|city={city}"
                            fid_cache_key = hashlib.sha256(fid_cache_key_raw.encode()).hexdigest()
                            logger.info(f"[DUAL CACHE] Creating FID cache key: {fid_cache_key_raw} -> {fid_cache_key}")
                            set_in_cache('property', fid_cache_key, response)
                            logger.info(f"[DUAL CACHE] Cached lat/lng response also with FID {norm_response_fid}")
                    except Exception as e:
                        logger.error(f"[DUAL CACHE ERROR] Failed to extract FID: {e}")
                # For FID-only requests, also cache with FID key for future FID requests
                elif fid and not lat and not lng:
                    try:
                        fid_cache_key_raw = f"user={current_user}|fid={norm_fid}|city={city}"
                        fid_cache_key = hashlib.sha256(fid_cache_key_raw.encode()).hexdigest()
                        logger.info(f"[FID CACHE] Creating FID cache key: {fid_cache_key_raw} -> {fid_cache_key}")
                        set_in_cache('property', fid_cache_key, response)
                        logger.info(f"[FID CACHE] Cached FID response for {norm_fid}")
                    except Exception as e:
                        logger.error(f"[FID CACHE ERROR] Failed to cache FID response: {e}")
            elif isinstance(response, tuple) and len(response) == 2:
                response_data, status_code = response
                if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
                    set_in_cache('property', cache_key, response)
                    # For lat/lng requests, also cache with FID key for future FID requests
                    if lat and lng and not fid:
                        try:
                            response_fid = None
                            if 'data' in response_data and isinstance(response_data['data'], list) and len(response_data['data']) > 0:
                                # Check if FID is directly in the first item
                                first_item = response_data['data'][0]
                                if 'fid' in first_item:
                                    response_fid = first_item.get('fid')
                                elif 'property_details' in first_item and 'fid' in first_item['property_details']:
                                    response_fid = first_item['property_details'].get('fid')
                            elif 'data' in response_data and isinstance(response_data['data'], dict):
                                # Check if FID is directly in data or in property_details
                                if 'fid' in response_data['data']:
                                    response_fid = response_data['data'].get('fid')
                                elif 'property_details' in response_data['data'] and 'fid' in response_data['data']['property_details']:
                                    response_fid = response_data['data']['property_details'].get('fid')
                            if response_fid:
                                norm_response_fid = normalize_fid(response_fid)
                                fid_cache_key_raw = f"user={current_user}|fid={norm_response_fid}|city={city}"
                                fid_cache_key = hashlib.sha256(fid_cache_key_raw.encode()).hexdigest()
                                logger.info(f"[DUAL CACHE] Creating FID cache key: {fid_cache_key_raw} -> {fid_cache_key}")
                                set_in_cache('property', fid_cache_key, response)
                                logger.info(f"[DUAL CACHE] Cached lat/lng response also with FID {norm_response_fid}")
                        except Exception as e:
                            logger.error(f"[DUAL CACHE ERROR] Failed to extract FID: {e}")
                    # For FID-only requests, also cache with FID key for future FID requests
                    elif fid and not lat and not lng:
                        try:
                            fid_cache_key_raw = f"user={current_user}|fid={norm_fid}|city={city}"
                            fid_cache_key = hashlib.sha256(fid_cache_key_raw.encode()).hexdigest()
                            logger.info(f"[FID CACHE] Creating FID cache key: {fid_cache_key_raw} -> {fid_cache_key}")
                            set_in_cache('property', fid_cache_key, response)
                            logger.info(f"[FID CACHE] Cached FID response for {norm_fid}")
                        except Exception as e:
                            logger.error(f"[FID CACHE ERROR] Failed to cache FID response: {e}")
            
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
        # if cached_response:
        #     return cached_response

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
    parser.add_argument('config_city', type=str, default='mexico', required=False, help='City for property commercial growth data', location='args')
    
    def validate_mexico_city(self, city_name):
        """
        Validate if the provided city is 'mexico' (case insensitive)
        Returns True if valid, False otherwise
        """
        if not city_name:
            return False
            
        city_lower = city_name.lower().strip()
        return city_lower == 'mexico'
    
    def is_queretaro_city(self, city_name):
        """
        Check if the provided city is Queretaro or El Marques
        Returns True if it's Queretaro city, False otherwise
        """
        if not city_name:
            return False
            
        city_lower = city_name.lower().strip()
        return city_lower in ['queretaro', 'el_marques']
    

    
    @authenticate
    def get(self,current_user):
        args = self.parser.parse_args()
        fid = args['fid']
        config_city = args.get('config_city', 'mexico')
        norm_fid = normalize_fid(fid)
        
        # Check if it's Queretaro city - return null values instead of error
        if self.is_queretaro_city(config_city):
            logger.info(f"Commercial growth API called for Queretaro city: {config_city} - returning null values")
            return Response.success(
                data=create_commercial_growth_structure(),
                message="Commercial growth data not available for Queretaro city - returning null values"
            )
        
        # Validate that only 'mexico' city is allowed for actual data
        if not self.validate_mexico_city(config_city):
            logger.warning(f"Commercial growth API called with unsupported city: {config_city}")
            return Response.bad_request(
                message=f"Commercial growth API is not available for city: {config_city}",
                data={
                    "provided_city": config_city,
                    "supported_cities": ["mexico", "queretaro", "el_marques"],
                    "available_regions": ["Mexico City", "Queretaro"],
                    "note": "Commercial growth data is available for Mexico City (with data) and Queretaro (with null values)"
                }
            )
        
        # Create cache key for commercial growth
        cache_key_raw = f"user={current_user}|fid={norm_fid}|config_city={config_city}"
        cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
        logger.info(f"[COMMERCIAL GROWTH CACHE] Cache key: {cache_key_raw} -> {cache_key}")
        
        # Check cache first
        cached_response = get_from_cache('commercial_growth', cache_key)
        if cached_response:
            logger.info(f"[COMMERCIAL GROWTH CACHE] Cache hit for FID {norm_fid}")
            return cached_response
        
        # If not in cache, fetch from database
        logger.info(f"[COMMERCIAL GROWTH CACHE] Cache miss for FID {norm_fid}, fetching from database")
        pc = PropertyController()
        response = pc.get_property_commercial_growth(norm_fid)
        
        # Cache successful responses
        if isinstance(response, dict) and response.get('message', '').lower() == 'success':
            set_in_cache('commercial_growth', cache_key, response)
            logger.info(f"[COMMERCIAL GROWTH CACHE] Cached response for FID {norm_fid}")
        elif isinstance(response, tuple) and len(response) == 2:
            response_data, status_code = response
            if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
                set_in_cache('commercial_growth', cache_key, response)
                logger.info(f"[COMMERCIAL GROWTH CACHE] Cached response for FID {norm_fid}")
        
        return response

class PropertyFilter(Resource):
    def post(self):
        data = request.get_json(force=True)
        config_city = data.get('config_city', 'mexico')
        pc = PropertyController()
        response = pc.filter_properties(data, config_city)
        return response

class AdvancedMunicipalitySearch(Resource):
    # Remove parser, use request.get_json()
    parser = reqparse.RequestParser()
    parser.add_argument('search_key_type', type=str, required=True )
    parser.add_argument('search_value', type=str, required=True )
    parser.add_argument('municipality_nm', type=str, required=False)
    parser.add_argument('config_city', type=str, required=False, default='mexico', help='City configuration')

    @authenticate
    def post(self, current_user):
        data = self.parser.parse_args()
        search_key_type = data.get('search_key_type')
        search_value = data.get('search_value')
        municipality_nm = data.get('municipality_nm')
        config_city = data.get('config_city', 'mexico')
        pc = PropertyController()
        return pc.advanced_municipality_search(search_key_type, search_value, municipality_nm, config_city)

# At the end of the file, add the resource to the API (example, actual registration may vary)
# from your main app or blueprint registration, add:
# api.add_resource(AdvancedMunicipalitySearch, '/properties/municipality_search')

# Property Folder API Routes
class PropertyFolderAPI(Resource):
    """API for managing property folders"""
    
    create_folder_parser = reqparse.RequestParser()
    create_folder_parser.add_argument('name', type=str, required=True, help='Folder name is required')
    create_folder_parser.add_argument('description', type=str, required=False)
    create_folder_parser.add_argument('config_city', type=str, required=True, help='City configuration is required')
    
    update_folder_parser = reqparse.RequestParser()
    update_folder_parser.add_argument('name', type=str, required=False)
    update_folder_parser.add_argument('description', type=str, required=False)
    
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('config_city', type=str, required=True, help='City filter for folders is required', location='args')
    
    @authenticate
    def get(self, current_user):
        """Get all folders for the current user, optionally filtered by city"""
        try:
            data = self.get_parser.parse_args()
            config_city = data.get('config_city')
            
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            folders = controller.get_user_folders(current_user, config_city)
            return Response.success(data=folders, message='Folders retrieved successfully')
        except Exception as e:
            logger.error(f"Error retrieving folders: {str(e)}")
            # Check if it's a missing config_city error
            if "config_city" in str(e) or "required" in str(e).lower():
                return Response.bad_request(message='config_city parameter is required')
            return Response.internal_server_error(message='Failed to retrieve folders')
    
    @authenticate
    def post(self, current_user):
        """Create a new folder"""
        try:
            data = self.create_folder_parser.parse_args()
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            result = controller.create_folder(
                user_id=current_user,
                name=data['name'],
                description=data.get('description'),
                config_city=data['config_city']
            )
            return result
        except Exception as e:
            logger.error(f"Error creating folder: {str(e)}")
            return Response.internal_server_error(message='Failed to create folder')

class PropertyFolderDetailAPI(Resource):
    """API for managing individual folders"""
    
    update_parser = reqparse.RequestParser()
    update_parser.add_argument('name', type=str, required=False)
    update_parser.add_argument('description', type=str, required=False)
    update_parser.add_argument('config_city', type=str, required=True, help='City configuration is required', location='args')
    
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('config_city', type=str, required=True, help='City filter for folder properties is required', location='args')
    
    delete_parser = reqparse.RequestParser()
    delete_parser.add_argument('config_city', type=str, required=True, help='City configuration is required', location='args')
    
    @authenticate
    def get(self, current_user, folder_id):
        """Get folder details and properties, optionally filtered by city"""
        try:
            data = self.get_parser.parse_args()
            config_city = data.get('config_city')
            
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            folder = controller.get_folder_details(current_user, folder_id, config_city)
            if not folder:
                return Response.not_found(message='Folder not found')
            return Response.success(data=folder, message='Folder retrieved successfully')
        except Exception as e:
            logger.error(f"Error retrieving folder: {str(e)}")
            # Check if it's a missing config_city error
            if "config_city" in str(e) or "required" in str(e).lower():
                return Response.bad_request(message='config_city parameter is required')
            return Response.internal_server_error(message='Failed to retrieve folder')
    
    @authenticate
    def put(self, current_user, folder_id):
        """Update folder details"""
        try:
            data = self.update_parser.parse_args()
            config_city = data.get('config_city')
            
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            result = controller.update_folder(
                user_id=current_user,
                folder_id=folder_id,
                name=data.get('name'),
                description=data.get('description'),
                config_city=config_city
            )
            return result
        except Exception as e:
            logger.error(f"Error updating folder: {str(e)}")
            # Check if it's a missing config_city error
            if "config_city" in str(e) or "required" in str(e).lower():
                return Response.bad_request(message='config_city parameter is required')
            return Response.internal_server_error(message='Failed to update folder')
    
    @authenticate
    def delete(self, current_user, folder_id):
        """Delete folder (soft delete) or remove city-specific properties"""
        try:
            data = self.delete_parser.parse_args()
            config_city = data.get('config_city')
            
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            result = controller.delete_folder(current_user, folder_id, config_city)
            return result
        except Exception as e:
            logger.error(f"Error deleting folder: {str(e)}")
            # Check if it's a missing config_city error
            if "config_city" in str(e) or "required" in str(e).lower():
                return Response.bad_request(message='config_city parameter is required')
            return Response.internal_server_error(message='Failed to delete folder')

class PropertySaveAPI(Resource):
    """API for saving properties to folders - unified create/save functionality"""
    
    save_parser = reqparse.RequestParser()
    save_parser.add_argument('fid', type=int, required=True, help='Property ID is required')
    save_parser.add_argument('folder_id', type=int, required=False, help='Folder ID (optional for existing folder)')
    save_parser.add_argument('folder_name', type=str, required=False, help='Folder name (will create if not exists)')
    save_parser.add_argument('config_city', type=str, required=True, help='City configuration is required')
    save_parser.add_argument('lat', type=float, required=False, help='Latitude of the property')
    save_parser.add_argument('long', type=float, required=False, help='Longitude of the property')
    save_parser.add_argument('notes', type=str, required=False)
    save_parser.add_argument('description', type=str, required=False, help='Folder description (used when creating new folder)')
    
    @authenticate
    def post(self, current_user):
        """Save property to folder - create new folder or use existing"""
        try:
            data = self.save_parser.parse_args()
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            
            # Use the unified method that handles all cases
            result = controller.save_property_to_folder_unified(
                user_id=current_user,
                fid=data['fid'],
                config_city=data.get('config_city'),
                folder_name=data.get('folder_name'),
                folder_id=data.get('folder_id'),
                lat=data.get('lat'),
                long=data.get('long'),
                notes=data.get('notes'),
                description=data.get('description')
            )
            
            return result
        except Exception as e:
            logger.error(f"Error saving property: {str(e)}")
            return Response.internal_server_error(message='Failed to save property')

class PropertyRemoveAPI(Resource):
    """API for removing properties from folders"""
    
    remove_parser = reqparse.RequestParser()
    remove_parser.add_argument('folder_id', type=int, required=True, help='Folder ID is required')
    remove_parser.add_argument('fid', type=int, required=True, help='Property ID is required')
    remove_parser.add_argument('config_city', type=str, required=True, help='City configuration is required')
    
    @authenticate
    def delete(self, current_user):
        """Remove property from folder"""
        try:
            data = self.remove_parser.parse_args()
            from module.properties.controller import PropertyFolderController
            controller = PropertyFolderController()
            result = controller.remove_property_from_folder(
                user_id=current_user,
                folder_id=data['folder_id'],
                fid=data['fid'],
                config_city=data.get('config_city')
            )
            return result
        except Exception as e:
            logger.error(f"Error removing property: {str(e)}")
            return Response.internal_server_error(message='Failed to remove property')