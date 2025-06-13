from flask_restful import Resource, reqparse
from flask import request, jsonify,send_file
from utils.responseUtils import Response
from module.properties.controller import PropertyController, UserPropertyController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate
from utils.streetViewUtils import get_street_view_image
import threading
import hashlib
from flask import make_response
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

# In-memory cache for full /property responses
property_response_cache = {}
cache_lock = threading.Lock()
# In-memory caches for userproperty and demographic responses
userproperty_response_cache = {}
userproperty_cache_lock = threading.Lock()
demographic_response_cache = {}
demographic_cache_lock = threading.Lock()

def normalize_fid(fid):
    if fid is None:
        return None
    try:
        f = float(fid)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except Exception:
        return str(fid)

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
        # Use module-level normalize_fid
        norm_fid = normalize_fid(fid)
        norm_lat = str(lat) if lat is not None else None
        norm_lng = str(lng) if lng is not None else None
        cache_key_raw = f"user={current_user}|fid={norm_fid}|lat={norm_lat}|lng={norm_lng}"
        cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
        logger = logging.getLogger(__name__)
        with cache_lock:
            logger.info(f"/property cache lookup for key: {cache_key_raw} (hash: {cache_key})")
            if cache_key in property_response_cache:
                logger.info(f"cache hit for key: {cache_key_raw} (hash: {cache_key})")
                return property_response_cache[cache_key]
            else:
                logger.info(f"/property cache miss for key: {cache_key_raw} (hash: {cache_key})")
        pc = PropertyController()
        response = pc.get_properties(current_user, fid, lat, lng)
        # Cache the response object
        with cache_lock:
            property_response_cache[cache_key] = response

        # --- Prefetch /property?fid=... in background if this was a lat/lng query ---
        def prefetch_fid_response(fid_to_prefetch, user):
            norm_fid_prefetch = normalize_fid(fid_to_prefetch)
            prefetch_cache_key_raw = f"user={user}|fid={norm_fid_prefetch}|lat=None|lng=None"
            prefetch_cache_key = hashlib.sha256(prefetch_cache_key_raw.encode()).hexdigest()
            with cache_lock:
                if prefetch_cache_key in property_response_cache:
                    logger.info(f"Prefetch: cache already exists for key: {prefetch_cache_key_raw}")
                    return
            logger.info(f"Prefetch: fetching and caching /property?fid={norm_fid_prefetch} for user={user}")
            pc2 = PropertyController()
            prefetch_response = pc2.get_properties(user, fid=norm_fid_prefetch, lat=None, lng=None)
            with cache_lock:
                property_response_cache[prefetch_cache_key] = prefetch_response
                logger.info(f"Prefetch: saved response for key: {prefetch_cache_key_raw}")

        # New: Prefetch userproperty in background
        def prefetch_userproperty_response(fid_to_prefetch, user):
            norm_fid = normalize_fid(fid_to_prefetch)
            cache_key = f"user={user}|fid={norm_fid}"
            with userproperty_cache_lock:
                if cache_key in userproperty_response_cache:
                    logger.info(f"UserProperty prefetch: cache already exists for key: {cache_key}")
                    return
            logger.info(f"UserProperty prefetch: fetching and caching for key: {cache_key}")
            upc = UserPropertyController()
            response = upc.get_user_properties(user, norm_fid, None)
            with userproperty_cache_lock:
                userproperty_response_cache[cache_key] = response

        # New: Prefetch demographic in background
        def prefetch_demographic_response(fid_to_prefetch, user):
            norm_fid = normalize_fid(fid_to_prefetch)
            cache_key = f"user={user}|fid={norm_fid}"
            with demographic_cache_lock:
                if cache_key in demographic_response_cache:
                    logger.info(f"Demographic prefetch: cache already exists for key: {cache_key}")
                    return
            logger.info(f"Demographic prefetch: fetching and caching for key: {cache_key}")
            pc = PropertyController()
            response = pc.get_property_demographic(norm_fid, user)
            with demographic_cache_lock:
                demographic_response_cache[cache_key] = response

        # If this was a lat/lng query and response contains a property with fid, prefetch /property?fid=...
        if not fid and lat and lng:
            logger.info(f"Prefetch check: response type is {type(response)}")
            try:
                # Try to extract JSON from the response
                data_json = None
                response_for_prefetch = response
                if isinstance(response, tuple):
                    logger.info(f"Prefetch check: response is a tuple, unpacking first element for prefetch logic: {response[0]}")
                    response_for_prefetch = response[0]
                if hasattr(response_for_prefetch, 'json'):
                    logger.info(f"Prefetch check: response has .json attribute: {response_for_prefetch.json}")
                    data_json = response_for_prefetch.json if callable(response_for_prefetch.json) else response_for_prefetch.json
                elif hasattr(response_for_prefetch, 'get_json'):
                    logger.info("Prefetch check: response has .get_json method, calling it...")
                    data_json = response_for_prefetch.get_json(force=True)
                elif isinstance(response_for_prefetch, dict):
                    logger.info(f"Prefetch check: response is a dict: {response_for_prefetch}")
                    data_json = response_for_prefetch
                else:
                    logger.info(f"Prefetch check: response has no .json or .get_json, value: {response_for_prefetch}")
                logger.info(f"Prefetch check: extracted data_json: {data_json}")
                # Assume response is a dict with 'data' key containing a list of properties
                if data_json and 'data' in data_json and data_json['data']:
                    first_property = data_json['data'][0]
                    fid_to_prefetch = None
                    if isinstance(first_property, dict):
                        # Try to extract fid from nested property_details
                        if 'property_details' in first_property and isinstance(first_property['property_details'], dict):
                            fid_to_prefetch = first_property['property_details'].get('fid')
                        else:
                            fid_to_prefetch = first_property.get('fid')
                    logger.info(f"Prefetch check: found fid_to_prefetch={fid_to_prefetch}")
                    if fid_to_prefetch:
                        threading.Thread(target=prefetch_fid_response, args=(fid_to_prefetch, current_user)).start()
                        threading.Thread(target=prefetch_userproperty_response, args=(fid_to_prefetch, current_user)).start()
                        threading.Thread(target=prefetch_demographic_response, args=(fid_to_prefetch, current_user)).start()
                else:
                    logger.info("Prefetch check: data_json did not contain 'data' or was empty")
            except Exception as e:
                logger.error(f"Prefetch error: {e}")
        return response
    
class PropertyDemographic(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required', location='args')
    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        norm_fid = normalize_fid(fid)
        cache_key = f"user={current_user}|fid={norm_fid}"
        with demographic_cache_lock:
            logger.info(f"[DEBUG] demographic_response_cache keys: {list(demographic_response_cache.keys())}")
            logger.info(f"[DEBUG] demographic_response_cache value for key {cache_key}: {demographic_response_cache.get(cache_key, None)}")
            if cache_key in demographic_response_cache:
                logger.info(f"/property/demographic cache hit for key: {cache_key}")
                return demographic_response_cache[cache_key]
        pc = PropertyController()
        response = pc.get_property_demographic(norm_fid, current_user)
        with demographic_cache_lock:
            demographic_response_cache[cache_key] = response
        return response
    

    
class UserProperty(Resource):
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('prop_status', type=str, required=False,location='args')
    get_parser.add_argument('fid', type=int, required=False,  location='args')
    update_parser = reqparse.RequestParser()
    update_parser.add_argument('fid', type=str, required=False, help='fid is required')
    update_parser.add_argument('prop_status', type=str, required=False, help='status is required')

    @authenticate
    def get(self, current_user):
        data = self.get_parser.parse_args()
        fid = data.get('fid')
        prop_status = data.get('prop_status')
        norm_fid = normalize_fid(fid)
        cache_key = f"user={current_user}|fid={norm_fid}"
        with userproperty_cache_lock:
            if cache_key in userproperty_response_cache:
                logger.info(f"/property/userproperty cache hit for key: {cache_key}")
                return userproperty_response_cache[cache_key]
        upc = UserPropertyController()
        response = upc.get_user_properties(current_user, norm_fid,  prop_status)
        with userproperty_cache_lock:
            userproperty_response_cache[cache_key] = response
        return response

    @authenticate
    def put(self, current_user):
        data = self.update_parser.parse_args()
        fid = data.get('fid')
        prop_status = data.get('prop_status')
        norm_fid = normalize_fid(fid)
        upc = UserPropertyController()
        response = upc.update_property_status(current_user, norm_fid, prop_status)
        return response

#route for get requested property
class RequestedProperties(Resource):
    @authenticate
    def get(self, current_user):
        upc = UserPropertyController()
        response = upc.get_requested_properties(current_user)
        return response
    

class UpdateRequestInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=False, help='fid is required')
    create_parser.add_argument('request_status', type=int, required=False, help='status is required')

    @authenticate
    def post(self, current_user):
        upc = UserPropertyController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        request_status = data.get('request_status')
        response = upc.update_property_request_status(fid, current_user, request_status)
        return response

class PropertyTraffic(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('fid', type=int, required=True, help='fid is required')

    def post(self):
        args = self.parser.parse_args()
        fid = args['fid']
        pc = PropertyController()
        return pc.get_property_traffic(fid)
    

class PropertyMarketInfo(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('spot2_id', type=str, required=False, help='fid is required', location='args')
    create_parser.add_argument('inmuebles24_id', type=str, required=False, help='fid is required', location='args')
    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        pc = PropertyController()
        spot2_id = data.get('spot2_id')
        inmuebles24_id = data.get('inmuebles24_id')
        response = pc.get_property_market_info(spot2_id, inmuebles24_id)
        return response