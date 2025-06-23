from flask_restful import Resource, reqparse
from flask import request, jsonify,send_file
from utils.responseUtils import Response
from module.properties.controller import PropertyController, UserPropertyController
from utils.commonUtil import authenticate
from utils.streetViewUtils import get_street_view_image
import threading
import hashlib
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
        # Use module-level normalize_fid
        norm_fid = normalize_fid(fid)
        norm_lat = str(lat) if lat is not None else None
        norm_lng = str(lng) if lng is not None else None
        cache_key_raw = f"user={current_user}|fid={norm_fid}|lat={norm_lat}|lng={norm_lng}"
        cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
        
        cached_response = get_from_cache('property', cache_key)
        if cached_response:
            return cached_response

        pc = PropertyController()
        response = pc.get_properties(current_user, fid, lat, lng)
        set_in_cache('property', cache_key, response)

        # --- Prefetching logic ---
        logger.info(f"Prefetch check: Starting prefetch logic check. fid={fid}, lat={lat}, lng={lng}")
        try:
            # Try to extract JSON from the response
            data_json = None
            response_for_prefetch = response
            if isinstance(response, tuple):
                response_for_prefetch = response[0]
            if hasattr(response_for_prefetch, 'json'):
                data_json = response_for_prefetch.json if callable(response_for_prefetch.json) else response_for_prefetch.json
            elif hasattr(response_for_prefetch, 'get_json'):
                data_json = response_for_prefetch.get_json(force=True)
            elif isinstance(response_for_prefetch, dict):
                data_json = response_for_prefetch
            
            if data_json and 'data' in data_json and data_json['data']:
                first_property = data_json['data'][0]
                fid_from_response = None
                if isinstance(first_property, dict):
                    property_details = first_property.get('property_details', first_property)
                    
                    fid_from_response = property_details.get('fid')
                    market_info = first_property.get('market_info', {})
                    spot2_id = normalize_market_id(market_info.get('ids_market_data_spot2'))
                    inmuebles24_id = normalize_market_id(property_details.get('ids_market_data_inmuebles24'))
                    propiedades_id = normalize_market_id(property_details.get('ids_market_data_propiedades'))
                
                effective_fid = normalize_fid(fid) if fid else fid_from_response
                logger.info(f"Prefetch check: effective_fid for prefetching is {effective_fid}")
                if effective_fid:
                    if not fid and lat and lng:
                        threading.Thread(target=prefetch_fid_response, args=(effective_fid, current_user)).start()

                    threading.Thread(target=prefetch_userproperty_response, args=(effective_fid, current_user)).start()
                    threading.Thread(target=prefetch_demographic_response, args=(effective_fid, current_user)).start()
                    
                    if spot2_id or inmuebles24_id or propiedades_id:
                        logger.info(f"Prefetch check: found market info ids: spot2_id={spot2_id}, inmuebles24_id={inmuebles24_id}, propiedades_id={propiedades_id}")
                        threading.Thread(target=prefetch_marketinfo_response, args=(spot2_id, inmuebles24_id, propiedades_id, current_user)).start()
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
        
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            return cached_response

        pc = PropertyController()
        response = pc.get_property_demographic(norm_fid, current_user)
        set_in_cache('demographic', cache_key, response)
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
        cache_key = f"user={current_user}|fid={norm_fid}|prop_status={prop_status}"
        
        cached_response = get_from_cache('user_property', cache_key)
        if cached_response:
            return cached_response

        upc = UserPropertyController()
        response = upc.get_user_properties(current_user, norm_fid,  prop_status)
        set_in_cache('user_property', cache_key, response)
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
    create_parser.add_argument('spot2_id', type=str, required=False, help='spot2_id is required', location='args')
    create_parser.add_argument('inmuebles24_id', type=str, required=False, help='inmuebles24_id is required', location='args')
    create_parser.add_argument('propiedades_id', type=str, required=False, help='propiedades_id is required', location='args')

    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        spot2_id = normalize_market_id(data.get('spot2_id'))
        inmuebles24_id = normalize_market_id(data.get('inmuebles24_id'))
        propiedades_id = normalize_market_id(data.get("propiedades_id"))
        
        cache_key_raw = f"user={current_user}|spot2_id={spot2_id}|inmuebles24_id={inmuebles24_id}|propiedades_id={propiedades_id}"
        cache_key = hashlib.sha256(cache_key_raw.encode()).hexdigest()
        
        cached_response = get_from_cache('market_info', cache_key)
        if cached_response:
            return cached_response

        pc = PropertyController()
        response = pc.get_property_market_info(spot2_id, inmuebles24_id, propiedades_id)

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