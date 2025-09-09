from flask_restful import Resource, reqparse
from flask import request, jsonify, Response as FlaskResponse
from utils.responseUtils import Response
from module.layers.controller import BrandController, TrafficController, PropertyLayerController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate
from logsmanager.logging_config import setup_logging
import logging
import time
from psycopg2.extras import RealDictCursor
import json


# Initialize logging
setup_logging()

# Retrieve the logger
logger = logging.getLogger(__name__)

def fetch_properties_layer_data_raw(city='mexico'):
    controller = PropertyLayerController()
    connection = None
    resp = None
    try:
        connection = controller.db.connect()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = controller.get_property_query(city=city)
        cursor.execute(query)
        res = cursor.fetchall()
        resp = {"message": "Success", "data": {"response": res}}, 200
        
    except Exception as e:
        logger.error(f"Error in fetch_properties_layer_data_raw for {city}: {str(e)}")
        if connection:
            connection.rollback()
        resp = {"message": "Internal Server Error", "data": str(e)}, 500
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            controller.db.disconnect(connection)
        return resp

# Initialize caches for both cities - only cache successful responses
logger.info("Initializing property layer caches...")

_property_layer_cache_mexico = fetch_properties_layer_data_raw('mexico')

_property_layer_cache_mexico_json = None
if isinstance(_property_layer_cache_mexico, tuple) and len(_property_layer_cache_mexico) == 2:
    response_data, status_code = _property_layer_cache_mexico
    if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
        _property_layer_cache_mexico_json = json.dumps(response_data)
        logger.info("Mexico property layer cache initialized")
    else:
        logger.warning(f"Mexico property layer data not cached - status: {status_code}")

_property_layer_cache_qro = fetch_properties_layer_data_raw('queretaro')

_property_layer_cache_qro_json = None
if isinstance(_property_layer_cache_qro, tuple) and len(_property_layer_cache_qro) == 2:
    response_data, status_code = _property_layer_cache_qro
    if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
        _property_layer_cache_qro_json = json.dumps(response_data)
        logger.info("Queretaro property layer cache initialized")
    else:
        logger.warning(f"Queretaro property layer data not cached - status: {status_code}")

logger.info("Cache initialization completed")

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
    create_parser.add_argument('category', type=str, required=False, help='Category is required')
    create_parser.add_argument('config_city', type=str, required=False, default='mexico', help='City is required')
    create_parser.add_argument('brand_names', type=str, required=False, help='Brand names is required')

    def post(self):
        logger.info("Received request to fetch brands.")
        brand_controller = BrandController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        category = data.get('category')
        brand_names = data.get('brand_names')
        logger.debug(f"Parsed input: fid={fid}, radius={radius} ,category={category}, brand_name={brand_names}")

        response = brand_controller.get_brands(radius, fid, category, brand_names, city=data.get('config_city'))
        logger.info(f"Successfully retrieved brands for fid={fid}, radius={radius}")
        
        return response
    
class SearchBrands(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('brand_name', type=str, required=True, help='Brand name is required', location='args')

    def get(self):
        logger.info("Received request to search brands.")
        data = self.create_parser.parse_args()
        brand_name = data.get('brand_name')
        logger.debug(f"Parsed input: brand_name={brand_name}")
        brand_controller = BrandController()
        response = brand_controller.search_brands(brand_name)
        return response

class Traffic(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('radius', type=str, required=False, help='User ID is required')
    create_parser.add_argument('fid', type=str, required=False, help='User ID is required')
    create_parser.add_argument('config_city', type=str, default='mexico', required=False, help='City is required')
    
    def post(self):
        logger.info("Received request to fetch traffic data.")
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        config_city = data.get('config_city', 'mexico')
        logger.debug(f"Parsed input: fid={fid}, radius={radius}")
        traffic_controller = TrafficController()
        response = traffic_controller.get_mobility_data_within_buffer(fid,radius,config_city=config_city)
        # if response.status_code == 200:
        logger.info(f"Successfully retrieved traffic data for fid={fid}, radius={radius}")
        # else:
        #     logger.warning(f"Failed to fetch traffic data: {response.message}")
        return response

class PropertyLayer(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('config_city', type=str, required=False, default='mexico', help='City configuration', location='args')

    def get(self):
        logger.info("Serving cached property layer data (pre-serialized JSON).")
        data = self.create_parser.parse_args()
        city = data.get('config_city', 'mexico')
        
        # Serve from appropriate cache based on city
        if city == 'queretaro' or city == 'el_marques':
            if _property_layer_cache_qro_json:
                return FlaskResponse(_property_layer_cache_qro_json, status=200, mimetype='application/json')
            else:
                logger.error("Queretaro/El Marques property layer cache is not available - fetching fresh data")
                controller = PropertyLayerController()
                return controller.get_property_layer(city=city)
        else:
            if _property_layer_cache_mexico_json:
                return FlaskResponse(_property_layer_cache_mexico_json, status=200, mimetype='application/json')
            else:
                logger.error("Mexico property layer cache is not available - fetching fresh data")
                controller = PropertyLayerController()
                return controller.get_property_layer(city='mexico')

class LandUseFilter(Resource):
    """
    API endpoint to get distinct land use values for filtering properties.
    Supports both Mexico City and Queretaro cities.
    """
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('config_city', type=str, required=False, default='mexico', help='City configuration (mexico, queretaro, el_marques)', location='args')

    def get(self):
        logger.info("Received request to fetch distinct land use values.")
        data = self.create_parser.parse_args()
        city = data.get('config_city', 'mexico')
        
        logger.info(f"Fetching land use values for city: {city}")
        
        # Validate city parameter
        valid_cities = ['mexico', 'queretaro', 'el_marques']
        if city not in valid_cities:
            logger.warning(f"Invalid city parameter: {city}")
            return Response.bad_request(
                message=f"Invalid city parameter: {city}",
                data={
                    "provided_city": city,
                    "valid_cities": valid_cities,
                    "note": "Use 'mexico' for Mexico City, 'queretaro' or 'el_marques' for Queretaro region"
                }
            )
        
        try:
            controller = BrandController()
            response = controller.get_distinct_land_use(city=city)
            logger.info(f"Successfully retrieved land use values for city: {city}")
            return response
            
        except Exception as e:
            logger.error(f"Error fetching land use values for city {city}: {str(e)}")
            return Response.internal_server_error(
                message=f"Failed to fetch land use values for {city}",
                data={"city": city, "error": str(e)}
            )