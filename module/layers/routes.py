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
    logger.info(f"Starting fetch_properties_layer_data_raw for city: {city}")
    controller = PropertyLayerController()
    connection = None
    resp = None
    try:
        logger.info(f"Connecting to database for {city}...")
        connection = controller.db.connect()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        logger.info(f"Generating property query for {city}...")
        query = controller.get_property_query(city=city)
        logger.info(f"Generated query for {city}, length: {len(query)}")
        logger.info(f"Query preview for {city} (first 300 chars): {query[:300]}...")
        
        logger.info(f"Executing query for {city}...")
        cursor.execute(query)
        logger.info(f"Query executed successfully for {city}")
        
        logger.info(f"Fetching results for {city}...")
        res = cursor.fetchall()
        logger.info(f"Fetched {len(res)} results for {city}")
        
        resp = {"message": "Success", "data": {"response": res}}, 200
        logger.info(f"Successfully created response for {city}")
        
    except Exception as e:
        logger.error(f"Error in fetch_properties_layer_data_raw for {city}: {str(e)}")
        logger.error(f"Error type for {city}: {type(e).__name__}")
        import traceback
        logger.error(f"Full traceback for {city}: {traceback.format_exc()}")
        
        if connection:
            logger.info(f"Rolling back transaction for {city}...")
            connection.rollback()
            logger.info(f"Transaction rolled back for city: {city}")
        resp = {"message": "Internal Server Error", "data": str(e)}, 500
        logger.error(f"Created error response for {city}: {resp}")
        
    finally:
        if cursor:
            logger.info(f"Closing cursor for {city}...")
            cursor.close()
            logger.info(f"Cursor closed for city: {city}")
        if connection:
            logger.info(f"Disconnecting from database for {city}...")
            controller.db.disconnect(connection)
        
        logger.info(f"Returning response for {city} with status: {resp[1] if isinstance(resp, tuple) else 'Unknown'}")
        return resp

# Initialize caches for both cities - only cache successful responses
logger.info("Starting cache initialization for property layer data...")

logger.info("Fetching Mexico property layer data...")
_property_layer_cache_mexico = fetch_properties_layer_data_raw('mexico')
logger.info(f"Mexico cache result type: {type(_property_layer_cache_mexico)}")
logger.info(f"Mexico cache result: {_property_layer_cache_mexico}")

_property_layer_cache_mexico_json = None
if isinstance(_property_layer_cache_mexico, tuple) and len(_property_layer_cache_mexico) == 2:
    response_data, status_code = _property_layer_cache_mexico
    logger.info(f"Mexico response status: {status_code}")
    logger.info(f"Mexico response data type: {type(response_data)}")
    if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
        _property_layer_cache_mexico_json = json.dumps(response_data)
        logger.info("Successfully cached Mexico property layer data")
    else:
        logger.warning(f"Mexico property layer data not cached - status: {status_code}, message: {response_data.get('message', 'No message')}")
else:
    logger.warning(f"Mexico property layer data not cached - unexpected response format: {_property_layer_cache_mexico}")

logger.info("Fetching Queretaro property layer data...")
_property_layer_cache_qro = fetch_properties_layer_data_raw('queretaro')
logger.info(f"Queretaro cache result type: {type(_property_layer_cache_qro)}")
logger.info(f"Queretaro cache result: {_property_layer_cache_qro}")

_property_layer_cache_qro_json = None
if isinstance(_property_layer_cache_qro, tuple) and len(_property_layer_cache_qro) == 2:
    response_data, status_code = _property_layer_cache_qro
    logger.info(f"Queretaro response status: {status_code}")
    logger.info(f"Queretaro response data type: {type(response_data)}")
    if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
        _property_layer_cache_qro_json = json.dumps(response_data)
        logger.info("Successfully cached Queretaro property layer data")
    else:
        logger.warning(f"Queretaro property layer data not cached - status: {status_code}, message: {response_data.get('message', 'No message')}")
        if isinstance(response_data, dict) and 'data' in response_data:
            logger.error(f"Queretaro error details: {response_data['data']}")
else:
    logger.warning(f"Queretaro property layer data not cached - unexpected response format: {_property_layer_cache_qro}")

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

    def post(self):
        logger.info("Received request to fetch brands.")
        brand_controller = BrandController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        category = data.get('category')
        logger.debug(f"Parsed input: fid={fid}, radius={radius} ,category={category}")

        response = brand_controller.get_brands(radius, fid, category, city=data.get('config_city'))
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