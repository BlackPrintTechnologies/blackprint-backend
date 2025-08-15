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
    logger.info(f"fetch_properties_layer_data_raw called for city: {city}")
    controller = PropertyLayerController()
    connection = None
    resp = None
    try:
        logger.info(f"Connecting to database for city: {city}")
        connection = controller.db.connect()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        logger.info(f"Getting property query for city: {city}")
        query = controller.get_property_query(city=city)
        logger.info(f"Query generated for {city}: {query[:500]}...")  # Log first 500 chars
        
        # Log the full query for debugging
        logger.info(f"Full query for {city}: {query}")
        
        logger.info(f"Executing query for city: {city}")
        cursor.execute(query)
        logger.info(f"Query executed successfully for city: {city}")
        
        res = cursor.fetchall()
        logger.info(f"Query returned {len(res)} rows for city: {city}")
        
        # Log sample data for debugging
        if res and len(res) > 0:
            sample_row = res[0]
            logger.info(f"Sample row keys for {city}: {list(sample_row.keys())}")
            logger.info(f"Sample row data for {city}: {dict(sample_row)}")
            
            # Check for geometry/centroid issues
            if 'geometry' in sample_row:
                logger.info(f"Geometry field type for {city}: {type(sample_row['geometry'])}, value: {sample_row['geometry']}")
            if 'centroid' in sample_row:
                logger.info(f"Centroid field type for {city}: {type(sample_row['centroid'])}, value: {sample_row['centroid']}")
        else:
            logger.warning(f"No results returned for city: {city}")
        
        resp = {"message": "Success", "data": {"response": res}}, 200
        logger.info(f"Success response created for city: {city}")
        
    except Exception as e:
        logger.error(f"Error in fetch_properties_layer_data_raw for city {city}: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        
        if connection:
            connection.rollback()
            logger.info(f"Transaction rolled back for city: {city}")
        resp = {"message": "Internal Server Error", "data": str(e)}, 500
    finally:
        if cursor:
            cursor.close()
            logger.info(f"Cursor closed for city: {city}")
        if connection:
            controller.db.disconnect(connection)
            logger.info(f"Database connection closed for city: {city}")
        logger.info(f"fetch_properties_layer_data_raw completed for city: {city}")
        return resp

# Initialize caches for both cities - only cache successful responses
_property_layer_cache_mexico = fetch_properties_layer_data_raw('mexico')
_property_layer_cache_mexico_json = None
if isinstance(_property_layer_cache_mexico, tuple) and len(_property_layer_cache_mexico) == 2:
    response_data, status_code = _property_layer_cache_mexico
    if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
        _property_layer_cache_mexico_json = json.dumps(response_data)
        logger.info("Successfully cached Mexico property layer data")
    else:
        logger.warning("Mexico property layer data not cached - not a successful response")
else:
    logger.warning("Mexico property layer data not cached - unexpected response format")

_property_layer_cache_qro = fetch_properties_layer_data_raw('queretaro')
_property_layer_cache_qro_json = None
if isinstance(_property_layer_cache_qro, tuple) and len(_property_layer_cache_qro) == 2:
    response_data, status_code = _property_layer_cache_qro
    if status_code < 400 and isinstance(response_data, dict) and response_data.get('message', '').lower() == 'success':
        _property_layer_cache_qro_json = json.dumps(response_data)
        logger.info("Successfully cached Queretaro property layer data")
    else:
        logger.warning("Queretaro property layer data not cached - not a successful response")
else:
    logger.warning("Queretaro property layer data not cached - unexpected response format")

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