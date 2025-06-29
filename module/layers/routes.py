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

def fetch_properties_layer_data_raw():
    controller = PropertyLayerController()
    connection = None
    resp = None
    try:
        connection = controller.db.connect()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        query = controller.get_property_query()
        cursor.execute(query)
        res = cursor.fetchall()
        resp = {"message": "Success", "data": {"response": res}}, 200
    except Exception as e:
        if connection:
            connection.rollback()
        resp = {"message": "Internal Server Error", "data": str(e)}, 500
    finally:
        if cursor:
            cursor.close()
        if connection:
            controller.db.disconnect(connection)
        return resp

_property_layer_cache = fetch_properties_layer_data_raw()
_property_layer_cache_json = json.dumps(_property_layer_cache[0])  # Only the dict, not the status

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

    def post(self):
        logger.info("Received request to fetch brands.")
        brand_controller = BrandController()
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        category = data.get('category')
        logger.debug(f"Parsed input: fid={fid}, radius={radius} ,category={category}")

        response = brand_controller.get_brands(radius, fid, category)
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

    
    def post(self):
        logger.info("Received request to fetch traffic data.")
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        radius = data.get('radius')
        logger.debug(f"Parsed input: fid={fid}, radius={radius}")
        traffic_controller = TrafficController()
        response = traffic_controller.get_mobility_data_within_buffer(fid,radius)
        # if response.status_code == 200:
        logger.info(f"Successfully retrieved traffic data for fid={fid}, radius={radius}")
        # else:
        #     logger.warning(f"Failed to fetch traffic data: {response.message}")
        return response

class PropertyLayer(Resource):
    create_parser = reqparse.RequestParser()

    def get(self):
        logger.info("Serving cached property layer data (pre-serialized JSON).")
        return FlaskResponse(_property_layer_cache_json, status=200, mimetype='application/json')