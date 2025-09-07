from flask_restful import Resource, reqparse
from module.pois.controller import POIsController
from utils.commonUtil import authenticate
import logging

logger = logging.getLogger(__name__)

class Brands(Resource):
    """Resource to get brands within specified catchment radius."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('radius', type=str, default='1000', required=False, location='json')
    # post_parser.add_argument('fid', type=str, required=True, help='FID is required', location='json')
    post_parser.add_argument('lat', type=str, required=False, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=str, required=False, help='Longitude is required', location='json')
    post_parser.add_argument('category', type=str, required=False, location='json')
    post_parser.add_argument('subcategories', type=str, required=False, location='json')
    post_parser.add_argument('subsubcategories', type=str, required=False, location='json')
    post_parser.add_argument('brand_names', type=str, required=False, location='json')
    post_parser.add_argument('config_city', type=str, default='mexico', required=False, location='json')
    
    # @authenticate
    def post(self):
        """POST /pois/brands - Get brands within specified catchment radius."""
        try:
            args = self.post_parser.parse_args()
            radius = args.get('radius', '1000')
            fid = args.get('fid')
            lat = args.get('lat')
            lng = args.get('lng')
            category = args.get('category')
            brand_names = args.get('brand_names')
            config_city = args.get('config_city', 'mexico')
            
            logger.info("Requesting brands for fid=%s, radius=%s, config_city=%s", 
                        fid, radius, config_city)
            
            poi_controller = POIsController()
            response = poi_controller.get_brands(radius, lat, lng, category, args.get('subcategories'), args.get('subsubcategories'), brand_names, config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in Brands POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class BrandSearch(Resource):
    """Resource to search brands by name pattern."""
    
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('brand_name', type=str, required=True, help='Brand name is required', location='args')
    get_parser.add_argument('config_city', type=str, default='mexico', required=False, location='args')
    
    @authenticate
    def get(self, current_user):
        """GET /pois/brands/search - Search brands by name pattern."""
        try:
            args = self.get_parser.parse_args()
            brand_name = args.get('brand_name')
            config_city = args.get('config_city', 'mexico')
            
            logger.info("User %s searching brands for name=%s, config_city=%s", 
                       current_user, brand_name, config_city)
            
            poi_controller = POIsController()
            response = poi_controller.search_brands(brand_name, config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in BrandSearch GET: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class POIHierarchy(Resource):
    """Resource to get POI category hierarchy."""
    
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('config_city', type=str, default='mexico', required=False, location='args')
    
    @authenticate
    def get(self, current_user):
        """GET /pois/hierarchy - Get POI category hierarchy."""
        try:
            args = self.get_parser.parse_args()
            config_city = args.get('config_city', 'mexico')
            
            logger.info("User %s requesting POI hierarchy for config_city=%s", 
                       current_user, config_city)
            
            poi_controller = POIsController()
            response = poi_controller.get_pois_hierarchy(config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in POIHierarchy GET: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class POIsByCoordinates(Resource):
    """Resource to get POIs within specified radius from coordinates."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=1000, required=False, location='json')
    post_parser.add_argument('config_city', type=str, default='mexico', required=False, location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /pois/coordinates - Get POIs within specified radius from coordinates."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 1000)
            config_city = args.get('config_city', 'mexico')
            
            logger.info("User %s requesting POIs for lat=%s, lng=%s, radius=%s, config_city=%s", 
                       current_user, lat, lng, radius, config_city)
            
            poi_controller = POIsController()
            response = poi_controller.get_pois(lat, lng, radius, config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in POIsByCoordinates POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500