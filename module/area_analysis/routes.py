from flask_restful import Resource, reqparse
from module.area_analysis.controller import AreaAnalysisController
from utils.commonUtil import authenticate
import logging

logger = logging.getLogger(__name__)

class AreaAnalysisTrafficByDay(Resource):
    """Resource to get traffic analysis by day of the week within a specified radius."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('user_type', type=str, required=False, location='json', 
                           choices=['vehiculo', 'peaton', 'estacionario'], 
                           help='User type must be one of: vehiculo, peaton, estacionario')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/traffic/daily - Get traffic analysis by day of the week."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            user_type = args.get('user_type')
            
            logger.info("User %s requesting traffic by day for lat=%s, lng=%s, radius=%s, user_type=%s", 
                       current_user, lat, lng, radius, user_type)
            
            controller = AreaAnalysisController()
            response = controller.get_traffic_by_day(lat, lng, radius, user_type)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisTrafficByDay POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class AreaAnalysisTrafficByHour(Resource):
    """Resource to get traffic analysis by hour of the day within a specified radius."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('user_type', type=str, required=False, location='json', 
                           choices=['vehiculo', 'peaton', 'estacionario'], 
                           help='User type must be one of: vehiculo, peaton, estacionario')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/traffic/hourly - Get traffic analysis by hour of the day."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            user_type = args.get('user_type')
            
            logger.info("User %s requesting traffic by hour for lat=%s, lng=%s, radius=%s, user_type=%s", 
                       current_user, lat, lng, radius, user_type)
            
            controller = AreaAnalysisController()
            response = controller.get_traffic_by_hour(lat, lng, radius, user_type)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisTrafficByHour POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class AreaAnalysisTrafficSummary(Resource):
    """Resource to get total traffic summary within a specified radius."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('user_type', type=str, required=False, location='json', 
                           choices=['vehiculo', 'peaton', 'estacionario'], 
                           help='User type must be one of: vehiculo, peaton, estacionario')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/traffic/summary - Get total traffic summary."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            user_type = args.get('user_type')
            
            logger.info("User %s requesting traffic summary for lat=%s, lng=%s, radius=%s, user_type=%s", 
                       current_user, lat, lng, radius, user_type)
            
            controller = AreaAnalysisController()
            response = controller.get_traffic_summary(lat, lng, radius, user_type)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisTrafficSummary POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class AreaAnalysisComprehensive(Resource):
    """Resource to get comprehensive area analysis including all metrics and user types."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('user_types', type=list, required=False, location='json',
                           help='List of user types to analyze (vehiculo, peaton, estacionario)')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/comprehensive - Get comprehensive area analysis."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            user_types = args.get('user_types')
            
            # Validate user_types if provided
            if user_types:
                valid_types = ['vehiculo', 'peaton', 'estacionario']
                invalid_types = [ut for ut in user_types if ut not in valid_types]
                if invalid_types:
                    return {
                        'message': f'Invalid user types: {invalid_types}. Valid types are: {valid_types}',
                        'status_code': 400
                    }, 400
            
            logger.info("User %s requesting comprehensive analysis for lat=%s, lng=%s, radius=%s, user_types=%s", 
                       current_user, lat, lng, radius, user_types)
            
            controller = AreaAnalysisController()
            response = controller.get_comprehensive_analysis(lat, lng, radius, user_types)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisComprehensive POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

