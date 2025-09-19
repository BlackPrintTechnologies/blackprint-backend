from flask_restful import Resource, reqparse
from module.area_analysis.controller import AreaAnalysisController
from utils.commonUtil import authenticate
import logging

logger = logging.getLogger(__name__)

class AreaAnalysisSummary(Resource):
    """Resource to get area analysis summary matching the UI mockup exactly."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/summary - Get complete area analysis summary for the UI."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            
            logger.info("User %s requesting area analysis summary for lat=%s, lng=%s, radius=%s", 
                       current_user, lat, lng, radius)
            
            controller = AreaAnalysisController()
            response = controller.get_area_summary(lat, lng, radius)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisSummary POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class AreaAnalysisTrafficPatterns(Resource):
    """Resource to get detailed traffic patterns (hourly and daily) for charts."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('pattern_type', type=str, required=True, location='json',
                           choices=['hourly', 'daily', 'both'],
                           help='Pattern type must be one of: hourly, daily, both')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/traffic-patterns - Get detailed traffic patterns for charts."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            pattern_type = args.get('pattern_type')
            
            logger.info("User %s requesting traffic patterns (%s) for lat=%s, lng=%s, radius=%s", 
                       current_user, pattern_type, lat, lng, radius)
            
            controller = AreaAnalysisController()
            response = controller.get_traffic_patterns(lat, lng, radius, pattern_type)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisTrafficPatterns POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

