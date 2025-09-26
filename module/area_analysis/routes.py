from flask_restful import Resource, reqparse
from module.area_analysis.controller import AreaAnalysisController
from module.area_analysis.active_search import ActiveSearchController
from utils.commonUtil import authenticate
import logging

logger = logging.getLogger(__name__)

# Initialize Controllers
active_search_controller = ActiveSearchController()

class AreaAnalysisSummary(Resource):
    """Resource to get area analysis summary matching the UI mockup exactly."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('config_city', type=str, default='queretaro', required=False, location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/traffic-summary - Get complete area analysis summary for the UI."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            config_city = args.get('config_city', 'queretaro')
            
            logger.info("User %s requesting area analysis summary for lat=%s, lng=%s, radius=%s, city=%s", 
                       current_user, lat, lng, radius, config_city)
            
            controller = AreaAnalysisController()
            response = controller.get_area_summary(lat, lng, radius, config_city)
            
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
    post_parser.add_argument('config_city', type=str, default='queretaro', required=False, location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/traffic-patterns - Get detailed traffic patterns for charts."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            config_city = args.get('config_city', 'queretaro')
            
            logger.info("User %s requesting traffic patterns for lat=%s, lng=%s, radius=%s, city=%s", 
                       current_user, lat, lng, radius, config_city)
            
            controller = AreaAnalysisController()
            response = controller.get_traffic_patterns(lat, lng, radius, config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisTrafficPatterns POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class AreaAnalysisDemographics(Resource):
    """Resource to get demographic and socioeconomic analysis for a specific area."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('config_city', type=str, default='mexico', required=False, location='json',
                           help='City configuration - mexico or queretaro')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/demographics - Get demographic and socioeconomic analysis."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            config_city = args.get('config_city', 'mexico')
            
            logger.info("User %s requesting demographics analysis for lat=%s, lng=%s, radius=%s, config_city=%s", 
                       current_user, lat, lng, radius, config_city)
            
            controller = AreaAnalysisController()
            response = controller.get_area_demographics(lat, lng, radius, config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisDemographics POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class AreaAnalysisSocioeconomic(Resource):
    """Resource to get socioeconomic analysis matching the UI mockup exactly."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('config_city', type=str, default='mexico', required=False, location='json',
                           help='City configuration - mexico or queretaro')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/socioeconomic - Get socioeconomic analysis for the UI."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            config_city = args.get('config_city', 'mexico')
            
            logger.info("User %s requesting socioeconomic analysis for lat=%s, lng=%s, radius=%s, config_city=%s", 
                       current_user, lat, lng, radius, config_city)
            
            controller = AreaAnalysisController()
            response = controller.get_area_socioeconomic(lat, lng, radius, config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisSocioeconomic POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500


class ActiveSearch(Resource):
    """Resource to get active search data for a user."""
    
    @authenticate
    def get(self, current_user):
        """GET /area-analysis/active - Get active search data for the current user."""
        try:
            logger.info("User %s requesting active search data", current_user)
            
            response = active_search_controller.get_active_search(current_user)
            
            return response
            
        except Exception as e:
            logger.error("Error in ActiveSearch GET: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500


class MobilityData(Resource):
    """Resource to get mobility data within specified radius from coordinates."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=1000, required=False, help='Radius in meters (default: 1000)', location='json')
    post_parser.add_argument('city', type=str, default='queretaro', required=False, help='City configuration (default: queretaro)', location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/mobility - Get mobility data within specified radius from coordinates."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 1000)
            city = args.get('city', 'queretaro')
            
            logger.info("User %s requesting mobility data for lat=%s, lng=%s, radius=%s, city=%s", 
                       current_user, lat, lng, radius, city)
            
            response = active_search_controller.get_mobility_data(lat, lng, radius, city)
            
            return response
            
        except Exception as e:
            logger.error("Error in MobilityData POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500


class POIsData(Resource):
    """Resource to get POIs data within specified radius from coordinates with comprehensive area analysis."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=1000, required=False, help='Radius in meters (default: 1000)', location='json')
    post_parser.add_argument('city', type=str, default='queretaro', required=False, help='City configuration (default: queretaro)', location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/pois - Get POIs data within specified radius from coordinates with comprehensive area analysis."""
        args = self.post_parser.parse_args()
        lat = args.get('lat')
        lng = args.get('lng')
        radius = args.get('radius', 1000)
        city = args.get('city', 'queretaro')
        
        logger.info("User %s requesting POIs data for lat=%s, lng=%s, radius=%s, city=%s", 
                    current_user, lat, lng, radius, city)
        
        response = active_search_controller.get_pois_data(lat, lng, radius, city)
        
        return response


class AreaAnalysisWeeklyTraffic(Resource):
    """Resource to get weekly traffic distribution by municipality."""
    
    post_parser = reqparse.RequestParser()
    post_parser.add_argument('lat', type=float, required=True, help='Latitude is required', location='json')
    post_parser.add_argument('lng', type=float, required=True, help='Longitude is required', location='json')
    post_parser.add_argument('radius', type=int, default=2000, required=False, location='json')
    post_parser.add_argument('config_city', type=str, default='queretaro', required=False, location='json')
    
    @authenticate
    def post(self, current_user):
        """POST /area-analysis/weekly-traffic - Get weekly traffic distribution by municipality."""
        try:
            args = self.post_parser.parse_args()
            lat = args.get('lat')
            lng = args.get('lng')
            radius = args.get('radius', 2000)
            config_city = args.get('config_city', 'queretaro')
            
            logger.info("User %s requesting weekly traffic by municipality for lat=%s, lng=%s, radius=%s, city=%s", 
                       current_user, lat, lng, radius, config_city)
            
            controller = AreaAnalysisController()
            response = controller.get_weekly_traffic_by_municipality(lat, lng, radius)
            
            return response
            
        except Exception as e:
            logger.error("Error in AreaAnalysisWeeklyTraffic POST: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

