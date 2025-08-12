from flask_restful import Resource, reqparse
from module.markets.controller import MarketsController
from utils.commonUtil import authenticate
import logging

logger = logging.getLogger(__name__)

class PropertyTypes(Resource):
    """
    Resource to get all distinct property types
    """
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('config_city', type=str, default="mexico", required=False, 
                           help='config_city is required', location='args')
    
    @authenticate
    def get(self, current_user):
        """
        GET /markets/property-types
        Get all distinct property types from market data
        """
        try:
            args = self.get_parser.parse_args()
            config_city = args.get('config_city', 'mexico')
            
            logger.info("User %s requesting property types for city: %s", 
                       current_user, config_city)
            
            markets_controller = MarketsController()
            response = markets_controller.get_all_distinct_property_types(city=config_city)
            
            return response
            
        except Exception as e:
            logger.error("Error in PropertyTypes GET: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

class MarketInfo(Resource):
    """
    Resource to get market information for a property
    Moved from PropertyMarketInfo
    """
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('spot2_id', type=int, required=False, location='args')
    get_parser.add_argument('inmuebles24_id', type=int, required=False, location='args')
    get_parser.add_argument('propiedades_id', type=int, required=False, location='args')
    get_parser.add_argument('config_city', type=str, default="mexico", required=False, 
                           help='config_city is required', location='args')
    
    @authenticate
    def get(self, current_user):
        """
        GET /markets/market-info
        Get market information for a property using market data IDs
        """
        try:
            args = self.get_parser.parse_args()
            spot2_id = args.get('spot2_id')
            inmuebles24_id = args.get('inmuebles24_id')
            propiedades_id = args.get('propiedades_id')
            config_city = args.get('config_city', 'mexico')
            
            # Validate that at least one ID is provided
            if not any([spot2_id, inmuebles24_id, propiedades_id]):
                return {
                    'message': 'At least one of spot2_id, inmuebles24_id, or propiedades_id is required',
                    'status_code': 400
                }, 400
            
            logger.info("User %s requesting market info for spot2_id=%s, inmuebles24_id=%s, propiedades_id=%s, city=%s", 
                       current_user, spot2_id, inmuebles24_id, propiedades_id, config_city)
            
            markets_controller = MarketsController()
            response = markets_controller.get_property_market_info(
                spot2_id=spot2_id,
                inmuebles24_id=inmuebles24_id,
                propiedades_id=propiedades_id,
                city=config_city
            )
            return response
            
        except Exception as e:
            logger.error("Error in MarketInfo GET: %s", str(e))
            return {'message': 'Internal server error', 'status_code': 500}, 500

# class MarketSummaryStats(Resource):
#     """
#     Resource to get market summary statistics
#     """
#     get_parser = reqparse.RequestParser()
#     get_parser.add_argument('config_city', type=str, default="mexico", required=False, 
#                            help='config_city is required', location='args')
    
#     @authenticate
#     def get(self, current_user):
#         """
#         GET /markets/summary-stats
#         Get market summary statistics for a city
#         """
#         try:
#             args = self.get_parser.parse_args()
#             config_city = args.get('config_city', 'mexico')
            
#             logger.info("User %s requesting market summary stats for city: %s", 
#                        current_user.get('id'), config_city)
            
#             markets_controller = MarketsController()
#             response = markets_controller.get_market_summary_stats(city=config_city)
            
#             return response, response.get('status_code', 200)
            
#         except Exception as e:
#             logger.error("Error in MarketSummaryStats GET: %s", str(e))
#             return {'message': 'Internal server error', 'status_code': 500}, 500

# class PropertyTypeDistribution(Resource):
#     """
#     Resource to get distribution of properties by property type
#     """
#     get_parser = reqparse.RequestParser()
#     get_parser.add_argument('config_city', type=str, default="mexico", required=False, 
#                            help='config_city is required', location='args')
    
#     @authenticate
#     def get(self, current_user):
#         """
#         GET /markets/property-type-distribution
#         Get distribution of properties by property type
#         """
#         try:
#             args = self.get_parser.parse_args()
#             config_city = args.get('config_city', 'mexico')
            
#             logger.info("User %s requesting property type distribution for city: %s", 
#                        current_user.get('id'), config_city)
            
#             markets_controller = MarketsController()
#             response = markets_controller.get_property_type_distribution(city=config_city)
            
#             return response, response.get('status_code', 200)
            
#         except Exception as e:
#             logger.error("Error in PropertyTypeDistribution GET: %s", str(e))
#             return {'message': 'Internal server error', 'status_code': 500}, 500

# class PriceRangeDistribution(Resource):
    # """
    # Resource to get distribution of properties by price range
    # """
    # get_parser = reqparse.RequestParser()
    # get_parser.add_argument('config_city', type=str, default="mexico", required=False, 
    #                        help='config_city is required', location='args')
    # get_parser.add_argument('price_type', type=str, default="buy", required=False, 
    #                        help='price_type (buy or rent) is required', location='args')
    
    # @authenticate
    # def get(self, current_user):
    #     """
    #     GET /markets/price-range-distribution
    #     Get distribution of properties by price range
    #     """
    #     try:
    #         args = self.get_parser.parse_args()
    #         config_city = args.get('config_city', 'mexico')
    #         price_type = args.get('price_type', 'buy')
            
    #         # Validate price_type
    #         if price_type not in ['buy', 'rent']:
    #             return {
    #                 'message': 'price_type must be either "buy" or "rent"',
    #                 'status_code': 400
    #             }, 400
            
    #         logger.info("User %s requesting price range distribution for city: %s, price_type: %s", 
    #                    current_user.get('id'), config_city, price_type)
            
    #         markets_controller = MarketsController()
    #         response = markets_controller.get_price_range_distribution(city=config_city, price_type=price_type)
            
    #         return response, response.get('status_code', 200)
            
    #     except Exception as e:
    #         logger.error("Error in PriceRangeDistribution GET: %s", str(e))
    #         return {'message': 'Internal server error', 'status_code': 500}, 500 