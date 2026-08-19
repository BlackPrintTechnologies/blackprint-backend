from flask_restful import Resource, reqparse
from flask import request, jsonify
from utils.responseUtils import Response
from module.search.controller import SavedSearchesController
from utils.commonUtil import authenticate
import logging

logger = logging.getLogger(__name__)

# Initialize Controllers
saved_searches_controller = SavedSearchesController()

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

class SavedSearches(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('user_id', type=int, required=False, help='User ID is required')
    create_parser.add_argument('search_name', type=str, required=False, help='Search name (optional, will be auto-generated if not provided)')
    create_parser.add_argument('search_query', type=str, required=False, help='Search query (optional, will be auto-generated if not provided)')
    create_parser.add_argument('search_value', type=dict, required=False, help='Search value is required')
    create_parser.add_argument('search_response', type=dict, required=False, help='Search response is required')
    
    # New location fields
    create_parser.add_argument('city', type=str, required=False, help='City for the search')
    create_parser.add_argument('municipality', type=str, required=False, help='Municipality for the search')
    create_parser.add_argument('colonia', type=str, required=False, help='Colonia for the search')
    create_parser.add_argument('zip_code', type=str, required=False, help='Zip code for the search')
    
    # New property filter fields
    create_parser.add_argument('property_types', type=list, location='json', required=False, help='Array of selected property types')
    create_parser.add_argument('availability', type=str, required=False, help='Property availability status')
    create_parser.add_argument('plot_dimensions_min', type=float, required=False, help='Minimum plot area in m²')
    create_parser.add_argument('plot_dimensions_max', type=float, required=False, help='Maximum plot area in m²')
    create_parser.add_argument('construction_dimensions_min', type=float, required=False, help='Minimum construction area in m²')
    create_parser.add_argument('construction_dimensions_max', type=float, required=False, help='Maximum construction area in m²')
    
    # Price and transaction fields
    create_parser.add_argument('price_min', type=float, required=False, help='Minimum price in MXN')
    create_parser.add_argument('price_max', type=float, required=False, help='Maximum price in MXN')
    create_parser.add_argument('transaction_type', type=str, required=False, help='Transaction type (Buy or Rent)')
    
    # Search metadata
    create_parser.add_argument('search_type', type=str, required=False, help='Type of search (Direct, Filter, Layer)')

    update_parser = reqparse.RequestParser()
    update_parser.add_argument('id', type=int, required=False)
    update_parser.add_argument('search_name', type=str, required=False)
    update_parser.add_argument('search_query', type=str, required=False)
    update_parser.add_argument('search_value', type=dict, required=False)
    update_parser.add_argument('search_response', type=dict, required=False)
    update_parser.add_argument('search_status', type=int, required=False)
    
    # New location fields for updates
    update_parser.add_argument('city', type=str, required=False)
    update_parser.add_argument('municipality', type=str, required=False)
    update_parser.add_argument('colonia', type=str, required=False)
    update_parser.add_argument('zip_code', type=str, required=False)
    
    # New property filter fields for updates
    update_parser.add_argument('property_types', type=list, location='json', required=False)
    update_parser.add_argument('availability', type=str, required=False)
    update_parser.add_argument('plot_dimensions_min', type=float, required=False)
    update_parser.add_argument('plot_dimensions_max', type=float, required=False)
    update_parser.add_argument('construction_dimensions_min', type=float, required=False)
    update_parser.add_argument('construction_dimensions_max', type=float, required=False)
    
    # Price and transaction fields for updates
    update_parser.add_argument('price_min', type=float, required=False)
    update_parser.add_argument('price_max', type=float, required=False)
    update_parser.add_argument('transaction_type', type=str, required=False)
    
    # Search metadata for updates
    update_parser.add_argument('search_type', type=str, required=False)

    @authenticate
    def get(self, current_user, search_id=None):
        # data = self.update_parser.parse_args()
        # search_id = data.get('id')
        response = saved_searches_controller.get_saved_searches(id=search_id, user_id=current_user)
        return response

    @authenticate
    def post(self, current_user):
        data = self.create_parser.parse_args()
        user_id = current_user
        search_name = data.get('search_name')
        search_query = data.get('search_query')
        search_value = data.get('search_value')
        search_response = data.get('search_response')
        
        # Extract new location fields
        city = data.get('city')
        municipality = data.get('municipality')
        colonia = data.get('colonia')
        zip_code = data.get('zip_code')
        
        # Extract new property filter fields
        property_types = data.get('property_types')
        availability = data.get('availability')
        plot_dimensions_min = data.get('plot_dimensions_min')
        plot_dimensions_max = data.get('plot_dimensions_max')
        construction_dimensions_min = data.get('construction_dimensions_min')
        construction_dimensions_max = data.get('construction_dimensions_max')
        
        # Extract price and transaction fields
        price_min = data.get('price_min')
        price_max = data.get('price_max')
        transaction_type = data.get('transaction_type')
        
        # Extract search metadata
        search_type = data.get('search_type')

        response = saved_searches_controller.create_saved_search(
            user_id=user_id,
            search_name=search_name,
            search_query=search_query,
            search_value=search_value,
            search_response=search_response,
            city=city,
            municipality=municipality,
            colonia=colonia,
            zip_code=zip_code,
            property_types=property_types,
            availability=availability,
            plot_dimensions_min=plot_dimensions_min,
            plot_dimensions_max=plot_dimensions_max,
            construction_dimensions_min=construction_dimensions_min,
            construction_dimensions_max=construction_dimensions_max,
            price_min=price_min,
            price_max=price_max,
            transaction_type=transaction_type,
            search_type=search_type
        )
        return response

    @authenticate
    def put(self, current_user):
        data = self.update_parser.parse_args()
        search_id = data.get('id')
        search_name = data.get('search_name')
        search_query = data.get('search_query')
        search_value = data.get('search_value')
        search_response = data.get('search_response')
        search_status = data.get('search_status')
        
        # Extract new location fields
        city = data.get('city')
        municipality = data.get('municipality')
        colonia = data.get('colonia')
        zip_code = data.get('zip_code')
        
        # Extract new property filter fields
        property_types = data.get('property_types')
        availability = data.get('availability')
        plot_dimensions_min = data.get('plot_dimensions_min')
        plot_dimensions_max = data.get('plot_dimensions_max')
        construction_dimensions_min = data.get('construction_dimensions_min')
        construction_dimensions_max = data.get('construction_dimensions_max')
        
        # Extract price and transaction fields
        price_min = data.get('price_min')
        price_max = data.get('price_max')
        transaction_type = data.get('transaction_type')
        
        # Extract search metadata
        search_type = data.get('search_type')

        response = saved_searches_controller.update_saved_search(
            id=search_id,
            search_name=search_name,
            search_query=search_query,
            search_value=search_value,
            search_response=search_response,
            search_status=search_status,
            city=city,
            municipality=municipality,
            colonia=colonia,
            zip_code=zip_code,
            property_types=property_types,
            availability=availability,
            plot_dimensions_min=plot_dimensions_min,
            plot_dimensions_max=plot_dimensions_max,
            construction_dimensions_min=construction_dimensions_min,
            construction_dimensions_max=construction_dimensions_max,
            price_min=price_min,
            price_max=price_max,
            transaction_type=transaction_type,
            search_type=search_type
        )
        return response

    @authenticate
    def delete(self, current_user):
        data = self.update_parser.parse_args()
        search_id = data.get('id')
        response = saved_searches_controller.delete_saved_search(id=search_id)
        return response
