from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from datetime import datetime
from decimal import Decimal
import logging
import json

logger = logging.getLogger(__name__)

class SavedSearchesController:
    def __init__(self):
        logger.debug("Initializing SavedSearchesController")
        self.db = Database()
    
    def _generate_search_name(self, city, municipality, property_types, transaction_type):
        """Generate a default search name based on the search criteria"""
        parts = []
        
        if city:
            parts.append(city.title())
        
        if municipality:
            parts.append(municipality.title())
        
        if property_types and len(property_types) > 0:
            if len(property_types) == 1:
                parts.append(property_types[0])
            else:
                parts.append(f"{len(property_types)} Property Types")
        
        if transaction_type:
            parts.append(f"for {transaction_type}")
        
        if parts:
            return " ".join(parts)
        else:
            return "Property Search"
    
    def _generate_search_query(self, city, municipality, property_types, transaction_type):
        """Generate a default search query based on the search criteria"""
        parts = []
        
        if property_types and len(property_types) > 0:
            if len(property_types) == 1:
                parts.append(property_types[0].lower())
            else:
                parts.append("properties")
        else:
            parts.append("properties")
        
        if transaction_type:
            parts.append(f"for {transaction_type.lower()}")
        
        if city:
            parts.append(f"in {city.title()}")
        
        if municipality:
            parts.append(f"{municipality.title()}")
        
        return " ".join(parts)
    
    def _convert_decimal_fields(self, data_dict):
        """Convert Decimal fields to float for JSON serialization"""
        decimal_fields = [
            'plot_dimensions_min', 'plot_dimensions_max',
            'construction_dimensions_min', 'construction_dimensions_max',
            'price_min', 'price_max'
        ]
        
        for field in decimal_fields:
            if field in data_dict and data_dict[field] is not None:
                if isinstance(data_dict[field], Decimal):
                    data_dict[field] = float(data_dict[field])
        
        return data_dict

    def get_saved_searches(self, id=None, user_id=None):
        connection = None
        cursor = None
        try:
            logger.info(f"Getting saved searches - id: {id}, user_id: {user_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM bp_saved_searches WHERE 1=1'
            if id:
                query += f' AND id = {id}'
            if user_id:
                query += f' AND user_id = {user_id}'

            logger.debug(f"Executing query: {query}")
            print(query, "<==============")
            cursor.execute(query)
            result = cursor.fetchall()
            
            logger.debug(f"Found {len(result)} saved searches")
            processed_result = []
            for search in result:
                search_dict = dict(search)
                search_dict['created_at'] = search_dict['created_at'].isoformat()
                search_dict['updated_at'] = search_dict['updated_at'].isoformat()
                
                # Convert Decimal fields to float for JSON serialization
                search_dict = self._convert_decimal_fields(search_dict)
                
                # Also handle any nested Decimal values in JSONB fields
                if 'property_types' in search_dict and search_dict['property_types']:
                    # property_types is JSONB, ensure it's properly formatted
                    if isinstance(search_dict['property_types'], str):
                        try:
                            # Parse and re-serialize to ensure proper JSON format
                            parsed = json.loads(search_dict['property_types'])
                            search_dict['property_types'] = parsed
                        except (json.JSONDecodeError, TypeError):
                            # If parsing fails, keep as is
                            pass
                
                processed_result.append(search_dict)

            logger.info("Successfully retrieved saved searches")
            return Response.success(data=processed_result)
        except Exception as e:
            logger.error(f"Error getting saved searches: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")

    def create_saved_search(self, user_id, search_name, search_query, search_value, search_response, 
                           city=None, municipality=None, colonia=None, zip_code=None,
                           property_types=None, availability=None, plot_dimensions_min=None, 
                           plot_dimensions_max=None, construction_dimensions_min=None, 
                           construction_dimensions_max=None, price_min=None, price_max=None,
                           transaction_type=None, search_type=None):
        connection = None
        cursor = None
        try:
            # Generate default search_name if not provided
            if not search_name:
                search_name = self._generate_search_name(city, municipality, property_types, transaction_type)
            
            # Generate default search_query if not provided
            if not search_query:
                search_query = self._generate_search_query(city, municipality, property_types, transaction_type)
            
            logger.info(f"Creating saved search - user_id: {user_id}, search_name: {search_name}")
            logger.debug(f"Search params - query: {search_query}, value: {search_value}, response: {search_response}")
            logger.debug(f"Location params - city: {city}, municipality: {municipality}, colonia: {colonia}, zip: {zip_code}")
            logger.debug(f"Property params - types: {property_types}, availability: {availability}")
            logger.debug(f"Dimension params - plot: {plot_dimensions_min}-{plot_dimensions_max}, construction: {construction_dimensions_min}-{construction_dimensions_max}")
            logger.debug(f"Price params - min: {price_min}, max: {price_max}, transaction: {transaction_type}")
            
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = f'''
                INSERT INTO bp_saved_searches (
                    user_id, search_name, search_query, search_value, search_response,
                    city, municipality, colonia, zip_code,
                    property_types, availability, plot_dimensions_min, plot_dimensions_max,
                    construction_dimensions_min, construction_dimensions_max, 
                    price_min, price_max, transaction_type, search_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            '''
            logger.debug(f"Executing query: {query}")
            print("query=====>", query)

            # Convert property_types list to JSON for JSONB column
            property_types_json = json.dumps(property_types) if property_types else None
            
            cursor.execute(query, (
                user_id, search_name, search_query, search_value, search_response,
                city, municipality, colonia, zip_code,
                property_types_json, availability, plot_dimensions_min, plot_dimensions_max,
                construction_dimensions_min, construction_dimensions_max, 
                price_min, price_max, transaction_type, search_type
            ))
            connection.commit()
            search_id = cursor.fetchone()['id']
            logger.info(f"Saved search created successfully with ID: {search_id}")
            return Response.created(data={"id": search_id})
        except Exception as e:
            logger.error(f"Error creating saved search: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")

    def update_saved_search(self, id, search_name=None, search_query=None, search_value=None, search_response=None, search_status=None,
                           city=None, municipality=None, colonia=None, zip_code=None,
                           property_types=None, availability=None, plot_dimensions_min=None, 
                           plot_dimensions_max=None, construction_dimensions_min=None, 
                           construction_dimensions_max=None, price_min=None, price_max=None,
                           transaction_type=None, search_type=None):
        connection = None
        cursor = None
        try:
            logger.info(f"Updating saved search - id: {id}")
            logger.debug(f"Update params - name: {search_name}, query: {search_query}, value: {search_value}, response: {search_response}, status: {search_status}")
            logger.debug(f"Location params - city: {city}, municipality: {municipality}, colonia: {colonia}, zip: {zip_code}")
            logger.debug(f"Property params - types: {property_types}, availability: {availability}")
            logger.debug(f"Dimension params - plot: {plot_dimensions_min}-{plot_dimensions_max}, construction: {construction_dimensions_min}-{construction_dimensions_max}")
            logger.debug(f"Price params - min: {price_min}, max: {price_max}, transaction: {transaction_type}")
            
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE bp_saved_searches SET '
            updates = []
            params = []

            if search_name:
                updates.append('search_name = %s')
                params.append(search_name)
            if search_query:
                updates.append('search_query = %s')
                params.append(search_query)
            if search_value:
                updates.append('search_value = %s')
                params.append(search_value)
            if search_response:
                updates.append('search_response = %s')
                params.append(search_response)
            if search_status is not None:
                updates.append('search_status = %s')
                params.append(search_status)
            
            # New location fields
            if city:
                updates.append('city = %s')
                params.append(city)
            if municipality:
                updates.append('municipality = %s')
                params.append(municipality)
            if colonia:
                updates.append('colonia = %s')
                params.append(colonia)
            if zip_code:
                updates.append('zip_code = %s')
                params.append(zip_code)
            
            # New property filter fields
            if property_types:
                updates.append('property_types = %s')
                # Convert property_types list to JSON for JSONB column
                property_types_json = json.dumps(property_types)
                params.append(property_types_json)
            if availability:
                updates.append('availability = %s')
                params.append(availability)
            if plot_dimensions_min is not None:
                updates.append('plot_dimensions_min = %s')
                params.append(plot_dimensions_min)
            if plot_dimensions_max is not None:
                updates.append('plot_dimensions_max = %s')
                params.append(plot_dimensions_max)
            if construction_dimensions_min is not None:
                updates.append('construction_dimensions_min = %s')
                params.append(construction_dimensions_min)
            if construction_dimensions_max is not None:
                updates.append('construction_dimensions_max = %s')
                params.append(construction_dimensions_max)
            
            # Price and transaction fields
            if price_min is not None:
                updates.append('price_min = %s')
                params.append(price_min)
            if price_max is not None:
                updates.append('price_max = %s')
                params.append(price_max)
            if transaction_type:
                updates.append('transaction_type = %s')
                params.append(transaction_type)
            
            # Search metadata
            if search_type:
                updates.append('search_type = %s')
                params.append(search_type)

            if not updates:
                logger.warning("No fields provided for update")
                return Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE id = %s'
            params.append(id)

            logger.debug(f"Executing update query: {query} with params: {params}")
            cursor.execute(query, tuple(params))
            connection.commit()
            logger.info(f"Saved search {id} updated successfully")
            return Response.success(message="Saved search updated successfully")
        except Exception as e:
            logger.error(f"Error updating saved search {id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")

    def delete_saved_search(self, id):
        connection = None
        cursor = None
        try:
            logger.info(f"Deleting saved search - id: {id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM bp_saved_searches WHERE id = %s'
            logger.debug(f"Executing delete query: {query} with param: {id}")

            cursor.execute(query, (id,))
            connection.commit()
            logger.info(f"Saved search {id} deleted successfully")
            return Response.success(message="Saved search deleted successfully")
        except Exception as e:
            logger.error(f"Error deleting saved search {id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")