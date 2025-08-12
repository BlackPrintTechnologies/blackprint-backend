import logging
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase
from module.markets.query import MarketsQueryController

logger = logging.getLogger(__name__)

class MarketsController:
    def __init__(self):
        self.redshift_connection = RedshiftDatabase()
        self.query_controller = MarketsQueryController()
    
    def get_all_distinct_property_types(self, city='mexico'):
        """
        Get all distinct property types from the market data tables
        Uses combined table approach for better performance
        """
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info("Fetching all distinct property types for city: %s", city)
            
            # Use the new query controller with combined table approach
            query = self.query_controller.get_all_distinct_property_types_query(city)
            
            logger.debug("Property types query: %s", query)
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Extract property types from results
            property_types = [row['property_type'] for row in results if row['property_type']]
            
            resp = Response.success(data=property_types, message='Success')
        except Exception as e:
            logger.error("Error fetching property types: %s", str(e))
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp
    
    def get_property_market_info(self, spot2_id, inmuebles24_id, propiedades_id, city='mexico'):
        """
        Get market information for a property from the appropriate market data tables
        Uses combined table approach for better performance, falls back to legacy tables if needed
        """
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info("Fetching market info for spot2_id=%s, inmuebles24_id=%s, propiedades_id=%s", 
                       spot2_id, inmuebles24_id, propiedades_id)
            
            # Try combined table first for better performance
            try:
                query = self.query_controller.get_property_market_info_combined_query(
                    spot2_id, inmuebles24_id, propiedades_id, city
                )
                logger.debug("Using combined table query: %s", query)
                cursor.execute(query)
                res = cursor.fetchall()
                
                # If combined table returns results, use them
                if res and len(res) > 0:
                    resp = Response.success(data=res, message='Success')
                    return resp
                    
            except Exception as combined_error:
                logger.warning("Combined table query failed, falling back to legacy tables: %s", str(combined_error))
            
            # Fallback to legacy tables if combined table fails or returns no results
            query = self.query_controller.get_property_market_info_legacy_query(
                spot2_id, inmuebles24_id, propiedades_id, city
            )
            
            logger.debug("Using legacy table query: %s", query)
            cursor.execute(query)
            res = cursor.fetchall()
            resp = Response.success(data=res, message='Success')
            
        except Exception as e:
            logger.error("Error fetching market info: %s", str(e))
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp
    
    def get_market_summary_stats(self, city='mexico'):
        """
        Get market summary statistics for a city
        Uses combined table for better performance
        """
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info("Fetching market summary stats for city: %s", city)
            
            query = self.query_controller.get_market_summary_stats_query(city)
            logger.debug("Market summary stats query: %s", query)
            cursor.execute(query)
            results = cursor.fetchall()
            
            resp = Response.success(data=results, message='Success')
        except Exception as e:
            logger.error("Error fetching market summary stats: %s", str(e))
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp
