import json
import logging
import traceback
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.dbUtils import  RedshiftDatabase
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

    def fetch_inmuebles24_images(self, ids_market_data_inmuebles24, city='mexico'):
        """Fetch images from inmuebles24 table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            if city == 'mexico':
                query = self.query_controller.get_property_market_info_legacy_query(
                    None, ids_market_data_inmuebles24, None, city
                )
            else:
                query = self.query_controller.get_property_market_info_combined_query(
                    None, ids_market_data_inmuebles24, None, city
                )
            print("query", query)
            cursor.execute(query)
            row = cursor.fetchone()
            if row and row.get('pictures'):
                # pictures is expected to be a JSON array or comma-separated string
                try:
                    # Try to parse as JSON
                    raw_images = json.loads(row['pictures']) if isinstance(row['pictures'], str) else row['pictures']
                    if isinstance(raw_images, str):
                        # If still a string, split by comma
                        raw_images = [img.strip() for img in raw_images.split(',') if img.strip()]
                    # Filter out None, empty strings, and "None" strings
                    images = [img for img in raw_images if img and img.strip() and img.strip().lower() != "none"]
                except Exception:
                    # Fallback: split by comma and filter
                    print(traceback.format_exc())
                    raw_images = [img.strip() for img in row['pictures'].split(',') if img.strip()]
                    images = [img for img in raw_images if img and img.strip() and img.strip().lower() != "none"]
        except Exception as e:
            logger.error(f"Error fetching inmuebles24 images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images


    def fetch_spot2_images(self, ids_market_data_spot2, city='mexico'):
        """Fetch images from spot2 table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            if city == 'mexico':
                query = self.query_controller.get_property_market_info_legacy_query(
                    ids_market_data_spot2, None, None, city
                )
            else:
                query = self.query_controller.get_property_market_info_combined_query(
                    ids_market_data_spot2, None, None, city
                )
            cursor.execute(query)
            row = cursor.fetchone()
            
            if row and row.get('pictures') and row.get('pictures') != 'None':
                # pictures is expected to be a JSON array or comma-separated string
                try:
                    # Try to parse as JSON
                    raw_images = json.loads(row['pictures']) if isinstance(row['pictures'], str) else row['pictures']
                    if isinstance(raw_images, str):
                        # If still a string, split by comma
                        raw_images = [img.strip() for img in raw_images.split(',') if img.strip()]
                    # Filter out None, empty strings, and "None" strings
                    images = [img for img in raw_images if img and img.strip() and img.strip().lower() != "none"]
                except Exception:
                    # Fallback: split by comma and filter
                    raw_images = [img.strip() for img in row['pictures'].split(',') if img.strip()]
                    images = [img for img in raw_images if img and img.strip() and img.strip().lower() != "none"]
        except Exception as e:
            logger.error(f"Error fetching spot2 images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images

    def fetch_propiedades_images(self, ids_market_data_propiedades, city='mexico'):
        """Fetch images from propiedades table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            if city == 'mexico':
                query = self.query_controller.get_property_market_info_legacy_query(
                    None, None, ids_market_data_propiedades, city
                )
            else:
                query = self.query_controller.get_property_market_info_combined_query(
                    None, None, ids_market_data_propiedades, city
                )
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                # Collect all non-null and non-"None" image URLs
                for i in range(1, 6):
                    image_url = row.get(f'image_{i}')
                    if (image_url and 
                        isinstance(image_url, str) and 
                        image_url.strip() and 
                        image_url.strip().lower() != "none"):
                        images.append(image_url.strip())
        except Exception as e:
            logger.error(f"Error fetching propiedades images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images
