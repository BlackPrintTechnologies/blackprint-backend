import json
import logging
import traceback
from functools import lru_cache
from psycopg2.extras import RealDictCursor
from module.pois.query import POIsQueryController
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase
from utils.iconUtils import IconMapper

logger = logging.getLogger(__name__)

class POIsController:
    def __init__(self):
        self.qc = POIsQueryController()
        self.db = Database()
        self.redshift_db = RedshiftDatabase()

    def get_pois(self, lat, lng, radius, config_city=None):
        """Get POIs within specified radius from lat/lng coordinates."""
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.qc.get_pois_by_coordinates_query(lat, lng, radius, config_city)
            logger.info(f"POIs query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            logger.info(f"POIs results count: {len(res)}")
            resp = Response.success(data={"response": res})
            
        except Exception as e:
            logger.error(f"Error in get_pois: {str(e)}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_brands(self, radius, lat, lng, category=None, subcategories=None, subsubcategories=None, brand_names=None, business_names=None, city="mexico"):
        """Get brands within specified catchment radius."""
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.qc.get_brand_query(radius, lat, lng, category_1=category, subcategories=subcategories, subsubcategories=subsubcategories, brand_names=brand_names, business_names=business_names, city=city)
            logger.info(f"Brand query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            #print the first 5 value of res
            print("res=====>", res[:5])
            
            # Add icon URLs and brand URLs to the results
            enhanced_results = []
            logger.info(f"Length of res: {len(res)}")
            for result in res:
                result['icon_url'] = IconMapper.get_icon_url(result['category_1'])
                result['brand_url'] = IconMapper.get_brand_url(result['brand'])
                enhanced_results.append(result)
            
            # Filter by category if specified
            # if category:
            #     enhanced_results = [result for result in enhanced_results if result['category_1'] in category]
            
            logger.info(f"Enhanced results count: {len(enhanced_results)}")
            resp = Response.success(data={"response": enhanced_results})
            
        except Exception as e:
            logger.error(f"Error in get_brands: {str(e)}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def search_brands(self, brand_name=None, city="mexico", chain_id=None):
        """Search brands by name pattern and/or chain_id."""
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.qc.get_brand_search_query(brand_name, city, chain_id)
            logger.info(f"Brand search query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            logger.info(f"Brand search results count: {len(res)}")
            resp = Response.success(data=res)
            
        except Exception as e:
            logger.error(f"Error in search_brands: {str(e)}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    @lru_cache(maxsize=100)
    def get_pois_hierarchy(self, config_city=None):
        """Get POI category hierarchy from places table."""
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.qc.get_pois_hierarchy_query(config_city)
            logger.info(f"POI hierarchy query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Build hierarchy structure
            hierarchy = self._build_hierarchy_from_results(res)
            
            logger.info(f"POI hierarchy built with {len(hierarchy)} top-level categories")
            resp = Response.success(data={"hierarchy": hierarchy})
            
        except Exception as e:
            logger.error(f"Error in get_pois_hierarchy: {str(e)}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def _build_hierarchy_from_results(self, results):
        """Build hierarchical structure from database results."""
        hierarchy = {}
        
        for row in results:
            cat1 = row.get('category_1', '').strip()
            cat2 = row.get('category_2', '').strip()
            cat3 = row.get('category_3', '').strip()
            
            if not cat1:
                continue
                
            # Create category_1 entry if it doesn't exist
            if cat1 not in hierarchy:
                hierarchy[cat1] = {
                    'id': cat1.lower().replace(' ', '_').replace('&', 'and'),
                    'name': cat1,
                    'subCategories': {}
                }
            
            # Add category_2 if it exists
            if cat2 and cat2 != cat1:
                if cat2 not in hierarchy[cat1]['subCategories']:
                    hierarchy[cat1]['subCategories'][cat2] = {
                        'id': cat2.lower().replace(' ', '_').replace('&', 'and'),
                        'name': cat2,
                        'subSubCategories': {}
                    }
                
                # Add category_3 if it exists
                if cat3 and cat3 != cat2 and cat3 != cat1:
                    if cat3 not in hierarchy[cat1]['subCategories'][cat2]['subSubCategories']:
                        hierarchy[cat1]['subCategories'][cat2]['subSubCategories'][cat3] = {
                            'id': cat3.lower().replace(' ', '_').replace('&', 'and'),
                            'name': cat3
                        }
        
        # Convert dictionaries to lists for final structure
        final_hierarchy = []
        for cat1_data in hierarchy.values():
            cat1_entry = {
                'id': cat1_data['id'],
                'name': cat1_data['name'],
                'subCategories': []
            }
            
            for cat2_data in cat1_data['subCategories'].values():
                cat2_entry = {
                    'id': cat2_data['id'],
                    'name': cat2_data['name'],
                    'subSubCategories': []
                }
                
                for cat3_data in cat2_data['subSubCategories'].values():
                    cat2_entry['subSubCategories'].append({
                        'id': cat3_data['id'],
                        'name': cat3_data['name']
                    })
                
                cat1_entry['subCategories'].append(cat2_entry)
            
            final_hierarchy.append(cat1_entry)
        
        return final_hierarchy