from module.markets.controller import MarketsController
from utils.dbUtils import Database, RedshiftDatabase
# from utils.connectionPoolDbUtils import postgres_pool ,redshift_pool
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from module.properties.query import QueryController
import json
import h3
from utils.cacheUtlis import cache_response
from utils.iconUtils import IconMapper
from utils.streetViewUtils import get_street_view_metadata_cached
from utils.normalization_utils import normalize_fid
# from flask import request
import time
from datetime import datetime
import logging
from logsmanager.logging_config import setup_logging
from concurrent.futures import ThreadPoolExecutor
from module.properties.commercial_growth_json import get_commercial_growth_json
import decimal
setup_logging()
logger = logging.getLogger(__name__)

config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
BASE_URL = config['BASE_URL']

class UserPropertyController:
    def __init__(self):
        self.qc = QueryController()
        self.db = Database()
        self.redshift_connection = RedshiftDatabase()
        self.user_property_status = ['view', 'shortlisted', 'not_interested', 'finalized']
        

    def get_additional_property_details(self, properties, config_city=None):
        connection = None
        cursor = None
        redshift_connection = None
        redshift_cursor = None
        resp = None  # Initialize resp at the start
        try:
            logger.info("Fetching additional properties : %s", properties)
            # print("FID QUERY I AM GETTING %s", fid_query)
            if not properties:
                logger.info("No requested properties found for user_id: %s", properties)
                resp = Response.success(data=[], message='No properties found')
                return resp
            
            # Extract FIDs and create filter for property query
            fids = [str(result['fid']) for result in properties]
            fid_col = "fid" if config_city == 'mexico' else "id_stg_demographic_socioeconomic_qro"
            fid_filter = f"WHERE {fid_col} IN ({','.join(fids)})"
            
            # Get full property details from Redshift using existing query controller
            redshift_connection = self.redshift_connection.connect()
            redshift_cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
            property_query = self.qc.get_property_query(fid_filter, city=config_city)
            logger.info("PROPERTY QUERY I AM GETTING %s",property_query)
            redshift_cursor.execute(property_query)
            property_results = redshift_cursor.fetchall()
            # logger.info("Properties result i am getting %s",property_results[0])
            # Process results using existing property JSON formatter
            if property_results:
                property_controller = PropertyController()
                formatted_results = property_controller.get_property_json(property_results, show_all_keys=False, city=config_city)  # Default to CDMX for user properties
                print("Formatted results I am getting %s",len(formatted_results))
                final_res = []
                for result in formatted_results:
                    fid = result['property_details']['fid']
                    prop_lat, prop_lng = result["property_details"]["lat"], result["property_details"]["lng"]
                    if prop_lat and prop_lng:
                        pano_id=get_street_view_metadata_cached(float(prop_lat),float(prop_lng))
                        if pano_id:
                            headings = [0, 45, 90, 135, 180, 225, 270, 315]
                            fov = 90  # Field of view
                            size = "600x300"  # Image size
                            # base_url = request.host_url.rstrip('/')  # Get the base URL
                            street_images = [
                                f"{BASE_URL}/properties/street_view_image?pano_id={pano_id}&heading={heading}&fov={fov}&size={size}"
                                for heading in headings
                            ]
                            result["property_details"]["street_images"] = street_images
                        else:
                            result["property_details"]["street_images"] = []
                    for item in properties:
                        if item['fid'] == fid:
                            for key, value in item.items():
                                if isinstance(value, datetime):
                                    item[key] = value.isoformat()
                            result['property_details'] = {**result['property_details'], **item}
                            final_res.append(result['property_details'])
                # logger.info("formated result %s",final_res)
                logger.info("Successfully fetched %d requested properties", len(final_res))
                resp = Response.success(data=final_res, message='Success')
            else:
                logger.warning("No property details found for requested FIDs")
                resp = Response.success(data=[], message='No property details found')
                
        except Exception as e:
            logger.error("Error fetching requested properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            if redshift_cursor:
                redshift_cursor.close()
            if redshift_connection:
                self.redshift_connection.disconnect(redshift_connection)
            return resp

    
    def get_user_properties(self, user_id, fid=None,  prop_status=None, config_city=None):
        connection = None
        cursor = None
        try:
            logger.info("Fetching user properties with status: %s", prop_status)
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = 'SELECT * FROM bp_user_property WHERE status = 1'
            if user_id:
                query += f" AND user_id = {user_id} "
            if prop_status:
                query += f" AND user_property_status = '{prop_status}' "
            if fid :
                query += f" AND fid = {fid} "
            if config_city:
                query += f" AND config_city = '{config_city}' "
            
            query += "order by updated_at desc"
            logger.debug("Executing query: %s", query)
            cursor.execute(query)
            result = cursor.fetchall()
            logger.info("Fetched %d user properties", len(result))
            resp = self.get_additional_property_details(result, config_city=config_city)
        except Exception as e:
            logger.error("Error fetching user properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp
        
    def update_property_request_status(self, fid, user, request_status=1, config_city='mexico'):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = f'''update bp_user_property set request_status = {request_status},  updated_at = now() where fid = {fid} and user_id = {user} '''
            print("QUERY I AM GETTING %s",query)
            cursor.execute(query)
            connection.commit() 
            resp = Response.success(message='Property requested successfully')
        except Exception as e:
            logger.error("Error fetching request info for property: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp

    def add_user_property(self, fid, user_id, prop_status, config_city=None):
        connection = None
        cursor = None
        try:
            logger.info("Adding user property with fid=%s, user_id=%s, status=%s", fid, user_id, prop_status)
            start_time = time.time()
            connection = self.db.connect()
            cursor = connection.cursor()
            query = f"INSERT INTO bp_user_property (fid, user_id, user_property_status, config_city) VALUES ({fid}, {user_id}, '{prop_status}', '{config_city}')"
            logger.debug("Executing query: %s", query)

            cursor.execute(query)
            connection.commit()
            logger.info("User property added successfully")
            resp = Response.created(message='Success')
            end_time = time.time()
            logger.debug("Execution time for add_user_property: %.4f seconds", end_time - start_time)
        except Exception as e:
            logger.error("Error adding user property: %s", str(e), exc_info=True)
            if 'unique constraint' in str(e):
                resp = 'User property already exists'
            else:
                resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp
        
    def update_property_status(self, user_id, fid , prop_status='view', config_city='mexico'):
        connection = None
        cursor = None
        resp = None
        try:
            if prop_status not in self.user_property_status:
                logger.error("Invalid property status: %s", prop_status)
                return Response.bad_request(message='Invalid property status')
            logger.info("Shortlisting property with fid=%s for user_id=%s", fid, user_id)
            start_time = time.time()
            connection = self.db.connect()
            cursor = connection.cursor()
            query = f"UPDATE bp_user_property SET user_property_status = '{prop_status}', updated_at = now()  WHERE fid = {fid} AND user_id = {user_id} AND config_city = '{config_city}' "
            logger.debug("Executing query: %s", query)
            cursor.execute(query)
            connection.commit()
            logger.info("Property shortlisted successfully")
            resp = Response.success(message='Success')
            end_time = time.time()
            logger.debug("Execution time for shortlist_property: %.4f seconds", end_time - start_time)
        except Exception as e:
            logger.error("Error shortlisting property: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp

    #get request properties by user
    def get_requested_properties(self, user_id, config_city=None):
        """Get all properties that have been requested by a specific user"""
        connection = None
        cursor = None
        redshift_connection = None
        redshift_cursor = None
        resp = None  # Initialize resp at the start
        try:
            logger.info("Fetching requested properties for user_id: %s , config_city: %s", user_id, config_city)
            # First get all requested property FIDs from PostgreSQL
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            fid_query = f'''
                SELECT *
                FROM bp_user_property 
                WHERE user_id = {user_id} 
                AND request_status = 1
                AND config_city = '{config_city}'
                order by updated_at desc
            '''
            # print("FID QUERY I AM GETTING %s", fid_query)
            cursor.execute(fid_query)
            fid_results = cursor.fetchall()
            resp  = self.get_additional_property_details(fid_results, config_city=config_city)
        except Exception as e:
            logger.error("Error fetching requested properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            if redshift_cursor:
                redshift_cursor.close()
            if redshift_connection:
                self.redshift_connection.disconnect(redshift_connection)
            return resp

class PropertyController:
    def __init__(self):
        self.qc = QueryController()
        self.db = Database()
        self.redshift_connection = RedshiftDatabase()
        self.markets_controller = MarketsController()


    def get_property_json(self, results, show_all_keys=True, city='mexico'):
        resp = []
        try:
            logger.info("Processing property JSON for %d results with city=%s", len(results), city)
            for result in results:
                traffic = {}  # Always initialize traffic to an empty dict
                # Handle different column structures for CDMX vs QRO
                if city == 'queretaro' or city == 'el_marques':
                    # QRO now uses same structure as CDMX (no demographic fields)
                    property_details = {
                        "fid": result["fid"],
                        "lat": json.loads(result['centroid'])['coordinates'][1] if result['centroid'] else None,
                        "lng" : json.loads(result['centroid'])['coordinates'][0] if result['centroid'] else None,
                        "is_on_market": result["is_on_market"],
                        "total_surface_area": result.get("total_surface_area", None),
                        "total_construction_area": result.get("total_construction_area", None),
                        "total_built_perm": result.get('total_built_perm', None),
                        "total_units": result.get('property_count_per_lot', None),
                        "street_address": result.get('nom_loc') + ' ' + result.get('nom_mun') ,
                        "year_built": result.get("year_built", None),
                        "special_facilities": result.get("special_facilities", None),
                        "unit_land_value": result.get("unit_land_value", None),
                        "land_value": result.get("land_value", None),
                        "usage_desc": result.get("usage_desc", None),
                        "key_vus": result.get("key_vus", None),
                        "predominant_level": result.get("predominant_level", None),
                        "h3_indexes": result["h3_indexes"],
                        "height": result.get("height", None),
                        "cos": result.get("cos", None),
                        "cus": result.get("cus", None),
                        "min_housing": result.get("min_housing", None),
                        "ids_market_data_inmuebles24": result.get("ids_market_data_inmuebles24", None),
                        "ids_market_data_propiedades": result.get("ids_market_data_propiedades", None),
                        "crecimiento_promedio_municipal": result.get("crecimiento_promedio_municipal", None),
                        "crecimiento_promedio_entidad": result.get("crecimiento_promedio_entidad", None),
                        "crecimiento_promedio_ageb": result.get("crecimiento_promedio_ageb", None)
                    }
                else:
                    # CDMX structure (original)
                    property_details = {
                        "fid": result["fid"],
                        "lat": json.loads(result['centroid'])['coordinates'][1] if result['centroid'] else None,
                        "lng" : json.loads(result['centroid'])['coordinates'][0] if result['centroid'] else None,
                        "is_on_market": result["is_on_market"],
                        "total_surface_area": result["total_surface_area"],
                        "total_construction_area": result["total_construction_area"],
                        "total_built_perm": result['total_built_perm'],
                        "total_units": result['property_count_per_lot'],
                        "street_address": result["street_address"],
                        "year_built": result["year_built"],
                        "special_facilities": result["special_facilities"],
                        "unit_land_value": result["unit_land_value"],
                        "land_value": result["land_value"],
                        "usage_desc": result["usage_desc"],
                        "key_vus": result["key_vus"],
                        "predominant_level": result["predominant_level"],
                        "h3_indexes": result["h3_indexes"],
                        "height": result.get("height", None),
                        "cos": result.get("cos", None),
                        "cus": result.get("cus", None),
                        "min_housing": result.get("min_housing", None),
                        "ids_market_data_inmuebles24": result.get("ids_market_data_inmuebles24", None),
                        "ids_market_data_propiedades": result.get("ids_market_data_propiedades", None),
                        "crecimiento_promedio_municipal": result.get("crecimiento_promedio_municipal", None),
                        "crecimiento_promedio_entidad": result.get("crecimiento_promedio_entidad", None),
                        "crecimiento_promedio_ageb": result.get("crecimiento_promedio_ageb", None)
                    }
                # --- Set street_images based on priority: inmuebles24 -> spot2 -> propiedades -> street view ---
                street_images = []

                # Pre-process all IDs to get valid ones (excluding -1)
                valid_inmuebles24_id = None
                if property_details.get("ids_market_data_inmuebles24"):
                    inmuebles24_ids = [id_.strip() for id_ in str(property_details["ids_market_data_inmuebles24"]).split(',') if id_.strip()]
                    valid_ids = [id_ for id_ in inmuebles24_ids if id_ != '-1']
                    if valid_ids:
                        valid_inmuebles24_id = valid_ids[0]

                valid_spot2_id = None
                if result.get("ids_market_data_spot2"):
                    spot2_ids = [id_.strip() for id_ in str(result["ids_market_data_spot2"]).split(',') if id_.strip()]
                    valid_ids = [id_ for id_ in spot2_ids if id_ != '-1']
                    if valid_ids:
                        valid_spot2_id = valid_ids[0]

                valid_propiedades_id = None
                if result.get("ids_market_data_propiedades"):
                    print("ids_market_data_propiedades", result["ids_market_data_propiedades"])
                    propiedades_ids = [id_.strip() for id_ in str(result["ids_market_data_propiedades"]).split(',') if id_.strip()]
                    valid_ids = [id_ for id_ in propiedades_ids if id_ != '-1']
                    if valid_ids:
                        valid_propiedades_id = valid_ids[0]
                print("valid id", valid_ids)
                
                # Helper function to check if images are valid
                def has_valid_images(images_list):
                    if not images_list:
                        return False
                    # Filter out None, empty strings, and "None" strings
                    valid_images = [img for img in images_list if img and img.strip() and img.strip().lower() != "none"]
                    return len(valid_images) > 0

                # Now use the valid IDs in the if-elif ladder
                if valid_inmuebles24_id:
                    images = self.markets_controller.fetch_inmuebles24_images(valid_inmuebles24_id, city)
                    print("INMUEBLES24 IMAGES", images, valid_inmuebles24_id)
                    if has_valid_images(images):
                        # Resize images to 600x300 and filter out None values
                        valid_images = [img for img in images if img and img.strip() and img.strip().lower() != "none"]
                        street_images = [
                            img.replace("1200x1200", "600x300") if "1200x1200" in img else img
                            for img in valid_images[:6]
                        ]

                # If no inmuebles24 images, try spot2
                if not street_images and valid_spot2_id:
                    spot2_images = self.markets_controller.fetch_spot2_images(valid_spot2_id, city)
                    print("SPOT2 IMAGES", spot2_images)
                    if has_valid_images(spot2_images):
                        valid_images = [img for img in spot2_images if img and img.strip() and img.strip().lower() != "none"]
                        street_images = valid_images[:6]

                # If no spot2 images, try propiedades
                if not street_images and valid_propiedades_id:
                    propiedades_images = self.markets_controller.fetch_propiedades_images(valid_propiedades_id, city)
                    print("PROPIEDADES IMAGES", propiedades_images)
                    if has_valid_images(propiedades_images):
                        valid_images = [img for img in propiedades_images if img and img.strip() and img.strip().lower() != "none"]
                        street_images = valid_images[:6]

                # If no marketplace images found, try street view
                if not street_images and property_details.get("lat") and property_details.get("lng"):
                    print("NO VALID IMAGES FOUND - FALLING BACK TO STREET VIEW")
                    prop_lat = property_details["lat"]
                    prop_lng = property_details["lng"]
                    pano_id = get_street_view_metadata_cached(float(prop_lat), float(prop_lng))
                    if pano_id:
                        headings = [0, 45, 90, 135, 180, 225, 270, 315]
                        fov = 90
                        size = "600x300"
                        street_images = [
                            f"{BASE_URL}/properties/street_view_image?pano_id={pano_id}&heading={heading}&fov={fov}&size={size}"
                            for heading in headings
                        ]
                        print("STREET VIEW IMAGES GENERATED:", len(street_images))
                    else:
                        print("NO STREET VIEW PANORAMA FOUND FOR COORDINATES")
                
                # Final filter to ensure no invalid images make it through
                street_images = [img for img in street_images if img and img.strip() and img.strip().lower() != "none"]
                property_details["street_images"] = street_images

                # Handle market_info for different cities
                if city == 'queretaro' or city == 'el_marques':
                    market_info = {
                        "ids_market_data_spot2" : result.get("ids_market_data_spot2", None),
                        "ids_market_data_inmuebles24" : result.get("ids_market_data_inmuebles24", None),
                        "ids_market_data_propiedades" : result.get("ids_market_data_propiedades", None),
                        # QRO doesn't have these market columns, use .get() with defaults
                        "rent_price_spot2": result.get("rent_price_spot2", None),
                        "rent_price_per_m2_spot2": result.get("rent_price_per_m2_spot2", None),
                        "buy_price_spot2": result.get("buy_price_spot2", None),
                        "buy_price_per_m2_spot2": result.get("buy_price_per_m2_spot2", None),
                        "total_area_spot2": result.get("total_area_spot2", None),
                        "property_type_spot2": result.get("property_type_spot2", None),
                        "rent_price_inmuebles24": result.get("rent_price_inmuebles24", None),
                        "rent_price_per_m2_inmuebles24": result.get("rent_price_per_m2_inmuebles24", None),
                        "buy_price_inmuebles24": result.get("buy_price_inmuebles24", None),
                        "buy_price_per_m2_inmuebles24": result.get("buy_price_per_m2_inmuebles24", None),
                        "total_area_inmuebles24": result.get("total_area_inmuebles24", None),
                        "property_type_inmuebles24": result.get("property_type_inmuebles24", None),
                        "rent_price_propiedades": result.get("rent_price_propiedades", None),
                        "rent_price_per_m2_propiedades": result.get("rent_price_per_m2_propiedades", None),
                        "buy_price_propiedades": result.get("buy_price_propiedades", None),
                        "buy_price_per_m2_propiedades": result.get("buy_price_per_m2_propiedades", None),
                        "total_area_propiedades": result.get("total_area_propiedades", None),
                        "block_type": result.get("block_type", None),
                        "density_d": result.get("density_d", None),
                        "scope": result.get("scope", None),
                        "floor_levels": result.get("floor_levels", None),
                        "open_space" : result.get("open_space", None),
                        "id_land_use": result.get('id_land_use', None),
                        "id_municipality": result.get("id_municipality", None),
                        "id_city_blocks": result.get("id_city_blocks", None),
                        "total_houses": result.get("total_houses", None),
                        "locality_size": result.get("locality_size", None),
                        "city_link": result.get("city_link", None)
                    }
                else:
                    # CDMX structure (original)
                    market_info = {
                        "ids_market_data_spot2" : result["ids_market_data_spot2"],
                        "ids_market_data_inmuebles24" : result["ids_market_data_inmuebles24"],
                        "ids_market_data_propiedades" : result["ids_market_data_propiedades"],
                        "rent_price_spot2": result["rent_price_spot2"],
                        "rent_price_per_m2_spot2": result["rent_price_per_m2_spot2"],
                        "buy_price_spot2": result["buy_price_spot2"],
                        "buy_price_per_m2_spot2": result["buy_price_per_m2_spot2"],
                        "total_area_spot2": result["total_area_spot2"],
                        "property_type_spot2": result["property_type_spot2"],
                        "rent_price_inmuebles24": result["rent_price_inmuebles24"],
                        "rent_price_per_m2_inmuebles24": result["rent_price_per_m2_inmuebles24"],
                        "buy_price_inmuebles24": result["buy_price_inmuebles24"],
                        "buy_price_per_m2_inmuebles24": result["buy_price_per_m2_inmuebles24"],
                        "total_area_inmuebles24": result["total_area_inmuebles24"],
                        "property_type_inmuebles24": result["property_type_inmuebles24"],
                        "rent_price_propiedades": result["rent_price_propiedades"],
                        "rent_price_per_m2_propiedades": result["rent_price_per_m2_propiedades"],
                        "buy_price_propiedades": result["buy_price_propiedades"],
                        "buy_price_per_m2_propiedades": result["buy_price_per_m2_propiedades"],
                        "total_area_propiedades": result["total_area_propiedades"],
                        "block_type": result["block_type"],
                        "density_d": result["density_d"],
                        "scope": result["scope"],
                        "floor_levels": result["floor_levels"],
                        "open_space" : result["open_space"],
                        "id_land_use": result['id_land_use'],
                        "id_municipality": result["id_municipality"],
                        "id_city_blocks": result["id_city_blocks"],
                        "total_houses": result["total_houses"],
                        "locality_size": result["locality_size"],
                        "city_link": result["city_link"]
                    }
                if show_all_keys:
                    # Handle POI data for different cities
                    if city == 'queretaro' or city == 'el_marques':
                        pois = {
                            #add category here for icon image
                            "category": {
                                category: IconMapper.get_icon_url(category) 
                                for category in IconMapper.CATEGORY_ICON_MAP
                            },
                            "front" : {
                                "brands_active_life_front": result.get("brands_active_life_front", None),
                                "brands_arts_and_entertainment_front": result.get("brands_arts_and_entertainment_front", None),
                                "brands_attractions_and_activities_front": result.get("brands_attractions_and_activities_front", None),
                                "brands_automotive_front": result.get("brands_automotive_front", None),
                                "brands_eat_and_drink_front": result.get("brands_eat_and_drink_front", None),
                                "brands_education_front": result.get("brands_education_front", None),
                                "brands_financial_service_front": result.get("brands_financial_service_front", None),
                                "brands_health_and_medical_front": result.get("brands_health_and_medical_front", None),
                                "brands_public_service_and_government_front": result.get("brands_public_service_and_government_front", None),
                                "brands_retail_front": result.get("brands_retail_front", None),
                            },
                            "500" : {
                                "brands_active_life_500m": result.get("brands_active_life_500m", None),
                                "brands_arts_and_entertainment_500m": result.get("brands_arts_and_entertainment_500m", None),
                                "brands_attractions_and_activities_500m": result.get("brands_attractions_and_activities_500m", None),
                                "brands_automotive_500m": result.get("brands_automotive_500m", None),
                                "brands_eat_and_drink_500m": result.get("brands_eat_and_drink_500m", None),
                                "brands_education_500m": result.get("brands_education_500m", None),
                                "brands_financial_service_500m": result.get("brands_financial_service_500m", None),
                                "brands_health_and_medical_500m": result.get("brands_health_and_medical_500m", None),
                                "brands_public_service_and_government_500m": result.get("brands_public_service_and_government_500m", None),
                                "brands_retail_500m": result.get("brands_retail_500m", None),
                            },
                            "1000" : {
                                "brands_active_life_1km": result.get("brands_active_life_1km", None),
                                "brands_arts_and_entertainment_1km": result.get("brands_arts_and_entertainment_1km", None),
                                "brands_attractions_and_activities_1km": result.get("brands_attractions_and_activities_1km", None),
                                "brands_automotive_1km": result.get("brands_automotive_1km", None),
                                "brands_eat_and_drink_1km": result.get("brands_eat_and_drink_1km", None),
                                "brands_education_1km": result.get("brands_education_1km", None),
                                "brands_financial_service_1km": result.get("brands_financial_service_1km", None),
                                "brands_health_and_medical_1km": result.get("brands_health_and_medical_1km", None),
                                "brands_public_service_and_government_1km": result.get("brands_public_service_and_government_1km", None),
                                "brands_retail_1km": result.get("brands_retail_1km", None)
                            },  
                        }
                    else:
                        # CDMX structure (original)
                        pois = {
                            #add category here for icon image
                            "category": {
                                category: IconMapper.get_icon_url(category) 
                                for category in IconMapper.CATEGORY_ICON_MAP
                            },
                            "front" : {
                                "brands_active_life_front": result["brands_active_life_front"],
                                "brands_arts_and_entertainment_front": result["brands_arts_and_entertainment_front"],
                                "brands_attractions_and_activities_front": result["brands_attractions_and_activities_front"],
                                "brands_automotive_front": result["brands_automotive_front"],
                                "brands_eat_and_drink_front": result["brands_eat_and_drink_front"],
                                "brands_education_front": result["brands_education_front"],
                                "brands_financial_service_front": result["brands_financial_service_front"],
                                "brands_health_and_medical_front": result["brands_health_and_medical_front"],
                                "brands_public_service_and_government_front": result["brands_public_service_and_government_front"],
                                "brands_retail_front": result["brands_retail_front"],
                            },
                            "500" : {
                                "brands_active_life_500m": result["brands_active_life_500m"],
                                "brands_arts_and_entertainment_500m": result["brands_arts_and_entertainment_500m"],
                                "brands_attractions_and_activities_500m": result["brands_attractions_and_activities_500m"],
                                "brands_automotive_500m": result["brands_automotive_500m"],
                                "brands_eat_and_drink_500m": result["brands_eat_and_drink_500m"],
                                "brands_education_500m": result["brands_education_500m"],
                                "brands_financial_service_500m": result["brands_financial_service_500m"],
                                "brands_health_and_medical_500m": result["brands_health_and_medical_500m"],
                                "brands_public_service_and_government_500m": result["brands_public_service_and_government_500m"],
                                "brands_retail_500m": result["brands_retail_500m"],
                            },
                            "1000" : {
                                "brands_active_life_1km": result["brands_active_life_1km"],
                                "brands_arts_and_entertainment_1km": result["brands_arts_and_entertainment_1km"],
                                "brands_attractions_and_activities_1km": result["brands_attractions_and_activities_1km"],
                                "brands_automotive_1km": result["brands_automotive_1km"],
                                "brands_eat_and_drink_1km": result["brands_eat_and_drink_1km"],
                                "brands_education_1km": result["brands_education_1km"],
                                "brands_financial_service_1km": result["brands_financial_service_1km"],
                                "brands_health_and_medical_1km": result["brands_health_and_medical_1km"],
                                "brands_public_service_and_government_1km": result["brands_public_service_and_government_1km"],
                                "brands_retail_1km": result["brands_retail_1km"]
                            },  
                        }
                if show_all_keys:
                    # Handle traffic data for different cities
                    if city == 'queretaro' or city == 'el_marques':
                        traffic = {
                            "front": {
                                "at_rest_avg_x_hour_0_front": result.get("at_rest_avg_x_hour_0_front", None),
                                "pedestrian_avg_x_hour_0_front": result.get("pedestrian_avg_x_hour_0_front", None),
                                "motor_vehicle_avg_x_hour_0_front": result.get("motor_vehicle_avg_x_hour_0_front", None),
                                "at_rest_avg_x_hour_1_front": result.get("at_rest_avg_x_hour_1_front", None),
                                "pedestrian_avg_x_hour_1_front": result.get("pedestrian_avg_x_hour_1_front", None),
                                "motor_vehicle_avg_x_hour_1_front": result.get("motor_vehicle_avg_x_hour_1_front", None),
                                "at_rest_avg_x_hour_2_front": result.get("at_rest_avg_x_hour_2_front", None),
                                "pedestrian_avg_x_hour_2_front": result.get("pedestrian_avg_x_hour_2_front", None),
                                "motor_vehicle_avg_x_hour_2_front": result.get("motor_vehicle_avg_x_hour_2_front", None),
                                "at_rest_avg_x_hour_3_front": result.get("at_rest_avg_x_hour_3_front", None),
                                "pedestrian_avg_x_hour_3_front": result.get("pedestrian_avg_x_hour_3_front", None),
                                "motor_vehicle_avg_x_hour_3_front": result.get("motor_vehicle_avg_x_hour_3_front", None),
                                "at_rest_avg_x_hour_4_front": result.get("at_rest_avg_x_hour_4_front", None),
                                "pedestrian_avg_x_hour_4_front": result.get("pedestrian_avg_x_hour_4_front", None),
                                "motor_vehicle_avg_x_hour_4_front": result.get("motor_vehicle_avg_x_hour_4_front", None),
                                "at_rest_avg_x_hour_5_front": result.get("at_rest_avg_x_hour_5_front", None),
                                "pedestrian_avg_x_hour_5_front": result.get("pedestrian_avg_x_hour_5_front", None),
                                "motor_vehicle_avg_x_hour_5_front": result.get("motor_vehicle_avg_x_hour_5_front", None),
                                "at_rest_avg_x_hour_6_front": result.get("at_rest_avg_x_hour_6_front", None),
                                "pedestrian_avg_x_hour_6_front": result.get("pedestrian_avg_x_hour_6_front", None),
                                "motor_vehicle_avg_x_hour_6_front": result.get("motor_vehicle_avg_x_hour_6_front", None),
                                "at_rest_avg_x_hour_7_front": result.get("at_rest_avg_x_hour_7_front", None),
                                "pedestrian_avg_x_hour_7_front": result.get("pedestrian_avg_x_hour_7_front", None),
                                "motor_vehicle_avg_x_hour_7_front": result.get("motor_vehicle_avg_x_hour_7_front", None),
                                "at_rest_avg_x_hour_8_front": result.get("at_rest_avg_x_hour_8_front", None),
                                "pedestrian_avg_x_hour_8_front": result.get("pedestrian_avg_x_hour_8_front", None),
                                "motor_vehicle_avg_x_hour_8_front": result.get("motor_vehicle_avg_x_hour_8_front", None),
                                "at_rest_avg_x_hour_9_front": result.get("at_rest_avg_x_hour_9_front", None),
                                "pedestrian_avg_x_hour_9_front": result.get("pedestrian_avg_x_hour_9_front", None),
                                "motor_vehicle_avg_x_hour_9_front": result.get("motor_vehicle_avg_x_hour_9_front", None),
                                "at_rest_avg_x_hour_10_front": result.get("at_rest_avg_x_hour_10_front", None),
                                "pedestrian_avg_x_hour_10_front": result.get("pedestrian_avg_x_hour_10_front", None),
                                "motor_vehicle_avg_x_hour_10_front": result.get("motor_vehicle_avg_x_hour_10_front", None),
                                "at_rest_avg_x_hour_11_front": result.get("at_rest_avg_x_hour_11_front", None),
                                "pedestrian_avg_x_hour_11_front": result.get("pedestrian_avg_x_hour_11_front", None),
                                "motor_vehicle_avg_x_hour_11_front": result.get("motor_vehicle_avg_x_hour_11_front", None),
                                "at_rest_avg_x_hour_12_front": result.get("at_rest_avg_x_hour_12_front", None),
                                "pedestrian_avg_x_hour_12_front": result.get("pedestrian_avg_x_hour_12_front", None),
                                "motor_vehicle_avg_x_hour_12_front": result.get("motor_vehicle_avg_x_hour_12_front", None),
                                "at_rest_avg_x_hour_13_front": result.get("at_rest_avg_x_hour_13_front", None),
                                "pedestrian_avg_x_hour_13_front": result.get("pedestrian_avg_x_hour_13_front", None),
                                "motor_vehicle_avg_x_hour_13_front": result.get("motor_vehicle_avg_x_hour_13_front", None),
                                "at_rest_avg_x_hour_14_front": result.get("at_rest_avg_x_hour_14_front", None),
                                "pedestrian_avg_x_hour_14_front": result.get("pedestrian_avg_x_hour_14_front", None),
                                "motor_vehicle_avg_x_hour_14_front": result.get("motor_vehicle_avg_x_hour_14_front", None),
                                "at_rest_avg_x_hour_15_front": result.get("at_rest_avg_x_hour_15_front", None),
                                "pedestrian_avg_x_hour_15_front": result.get("pedestrian_avg_x_hour_15_front", None),
                                "motor_vehicle_avg_x_hour_15_front": result.get("motor_vehicle_avg_x_hour_15_front", None),
                                "at_rest_avg_x_hour_16_front": result.get("at_rest_avg_x_hour_16_front", None),
                                "pedestrian_avg_x_hour_16_front": result.get("pedestrian_avg_x_hour_16_front", None),
                                "motor_vehicle_avg_x_hour_16_front": result.get("motor_vehicle_avg_x_hour_16_front", None),
                                "at_rest_avg_x_hour_17_front": result.get("at_rest_avg_x_hour_17_front", None),
                                "pedestrian_avg_x_hour_17_front": result.get("pedestrian_avg_x_hour_17_front", None),
                                "motor_vehicle_avg_x_hour_17_front": result.get("motor_vehicle_avg_x_hour_17_front", None),
                                "at_rest_avg_x_hour_18_front": result.get("at_rest_avg_x_hour_18_front", None),
                                "pedestrian_avg_x_hour_18_front": result.get("pedestrian_avg_x_hour_18_front", None),
                                "motor_vehicle_avg_x_hour_18_front": result.get("motor_vehicle_avg_x_hour_18_front", None),
                                "at_rest_avg_x_hour_19_front": result.get("at_rest_avg_x_hour_19_front", None),
                                "pedestrian_avg_x_hour_19_front": result.get("pedestrian_avg_x_hour_19_front", None),
                                "motor_vehicle_avg_x_hour_19_front": result.get("motor_vehicle_avg_x_hour_19_front", None),
                                "at_rest_avg_x_hour_20_front": result.get("at_rest_avg_x_hour_20_front", None),
                                "pedestrian_avg_x_hour_20_front": result.get("pedestrian_avg_x_hour_20_front", None),
                                "motor_vehicle_avg_x_hour_20_front": result.get("motor_vehicle_avg_x_hour_20_front", None),
                                "at_rest_avg_x_hour_21_front": result.get("at_rest_avg_x_hour_21_front", None),
                                "pedestrian_avg_x_hour_21_front": result.get("pedestrian_avg_x_hour_21_front", None),
                                "motor_vehicle_avg_x_hour_21_front": result.get("motor_vehicle_avg_x_hour_21_front", None),
                                "at_rest_avg_x_hour_22_front": result.get("at_rest_avg_x_hour_22_front", None),
                                "pedestrian_avg_x_hour_22_front": result.get("pedestrian_avg_x_hour_22_front", None),
                                "motor_vehicle_avg_x_hour_22_front": result.get("motor_vehicle_avg_x_hour_22_front", None),
                                "at_rest_avg_x_hour_23_front": result.get("at_rest_avg_x_hour_23_front", None),
                                "pedestrian_avg_x_hour_23_front": result.get("pedestrian_avg_x_hour_23_front", None),
                                "motor_vehicle_avg_x_hour_23_front": result.get("motor_vehicle_avg_x_hour_23_front", None),
                                "at_rest_avg_x_day_of_week_1_front": result.get("at_rest_avg_x_day_of_week_1_front", None),
                                "pedestrian_avg_x_day_of_week_1_front": result.get("pedestrian_avg_x_day_of_week_1_front", None),
                                "motor_vehicle_avg_x_day_of_week_1_front": result.get("motor_vehicle_avg_x_day_of_week_1_front", None),
                                "at_rest_avg_x_day_of_week_2_front": result.get("at_rest_avg_x_day_of_week_2_front", None),
                                "pedestrian_avg_x_day_of_week_2_front": result.get("pedestrian_avg_x_day_of_week_2_front", None),
                                "motor_vehicle_avg_x_day_of_week_2_front": result.get("motor_vehicle_avg_x_day_of_week_2_front", None),
                                "at_rest_avg_x_day_of_week_3_front": result.get("at_rest_avg_x_day_of_week_3_front", None),
                                "pedestrian_avg_x_day_of_week_3_front": result.get("pedestrian_avg_x_day_of_week_3_front", None),
                                "motor_vehicle_avg_x_day_of_week_3_front": result.get("motor_vehicle_avg_x_day_of_week_3_front", None),
                                "at_rest_avg_x_day_of_week_4_front": result.get("at_rest_avg_x_day_of_week_4_front", None),
                                "pedestrian_avg_x_day_of_week_4_front": result.get("pedestrian_avg_x_day_of_week_4_front", None),
                                "motor_vehicle_avg_x_day_of_week_4_front": result.get("motor_vehicle_avg_x_day_of_week_4_front", None),
                                "at_rest_avg_x_day_of_week_5_front": result.get("at_rest_avg_x_day_of_week_5_front", None),
                                "pedestrian_avg_x_day_of_week_5_front": result.get("pedestrian_avg_x_day_of_week_5_front", None),
                                "motor_vehicle_avg_x_day_of_week_5_front": result.get("motor_vehicle_avg_x_day_of_week_5_front", None),
                                "at_rest_avg_x_day_of_week_6_front": result.get("at_rest_avg_x_day_of_week_6_front", None),
                                "pedestrian_avg_x_day_of_week_6_front": result.get("pedestrian_avg_x_day_of_week_6_front", None),
                                "motor_vehicle_avg_x_day_of_week_6_front": result.get("motor_vehicle_avg_x_day_of_week_6_front", None),
                                "at_rest_avg_x_day_of_week_7_front": result.get("at_rest_avg_x_day_of_week_7_front", None),
                                "pedestrian_avg_x_day_of_week_7_front": result.get("pedestrian_avg_x_day_of_week_7_front", None),
                                "motor_vehicle_avg_x_day_of_week_7_front": result.get("motor_vehicle_avg_x_day_of_week_7_front", None)
                            },
                            "500": {
                                "at_rest_avg_x_hour_0_500m": result.get("at_rest_avg_x_hour_0_500m", None),
                                "pedestrian_avg_x_hour_0_500m": result.get("pedestrian_avg_x_hour_0_500m", None),
                                "motor_vehicle_avg_x_hour_0_500m": result.get("motor_vehicle_avg_x_hour_0_500m", None),
                                "at_rest_avg_x_hour_1_500m": result.get("at_rest_avg_x_hour_1_500m", None),
                                "pedestrian_avg_x_hour_1_500m": result.get("pedestrian_avg_x_hour_1_500m", None),
                                "motor_vehicle_avg_x_hour_1_500m": result.get("motor_vehicle_avg_x_hour_1_500m", None),
                                "at_rest_avg_x_hour_2_500m": result.get("at_rest_avg_x_hour_2_500m", None),
                                "pedestrian_avg_x_hour_2_500m": result.get("pedestrian_avg_x_hour_2_500m", None),
                                "motor_vehicle_avg_x_hour_2_500m": result.get("motor_vehicle_avg_x_hour_2_500m", None),
                                "at_rest_avg_x_hour_3_500m": result.get("at_rest_avg_x_hour_3_500m", None),
                                "pedestrian_avg_x_hour_3_500m": result.get("pedestrian_avg_x_hour_3_500m", None),
                                "motor_vehicle_avg_x_hour_3_500m": result.get("motor_vehicle_avg_x_hour_3_500m", None),
                                "at_rest_avg_x_hour_4_500m": result.get("at_rest_avg_x_hour_4_500m", None),
                                "pedestrian_avg_x_hour_4_500m": result.get("pedestrian_avg_x_hour_4_500m", None),
                                "motor_vehicle_avg_x_hour_4_500m": result.get("motor_vehicle_avg_x_hour_4_500m", None),
                                "at_rest_avg_x_hour_5_500m": result.get("at_rest_avg_x_hour_5_500m", None),
                                "pedestrian_avg_x_hour_5_500m": result.get("pedestrian_avg_x_hour_5_500m", None),
                                "motor_vehicle_avg_x_hour_5_500m": result.get("motor_vehicle_avg_x_hour_5_500m", None),
                                "at_rest_avg_x_hour_6_500m": result.get("at_rest_avg_x_hour_6_500m", None),
                                "pedestrian_avg_x_hour_6_500m": result.get("pedestrian_avg_x_hour_6_500m", None),
                                "motor_vehicle_avg_x_hour_6_500m": result.get("motor_vehicle_avg_x_hour_6_500m", None),
                                "at_rest_avg_x_hour_7_500m": result.get("at_rest_avg_x_hour_7_500m", None),
                                "pedestrian_avg_x_hour_7_500m": result.get("pedestrian_avg_x_hour_7_500m", None),
                                "motor_vehicle_avg_x_hour_7_500m": result.get("motor_vehicle_avg_x_hour_7_500m", None),
                                "at_rest_avg_x_hour_8_500m": result.get("at_rest_avg_x_hour_8_500m", None),
                                "pedestrian_avg_x_hour_8_500m": result.get("pedestrian_avg_x_hour_8_500m", None),
                                "motor_vehicle_avg_x_hour_8_500m": result.get("motor_vehicle_avg_x_hour_8_500m", None),
                                "at_rest_avg_x_hour_9_500m": result.get("at_rest_avg_x_hour_9_500m", None),
                                "pedestrian_avg_x_hour_9_500m": result.get("pedestrian_avg_x_hour_9_500m", None),
                                "motor_vehicle_avg_x_hour_9_500m": result.get("motor_vehicle_avg_x_hour_9_500m", None),
                                "at_rest_avg_x_hour_10_500m": result.get("at_rest_avg_x_hour_10_500m", None),
                                "pedestrian_avg_x_hour_10_500m": result.get("pedestrian_avg_x_hour_10_500m", None),
                                "motor_vehicle_avg_x_hour_10_500m": result.get("motor_vehicle_avg_x_hour_10_500m", None),
                                "at_rest_avg_x_hour_11_500m": result.get("at_rest_avg_x_hour_11_500m", None),
                                "pedestrian_avg_x_hour_11_500m": result.get("pedestrian_avg_x_hour_11_500m", None),
                                "motor_vehicle_avg_x_hour_11_500m": result.get("motor_vehicle_avg_x_hour_11_500m", None),
                                "at_rest_avg_x_hour_12_500m": result.get("at_rest_avg_x_hour_12_500m", None),
                                "pedestrian_avg_x_hour_12_500m": result.get("pedestrian_avg_x_hour_12_500m", None),
                                "motor_vehicle_avg_x_hour_12_500m": result.get("motor_vehicle_avg_x_hour_12_500m", None),
                                "at_rest_avg_x_hour_13_500m": result.get("at_rest_avg_x_hour_13_500m", None),
                                "pedestrian_avg_x_hour_13_500m": result.get("pedestrian_avg_x_hour_13_500m", None),
                                "motor_vehicle_avg_x_hour_13_500m": result.get("motor_vehicle_avg_x_hour_13_500m", None),
                                "at_rest_avg_x_hour_14_500m": result.get("at_rest_avg_x_hour_14_500m", None),
                                "pedestrian_avg_x_hour_14_500m": result.get("pedestrian_avg_x_hour_14_500m", None),
                                "motor_vehicle_avg_x_hour_14_500m": result.get("motor_vehicle_avg_x_hour_14_500m", None),
                                "at_rest_avg_x_hour_15_500m": result.get("at_rest_avg_x_hour_15_500m", None),
                                "pedestrian_avg_x_hour_15_500m": result.get("pedestrian_avg_x_hour_15_500m", None),
                                "motor_vehicle_avg_x_hour_15_500m": result.get("motor_vehicle_avg_x_hour_15_500m", None),
                                "at_rest_avg_x_hour_16_500m": result.get("at_rest_avg_x_hour_16_500m", None),
                                "pedestrian_avg_x_hour_16_500m": result.get("pedestrian_avg_x_hour_16_500m", None),
                                "motor_vehicle_avg_x_hour_16_500m": result.get("motor_vehicle_avg_x_hour_16_500m", None),
                                "at_rest_avg_x_hour_17_500m": result.get("at_rest_avg_x_hour_17_500m", None),
                                "pedestrian_avg_x_hour_17_500m": result.get("pedestrian_avg_x_hour_17_500m", None),
                                "motor_vehicle_avg_x_hour_17_500m": result.get("motor_vehicle_avg_x_hour_17_500m", None),
                                "at_rest_avg_x_hour_18_500m": result.get("at_rest_avg_x_hour_18_500m", None),
                                "pedestrian_avg_x_hour_18_500m": result.get("pedestrian_avg_x_hour_18_500m", None),
                                "motor_vehicle_avg_x_hour_18_500m": result.get("motor_vehicle_avg_x_hour_18_500m", None),
                                "at_rest_avg_x_hour_19_500m": result.get("at_rest_avg_x_hour_19_500m", None),
                                "pedestrian_avg_x_hour_19_500m": result.get("pedestrian_avg_x_hour_19_500m", None),
                                "motor_vehicle_avg_x_hour_19_500m": result.get("motor_vehicle_avg_x_hour_19_500m", None),
                                "at_rest_avg_x_hour_20_500m": result.get("at_rest_avg_x_hour_20_500m", None),
                                "pedestrian_avg_x_hour_20_500m": result.get("pedestrian_avg_x_hour_20_500m", None),
                                "motor_vehicle_avg_x_hour_20_500m": result.get("motor_vehicle_avg_x_hour_20_500m", None),
                                "at_rest_avg_x_hour_21_500m": result.get("at_rest_avg_x_hour_21_500m", None),
                                "pedestrian_avg_x_hour_21_500m": result.get("pedestrian_avg_x_hour_21_500m", None),
                                "motor_vehicle_avg_x_hour_21_500m": result.get("motor_vehicle_avg_x_hour_21_500m", None),
                                "at_rest_avg_x_hour_22_500m": result.get("at_rest_avg_x_hour_22_500m", None),
                                "pedestrian_avg_x_hour_22_500m": result.get("pedestrian_avg_x_hour_22_500m", None),
                                "motor_vehicle_avg_x_hour_22_500m": result.get("motor_vehicle_avg_x_hour_22_500m", None),
                                "at_rest_avg_x_hour_23_500m": result.get("at_rest_avg_x_hour_23_500m", None),
                                "pedestrian_avg_x_hour_23_500m": result.get("pedestrian_avg_x_hour_23_500m", None),
                                "motor_vehicle_avg_x_hour_23_500m": result.get("motor_vehicle_avg_x_hour_23_500m", None),
                                "at_rest_avg_x_day_of_week_1_500m": result.get("at_rest_avg_x_day_of_week_1_500m", None),
                                "pedestrian_avg_x_day_of_week_1_500m": result.get("pedestrian_avg_x_day_of_week_1_500m", None),
                                "motor_vehicle_avg_x_day_of_week_1_500m": result.get("motor_vehicle_avg_x_day_of_week_1_500m", None),
                                "at_rest_avg_x_day_of_week_2_500m": result.get("at_rest_avg_x_day_of_week_2_500m", None),
                                "pedestrian_avg_x_day_of_week_2_500m": result.get("pedestrian_avg_x_day_of_week_2_500m", None),
                                "motor_vehicle_avg_x_day_of_week_2_500m": result.get("motor_vehicle_avg_x_day_of_week_2_500m", None),
                                "at_rest_avg_x_day_of_week_3_500m": result.get("at_rest_avg_x_day_of_week_3_500m", None),
                                "pedestrian_avg_x_day_of_week_3_500m": result.get("pedestrian_avg_x_day_of_week_3_500m", None),
                                "motor_vehicle_avg_x_day_of_week_3_500m": result.get("motor_vehicle_avg_x_day_of_week_3_500m", None),
                                "at_rest_avg_x_day_of_week_4_500m": result.get("at_rest_avg_x_day_of_week_4_500m", None),
                                "pedestrian_avg_x_day_of_week_4_500m": result.get("pedestrian_avg_x_day_of_week_4_500m", None),
                                "motor_vehicle_avg_x_day_of_week_4_500m": result.get("motor_vehicle_avg_x_day_of_week_4_500m", None),
                                "at_rest_avg_x_day_of_week_5_500m": result.get("at_rest_avg_x_day_of_week_5_500m", None),
                                "pedestrian_avg_x_day_of_week_5_500m": result.get("pedestrian_avg_x_day_of_week_5_500m", None),
                                "motor_vehicle_avg_x_day_of_week_5_500m": result.get("motor_vehicle_avg_x_day_of_week_5_500m", None),
                                "at_rest_avg_x_day_of_week_6_500m": result.get("at_rest_avg_x_day_of_week_6_500m", None),
                                "pedestrian_avg_x_day_of_week_6_500m": result.get("pedestrian_avg_x_day_of_week_6_500m", None),
                                "motor_vehicle_avg_x_day_of_week_6_500m": result.get("motor_vehicle_avg_x_day_of_week_6_500m", None),
                                "at_rest_avg_x_day_of_week_7_500m": result.get("at_rest_avg_x_day_of_week_7_500m", None),
                                "pedestrian_avg_x_day_of_week_7_500m": result.get("pedestrian_avg_x_day_of_week_7_500m", None),
                                "motor_vehicle_avg_x_day_of_week_7_500m": result.get("motor_vehicle_avg_x_day_of_week_7_500m", None)
                            },
                           "1000":{
                                "at_rest_avg_x_hour_0_1000m": result.get("at_rest_avg_x_hour_0_1km", None),
                                "pedestrian_avg_x_hour_0_1000m": result.get("pedestrian_avg_x_hour_0_1km", None),
                                "motor_vehicle_avg_x_hour_0_1000m": result.get("motor_vehicle_avg_x_hour_0_1km", None),
                                "at_rest_avg_x_hour_1_1000m": result.get("at_rest_avg_x_hour_1_1km", None),
                                "pedestrian_avg_x_hour_1_1000m": result.get("pedestrian_avg_x_hour_1_1km", None),
                                "motor_vehicle_avg_x_hour_1_1000m": result.get("motor_vehicle_avg_x_hour_1_1km", None),
                                "at_rest_avg_x_hour_2_1000m": result.get("at_rest_avg_x_hour_2_1km", None),   
                                "pedestrian_avg_x_hour_2_1000m": result.get("pedestrian_avg_x_hour_2_1km", None),
                                "motor_vehicle_avg_x_hour_2_1000m": result.get("motor_vehicle_avg_x_hour_2_1km", None),
                                "at_rest_avg_x_hour_3_1000m": result.get("at_rest_avg_x_hour_3_1km", None),
                                "pedestrian_avg_x_hour_3_1000m": result.get("pedestrian_avg_x_hour_3_1km", None),
                                "motor_vehicle_avg_x_hour_3_1000m": result.get("motor_vehicle_avg_x_hour_3_1km", None),
                                "at_rest_avg_x_hour_4_1000m": result.get("at_rest_avg_x_hour_4_1km", None),
                                "pedestrian_avg_x_hour_4_1000m": result.get("pedestrian_avg_x_hour_4_1km", None),
                                "motor_vehicle_avg_x_hour_4_1000m": result.get("motor_vehicle_avg_x_hour_4_1km", None),
                                "at_rest_avg_x_hour_5_1000m": result.get("at_rest_avg_x_hour_5_1km", None),
                                "pedestrian_avg_x_hour_5_1000m": result.get("pedestrian_avg_x_hour_5_1km", None),
                                "motor_vehicle_avg_x_hour_5_1000m": result.get("motor_vehicle_avg_x_hour_5_1km", None),
                                "at_rest_avg_x_hour_6_1000m": result.get("at_rest_avg_x_hour_6_1km", None),
                                "pedestrian_avg_x_hour_6_1000m": result.get("pedestrian_avg_x_hour_6_1km", None),
                                "motor_vehicle_avg_x_hour_6_1000m": result.get("motor_vehicle_avg_x_hour_6_1km", None),
                                "at_rest_avg_x_hour_7_1000m": result.get("at_rest_avg_x_hour_7_1km", None),
                                "pedestrian_avg_x_hour_7_1000m": result.get("pedestrian_avg_x_hour_7_1km", None),
                                "motor_vehicle_avg_x_hour_7_1000m": result.get("motor_vehicle_avg_x_hour_7_1km", None),
                                "at_rest_avg_x_hour_8_1000m": result.get("at_rest_avg_x_hour_8_1km", None),
                                "pedestrian_avg_x_hour_8_1000m": result.get("pedestrian_avg_x_hour_8_1km", None),
                                "motor_vehicle_avg_x_hour_8_1000m": result.get("motor_vehicle_avg_x_hour_8_1km", None),
                                "at_rest_avg_x_hour_9_1000m": result.get("at_rest_avg_x_hour_9_1km", None),
                                "pedestrian_avg_x_hour_9_1000m": result.get("pedestrian_avg_x_hour_9_1km", None),
                                "motor_vehicle_avg_x_hour_9_1000m": result.get("motor_vehicle_avg_x_hour_9_1km", None),
                                "at_rest_avg_x_hour_10_1000m": result.get("at_rest_avg_x_hour_10_1km", None),
                                "pedestrian_avg_x_hour_10_1000m": result.get("pedestrian_avg_x_hour_10_1km", None),
                                "motor_vehicle_avg_x_hour_10_1000m": result.get("motor_vehicle_avg_x_hour_10_1km", None),
                                "at_rest_avg_x_hour_11_1000m": result.get("at_rest_avg_x_hour_11_1km", None),
                                "pedestrian_avg_x_hour_11_1000m": result.get("pedestrian_avg_x_hour_11_1km", None),
                                "motor_vehicle_avg_x_hour_11_1000m": result.get("motor_vehicle_avg_x_hour_11_1km", None),
                                "at_rest_avg_x_hour_12_1000m": result.get("at_rest_avg_x_hour_12_1km", None),
                                "pedestrian_avg_x_hour_12_1000m": result.get("pedestrian_avg_x_hour_12_1km", None),
                                "motor_vehicle_avg_x_hour_12_1000m": result.get("motor_vehicle_avg_x_hour_12_1km", None),
                                "at_rest_avg_x_hour_13_1000m": result.get("at_rest_avg_x_hour_13_1km", None),
                                "pedestrian_avg_x_hour_13_1000m": result.get("pedestrian_avg_x_hour_13_1km", None),
                                "motor_vehicle_avg_x_hour_13_1000m": result.get("motor_vehicle_avg_x_hour_13_1km", None),
                                "at_rest_avg_x_hour_14_1000m": result.get("at_rest_avg_x_hour_14_1km", None),
                                "pedestrian_avg_x_hour_14_1000m": result.get("pedestrian_avg_x_hour_14_1km", None),
                                "motor_vehicle_avg_x_hour_14_1000m": result.get("motor_vehicle_avg_x_hour_14_1km", None),
                                "at_rest_avg_x_hour_15_1000m": result.get("at_rest_avg_x_hour_15_1km", None),
                                "pedestrian_avg_x_hour_15_1000m": result.get("pedestrian_avg_x_hour_15_1km", None),
                                "motor_vehicle_avg_x_hour_15_1000m": result.get("motor_vehicle_avg_x_hour_15_1km", None),
                                "at_rest_avg_x_hour_16_1000m": result.get("at_rest_avg_x_hour_16_1km", None),
                                "pedestrian_avg_x_hour_16_1000m": result.get("pedestrian_avg_x_hour_16_1km", None),
                                "motor_vehicle_avg_x_hour_16_1000m": result.get("motor_vehicle_avg_x_hour_16_1km", None),
                                "at_rest_avg_x_hour_17_1000m": result.get("at_rest_avg_x_hour_17_1km", None),
                                "pedestrian_avg_x_hour_17_1000m": result.get("pedestrian_avg_x_hour_17_1km", None),
                                "motor_vehicle_avg_x_hour_17_1000m": result.get("motor_vehicle_avg_x_hour_17_1km", None),
                                "at_rest_avg_x_hour_18_1000m": result.get("at_rest_avg_x_hour_18_1km", None),
                                "pedestrian_avg_x_hour_18_1000m": result.get("pedestrian_avg_x_hour_18_1km", None),
                                "motor_vehicle_avg_x_hour_18_1000m": result.get("motor_vehicle_avg_x_hour_18_1km", None),
                                "at_rest_avg_x_hour_19_1000m": result.get("at_rest_avg_x_hour_19_1km", None),
                                "pedestrian_avg_x_hour_19_1000m": result.get("pedestrian_avg_x_hour_19_1km", None),
                                "motor_vehicle_avg_x_hour_19_1000m": result.get("motor_vehicle_avg_x_hour_19_1km", None),
                                "at_rest_avg_x_hour_20_1000m": result.get("at_rest_avg_x_hour_20_1km", None),
                                "pedestrian_avg_x_hour_20_1000m": result.get("pedestrian_avg_x_hour_20_1km", None),
                                "motor_vehicle_avg_x_hour_20_1000m": result.get("motor_vehicle_avg_x_hour_20_1km", None),
                                "at_rest_avg_x_hour_21_1000m": result.get("at_rest_avg_x_hour_21_1km", None),
                                "pedestrian_avg_x_hour_21_1000m": result.get("pedestrian_avg_x_hour_21_1km", None),
                                "motor_vehicle_avg_x_hour_21_1000m": result.get("motor_vehicle_avg_x_hour_21_1km", None),
                                "at_rest_avg_x_hour_22_1000m": result.get("at_rest_avg_x_hour_22_1km", None),
                                "pedestrian_avg_x_hour_22_1000m": result.get("pedestrian_avg_x_hour_22_1km", None),
                                "motor_vehicle_avg_x_hour_22_1000m": result.get("motor_vehicle_avg_x_hour_22_1km", None),
                                "at_rest_avg_x_hour_23_1000m": result.get("at_rest_avg_x_hour_23_1km", None),
                                "pedestrian_avg_x_hour_23_1000m": result.get("pedestrian_avg_x_hour_23_1km", None),
                                "motor_vehicle_avg_x_hour_23_1000m": result.get("motor_vehicle_avg_x_hour_23_1km", None),
                                "at_rest_avg_x_day_of_week_1_1000m": result.get("at_rest_avg_x_day_of_week_1_1km", None),
                                "pedestrian_avg_x_day_of_week_1_1000m": result.get("pedestrian_avg_x_day_of_week_1_1km", None),
                                "motor_vehicle_avg_x_day_of_week_1_1000m": result.get("motor_vehicle_avg_x_day_of_week_1_1km", None),
                                "at_rest_avg_x_day_of_week_2_1000m": result.get("at_rest_avg_x_day_of_week_2_1km", None),
                                "pedestrian_avg_x_day_of_week_2_1000m": result.get("pedestrian_avg_x_day_of_week_2_1km", None),
                                "motor_vehicle_avg_x_day_of_week_2_1000m": result.get("motor_vehicle_avg_x_day_of_week_2_1km", None),
                                "at_rest_avg_x_day_of_week_3_1000m": result.get("at_rest_avg_x_day_of_week_3_1km", None),
                                "pedestrian_avg_x_day_of_week_3_1000m": result.get("pedestrian_avg_x_day_of_week_3_1km", None),
                                "motor_vehicle_avg_x_day_of_week_3_1000m": result.get("motor_vehicle_avg_x_day_of_week_3_1km", None),
                                "at_rest_avg_x_day_of_week_4_1000m": result.get("at_rest_avg_x_day_of_week_4_1km", None),
                                "pedestrian_avg_x_day_of_week_4_1000m": result.get("pedestrian_avg_x_day_of_week_4_1km", None),
                                "motor_vehicle_avg_x_day_of_week_4_1000m": result.get("motor_vehicle_avg_x_day_of_week_4_1km", None),
                                "at_rest_avg_x_day_of_week_5_1000m": result.get("at_rest_avg_x_day_of_week_5_1km", None),
                                "pedestrian_avg_x_day_of_week_5_1000m": result.get("pedestrian_avg_x_day_of_week_5_1km", None),
                                "motor_vehicle_avg_x_day_of_week_5_1000m": result.get("motor_vehicle_avg_x_day_of_week_5_1km", None),
                                "at_rest_avg_x_day_of_week_6_1000m": result.get("at_rest_avg_x_day_of_week_6_1km", None),
                                "pedestrian_avg_x_day_of_week_6_1000m": result.get("pedestrian_avg_x_day_of_week_6_1km", None),
                                "motor_vehicle_avg_x_day_of_week_6_1000m": result.get("motor_vehicle_avg_x_day_of_week_6_1km", None),
                                "at_rest_avg_x_day_of_week_7_1000m": result.get("at_rest_avg_x_day_of_week_7_1km", None),
                                "pedestrian_avg_x_day_of_week_7_1000m": result.get("pedestrian_avg_x_day_of_week_7_1km", None),
                                "motor_vehicle_avg_x_day_of_week_7_1000m": result.get("motor_vehicle_avg_x_day_of_week_7_1km", None)
                            }

                        }
                    else:
                        # CDMX structure (original complete traffic data) - use direct access like original
                        traffic = {
                            "front": {
                                "at_rest_avg_x_hour_0_front": result["at_rest_avg_x_hour_0_front"],
                                "pedestrian_avg_x_hour_0_front": result["pedestrian_avg_x_hour_0_front"],
                                "motor_vehicle_avg_x_hour_0_front": result["motor_vehicle_avg_x_hour_0_front"],
                                "at_rest_avg_x_hour_1_front": result["at_rest_avg_x_hour_1_front"],
                                "pedestrian_avg_x_hour_1_front": result["pedestrian_avg_x_hour_1_front"],
                                "motor_vehicle_avg_x_hour_1_front": result["motor_vehicle_avg_x_hour_1_front"],
                                "at_rest_avg_x_hour_2_front": result["at_rest_avg_x_hour_2_front"],
                                "pedestrian_avg_x_hour_2_front": result["pedestrian_avg_x_hour_2_front"],
                                "motor_vehicle_avg_x_hour_2_front": result["motor_vehicle_avg_x_hour_2_front"],
                                "at_rest_avg_x_hour_3_front": result["at_rest_avg_x_hour_3_front"],
                                "pedestrian_avg_x_hour_3_front": result["pedestrian_avg_x_hour_3_front"],
                                "motor_vehicle_avg_x_hour_3_front": result["motor_vehicle_avg_x_hour_3_front"],
                                "at_rest_avg_x_hour_4_front": result["at_rest_avg_x_hour_4_front"],
                                "pedestrian_avg_x_hour_4_front": result["pedestrian_avg_x_hour_4_front"],
                                "motor_vehicle_avg_x_hour_4_front": result["motor_vehicle_avg_x_hour_4_front"],
                                "at_rest_avg_x_hour_5_front": result["at_rest_avg_x_hour_5_front"],
                                "pedestrian_avg_x_hour_5_front": result["pedestrian_avg_x_hour_5_front"],
                                "motor_vehicle_avg_x_hour_5_front": result["motor_vehicle_avg_x_hour_5_front"],
                                "at_rest_avg_x_hour_6_front": result["at_rest_avg_x_hour_6_front"],
                                "pedestrian_avg_x_hour_6_front": result["pedestrian_avg_x_hour_6_front"],
                                "motor_vehicle_avg_x_hour_6_front": result["motor_vehicle_avg_x_hour_6_front"],
                                "at_rest_avg_x_hour_7_front": result["at_rest_avg_x_hour_7_front"],
                                "pedestrian_avg_x_hour_7_front": result["pedestrian_avg_x_hour_7_front"],
                                "motor_vehicle_avg_x_hour_7_front": result["motor_vehicle_avg_x_hour_7_front"],
                                "at_rest_avg_x_hour_8_front": result["at_rest_avg_x_hour_8_front"],
                                "pedestrian_avg_x_hour_8_front": result["pedestrian_avg_x_hour_8_front"],
                                "motor_vehicle_avg_x_hour_8_front": result["motor_vehicle_avg_x_hour_8_front"],
                                "at_rest_avg_x_hour_9_front": result["at_rest_avg_x_hour_9_front"],
                                "pedestrian_avg_x_hour_9_front": result["pedestrian_avg_x_hour_9_front"],
                                "motor_vehicle_avg_x_hour_9_front": result["motor_vehicle_avg_x_hour_9_front"],
                                "at_rest_avg_x_hour_10_front": result["at_rest_avg_x_hour_10_front"],
                                "pedestrian_avg_x_hour_10_front": result["pedestrian_avg_x_hour_10_front"],
                                "motor_vehicle_avg_x_hour_10_front": result["motor_vehicle_avg_x_hour_10_front"],
                                "at_rest_avg_x_hour_11_front": result["at_rest_avg_x_hour_11_front"],
                                "pedestrian_avg_x_hour_11_front": result["pedestrian_avg_x_hour_11_front"],
                                "motor_vehicle_avg_x_hour_11_front": result["motor_vehicle_avg_x_hour_11_front"],
                                "at_rest_avg_x_hour_12_front": result["at_rest_avg_x_hour_12_front"],
                                "pedestrian_avg_x_hour_12_front": result["pedestrian_avg_x_hour_12_front"],
                                "motor_vehicle_avg_x_hour_12_front": result["motor_vehicle_avg_x_hour_12_front"],
                                "at_rest_avg_x_hour_13_front": result["at_rest_avg_x_hour_13_front"],
                                "pedestrian_avg_x_hour_13_front": result["pedestrian_avg_x_hour_13_front"],
                                "motor_vehicle_avg_x_hour_13_front": result["motor_vehicle_avg_x_hour_13_front"],
                                "at_rest_avg_x_hour_14_front": result["at_rest_avg_x_hour_14_front"],
                                "pedestrian_avg_x_hour_14_front": result["pedestrian_avg_x_hour_14_front"],
                                "motor_vehicle_avg_x_hour_14_front": result["motor_vehicle_avg_x_hour_14_front"],
                                "at_rest_avg_x_hour_15_front": result["at_rest_avg_x_hour_15_front"],
                                "pedestrian_avg_x_hour_15_front": result["pedestrian_avg_x_hour_15_front"],
                                "motor_vehicle_avg_x_hour_15_front": result["motor_vehicle_avg_x_hour_15_front"],
                                "at_rest_avg_x_hour_16_front": result["at_rest_avg_x_hour_16_front"],
                                "pedestrian_avg_x_hour_16_front": result["pedestrian_avg_x_hour_16_front"],
                                "motor_vehicle_avg_x_hour_16_front": result["motor_vehicle_avg_x_hour_16_front"],
                                "at_rest_avg_x_hour_17_front": result["at_rest_avg_x_hour_17_front"],
                                "pedestrian_avg_x_hour_17_front": result["pedestrian_avg_x_hour_17_front"],
                                "motor_vehicle_avg_x_hour_17_front": result["motor_vehicle_avg_x_hour_17_front"],
                                "at_rest_avg_x_hour_18_front": result["at_rest_avg_x_hour_18_front"],
                                "pedestrian_avg_x_hour_18_front": result["pedestrian_avg_x_hour_18_front"],
                                "motor_vehicle_avg_x_hour_18_front": result["motor_vehicle_avg_x_hour_18_front"],
                                "at_rest_avg_x_hour_19_front": result["at_rest_avg_x_hour_19_front"],
                                "pedestrian_avg_x_hour_19_front": result["pedestrian_avg_x_hour_19_front"],
                                "motor_vehicle_avg_x_hour_19_front": result["motor_vehicle_avg_x_hour_19_front"],
                                "at_rest_avg_x_hour_20_front": result["at_rest_avg_x_hour_20_front"],
                                "pedestrian_avg_x_hour_20_front": result["pedestrian_avg_x_hour_20_front"],
                                "motor_vehicle_avg_x_hour_20_front": result["motor_vehicle_avg_x_hour_20_front"],
                                "at_rest_avg_x_hour_21_front": result["at_rest_avg_x_hour_21_front"],
                                "pedestrian_avg_x_hour_21_front": result["pedestrian_avg_x_hour_21_front"],
                                "motor_vehicle_avg_x_hour_21_front": result["motor_vehicle_avg_x_hour_21_front"],
                                "at_rest_avg_x_hour_22_front": result["at_rest_avg_x_hour_22_front"],
                                "pedestrian_avg_x_hour_22_front": result["pedestrian_avg_x_hour_22_front"],
                                "motor_vehicle_avg_x_hour_22_front": result["motor_vehicle_avg_x_hour_22_front"],
                                "at_rest_avg_x_hour_23_front": result["at_rest_avg_x_hour_23_front"],
                                "pedestrian_avg_x_hour_23_front": result["pedestrian_avg_x_hour_23_front"],
                                "motor_vehicle_avg_x_hour_23_front": result["motor_vehicle_avg_x_hour_23_front"],
                                "at_rest_avg_x_day_of_week_1_front": result["at_rest_avg_x_day_of_week_1_front"],
                                "pedestrian_avg_x_day_of_week_1_front": result["pedestrian_avg_x_day_of_week_1_front"],
                                "motor_vehicle_avg_x_day_of_week_1_front": result["motor_vehicle_avg_x_day_of_week_1_front"],
                                "at_rest_avg_x_day_of_week_2_front": result["at_rest_avg_x_day_of_week_2_front"],
                                "pedestrian_avg_x_day_of_week_2_front": result["pedestrian_avg_x_day_of_week_2_front"],
                                "motor_vehicle_avg_x_day_of_week_2_front": result["motor_vehicle_avg_x_day_of_week_2_front"],
                                "at_rest_avg_x_day_of_week_3_front": result["at_rest_avg_x_day_of_week_3_front"],
                                "pedestrian_avg_x_day_of_week_3_front": result["pedestrian_avg_x_day_of_week_3_front"],
                                "motor_vehicle_avg_x_day_of_week_3_front": result["motor_vehicle_avg_x_day_of_week_3_front"],
                                "at_rest_avg_x_day_of_week_4_front": result["at_rest_avg_x_day_of_week_4_front"],
                                "pedestrian_avg_x_day_of_week_4_front": result["pedestrian_avg_x_day_of_week_4_front"],
                                "motor_vehicle_avg_x_day_of_week_4_front": result["motor_vehicle_avg_x_day_of_week_4_front"],
                                "at_rest_avg_x_day_of_week_5_front": result["at_rest_avg_x_day_of_week_5_front"],
                                "pedestrian_avg_x_day_of_week_5_front": result["pedestrian_avg_x_day_of_week_5_front"],
                                "motor_vehicle_avg_x_day_of_week_5_front": result["motor_vehicle_avg_x_day_of_week_5_front"],
                                "at_rest_avg_x_day_of_week_6_front": result["at_rest_avg_x_day_of_week_6_front"],
                                "pedestrian_avg_x_day_of_week_6_front": result["pedestrian_avg_x_day_of_week_6_front"],
                                "motor_vehicle_avg_x_day_of_week_6_front": result["motor_vehicle_avg_x_day_of_week_6_front"],
                                "at_rest_avg_x_day_of_week_7_front": result["at_rest_avg_x_day_of_week_7_front"],
                                "pedestrian_avg_x_day_of_week_7_front": result["pedestrian_avg_x_day_of_week_7_front"],
                                "motor_vehicle_avg_x_day_of_week_7_front": result["motor_vehicle_avg_x_day_of_week_7_front"]
                            },
                            "500": {
                                "at_rest_avg_x_day_of_week_1_500m": result["at_rest_avg_x_day_of_week_1_500m"],
                                "pedestrian_avg_x_day_of_week_1_500m": result["pedestrian_avg_x_day_of_week_1_500m"],
                                "motor_vehicle_avg_x_day_of_week_1_500m": result["motor_vehicle_avg_x_day_of_week_1_500m"],
                                "at_rest_avg_x_day_of_week_2_500m": result["at_rest_avg_x_day_of_week_2_500m"],
                                "pedestrian_avg_x_day_of_week_2_500m": result["pedestrian_avg_x_day_of_week_2_500m"],
                                "motor_vehicle_avg_x_day_of_week_2_500m": result["motor_vehicle_avg_x_day_of_week_2_500m"],
                                "at_rest_avg_x_day_of_week_3_500m": result["at_rest_avg_x_day_of_week_3_500m"],
                                "pedestrian_avg_x_day_of_week_3_500m": result["pedestrian_avg_x_day_of_week_3_500m"],
                                "motor_vehicle_avg_x_day_of_week_3_500m": result["motor_vehicle_avg_x_day_of_week_3_500m"],
                                "at_rest_avg_x_day_of_week_4_500m": result["at_rest_avg_x_day_of_week_4_500m"],
                                "pedestrian_avg_x_day_of_week_4_500m": result["pedestrian_avg_x_day_of_week_4_500m"],
                                "motor_vehicle_avg_x_day_of_week_4_500m": result["motor_vehicle_avg_x_day_of_week_4_500m"],
                                "at_rest_avg_x_day_of_week_5_500m": result["at_rest_avg_x_day_of_week_5_500m"],
                                "pedestrian_avg_x_day_of_week_5_500m": result["pedestrian_avg_x_day_of_week_5_500m"],
                                "motor_vehicle_avg_x_day_of_week_5_500m": result["motor_vehicle_avg_x_day_of_week_5_500m"],
                                "at_rest_avg_x_day_of_week_6_500m": result["at_rest_avg_x_day_of_week_6_500m"],
                                "pedestrian_avg_x_day_of_week_6_500m": result["pedestrian_avg_x_day_of_week_6_500m"],
                                "motor_vehicle_avg_x_day_of_week_6_500m": result["motor_vehicle_avg_x_day_of_week_6_500m"],
                                "at_rest_avg_x_day_of_week_7_500m": result["at_rest_avg_x_day_of_week_7_500m"],
                                "pedestrian_avg_x_day_of_week_7_500m": result["pedestrian_avg_x_day_of_week_7_500m"],
                                "motor_vehicle_avg_x_day_of_week_7_500m": result["motor_vehicle_avg_x_day_of_week_7_500m"],
                                "at_rest_avg_x_hour_0_500m": result["at_rest_avg_x_hour_0_500m"],
                                "pedestrian_avg_x_hour_0_500m": result["pedestrian_avg_x_hour_0_500m"],
                                "motor_vehicle_avg_x_hour_0_500m": result["motor_vehicle_avg_x_hour_0_500m"],
                                "at_rest_avg_x_hour_1_500m": result["at_rest_avg_x_hour_1_500m"],
                                "pedestrian_avg_x_hour_1_500m": result["pedestrian_avg_x_hour_1_500m"],
                                "motor_vehicle_avg_x_hour_1_500m": result["motor_vehicle_avg_x_hour_1_500m"],
                                "at_rest_avg_x_hour_2_500m": result["at_rest_avg_x_hour_2_500m"],
                                "pedestrian_avg_x_hour_2_500m": result["pedestrian_avg_x_hour_2_500m"],
                                "motor_vehicle_avg_x_hour_2_500m": result["motor_vehicle_avg_x_hour_2_500m"],
                                "at_rest_avg_x_hour_3_500m": result["at_rest_avg_x_hour_3_500m"],
                                "pedestrian_avg_x_hour_3_500m": result["pedestrian_avg_x_hour_3_500m"],
                                "motor_vehicle_avg_x_hour_3_500m": result["motor_vehicle_avg_x_hour_3_500m"],
                                "at_rest_avg_x_hour_4_500m": result["at_rest_avg_x_hour_4_500m"],
                                "pedestrian_avg_x_hour_4_500m": result["pedestrian_avg_x_hour_4_500m"],
                                "motor_vehicle_avg_x_hour_4_500m": result["motor_vehicle_avg_x_hour_4_500m"],
                                "at_rest_avg_x_hour_5_500m": result["at_rest_avg_x_hour_5_500m"],
                                "pedestrian_avg_x_hour_5_500m": result["pedestrian_avg_x_hour_5_500m"],
                                "motor_vehicle_avg_x_hour_5_500m": result["motor_vehicle_avg_x_hour_5_500m"],
                                "at_rest_avg_x_hour_6_500m": result["at_rest_avg_x_hour_6_500m"],
                                "pedestrian_avg_x_hour_6_500m": result["pedestrian_avg_x_hour_6_500m"],
                                "motor_vehicle_avg_x_hour_6_500m": result["motor_vehicle_avg_x_hour_6_500m"],
                                "at_rest_avg_x_hour_7_500m": result["at_rest_avg_x_hour_7_500m"],
                                "pedestrian_avg_x_hour_7_500m": result["pedestrian_avg_x_hour_7_500m"],
                                "motor_vehicle_avg_x_hour_7_500m": result["motor_vehicle_avg_x_hour_7_500m"],
                                "at_rest_avg_x_hour_8_500m": result["at_rest_avg_x_hour_8_500m"],
                                "pedestrian_avg_x_hour_8_500m": result["pedestrian_avg_x_hour_8_500m"],
                                "motor_vehicle_avg_x_hour_8_500m": result["motor_vehicle_avg_x_hour_8_500m"],
                                "at_rest_avg_x_hour_9_500m": result["at_rest_avg_x_hour_9_500m"],
                                "pedestrian_avg_x_hour_9_500m": result["pedestrian_avg_x_hour_9_500m"],
                                "motor_vehicle_avg_x_hour_9_500m": result["motor_vehicle_avg_x_hour_9_500m"],
                                "at_rest_avg_x_hour_10_500m": result["at_rest_avg_x_hour_10_500m"],
                                "pedestrian_avg_x_hour_10_500m": result["pedestrian_avg_x_hour_10_500m"],
                                "motor_vehicle_avg_x_hour_10_500m": result["motor_vehicle_avg_x_hour_10_500m"],
                                "at_rest_avg_x_hour_11_500m": result["at_rest_avg_x_hour_11_500m"],
                                "pedestrian_avg_x_hour_11_500m": result["pedestrian_avg_x_hour_11_500m"],
                                "motor_vehicle_avg_x_hour_11_500m": result["motor_vehicle_avg_x_hour_11_500m"],
                                "at_rest_avg_x_hour_12_500m": result["at_rest_avg_x_hour_12_500m"],
                                "pedestrian_avg_x_hour_12_500m": result["pedestrian_avg_x_hour_12_500m"],
                                "motor_vehicle_avg_x_hour_12_500m": result["motor_vehicle_avg_x_hour_12_500m"],
                                "at_rest_avg_x_hour_13_500m": result["at_rest_avg_x_hour_13_500m"],
                                "pedestrian_avg_x_hour_13_500m": result["pedestrian_avg_x_hour_13_500m"],
                                "motor_vehicle_avg_x_hour_13_500m": result["motor_vehicle_avg_x_hour_13_500m"],
                                "at_rest_avg_x_hour_14_500m": result["at_rest_avg_x_hour_14_500m"],
                                "pedestrian_avg_x_hour_14_500m": result["pedestrian_avg_x_hour_14_500m"],
                                "motor_vehicle_avg_x_hour_14_500m": result["motor_vehicle_avg_x_hour_14_500m"],
                                "at_rest_avg_x_hour_15_500m": result["at_rest_avg_x_hour_15_500m"],
                                "pedestrian_avg_x_hour_15_500m": result["pedestrian_avg_x_hour_15_500m"],
                                "motor_vehicle_avg_x_hour_15_500m": result["motor_vehicle_avg_x_hour_15_500m"],
                                "at_rest_avg_x_hour_16_500m": result["at_rest_avg_x_hour_16_500m"],
                                "pedestrian_avg_x_hour_16_500m": result["pedestrian_avg_x_hour_16_500m"],
                                "motor_vehicle_avg_x_hour_16_500m": result["motor_vehicle_avg_x_hour_16_500m"],
                                "at_rest_avg_x_hour_17_500m": result["at_rest_avg_x_hour_17_500m"],
                                "pedestrian_avg_x_hour_17_500m": result["pedestrian_avg_x_hour_17_500m"],
                                "motor_vehicle_avg_x_hour_17_500m": result["motor_vehicle_avg_x_hour_17_500m"],
                                "at_rest_avg_x_hour_18_500m": result["at_rest_avg_x_hour_18_500m"],
                                "pedestrian_avg_x_hour_18_500m": result["pedestrian_avg_x_hour_18_500m"],
                                "motor_vehicle_avg_x_hour_18_500m": result["motor_vehicle_avg_x_hour_18_500m"],
                                "at_rest_avg_x_hour_19_500m": result["at_rest_avg_x_hour_19_500m"],
                                "pedestrian_avg_x_hour_19_500m": result["pedestrian_avg_x_hour_19_500m"],
                                "motor_vehicle_avg_x_hour_19_500m": result["motor_vehicle_avg_x_hour_19_500m"],
                                "at_rest_avg_x_hour_20_500m": result["at_rest_avg_x_hour_20_500m"],
                                "pedestrian_avg_x_hour_20_500m": result["pedestrian_avg_x_hour_20_500m"],
                                "motor_vehicle_avg_x_hour_20_500m": result["motor_vehicle_avg_x_hour_20_500m"],
                                "at_rest_avg_x_hour_21_500m": result["at_rest_avg_x_hour_21_500m"],
                                "pedestrian_avg_x_hour_21_500m": result["pedestrian_avg_x_hour_21_500m"],
                                "motor_vehicle_avg_x_hour_21_500m": result["motor_vehicle_avg_x_hour_21_500m"],
                                "at_rest_avg_x_hour_22_500m": result["at_rest_avg_x_hour_22_500m"],
                                "pedestrian_avg_x_hour_22_500m": result["pedestrian_avg_x_hour_22_500m"],
                                "motor_vehicle_avg_x_hour_22_500m": result["motor_vehicle_avg_x_hour_22_500m"],
                                "at_rest_avg_x_hour_23_500m": result["at_rest_avg_x_hour_23_500m"],
                                "pedestrian_avg_x_hour_23_500m": result["pedestrian_avg_x_hour_23_500m"],
                                "motor_vehicle_avg_x_hour_23_500m": result["motor_vehicle_avg_x_hour_23_500m"]
                            },
                            "1000":{
                                "at_rest_avg_x_hour_0_1000m": result.get("at_rest_avg_x_hour_0_1km", None),
                                "pedestrian_avg_x_hour_0_1000m": result.get("pedestrian_avg_x_hour_0_1km", None),
                                "motor_vehicle_avg_x_hour_0_1000m": result.get("motor_vehicle_avg_x_hour_0_1km", None),
                                "at_rest_avg_x_hour_1_1000m": result.get("at_rest_avg_x_hour_1_1km", None),
                                "pedestrian_avg_x_hour_1_1000m": result.get("pedestrian_avg_x_hour_1_1km", None),
                                "motor_vehicle_avg_x_hour_1_1000m": result.get("motor_vehicle_avg_x_hour_1_1km", None),
                                "at_rest_avg_x_hour_2_1000m": result.get("at_rest_avg_x_hour_2_1km", None),   
                                "pedestrian_avg_x_hour_2_1000m": result.get("pedestrian_avg_x_hour_2_1km", None),
                                "motor_vehicle_avg_x_hour_2_1000m": result.get("motor_vehicle_avg_x_hour_2_1km", None),
                                "at_rest_avg_x_hour_3_1000m": result.get("at_rest_avg_x_hour_3_1km", None),
                                "pedestrian_avg_x_hour_3_1000m": result.get("pedestrian_avg_x_hour_3_1km", None),
                                "motor_vehicle_avg_x_hour_3_1000m": result.get("motor_vehicle_avg_x_hour_3_1km", None),
                                "at_rest_avg_x_hour_4_1000m": result.get("at_rest_avg_x_hour_4_1km", None),
                                "pedestrian_avg_x_hour_4_1000m": result.get("pedestrian_avg_x_hour_4_1km", None),
                                "motor_vehicle_avg_x_hour_4_1000m": result.get("motor_vehicle_avg_x_hour_4_1km", None),
                                "at_rest_avg_x_hour_5_1000m": result.get("at_rest_avg_x_hour_5_1km", None),
                                "pedestrian_avg_x_hour_5_1000m": result.get("pedestrian_avg_x_hour_5_1km", None),
                                "motor_vehicle_avg_x_hour_5_1000m": result.get("motor_vehicle_avg_x_hour_5_1km", None),
                                "at_rest_avg_x_hour_6_1000m": result.get("at_rest_avg_x_hour_6_1km", None),
                                "pedestrian_avg_x_hour_6_1000m": result.get("pedestrian_avg_x_hour_6_1km", None),
                                "motor_vehicle_avg_x_hour_6_1000m": result.get("motor_vehicle_avg_x_hour_6_1km", None),
                                "at_rest_avg_x_hour_7_1000m": result.get("at_rest_avg_x_hour_7_1km", None),
                                "pedestrian_avg_x_hour_7_1000m": result.get("pedestrian_avg_x_hour_7_1km", None),
                                "motor_vehicle_avg_x_hour_7_1000m": result.get("motor_vehicle_avg_x_hour_7_1km", None),
                                "at_rest_avg_x_hour_8_1000m": result.get("at_rest_avg_x_hour_8_1km", None),
                                "pedestrian_avg_x_hour_8_1000m": result.get("pedestrian_avg_x_hour_8_1km", None),
                                "motor_vehicle_avg_x_hour_8_1000m": result.get("motor_vehicle_avg_x_hour_8_1km", None),
                                "at_rest_avg_x_hour_9_1000m": result.get("at_rest_avg_x_hour_9_1km", None),
                                "pedestrian_avg_x_hour_9_1000m": result.get("pedestrian_avg_x_hour_9_1km", None),
                                "motor_vehicle_avg_x_hour_9_1000m": result.get("motor_vehicle_avg_x_hour_9_1km", None),
                                "at_rest_avg_x_hour_10_1000m": result.get("at_rest_avg_x_hour_10_1km", None),
                                "pedestrian_avg_x_hour_10_1000m": result.get("pedestrian_avg_x_hour_10_1km", None),
                                "motor_vehicle_avg_x_hour_10_1000m": result.get("motor_vehicle_avg_x_hour_10_1km", None),
                                "at_rest_avg_x_hour_11_1000m": result.get("at_rest_avg_x_hour_11_1km", None),
                                "pedestrian_avg_x_hour_11_1000m": result.get("pedestrian_avg_x_hour_11_1km", None),
                                "motor_vehicle_avg_x_hour_11_1000m": result.get("motor_vehicle_avg_x_hour_11_1km", None),
                                "at_rest_avg_x_hour_12_1000m": result.get("at_rest_avg_x_hour_12_1km", None),
                                "pedestrian_avg_x_hour_12_1000m": result.get("pedestrian_avg_x_hour_12_1km", None),
                                "motor_vehicle_avg_x_hour_12_1000m": result.get("motor_vehicle_avg_x_hour_12_1km", None),
                                "at_rest_avg_x_hour_13_1000m": result.get("at_rest_avg_x_hour_13_1km", None),
                                "pedestrian_avg_x_hour_13_1000m": result.get("pedestrian_avg_x_hour_13_1km", None),
                                "motor_vehicle_avg_x_hour_13_1000m": result.get("motor_vehicle_avg_x_hour_13_1km", None),
                                "at_rest_avg_x_hour_14_1000m": result.get("at_rest_avg_x_hour_14_1km", None),
                                "pedestrian_avg_x_hour_14_1000m": result.get("pedestrian_avg_x_hour_14_1km", None),
                                "motor_vehicle_avg_x_hour_14_1000m": result.get("motor_vehicle_avg_x_hour_14_1km", None),
                                "at_rest_avg_x_hour_15_1000m": result.get("at_rest_avg_x_hour_15_1km", None),
                                "pedestrian_avg_x_hour_15_1000m": result.get("pedestrian_avg_x_hour_15_1km", None),
                                "motor_vehicle_avg_x_hour_15_1000m": result.get("motor_vehicle_avg_x_hour_15_1km", None),
                                "at_rest_avg_x_hour_16_1000m": result.get("at_rest_avg_x_hour_16_1km", None),
                                "pedestrian_avg_x_hour_16_1000m": result.get("pedestrian_avg_x_hour_16_1km", None),
                                "motor_vehicle_avg_x_hour_16_1000m": result.get("motor_vehicle_avg_x_hour_16_1km", None),
                                "at_rest_avg_x_hour_17_1000m": result.get("at_rest_avg_x_hour_17_1km", None),
                                "pedestrian_avg_x_hour_17_1000m": result.get("pedestrian_avg_x_hour_17_1km", None),
                                "motor_vehicle_avg_x_hour_17_1000m": result.get("motor_vehicle_avg_x_hour_17_1km", None),
                                "at_rest_avg_x_hour_18_1000m": result.get("at_rest_avg_x_hour_18_1km", None),
                                "pedestrian_avg_x_hour_18_1000m": result.get("pedestrian_avg_x_hour_18_1km", None),
                                "motor_vehicle_avg_x_hour_18_1000m": result.get("motor_vehicle_avg_x_hour_18_1km", None),
                                "at_rest_avg_x_hour_19_1000m": result.get("at_rest_avg_x_hour_19_1km", None),
                                "pedestrian_avg_x_hour_19_1000m": result.get("pedestrian_avg_x_hour_19_1km", None),
                                "motor_vehicle_avg_x_hour_19_1000m": result.get("motor_vehicle_avg_x_hour_19_1km", None),
                                "at_rest_avg_x_hour_20_1000m": result.get("at_rest_avg_x_hour_20_1km", None),
                                "pedestrian_avg_x_hour_20_1000m": result.get("pedestrian_avg_x_hour_20_1km", None),
                                "motor_vehicle_avg_x_hour_20_1000m": result.get("motor_vehicle_avg_x_hour_20_1km", None),
                                "at_rest_avg_x_hour_21_1000m": result.get("at_rest_avg_x_hour_21_1km", None),
                                "pedestrian_avg_x_hour_21_1000m": result.get("pedestrian_avg_x_hour_21_1km", None),
                                "motor_vehicle_avg_x_hour_21_1000m": result.get("motor_vehicle_avg_x_hour_21_1km", None),
                                "at_rest_avg_x_hour_22_1000m": result.get("at_rest_avg_x_hour_22_1km", None),
                                "pedestrian_avg_x_hour_22_1000m": result.get("pedestrian_avg_x_hour_22_1km", None),
                                "motor_vehicle_avg_x_hour_22_1000m": result.get("motor_vehicle_avg_x_hour_22_1km", None),
                                "at_rest_avg_x_hour_23_1000m": result.get("at_rest_avg_x_hour_23_1km", None),
                                "pedestrian_avg_x_hour_23_1000m": result.get("pedestrian_avg_x_hour_23_1km", None),
                                "motor_vehicle_avg_x_hour_23_1000m": result.get("motor_vehicle_avg_x_hour_23_1km", None),
                                "at_rest_avg_x_day_of_week_1_1000m": result.get("at_rest_avg_x_day_of_week_1_1km", None),
                                "pedestrian_avg_x_day_of_week_1_1000m": result.get("pedestrian_avg_x_day_of_week_1_1km", None),
                                "motor_vehicle_avg_x_day_of_week_1_1000m": result.get("motor_vehicle_avg_x_day_of_week_1_1km", None),
                                "at_rest_avg_x_day_of_week_2_1000m": result.get("at_rest_avg_x_day_of_week_2_1km", None),
                                "pedestrian_avg_x_day_of_week_2_1000m": result.get("pedestrian_avg_x_day_of_week_2_1km", None),
                                "motor_vehicle_avg_x_day_of_week_2_1000m": result.get("motor_vehicle_avg_x_day_of_week_2_1km", None),
                                "at_rest_avg_x_day_of_week_3_1000m": result.get("at_rest_avg_x_day_of_week_3_1km", None),
                                "pedestrian_avg_x_day_of_week_3_1000m": result.get("pedestrian_avg_x_day_of_week_3_1km", None),
                                "motor_vehicle_avg_x_day_of_week_3_1000m": result.get("motor_vehicle_avg_x_day_of_week_3_1km", None),
                                "at_rest_avg_x_day_of_week_4_1000m": result.get("at_rest_avg_x_day_of_week_4_1km", None),
                                "pedestrian_avg_x_day_of_week_4_1000m": result.get("pedestrian_avg_x_day_of_week_4_1km", None),
                                "motor_vehicle_avg_x_day_of_week_4_1000m": result.get("motor_vehicle_avg_x_day_of_week_4_1km", None),
                                "at_rest_avg_x_day_of_week_5_1000m": result.get("at_rest_avg_x_day_of_week_5_1km", None),
                                "pedestrian_avg_x_day_of_week_5_1000m": result.get("pedestrian_avg_x_day_of_week_5_1km", None),
                                "motor_vehicle_avg_x_day_of_week_5_1000m": result.get("motor_vehicle_avg_x_day_of_week_5_1km", None),
                                "at_rest_avg_x_day_of_week_6_1000m": result.get("at_rest_avg_x_day_of_week_6_1km", None),
                                "pedestrian_avg_x_day_of_week_6_1000m": result.get("pedestrian_avg_x_day_of_week_6_1km", None),
                                "motor_vehicle_avg_x_day_of_week_6_1000m": result.get("motor_vehicle_avg_x_day_of_week_6_1km", None),
                                "at_rest_avg_x_day_of_week_7_1000m": result.get("at_rest_avg_x_day_of_week_7_1km", None),
                                "pedestrian_avg_x_day_of_week_7_1000m": result.get("pedestrian_avg_x_day_of_week_7_1km", None),
                                "motor_vehicle_avg_x_day_of_week_7_1000m": result.get("motor_vehicle_avg_x_day_of_week_7_1km", None)
                            }
                        }
                if show_all_keys:
                    resp.append( {
                                "property_details": property_details,
                                "market_info": market_info,
                                "pois": pois,
                                "traffic": traffic
                            })
                else:
                    resp.append({
                        "property_details": property_details,
                        "market_info": market_info,
                        "traffic": traffic
                    })
            return resp
        except Exception as e:
            logger.error("Error processing property JSON: %s", str(e), exc_info=True)
            raise e

    def get_properties(self, current_user, fid=None, lat=None, lng=None, city='mexico'):
        from utils.streetViewUtils import get_street_view_metadata_cached
        import copy
        logger.info(f"[get_properties] Called with city={city}, fid={fid}, lat={lat}, lng={lng}")
        connection = None
        cursor = None
        resp = None
        try:
            logger.info("Fetching properties for user=%s, fid=%s, lat=%s, lng=%s", current_user, fid, lat, lng)
            filter_query = 'WHERE 1=1'
            if fid:
                filter_query += f" AND fid = {fid}"
            elif lat and lng:
                if city == 'queretaro' or city == 'el_marques':
                    # QRO: Use H3 resolution 12 with neighbors to handle cell boundary issues
                    h3Index = h3.latlng_to_cell(float(lat), float(lng), 12)
                    h3_index_decimal = str(int(h3Index, 16))
                    
                    # Get neighbors to handle properties spanning multiple H3 cells
                    neighbors = h3.grid_disk(h3Index, 1)  # Get the cell + its immediate neighbors
                    neighbor_decimals = [str(int(neighbor, 16)) for neighbor in neighbors]
                    
                    # Create OR condition for the cell and its neighbors
                    h3_conditions = " OR ".join([f"h3_indexes ILIKE '%{h3_decimal}%'" for h3_decimal in neighbor_decimals])
                    filter_query += f" AND ({h3_conditions})"
                else:
                    # CDMX: Use original approach with H3 resolution 13
                    h3Index = h3.latlng_to_cell(float(lat), float(lng), 13)
                    h3_index_decimal = str(int(h3Index, 16))
                    filter_query += f" AND h3_indexes ILIKE '%{h3_index_decimal}%'"
            else:
                logger.warning("Invalid request: Missing fid or lat/lng")
                return Response.bad_request(message="Invalid request")

            query = self.qc.get_property_query(filter_query, city=city)
            logger.info(f"[get_properties] Executing query for city={city}: ")

            def fetch_property_details():
                # connection = self.db.connect('redshiftdb')
                connection = self.redshift_connection.connect()
                cur = connection.cursor(cursor_factory=RealDictCursor)
                cur.execute(query)
                result = cur.fetchall()
                cur.close()
                self.redshift_connection.disconnect(connection)
                return result

            def fetch_pano_id(lat, lng):
                return get_street_view_metadata_cached(float(lat), float(lng))

            with ThreadPoolExecutor(max_workers=2) as executor:
                future_details = executor.submit(fetch_property_details)
                # Wait for property details to get lat/lng
                result = future_details.result()
                if not result:
                    return Response.not_found(message="Property not found")
                result_jsons = self.get_property_json(result, city=city)
                # print("RESULT_JSONS",result_jsons)
                # Assume only one property for lat/lng
                prop_lat, prop_lng = None, None
                if result_jsons and result_jsons[0]["property_details"].get("lat") and result_jsons[0]["property_details"].get("lng"):
                    prop_lat = result_jsons[0]["property_details"]["lat"]
                    prop_lng = result_jsons[0]["property_details"]["lng"]
                if prop_lat and prop_lng:
                    future_pano = executor.submit(fetch_pano_id, prop_lat, prop_lng)
                    pano_id = future_pano.result()
                    for res_json in result_jsons:
                        # Only set street view images if street_images is empty
                        if not res_json["property_details"].get("street_images"):
                            if pano_id:
                                headings = [0, 45, 90, 135, 180, 225, 270, 315]
                                fov = 90
                                size = "600x300"
                                street_images = [
                                    f"{BASE_URL}/properties/street_view_image?pano_id={pano_id}&heading={heading}&fov={fov}&size={size}"
                                    for heading in headings
                                ]
                                res_json["property_details"]["street_images"] = street_images
                            else:
                                print("Street view Image is updating here")
                                res_json["property_details"]["street_images"] = []
                upc = UserPropertyController()
                if fid:
                    upc.add_user_property(fid, current_user, 'view', config_city=city)
                logger.info(f"[get_properties] Successfully fetched {len(result_jsons)} properties")
                resp = Response.success(data=result_jsons, message='Success')
        except Exception as e:
            logger.error("Error fetching properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp

    def get_property_market_info(self, spot2_id, inmuebles24_id, propiedades_id, city='mexico'):
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info("Fetching market infor for  spot2_id=%s, inmuebles24_id=%s, propiedades_id=%s",  spot2_id, inmuebles24_id, propiedades_id)
            query = self.qc.get_market_info_query(spot2_id, inmuebles24_id, propiedades_id, city=city)
            logger.debug("Market info query: %s", query)
            cursor.execute(query)
            res = cursor.fetchall()
            resp = Response.success(data=res, message='Success')
        except Exception as e:
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp
        
    @staticmethod
    def get_demographic_json(result, city='mexico'):
        # print("DEMOGRAPHIC RESULT",result)
        result = result[0]
        try:
            if city == 'queretaro' or city == 'el_marques':
                # QRO demographic structure - use .get() to handle missing columns
                demographic = {
                    "general" : {
                        "block" : {
                                "neighborhood" : result.get("nom_loc", None),
                                "predominant_level" : result.get("niv_predom", None),
                                "ageb_code" : result.get("cve_ageb", None),
                                "total_household": result.get("tot_vivien", None),
                                "average_household_size": result.get("pobtot", None) / result.get("tot_vivien", 1) if result.get("tot_vivien") else None,
                                "average_number_of_rooms": None  # Not available in QRO at block level
                                },
                        "colonia": {
                                "neighborhood" : result.get("nom_loc", None),
                                "predominant_level" : result.get("predominant_level_colonia", None),
                                "ageb_code" : result.get("cve_ageb", None),
                                "total_household": result.get("total_houses_colonia", None),
                                "average_household_size": result.get("prom_ocup_colonia", None),
                                "average_number_of_rooms": result.get("pro_ocup_c_colonia", None)
                                },
                        
                        "alcaldia": {
                                "neighborhood" : result.get('nom_mun', None),
                                "predominant_level" : result.get("predominant_level_alcaldia", None),
                                "ageb_code" : result.get("cve_ageb", None),
                                "total_household": result.get("total_houses_alcaldia", None),
                                "average_household_size": result.get("prom_ocup_alcaldia", None),
                                "average_number_of_rooms": result.get("pro_ocup_c_alcaldia", None)
                        }
                    },
                    "socio_economic_level": {
                        "block": {
                            "ses_ab": result.get("pct_viv_ab", None),
                            "ses_c_plus": result.get("pct_viv_cp", None),
                            "ses_c": result.get("pct_viv_c", None),
                            "ses_c_minus": result.get("pct_viv_cm", None),
                            "ses_d": result.get("pct_viv_d", None),
                            "ses_d_plus": result.get("pct_viv_dp", None),
                            "ses_e": result.get("pct_viv_e", None)
                        },
                        "colonia": {
                            "ses_ab": result.get("ses_ab_colonia", None),
                            "ses_c_plus": result.get("ses_c_plus_colonia", None),
                            "ses_c": result.get("ses_c_colonia", None),
                            "ses_c_minus": result.get("ses_c_minus_colonia", None),
                            "ses_d": result.get("ses_d_colonia", None),
                            "ses_d_plus": result.get("ses_d_plus_colonia", None),
                            "ses_e": result.get("ses_e_colonia", None)
                        },
                        "alcaldia": {
                            "ses_ab": result.get("ses_ab_alcaldia", None),
                            "ses_c_plus": result.get("ses_c_plus_alcaldia", None),
                            "ses_c": result.get("ses_c_alcaldia", None),
                            "ses_c_minus": result.get("ses_c_minus_alcaldia", None),
                            "ses_d": result.get("ses_d_alcaldia", None),
                            "ses_d_plus": result.get("ses_d_plus_alcaldia", None),
                            "ses_e": result.get("ses_e_alcaldia", None)
                        }
                    },
                    "population": {
                        "block": {
                            "total_population": result.get("pobtot", None),
                            "male_population": None,  # Not available at block level in QRO
                            "female_population": None,  # Not available at block level in QRO
                        },
                        "colonia": {
                            "total_population": result.get("pobtot_colonia", None),
                            "male_population": result.get("pobmas_colonia", None),
                            "female_population": result.get("pobfem_colonia", None),
                        },
                        "alcaldia": {
                            "total_population": result.get("pobtot_alcaldia", None),
                            "male_population": result.get("pobmas_alcaldia", None),
                            "female_population": result.get("pobfem_alcaldia", None),
                        }
                    },
                    "education": {
                        "block": {
                            "education_3_5": result.get("p_3a5", None),
                            "education_6_11": result.get("p_6a11", None),
                            "education_12_14": result.get("p_12a14", None),
                            "education_15_17": result.get("p_15a17", None),
                            "education_18_24": result.get("p_18a24", None),
                            "education_3_5_attending_school": result.get("p3a5_noa", None),
                            "education_6_11_attending_school": result.get("p6a11_noa", None),
                            "education_12_14_attending_school": result.get("p12a14noa", None),
                            "education_15_17_attending_school": result.get("p15a17a", None),
                            "education_18_24_attending_school": result.get("p18a24a", None)
                        },
                        "colonia": {
                            "education_3_5": result.get("p_3a5_colonia", None),
                            "education_6_11": result.get("p_6a11_colonia", None),
                            "education_12_14": result.get("p_12a14_colonia", None),
                            "education_15_17": result.get("p_15a17_colonia", None),
                            "education_18_24": result.get("p_18a24_colonia", None),
                            "education_3_5_attending_school": result.get("p3a5_noa_colonia", None),
                            "education_6_11_attending_school": (result.get("p6a11_noaf_colonia", 0) or 0) + (result.get("p6a11_noam_colonia", 0) or 0),
                            "education_12_14_attending_school": (result.get("p12a14noaf_colonia", 0) or 0) + (result.get("p12a14noam_colonia", 0) or 0),
                            "education_15_17_attending_school": result.get("p15a17a_colonia", None),
                            "education_18_24_attending_school": result.get("p18a24a_colonia", None)
                        },
                        "alcaldia": {
                            "education_3_5": result.get("p_3a5_alcaldia", None),
                            "education_6_11": result.get("p_6a11_alcaldia", None),
                            "education_12_14": result.get("p_12a14_alcaldia", None),
                            "education_15_17": result.get("p_15a17_alcaldia", None),
                            "education_18_24": result.get("p_18a24_alcaldia", None),
                            "education_3_5_attending_school": result.get("p3a5_noa_alcaldia", None),
                            "education_6_11_attending_school": result.get("p6a11_noa_alcaldia", None),
                            "education_12_14_attending_school": result.get("p12a14noa_alcaldia", None),
                            "education_15_17_attending_school": result.get("p15a17a_alcaldia", None),
                            "education_18_24_attending_school": result.get("p18a24a_alcaldia", None)
                        }
                    },
                    "workforce": {
                        "block": {
                            "total_workforce": result.get("pea", None),
                            "total_male_workforce": result.get("pea_m", None),
                            "total_female_workforce": result.get("pea_f", None),
                            "total_inactive_population": result.get("pe_inac", None),
                            "total_inactive_male_population": result.get("pe_inac_m", None),
                            "total_inactive_female_population": result.get("pe_inac_f", None)
                        },
                        "colonia": {
                            "total_workforce": result.get("pea_colonia", None),
                            "total_male_workforce": result.get("pea_m_colonia", None),
                            "total_female_workforce": result.get("pea_f_colonia", None),
                            "total_inactive_population": result.get("pe_inac_colonia", None),
                            "total_inactive_male_population": result.get("pe_inac_m_colonia", None),
                            "total_inactive_female_population": result.get("pe_inac_f_colonia", None)
                        },
                        "alcaldia": {
                            "total_workforce": result.get("pea_alcaldia", None),
                            "total_male_workforce": result.get("pea_m_alcaldia", None),
                            "total_female_workforce": result.get("pea_f_alcaldia", None),
                            "total_inactive_population": result.get("pe_inac_alcaldia", None),
                            "total_inactive_male_population": result.get("pe_inac_m_alcaldia", None),
                            "total_inactive_female_population": result.get("pe_inac_f_alcaldia", None)
                        }
                    },
                    "employment": {
                        "block": {
                            "total_employed_population": result.get("pocupada", None),
                            "total_male_employed_population": result.get("pocupada_m", None),
                            "total_female_emloyed_population": result.get("pocupada_f", None),  # Note: keeping Mexico's typo "emloyed"
                            "total_unemployed_population": result.get("pdesocup", None),
                            "total_unemployed_male_population": result.get("pdesocup_m", None),
                            "total_unemployed_female_population": result.get("pdesocup_f", None)
                        },
                        "colonia": {
                            "total_employed_population": result.get("pocupada_colonia", None),
                            "total_male_employed_population": result.get("pocupada_m_colonia", None),
                            "total_female_emloyed_population": result.get("pocupada_f_colonia", None),
                            "total_unemployed_population": result.get("pdesocup_colonia", None),
                            "total_unemployed_male_population": result.get("pdesocup_m_colonia", None),
                            "total_unemployed_female_population": result.get("pdesocup_f_colonia", None)
                        },
                        "alcaldia": {
                            "total_employed_population": result.get("pocupada_alcaldia", None),
                            "total_male_employed_population": result.get("pocupada_m_alcaldia", None),
                            "total_female_emloyed_population": result.get("pocupada_f_alcaldia", None),
                            "total_unemployed_population": result.get("pdesocup_alcaldia", None),
                            "total_unemployed_male_population": result.get("pdesocup_m_alcaldia", None),
                            "total_unemployed_female_population": result.get("pdesocup_f_alcaldia", None)
                        }
                    },
                    "population_growth": {
                        "block": { 
                            "2000": [result.get('pob_2000_ageb', 0), 0],
                            "2005": [result.get('pob_2005_ageb', 0), float(result.get('cambio_porcentual_2005_ageb', 0) or 0)],
                            "2010": [result.get('pob_2010_ageb', 0), float(result.get('cambio_porcentual_2010_ageb', 0) or 0)],
                            "2015": [result.get('pob_2015_ageb', 0), float(result.get('cambio_porcentual_2015_ageb', 0) or 0)],
                            "2020": [result.get('pob_2020_ageb', 0), float(result.get('cambio_porcentual_2020_ageb', 0) or 0)]
                        },
                        "colonia": {
                            "2000": [result.get('pob_2000_entidad', 0), 0],
                            "2005": [result.get('pob_2005_entidad', 0), float(result.get('cambio_porcentual_2005_entidad', 0) or 0)],
                            "2010": [result.get('pob_2010_entidad', 0), float(result.get('cambio_porcentual_2010_entidad', 0) or 0)],
                            "2015": [result.get('pob_2015_entidad', 0), float(result.get('cambio_porcentual_2015_entidad', 0) or 0)],
                            "2020": [result.get('pob_2020_entidad', 0), float(result.get('cambio_porcentual_2020_entidad', 0) or 0)]
                        },
                        "alcaldia": {
                            "2000": [result.get('pob_2000_municipal', 0), 0],
                            "2005": [result.get('pob_2005_municipal', 0), float(result.get('cambio_porcentual_2005_municipal', 0) or 0)],
                            "2010": [result.get('pob_2010_municipal', 0), float(result.get('cambio_porcentual_2010_municipal', 0) or 0)],
                            "2015": [result.get('pob_2015_municipal', 0), float(result.get('cambio_porcentual_2015_municipal', 0) or 0)],
                            "2020": [result.get('pob_2020_municipal', 0), float(result.get('cambio_porcentual_2020_municipal', 0) or 0)]
                        }
                    }
                }
            else:
                # CDMX demographic structure - original structure
                demographic = {
                    "general" : {
                        "block" : {
                                "neighborhood" : result["neighborhood"],
                                "predominant_level" : result["predominant_level"],
                                "ageb_code" : result["ageb_code"],
                                "total_household": result["vivtot"],
                                "average_household_size": result["prom_ocup"],
                                "average_number_of_rooms": result["pro_ocup_c"]
                                
                                },
                        "colonia": {
                                "neighborhood" : result["neighborhood"],
                                "predominant_level" : result["predominant_level"],
                                "ageb_code" : result["ageb_code"],
                                "total_household": result["vivtot_colonia"],
                                "average_household_size": result["prom_ocup_colonia"],
                                "average_number_of_rooms": result["pro_ocup_c_colonia"]
                                },
                        
                        "alcaldia": {
                                "neighborhood" : result['nom_mun'],
                                "predominant_level" : result["predominant_level"],
                                "ageb_code" : result["ageb_code"],
                                "total_household": result["vivtot_alcaldia"],
                                "average_household_size": result["prom_ocup_alcaldia"],
                                "average_number_of_rooms": result["pro_ocup_c_alcaldia"]
                        }
                    },
                    "socio_economic_level": {
                        "block": {
                            "ses_ab": result["ses_ab"],
                            "ses_c_plus": result["ses_c_plus"],
                            "ses_c": result["ses_c"],
                            "ses_c_minus": result["ses_c_minus"],
                            "ses_d": result["ses_d"],
                            "ses_d_plus": result["ses_d_plus"],
                            "ses_e": result["ses_e"]
                        },
                        "colonia": {
                            "ses_ab": result["ses_ab_colonia"],
                            "ses_c_plus": result["ses_c_plus_colonia"],
                            "ses_c": result["ses_c_colonia"],
                            "ses_c_minus": result["ses_c_minus_colonia"],
                            "ses_d": result["ses_d_colonia"],
                            "ses_d_plus": result["ses_d_plus_colonia"],
                            "ses_e": result["ses_e_colonia"]
                        },
                        "alcaldia": {
                            "ses_ab": result["ses_ab_alcaldia"],
                            "ses_c_plus": result["ses_c_plus_alcaldia"],
                            "ses_c": result["ses_c_alcaldia"],
                            "ses_c_minus": result["ses_c_minus_alcaldia"],
                            "ses_d": result["ses_d_alcaldia"],
                            "ses_d_plus": result["ses_d_plus_alcaldia"],
                            "ses_e": result["ses_e_alcaldia"]
                        }
                    },
                    "population": {
                        "block": {
                            "total_population": result["pobtot"],
                            "male_population": result["pobmas"],
                            "female_population": result["pobfem"],
                        },
                        "colonia": {
                            "total_population": result["pobtot_colonia"],
                            "male_population": result["pobmas_colonia"], 
                            "female_population": result["pobfem_colonia"],
                        },
                        "alcaldia": {
                            "total_population": result["pobtot_alcaldia"],
                            "male_population": result["pobmas_alcaldia"],
                            "female_population": result["pobfem_alcaldia"],
                        }
                    },
                    "education": {
                        "block": {
                            "education_3_5": result["p_3a5"],
                            "education_6_11": result["p_6a11"],
                            "education_12_14": result["p_12a14"],
                            "education_15_17": result["p_15a17"],
                            "education_18_24": result["p_18a24"],
                            "education_3_5_attending_school": result["p3a5_noa"],
                            "education_6_11_attending_school": result["p6a11_noa"],
                            "education_12_14_attending_school": result["p12a14noa"],
                            "education_15_17_attending_school": result["p15a17a"],
                            "education_18_24_attending_school": result["p18a24a"]
                        },
                        "colonia": {
                            "education_3_5": result["p_3a5_colonia"],
                            "education_6_11": result["p_6a11_colonia"],
                            "education_12_14": result["p_12a14_colonia"],
                            "education_15_17": result["p_15a17_colonia"],
                            "education_18_24": result["p_18a24_colonia"],
                            "education_3_5_attending_school": result["p3a5_noa_colonia"],
                            "education_6_11_attending_school": result["p6a11_noa_colonia"],
                            "education_12_14_attending_school": result["p12a14noa_colonia"],
                            "education_15_17_attending_school": result["p15a17a_colonia"],
                            "education_18_24_attending_school": result["p18a24a_colonia"]
                        },
                        "alcaldia": {
                            "education_3_5": result["p_3a5_alcaldia"],
                            "education_6_11": result["p_6a11_alcaldia"],
                            "education_12_14": result["p_12a14_alcaldia"],
                            "education_15_17": result["p_15a17_alcaldia"],
                            "education_18_24": result["p_18a24_alcaldia"],
                            "education_3_5_attending_school": result["p3a5_noa_alcaldia"],
                            "education_6_11_attending_school": result["p6a11_noa_alcaldia"],
                            "education_12_14_attending_school": result["p12a14noa_alcaldia"],
                            "education_15_17_attending_school": result["p15a17a_alcaldia"],
                            "education_18_24_attending_school": result["p18a24a_alcaldia"]
                        }
                    },
                    "workforce": {
                        "block": {
                            "total_workforce": result["pea"],
                            "total_male_workforce": result["pea_m"],
                            "total_female_workforce": result["pea_f"],
                            "total_inactive_population": result["pe_inac"],
                            "total_inactive_male_population": result["pe_inac_m"],
                            "total_inactive_female_population": result["pe_inac_f"]
                        },
                        "colonia": {
                            "total_workforce": result["pea_colonia"],
                            "total_male_workforce": result["pea_m_colonia"],
                            "total_female_workforce": result["pea_f_colonia"],
                            "total_inactive_population": result["pe_inac_colonia"],
                            "total_inactive_male_population": result["pe_inac_m_colonia"],
                            "total_inactive_female_population": result["pe_inac_f_colonia"]
                        },
                        "alcaldia": {
                            "total_workforce": result["pea_alcaldia"],
                            "total_male_workforce" : result["pea_m_alcaldia"],
                            "total_female_workforce": result["pea_f_alcaldia"],
                            "total_inactive_population": result["pe_inac_alcaldia"],
                            "total_inactive_male_population": result["pe_inac_m_alcaldia"],
                            "total_inactive_female_population": result["pe_inac_f_alcaldia"]
                            
                    }
                    },
                    "employment": {
                        "block": {
                            "total_employed_population": result["pocupada"],
                            "total_male_employed_population": result["pocupada_m"],
                            "total_female_emloyed_population": result["pocupada_f"],
                            "total_unemployed_population": result["pdesocup"],
                            "total_unemployed_male_population": result["pdesocup_m"],
                            "total_unemployed_female_population": result["pdesocup_f"]
                        },
                        "colonia": {
                            "total_employed_population": result["pocupada_colonia"],
                            "total_male_employed_population": result["pocupada_m_colonia"],
                            "total_female_emloyed_population": result["pocupada_f_colonia"],
                            "total_unemployed_population": result["pdesocup_colonia"],
                            "total_unemployed_male_population": result["pdesocup_m_colonia"],
                            "total_unemployed_female_population": result["pdesocup_f_colonia"] 
                            },
                        "alcaldia": {
                            "total_employed_population": result["pocupada_alcaldia"],
                            "total_male_employed_population": result["pocupada_m_alcaldia"],
                            "total_female_emloyed_population": result["pocupada_f_alcaldia"],
                            "total_unemployed_population": result["pdesocup_alcaldia"],
                            "total_unemployed_male_population": result["pdesocup_m_alcaldia"],
                            "total_unemployed_female_population": result["pdesocup_f_alcaldia"],
                        }   
                    },    
                   "population_growth": {
                            "block": { 
                                "2000": [result.get('pob_2000_ageb', 0), 0],
                                "2005": [result.get('pob_2005_ageb', 0), float(result.get('cambio_porcentual_2005_ageb', 0) or 0)],
                                "2010": [result.get('pob_2010_ageb', 0), float(result.get('cambio_porcentual_2010_ageb', 0) or 0)],
                                "2015": [result.get('pob_2015_ageb', 0), float(result.get('cambio_porcentual_2015_ageb', 0) or 0)],
                                "2020": [result.get('pob_2020_ageb', 0), float(result.get('cambio_porcentual_2020_ageb', 0) or 0)]
                            },
                            "colonia": {
                                "2000": [result.get('pob_2000_entidad', 0), 0],
                                "2005": [result.get('pob_2005_entidad', 0), float(result.get('cambio_porcentual_2005_entidad', 0) or 0)],
                                "2010": [result.get('pob_2010_entidad', 0), float(result.get('cambio_porcentual_2010_entidad', 0) or 0)],
                                "2015": [result.get('pob_2015_entidad', 0), float(result.get('cambio_porcentual_2015_entidad', 0) or 0)],
                                "2020": [result.get('pob_2020_entidad', 0), float(result.get('cambio_porcentual_2020_entidad', 0) or 0)]
                            },
                            "alcaldia": {
                                "2000": [result.get('pob_2000_municipal', 0), 0],
                                "2005": [result.get('pob_2005_municipal', 0), float(result.get('cambio_porcentual_2005_municipal', 0) or 0)],
                                "2010": [result.get('pob_2010_municipal', 0), float(result.get('cambio_porcentual_2010_municipal', 0) or 0)],
                                "2015": [result.get('pob_2015_municipal', 0), float(result.get('cambio_porcentual_2015_municipal', 0) or 0)],
                                "2020": [result.get('pob_2020_municipal', 0), float(result.get('cambio_porcentual_2020_municipal', 0) or 0)]
                            }
                        }
                }
            return demographic
        except Exception as e:
            raise e
    # @cache_response(prefix='demographic',expiration=3600)
    
    def get_property_demographic(self, fid, current_user, city='mexico'):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info("Fetching demographic data for fid=%s, user=%s, city=%s", fid, current_user, city)
            start_time = time.time()
            # connection = self.db.connect('redshiftdb')
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.qc.get_demographics_query(fid, city=city)
            logger.info("Executing demographic query for city=%s: %s", city, query)

            cursor.execute(query)
            res = cursor.fetchall()
            logger.info("Fetched demographic data for fid=%s", fid)
            if res:
                response = self.get_demographic_json(res, city=city)
                json_time = time.time()  # Time after JSON conversion
                
                upc = UserPropertyController()
                # print(f"Calling add_user_property with fid={fid}, current_user={current_user}", flush=True)

                upc.add_user_property(fid, current_user, 'view', config_city=city)
                add_property_time = time.time()  # Time after adding user property
                
                resp = Response.success(data=response, message='Success')
                
            else:
                resp = Response.bad_request(message="Property not found")
                logger.info("No property found")
            end_time = time.time()  # End time of function
            
        except Exception as e:
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp

    def get_property_traffic(self, fid, config_city='mexico'):
        connection = None
        cursor = None
        try:
            # Ensure fid is an integer
            if isinstance(fid, int):
                fid_value = fid
            elif isinstance(fid, str):
                try:
                    fid_value = int(fid)
                except ValueError:
                    return Response.bad_request(message="Invalid fid: must be an integer or string representing an integer fid.")
            else:
                return Response.bad_request(message="Invalid fid type.")

            if config_city == 'queretaro' or config_city == 'el_marques':
                table_name = 'blackprint_db_prd.presentation.dataset_mobility_data_h3_qro'
                id_col = 'id_stg_demographic_socioeconomic_qro'
            else:
                table_name = 'blackprint_db_prd.presentation.dataset_mobility_data_h3'
                id_col = 'fid'
            
            query = f"""
                SELECT 
                    type,
                    min_pedestrian,
                    max_pedestrian,
                    min_motor_vehicle,
                    max_motor_vehicle
                FROM {table_name}
                WHERE {id_col} = %s AND type IN ('CIRCLE_500_METERS', 'FRONT_OF_STORE', 'CIRCLE_1000_METERS')
            """

            # Use Redshift connection
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, (fid_value,))
            results = cursor.fetchall()

            # Organize results by type/radius
            response = {}
            for row in results:
                if row['type'] == 'CIRCLE_500_METERS':
                    response['500m'] = {
                        'min_pedestrian': row['min_pedestrian'],
                        'max_pedestrian': row['max_pedestrian'],
                        'min_motor_vehicle': row['min_motor_vehicle'],
                        'max_motor_vehicle': row['max_motor_vehicle']
                    }
                elif row['type'] == 'FRONT_OF_STORE':
                    response['50m'] = {
                        'min_pedestrian': row['min_pedestrian'],
                        'max_pedestrian': row['max_pedestrian'],
                        'min_motor_vehicle': row['min_motor_vehicle'],
                        'max_motor_vehicle': row['max_motor_vehicle']
                    }
                elif row['type'] == 'CIRCLE_1000_METERS':
                    response['1000m'] = {
                        'min_pedestrian': row['min_pedestrian'],
                        'max_pedestrian': row['max_pedestrian'],
                        'min_motor_vehicle': row['min_motor_vehicle'],
                        'max_motor_vehicle': row['max_motor_vehicle']
                    }

            return Response.success(data={"response": response})

        except Exception as e:
            return Response.internal_server_error(message=str(e))

        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)

    def get_property_details_bundle(self, current_user, fid):
        """
        Fetches all property-related details in a single, consolidated call.
        """
        from utils.normalization_utils import normalize_fid, normalize_market_id
        logger.info(f"Fetching details bundle for fid={fid}, user={current_user}")
        norm_fid = normalize_fid(fid)
        if not norm_fid:
            return Response.bad_request("Invalid FID provided.")

        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit tasks to run concurrently
            property_future = executor.submit(self.get_properties, current_user, fid=norm_fid)
            demographic_future = executor.submit(self.get_property_demographic, norm_fid, current_user)
            userproperty_future = executor.submit(UserPropertyController().get_user_properties, current_user, fid=norm_fid)
            traffic_future = executor.submit(self.get_property_traffic, norm_fid)

            # Resolve the main property future first to get market info IDs
            property_response_tuple = property_future.result()
            if not isinstance(property_response_tuple, tuple) or property_response_tuple[1] != 200:
                logger.error(f"Failed to get main property details for fid={norm_fid}")
                return property_response_tuple

            property_data = property_response_tuple[0]['data'][0]
            
            market_info = property_data.get('market_info', {})
            property_details = property_data.get('property_details', {})
            
            spot2_id = market_info.get('ids_market_data_spot2')
            inmuebles24_id = property_details.get('ids_market_data_inmuebles24')
            propiedades_id = property_details.get('ids_market_data_propiedades')
            
            market_future = executor.submit(self.get_property_market_info, spot2_id, inmuebles24_id, propiedades_id)

            # Resolve all futures
            demographic_response = demographic_future.result()
            userproperty_response = userproperty_future.result()
            traffic_response = traffic_future.result()
            market_response = market_future.result()

            # Assemble the final response bundle
            bundle = {
                "property": property_data,
                "demographics": demographic_response[0].get('data') if isinstance(demographic_response, tuple) and demographic_response[1] == 200 else {},
                "user_property": userproperty_response[0].get('data') if isinstance(userproperty_response, tuple) and userproperty_response[1] == 200 else {},
                "traffic": traffic_response[0].get('data') if isinstance(traffic_response, tuple) and traffic_response[1] == 200 else {},
                "market_info_details": market_response[0].get('data') if isinstance(market_response, tuple) and market_response[1] == 200 else {}
            }

            return Response.success(data=bundle, message="Success")

    def convert_decimal(self, obj):
        if isinstance(obj, list):
            return [self.convert_decimal(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: self.convert_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return obj

    def get_property_commercial_growth(self, fid):
        """Fetch commercial growth data for a property by fid from Redshift and return formatted JSON."""
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            # Get the commercial growth query from QueryController
            query = self.qc.get_commercial_growth_query(fid)
            cursor.execute(query)
            result = cursor.fetchall()
            if not result:
                return Response.success(data=None, message="No commercial growth data found")
            # Format the result using the template function
            formatted = get_commercial_growth_json(result)
            formatted = self.convert_decimal(formatted)
            return Response.success(data=formatted, message="Success")
        except Exception as e:
            logger.error(f"Error fetching commercial growth data: {e}", exc_info=True)
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)

    def filter_properties(self, filters, city='mexico'):
        """
        Accepts a dict of filter fields and returns filtered properties.
        Maps UI keys to DB columns for correct filtering.
        """
        # Determine column mapping based on city
        if city == 'queretaro' or city == 'el_marques':
            # QRO/El Marques column mapping - Enhanced with market data filters
            FILTER_COLUMN_MAP = {
                "availability": "v.is_on_market",
                "geometry": "v.geometry_type",
                "id_municipality": "v.cve_mun",
                "municipality": "v.nom_mun",
                "alcaldia": "v.nom_mun",
                "colonia": "v.nom_loc",
                # Market data filters now supported via dim_market_data_combined
                "property_type": "mdc.property_type",
                "operation_type": "mdc.operation_type",
                "rent": "mdc.rent_price_clean",
                "buy": "mdc.buy_price_clean",
                "dimension_min": "mdc.property_dimension_clean",
                "dimension_max": "mdc.property_dimension_clean",
                # Still unsupported (no equivalent columns)
                "plot_min": None,
                "plot_max": None,
                "construction_min": None,
                "construction_max": None,
                "zip_code": None,
                #improvements in filter 
                "id_stg_demographic_socioeconomic_qro": "id_stg_demographic_socioeconomic_qro",
            }
        else:
            # Mexico column mapping
            FILTER_COLUMN_MAP = {
                "availability": "is_on_market",
                "property_type": ["property_type_spot2", "property_type_inmuebles24", "property_type_propiedades"],
                "plot_min": "total_surface_area",
                "plot_max": "total_surface_area",
                "buy": ["buy_price_spot2",  "buy_price_inmuebles24", "buy_price_propiedades"],
                "rent": ["rent_price_spot2", "rent_price_inmuebles24", "rent_price_propiedades"],
                "construction_min": "total_construction_area",
                "construction_max": "total_construction_area",
                "geometry": "block_type",
                "id_municipality": "id_municipality",
                "city": "city",
                "municipality": "municipality_nm",
                "alcaldia": "nom_mun",
                "colonia": "neighborhood",
                "zip_code": "zip_code"
            }
        connection = None
        cursor = None
        resp = None
        try:
            # Log the request payload from frontend
            logger.info(f"[FILTER REQUEST] City: {city}, Payload: {filters}")
            filter_query = 'WHERE 1=1'
            # Availability (example: is_on_market)
            if 'availability' in filters and filters['availability'] and FILTER_COLUMN_MAP['availability']:
                filter_query += f" AND {FILTER_COLUMN_MAP['availability']} = '{filters['availability']}'"
            
            # Property Type (multi-select) - Updated for both Mexico and QRO
            if 'property_type' in filters and filters['property_type'] and FILTER_COLUMN_MAP['property_type']:
                types = filters['property_type']
                cols = FILTER_COLUMN_MAP['property_type']
                
                if isinstance(cols, list):
                    # Mexico: multiple columns (list)
                    if isinstance(types, list):
                        type_list = ','.join([f"'{t}'" for t in types])
                        filter_query += " AND (" + " OR ".join([f"{col} IN ({type_list})" for col in cols]) + ")"
                    else:
                        filter_query += " AND (" + " OR ".join([f"{col} = '{types}'" for col in cols]) + ")"
                else:
                    # QRO: single column (string)
                    if isinstance(types, list):
                        type_list = ','.join([f"'{t}'" for t in types])
                        filter_query += f" AND {cols} IN ({type_list})"
                    else:
                        filter_query += f" AND {cols} = '{types}'"
            
            # Plot Dimensions (range) - Only for Mexico
            if 'plot_min' in filters and filters['plot_min'] is not None and FILTER_COLUMN_MAP['plot_min']:
                filter_query += f" AND {FILTER_COLUMN_MAP['plot_min']} >= {filters['plot_min']}"
            if 'plot_max' in filters and filters['plot_max'] is not None and FILTER_COLUMN_MAP['plot_max']:
                filter_query += f" AND {FILTER_COLUMN_MAP['plot_max']} <= {filters['plot_max']}"
            
            # Construction Dimensions (range) - Only for Mexico
            if 'construction_min' in filters and filters['construction_min'] is not None and FILTER_COLUMN_MAP['construction_min']:
                filter_query += f" AND {FILTER_COLUMN_MAP['construction_min']} >= {filters['construction_min']}"
            if 'construction_max' in filters and filters['construction_max'] is not None and FILTER_COLUMN_MAP['construction_max']:
                filter_query += f" AND {FILTER_COLUMN_MAP['construction_max']} <= {filters['construction_max']}"
            print(f"filter_query: {filter_query}")
            breakpoint()
            # Operation/price consolidation for QRO (venta/renta variants)
            if city in ('queretaro','el_marques'):
                if filters.get('price_type'):
                    pt = str(filters['price_type']).lower()
                    if pt == 'rent':
                        filter_query += " AND lower(mdc.operation_type) = 'renta'"
                        if filters.get('price_min') is not None:
                            filter_query += f" AND mdc.rent_price_clean >= {filters['price_min']}"
                        if filters.get('price_max') is not None:
                            filter_query += f" AND mdc.rent_price_clean <= {filters['price_max']}"
                    elif pt == 'buy':
                        filter_query += " AND lower(mdc.operation_type) = 'venta'"
                        if filters.get('price_min') is not None:
                            filter_query += f" AND mdc.buy_price_clean >= {filters['price_min']}"
                        if filters.get('price_max') is not None:
                            filter_query += f" AND mdc.buy_price_clean <= {filters['price_max']}"
                elif filters.get('operation_type'):
                    # free-form operation_type support ensures Renta/renta, Venta/venta
                    op = filters['operation_type']
                    if isinstance(op, list):
                        mapped = ["'renta'" if str(x).lower()== 'rent' else f"'{str(x).lower()}'" for x in op]
                        filter_query += f" AND lower(mdc.operation_type) IN ({','.join(mapped)})"
                    else:
                        filter_query += f" AND lower(mdc.operation_type) = '{str(op).lower()}'"
            else:
                # Mexico branch unchanged
                if 'price_type' in filters and filters['price_type']:
                    price_type = filters['price_type'].lower()
                    price_fields = FILTER_COLUMN_MAP.get(price_type, [])
                    if price_fields:
                        if isinstance(price_fields, list):
                            if 'price_min' in filters and filters['price_min'] is not None:
                                min_conditions = " OR ".join([f"{field} >= {filters['price_min']}" for field in price_fields])
                                filter_query += f" AND ({min_conditions})"
                            if 'price_max' in filters and filters['price_max'] is not None:
                                max_conditions = " OR ".join([f"{field} <= {filters['price_max']}" for field in price_fields])
                                filter_query += f" AND ({max_conditions})"
            
            # Geometry (location on block)
            if 'geometry' in filters and filters['geometry'] and FILTER_COLUMN_MAP['geometry']:
                filter_query += f" AND {FILTER_COLUMN_MAP['geometry']} = '{filters['geometry']}'"
            
            # Municipality fields 
            for key in ["id_municipality", "municipality", "alcaldia", "colonia"]:
                if key in filters and filters[key] and FILTER_COLUMN_MAP[key]:
                    if key == "id_municipality":
                        # For QRO, we'll handle id_municipality translation after cursor is created
                        # For other cities, use direct mapping
                        if city != 'queretaro' and city != 'el_marques':
                            if isinstance(filters[key], list):
                                filter_query += f" AND {FILTER_COLUMN_MAP[key]} in ({','.join([str(f) for f in filters[key]])}) "
                            else:
                                filter_query += f" AND {FILTER_COLUMN_MAP[key]} = {filters[key]}"
                    else:
                        # Handle other municipality fields - extract name from object if needed
                        value = filters[key]
                        if isinstance(value, dict) and 'name' in value:
                            value = value['name']
                        elif isinstance(value, list) and len(value) > 0:
                            # If it's a list of objects, extract names
                            if isinstance(value[0], dict) and 'name' in value[0]:
                                value = value[0]['name']
                            else:
                                value = str(value[0])
                        elif not isinstance(value, str):
                            value = str(value)
                        
                        filter_query += f" AND {FILTER_COLUMN_MAP[key]} ILIKE '%{value}%'"
            
            # Remove separate operation_type block for QRO to avoid duplication; handled above
            
            # Property Dimension filters (New for QRO)
            if 'dimension_min' in filters and filters['dimension_min'] is not None and FILTER_COLUMN_MAP.get('dimension_min'):
                filter_query += f" AND {FILTER_COLUMN_MAP['dimension_min']} >= {filters['dimension_min']}"
            if 'dimension_max' in filters and filters['dimension_max'] is not None and FILTER_COLUMN_MAP.get('dimension_max'):
                filter_query += f" AND {FILTER_COLUMN_MAP['dimension_max']} <= {filters['dimension_max']}"

            
            # connection = self.db.connect('redshiftdb')
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Handle QRO id_municipality translation after cursor is created
            if (city == 'queretaro' or city == 'el_marques') and 'id_municipality' in filters and filters['id_municipality']:
                staging_ids = filters['id_municipality'] if isinstance(filters['id_municipality'], list) else [filters['id_municipality']]
                logger.info(f"[MUNICIPALITY TRANSLATION] Converting staging IDs {staging_ids} to cve_mun values")
                
                # Query to get cve_mun values for the given id_stg_municipality values
                translation_query = f"""
                    SELECT DISTINCT v.cve_mun 
                    FROM blackprint_db_prd.data_product.v_qro v
                    JOIN staging.stg_municipality sm ON v.nom_mun = sm.d_mnpio
                    WHERE sm.id_stg_municipality IN ({','.join([str(id) for id in staging_ids])})
                """
                
                cursor.execute(translation_query)
                cve_mun_results = cursor.fetchall()
                cve_mun_values = [str(row['cve_mun']) for row in cve_mun_results]
                
                if cve_mun_values:
                    logger.info(f"[MUNICIPALITY TRANSLATION] Mapped staging IDs {staging_ids} to cve_mun values {cve_mun_values}")
                    # filter_query += f" AND v.cve_mun IN ({','.join(cve_mun_values)})"
                else:
                    logger.warning(f"[MUNICIPALITY TRANSLATION] No cve_mun mapping found for staging IDs {staging_ids}")
            
            query = self.qc.get_property_query(filter_query, city=city)
            query = query + " limit 15"
            # Log the generated query for debugging
            logger.info(f"[FILTER QUERY] Generated SQL for city {city}: {query}")
            cursor.execute(query)
            result = cursor.fetchall()
            if not result:
                logger.info(f"[FILTER RESULT] No properties found for city: {city}")
                return Response.success(data=[], message='No properties found')
            result_jsons = self.get_property_json(result, show_all_keys=filters.get('show_all_keys', True), city=city)
            # Log the final result count
            logger.info(f"[FILTER RESULT] Found {len(result_jsons)} properties for city: {city}")
            resp = Response.success(data=result_jsons, message='Success')
        except Exception as e:
            logger.error("Error filtering properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
            return resp

    def advanced_municipality_search(self, search_key_type=None, search_value=None, municipality_nm=None, config_city='mexico'):
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            allowed_keys = {"nb_cleaned", "zip_code", "municipality_nm"}
            
            # Determine which table and columns to use based on config_city
            if config_city == 'queretaro' or config_city == 'el_marques':
                # Use staging.stg_municipality for QRO/El Marques
                table_name = 'presentation.dim_municipality_qro'
                id_col = 'id_stg_demographic_socioeconomic_qro'
                # Map search_key_type to stg_municipality columns
                column_map = {
                    "nb_cleaned": "city",  # neighborhood/settlement
                    "zip_code": "zip_code",        # zip code
                    "municipality_nm": "municipality"  # municipality
                }
            else:
                # Use data_product.v_municipality for other cities
                table_name = 'data_product.v_municipality'
                id_col = 'id_municipality'
                column_map = {
                    "nb_cleaned": "nb_cleaned",
                    "zip_code": "zip_code", 
                    "municipality_nm": "municipality_nm"
                }
            
            # If both municipality_nm and (search_key_type + search_value) are provided, filter within municipality_nm
            if municipality_nm and search_key_type in {"nb_cleaned", "zip_code"} and search_value:
                search_column = column_map.get(search_key_type)
                municipality_column = column_map.get("municipality_nm")
                
                # Use GROUP BY to eliminate duplicates for all cities
                query = f"""
                    SELECT {search_column} as {search_key_type}, MIN({id_col}) as id_municipality
                    FROM {table_name} 
                    WHERE {municipality_column} ILIKE %s AND {search_column} ILIKE %s 
                    GROUP BY {search_column}
                    ORDER BY {search_column}
                    LIMIT 50
                """
                cursor.execute(query, (f"%{municipality_nm}%", f"%{search_value}%"))
                results = cursor.fetchall()
                items = [{"id": row["id_municipality"], "name": row[search_key_type]} for row in results]
                message = "Municipality IDs found" if items else "No municipality ID found"
                return Response.success(
                    data={
                        "municipalities": items,
                        "search_key_type": search_key_type,
                        "search_value": search_value,
                        "municipality_nm": municipality_nm,
                        "config_city": config_city
                    },
                    message=message
                )
            elif search_key_type == "municipality_nm" and search_value:
                municipality_column = column_map.get("municipality_nm")
                
                query = f"""
                    SELECT {municipality_column} as municipality_nm, {id_col} as id_municipality
                    FROM (
                        SELECT {municipality_column}, {id_col},
                            ROW_NUMBER() OVER (PARTITION BY {municipality_column} ORDER BY {id_col}) as row_num
                        FROM {table_name}
                        WHERE {municipality_column} ILIKE %s
                    ) t
                    WHERE row_num = 1
                    LIMIT 50
                """
                cursor.execute(query, (f"%{search_value}%",))

                results = cursor.fetchall()
                items = [{"id": row["id_municipality"], "name": row["municipality_nm"]} for row in results]
                message = "Municipality IDs found" if items else "No municipality ID found"
                return Response.success(
                    data={
                        "municipalities": items,
                        "search_key_type": search_key_type,
                        "search_value": search_value,
                        "municipality_nm": municipality_nm,
                        "config_city": config_city
                    },
                    message=message
                )
            else:
                return Response.bad_request(message="Invalid or missing search parameters")
        except Exception as e:
            logger.error("Error in advanced municipality search: %s", str(e), exc_info=True)
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
