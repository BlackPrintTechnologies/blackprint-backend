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
# from flask import request
import time
from datetime import datetime
import logging
from logsmanager.logging_config import setup_logging
from concurrent.futures import ThreadPoolExecutor
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

    def get_additional_property_details(self, properties):
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
            fid_filter = f"WHERE fid IN ({','.join(fids)})"
            
            # Get full property details from Redshift using existing query controller
            redshift_connection = self.redshift_connection.connect()
            redshift_cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
            property_query = self.qc.get_property_query(fid_filter)
            logger.info("PROPERTY QUERY I AM GETTING %s",property_query)
            redshift_cursor.execute(property_query)
            property_results = redshift_cursor.fetchall()
            # logger.info("Properties result i am getting %s",property_results[0])
            # Process results using existing property JSON formatter
            if property_results:
                property_controller = PropertyController()
                formatted_results = property_controller.get_property_json(property_results)
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

    
    def get_user_properties(self, user_id, fid=None,  prop_status=None):
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
            
            query += "order by updated_at desc"
            logger.debug("Executing query: %s", query)
            cursor.execute(query)
            result = cursor.fetchall()
            logger.info("Fetched %d user properties", len(result))
            resp = self.get_additional_property_details(result)
        except Exception as e:
            logger.error("Error fetching user properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp
        
    def update_property_request_status(self, fid, user, request_status=1):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = f'''update bp_user_property set request_status = {request_status},  updated_at = now()  where fid = {fid} and user_id = {user} returning id'''
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

    def add_user_property(self, fid, user_id, prop_status):
        connection = None
        cursor = None
        try:
            logger.info("Adding user property with fid=%s, user_id=%s, status=%s", fid, user_id, prop_status)
            start_time = time.time()
            connection = self.db.connect()
            cursor = connection.cursor()
            query = f"INSERT INTO bp_user_property (fid, user_id, user_property_status) VALUES ({fid}, {user_id}, '{prop_status}')"
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
        
    def update_property_status(self, user_id, fid , prop_status='view'):
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
            query = f"UPDATE bp_user_property SET user_property_status = '{prop_status}', updated_at = now()  WHERE fid = {fid} AND user_id = {user_id}"
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
    def get_requested_properties(self, user_id):
        """Get all properties that have been requested by a specific user"""
        connection = None
        cursor = None
        redshift_connection = None
        redshift_cursor = None
        resp = None  # Initialize resp at the start
        try:
            logger.info("Fetching requested properties for user_id: %s", user_id)
            # First get all requested property FIDs from PostgreSQL
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            fid_query = f'''
                SELECT *
                FROM bp_user_property 
                WHERE user_id = {user_id} 
                AND request_status = 1
                order by updated_at desc
            '''
            # print("FID QUERY I AM GETTING %s", fid_query)
            cursor.execute(fid_query)
            fid_results = cursor.fetchall()
            resp  = self.get_additional_property_details(fid_results)
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
        self.db = Database()
        self.redshift_connection = RedshiftDatabase()
        # self.db = postgres_pool
        # self.redshift_connection = redshift_pool
        self.qc = QueryController()

    def _fetch_inmuebles24_images(self, ids_market_data_inmuebles24):
        """Fetch images from inmuebles24 table for the given id from Redshift."""
        connection = None
        cursor = None
        images = []
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = '''SELECT pictures FROM presentation.dim_market_data_inmuebles24 WHERE id_market_data_inmuebles24 = %s LIMIT 1'''
            cursor.execute(query, (ids_market_data_inmuebles24,))
            row = cursor.fetchone()
            if row and row.get('pictures'):
                # pictures is expected to be a JSON array or comma-separated string
                try:
                    # Try to parse as JSON
                    images = json.loads(row['pictures']) if isinstance(row['pictures'], str) else row['pictures']
                    if isinstance(images, str):
                        # If still a string, split by comma
                        images = [img.strip() for img in images.split(',') if img.strip()]
                except Exception:
                    # Fallback: split by comma
                    images = [img.strip() for img in row['pictures'].split(',') if img.strip()]
        except Exception as e:
            logger.error(f"Error fetching inmuebles24 images: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)
        return images

    def get_property_json(self, results):
        resp = []
        try:
            logger.info("Processing property JSON for %d results", len(results))
            for result in results:
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
                    "crecimiento_promedio_municipal": result.get("crecimiento_promedio_municipal", None),
                    "crecimiento_promedio_entidad": result.get("crecimiento_promedio_entidad", None),
                    "crecimiento_promedio_ageb": result.get("crecimiento_promedio_ageb", None)
                }
                # --- Set street_images based on inmuebles24 or street view ---
                ids_market_data_inmuebles24 = property_details.get("ids_market_data_inmuebles24")
                prop_lat = property_details.get("lat")
                prop_lng = property_details.get("lng")
                street_images = []
                print("IDS_MARKET_DATA_INMUEBLES24", ids_market_data_inmuebles24)
                selected_id = None
                if ids_market_data_inmuebles24:
                    ids = [id_.strip() for id_ in str(ids_market_data_inmuebles24).split(',') if id_.strip()]
                    ids = [id_ for id_ in ids if id_ != '-1']
                    if ids:
                        selected_id = ids[0]
                if selected_id:
                    images = self._fetch_inmuebles24_images(selected_id)
                    print("IMAGES", images)
                    if images:
                        # Resize images to 600x300
                        resized_images = [
                            img.replace("1200x1200", "600x300") if "1200x1200" in img else img
                            for img in images[:6]
                        ]
                        street_images = resized_images
                if not street_images and prop_lat and prop_lng:
                    pano_id = get_street_view_metadata_cached(float(prop_lat), float(prop_lng))
                    if pano_id:
                        headings = [0, 45, 90, 135, 180, 225, 270, 315]
                        fov = 90
                        size = "600x300"
                        street_images = [
                            f"{BASE_URL}/properties/street_view_image?pano_id={pano_id}&heading={heading}&fov={fov}&size={size}"
                            for heading in headings
                        ]
                property_details["street_images"] = street_images

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

                traffic = {
                    "front" : {
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
                    "motor_vehicle_avg_x_day_of_week_7_front": result["motor_vehicle_avg_x_day_of_week_7_front"],
                    } ,
                    "500" : {
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
                    } 
                    }
                
                resp.append( {
                            "property_details": property_details,
                            "market_info": market_info,
                            "pois": pois,
                            "traffic": traffic
                        })
            return resp
        except Exception as e:
            logger.error("Error processing property JSON: %s", str(e), exc_info=True)
            raise e

    def get_properties(self, current_user, fid=None, lat=None, lng=None):
        from utils.streetViewUtils import get_street_view_metadata_cached
        import copy
        connection = None
        cursor = None
        resp = None
        try:
            logger.info("Fetching properties for user=%s, fid=%s, lat=%s, lng=%s", current_user, fid, lat, lng)
            filter_query = 'WHERE 1=1'
            if fid:
                filter_query += f" AND fid = {fid}"
            elif lat and lng:
                h3Index = h3.latlng_to_cell(float(lat), float(lng), 13)
                h3_index_decimal = str(int(h3Index, 16))
                filter_query += f" AND h3_indexes ILIKE '%{h3_index_decimal}%'"
            else:
                logger.warning("Invalid request: Missing fid or lat/lng")
                return Response.bad_request(message="Invalid request")

            query = self.qc.get_property_query(filter_query)

            def fetch_property_details():
                conn = self.redshift_connection.connect()
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(query)
                result = cur.fetchall()
                cur.close()
                self.redshift_connection.disconnect(conn)
                return result

            def fetch_pano_id(lat, lng):
                return get_street_view_metadata_cached(float(lat), float(lng))

            with ThreadPoolExecutor(max_workers=2) as executor:
                future_details = executor.submit(fetch_property_details)
                # Wait for property details to get lat/lng
                result = future_details.result()
                if not result:
                    return Response.not_found(message="Property not found")
                result_jsons = self.get_property_json(result)
                print("RESULT_JSONS",result_jsons)
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
                    upc.add_user_property(fid, current_user, 'view')
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

    def get_property_market_info(self, spot2_id, inmuebles24_id, propiedades_id):
        connection = None
        cursor = None
        try:
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info("Fetching market infor for  spot2_id=%s, inmuebles24_id=%s, propiedades_id=%s",  spot2_id, inmuebles24_id, propiedades_id)
            query = self.qc.get_market_info_query(spot2_id, inmuebles24_id, propiedades_id)
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
    def get_demographic_json(result):
        # print("DEMOGRAPHIC RESULT",result)
        result = result[0]
        try:
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
               "population_growth":{
                   "block" : { 
                       "2000" : result['pob_2000_ageb'],
                       "2005": result['pob_2005_ageb'],
                       "2010": result['pob_2010_ageb'],
                       "2015": result['pob_2015_ageb'],
                       "2020": result['pob_2020_ageb']
                   },
                   "colonia":{
                       "2000": result['pob_2000_entidad'],
                       "2005": result['pob_2005_entidad'],
                       "2010": result['pob_2010_entidad'],
                       "2015": result['pob_2015_entidad'],
                       "2020": result['pob_2020_entidad']
                     
                   },
                   "alcaldia":{
                       "2000": result['pob_2000_municipal'],
                       "2005": result['pob_2010_municipal'],
                       "2010": result['pob_2005_municipal'],
                       "2015": result['pob_2015_municipal'],
                       "2020": result['pob_2020_municipal']
                   }
               }
               
                }
            
            return demographic
        except Exception as e:
            raise e
    # @cache_response(prefix='demographic',expiration=3600)
    
    def get_property_demographic(self, fid, current_user):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info("Fetching demographic data for fid=%s, user=%s", fid, current_user)
            start_time = time.time()
            connection = self.redshift_connection.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.qc.get_demographics_query(fid)
            logger.debug("Executing query: %s", query)

            cursor.execute(query)
            res = cursor.fetchall()
            logger.info("Fetched demographic data for fid=%s", fid)
            if res:
                response = self.get_demographic_json(res)
                json_time = time.time()  # Time after JSON conversion
                
                upc = UserPropertyController()
                # print(f"Calling add_user_property with fid={fid}, current_user={current_user}", flush=True)

                upc.add_user_property(fid, current_user, 'view')
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

    def get_property_traffic(self, fid):
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

            query = f"""
                SELECT 
                    type,
                    min_pedestrian,
                    max_pedestrian,
                    min_motor_vehicle,
                    max_motor_vehicle
                FROM blackprint_db_prd.presentation.dataset_mobility_data_h3
                WHERE fid = %s AND type IN ('CIRCLE_500_METERS', 'FRONT_OF_STORE')
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

            return Response.success(data={"response": response})

        except Exception as e:
            return Response.internal_server_error(message=str(e))

        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_connection.disconnect(connection)

