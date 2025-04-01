from utils.dbUtils import Database, RedshiftDatabase
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from module.properties.new_query import QueryController
import json
import h3
from utils.cacheUtlis import cache_response
from utils.iconUtils import IconMapper
from utils.streetViewUtils import get_street_view_metadata
from flask import request
import time
from datetime import datetime
import logging
from logsmanager.logging_config import setup_logging
setup_logging()
logger = logging.getLogger(__name__)
logger.info("I am testing the new query controller")
class UserPropertyController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.qc = QueryController()
    
    def get_user_properties(self, prop_status=None):
        connection = None
        cursor = None
        try:
            logger.info("Fetching user properties with status: %s", prop_status)
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = 'SELECT * FROM bp_user_property WHERE status = 1'
            if prop_status:
                query += f" AND user_property_status = '{prop_status}'"
            logger.debug("Executing query: %s", query)

            cursor.execute(query)
            result = cursor.fetchall()
            logger.info("Fetched %d user properties", len(result))
            resp = Response.success(data=result, message='Success')
        except Exception as e:
            logger.error("Error fetching user properties: %s", str(e), exc_info=True)
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

class PropertyController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        # Initialize QueryController with all fields enabled by default
        self.qc = QueryController(
            poi=True,
            traffic=True,
            market_value=True,
            property=True,
            demographics=True
        )

    @staticmethod
    def get_property_json(results):
        resp = []
        try:
            logger.info("Processing property JSON for %d results", len(results))
            for result in results:
                property_details = {
                    "fid": result["fid"],
                    "lat": json.loads(result['centroid'])['coordinates'][1] if result['centroid'] else None,
                    "lng": json.loads(result['centroid'])['coordinates'][0] if result['centroid'] else None,
                    "is_on_market": result["is_on_market"],
                    "total_surface_area": result["total_surface_area"],
                    "total_construction_area": result["total_construction_area"],
                    "street_address": result["street_address"],
                    "year_built": result["year_built"],
                    "special_facilities": result["special_facilities"],
                    "unit_land_value": result["unit_land_value"],
                    "land_value": result["land_value"],
                    "usage_desc": result["usage_desc"],
                    "key_vus": result["key_vus"],
                    "predominant_level": result["predominant_level"],
                    "h3_indexes": result["h3_indexes"]
                }

                market_info = {
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
                    "open_space": result["open_space"],
                    "id_land_use": result["id_land_use"],
                    "id_municipality": result["id_municipality"],
                    "id_city_blocks": result["id_city_blocks"],
                    "total_houses": result["total_houses"],
                    "locality_size": result["locality_size"],
                    "city_link": result.get("city_link")
                }

                pois = {
                    "category": {
                        category: IconMapper.get_icon_url(category) 
                        for category in IconMapper.CATEGORY_ICON_MAP
                    },
                    "front": {
                        "brands_active_life_front": result["brands_active_life_front"],
                        "brands_arts_and_entertainment_front": result["brands_arts_and_entertainment_front"],
                        "brands_attractions_and_activities_front": result["brands_attractions_and_activities_front"],
                        "brands_automotive_front": result["brands_automotive_front"],
                        "brands_eat_and_drink_front": result["brands_eat_and_drink_front"],
                        "brands_education_front": result["brands_education_front"],
                        "brands_financial_service_front": result["brands_financial_service_front"],
                        "brands_health_and_medical_front": result["brands_health_and_medical_front"],
                        "brands_public_service_and_government_front": result["brands_public_service_and_government_front"],
                        "brands_retail_front": result["brands_retail_front"]
                    },
                    "500": {
                        "brands_active_life_500m": result["brands_active_life_500m"],
                        "brands_arts_and_entertainment_500m": result["brands_arts_and_entertainment_500m"],
                        "brands_attractions_and_activities_500m": result["brands_attractions_and_activities_500m"],
                        "brands_automotive_500m": result["brands_automotive_500m"],
                        "brands_eat_and_drink_500m": result["brands_eat_and_drink_500m"],
                        "brands_education_500m": result["brands_education_500m"],
                        "brands_financial_service_500m": result["brands_financial_service_500m"],
                        "brands_health_and_medical_500m": result["brands_health_and_medical_500m"],
                        "brands_public_service_and_government_500m": result["brands_public_service_and_government_500m"],
                        "brands_retail_500m": result["brands_retail_500m"]
                    },
                    "1000": {
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
                    }
                }

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
                    }
                }

                resp.append({
                    "property_details": property_details,
                    "market_info": market_info,
                    "pois": pois,
                    "traffic": traffic
                })
            return resp
        except Exception as e:
            logger.error("Error processing property JSON: %s", str(e), exc_info=True)
            raise e

    @staticmethod
    def get_demographic_json(result):
        try:
            demographic = {
                "general": {
                    "block": {
                        "neighborhood": result[0]["neighborhood"],
                        "predominant_level": result[0]["predominant_level"],
                        "ageb_code": result[0]["ageb_code"],
                        "total_household": result[0]["vivtot"],
                        "average_household_size": result[0]["prom_ocup"],
                        "average_number_of_rooms": result[0]["pro_ocup_c"]
                    },
                    "colonia": {
                        "neighborhood": result[0]["neighborhood"],
                        "predominant_level": result[0]["predominant_level"],
                        "ageb_code": result[0]["ageb_code"],
                        "total_household": result[0]["vivtot_colonia"],
                        "average_household_size": result[0]["prom_ocup_colonia"],
                        "average_number_of_rooms": result[0]["pro_ocup_c_colonia"]
                    },
                    "alcaldia": {
                        "neighborhood": result[0]["neighborhood"],
                        "predominant_level": result[0]["predominant_level"],
                        "ageb_code": result[0]["ageb_code"],
                        "total_household": result[0]["vivtot"],
                        "average_household_size": result[0]["prom_ocup_alcaldia"],
                        "average_number_of_rooms": result[0]["pro_ocup_c_alcaldia"]
                    }
                },
                "socio_economic_level": {
                    "block": {
                        "ses_ab": result[0]["ses_ab"],
                        "ses_c_plus": result[0]["ses_c_plus"],
                        "ses_c": result[0]["ses_c"],
                        "ses_c_minus": result[0]["ses_c_minus"],
                        "ses_d": result[0]["ses_d"],
                        "ses_d_plus": result[0]["ses_d_plus"],
                        "ses_e": result[0]["ses_e"]
                    },
                    "colonia": {
                        "ses_ab": result[0]["ses_ab_colonia"],
                        "ses_c_plus": result[0]["ses_c_plus_colonia"],
                        "ses_c": result[0]["ses_c_colonia"],
                        "ses_c_minus": result[0]["ses_c_minus_colonia"],
                        "ses_d": result[0]["ses_d_colonia"],
                        "ses_d_plus": result[0]["ses_d_plus_colonia"],
                        "ses_e": result[0]["ses_e_colonia"]
                    },
                    "alcaldia": {
                        "ses_ab": result[0]["ses_ab_alcaldia"],
                        "ses_c_plus": result[0]["ses_c_plus_alcaldia"],
                        "ses_c": result[0]["ses_c_alcaldia"],
                        "ses_c_minus": result[0]["ses_c_minus_alcaldia"],
                        "ses_d": result[0]["ses_d_alcaldia"],
                        "ses_d_plus": result[0]["ses_d_plus_alcaldia"],
                        "ses_e": result[0]["ses_e_alcaldia"]
                    }
                },
                "population": {
                    "block": {
                        "total_population": result[0]["pobtot"],
                        "male_population": result[0]["pobmas"],
                        "female_population": result[0]["pobfem"],
                    },
                    "colonia": {
                        "total_population": result[0]["pobtot_colonia"],
                        "male_population": result[0]["pobmas_colonia"],
                        "female_population": result[0]["pobfem_colonia"],
                    },
                    "alcaldia": {
                        "total_population": result[0]["pobtot_alcaldia"],
                        "male_population": result[0]["pobmas_alcaldia"],
                        "female_population": result[0]["pobfem_alcaldia"],
                    }
                },
                "education": {
                    "block": {
                        "education_3_5": result[0]["p_3a5"],
                        "education_6_11": result[0]["p_6a11"],
                        "education_12_14": result[0]["p_12a14"],
                        "education_15_17": result[0]["p_15a17"],
                        "education_18_24": result[0]["p_18a24"],
                        "education_3_5_attending_school": result[0]["p3a5_noa"],
                        "education_6_11_attending_school": result[0]["p6a11_noa"],
                        "education_12_14_attending_school": result[0]["p12a14noa"],
                        "education_15_17_attending_school": result[0]["p15a17a"],
                        "education_18_24_attending_school": result[0]["p18a24a"]
                    },
                    "colonia": {
                        "education_3_5": result[0]["p_3a5_colonia"],
                        "education_6_11": result[0]["p_6a11_colonia"],
                        "education_12_14": result[0]["p_12a14_colonia"],
                        "education_15_17": result[0]["p_15a17_colonia"],
                        "education_18_24": result[0]["p_18a24_colonia"],
                        "education_3_5_attending_school": result[0]["p3a5_noa_colonia"],
                        "education_6_11_attending_school": result[0]["p6a11_noa_colonia"],
                        "education_12_14_attending_school": result[0]["p12a14noa_colonia"],
                        "education_15_17_attending_school": result[0]["p15a17a_colonia"],
                        "education_18_24_attending_school": result[0]["p18a24a_colonia"]
                    },
                    "alcaldia": {
                        "education_3_5": result[0]["p_3a5_alcaldia"],
                        "education_6_11": result[0]["p_6a11_alcaldia"],
                        "education_12_14": result[0]["p_12a14_alcaldia"],
                        "education_15_17": result[0]["p_15a17_alcaldia"],
                        "education_18_24": result[0]["p_18a24_alcaldia"],
                        "education_3_5_attending_school": result[0]["p3a5_noa_alcaldia"],
                        "education_6_11_attending_school": result[0]["p6a11_noa_alcaldia"],
                        "education_12_14_attending_school": result[0]["p12a14noa_alcaldia"],
                        "education_15_17_attending_school": result[0]["p15a17a_alcaldia"],
                        "education_18_24_attending_school": result[0]["p18a24a_alcaldia"]
                    }
                },
                "workforce": {
                    "block": {
                        "total_workforce": result[0]["pea"],
                        "total_male_workforce": result[0]["pea_m"],
                        "total_female_workforce": result[0]["pea_f"],
                        "total_inactive_population": result[0]["pe_inac"],
                        "total_inactive_male_population": result[0]["pe_inac_m"],
                        "total_inactive_female_population": result[0]["pe_inac_f"]
                    },
                    "colonia": {
                        "total_workforce": result[0]["pea_colonia"],
                        "total_male_workforce": result[0]["pea_m_colonia"],
                        "total_female_workforce": result[0]["pea_f_colonia"],
                        "total_inactive_population": result[0]["pe_inac_colonia"],
                        "total_inactive_male_population": result[0]["pe_inac_m_colonia"],
                        "total_inactive_female_population": result[0]["pe_inac_f_colonia"]
                    },
                    "alcaldia": {
                        "total_workforce": result[0]["pea_alcaldia"],
                        "total_male_workforce": result[0]["pea_m_alcaldia"],
                        "total_female_workforce": result[0]["pea_f_alcaldia"],
                        "total_inactive_population": result[0]["pe_inac_alcaldia"],
                        "total_inactive_male_population": result[0]["pe_inac_m_alcaldia"],
                        "total_inactive_female_population": result[0]["pe_inac_f_alcaldia"]
                    }
                },
                "employment": {
                    "block": {
                        "total_employed_population": result[0]["pocupada"],
                        "total_male_employed_population": result[0]["pocupada_m"],
                        "total_female_emloyed_population": result[0]["pocupada_f"],
                        "total_unemployed_population": result[0]["pdesocup"],
                        "total_unemployed_male_population": result[0]["pdesocup_m"],
                        "total_unemployed_female_population": result[0]["pdesocup_f"]
                    },
                    "colonia": {
                        "total_employed_population": result[0]["pocupada_colonia"],
                        "total_male_employed_population": result[0]["pocupada_m_colonia"],
                        "total_female_emloyed_population": result[0]["pocupada_f_colonia"],
                        "total_unemployed_population": result[0]["pdesocup_colonia"],
                        "total_unemployed_male_population": result[0]["pdesocup_m_colonia"],
                        "total_unemployed_female_population": result[0]["pdesocup_f_colonia"]
                    },
                    "alcaldia": {
                        "total_employed_population": result[0]["pocupada_alcaldia"],
                        "total_male_employed_population": result[0]["pocupada_m_alcaldia"],
                        "total_female_emloyed_population": result[0]["pocupada_f_alcaldia"],
                        "total_unemployed_population": result[0]["pdesocup_alcaldia"],
                        "total_unemployed_male_population": result[0]["pdesocup_m_alcaldia"],
                        "total_unemployed_female_population": result[0]["pdesocup_f_alcaldia"],
                    }
                }
            }

            return demographic
        except Exception as e:
            raise e

    def get_properties(self, current_user, fid=None, lat=None, lng=None):
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
                print("h3_index_decimal", h3_index_decimal)
                filter_query += f" AND h3_indexes ILIKE '%{h3_index_decimal}%'"
                print("filter_query", filter_query)
            else:
                logger.warning("Invalid request: Missing fid or lat/lng")
                return Response.bad_request(message="Invalid request")

            # Clean up any trailing commas and spaces
            filter_query = filter_query.rstrip(', ')
            
            query = self.qc.get_property_query(filter_query)
            st = time.time()
            logger.debug("Executing query: %s", query)
            connection = self.redshift_db.connect()
            print("time taken to connect to redshift", time.time()-st)
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            print("time for cursor creation", time.time()-st)
            cursor.execute(query)
            print("time for query execution", time.time()-st)
            result = cursor.fetchall()
            print("time for fetchall", time.time()-st)
            logger.info("Fetched %d properties", len(result))
            
            # Debug log for first result
            if result:
                logger.debug("First result fields: %s", list(result[0].keys()))
                logger.debug("First result street_address: %s", result[0].get("street_address"))
            
            if not result:
                resp = Response.not_found(message="Property not found")
            else:
                result_jsons = self.get_property_json(result)
                
                # Debug log for processed JSON
                if result_jsons:
                    logger.debug("First processed JSON property_details: %s", result_jsons[0].get("property_details", {}))
                
                # Adding Street View Images to property_details
                if lat and lng:
                    pano_id = get_street_view_metadata(float(lat), float(lng))
                    if pano_id:
                        headings = [0, 45, 90, 135, 180, 225, 270, 315]  # Front, front-right, right, back-right, back, back-left, left, front-left
                        fov = 90  # Field of view
                        size = "600x300"  # Image size
                        base_url = request.host_url.rstrip('/')  # Get the base URL
                        street_images = [
                            f"{base_url}/properties/street_view_image?pano_id={pano_id}&heading={heading}&fov={fov}&size={size}"
                            for heading in headings
                        ]
                        result_jsons[0]["property_details"]["street_images"] = street_images
                    else:
                        result_jsons[0]["property_details"]["street_images"] = []
                        
                if fid:
                    result_json = result_jsons[0]
                else:
                    result_json = result_jsons
                upc = UserPropertyController()
                if fid:
                    upc.add_user_property(fid, current_user, 'view')
                resp = Response.success(data=result_json, message='Success')
        except Exception as e:
            logger.error("Error fetching properties: %s", str(e), exc_info=True)
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_property_demographic(self, fid, current_user):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info("Fetching demographic data for fid=%s, user=%s", fid, current_user)
            start_time = time.time()
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.qc.get_demographics_query(fid)
            logger.debug("Executing query: %s", query)

            cursor.execute(query)
            res = cursor.fetchall()
            logger.info("Fetched demographic data for fid=%s", fid)
            if res:
                response = self.get_demographic_json(res)
                json_time = time.time()
                
                upc = UserPropertyController()
                upc.add_user_property(fid, current_user, 'view')
                add_property_time = time.time()
                
                resp = Response.success(data=response, message='Success')
            else:
                resp = Response.bad_request(message="Property not found")
                logger.info("No property found")
        except Exception as e:
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_property_pois_and_demographics(self, fid):
        connection = None
        cursor = None
        resp = None
        try:
            # Initialize QueryController with specific parameters for this method
            qc = QueryController(
                poi=True,       # Include POI fields
                demographics=True,  # Include demographic fields
                property=True,  # Include basic property fields
                market_value=False,  # Exclude market value fields
                traffic=False   # Exclude traffic fields
            )
            
            query = qc.get_property_query(f"WHERE fid = {fid}")
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchall()
            
            if result:
                property_details, _, pois, _ = self.get_property_json(result)
                demographic_data = self.get_demographic_json(result)
                result_json = {
                    "property_details": property_details,
                    "pois": pois,
                    "demographics": demographic_data
                }
                resp = Response.success(data=result_json, message='Success')
            else:
                resp = Response.bad_request(message="Property not found")
        except Exception as e:
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_property_pois(self, fid):
        connection = None
        cursor = None
        resp = None
        try:
            # Initialize QueryController with only POI fields
            qc = QueryController(
                poi=True,
                property=True,
                market_value=False,
                traffic=False,
                demographics=False
            )
            
            query = qc.get_property_query(f"WHERE fid = {fid}")
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchall()
            
            if result:
                property_details, _, pois, _ = self.get_property_json(result)
                result_json = {
                    "property_details": property_details,
                    "pois": pois
                }
                resp = Response.success(data=result_json, message='Success')
            else:
                resp = Response.bad_request(message="Property not found")
        except Exception as e:
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_property_traffic(self, fid):
        connection = None
        cursor = None
        resp = None
        try:
            # Initialize QueryController with only traffic fields
            qc = QueryController(
                traffic=True,
                property=True,
                market_value=False,
                poi=False,
                demographics=False
            )
            
            query = qc.get_property_query(f"WHERE fid = {fid}")
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchall()
            
            if result:
                property_details, _, _, traffic = self.get_property_json(result)
                result_json = {
                    "property_details": property_details,
                    "traffic": traffic
                }
                resp = Response.success(data=result_json, message='Success')
            else:
                resp = Response.bad_request(message="Property not found")
        except Exception as e:
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp