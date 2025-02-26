from utils.dbUtils import Database, RedshiftDatabase
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
import json

class PropertyController :
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()


    @staticmethod
    def get_property_query(fid):
        query = f'''
                Select  
                fid,
                centroid,
                is_on_market,
                total_surface_area,
                total_construction_area,
                year_built,
                special_facilities,
                unit_land_value,
                land_value,
                key_vus,
                predominant_level,
                total_houses,
                locality_size,
                floor_levels,
                open_space,
                id_land_use,
                id_municipality,
                id_city_blocks,
                rent_price_spot2,
                rent_price_per_m2_spot2,
                buy_price_spot2,
                buy_price_per_m2_spot2,
                total_area_spot2,
                property_type_spot2,
                rent_price_inmuebles24,
                rent_price_per_m2_inmuebles24,
                buy_price_inmuebles24,
                buy_price_per_m2_inmuebles24,
                total_area_inmuebles24,
                property_type_inmuebles24,
                rent_price_propiedades,
                rent_price_per_m2_propiedades,
                buy_price_propiedades,
                buy_price_per_m2_propiedades,
                total_area_propiedades,
                block_type,
                density_d,
                usage_desc,
                scope,
                brands_active_life_front,
                brands_arts_and_entertainment_front,
                brands_attractions_and_activities_front,
                brands_automotive_front,
                brands_eat_and_drink_front,
                brands_education_front,
                brands_financial_service_front,
                brands_health_and_medical_front,
                brands_public_service_and_government_front,
                brands_retail_front,
                brands_active_life_500m,
                brands_arts_and_entertainment_500m,
                brands_attractions_and_activities_500m,
                brands_automotive_500m,
                brands_eat_and_drink_500m,
                brands_education_500m,
                brands_financial_service_500m,
                brands_health_and_medical_500m,
                brands_public_service_and_government_500m,
                brands_retail_500m,
                brands_active_life_1km,
                brands_arts_and_entertainment_1km,
                brands_attractions_and_activities_1km,
                brands_automotive_1km,
                brands_eat_and_drink_1km,
                brands_education_1km,
                brands_financial_service_1km,
                brands_health_and_medical_1km,
                brands_public_service_and_government_1km,
                brands_retail_1km,
                at_rest_avg_x_hour_0_front,
                pedestrian_avg_x_hour_0_front,
                motor_vehicle_avg_x_hour_0_front,
                at_rest_avg_x_hour_1_front,
                pedestrian_avg_x_hour_1_front,
                motor_vehicle_avg_x_hour_1_front,
                at_rest_avg_x_hour_2_front,
                pedestrian_avg_x_hour_2_front,
                motor_vehicle_avg_x_hour_2_front,
                at_rest_avg_x_hour_3_front,
                pedestrian_avg_x_hour_3_front,
                motor_vehicle_avg_x_hour_3_front,
                at_rest_avg_x_hour_4_front,
                pedestrian_avg_x_hour_4_front,
                motor_vehicle_avg_x_hour_4_front,
                at_rest_avg_x_hour_5_front,
                pedestrian_avg_x_hour_5_front,
                motor_vehicle_avg_x_hour_5_front,
                at_rest_avg_x_hour_6_front,
                pedestrian_avg_x_hour_6_front,
                motor_vehicle_avg_x_hour_6_front,
                at_rest_avg_x_hour_7_front,
                pedestrian_avg_x_hour_7_front,
                motor_vehicle_avg_x_hour_7_front,
                at_rest_avg_x_hour_8_front,
                pedestrian_avg_x_hour_8_front,
                motor_vehicle_avg_x_hour_8_front,
                at_rest_avg_x_hour_9_front,
                pedestrian_avg_x_hour_9_front,
                motor_vehicle_avg_x_hour_9_front,
                at_rest_avg_x_hour_10_front,
                pedestrian_avg_x_hour_10_front,
                motor_vehicle_avg_x_hour_10_front,
                at_rest_avg_x_hour_11_front,
                pedestrian_avg_x_hour_11_front,
                motor_vehicle_avg_x_hour_11_front,
                at_rest_avg_x_hour_12_front,
                pedestrian_avg_x_hour_12_front,
                motor_vehicle_avg_x_hour_12_front,
                at_rest_avg_x_hour_13_front,
                pedestrian_avg_x_hour_13_front,
                motor_vehicle_avg_x_hour_13_front,
                at_rest_avg_x_hour_14_front,
                pedestrian_avg_x_hour_14_front,
                motor_vehicle_avg_x_hour_14_front,
                at_rest_avg_x_hour_15_front,
                pedestrian_avg_x_hour_15_front,
                motor_vehicle_avg_x_hour_15_front,
                at_rest_avg_x_hour_16_front,
                pedestrian_avg_x_hour_16_front,
                motor_vehicle_avg_x_hour_16_front,
                at_rest_avg_x_hour_17_front,
                pedestrian_avg_x_hour_17_front,
                motor_vehicle_avg_x_hour_17_front,
                at_rest_avg_x_hour_18_front,
                pedestrian_avg_x_hour_18_front,
                motor_vehicle_avg_x_hour_18_front,
                at_rest_avg_x_hour_19_front,
                pedestrian_avg_x_hour_19_front,
                motor_vehicle_avg_x_hour_19_front,
                at_rest_avg_x_hour_20_front,
                pedestrian_avg_x_hour_20_front,
                motor_vehicle_avg_x_hour_20_front,
                at_rest_avg_x_hour_21_front,
                pedestrian_avg_x_hour_21_front,
                motor_vehicle_avg_x_hour_21_front,
                at_rest_avg_x_hour_22_front,
                pedestrian_avg_x_hour_22_front,
                motor_vehicle_avg_x_hour_22_front,
                at_rest_avg_x_hour_23_front,
                pedestrian_avg_x_hour_23_front,
                motor_vehicle_avg_x_hour_23_front,
                at_rest_avg_x_day_of_week_1_front,
                pedestrian_avg_x_day_of_week_1_front,
                motor_vehicle_avg_x_day_of_week_1_front,
                at_rest_avg_x_day_of_week_2_front,
                pedestrian_avg_x_day_of_week_2_front,
                motor_vehicle_avg_x_day_of_week_2_front,
                at_rest_avg_x_day_of_week_3_front,
                pedestrian_avg_x_day_of_week_3_front,
                motor_vehicle_avg_x_day_of_week_3_front,
                at_rest_avg_x_day_of_week_4_front,
                pedestrian_avg_x_day_of_week_4_front,
                motor_vehicle_avg_x_day_of_week_4_front,
                at_rest_avg_x_day_of_week_5_front,
                pedestrian_avg_x_day_of_week_5_front,
                motor_vehicle_avg_x_day_of_week_5_front,
                at_rest_avg_x_day_of_week_6_front,
                pedestrian_avg_x_day_of_week_6_front,
                motor_vehicle_avg_x_day_of_week_6_front,
                at_rest_avg_x_day_of_week_7_front,
                pedestrian_avg_x_day_of_week_7_front,
                motor_vehicle_avg_x_day_of_week_7_front,
                at_rest_avg_x_day_of_week_1_500m,
                pedestrian_avg_x_day_of_week_1_500m,
                motor_vehicle_avg_x_day_of_week_1_500m,
                at_rest_avg_x_day_of_week_2_500m,
                pedestrian_avg_x_day_of_week_2_500m,
                motor_vehicle_avg_x_day_of_week_2_500m,
                at_rest_avg_x_day_of_week_3_500m,
                pedestrian_avg_x_day_of_week_3_500m,
                motor_vehicle_avg_x_day_of_week_3_500m,
                at_rest_avg_x_day_of_week_4_500m,
                pedestrian_avg_x_day_of_week_4_500m,
                motor_vehicle_avg_x_day_of_week_4_500m,
                at_rest_avg_x_day_of_week_5_500m,
                pedestrian_avg_x_day_of_week_5_500m,
                motor_vehicle_avg_x_day_of_week_5_500m,
                at_rest_avg_x_day_of_week_6_500m,
                pedestrian_avg_x_day_of_week_6_500m,
                motor_vehicle_avg_x_day_of_week_6_500m,
                at_rest_avg_x_day_of_week_7_500m,
                pedestrian_avg_x_day_of_week_7_500m,
                motor_vehicle_avg_x_day_of_week_7_500m,
                at_rest_avg_x_hour_0_500m,
                pedestrian_avg_x_hour_0_500m,
                motor_vehicle_avg_x_hour_0_500m,
                at_rest_avg_x_hour_1_500m,
                pedestrian_avg_x_hour_1_500m,
                motor_vehicle_avg_x_hour_1_500m,
                at_rest_avg_x_hour_2_500m,
                pedestrian_avg_x_hour_2_500m,
                motor_vehicle_avg_x_hour_2_500m,
                at_rest_avg_x_hour_3_500m,
                pedestrian_avg_x_hour_3_500m,
                motor_vehicle_avg_x_hour_3_500m,
                at_rest_avg_x_hour_4_500m,
                pedestrian_avg_x_hour_4_500m,
                motor_vehicle_avg_x_hour_4_500m,
                at_rest_avg_x_hour_5_500m,
                pedestrian_avg_x_hour_5_500m,
                motor_vehicle_avg_x_hour_5_500m,
                at_rest_avg_x_hour_6_500m,
                pedestrian_avg_x_hour_6_500m,
                motor_vehicle_avg_x_hour_6_500m,
                at_rest_avg_x_hour_7_500m,
                pedestrian_avg_x_hour_7_500m,
                motor_vehicle_avg_x_hour_7_500m,
                at_rest_avg_x_hour_8_500m,
                pedestrian_avg_x_hour_8_500m,
                motor_vehicle_avg_x_hour_8_500m,
                at_rest_avg_x_hour_9_500m,
                pedestrian_avg_x_hour_9_500m,
                motor_vehicle_avg_x_hour_9_500m,
                at_rest_avg_x_hour_10_500m,
                pedestrian_avg_x_hour_10_500m,
                motor_vehicle_avg_x_hour_10_500m,
                at_rest_avg_x_hour_11_500m,
                pedestrian_avg_x_hour_11_500m,
                motor_vehicle_avg_x_hour_11_500m,
                at_rest_avg_x_hour_12_500m,
                pedestrian_avg_x_hour_12_500m,
                motor_vehicle_avg_x_hour_12_500m,
                at_rest_avg_x_hour_13_500m,
                pedestrian_avg_x_hour_13_500m,
                motor_vehicle_avg_x_hour_13_500m,
                at_rest_avg_x_hour_14_500m,
                pedestrian_avg_x_hour_14_500m,
                motor_vehicle_avg_x_hour_14_500m,
                at_rest_avg_x_hour_15_500m,
                pedestrian_avg_x_hour_15_500m,
                motor_vehicle_avg_x_hour_15_500m,
                at_rest_avg_x_hour_16_500m,
                pedestrian_avg_x_hour_16_500m,
                motor_vehicle_avg_x_hour_16_500m,
                at_rest_avg_x_hour_17_500m,
                pedestrian_avg_x_hour_17_500m,
                motor_vehicle_avg_x_hour_17_500m,
                at_rest_avg_x_hour_18_500m,
                pedestrian_avg_x_hour_18_500m,
                motor_vehicle_avg_x_hour_18_500m,
                at_rest_avg_x_hour_19_500m,
                pedestrian_avg_x_hour_19_500m,
                motor_vehicle_avg_x_hour_19_500m,
                at_rest_avg_x_hour_20_500m,
                pedestrian_avg_x_hour_20_500m,
                motor_vehicle_avg_x_hour_20_500m,
                at_rest_avg_x_hour_21_500m,
                pedestrian_avg_x_hour_21_500m,
                motor_vehicle_avg_x_hour_21_500m,
                at_rest_avg_x_hour_22_500m,
                pedestrian_avg_x_hour_22_500m,
                motor_vehicle_avg_x_hour_22_500m,
                at_rest_avg_x_hour_23_500m,
                pedestrian_avg_x_hour_23_500m,
                motor_vehicle_avg_x_hour_23_500m
                FROM blackprint_db_prd.data_product.v_parcel_v3 
                where fid = {fid}               
        '''
        return query
    
    @staticmethod
    def get_property_json(result):
        try :
            property_details = {
                "fid": result[0]["fid"],
                "lat": json.loads(result[0]['centroid'])['coordinates'][1] if result[0]['centroid'] else None,
                "lng" : json.loads(result[0]['centroid'])['coordinates'][0] if result[0]['centroid'] else None,
                "is_on_market": result[0]["is_on_market"],
                "total_surface_area": result[0]["total_surface_area"],
                "total_construction_area": result[0]["total_construction_area"],
                "year_built": result[0]["year_built"],
                "special_facilities": result[0]["special_facilities"],
                "unit_land_value": result[0]["unit_land_value"],
                "land_value": result[0]["land_value"],
                "key_vus": result[0]["key_vus"],
                "predominant_level": result[0]["predominant_level"],
            }

            market_info = {
                "rent_price_spot2": result[0]["rent_price_spot2"],
                "rent_price_per_m2_spot2": result[0]["rent_price_per_m2_spot2"],
                "buy_price_spot2": result[0]["buy_price_spot2"],
                "buy_price_per_m2_spot2": result[0]["buy_price_per_m2_spot2"],
                "total_area_spot2": result[0]["total_area_spot2"],
                "property_type_spot2": result[0]["property_type_spot2"],
                "rent_price_inmuebles24": result[0]["rent_price_inmuebles24"],
                "rent_price_per_m2_inmuebles24": result[0]["rent_price_per_m2_inmuebles24"],
                "buy_price_inmuebles24": result[0]["buy_price_inmuebles24"],
                "buy_price_per_m2_inmuebles24": result[0]["buy_price_per_m2_inmuebles24"],
                "total_area_inmuebles24": result[0]["total_area_inmuebles24"],
                "property_type_inmuebles24": result[0]["property_type_inmuebles24"],
                "rent_price_propiedades": result[0]["rent_price_propiedades"],
                "rent_price_per_m2_propiedades": result[0]["rent_price_per_m2_propiedades"],
                "buy_price_propiedades": result[0]["buy_price_propiedades"],
                "buy_price_per_m2_propiedades": result[0]["buy_price_per_m2_propiedades"],
                "total_area_propiedades": result[0]["total_area_propiedades"],
                "block_type": result[0]["block_type"],
                "density_d": result[0]["density_d"],
                "usage_desc": result[0]["usage_desc"],
                "scope": result[0]["scope"],
                "floor_levels": result[0]["floor_levels"],
                "open_space" : result[0]["open_space"],
                "id_land_use": result[0]['id_land_use'],
                "id_municipality": result[0]["id_municipality"],
                "id_city_blocks": result[0]["id_city_blocks"],
                "total_houses": result[0]["total_houses"],
                "locality_size": result[0]["locality_size"]
            }

            pois = {
                "front" : {
                    "brands_active_life_front": result[0]["brands_active_life_front"],
                    "brands_arts_and_entertainment_front": result[0]["brands_arts_and_entertainment_front"],
                    "brands_attractions_and_activities_front": result[0]["brands_attractions_and_activities_front"],
                    "brands_automotive_front": result[0]["brands_automotive_front"],
                    "brands_eat_and_drink_front": result[0]["brands_eat_and_drink_front"],
                    "brands_education_front": result[0]["brands_education_front"],
                    "brands_financial_service_front": result[0]["brands_financial_service_front"],
                    "brands_health_and_medical_front": result[0]["brands_health_and_medical_front"],
                    "brands_public_service_and_government_front": result[0]["brands_public_service_and_government_front"],
                    "brands_retail_front": result[0]["brands_retail_front"],
                },
                "500" : {
                    "brands_active_life_500m": result[0]["brands_active_life_500m"],
                    "brands_arts_and_entertainment_500m": result[0]["brands_arts_and_entertainment_500m"],
                    "brands_attractions_and_activities_500m": result[0]["brands_attractions_and_activities_500m"],
                    "brands_automotive_500m": result[0]["brands_automotive_500m"],
                    "brands_eat_and_drink_500m": result[0]["brands_eat_and_drink_500m"],
                    "brands_education_500m": result[0]["brands_education_500m"],
                    "brands_financial_service_500m": result[0]["brands_financial_service_500m"],
                    "brands_health_and_medical_500m": result[0]["brands_health_and_medical_500m"],
                    "brands_public_service_and_government_500m": result[0]["brands_public_service_and_government_500m"],
                    "brands_retail_500m": result[0]["brands_retail_500m"],
                },
                "1000" : {
                    "brands_active_life_1km": result[0]["brands_active_life_1km"],
                    "brands_arts_and_entertainment_1km": result[0]["brands_arts_and_entertainment_1km"],
                    "brands_attractions_and_activities_1km": result[0]["brands_attractions_and_activities_1km"],
                    "brands_automotive_1km": result[0]["brands_automotive_1km"],
                    "brands_eat_and_drink_1km": result[0]["brands_eat_and_drink_1km"],
                    "brands_education_1km": result[0]["brands_education_1km"],
                    "brands_financial_service_1km": result[0]["brands_financial_service_1km"],
                    "brands_health_and_medical_1km": result[0]["brands_health_and_medical_1km"],
                    "brands_public_service_and_government_1km": result[0]["brands_public_service_and_government_1km"],
                    "brands_retail_1km": result[0]["brands_retail_1km"]
                },
            }

            traffic = {
                "front" : {
                "at_rest_avg_x_hour_0_front": result[0]["at_rest_avg_x_hour_0_front"],
                "pedestrian_avg_x_hour_0_front": result[0]["pedestrian_avg_x_hour_0_front"],
                "motor_vehicle_avg_x_hour_0_front": result[0]["motor_vehicle_avg_x_hour_0_front"],
                "at_rest_avg_x_hour_1_front": result[0]["at_rest_avg_x_hour_1_front"],
                "pedestrian_avg_x_hour_1_front": result[0]["pedestrian_avg_x_hour_1_front"],
                "motor_vehicle_avg_x_hour_1_front": result[0]["motor_vehicle_avg_x_hour_1_front"],
                "at_rest_avg_x_hour_2_front": result[0]["at_rest_avg_x_hour_2_front"],
                "pedestrian_avg_x_hour_2_front": result[0]["pedestrian_avg_x_hour_2_front"],
                "motor_vehicle_avg_x_hour_2_front": result[0]["motor_vehicle_avg_x_hour_2_front"],
                "at_rest_avg_x_hour_3_front": result[0]["at_rest_avg_x_hour_3_front"],
                "pedestrian_avg_x_hour_3_front": result[0]["pedestrian_avg_x_hour_3_front"],
                "motor_vehicle_avg_x_hour_3_front": result[0]["motor_vehicle_avg_x_hour_3_front"],
                "at_rest_avg_x_hour_4_front": result[0]["at_rest_avg_x_hour_4_front"],
                "pedestrian_avg_x_hour_4_front": result[0]["pedestrian_avg_x_hour_4_front"],
                "motor_vehicle_avg_x_hour_4_front": result[0]["motor_vehicle_avg_x_hour_4_front"],
                "at_rest_avg_x_hour_5_front": result[0]["at_rest_avg_x_hour_5_front"],
                "pedestrian_avg_x_hour_5_front": result[0]["pedestrian_avg_x_hour_5_front"],
                "motor_vehicle_avg_x_hour_5_front": result[0]["motor_vehicle_avg_x_hour_5_front"],
                "at_rest_avg_x_hour_6_front": result[0]["at_rest_avg_x_hour_6_front"],
                "pedestrian_avg_x_hour_6_front": result[0]["pedestrian_avg_x_hour_6_front"],
                "motor_vehicle_avg_x_hour_6_front": result[0]["motor_vehicle_avg_x_hour_6_front"],
                "at_rest_avg_x_hour_7_front": result[0]["at_rest_avg_x_hour_7_front"],
                "pedestrian_avg_x_hour_7_front": result[0]["pedestrian_avg_x_hour_7_front"],
                "motor_vehicle_avg_x_hour_7_front": result[0]["motor_vehicle_avg_x_hour_7_front"],
                "at_rest_avg_x_hour_8_front": result[0]["at_rest_avg_x_hour_8_front"],
                "pedestrian_avg_x_hour_8_front": result[0]["pedestrian_avg_x_hour_8_front"],
                "motor_vehicle_avg_x_hour_8_front": result[0]["motor_vehicle_avg_x_hour_8_front"],
                "at_rest_avg_x_hour_9_front": result[0]["at_rest_avg_x_hour_9_front"],
                "pedestrian_avg_x_hour_9_front": result[0]["pedestrian_avg_x_hour_9_front"],
                "motor_vehicle_avg_x_hour_9_front": result[0]["motor_vehicle_avg_x_hour_9_front"],
                "at_rest_avg_x_hour_10_front": result[0]["at_rest_avg_x_hour_10_front"],
                "pedestrian_avg_x_hour_10_front": result[0]["pedestrian_avg_x_hour_10_front"],
                "motor_vehicle_avg_x_hour_10_front": result[0]["motor_vehicle_avg_x_hour_10_front"],
                "at_rest_avg_x_hour_11_front": result[0]["at_rest_avg_x_hour_11_front"],
                "pedestrian_avg_x_hour_11_front": result[0]["pedestrian_avg_x_hour_11_front"],
                "motor_vehicle_avg_x_hour_11_front": result[0]["motor_vehicle_avg_x_hour_11_front"],
                "at_rest_avg_x_hour_12_front": result[0]["at_rest_avg_x_hour_12_front"],
                "pedestrian_avg_x_hour_12_front": result[0]["pedestrian_avg_x_hour_12_front"],
                "motor_vehicle_avg_x_hour_12_front": result[0]["motor_vehicle_avg_x_hour_12_front"],
                "at_rest_avg_x_hour_13_front": result[0]["at_rest_avg_x_hour_13_front"],
                "pedestrian_avg_x_hour_13_front": result[0]["pedestrian_avg_x_hour_13_front"],
                "motor_vehicle_avg_x_hour_13_front": result[0]["motor_vehicle_avg_x_hour_13_front"],
                "at_rest_avg_x_hour_14_front": result[0]["at_rest_avg_x_hour_14_front"],
                "pedestrian_avg_x_hour_14_front": result[0]["pedestrian_avg_x_hour_14_front"],
                "motor_vehicle_avg_x_hour_14_front": result[0]["motor_vehicle_avg_x_hour_14_front"],
                "at_rest_avg_x_hour_15_front": result[0]["at_rest_avg_x_hour_15_front"],
                "pedestrian_avg_x_hour_15_front": result[0]["pedestrian_avg_x_hour_15_front"],
                "motor_vehicle_avg_x_hour_15_front": result[0]["motor_vehicle_avg_x_hour_15_front"],
                "at_rest_avg_x_hour_16_front": result[0]["at_rest_avg_x_hour_16_front"],
                "pedestrian_avg_x_hour_16_front": result[0]["pedestrian_avg_x_hour_16_front"],
                "motor_vehicle_avg_x_hour_16_front": result[0]["motor_vehicle_avg_x_hour_16_front"],
                "at_rest_avg_x_hour_17_front": result[0]["at_rest_avg_x_hour_17_front"],
                "pedestrian_avg_x_hour_17_front": result[0]["pedestrian_avg_x_hour_17_front"],
                "motor_vehicle_avg_x_hour_17_front": result[0]["motor_vehicle_avg_x_hour_17_front"],
                "at_rest_avg_x_hour_18_front": result[0]["at_rest_avg_x_hour_18_front"],
                "pedestrian_avg_x_hour_18_front": result[0]["pedestrian_avg_x_hour_18_front"],
                "motor_vehicle_avg_x_hour_18_front": result[0]["motor_vehicle_avg_x_hour_18_front"],
                "at_rest_avg_x_hour_19_front": result[0]["at_rest_avg_x_hour_19_front"],
                "pedestrian_avg_x_hour_19_front": result[0]["pedestrian_avg_x_hour_19_front"],
                "motor_vehicle_avg_x_hour_19_front": result[0]["motor_vehicle_avg_x_hour_19_front"],
                "at_rest_avg_x_hour_20_front": result[0]["at_rest_avg_x_hour_20_front"],
                "pedestrian_avg_x_hour_20_front": result[0]["pedestrian_avg_x_hour_20_front"],
                "motor_vehicle_avg_x_hour_20_front": result[0]["motor_vehicle_avg_x_hour_20_front"],
                "at_rest_avg_x_hour_21_front": result[0]["at_rest_avg_x_hour_21_front"],
                "pedestrian_avg_x_hour_21_front": result[0]["pedestrian_avg_x_hour_21_front"],
                "motor_vehicle_avg_x_hour_21_front": result[0]["motor_vehicle_avg_x_hour_21_front"],
                "at_rest_avg_x_hour_22_front": result[0]["at_rest_avg_x_hour_22_front"],
                "pedestrian_avg_x_hour_22_front": result[0]["pedestrian_avg_x_hour_22_front"],
                "motor_vehicle_avg_x_hour_22_front": result[0]["motor_vehicle_avg_x_hour_22_front"],
                "at_rest_avg_x_hour_23_front": result[0]["at_rest_avg_x_hour_23_front"],
                "pedestrian_avg_x_hour_23_front": result[0]["pedestrian_avg_x_hour_23_front"],
                "motor_vehicle_avg_x_hour_23_front": result[0]["motor_vehicle_avg_x_hour_23_front"],
                "at_rest_avg_x_day_of_week_1_front": result[0]["at_rest_avg_x_day_of_week_1_front"],
                "pedestrian_avg_x_day_of_week_1_front": result[0]["pedestrian_avg_x_day_of_week_1_front"],
                "motor_vehicle_avg_x_day_of_week_1_front": result[0]["motor_vehicle_avg_x_day_of_week_1_front"],
                "at_rest_avg_x_day_of_week_2_front": result[0]["at_rest_avg_x_day_of_week_2_front"],
                "pedestrian_avg_x_day_of_week_2_front": result[0]["pedestrian_avg_x_day_of_week_2_front"],
                "motor_vehicle_avg_x_day_of_week_2_front": result[0]["motor_vehicle_avg_x_day_of_week_2_front"],
                "at_rest_avg_x_day_of_week_3_front": result[0]["at_rest_avg_x_day_of_week_3_front"],
                "pedestrian_avg_x_day_of_week_3_front": result[0]["pedestrian_avg_x_day_of_week_3_front"],
                "motor_vehicle_avg_x_day_of_week_3_front": result[0]["motor_vehicle_avg_x_day_of_week_3_front"],
                "at_rest_avg_x_day_of_week_4_front": result[0]["at_rest_avg_x_day_of_week_4_front"],
                "pedestrian_avg_x_day_of_week_4_front": result[0]["pedestrian_avg_x_day_of_week_4_front"],
                "motor_vehicle_avg_x_day_of_week_4_front": result[0]["motor_vehicle_avg_x_day_of_week_4_front"],
                "at_rest_avg_x_day_of_week_5_front": result[0]["at_rest_avg_x_day_of_week_5_front"],
                "pedestrian_avg_x_day_of_week_5_front": result[0]["pedestrian_avg_x_day_of_week_5_front"],
                "motor_vehicle_avg_x_day_of_week_5_front": result[0]["motor_vehicle_avg_x_day_of_week_5_front"],
                "at_rest_avg_x_day_of_week_6_front": result[0]["at_rest_avg_x_day_of_week_6_front"],
                "pedestrian_avg_x_day_of_week_6_front": result[0]["pedestrian_avg_x_day_of_week_6_front"],
                "motor_vehicle_avg_x_day_of_week_6_front": result[0]["motor_vehicle_avg_x_day_of_week_6_front"],
                "at_rest_avg_x_day_of_week_7_front": result[0]["at_rest_avg_x_day_of_week_7_front"],
                "pedestrian_avg_x_day_of_week_7_front": result[0]["pedestrian_avg_x_day_of_week_7_front"],
                "motor_vehicle_avg_x_day_of_week_7_front": result[0]["motor_vehicle_avg_x_day_of_week_7_front"],
                } ,
                "500" : {
                "at_rest_avg_x_day_of_week_1_500m": result[0]["at_rest_avg_x_day_of_week_1_500m"],
                "pedestrian_avg_x_day_of_week_1_500m": result[0]["pedestrian_avg_x_day_of_week_1_500m"],
                "motor_vehicle_avg_x_day_of_week_1_500m": result[0]["motor_vehicle_avg_x_day_of_week_1_500m"],
                "at_rest_avg_x_day_of_week_2_500m": result[0]["at_rest_avg_x_day_of_week_2_500m"],
                "pedestrian_avg_x_day_of_week_2_500m": result[0]["pedestrian_avg_x_day_of_week_2_500m"],
                "motor_vehicle_avg_x_day_of_week_2_500m": result[0]["motor_vehicle_avg_x_day_of_week_2_500m"],
                "at_rest_avg_x_day_of_week_3_500m": result[0]["at_rest_avg_x_day_of_week_3_500m"],
                "pedestrian_avg_x_day_of_week_3_500m": result[0]["pedestrian_avg_x_day_of_week_3_500m"],
                "motor_vehicle_avg_x_day_of_week_3_500m": result[0]["motor_vehicle_avg_x_day_of_week_3_500m"],
                "at_rest_avg_x_day_of_week_4_500m": result[0]["at_rest_avg_x_day_of_week_4_500m"],
                "pedestrian_avg_x_day_of_week_4_500m": result[0]["pedestrian_avg_x_day_of_week_4_500m"],
                "motor_vehicle_avg_x_day_of_week_4_500m": result[0]["motor_vehicle_avg_x_day_of_week_4_500m"],
                "at_rest_avg_x_day_of_week_5_500m": result[0]["at_rest_avg_x_day_of_week_5_500m"],
                "pedestrian_avg_x_day_of_week_5_500m": result[0]["pedestrian_avg_x_day_of_week_5_500m"],
                "motor_vehicle_avg_x_day_of_week_5_500m": result[0]["motor_vehicle_avg_x_day_of_week_5_500m"],
                "at_rest_avg_x_day_of_week_6_500m": result[0]["at_rest_avg_x_day_of_week_6_500m"],
                "pedestrian_avg_x_day_of_week_6_500m": result[0]["pedestrian_avg_x_day_of_week_6_500m"],
                "motor_vehicle_avg_x_day_of_week_6_500m": result[0]["motor_vehicle_avg_x_day_of_week_6_500m"],
                "at_rest_avg_x_day_of_week_7_500m": result[0]["at_rest_avg_x_day_of_week_7_500m"],
                "pedestrian_avg_x_day_of_week_7_500m": result[0]["pedestrian_avg_x_day_of_week_7_500m"],
                "motor_vehicle_avg_x_day_of_week_7_500m": result[0]["motor_vehicle_avg_x_day_of_week_7_500m"],
                "at_rest_avg_x_hour_0_500m": result[0]["at_rest_avg_x_hour_0_500m"],
                "pedestrian_avg_x_hour_0_500m": result[0]["pedestrian_avg_x_hour_0_500m"],
                "motor_vehicle_avg_x_hour_0_500m": result[0]["motor_vehicle_avg_x_hour_0_500m"],
                "at_rest_avg_x_hour_1_500m": result[0]["at_rest_avg_x_hour_1_500m"],
                "pedestrian_avg_x_hour_1_500m": result[0]["pedestrian_avg_x_hour_1_500m"],
                "motor_vehicle_avg_x_hour_1_500m": result[0]["motor_vehicle_avg_x_hour_1_500m"],
                "at_rest_avg_x_hour_2_500m": result[0]["at_rest_avg_x_hour_2_500m"],
                "pedestrian_avg_x_hour_2_500m": result[0]["pedestrian_avg_x_hour_2_500m"],
                "motor_vehicle_avg_x_hour_2_500m": result[0]["motor_vehicle_avg_x_hour_2_500m"],
                "at_rest_avg_x_hour_3_500m": result[0]["at_rest_avg_x_hour_3_500m"],
                "pedestrian_avg_x_hour_3_500m": result[0]["pedestrian_avg_x_hour_3_500m"],
                "motor_vehicle_avg_x_hour_3_500m": result[0]["motor_vehicle_avg_x_hour_3_500m"],
                "at_rest_avg_x_hour_4_500m": result[0]["at_rest_avg_x_hour_4_500m"],
                "pedestrian_avg_x_hour_4_500m": result[0]["pedestrian_avg_x_hour_4_500m"],
                "motor_vehicle_avg_x_hour_4_500m": result[0]["motor_vehicle_avg_x_hour_4_500m"],
                "at_rest_avg_x_hour_5_500m": result[0]["at_rest_avg_x_hour_5_500m"],
                "pedestrian_avg_x_hour_5_500m": result[0]["pedestrian_avg_x_hour_5_500m"],
                "motor_vehicle_avg_x_hour_5_500m": result[0]["motor_vehicle_avg_x_hour_5_500m"],
                "at_rest_avg_x_hour_6_500m": result[0]["at_rest_avg_x_hour_6_500m"],
                "pedestrian_avg_x_hour_6_500m": result[0]["pedestrian_avg_x_hour_6_500m"],
                "motor_vehicle_avg_x_hour_6_500m": result[0]["motor_vehicle_avg_x_hour_6_500m"],
                "at_rest_avg_x_hour_7_500m": result[0]["at_rest_avg_x_hour_7_500m"],
                "pedestrian_avg_x_hour_7_500m": result[0]["pedestrian_avg_x_hour_7_500m"],
                "motor_vehicle_avg_x_hour_7_500m": result[0]["motor_vehicle_avg_x_hour_7_500m"],
                "at_rest_avg_x_hour_8_500m": result[0]["at_rest_avg_x_hour_8_500m"],
                "pedestrian_avg_x_hour_8_500m": result[0]["pedestrian_avg_x_hour_8_500m"],
                "motor_vehicle_avg_x_hour_8_500m": result[0]["motor_vehicle_avg_x_hour_8_500m"],
                "at_rest_avg_x_hour_9_500m": result[0]["at_rest_avg_x_hour_9_500m"],
                "pedestrian_avg_x_hour_9_500m": result[0]["pedestrian_avg_x_hour_9_500m"],
                "motor_vehicle_avg_x_hour_9_500m": result[0]["motor_vehicle_avg_x_hour_9_500m"],
                "at_rest_avg_x_hour_10_500m": result[0]["at_rest_avg_x_hour_10_500m"],
                "pedestrian_avg_x_hour_10_500m": result[0]["pedestrian_avg_x_hour_10_500m"],
                "motor_vehicle_avg_x_hour_10_500m": result[0]["motor_vehicle_avg_x_hour_10_500m"],
                "at_rest_avg_x_hour_11_500m": result[0]["at_rest_avg_x_hour_11_500m"],
                "pedestrian_avg_x_hour_11_500m": result[0]["pedestrian_avg_x_hour_11_500m"],
                "motor_vehicle_avg_x_hour_11_500m": result[0]["motor_vehicle_avg_x_hour_11_500m"],
                "at_rest_avg_x_hour_12_500m": result[0]["at_rest_avg_x_hour_12_500m"],
                "pedestrian_avg_x_hour_12_500m": result[0]["pedestrian_avg_x_hour_12_500m"],
                "motor_vehicle_avg_x_hour_12_500m": result[0]["motor_vehicle_avg_x_hour_12_500m"],
                "at_rest_avg_x_hour_13_500m": result[0]["at_rest_avg_x_hour_13_500m"],
                "pedestrian_avg_x_hour_13_500m": result[0]["pedestrian_avg_x_hour_13_500m"],
                "motor_vehicle_avg_x_hour_13_500m": result[0]["motor_vehicle_avg_x_hour_13_500m"],
                "at_rest_avg_x_hour_14_500m": result[0]["at_rest_avg_x_hour_14_500m"],
                "pedestrian_avg_x_hour_14_500m": result[0]["pedestrian_avg_x_hour_14_500m"],
                "motor_vehicle_avg_x_hour_14_500m": result[0]["motor_vehicle_avg_x_hour_14_500m"],
                "at_rest_avg_x_hour_15_500m": result[0]["at_rest_avg_x_hour_15_500m"],
                "pedestrian_avg_x_hour_15_500m": result[0]["pedestrian_avg_x_hour_15_500m"],
                "motor_vehicle_avg_x_hour_15_500m": result[0]["motor_vehicle_avg_x_hour_15_500m"],
                "at_rest_avg_x_hour_16_500m": result[0]["at_rest_avg_x_hour_16_500m"],
                "pedestrian_avg_x_hour_16_500m": result[0]["pedestrian_avg_x_hour_16_500m"],
                "motor_vehicle_avg_x_hour_16_500m": result[0]["motor_vehicle_avg_x_hour_16_500m"],
                "at_rest_avg_x_hour_17_500m": result[0]["at_rest_avg_x_hour_17_500m"],
                "pedestrian_avg_x_hour_17_500m": result[0]["pedestrian_avg_x_hour_17_500m"],
                "motor_vehicle_avg_x_hour_17_500m": result[0]["motor_vehicle_avg_x_hour_17_500m"],
                "at_rest_avg_x_hour_18_500m": result[0]["at_rest_avg_x_hour_18_500m"],
                "pedestrian_avg_x_hour_18_500m": result[0]["pedestrian_avg_x_hour_18_500m"],
                "motor_vehicle_avg_x_hour_18_500m": result[0]["motor_vehicle_avg_x_hour_18_500m"],
                "at_rest_avg_x_hour_19_500m": result[0]["at_rest_avg_x_hour_19_500m"],
                "pedestrian_avg_x_hour_19_500m": result[0]["pedestrian_avg_x_hour_19_500m"],
                "motor_vehicle_avg_x_hour_19_500m": result[0]["motor_vehicle_avg_x_hour_19_500m"],
                "at_rest_avg_x_hour_20_500m": result[0]["at_rest_avg_x_hour_20_500m"],
                "pedestrian_avg_x_hour_20_500m": result[0]["pedestrian_avg_x_hour_20_500m"],
                "motor_vehicle_avg_x_hour_20_500m": result[0]["motor_vehicle_avg_x_hour_20_500m"],
                "at_rest_avg_x_hour_21_500m": result[0]["at_rest_avg_x_hour_21_500m"],
                "pedestrian_avg_x_hour_21_500m": result[0]["pedestrian_avg_x_hour_21_500m"],
                "motor_vehicle_avg_x_hour_21_500m": result[0]["motor_vehicle_avg_x_hour_21_500m"],
                "at_rest_avg_x_hour_22_500m": result[0]["at_rest_avg_x_hour_22_500m"],
                "pedestrian_avg_x_hour_22_500m": result[0]["pedestrian_avg_x_hour_22_500m"],
                "motor_vehicle_avg_x_hour_22_500m": result[0]["motor_vehicle_avg_x_hour_22_500m"],
                "at_rest_avg_x_hour_23_500m": result[0]["at_rest_avg_x_hour_23_500m"],
                "pedestrian_avg_x_hour_23_500m": result[0]["pedestrian_avg_x_hour_23_500m"],
                "motor_vehicle_avg_x_hour_23_500m": result[0]["motor_vehicle_avg_x_hour_23_500m"]
                } 
                }


            return property_details, market_info, pois, traffic
        except Exception as e :
            raise e

    def get_properties(self, fid):
        connection = None 
        cursor = None
        try :
            query = self.get_property_query(fid)
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchall()
            property_details,  market_info, pois, traffic = self.get_property_json(result)
            result_json = {
                "property_details": property_details,
                "market_info": market_info,
                "pois": pois,
                "traffic": traffic
            }
            resp = Response.success(data=result_json, message='Success')
        except Exception as e :
            resp =  Response.internal_server_error(message=str(e))
        finally :
            return resp

    def get_property_market_info(self, fid):
        connection = None
        cursor = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            spot2_query = f''' SELECT 
                        id_market_data_spot2 as id_spot2,
                        title as title_spot2,
                        address as address_spot2,
                        delegacion as delegacion_spot2,
                        latitude as latitude_spot2,
                        longitude as longitude_spot2,
                        description as description_spot2,
                        operation_type as operation_type_spot2,
                        rent_price_clean as rent_price_mxn_spot2,
                        rent_price_per_m2 as rent_price_per_m2_spot2,
                        buy_price_clean as buy_price_mxn_spot2,
                        buy_price_per_m2 as buy_price_per_m2_spot2,
                        maintenance_price as maintenance_price_mxn_spot2,
                        property_type as property_type_spot2,
                        total_area_clean as total_area_spot2,
                        amenities as amenities_spot2,
                        pictures as pictures_spot2,
                        url as url_spot2,
                        parking_spaces as parking_spaces_spot2,
                        condition as condition_spot2,
                        FROM blackprint_db_prd.presentation.dim_market_data_spot2
                        WHERE id_market_data_spot2 = ANY({fid}::int[])
                        ORDER BY date_published DESC '''

        except Exception as e:
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp
        
    @staticmethod
    def get_demographics_query(fid):
        query = f'''select 
                fid,
                neighborhood,
                predominant_level,
                ageb_code,
                vivtot, -- total houses
                vivtot_colonia,
                vivtot_alcaldia,
                prom_ocup, -- average household size
                prom_ocup_colonia, 
                prom_ocup_alcaldia,
                pro_ocup_c, -- average number of rooms
                pro_ocup_c_colonia, 
                pro_ocup_c_alcaldia, 
                ses_ab, -- socio-economic level AB
                ses_ab_colonia,
                ses_ab_alcaldia,
                ses_c_plus , -- socio-economic level C+
                ses_c_plus_colonia,
                ses_c_plus_alcaldia,
                ses_c, -- socio-economic level C
                ses_c_colonia,
                ses_c_alcaldia,
                ses_c_minus, -- socio-economic level C-
                ses_c_minus_colonia,
                ses_c_minus_alcaldia,
                ses_d , -- socio-economic level D
                ses_d_colonia,
                ses_d_alcaldia,
                ses_d_plus,  -- socio-economic level D+
                ses_d_plus_colonia,
                ses_d_plus_alcaldia,
                ses_e, -- socio-economic level E
                ses_e_colonia,
                ses_e_alcaldia,
                pobtot, -- total population
                pobtot_colonia,
                pobtot_alcaldia,
                pobmas, -- total male population
                pobmas_colonia,
                pobmas_alcaldia,
                pobfem, -- total female population
                pobfem_colonia,
                pobfem_alcaldia,
                p_3a5, -- total education between 3 and 5 years
                p_3a5_colonia,
                p_3a5_alcaldia,
                p3a5_noa, -- total education between 3 and 5 years attending school
                p3a5_noa_colonia,
                p3a5_noa_alcaldia,
                p_6a11, -- total education between 6 and 11 years
                p_6a11_colonia,
                p_6a11_alcaldia,
                p6a11_noa, -- total education between 6 and 11 years attending school
                p6a11_noa_colonia,
                p6a11_noa_alcaldia,
                p_12a14, -- total education between 12 and 14 years
                p_12a14_colonia,
                p_12a14_alcaldia,
                p12a14noa, -- total education between 12 and 14 years attending school
                p12a14noa_colonia,
                p12a14noa_alcaldia,
                p_15a17, -- total education between 15 and 17 years
                p_15a17_colonia,
                p_15a17_alcaldia,
                p15a17a, -- total education between 15 and 17 years attending school
                p15a17a_colonia,
                p15a17a_alcaldia,
                p_18a24 , -- total education between 18 and 24 years
                p_18a24_colonia,
                p_18a24_alcaldia,
                p18a24a, -- total education between 18 and 24 years attending school
                p18a24a_colonia,
                p18a24a_alcaldia,
                pea, -- total workforce
                pea_colonia,
                pea_alcaldia,
                pea_m,  -- total male workforce
                pea_m_colonia,
                pea_m_alcaldia,
                pea_f,  -- total female workforce
                pea_f_colonia,
                pea_f_alcaldia,
                pe_inac, -- total inactive population
                pe_inac_colonia,
                pe_inac_alcaldia,
                pe_inac_m, -- total inactive male population
                pe_inac_m_colonia,
                pe_inac_m_alcaldia,
                pe_inac_f, -- total inactive female population
                pe_inac_f_colonia,
                pe_inac_f_alcaldia,
                pocupada, -- total employed population
                pocupada_colonia, 
                pocupada_alcaldia,
                pocupada_m, -- total employed male population
                pocupada_m_colonia,
                pocupada_m_alcaldia,
                pocupada_f, -- total employed female population
                pocupada_f_colonia,
                pocupada_f_alcaldia,
                pdesocup, -- total unemployed population
                pdesocup_colonia,
                pdesocup_alcaldia,
                pdesocup_m, -- total unemployed male population
                pdesocup_m_colonia,
                pdesocup_m_alcaldia,
                pdesocup_f, -- total unemployed female population
                pdesocup_f_colonia,
                pdesocup_f_alcaldia
                from blackprint_db_prd.data_product.v_parcel_v3
                where fid = {fid}
                    '''
        return query


    @staticmethod
    def get_demographic_json(result):
        try :
            demographic = {
                "general" : {
                    "block" : {
                            "neighborhood" : result[0]["neighborhood"],
                            "predominant_level" : result[0]["predominant_level"],
                            "ageb_code" : result[0]["ageb_code"],
                            "total_household": result[0]["vivtot"],
                            "average_household_size": result[0]["prom_ocup"],
                            "average_number_of_rooms": result[0]["pro_ocup_c"]
                            },
                    "colonia": {
                            "neighborhood" : result[0]["neighborhood"],
                            "predominant_level" : result[0]["predominant_level"],
                            "ageb_code" : result[0]["ageb_code"],
                            "total_household": result[0]["vivtot_colonia"],
                            "average_household_size": result[0]["prom_ocup_colonia"],
                            "average_number_of_rooms": result[0]["pro_ocup_c_colonia"]
                            },
                    
                    "alcaldia": {
                            "neighborhood" : result[0]["neighborhood"],
                            "predominant_level" : result[0]["predominant_level"],
                            "ageb_code" : result[0]["ageb_code"],
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
                            "total_male_workforce" : result[0]["pea_m_alcaldia"],
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
        except Exception as e :
            raise e

    def get_property_demographic(self, fid):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.get_demographics_query(fid)
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            if res:
                response = self.get_demographic_json(res)
                resp =  Response.success(data=response, message='Success')
            else:
                resp =  Response.bad_request(message="Property not found")
        except Exception as e:
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect()
            return resp
        

    def get_property_pois(self):
        pass

    def get_property_traffic(self):
        pass