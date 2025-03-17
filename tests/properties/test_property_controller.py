import pytest
from unittest.mock import patch, MagicMock
from utils.responseUtils import Response
from module.properties.controller import PropertyController

# Fixture to mock the Database, RedshiftDatabase, and QueryController classes
@pytest.fixture
def mock_db():
    with patch('module.properties.controller.Database') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_redshift_db():
    with patch('module.properties.controller.RedshiftDatabase') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_query_controller():
    with patch('module.properties.controller.QueryController') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance

# Test PropertyController
class TestPropertyController:
    def test_get_property_json(self):
        # Sample result data
        result = [{
            "fid": 123,
            "centroid": '{"coordinates": [10.0, 20.0]}',
            "is_on_market": True,
            "total_surface_area": 1000,
            "total_construction_area": 800,
            "year_built": 2000,
            "special_facilities": "Facility A",
            "unit_land_value": 500,
            "land_value": 500000,
            "key_vus": "VUS123",
            "predominant_level": 5,
            "rent_price_spot2": 10000,
            "rent_price_per_m2_spot2": 10,
            "buy_price_spot2": 500000,
            "buy_price_per_m2_spot2": 500,
            "total_area_spot2": 100,
            "property_type_spot2": "Residential",
            "rent_price_inmuebles24": 12000,
            "rent_price_per_m2_inmuebles24": 12,
            "buy_price_inmuebles24": 600000,
            "buy_price_per_m2_inmuebles24": 600,
            "total_area_inmuebles24": 120,
            "property_type_inmuebles24": "Commercial",
            "rent_price_propiedades": 11000,
            "rent_price_per_m2_propiedades": 11,
            "buy_price_propiedades": 550000,
            "buy_price_per_m2_propiedades": 550,
            "total_area_propiedades": 110,
            "block_type": "Type A",
            "density_d": 50,
            "usage_desc": "Residential",
            "scope": "Local",
            "floor_levels": 5,
            "open_space": 200,
            "id_land_use": 1,
            "id_municipality": 2,
            "id_city_blocks": 3,
            "total_houses": 100,
            "locality_size": 5000,
            "brands_active_life_front": 5,
            "brands_arts_and_entertainment_front": 3,
            "brands_attractions_and_activities_front": 2,
            "brands_automotive_front": 4,
            "brands_eat_and_drink_front": 6,
            "brands_education_front": 1,
            "brands_financial_service_front": 2,
            "brands_health_and_medical_front": 3,
            "brands_public_service_and_government_front": 4,
            "brands_retail_front": 5,
            "brands_active_life_500m": 10,
            "brands_arts_and_entertainment_500m": 8,
            "brands_attractions_and_activities_500m": 7,
            "brands_automotive_500m": 9,
            "brands_eat_and_drink_500m": 11,
            "brands_education_500m": 6,
            "brands_financial_service_500m": 7,
            "brands_health_and_medical_500m": 8,
            "brands_public_service_and_government_500m": 9,
            "brands_retail_500m": 10,
            "brands_active_life_1km": 15,
            "brands_arts_and_entertainment_1km": 13,
            "brands_attractions_and_activities_1km": 12,
            "brands_automotive_1km": 14,
            "brands_eat_and_drink_1km": 16,
            "brands_education_1km": 11,
            "brands_financial_service_1km": 12,
            "brands_health_and_medical_1km": 13,
            "brands_public_service_and_government_1km": 14,
            "brands_retail_1km": 15,
            "at_rest_avg_x_hour_0_front": 10,
            "pedestrian_avg_x_hour_0_front": 20,
            "motor_vehicle_avg_x_hour_0_front": 30,
            "at_rest_avg_x_hour_1_front": 11,
            "pedestrian_avg_x_hour_1_front": 21,
            "motor_vehicle_avg_x_hour_1_front": 31,
            "at_rest_avg_x_hour_2_front": 12,
            "pedestrian_avg_x_hour_2_front": 22,
            "motor_vehicle_avg_x_hour_2_front": 32,
            "at_rest_avg_x_hour_3_front": 13,
            "pedestrian_avg_x_hour_3_front": 23,
            "motor_vehicle_avg_x_hour_3_front": 33,
            "at_rest_avg_x_hour_4_front": 14,
            "pedestrian_avg_x_hour_4_front": 24,
            "motor_vehicle_avg_x_hour_4_front": 34,
            "at_rest_avg_x_hour_5_front": 15,
            "pedestrian_avg_x_hour_5_front": 25,
            "motor_vehicle_avg_x_hour_5_front": 35,
            "at_rest_avg_x_hour_6_front": 16,
            "pedestrian_avg_x_hour_6_front": 26,
            "motor_vehicle_avg_x_hour_6_front": 36,
            "at_rest_avg_x_hour_7_front": 17,
            "pedestrian_avg_x_hour_7_front": 27,
            "motor_vehicle_avg_x_hour_7_front": 37,
            "at_rest_avg_x_hour_8_front": 18,
            "pedestrian_avg_x_hour_8_front": 28,
            "motor_vehicle_avg_x_hour_8_front": 38,
            "at_rest_avg_x_hour_9_front": 19,
            "pedestrian_avg_x_hour_9_front": 29,
            "motor_vehicle_avg_x_hour_9_front": 39,
            "at_rest_avg_x_hour_10_front": 20,
            "pedestrian_avg_x_hour_10_front": 30,
            "motor_vehicle_avg_x_hour_10_front": 40,
            "at_rest_avg_x_hour_11_front": 21,
            "pedestrian_avg_x_hour_11_front": 31,
            "motor_vehicle_avg_x_hour_11_front": 41,
            "at_rest_avg_x_hour_12_front": 22,
            "pedestrian_avg_x_hour_12_front": 32,
            "motor_vehicle_avg_x_hour_12_front": 42,
            "at_rest_avg_x_hour_13_front": 23,
            "pedestrian_avg_x_hour_13_front": 33,
            "motor_vehicle_avg_x_hour_13_front": 43,
            "at_rest_avg_x_hour_14_front": 24,
            "pedestrian_avg_x_hour_14_front": 34,
            "motor_vehicle_avg_x_hour_14_front": 44,
            "at_rest_avg_x_hour_15_front": 25,
            "pedestrian_avg_x_hour_15_front": 35,
            "motor_vehicle_avg_x_hour_15_front": 45,
            "at_rest_avg_x_hour_16_front": 26,
            "pedestrian_avg_x_hour_16_front": 36,
            "motor_vehicle_avg_x_hour_16_front": 46,
            "at_rest_avg_x_hour_17_front": 27,
            "pedestrian_avg_x_hour_17_front": 37,
            "motor_vehicle_avg_x_hour_17_front": 47,
            "at_rest_avg_x_hour_18_front": 28,
            "pedestrian_avg_x_hour_18_front": 38,
            "motor_vehicle_avg_x_hour_18_front": 48,
            "at_rest_avg_x_hour_19_front": 29,
            "pedestrian_avg_x_hour_19_front": 39,
            "motor_vehicle_avg_x_hour_19_front": 49,
            "at_rest_avg_x_hour_20_front": 30,
            "pedestrian_avg_x_hour_20_front": 40,
            "motor_vehicle_avg_x_hour_20_front": 50,
            "at_rest_avg_x_hour_21_front": 31,
            "pedestrian_avg_x_hour_21_front": 41,
            "motor_vehicle_avg_x_hour_21_front": 51,
            "at_rest_avg_x_hour_22_front": 32,
            "pedestrian_avg_x_hour_22_front": 42,
            "motor_vehicle_avg_x_hour_22_front": 52,
            "at_rest_avg_x_hour_23_front": 33,
            "pedestrian_avg_x_hour_23_front": 43,
            "motor_vehicle_avg_x_hour_23_front": 53,
            "at_rest_avg_x_day_of_week_1_front": 10,
            "pedestrian_avg_x_day_of_week_1_front": 20,
            "motor_vehicle_avg_x_day_of_week_1_front": 30,
            "at_rest_avg_x_day_of_week_2_front": 11,
            "pedestrian_avg_x_day_of_week_2_front": 21,
            "motor_vehicle_avg_x_day_of_week_2_front": 31,
            "at_rest_avg_x_day_of_week_3_front": 12,
            "pedestrian_avg_x_day_of_week_3_front": 22,
            "motor_vehicle_avg_x_day_of_week_3_front": 32,
            "at_rest_avg_x_day_of_week_4_front": 13,
            "pedestrian_avg_x_day_of_week_4_front": 23,
            "motor_vehicle_avg_x_day_of_week_4_front": 33,
            "at_rest_avg_x_day_of_week_5_front": 14,
            "pedestrian_avg_x_day_of_week_5_front": 24,
            "motor_vehicle_avg_x_day_of_week_5_front": 34,
            "at_rest_avg_x_day_of_week_6_front": 15,
            "pedestrian_avg_x_day_of_week_6_front": 25,
            "motor_vehicle_avg_x_day_of_week_6_front": 35,
            "at_rest_avg_x_day_of_week_7_front": 16,
            "pedestrian_avg_x_day_of_week_7_front": 26,
            "motor_vehicle_avg_x_day_of_week_7_front": 36,
            "at_rest_avg_x_day_of_week_1_500m": 10,
            "pedestrian_avg_x_day_of_week_1_500m": 20,
            "motor_vehicle_avg_x_day_of_week_1_500m": 30,
            "at_rest_avg_x_day_of_week_2_500m": 11,
            "pedestrian_avg_x_day_of_week_2_500m": 21,
            "motor_vehicle_avg_x_day_of_week_2_500m": 31,
            "at_rest_avg_x_day_of_week_3_500m": 12,
            "pedestrian_avg_x_day_of_week_3_500m": 22,
            "motor_vehicle_avg_x_day_of_week_3_500m": 32,
            "at_rest_avg_x_day_of_week_4_500m": 13,
            "pedestrian_avg_x_day_of_week_4_500m": 23,
            "motor_vehicle_avg_x_day_of_week_4_500m": 33,
            "at_rest_avg_x_day_of_week_5_500m": 14,
            "pedestrian_avg_x_day_of_week_5_500m": 24,
            "motor_vehicle_avg_x_day_of_week_5_500m": 34,
            "at_rest_avg_x_day_of_week_6_500m": 15,
            "pedestrian_avg_x_day_of_week_6_500m": 25,
            "motor_vehicle_avg_x_day_of_week_6_500m": 35,
            "at_rest_avg_x_day_of_week_7_500m": 16,
            "pedestrian_avg_x_day_of_week_7_500m": 26,
            "motor_vehicle_avg_x_day_of_week_7_500m": 36,
            "at_rest_avg_x_hour_0_500m": 10,
            "pedestrian_avg_x_hour_0_500m": 20,
            "motor_vehicle_avg_x_hour_0_500m": 30,
            "at_rest_avg_x_hour_1_500m": 11,
            "pedestrian_avg_x_hour_1_500m": 21,
            "motor_vehicle_avg_x_hour_1_500m": 31,
            "at_rest_avg_x_hour_2_500m": 12,
            "pedestrian_avg_x_hour_2_500m": 22,
            "motor_vehicle_avg_x_hour_2_500m": 32,
            "at_rest_avg_x_hour_3_500m": 13,
            "pedestrian_avg_x_hour_3_500m": 23,
            "motor_vehicle_avg_x_hour_3_500m": 33,
            "at_rest_avg_x_hour_4_500m": 14,
            "pedestrian_avg_x_hour_4_500m": 24,
            "motor_vehicle_avg_x_hour_4_500m": 34,
            "at_rest_avg_x_hour_5_500m": 15,
            "pedestrian_avg_x_hour_5_500m": 25,
            "motor_vehicle_avg_x_hour_5_500m": 35,
            "at_rest_avg_x_hour_6_500m": 16,
            "pedestrian_avg_x_hour_6_500m": 26,
            "motor_vehicle_avg_x_hour_6_500m": 36,
            "at_rest_avg_x_hour_7_500m": 17,
            "pedestrian_avg_x_hour_7_500m": 27,
            "motor_vehicle_avg_x_hour_7_500m": 37,
            "at_rest_avg_x_hour_8_500m": 18,
            "pedestrian_avg_x_hour_8_500m": 28,
            "motor_vehicle_avg_x_hour_8_500m": 38,
            "at_rest_avg_x_hour_9_500m": 19,
            "pedestrian_avg_x_hour_9_500m": 29,
            "motor_vehicle_avg_x_hour_9_500m": 39,
            "at_rest_avg_x_hour_10_500m": 20,
            "pedestrian_avg_x_hour_10_500m": 30,
            "motor_vehicle_avg_x_hour_10_500m": 40,
            "at_rest_avg_x_hour_11_500m": 21,
            "pedestrian_avg_x_hour_11_500m": 31,
            "motor_vehicle_avg_x_hour_11_500m": 41,
            "at_rest_avg_x_hour_12_500m": 22,
            "pedestrian_avg_x_hour_12_500m": 32,
            "motor_vehicle_avg_x_hour_12_500m": 42,
            "at_rest_avg_x_hour_13_500m": 23,
            "pedestrian_avg_x_hour_13_500m": 33,
            "motor_vehicle_avg_x_hour_13_500m": 43,
            "at_rest_avg_x_hour_14_500m": 24,
            "pedestrian_avg_x_hour_14_500m": 34,
            "motor_vehicle_avg_x_hour_14_500m": 44,
            "at_rest_avg_x_hour_15_500m": 25,
            "pedestrian_avg_x_hour_15_500m": 35,
            "motor_vehicle_avg_x_hour_15_500m": 45,
            "at_rest_avg_x_hour_16_500m": 26,
            "pedestrian_avg_x_hour_16_500m": 36,
            "motor_vehicle_avg_x_hour_16_500m": 46,
            "at_rest_avg_x_hour_17_500m": 27,
            "pedestrian_avg_x_hour_17_500m": 37,
            "motor_vehicle_avg_x_hour_17_500m": 47,
            "at_rest_avg_x_hour_18_500m": 28,
            "pedestrian_avg_x_hour_18_500m": 38,
            "motor_vehicle_avg_x_hour_18_500m": 48,
            "at_rest_avg_x_hour_19_500m": 29,
            "pedestrian_avg_x_hour_19_500m": 39,
            "motor_vehicle_avg_x_hour_19_500m": 49,
            "at_rest_avg_x_hour_20_500m": 30,
            "pedestrian_avg_x_hour_20_500m": 40,
            "motor_vehicle_avg_x_hour_20_500m": 50,
            "at_rest_avg_x_hour_21_500m": 31,
            "pedestrian_avg_x_hour_21_500m": 41,
            "motor_vehicle_avg_x_hour_21_500m": 51,
            "at_rest_avg_x_hour_22_500m": 32,
            "pedestrian_avg_x_hour_22_500m": 42,
            "motor_vehicle_avg_x_hour_22_500m": 52,
            "at_rest_avg_x_hour_23_500m": 33,
            "pedestrian_avg_x_hour_23_500m": 43,
            "motor_vehicle_avg_x_hour_23_500m": 53
        }]

        # Call the method
        controller = PropertyController()
        property_details, market_info, pois, traffic = controller.get_property_json(result)

        # Assert the response
        assert property_details["fid"] == 123
        assert market_info["rent_price_spot2"] == 10000
        assert pois["front"]["brands_active_life_front"] == 5
        assert traffic["front"]["at_rest_avg_x_hour_0_front"] == 10

    def test_get_properties(self, mock_redshift_db, mock_query_controller):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_redshift_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"fid": 123, "centroid": '{"coordinates": [10.0, 20.0]}'}]

        # Mock the query
        mock_query_controller.get_property_query.return_value = "SELECT * FROM properties WHERE fid = 123"

        # Call the method
        controller = PropertyController()
        response = controller.get_properties(123)
        print("Test Get Properties",response)
        # Assert the response
        assert response.status == "success"
        assert response.data["property_details"]["fid"] == 123

    def test_get_property_demographic(self, mock_redshift_db, mock_query_controller):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_redshift_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"fid": 123, "neighborhood": "Test Neighborhood"}]

        # Mock the query
        mock_query_controller.get_demographics_query.return_value = "SELECT * FROM demographics WHERE fid = 123"

        # Call the method
        controller = PropertyController()
        response = controller.get_property_demographic(123)

        # Assert the response
        assert response.status == "success"
        assert response.data["general"]["block"]["neighborhood"] == "Test Neighborhood"

    def test_get_property_market_info(self, mock_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"id_spot2": 1, "title_spot2": "Test Property"}]

        # Call the method
        controller = PropertyController()
        response = controller.get_property_market_info(123)

        # Assert the response
        assert response.status == "success"
        assert response.data[0]["id_spot2"] == 1