import pytest
from unittest.mock import patch, MagicMock
from utils.responseUtils import Response
from module.layers.controller import BrandController, TrafficController

# Fixture to mock the RedshiftDatabase class
@pytest.fixture
def mock_redshift_db():
    with patch('module.layers.controller.RedshiftDatabase') as mock:
        mock_instance = MagicMock()
        mock.return_value = mock_instance
        yield mock_instance

# Test BrandController
class TestBrandController:
    def test_get_brand_query_500(self):
        # Test the query generation for catchment '500'
        query = BrandController.get_brand_query('500', 123)
        expected_query = '''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_500m FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = 123), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_500m FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = 123), ',')
                        )
                        SELECT brand, geometry_wkt, category_1 FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''
        assert query == expected_query

    def test_get_brand_query_1000(self):
        # Test the query generation for catchment '1000'
        query = BrandController.get_brand_query('1000', 123)
        expected_query = '''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_1km FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = 123), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_1km FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = 123), ',')
                        )
                        SELECT brand, geometry_wkt, category_1 FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''
        assert query == expected_query

    def test_get_brand_query_5(self):
        # Test the query generation for catchment '5'
        query = BrandController.get_brand_query('5', 123)
        expected_query = '''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_front FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = 123), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_front FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = 123), ',')
                        )
                        SELECT brand, geometry_wkt, category_1 FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''
        assert query == expected_query

    def test_get_brands(self, mock_redshift_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_redshift_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"brand": "Test Brand", "geometry_wkt": "POINT(0 0)", "category_1": "Test Category"}]

        # Call the method
        controller = BrandController()
        response = controller.get_brands('500', 123)
        print("Test get response", response)

        # Assert the response
        expected_response = Response.success(data={"response": [{"brand": "Test Brand", "geometry_wkt": "POINT(0 0)", "category_1": "Test Category"}]})
        assert response == expected_response

    def test_get_brands_error(self, mock_redshift_db):
        # Mock the database to raise an exception
        mock_redshift_db.connect.side_effect = Exception("Database error")

        # Call the method
        controller = BrandController()
        response = controller.get_brands('500', 123)

        # Assert the response
        expected_response = Response.internal_server_error(message="Database error")
        assert response == expected_response

# Test TrafficController
class TestTrafficController:
    def test_get_traffic_query_500(self):
        # Test the query generation for catchment '500'
        query = TrafficController.get_traffic_query('500', 123)
        expected_query = '''SELECT *
                        FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid=123 and type='CIRCLE_500_METERS' '''
        assert query == expected_query

    def test_get_traffic_query_1000(self):
        # Test the query generation for catchment '1000'
        query = TrafficController.get_traffic_query('1000', 123)
        expected_query = '''SELECT *
                        FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid=123 and type='CIRCLE_1000_METERS' '''
        assert query == expected_query

    def test_get_traffic_query_5(self):
        # Test the query generation for catchment '5'
        query = TrafficController.get_traffic_query('5', 123)
        expected_query = '''SELECT *
                        FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid=123 and type='FRONT_OF_STORE' '''
        assert query == expected_query

    def test_get_mobility_data_within_buffer(self, mock_redshift_db):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_redshift_db.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [{"fid": 123, "type": "CIRCLE_500_METERS", "data": "Test Data"}]

        # Call the method
        controller = TrafficController()
        response = controller.get_mobility_data_within_buffer(123, '500')

        # Assert the response
        expected_response = Response.success(data={"response": [{"fid": 123, "type": "CIRCLE_500_METERS", "data": "Test Data"}]})
        assert response == expected_response

    def test_get_mobility_data_within_buffer_error(self, mock_redshift_db):
        # Mock the database to raise an exception
        mock_redshift_db.connect.side_effect = Exception("Database error")

        # Call the method
        controller = TrafficController()
        response = controller.get_mobility_data_within_buffer(123, '500')

        # Assert the response
        expected_response = Response.internal_server_error(message="Database error")
        assert response == expected_response