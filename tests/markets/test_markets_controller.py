import pytest
from unittest.mock import Mock, patch, MagicMock
from module.markets.controller import MarketsController
from utils.responseUtils import Response

class TestMarketsController:
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.controller = MarketsController()
        self.mock_connection = Mock()
        self.mock_cursor = Mock()
        
    @patch('module.markets.controller.ConnectionPoolDbUtils')
    def test_get_all_distinct_property_types_mexico(self, mock_connection_pool):
        """Test getting property types for Mexico city"""
        # Mock the connection and cursor
        mock_connection_pool.return_value.connect.return_value = self.mock_connection
        self.mock_connection.cursor.return_value = self.mock_cursor
        
        # Mock the query results
        mock_results = [
            {'property_type': 'Residential'},
            {'property_type': 'Commercial'},
            {'property_type': 'Industrial'}
        ]
        self.mock_cursor.fetchall.return_value = mock_results
        
        # Call the method
        response = self.controller.get_all_distinct_property_types(city='mexico')
        
        # Assertions
        assert response['message'] == 'Success'
        assert response['data'] == ['Residential', 'Commercial', 'Industrial']
        assert 'status_code' in response
        
    @patch('module.markets.controller.ConnectionPoolDbUtils')
    def test_get_all_distinct_property_types_queretaro(self, mock_connection_pool):
        """Test getting property types for Queretaro city"""
        # Mock the connection and cursor
        mock_connection_pool.return_value.connect.return_value = self.mock_connection
        self.mock_connection.cursor.return_value = self.mock_cursor
        
        # Mock the query results
        mock_results = [
            {'property_type': 'Residential'},
            {'property_type': 'Commercial'}
        ]
        self.mock_cursor.fetchall.return_value = mock_results
        
        # Call the method
        response = self.controller.get_all_distinct_property_types(city='queretaro')
        
        # Assertions
        assert response['message'] == 'Success'
        assert response['data'] == ['Residential', 'Commercial']
        assert 'status_code' in response
        
    @patch('module.markets.controller.ConnectionPoolDbUtils')
    def test_get_property_market_info_inmuebles24(self, mock_connection_pool):
        """Test getting market info for inmuebles24 property"""
        # Mock the connection and cursor
        mock_connection_pool.return_value.connect.return_value = self.mock_connection
        self.mock_connection.cursor.return_value = self.mock_cursor
        
        # Mock the query results
        mock_results = [
            {
                'id_market_data_inmuebles24': 123,
                'title': 'Test Property',
                'property_type': 'Residential',
                'rent_price': 5000
            }
        ]
        self.mock_cursor.fetchall.return_value = mock_results
        
        # Call the method
        response = self.controller.get_property_market_info(
            spot2_id=None,
            inmuebles24_id=123,
            propiedades_id=None,
            city='mexico'
        )
        
        # Assertions
        assert response['message'] == 'Success'
        assert response['data'] == mock_results
        assert 'status_code' in response
        
    @patch('module.markets.controller.ConnectionPoolDbUtils')
    def test_get_property_market_info_spot2(self, mock_connection_pool):
        """Test getting market info for spot2 property"""
        # Mock the connection and cursor
        mock_connection_pool.return_value.connect.return_value = self.mock_connection
        self.mock_connection.cursor.return_value = self.mock_cursor
        
        # Mock the query results
        mock_results = [
            {
                'id_market_data_spot2': 456,
                'title': 'Test Property',
                'property_type': 'Commercial',
                'buy_price': 1000000
            }
        ]
        self.mock_cursor.fetchall.return_value = mock_results
        
        # Call the method
        response = self.controller.get_property_market_info(
            spot2_id=456,
            inmuebles24_id=None,
            propiedades_id=None,
            city='mexico'
        )
        
        # Assertions
        assert response['message'] == 'Success'
        assert response['data'] == mock_results
        assert 'status_code' in response
        
    @patch('module.markets.controller.ConnectionPoolDbUtils')
    def test_get_property_market_info_no_ids(self, mock_connection_pool):
        """Test getting market info with no valid IDs"""
        # Mock the connection and cursor
        mock_connection_pool.return_value.connect.return_value = self.mock_connection
        self.mock_connection.cursor.return_value = self.mock_cursor
        
        # Mock the query results (empty since no valid IDs)
        self.mock_cursor.fetchall.return_value = []
        
        # Call the method
        response = self.controller.get_property_market_info(
            spot2_id=None,
            inmuebles24_id=None,
            propiedades_id=None,
            city='mexico'
        )
        
        # Assertions
        assert response['message'] == 'Success'
        assert response['data'] == []
        assert 'status_code' in response 