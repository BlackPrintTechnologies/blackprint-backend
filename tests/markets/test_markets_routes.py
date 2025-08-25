import pytest
from unittest.mock import Mock, patch, MagicMock
from module.markets.routes import PropertyTypes, MarketInfo
from module.markets.controller import MarketsController

class TestPropertyTypes:
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.resource = PropertyTypes()
        self.mock_user = {'id': 123, 'email': 'test@example.com'}
        
    @patch('module.markets.routes.MarketsController')
    def test_get_property_types_success(self, mock_controller_class):
        """Test successful property types retrieval"""
        # Mock the controller
        mock_controller = Mock()
        mock_controller_class.return_value = mock_controller
        
        # Mock the response
        mock_response = {
            'message': 'Success',
            'data': ['Residential', 'Commercial', 'Industrial'],
            'status_code': 200
        }
        mock_controller.get_all_distinct_property_types.return_value = mock_response
        
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.return_value = 'mexico'
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 200
            assert response['message'] == 'Success'
            assert response['data'] == ['Residential', 'Commercial', 'Industrial']
            
    @patch('module.markets.routes.MarketsController')
    def test_get_property_types_queretaro(self, mock_controller_class):
        """Test property types retrieval for Queretaro city"""
        # Mock the controller
        mock_controller = Mock()
        mock_controller_class.return_value = mock_controller
        
        # Mock the response
        mock_response = {
            'message': 'Success',
            'data': ['Residential', 'Commercial'],
            'status_code': 200
        }
        mock_controller.get_all_distinct_property_types.return_value = mock_response
        
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.return_value = 'queretaro'
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 200
            assert response['message'] == 'Success'
            assert response['data'] == ['Residential', 'Commercial']
            
    @patch('module.markets.routes.MarketsController')
    def test_get_property_types_exception(self, mock_controller_class):
        """Test property types retrieval with exception"""
        # Mock the controller to raise an exception
        mock_controller = Mock()
        mock_controller_class.return_value = mock_controller
        mock_controller.get_all_distinct_property_types.side_effect = Exception("Database error")
        
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.return_value = 'mexico'
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 500
            assert response['message'] == 'Internal server error'

class TestMarketInfo:
    
    def setup_method(self):
        """Set up test fixtures before each test method"""
        self.resource = MarketInfo()
        self.mock_user = {'id': 123, 'email': 'test@example.com'}
        
    @patch('module.markets.routes.MarketsController')
    def test_get_market_info_inmuebles24_success(self, mock_controller_class):
        """Test successful market info retrieval for inmuebles24 property"""
        # Mock the controller
        mock_controller = Mock()
        mock_controller_class.return_value = mock_controller
        
        # Mock the response
        mock_response = {
            'message': 'Success',
            'data': [{
                'id_market_data_inmuebles24': 123,
                'title': 'Test Property',
                'property_type': 'Residential'
            }],
            'status_code': 200
        }
        mock_controller.get_property_market_info.return_value = mock_response
        
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.side_effect = lambda key, default=None: {
                'spot2_id': None,
                'inmuebles24_id': 123,
                'propiedades_id': None,
                'config_city': 'mexico'
            }.get(key, default)
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 200
            assert response['message'] == 'Success'
            assert len(response['data']) == 1
            assert response['data'][0]['id_market_data_inmuebles24'] == 123
            
    @patch('module.markets.routes.MarketsController')
    def test_get_market_info_spot2_success(self, mock_controller_class):
        """Test successful market info retrieval for spot2 property"""
        # Mock the controller
        mock_controller = Mock()
        mock_controller_class.return_value = mock_controller
        
        # Mock the response
        mock_response = {
            'message': 'Success',
            'data': [{
                'id_market_data_spot2': 456,
                'title': 'Test Property',
                'property_type': 'Commercial'
            }],
            'status_code': 200
        }
        mock_controller.get_property_market_info.return_value = mock_response
        
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.side_effect = lambda key, default=None: {
                'spot2_id': 456,
                'inmuebles24_id': None,
                'propiedades_id': None,
                'config_city': 'mexico'
            }.get(key, default)
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 200
            assert response['message'] == 'Success'
            assert len(response['data']) == 1
            assert response['data'][0]['id_market_data_spot2'] == 456
            
    def test_get_market_info_no_ids(self):
        """Test market info retrieval with no valid IDs"""
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.side_effect = lambda key, default=None: {
                'spot2_id': None,
                'inmuebles24_id': None,
                'propiedades_id': None,
                'config_city': 'mexico'
            }.get(key, default)
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 400
            assert 'At least one of spot2_id, inmuebles24_id, or propiedades_id is required' in response['message']
            
    @patch('module.markets.routes.MarketsController')
    def test_get_market_info_exception(self, mock_controller_class):
        """Test market info retrieval with exception"""
        # Mock the controller to raise an exception
        mock_controller = Mock()
        mock_controller_class.return_value = mock_controller
        mock_controller.get_property_market_info.side_effect = Exception("Database error")
        
        # Mock request arguments
        with patch('module.markets.routes.reqparse.RequestParser') as mock_parser:
            mock_args = Mock()
            mock_args.get.side_effect = lambda key, default=None: {
                'spot2_id': 123,
                'inmuebles24_id': None,
                'propiedades_id': None,
                'config_city': 'mexico'
            }.get(key, default)
            mock_parser.return_value.parse_args.return_value = mock_args
            
            # Call the method
            response, status_code = self.resource.get(self.mock_user)
            
            # Assertions
            assert status_code == 500
            assert response['message'] == 'Internal server error' 