"""
Refactored QueryController using Unified City Management System
Eliminates over 1500 lines of redundant column definitions and hardcoded city logic
COMPLETE VERSION - All methods use clean builder pattern
"""
from utils.city_manager import city_manager, query_builder


class QueryController:
    """
    Unified QueryController that eliminates redundancy between QRO and Mexico cities.
    ALL methods now use the clean builder pattern - no more hardcoded SQL!
    """

    def __init__(self):
        self.city_manager = city_manager
        self.query_builder = query_builder

    @staticmethod
    def get_property_query(filter_clause, city='mexico', show_all_keys=True):
        """
        Main property query method - clean and unified.
        Called by controller: self.qc.get_property_query(filter_query, city=city)
        """
        normalized_city = city_manager.normalize_city_name(city)
        return query_builder.build_complete_property_query(filter_clause, normalized_city, show_all_keys)
    
    @staticmethod
    def get_demographics_query(fid, city='mexico'):
        """
        Demographics query using centralized city management.
        Called by controller: self.qc.get_demographics_query(fid, city=city)
        """
        normalized_city = city_manager.normalize_city_name(city)
        return query_builder.build_demographics_query(fid, normalized_city)
    
    @staticmethod
    def get_commercial_growth_query(fid):
        """
        Commercial growth query - Mexico City only.
        Called by controller: self.qc.get_commercial_growth_query(fid)
        """
        return query_builder.build_commercial_growth_query(fid)
    
    @staticmethod  
    def get_market_info_query(spot2, inmuebles24, propiedades, city='mexico'):
        """
        Market info query using centralized city management.
        Called by controller: self.qc.get_market_info_query(spot2_id, inmuebles24_id, propiedades_id, city=city)
        """
        normalized_city = city_manager.normalize_city_name(city)
        return query_builder.build_market_info_query(spot2, inmuebles24, propiedades, normalized_city)