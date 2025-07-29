"""
Centralized City Management System
Eliminates redundancy between QRO and Mexico city configurations
"""
import json
from typing import Dict, List, Optional, Any
from enum import Enum


class CityCode(Enum):
    """Standardized city codes"""
    CDMX = "cdmx"
    QRO = "qro"


class CityManager:
    """
    Centralized city configuration management.
    Eliminates hardcoded city logic scattered across the codebase.
    """
    
    def __init__(self, config_path: str = 'app.json'):
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        
        # City configurations from app.json
        self.city_configs = self.config.get('CITIES', {})
        
        # Standardized city name mapping to eliminate inconsistencies
        self.city_name_mapping = {
            # All variations -> standard code
            'mexico': CityCode.CDMX.value,
            'cdmx': CityCode.CDMX.value,
            'mexico_city': CityCode.CDMX.value,
            'queretaro': CityCode.QRO.value,
            'qro': CityCode.QRO.value,
            'el_marques': CityCode.QRO.value,  # El Marques is part of QRO region
        }
        
        # Database table and column mappings
        self.table_mappings = {
            CityCode.CDMX.value: {
                'main_table': 'blackprint_db_prd.data_product.v_parcel_v3',
                'fid_column': 'fid',
                'fid_alias': 'fid'
            },
            CityCode.QRO.value: {
                'main_table': 'blackprint_db_prd.data_product.v_qro',
                'fid_column': 'id_stg_demographic_socioeconomic_qro',
                'fid_alias': 'id_stg_demographic_socioeconomic_qro as fid'
            }
        }
    
    def normalize_city_name(self, city: str) -> str:
        """Convert any city input to standardized city code"""
        if not city:
            return self.config.get('DEFAULT_CITY', CityCode.CDMX.value)
        
        normalized = city.lower().strip()
        return self.city_name_mapping.get(normalized, normalized)
    
    def get_city_config(self, city: str) -> Dict[str, Any]:
        """Get complete configuration for a city"""
        normalized_city = self.normalize_city_name(city)
        return self.city_configs.get(normalized_city, self.city_configs.get(self.config['DEFAULT_CITY']))
    
    def get_main_table(self, city: str) -> str:
        """Get the main data table for a city"""
        normalized_city = self.normalize_city_name(city)
        return self.table_mappings[normalized_city]['main_table']
    
    def get_fid_column(self, city: str) -> str:
        """Get the primary key column name"""
        normalized_city = self.normalize_city_name(city)
        return self.table_mappings[normalized_city]['fid_column']
    
    def get_fid_column_with_alias(self, city: str) -> str:
        """Get the FID column with proper aliasing for queries"""
        normalized_city = self.normalize_city_name(city)
        return self.table_mappings[normalized_city]['fid_alias']
    
    def is_qro_city(self, city: str) -> bool:
        """Check if city is QRO region"""
        return self.normalize_city_name(city) == CityCode.QRO.value
    
    def is_cdmx_city(self, city: str) -> bool:
        """Check if city is CDMX region"""
        return self.normalize_city_name(city) == CityCode.CDMX.value


class ColumnGenerator:
    """
    Generates standardized column lists to eliminate massive duplication.
    Replaces hundreds of hardcoded column definitions.
    """
    
    @staticmethod
    def get_common_base_columns() -> List[str]:
        """Base columns available in both cities"""
        return [
            'ids_market_data_spot2',
            'ids_market_data_inmuebles24', 
            'centroid',
            'is_on_market',
            'h3_indexes',
            'bbox',
            'geometry_type'
        ]
    
    @staticmethod
    def get_poi_distance_columns(distances: List[str] = None) -> List[str]:
        """
        Generate POI columns for specified distances.
        This eliminates the massive duplication in query.py
        """
        if distances is None:
            distances = ['100m', '200m', '250m', '500m', '1km', 'front']
        
        # POI categories that exist in both cities
        poi_categories = [
            'accommodation', 'active_life', 'arts_and_entertainment', 
            'attractions_and_activities', 'automotive', 'beauty_and_spa',
            'business_to_business', 'eat_and_drink', 'education', 
            'financial_service', 'health_and_medical', 'home_service',
            'mass_media', 'pets', 'private_establishments_and_corporates',
            'professional_services', 'public_service_and_government',
            'real_estate', 'religious_organization', 'retail',
            'structure_and_geography', 'travel'
        ]
        
        # Categories that have brand data
        brand_categories = [
            'active_life', 'arts_and_entertainment', 'attractions_and_activities',
            'automotive', 'eat_and_drink', 'education', 'financial_service',
            'health_and_medical', 'pets', 'public_service_and_government', 'retail'
        ]
        
        columns = []
        for distance in distances:
            # POI count columns
            for category in poi_categories:
                columns.append(f'{category}_{distance}')
            
            # POI ID columns
            columns.append(f'ids_pois_{distance}')
            
            # Brand count columns
            for category in brand_categories:
                columns.append(f'brands_{category}_{distance}')
        
        return columns
    
    @staticmethod
    def get_traffic_columns(distances: List[str] = None) -> List[str]:
        """
        Generate traffic columns for specified distances.
        Eliminates massive duplication of hourly/daily traffic data.
        """
        if distances is None:
            distances = ['front', '500m', '1km']
        
        traffic_metrics = ['at_rest_avg', 'pedestrian_avg', 'motor_vehicle_avg']
        columns = []
        
        for distance in distances:
            # 24-hour traffic data (0-23)
            for hour in range(24):
                for metric in traffic_metrics:
                    columns.append(f'{metric}_x_hour_{hour}_{distance}')
            
            # Weekly traffic data (1-7)
            for day in range(1, 8):
                for metric in traffic_metrics:
                    columns.append(f'{metric}_x_day_of_week_{day}_{distance}')
        
        return columns
    
    @staticmethod
    def get_traffic_columns_front_500m() -> List[str]:
        """
        Generate traffic columns for front and 500m distances only.
        Used for Mexico City which doesn't have 1km traffic data.
        """
        return ColumnGenerator.get_traffic_columns(['front', '500m'])
    
    @staticmethod  
    def get_cdmx_specific_columns() -> List[str]:
        """Columns only available in CDMX v_parcel table"""
        return [
            'ids_market_data_propiedades',
            'street_address', 'total_surface_area', 'total_construction_area',
            'year_built', 'special_facilities', 'unit_land_value', 'land_value',
            'key_vus', 'predominant_level', 'total_houses', 'locality_size',
            'floor_levels', 'open_space', 'id_land_use', 'id_municipality',
            'id_city_blocks', 'rent_price_spot2', 'rent_price_per_m2_spot2',
            'buy_price_spot2', 'buy_price_per_m2_spot2', 'total_area_spot2',
            'property_type_spot2', 'rent_price_inmuebles24', 'rent_price_per_m2_inmuebles24',
            'buy_price_inmuebles24', 'buy_price_per_m2_inmuebles24', 'total_area_inmuebles24',
            'property_type_inmuebles24', 'rent_price_propiedades', 'rent_price_per_m2_propiedades',
            'buy_price_propiedades', 'buy_price_per_m2_propiedades', 'total_area_propiedades',
            'block_type', 'density_d', 'usage_desc', 'city_link', 'scope', 'height',
            'cos', 'cus', 'total_built_perm', 'property_count_per_lot', 'min_housing',
            'crecimiento_promedio_municipal', 'crecimiento_promedio_entidad', 'crecimiento_promedio_ageb'
        ]
    
    @staticmethod
    def get_qro_demographic_columns() -> List[str]:
        """Demographic columns specific to QRO v_qro table"""
        return [
            'cvegep', 'cve_ent', 'nom_ent', 'cve_mun', 'nom_mun', 'cve_loc', 'nom_loc',
            'cve_ageb', 'cve_mza', 'ambito', 'data_sourc', 'p_60ymas', 'p15a17a',
            'pe_inac_f', 'p_12ymas', 'p_3a5', 'p_12a14', 'pdesocup', 'p_6a11',
            'p_15a17', 'p_18a24', 'pocupada_m', 'p15sec_co', 'p18a24a', 'pea_f',
            'p_8a_14', 'graproes_m', 'p18ym_pb', 'pob0_14', 'pocupada_f', 'p15ym_se',
            'p12a14noa', 'pdesocup_f', 'p15pri_in', 'p_15ymas', 'p8a14an', 'grapoes_f',
            'p_3ymas', 'p_5ymas', 'p3a5_noa', 'pe_inac', 'p6a11_noa', 'pe_inac_m',
            'pdesocup_m', 'pocupada', 'p15sec_in', 'p15pri_co', 'pobtot', 'pea',
            'p15ym_an', 'p_0a2', 'pob15_64', 'pob65_mas', 'graproes', 'pea_m',
            'p_18ymas', 'niv_predom', 'tot_vivien', 'tot_viv_ab', 'tot_viv_cp',
            'tot_viv_c', 'tot_viv_cm', 'tot_viv_dp', 'tot_viv_d', 'tot_viv_e',
            'pct_viv_ab', 'pct_viv_cp', 'pct_viv_c', 'pct_viv_cm', 'pct_viv_dp',
            'pct_viv_d', 'pct_viv_e'
        ]


class UnifiedQueryBuilder:
    """
    Builds SQL queries using centralized city management.
    Replaces the massive redundant query.py file.
    """
    
    def __init__(self, city_manager: CityManager):
        self.city_manager = city_manager
        self.column_generator = ColumnGenerator()
    
    def build_property_query_columns(self, city: str, show_all_keys: bool = True) -> str:
        """Build complete column list for property queries"""
        normalized_city = self.city_manager.normalize_city_name(city)
        
        # Start with FID column (properly aliased)
        columns = [self.city_manager.get_fid_column_with_alias(city)]
        
        # Add common base columns
        columns.extend(self.column_generator.get_common_base_columns())
        
        # Add city-specific columns
        if normalized_city == CityCode.CDMX.value:
            columns.extend(self.column_generator.get_cdmx_specific_columns())
        elif normalized_city == CityCode.QRO.value:
            columns.extend(self.column_generator.get_qro_demographic_columns())
        
        # Add POI and traffic columns if requested (city-aware distances)
        if show_all_keys:
            # Both cities have POI data for all distances
            columns.extend(self.column_generator.get_poi_distance_columns())
            
            # Traffic columns: Mexico has front+500m, QRO has front+500m+1km
            if normalized_city == CityCode.CDMX.value:
                # Mexico: only front and 500m traffic columns
                columns.extend(self.column_generator.get_traffic_columns_front_500m())
            elif normalized_city == CityCode.QRO.value:
                # QRO: all traffic columns including 1km
                columns.extend(self.column_generator.get_traffic_columns())
        
        return ',\n                '.join(columns)
    
    def build_complete_property_query(self, filter_clause: str, city: str, show_all_keys: bool = True) -> str:
        """Build complete SQL query for property data"""
        table = self.city_manager.get_main_table(city)
        columns = self.build_property_query_columns(city, show_all_keys)
        
        return f'''SELECT {columns}
        FROM {table}
        {filter_clause}'''
    
    def build_filter_conditions(self, filters: Dict[str, Any], city: str) -> str:
        """Build WHERE clause based on filters and city capabilities"""
        normalized_city = self.city_manager.normalize_city_name(city)
        conditions = []
        
        # Common filters for both cities
        if 'geometry' in filters:
            conditions.append(f"ST_Intersects(centroid, ST_SetSRID(ST_GeomFromGeoJSON('{filters['geometry']}'), 4326))")
        
        if 'availability' in filters and filters['availability']:
            if filters['availability'] == 'available':
                conditions.append("is_on_market = true")
            elif filters['availability'] == 'not_available':
                conditions.append("is_on_market = false")
        
        # City-specific filters
        if normalized_city == CityCode.CDMX.value:
            conditions.extend(self._build_cdmx_filters(filters))
        elif normalized_city == CityCode.QRO.value:
            conditions.extend(self._build_qro_filters(filters))
        
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""
    
    def _build_cdmx_filters(self, filters: Dict[str, Any]) -> List[str]:
        """Build CDMX-specific filter conditions"""
        conditions = []
        
        if 'property_type' in filters and filters['property_type']:
            property_condition = f"(property_type_spot2 = '{filters['property_type']}' OR property_type_inmuebles24 = '{filters['property_type']}' OR property_type_propiedades = '{filters['property_type']}')"
            conditions.append(property_condition)
        
        if 'plot_min' in filters or 'plot_max' in filters:
            plot_conditions = []
            if 'plot_min' in filters:
                plot_conditions.append(f"total_surface_area >= {filters['plot_min']}")
            if 'plot_max' in filters:
                plot_conditions.append(f"total_surface_area <= {filters['plot_max']}")
            if plot_conditions:
                conditions.append(f"({' AND '.join(plot_conditions)})")
        
        if 'price_min' in filters or 'price_max' in filters:
            price_type = filters.get('price_type', 'buy')
            price_columns = ['rent_price_spot2', 'rent_price_inmuebles24', 'rent_price_propiedades'] if price_type == 'rent' else ['buy_price_spot2', 'buy_price_inmuebles24', 'buy_price_propiedades']
            
            price_conditions = []
            for col in price_columns:
                col_conditions = []
                if 'price_min' in filters:
                    col_conditions.append(f"{col} >= {filters['price_min']}")
                if 'price_max' in filters:
                    col_conditions.append(f"{col} <= {filters['price_max']}")
                if col_conditions:
                    price_conditions.append(f"({' AND '.join(col_conditions)})")
            
            if price_conditions:
                conditions.append(f"({' OR '.join(price_conditions)})")
        
        return conditions
    
    def _build_qro_filters(self, filters: Dict[str, Any]) -> List[str]:
        """Build QRO-specific filter conditions"""
        conditions = []
        
        # QRO has different data structure - mainly demographic data
        # Can add QRO-specific filtering logic here as needed
        
        return conditions


# Global instances for use throughout the application
city_manager = CityManager()
query_builder = UnifiedQueryBuilder(city_manager)