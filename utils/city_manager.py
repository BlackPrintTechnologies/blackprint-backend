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

    @staticmethod
    def get_all_business_category_columns() -> str:
        """
        Generate all business category columns for commercial growth.
        Returns formatted string ready for SQL query.
        """
        categories = [
            'eat_and_drink', 'health_and_medical', 'beauty_and_spa', 'financial_service',
            'arts_and_entertainment', 'active_life', 'retail', 'pets', 
            'attractions_and_activities', 'education', 'others'
        ]
        
        levels = ['ageb', 'municipal', 'entidad']
        years = ['2010', '2015', '2017', '2020', '2023']
        
        columns = []
        
        for category in categories:
            for level in levels:
                for year in years:
                    columns.append(f'total_businesses_{year}_{category}_{level}')
                    if year != '2010':  # No growth for base year 2010
                        columns.append(f'economic_growth_{year}_{category}_{level}')
        
        return ',\n            '.join(columns)


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

    def build_demographics_query(self, fid: str, normalized_city: str) -> str:
        """
        Build demographics query dynamically based on city.
        Eliminates hardcoded SQL in query.py
        """
        if self.city_manager.is_qro_city(normalized_city):
            # QRO demographic query with all population growth columns
            query = f'''select 
                id_stg_demographic_socioeconomic_qro as fid,
                cvegep, cve_ent, nom_ent, cve_mun, nom_mun, cve_loc, nom_loc,
                cve_ageb, cve_mza, ambito, data_sourc, pobtot, pea, pea_m, pea_f,
                p_18ymas, p_15ymas, p_12ymas, p_3ymas, p_0a2, p_3a5, p_6a11,
                p_12a14, p_15a17, p_18a24, p_60ymas, pob15_64, pob65_mas,
                p15ym_an, p15ym_se, p15pri_in, p15pri_co, p15sec_in, p15sec_co,
                p18a24a, p18ym_pb, graproes, graproes_m, grapoes_f, p3a5_noa,
                p6a11_noa, p8a14an, p12a14noa, pe_inac, pe_inac_m, pe_inac_f,
                pocupada, pocupada_m, pocupada_f, pdesocup, pdesocup_m, pdesocup_f,
                niv_predom, tot_vivien, tot_viv_ab, tot_viv_cp, tot_viv_c, tot_viv_cm,
                tot_viv_dp, tot_viv_d, tot_viv_e, pct_viv_ab, pct_viv_cp,
                pct_viv_c, pct_viv_cm, pct_viv_dp, pct_viv_d, pct_viv_e,
                geometry_type, bbox,
                -- Population growth fields AGEB level
                pob_2000_ageb, pob_2005_ageb, pob_2010_ageb, pob_2015_ageb, pob_2020_ageb,
                cambio_porcentual_2005_ageb, cambio_porcentual_2010_ageb,
                cambio_porcentual_2015_ageb, cambio_porcentual_2020_ageb,
                -- Population growth fields Entity level
                pob_2000_entidad, pob_2005_entidad, pob_2010_entidad,
                pob_2015_entidad, pob_2020_entidad, cambio_porcentual_2005_entidad,
                cambio_porcentual_2010_entidad, cambio_porcentual_2015_entidad,
                cambio_porcentual_2020_entidad,
                -- Population growth fields Municipal level
                pob_2000_municipal, pob_2005_municipal, pob_2010_municipal,
                pob_2015_municipal, pob_2020_municipal, cambio_porcentual_2005_municipal,
                cambio_porcentual_2010_municipal, cambio_porcentual_2015_municipal,
                cambio_porcentual_2020_municipal
                from {self.city_manager.get_main_table(normalized_city)}
                where id_stg_demographic_socioeconomic_qro = {fid}
                    '''
        else:
            # CDMX demographic query with all population growth columns
            query = f'''select 
                fid, neighborhood, nom_mun, predominant_level, ageb_code,
                vivtot, vivtot_colonia, vivtot_alcaldia, prom_ocup, prom_ocup_colonia, 
                prom_ocup_alcaldia, pro_ocup_c, pro_ocup_c_colonia, pro_ocup_c_alcaldia, 
                ses_ab, ses_ab_colonia, ses_ab_alcaldia, ses_c_plus, ses_c_plus_colonia,
                ses_c_plus_alcaldia, ses_c, ses_c_colonia, ses_c_alcaldia,
                ses_c_minus, ses_c_minus_colonia, ses_c_minus_alcaldia, ses_d,
                ses_d_colonia, ses_d_alcaldia, ses_d_plus, ses_d_plus_colonia,
                ses_d_plus_alcaldia, ses_e, ses_e_colonia, ses_e_alcaldia,
                pobtot, pobtot_colonia, pobtot_alcaldia, pobmas, pobmas_colonia,
                pobmas_alcaldia, pobfem, pobfem_colonia, pobfem_alcaldia,
                p_3a5, p_3a5_colonia, p_3a5_alcaldia, p3a5_noa, p3a5_noa_colonia,
                p3a5_noa_alcaldia, p_6a11, p_6a11_colonia, p_6a11_alcaldia,
                p6a11_noa, p6a11_noa_colonia, p6a11_noa_alcaldia, p_12a14,
                p_12a14_colonia, p_12a14_alcaldia, p12a14noa, p12a14noa_colonia,
                p12a14noa_alcaldia, p_15a17, p_15a17_colonia, p_15a17_alcaldia,
                p15a17a, p15a17a_colonia, p15a17a_alcaldia, p_18a24,
                p_18a24_colonia, p_18a24_alcaldia, p18a24a, p18a24a_colonia,
                p18a24a_alcaldia, pea, pea_colonia, pea_alcaldia, pea_m,
                pea_m_colonia, pea_m_alcaldia, pea_f, pea_f_colonia, pea_f_alcaldia,
                pe_inac, pe_inac_colonia, pe_inac_alcaldia, pe_inac_m, pe_inac_m_colonia,
                pe_inac_m_alcaldia, pe_inac_f, pe_inac_f_colonia, pe_inac_f_alcaldia,
                pocupada, pocupada_colonia, pocupada_alcaldia, pocupada_m,
                pocupada_m_colonia, pocupada_m_alcaldia, pocupada_f,
                pocupada_f_colonia, pocupada_f_alcaldia, pdesocup, pdesocup_colonia,
                pdesocup_alcaldia, pdesocup_m, pdesocup_m_colonia, pdesocup_m_alcaldia,
                pdesocup_f, pdesocup_f_colonia, pdesocup_f_alcaldia,
                -- Population growth columns for AGEB level
                pob_2000_ageb, pob_2005_ageb, pob_2010_ageb, pob_2015_ageb, pob_2020_ageb,
                cambio_porcentual_2005_ageb, cambio_porcentual_2010_ageb,
                cambio_porcentual_2015_ageb, cambio_porcentual_2020_ageb,
                -- Population growth columns for Municipal level
                pob_2000_municipal, pob_2005_municipal, pob_2010_municipal,
                pob_2015_municipal, pob_2020_municipal, cambio_porcentual_2005_municipal,
                cambio_porcentual_2010_municipal, cambio_porcentual_2015_municipal,
                cambio_porcentual_2020_municipal,
                -- Population growth columns for Entity level
                pob_2000_entidad, pob_2005_entidad, pob_2010_entidad,
                pob_2015_entidad, pob_2020_entidad, cambio_porcentual_2005_entidad,
                cambio_porcentual_2010_entidad, cambio_porcentual_2015_entidad,
                cambio_porcentual_2020_entidad
                from {self.city_manager.get_main_table(normalized_city)}
                where fid = {fid}
                '''
        return query

    def build_commercial_growth_query(self, fid: str) -> str:
        """
        Build commercial growth query (Mexico City only).
        Eliminates massive hardcoded SQL in query.py
        """
        query = f'''SELECT 
            fid,
            -- AGEB base columns
            total_businesses_2010_ageb, total_businesses_2015_ageb, economic_growth_2015_ageb,
            total_businesses_2017_ageb, economic_growth_2017_ageb, total_businesses_2020_ageb,
            economic_growth_2020_ageb, total_businesses_2023_ageb, economic_growth_2023_ageb,
            -- MUNICIPAL base columns
            total_businesses_2010_municipal, total_businesses_2015_municipal, economic_growth_2015_municipal,
            total_businesses_2017_municipal, economic_growth_2017_municipal, total_businesses_2020_municipal,
            economic_growth_2020_municipal, total_businesses_2023_municipal, economic_growth_2023_municipal,
            -- ENTIDAD base columns
            total_businesses_2010_entidad, total_businesses_2015_entidad, economic_growth_2015_entidad,
            total_businesses_2017_entidad, economic_growth_2017_entidad, total_businesses_2020_entidad,
            economic_growth_2020_entidad, total_businesses_2023_entidad, economic_growth_2023_entidad,
            -- All business category columns
            {self.column_generator.get_all_business_category_columns()}
            FROM blackprint_db_prd.data_product.v_parcel_v3
            where fid = {fid}
            '''
        return query

    def build_market_info_query(self, spot2: str, inmuebles24: str, propiedades: str, normalized_city: str) -> str:
        """
        Build market info query dynamically based on city and data source.
        Eliminates hardcoded SQL in query.py
        """
        city_config = self.city_manager.get_city_config(normalized_city)
        
        if inmuebles24:
            table = city_config.get('market_data_inmuebles24')
            query = f'''SELECT
                    "id_market_data_inmuebles24" AS "id_market_data_inmuebles24",
                    "title" AS "title", "description" AS "description",
                    "rent_price" AS "rent_price", "rent_price_clean" AS "rent_price_clean",
                    "rent_price_per_m2" AS "rent_price_per_m2", "buy_price" AS "buy_price",
                    "buy_price_clean" AS "buy_price_clean", "buy_price_per_m2" AS "buy_price_per_m2",
                    "maintenance_price" AS "maintenance_price", "publication_date" AS "publication_date",
                    "parking_lot" AS "parking_lot", "bathrooms" AS "bathrooms",
                    "bedrooms" AS "bedrooms", "age" AS "age", "pictures" AS "pictures",
                    "property_type" AS "property_type", "operation_type" AS "operation_type",
                    "property_dimension" AS "total_area", "property_dimension_clean" AS "total_area_clean",
                    "zone" AS "zone", "city" AS "city", "address" AS "address",
                    "url" AS "url", "amenities" AS "amenities"
                    FROM {table}
                    WHERE id_market_data_inmuebles24 = {inmuebles24} '''
        elif spot2:
            table = city_config.get('market_data_spot2')
            query = f'''SELECT
                        "id_market_data_spot2" AS "id_market_data_spot2",
                        "title" AS "title", "address" AS "address", "street_address" AS "street_address",
                        "city" AS "city", "zip_code" AS "zip_code", "description" AS "description",
                        "operation_type" AS "operation_type", "rent_price" AS "rent_price",
                        "rent_price_clean" AS "rent_price_clean", "rent_price_per_m2" AS "rent_price_per_m2",
                        "buy_price" AS "buy_price", "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2", "maintenance_price" AS "maintenance_price",
                        "property_type" AS "property_type", "total_area" AS "total_area",
                        "total_area_clean" AS "total_area_clean", "amenities" AS "amenities",
                        "pictures" AS "pictures", "url" AS "url", "parking_spaces" AS "parking_spaces",
                        "condition" AS "condition", "date_published" AS "publication_date"
                    FROM {table}
                    WHERE id_market_data_spot2 = {spot2} '''
        elif propiedades:
            if self.city_manager.is_qro_city(normalized_city):
                # QRO doesn't have propiedades table
                query = '''SELECT
                        NULL AS "id_market_data_propiedades", 
                        NULL AS "url", 
                        NULL AS "property_type",
                        NULL AS "description", 
                        NULL AS "buy_price", 
                        NULL AS "buy_price_usd",
                        NULL AS "buy_price_clean", 
                        NULL AS "buy_price_per_m2", 
                        NULL AS "rent_price",
                        NULL AS "rent_price_usd", 
                        NULL AS "rent_price_clean", 
                        NULL AS "rent_price_per_m2",
                        NULL AS "size", 
                        NULL AS "total_area_clean", 
                        NULL AS "postal_code",
                        NULL AS "street_address", 
                        NULL AS "bedrooms", 
                        NULL AS "bathrooms",
                        NULL AS "geometry_coords"
                        WHERE 1=0 '''
            else:
                table = city_config.get('market_data_propiedades')
                query = f'''SELECT
                        "id_market_data_propiedades" AS "id_market_data_propiedades",
                        "url" AS "url", 
                        "property_type" AS "property_type", 
                        "description" AS "description",
                        "buy_price" AS "buy_price", 
                        "buy_price_usd" AS "buy_price_usd",
                        "buy_price_clean" AS "buy_price_clean", 
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "rent_price" AS "rent_price", 
                        "rent_price_usd" AS "rent_price_usd",
                        "rent_price_clean" AS "rent_price_clean", 
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "size" AS "size", 
                        "total_area_clean" AS "total_area_clean",
                        "postal_code" AS "postal_code", 
                        "street_address" AS "street_address",
                        "bedrooms" AS "bedrooms", 
                        "bathrooms" AS "bathrooms",
                        "geometry_coords" AS "geometry_coords"
                        FROM {table}
                        WHERE id_market_data_propiedades = {propiedades} '''
        else:
            query = "SELECT NULL WHERE 1=0"
        
        return query


# Global instances for use throughout the application
city_manager = CityManager()
query_builder = UnifiedQueryBuilder(city_manager)