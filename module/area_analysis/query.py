import logging

logger = logging.getLogger(__name__)

class AreaAnalysisQuery:
    """Query builder class for area analysis operations."""
    
    def __init__(self):
        pass
    
    def build_traffic_by_day_query(self, lat, lng, radius, user_type=None):
        """Build SQL query for traffic data by day of the week."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT  SUM(CASE WHEN a.dia_de_la_semana = 'Monday' THEN a.total_usuarios_unicos ELSE 0 END) AS monday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Tuesday' THEN a.total_usuarios_unicos ELSE 0 END) AS tuesday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Wednesday' THEN a.total_usuarios_unicos ELSE 0 END) AS wednesday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Thursday' THEN a.total_usuarios_unicos ELSE 0 END) AS thursday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Friday' THEN a.total_usuarios_unicos ELSE 0 END) AS friday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Saturday' THEN a.total_usuarios_unicos ELSE 0 END) AS saturday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Sunday' THEN a.total_usuarios_unicos ELSE 0 END) AS sunday
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_dia_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query

    def build_traffic_by_hour_query(self, lat, lng, radius, user_type=None):
        """Build SQL query for traffic data by hour of the day."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT  SUM(CASE WHEN a.hour = 0 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_0,
                SUM(CASE WHEN a.hour = 1 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_1,
                SUM(CASE WHEN a.hour = 2 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_2,
                SUM(CASE WHEN a.hour = 3 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_3,
                SUM(CASE WHEN a.hour = 4 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_4,
                SUM(CASE WHEN a.hour = 5 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_5,
                SUM(CASE WHEN a.hour = 6 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_6,
                SUM(CASE WHEN a.hour = 7 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_7,
                SUM(CASE WHEN a.hour = 8 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_8,
                SUM(CASE WHEN a.hour = 9 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_9,
                SUM(CASE WHEN a.hour = 10 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_10,
                SUM(CASE WHEN a.hour = 11 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_11,
                SUM(CASE WHEN a.hour = 12 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_12,
                SUM(CASE WHEN a.hour = 13 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_13,
                SUM(CASE WHEN a.hour = 14 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_14,
                SUM(CASE WHEN a.hour = 15 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_15,
                SUM(CASE WHEN a.hour = 16 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_16,
                SUM(CASE WHEN a.hour = 17 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_17,
                SUM(CASE WHEN a.hour = 18 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_18,
                SUM(CASE WHEN a.hour = 19 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_19,
                SUM(CASE WHEN a.hour = 20 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_20,
                SUM(CASE WHEN a.hour = 21 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_21,
                SUM(CASE WHEN a.hour = 22 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_22,
                SUM(CASE WHEN a.hour = 23 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_23
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query

    def build_traffic_summary_query(self, lat, lng, radius, user_type=None):
        """Build SQL query for total traffic summary."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT  SUM(a.total_usuarios_unicos) as total_users
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query

    def build_population_query(self, lat, lng, radius, config_city='queretaro'):
        """Build SQL query to get population data within the specified area using proper spatial calculations."""
        
        if config_city == 'queretaro':
            query = f"""
            SELECT 
                SUM(d.pobtot) as total_population,
                MAX(d.pobtot_alcaldia) as municipality_population,
                MAX(d.cve_mun) as municipality_code,
                MAX(d.nom_mun) as municipality_name
            FROM blackprint_db_prd.data_product.v_qro d
            WHERE d.centroid IS NOT NULL
            AND d.centroid != ''
            AND ST_DWithin(
                ST_Transform(ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), 3857),
                ST_Transform(ST_SetSRID(ST_MakePoint(
                    CAST(JSON_EXTRACT_PATH_TEXT(d.centroid, 'coordinates', '0') AS FLOAT),
                    CAST(JSON_EXTRACT_PATH_TEXT(d.centroid, 'coordinates', '1') AS FLOAT)
                ), 4326), 3857),
                {radius}
            )
            """
        else:
            # For Mexico City (CDMX)
            query = f"""
            SELECT 
                SUM(d.pobtot) as total_population,
                MAX(d.pobtot_alcaldia) as municipality_population,
                MAX(d.cve_mun) as municipality_code,
                MAX(d.nom_mun) as municipality_name
            FROM blackprint_db_prd.data_product.v_parcel_v3 d
            WHERE d.centroid IS NOT NULL
            AND d.centroid != ''
            AND ST_DWithin(
                ST_Transform(ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), 3857),
                ST_Transform(ST_SetSRID(ST_MakePoint(
                    CAST(SPLIT_PART(d.centroid, ',', 2) AS FLOAT),
                    CAST(SPLIT_PART(d.centroid, ',', 1) AS FLOAT)
                ), 4326), 3857),
                {radius}
            )
            """
        return query

    def build_municipality_traffic_query(self, municipality_code, user_type=None):
        """Build SQL query for municipality-level traffic data using dynamic H3 generation."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        # Use the working approach - generate H3 hexagons dynamically from municipality polygons
        # This ensures H3 values match the traffic data region
        query = f"""
        WITH municipality_polygons AS (
            SELECT ST_GeomFromGeoJSON(geometry_geojson) as geom
            FROM blackprint_db_prd.data_product.v_qro 
            WHERE cve_mun = '{municipality_code}'
            AND geometry_geojson IS NOT NULL
            AND geometry_geojson != ''
            
        ),
        h3_values AS (
            SELECT H3_Polyfill(geom, 10) AS h3_indexes 
            FROM municipality_polygons
        ),
        h3_index AS (
            SELECT DISTINCT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT SUM(a.total_usuarios_unicos) as total_users
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query

    def build_socioeconomic_query(self, lat, lng, radius, config_city='mexico'):
        """Build SQL query to get socioeconomic data within the specified area."""
        # Convert radius from meters to degrees (approximate conversion for latitude)
        # 1 degree ≈ 111,000 meters
        radius_degrees = radius / 111000.0
        
        if config_city == 'queretaro':
            # QRO socioeconomic query - use stg_demographic_socioeconomic_qro table
            query = f"""
            SELECT 
                d.pobtot as total_population,
                d.niv_predom as predominant_level,
                d.tot_vivien as total_households,
                d.pct_viv_ab, d.pct_viv_cp, d.pct_viv_c, d.pct_viv_cm, 
                d.pct_viv_dp, d.pct_viv_d, d.pct_viv_e,
                d.graproes as education_level,
                d.nom_loc as neighborhood,
                d.nom_mun as municipality
            FROM blackprint_db_prd.staging.stg_demographic_socioeconomic_qro d
            WHERE d.geometry_coords IS NOT NULL
            AND ST_DWithin(
                ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326),
                CASE 
                    WHEN ST_SRID(d.geometry_coords) = 0 THEN ST_SetSRID(d.geometry_coords, 4326)
                    ELSE d.geometry_coords 
                END,
                {radius_degrees}
            )
            """
        else:
            # CDMX socioeconomic query - use v_parcel_v3 table
            query = f"""
            SELECT 
                d.pobtot as total_population,
                d.predominant_level,
                d.vivtot as total_households,
                d.ses_ab as pct_viv_ab, 
                d.ses_c_plus as pct_viv_cp, 
                d.ses_c as pct_viv_c, 
                d.ses_c_minus as pct_viv_cm,
                d.ses_d_plus as pct_viv_dp, 
                d.ses_d as pct_viv_d, 
                d.ses_e as pct_viv_e,
                d.graproes as education_level,
                d.neighborhood,
                d.nom_mun as municipality
            FROM blackprint_db_prd.data_product.v_parcel_v3 d
            WHERE d.centroid IS NOT NULL
            AND d.centroid != ''
            AND d.centroid LIKE '%coordinates%'
            LIMIT 1000
            """
        return query

    def build_mexico_demographics_query(self, lat, lng, radius):
        """
        Build optimized Mexico City demographics query using accurate spatial filtering.
        Uses PostGIS ST_Buffer with proper coordinate system transformations for precise radius filtering.
        """
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        buffered_4326 AS (
          SELECT ST_Transform(geom, 4326) AS geom FROM buffered
        )
        SELECT 
            -- General demographic data
            d.neighborhood,
            d.predominant_level,
            d.ageb_code,
            d.vivtot,
            d.prom_ocup,
            d.pro_ocup_c,
            
            -- Colonia level data
            d.vivtot_colonia,
            d.prom_ocup_colonia,
            d.pro_ocup_c_colonia,
            
            -- Alcaldia level data
            d.nom_mun,
            d.vivtot_alcaldia,
            d.prom_ocup_alcaldia,
            d.pro_ocup_c_alcaldia,
            
            -- Socioeconomic data - Block level
            d.ses_ab,
            d.ses_c_plus,
            d.ses_c,
            d.ses_c_minus,
            d.ses_d,
            d.ses_d_plus,
            d.ses_e,
            
            -- Socioeconomic data - Colonia level
            d.ses_ab_colonia,
            d.ses_c_plus_colonia,
            d.ses_c_colonia,
            d.ses_c_minus_colonia,
            d.ses_d_colonia,
            d.ses_d_plus_colonia,
            d.ses_e_colonia,
            
            -- Socioeconomic data - Alcaldia level
            d.ses_ab_alcaldia,
            d.ses_c_plus_alcaldia,
            d.ses_c_alcaldia,
            d.ses_c_minus_alcaldia,
            d.ses_d_alcaldia,
            d.ses_d_plus_alcaldia,
            d.ses_e_alcaldia,
            
            -- Population data - Block level
            (d.pobmas_alcaldia + d.pobfem_alcaldia) as pobtot,
            d.pobmas,
            d.pobfem,
            
            -- Population data - Colonia level
            d.pobtot_colonia,
            d.pobmas_colonia,
            d.pobfem_colonia,
            
            -- Population data - Alcaldia level
            d.pobtot_alcaldia,
            d.pobmas_alcaldia,
            d.pobfem_alcaldia,
            
            -- Education data - Block level
            d.p_3a5,
            d.p_6a11,
            d.p_12a14,
            d.p_15a17,
            d.p_18a24,
            d.p3a5_noa,
            d.p6a11_noa,
            d.p12a14noa,
            d.p15a17a,
            d.p18a24a,
            
            -- Gender-specific age data for age pyramid
            d.p_0a2,
            d.p_0a2_f,
            d.p_0a2_m,
            d.p_3a5_f,
            d.p_3a5_m,
            d.p_6a11_f,
            d.p_6a11_m,
            d.p_12a14_f,
            d.p_12a14_m,
            d.p_15a17_f,
            d.p_15a17_m,
            d.p_18a24_f,
            d.p_18a24_m,
            d.p_60ymas,
            d.p_60ymas_f,
            d.p_60ymas_m,
            
            -- Education data - Colonia level
            d.p_3a5_colonia,
            d.p_6a11_colonia,
            d.p_12a14_colonia,
            d.p_15a17_colonia,
            d.p_18a24_colonia,
            d.p3a5_noa_colonia,
            d.p6a11_noa_colonia,
            d.p12a14noa_colonia,
            d.p15a17a_colonia,
            d.p18a24a_colonia,
            
            -- Education data - Alcaldia level
            d.p_3a5_alcaldia,
            d.p_6a11_alcaldia,
            d.p_12a14_alcaldia,
            d.p_15a17_alcaldia,
            d.p_18a24_alcaldia,
            d.p3a5_noa_alcaldia,
            d.p6a11_noa_alcaldia,
            d.p12a14noa_alcaldia,
            d.p15a17a_alcaldia,
            d.p18a24a_alcaldia,
            
            -- Workforce data - Block level
            d.pea,
            d.pea_m,
            d.pea_f,
            d.pe_inac,
            d.pe_inac_m,
            d.pe_inac_f,
            
            -- Workforce data - Colonia level
            d.pea_colonia,
            d.pea_m_colonia,
            d.pea_f_colonia,
            d.pe_inac_colonia,
            d.pe_inac_m_colonia,
            d.pe_inac_f_colonia,
            
            -- Workforce data - Alcaldia level
            d.pea_alcaldia,
            d.pea_m_alcaldia,
            d.pea_f_alcaldia,
            d.pe_inac_alcaldia,
            d.pe_inac_m_alcaldia,
            d.pe_inac_f_alcaldia,
            
            -- Employment data - Block level
            d.pocupada,
            d.pocupada_m,
            d.pocupada_f,
            d.pdesocup,
            d.pdesocup_m,
            d.pdesocup_f,
            
            -- Employment data - Colonia level
            d.pocupada_colonia,
            d.pocupada_m_colonia,
            d.pocupada_f_colonia,
            d.pdesocup_colonia,
            d.pdesocup_m_colonia,
            d.pdesocup_f_colonia,
            
            -- Employment data - Alcaldia level
            d.pocupada_alcaldia,
            d.pocupada_m_alcaldia,
            d.pocupada_f_alcaldia,
            d.pdesocup_alcaldia,
            d.pdesocup_m_alcaldia,
            d.pdesocup_f_alcaldia,
            
            -- Population growth data - Block level
            d.pob_2000_ageb,
            d.pob_2005_ageb,
            d.pob_2010_ageb,
            d.pob_2015_ageb,
            d.pob_2020_ageb,
            d.cambio_porcentual_2005_ageb,
            d.cambio_porcentual_2010_ageb,
            d.cambio_porcentual_2015_ageb,
            d.cambio_porcentual_2020_ageb,
            
            -- Population growth data - Colonia level
            d.pob_2000_entidad,
            d.pob_2005_entidad,
            d.pob_2010_entidad,
            d.pob_2015_entidad,
            d.pob_2020_entidad,
            d.cambio_porcentual_2005_entidad,
            d.cambio_porcentual_2010_entidad,
            d.cambio_porcentual_2015_entidad,
            d.cambio_porcentual_2020_entidad,
            
            -- Population growth data - Alcaldia level
            d.pob_2000_municipal,
            d.pob_2005_municipal,
            d.pob_2010_municipal,
            d.pob_2015_municipal,
            d.pob_2020_municipal,
            d.cambio_porcentual_2005_municipal,
            d.cambio_porcentual_2010_municipal,
            d.cambio_porcentual_2015_municipal,
            d.cambio_porcentual_2020_municipal,
            
            -- Centroid for distance filtering
            d.centroid,
            
            -- Area data for municipality calculation
            d.total_area,
            d.municipality_code,
            d.municipality_nm
            
        FROM blackprint_db_prd.data_product.v_parcel_v3 d
        CROSS JOIN buffered_4326 b
        WHERE d.centroid IS NOT NULL
        AND d.centroid != ''
        AND d.centroid LIKE '%coordinates%'
        AND ST_Intersects(
            ST_SetSRID(
                ST_MakePoint(
                    CAST(SPLIT_PART(REPLACE(REPLACE(d.centroid, '{{"type":"Point","coordinates":[', ''), ']}}', ''), ',', 1) AS FLOAT),
                    CAST(SPLIT_PART(REPLACE(REPLACE(d.centroid, '{{"type":"Point","coordinates":[', ''), ']}}', ''), ',', 2) AS FLOAT)
                ), 
                4326
            ),
            b.geom
        )
        """
        return query

    def build_queretaro_demographics_query(self, lat, lng, radius):
        """
        Build Queretaro demographics query with all aggregations included.
        This query performs all the aggregations that were previously done in the controller,
        providing better control and performance by doing calculations at the database level.
        """
        query = f"""
        WITH demographic_data AS (
        SELECT 
              -- ===== WORKFORCE DATA (SUM across all records in radius) =====
              SUM(d.pea) as pea,                    -- Total economically active population
              SUM(d.pea_m) as pea_m,                -- Total economically active male population
              SUM(d.pea_f) as pea_f,                -- Total economically active female population
              SUM(d.pe_inac) as pe_inac,            -- Total economically inactive population
              SUM(d.pe_inac_m) as pe_inac_m,        -- Total economically inactive male population
              SUM(d.pe_inac_f) as pe_inac_f,        -- Total economically inactive female population
              
              -- ===== EMPLOYMENT DATA (SUM across all records in radius) =====
              SUM(d.pocupada) as pocupada,          -- Total employed population
              SUM(d.pocupada_m) as pocupada_m,      -- Total employed male population
              SUM(d.pocupada_f) as pocupada_f,      -- Total employed female population
              SUM(d.pdesocup) as pdesocup,          -- Total unemployed population
              SUM(d.pdesocup_m) as pdesocup_m,      -- Total unemployed male population
              SUM(d.pdesocup_f) as pdesocup_f,      -- Total unemployed female population
              
              -- ===== EDUCATION DATA (SUM across all records in radius) =====
              SUM(d.p_3a5) as p_3a5,                -- Population aged 3-5 years
              SUM(d.p_6a11) as p_6a11,              -- Population aged 6-11 years
              SUM(d.p_12a14) as p_12a14,            -- Population aged 12-14 years
              SUM(d.p_15a17) as p_15a17,            -- Population aged 15-17 years
              SUM(d.p_18a24) as p_18a24,            -- Population aged 18-24 years
              SUM(d.p_60ymas) as p_60ymas,          -- Population aged 60+ years
              SUM(d.p3a5_noa) as p3a5_noa,          -- Population aged 3-5 not attending school
              SUM(d.p6a11_noa) as p6a11_noa,        -- Population aged 6-11 not attending school
              SUM(d.p12a14noa) as p12a14noa,        -- Population aged 12-14 not attending school
              SUM(d.p15a17a) as p15a17a,            -- Population aged 15-17 attending school
              SUM(d.p18a24a) as p18a24a,            -- Population aged 18-24 attending school
              
              -- ===== AGE PYRAMID DATA (Block-level totals, municipality-level gender ratios) =====
              SUM(d.p_0a2) as p_0a2,                -- Population aged 0-2 years (block level)
              SUM(d.p_3a5) as p_3a5,                -- Population aged 3-5 years (block level)
              SUM(d.p_6a11) as p_6a11,              -- Population aged 6-11 years (block level)
              SUM(d.p_12a14) as p_12a14,            -- Population aged 12-14 years (block level)
              SUM(d.p_15a17) as p_15a17,            -- Population aged 15-17 years (block level)
              SUM(d.p_18a24) as p_18a24,            -- Population aged 18-24 years (block level)
              SUM(d.p_60ymas) as p_60ymas,          -- Population aged 60+ years (block level)
              
              -- Municipality-level gender ratios for proportional scaling
              MAX(d.p_0a2_m_alcaldia) as p_0a2_m_alcaldia,   -- Male population aged 0-2 years (municipality)
              MAX(d.p_0a2_f_alcaldia) as p_0a2_f_alcaldia,   -- Female population aged 0-2 years (municipality)
              MAX(d.p_3a5_m_alcaldia) as p_3a5_m_alcaldia,   -- Male population aged 3-5 years (municipality)
              MAX(d.p_3a5_f_alcaldia) as p_3a5_f_alcaldia,   -- Female population aged 3-5 years (municipality)
              MAX(d.p_6a11_m_alcaldia) as p_6a11_m_alcaldia, -- Male population aged 6-11 years (municipality)
              MAX(d.p_6a11_f_alcaldia) as p_6a11_f_alcaldia, -- Female population aged 6-11 years (municipality)
              MAX(d.p_12a14_m_alcaldia) as p_12a14_m_alcaldia, -- Male population aged 12-14 years (municipality)
              MAX(d.p_12a14_f_alcaldia) as p_12a14_f_alcaldia, -- Female population aged 12-14 years (municipality)
              MAX(d.p_15a17_m_alcaldia) as p_15a17_m_alcaldia, -- Male population aged 15-17 years (municipality)
              MAX(d.p_15a17_f_alcaldia) as p_15a17_f_alcaldia, -- Female population aged 15-17 years (municipality)
              MAX(d.p_18a24_m_alcaldia) as p_18a24_m_alcaldia, -- Male population aged 18-24 years (municipality)
              MAX(d.p_18a24_f_alcaldia) as p_18a24_f_alcaldia, -- Female population aged 18-24 years (municipality)
              MAX(d.p_60ymas_m_alcaldia) as p_60ymas_m_alcaldia, -- Male population aged 60+ years (municipality)
              MAX(d.p_60ymas_f_alcaldia) as p_60ymas_f_alcaldia, -- Female population aged 60+ years (municipality)
              
              -- Municipality-level totals for ratio calculation
              MAX(d.p_0a2_alcaldia) as p_0a2_alcaldia,       -- Total population aged 0-2 years (municipality)
              MAX(d.p_3a5_alcaldia) as p_3a5_alcaldia,       -- Total population aged 3-5 years (municipality)
              MAX(d.p_6a11_alcaldia) as p_6a11_alcaldia,     -- Total population aged 6-11 years (municipality)
              MAX(d.p_12a14_alcaldia) as p_12a14_alcaldia,   -- Total population aged 12-14 years (municipality)
              MAX(d.p_15a17_alcaldia) as p_15a17_alcaldia,   -- Total population aged 15-17 years (municipality)
              MAX(d.p_18a24_alcaldia) as p_18a24_alcaldia,   -- Total population aged 18-24 years (municipality)
              MAX(d.p_60ymas_alcaldia) as p_60ymas_alcaldia, -- Total population aged 60+ years (municipality)
              
              -- ===== HISTORICAL POPULATION DATA (SUM across all records in radius) =====
              SUM(d.pob_2000_ageb) as pob_2000_ageb, -- Population in 2000 (block level)
              SUM(d.pob_2005_ageb) as pob_2005_ageb, -- Population in 2005 (block level)
              SUM(d.pob_2010_ageb) as pob_2010_ageb, -- Population in 2010 (block level)
              SUM(d.pob_2020_ageb) as pob_2020_ageb, -- Population in 2020 (block level)
              
              -- ===== CURRENT POPULATION AND HOUSEHOLDS (Direct sum from colonia level) =====
              -- Sum population directly from all colonias that intersect with the radius
              SUM(d.tot_vivien) as vivtot,                    -- Total households in selected area
              SUM(COALESCE(d.pobtot, 0)) as pobtot,   -- Total population from all intersecting colonias
              SUM(COALESCE(d.pobmas_colonia, 0)) as pobmas,   -- Total male population from all intersecting colonias
              SUM(COALESCE(d.pobfem_colonia, 0)) as pobfem,   -- Total female population from all intersecting colonias
              
              -- ===== SOCIOECONOMIC LEVELS (AVERAGE across all records) =====
              AVG(d.pct_viv_ab) as ses_ab,          -- Percentage of households in socioeconomic level AB (highest)
              AVG(d.pct_viv_cp) as ses_c_plus,      -- Percentage of households in socioeconomic level C+
              AVG(d.pct_viv_c) as ses_c,            -- Percentage of households in socioeconomic level C
              AVG(d.pct_viv_cm) as ses_c_minus,     -- Percentage of households in socioeconomic level C-
              AVG(d.pct_viv_dp) as ses_d_plus,      -- Percentage of households in socioeconomic level D+
              AVG(d.pct_viv_d) as ses_d,            -- Percentage of households in socioeconomic level D
              AVG(d.pct_viv_e) as ses_e,            -- Percentage of households in socioeconomic level E (lowest)
              
              -- ===== POPULATION GROWTH RATES (AVERAGE across all records) =====
              AVG(d.cambio_porcentual_2005_ageb) as cambio_porcentual_2005_ageb, -- Population growth rate 2000-2005
              AVG(d.cambio_porcentual_2010_ageb) as cambio_porcentual_2010_ageb, -- Population growth rate 2005-2010
              AVG(d.cambio_porcentual_2020_ageb) as cambio_porcentual_2020_ageb, -- Population growth rate 2010-2020
              
              -- ===== LOCATION IDENTIFIERS (from first record) =====
              MAX(d.nom_loc) as neighborhood,       -- Neighborhood name
              MAX(d.niv_predom) as predominant_level, -- Predominant socioeconomic level
              MAX(d.cve_ageb) as ageb_code,         -- Block code (AGEB)
              MAX(d.nom_mun) as nom_mun,            -- Municipality name
              MAX(d.cve_mun) as municipality_code,  -- Municipality code
              MAX(d.nom_mun) as municipality_nm,    -- Municipality name (duplicate)
              MAX(d.centroid) as centroid,          -- Geographic centroid coordinates
              
              -- ===== MUNICIPALITY LEVEL DATA (from first record - same for all records in same municipality) =====
              MAX(d.pobtot_alcaldia) as pobtot_alcaldia,     -- Total population in municipality
              MAX(d.pobmas_alcaldia) as pobmas_alcaldia,     -- Total male population in municipality
              MAX(d.pobfem_alcaldia) as pobfem_alcaldia,     -- Total female population in municipality
              MAX(d.vivtot_alcaldia) as vivtot_alcaldia,     -- Total households in municipality
              MAX(d.pea_alcaldia) as pea_alcaldia,           -- Total economically active population in municipality
              MAX(d.pea_m_alcaldia) as pea_m_alcaldia,       -- Total economically active male population in municipality
              MAX(d.pea_f_alcaldia) as pea_f_alcaldia,       -- Total economically active female population in municipality
              MAX(d.pe_inac_alcaldia) as pe_inac_alcaldia,   -- Total economically inactive population in municipality
              MAX(d.pe_inac_m_alcaldia) as pe_inac_m_alcaldia, -- Total economically inactive male population in municipality
              MAX(d.pe_inac_f_alcaldia) as pe_inac_f_alcaldia, -- Total economically inactive female population in municipality
              MAX(d.pocupada_alcaldia) as pocupada_alcaldia, -- Total employed population in municipality
              MAX(d.pocupada_m_alcaldia) as pocupada_m_alcaldia, -- Total employed male population in municipality
              MAX(d.pocupada_f_alcaldia) as pocupada_f_alcaldia, -- Total employed female population in municipality
              MAX(d.pdesocup_alcaldia) as pdesocup_alcaldia, -- Total unemployed population in municipality
              MAX(d.pdesocup_m_alcaldia) as pdesocup_m_alcaldia, -- Total unemployed male population in municipality
              MAX(d.pdesocup_f_alcaldia) as pdesocup_f_alcaldia, -- Total unemployed female population in municipality
              
              -- ===== MUNICIPALITY LEVEL EDUCATION DATA =====
              MAX(d.p_3a5_alcaldia) as p_3a5_alcaldia,       -- Population aged 3-5 in municipality
              MAX(d.p_6a11_alcaldia) as p_6a11_alcaldia,     -- Population aged 6-11 in municipality
              MAX(d.p_12a14_alcaldia) as p_12a14_alcaldia,   -- Population aged 12-14 in municipality
              MAX(d.p_15a17_alcaldia) as p_15a17_alcaldia,   -- Population aged 15-17 in municipality
              MAX(d.p_18a24_alcaldia) as p_18a24_alcaldia,   -- Population aged 18-24 in municipality
              MAX(d.p3a5_noa_alcaldia) as p3a5_noa_alcaldia, -- Population aged 3-5 not attending school in municipality
              MAX(d.p6a11_noa_alcaldia) as p6a11_noa_alcaldia, -- Population aged 6-11 not attending school in municipality
              MAX(d.p12a14noa_alcaldia) as p12a14noa_alcaldia, -- Population aged 12-14 not attending school in municipality
              MAX(d.p15a17a_alcaldia) as p15a17a_alcaldia,   -- Population aged 15-17 attending school in municipality
              MAX(d.p18a24a_alcaldia) as p18a24a_alcaldia,   -- Population aged 18-24 attending school in municipality
              
              -- ===== MUNICIPALITY LEVEL AGE PYRAMID DATA =====
              -- (Moved to main age pyramid section above for proportional scaling)
              
              -- ===== MUNICIPALITY LEVEL HISTORICAL DATA =====
              MAX(d.pob_2000_municipal) as pob_2000_municipal, -- Population in 2000 (municipality level)
              MAX(d.pob_2005_municipal) as pob_2005_municipal, -- Population in 2005 (municipality level)
              MAX(d.pob_2010_municipal) as pob_2010_municipal, -- Population in 2010 (municipality level)
              MAX(d.pob_2020_municipal) as pob_2020_municipal, -- Population in 2020 (municipality level)
              MAX(d.prom_ocup_alcaldia) as prom_ocup_alcaldia, -- Average household occupancy in municipality
              MAX(d.pro_ocup_c_alcaldia) as pro_ocup_c_alcaldia, -- Average number of rooms per household in municipality
              
              -- ===== MUNICIPALITY LEVEL SOCIOECONOMIC DATA =====
              MAX(d.pct_ses_ab_alcaldia) as ses_ab_alcaldia,     -- Percentage of households in SES AB in municipality
              MAX(d.pct_ses_c_plus_alcaldia) as ses_c_plus_alcaldia, -- Percentage of households in SES C+ in municipality
              MAX(d.pct_ses_c_alcaldia) as ses_c_alcaldia,       -- Percentage of households in SES C in municipality
              MAX(d.pct_ses_c_minus_alcaldia) as ses_c_minus_alcaldia, -- Percentage of households in SES C- in municipality
              MAX(d.pct_ses_d_plus_alcaldia) as ses_d_plus_alcaldia, -- Percentage of households in SES D+ in municipality
              MAX(d.pct_ses_d_alcaldia) as ses_d_alcaldia,       -- Percentage of households in SES D in municipality
              MAX(d.pct_ses_e_alcaldia) as ses_e_alcaldia,       -- Percentage of households in SES E in municipality
              MAX(d.cambio_porcentual_2005_municipal) as cambio_porcentual_2005_municipal, -- Population growth rate 2000-2005 (municipality)
              MAX(d.cambio_porcentual_2010_municipal) as cambio_porcentual_2010_municipal, -- Population growth rate 2005-2010 (municipality)
              MAX(d.cambio_porcentual_2020_municipal) as cambio_porcentual_2020_municipal, -- Population growth rate 2010-2020 (municipality)
              
              -- ===== COLONIA (NEIGHBORHOOD) LEVEL DATA =====
              MAX(d.pobtot_colonia) as pobtot_colonia,           -- Total population in colonia
              MAX(d.pobmas_colonia) as pobmas_colonia,           -- Total male population in colonia
              MAX(d.pobfem_colonia) as pobfem_colonia,           -- Total female population in colonia
              MAX(d.vivtot_colonia) as vivtot_colonia,           -- Total households in colonia
              MAX(d.pea_colonia) as pea_colonia,                 -- Total economically active population in colonia
              MAX(d.pea_m_colonia) as pea_m_colonia,             -- Total economically active male population in colonia
              MAX(d.pea_f_colonia) as pea_f_colonia,             -- Total economically active female population in colonia
              MAX(d.pe_inac_colonia) as pe_inac_colonia,         -- Total economically inactive population in colonia
              MAX(d.pe_inac_m_colonia) as pe_inac_m_colonia,     -- Total economically inactive male population in colonia
              MAX(d.pe_inac_f_colonia) as pe_inac_f_colonia,     -- Total economically inactive female population in colonia
              MAX(d.pocupada_colonia) as pocupada_colonia,       -- Total employed population in colonia
              MAX(d.pocupada_m_colonia) as pocupada_m_colonia,   -- Total employed male population in colonia
              MAX(d.pocupada_f_colonia) as pocupada_f_colonia,   -- Total employed female population in colonia
              MAX(d.pdesocup_colonia) as pdesocup_colonia,       -- Total unemployed population in colonia
              MAX(d.pdesocup_m_colonia) as pdesocup_m_colonia,   -- Total unemployed male population in colonia
              MAX(d.pdesocup_f_colonia) as pdesocup_f_colonia,   -- Total unemployed female population in colonia
              
              -- ===== COLONIA LEVEL EDUCATION DATA =====
              MAX(d.p_3a5_colonia) as p_3a5_colonia,             -- Population aged 3-5 in colonia
              MAX(d.p_6a11_colonia) as p_6a11_colonia,           -- Population aged 6-11 in colonia
              MAX(d.p_12a14_colonia) as p_12a14_colonia,         -- Population aged 12-14 in colonia
              MAX(d.p_15a17_colonia) as p_15a17_colonia,         -- Population aged 15-17 in colonia
              MAX(d.p_18a24_colonia) as p_18a24_colonia,         -- Population aged 18-24 in colonia
              MAX(d.p3a5_noa_colonia) as p3a5_noa_colonia,       -- Population aged 3-5 not attending school in colonia
              MAX(d.p6a11_noa_colonia) as p6a11_noa_colonia,     -- Population aged 6-11 not attending school in colonia
              MAX(d.p12a14noa_colonia) as p12a14noa_colonia,     -- Population aged 12-14 not attending school in colonia
              MAX(d.p15a17a_colonia) as p15a17a_colonia,         -- Population aged 15-17 attending school in colonia
              MAX(d.p18a24a_colonia) as p18a24a_colonia,         -- Population aged 18-24 attending school in colonia
              
              -- ===== COLONIA LEVEL AGE PYRAMID DATA =====
              MAX(d.p_0a2_colonia) as p_0a2_colonia,             -- Population aged 0-2 in colonia
              MAX(d.p_0a2_m_colonia) as p_0a2_m_colonia,         -- Male population aged 0-2 in colonia
              MAX(d.p_0a2_f_colonia) as p_0a2_f_colonia,         -- Female population aged 0-2 in colonia
              MAX(d.p_3a5_m_colonia) as p_3a5_m_colonia,         -- Male population aged 3-5 in colonia
              MAX(d.p_3a5_f_colonia) as p_3a5_f_colonia,         -- Female population aged 3-5 in colonia
              MAX(d.p_6a11_m_colonia) as p_6a11_m_colonia,       -- Male population aged 6-11 in colonia
              MAX(d.p_6a11_f_colonia) as p_6a11_f_colonia,       -- Female population aged 6-11 in colonia
              MAX(d.p_12a14_m_colonia) as p_12a14_m_colonia,     -- Male population aged 12-14 in colonia
              MAX(d.p_12a14_f_colonia) as p_12a14_f_colonia,     -- Female population aged 12-14 in colonia
              MAX(d.p_15a17_m_colonia) as p_15a17_m_colonia,     -- Male population aged 15-17 in colonia
              MAX(d.p_15a17_f_colonia) as p_15a17_f_colonia,     -- Female population aged 15-17 in colonia
              MAX(d.p_18a24_m_colonia) as p_18a24_m_colonia,     -- Male population aged 18-24 in colonia
              MAX(d.p_18a24_f_colonia) as p_18a24_f_colonia,     -- Female population aged 18-24 in colonia
              MAX(d.p_60ymas_colonia) as p_60ymas_colonia,       -- Population aged 60+ in colonia
              MAX(d.p_60ymas_m_colonia) as p_60ymas_m_colonia,   -- Male population aged 60+ in colonia
              MAX(d.p_60ymas_f_colonia) as p_60ymas_f_colonia,   -- Female population aged 60+ in colonia
              MAX(d.prom_ocup_colonia) as prom_ocup_colonia,     -- Average household occupancy in colonia
              MAX(d.pro_ocup_c_colonia) as pro_ocup_c_colonia,   -- Average number of rooms per household in colonia
              
              -- ===== COLONIA LEVEL SOCIOECONOMIC DATA =====
              MAX(d.pct_ses_ab_colonia) as ses_ab_colonia,       -- Percentage of households in SES AB in colonia
              MAX(d.pct_ses_c_plus_colonia) as ses_c_plus_colonia, -- Percentage of households in SES C+ in colonia
              MAX(d.pct_ses_c_colonia) as ses_c_colonia,         -- Percentage of households in SES C in colonia
              MAX(d.pct_ses_c_minus_colonia) as ses_c_minus_colonia, -- Percentage of households in SES C- in colonia
              MAX(d.pct_ses_d_plus_colonia) as ses_d_plus_colonia, -- Percentage of households in SES D+ in colonia
              MAX(d.pct_ses_d_colonia) as ses_d_colonia,         -- Percentage of households in SES D in colonia
              MAX(d.pct_ses_e_colonia) as ses_e_colonia,         -- Percentage of households in SES E in colonia
              
              -- ===== STATE LEVEL HISTORICAL DATA =====
              MAX(d.pob_2000_entidad) as pob_2000_entidad,       -- Population in 2000 (state level)
              MAX(d.pob_2005_entidad) as pob_2005_entidad,       -- Population in 2005 (state level)
              MAX(d.pob_2010_entidad) as pob_2010_entidad,       -- Population in 2010 (state level)
              MAX(d.pob_2020_entidad) as pob_2020_entidad,       -- Population in 2020 (state level)
              MAX(d.cambio_porcentual_2005_entidad) as cambio_porcentual_2005_entidad, -- Population growth rate 2000-2005 (state)
              MAX(d.cambio_porcentual_2010_entidad) as cambio_porcentual_2010_entidad, -- Population growth rate 2005-2010 (state)
              MAX(d.cambio_porcentual_2020_entidad) as cambio_porcentual_2020_entidad  -- Population growth rate 2010-2020 (state)
            
          FROM data_product.v_qro d
          WHERE d.centroid IS NOT NULL
          AND ST_Intersects(
              ST_SetSRID(
                  ST_MakePoint(
                      CAST(JSON_EXTRACT_PATH_TEXT(d.centroid, 'coordinates', '0') AS FLOAT),
                      CAST(JSON_EXTRACT_PATH_TEXT(d.centroid, 'coordinates', '1') AS FLOAT)
                  ), 
                  4326
              ),
              ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), 3857), {radius}), 4326)
          )
        )
        SELECT 
            *,
            -- ===== CALCULATED METRICS =====
            ROUND(CAST(({radius} / 1000.0) * ({radius} / 1000.0) * 3.14159 AS DECIMAL(10,2)), 2) as area_km2,  -- Area in km² using provided radius
            CASE 
                WHEN pobtot > 0 AND ({radius} / 1000.0) * ({radius} / 1000.0) * 3.14159 > 0 
                THEN ROUND(CAST(pobtot AS DECIMAL(15,2)) / CAST(({radius} / 1000.0) * ({radius} / 1000.0) * 3.14159 AS DECIMAL(10,2)), 2) 
                ELSE 0 
            END as population_density,  -- Population density (persons/km²)
            CASE 
                WHEN pobtot > 0 THEN ROUND(CAST(pobmas AS DECIMAL(15,2)) / CAST(pobtot AS DECIMAL(15,2)) * 100, 1) 
                ELSE NULL 
            END as male_percentage,  -- Male percentage
            CASE 
                WHEN pobtot > 0 THEN ROUND(CAST(pobfem AS DECIMAL(15,2)) / CAST(pobtot AS DECIMAL(15,2)) * 100, 1) 
                ELSE NULL 
            END as female_percentage,  -- Female percentage
            CASE 
                WHEN vivtot > 0 THEN ROUND(CAST(pobtot AS DECIMAL(15,2)) / CAST(vivtot AS DECIMAL(15,2)), 2) 
                ELSE 0 
            END as average_household_size,  -- Average household size
            CASE 
                WHEN pobtot_alcaldia > 0 THEN ROUND(CAST(pobmas_alcaldia AS DECIMAL(15,2)) / CAST(pobtot_alcaldia AS DECIMAL(15,2)) * 100, 1) 
                ELSE NULL 
            END as municipality_male_percentage,  -- Municipality male percentage
            CASE 
                WHEN pobtot_alcaldia > 0 THEN ROUND(CAST(pobfem_alcaldia AS DECIMAL(15,2)) / CAST(pobtot_alcaldia AS DECIMAL(15,2)) * 100, 1) 
                ELSE NULL 
            END as municipality_female_percentage,  -- Municipality female percentage
            
            -- ===== INPUT PARAMETERS FOR REFERENCE =====
            CAST({lat} AS DECIMAL(10,7)) as center_lat,     -- Input latitude
            CAST({lng} AS DECIMAL(10,7)) as center_lng,     -- Input longitude
            {radius} as radius_meters -- Input radius in meters
        FROM demographic_data
        """
        return query

    def get_active_search_query(self, user_id):
        """Get active search query for a specific user."""
        query = f"""
            SELECT * FROM active_search WHERE user_id = {user_id}
        """
        return query

    def _get_mobility_query(self, lat, lng, radius):
        """Get mobility data query within specified radius from coordinates."""
        query = f"""
            WITH point_geom AS (
                SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
            ),
            point_projected AS (
                SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
            ),
            buffered AS (
                -- radius in meters
                SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
            ),
            h3_values AS (
                SELECT H3_Polyfill(ST_Transform(geom, 4326), 10) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT 
                    o::VARCHAR AS h3_value,
                    LENGTH(o::VARCHAR) AS varchar_length,
                    o::BIGINT AS bigint_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT * 
            FROM blackprint_db_prd.presentation.dataset_mobility_data_h3_qro a
            INNER JOIN h3_index b ON a.h3_index = b.h3_value
        """
        return query

    def _get_pois_query(self, lat, lng, radius):
        """Get POIs data query within specified radius from coordinates."""
        query = f"""
            WITH point_geom AS (
                SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
                ),
                point_projected AS (
                SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
                ),
                buffered AS (
                -- radius n meters
                SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
                ),
                h3_values AS (
                SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
                ),
                h3_index AS (
                    SELECT o AS h3_value
                    FROM h3_values i, i.h3_indexes o
                )
                SELECT case when chain_id != 'None' then chain_id else null end as brand, name as names_pri, geometry_wkt, main_category ,
                         sub_category , sub_sub_category, business_category, open_closed_status, popularity_score, average_stars, number_of_reviews, sentiment_score
                FROM blackprint_db_prd.presentation.dim_pois_qro a
                INNER JOIN h3_index b ON a.h3_value = b.h3_value
        """
        return query

    def _get_socioeconomic_query(self, lat, lng, radius):
        """Get socioeconomic data query within specified radius from coordinates."""
        query = f"""
            SELECT
            id_ses_ageb AS "id_ses_ageb",
            state_code AS "state_code",
            municipality_code AS "municipality_code",
            locality_code AS "locality_code",
            ageb_code AS "ageb_code",
            ses_ab AS "ses_ab",
            ses_c_plus AS "ses_c_plus",
            ses_c AS "ses_c",
            ses_c_minus AS "ses_c_minus",
            ses_d_plus AS "ses_d_plus",
            ses_d AS "ses_d",
            ses_e AS "ses_e",
            predominant_level AS "predominant_level",
            total_houses AS "total_houses",
            locality_size AS "locality_size"
            FROM
            blackprint_db_prd.presentation.dim_ses_ageb_qro
            INNER JOIN blackprint_db_prd.data_product.v_qro ON blackprint_db_prd.presentation.dim_ses_ageb_qro.id_ses_ageb = blackprint_db_prd.data_product.v_qro.id_ses_ageb
            WHERE 
            ST_Distance(
                ST_GeomFromText(
                    'POINT(' || 
                    CAST(JSON_EXTRACT(centroid, '$.coordinates[0]') AS VARCHAR) || ' ' || 
                    CAST(JSON_EXTRACT(centroid, '$.coordinates[1]') AS VARCHAR) || 
                    ')', 4326
                ),
                ST_GeomFromText('POINT({lng} {lat})', 4326)
            ) <= {radius}
        """
        return query

    def build_weekly_traffic_by_municipality_query(self, lat, lng, radius):
        """Build SQL query for weekly traffic distribution by municipality."""
        query = f"""
        WITH point_geom AS (
            SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
            SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
            SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
            SELECT H3_Polyfill(ST_Transform(geom, 4326), 10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        ),
        municipality_mapping AS (
            SELECT DISTINCT 
                h.h3_value,
                v.cve_mun,
                v.nom_mun
            FROM h3_index h
            INNER JOIN blackprint_db_prd.data_product.v_qro v 
                ON ST_Intersects(
                    ST_SetSRID(ST_MakePoint(
                        CAST(JSON_EXTRACT_PATH_TEXT(v.centroid, 'coordinates', '0') AS FLOAT),
                        CAST(JSON_EXTRACT_PATH_TEXT(v.centroid, 'coordinates', '1') AS FLOAT)
                    ), 4326),
                    ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), 3857), {radius}), 4326)
                )
        )
        SELECT 
            m.cve_mun,
            m.nom_mun,
            t.dia_de_la_semana,
            t.tipo_usuario,
            SUM(t.total_usuarios_unicos) as total_users
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_dia_qro t
        INNER JOIN municipality_mapping m ON t.h3_index::VARCHAR = m.h3_value::VARCHAR
        GROUP BY m.cve_mun, m.nom_mun, t.dia_de_la_semana, t.tipo_usuario
        ORDER BY m.cve_mun, t.dia_de_la_semana, t.tipo_usuario
        """
        return query
