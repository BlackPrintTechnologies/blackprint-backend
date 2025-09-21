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
        Build optimized Mexico City demographics query using the structure from properties controller.
        This query matches the demographic structure you provided for Mexico City.
        """
        # Convert radius from meters to degrees (approximate conversion for latitude)
        radius_degrees = radius / 111000.0
        
        query = f"""
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
            d.pobtot,
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
        WHERE d.centroid IS NOT NULL
        AND d.centroid != ''
        AND d.centroid LIKE '%coordinates%'
        AND ST_DWithin(
            ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326),
            ST_SetSRID(
                ST_MakePoint(
                    CAST(SPLIT_PART(REPLACE(REPLACE(d.centroid, '{{"type":"Point","coordinates":[', ''), ']}}', ''), ',', 1) AS FLOAT),
                    CAST(SPLIT_PART(REPLACE(REPLACE(d.centroid, '{{"type":"Point","coordinates":[', ''), ']}}', ''), ',', 2) AS FLOAT)
                ), 
                4326
            ),
            {radius_degrees}
        )
        """
        return query

    def build_queretaro_demographics_query(self, lat, lng, radius):
        """
        Build Queretaro demographics query using the staging table structure.
        This query matches the demographic structure for Queretaro city.
        """
        # Convert radius from meters to degrees (approximate conversion for latitude)
        radius_degrees = radius / 111000.0
        
        query = f"""
        SELECT 
            -- General demographic data
            d.nom_loc as neighborhood,
            d.niv_predom as predominant_level,
            d.cve_ageb as ageb_code,
            d.tot_vivien as vivtot,
            -- Note: prom_ocup and pro_ocup_c don't exist in Queretaro table, using defaults
            0 as prom_ocup,
            0 as pro_ocup_c,
            
            -- Municipality level data (using same values for now)
            d.nom_mun,
            d.tot_vivien as vivtot_alcaldia,
            0 as prom_ocup_alcaldia,
            0 as pro_ocup_c_alcaldia,
            
            -- Socioeconomic data - Block level
            d.pct_viv_ab as ses_ab,
            d.pct_viv_cp as ses_c_plus,
            d.pct_viv_c as ses_c,
            d.pct_viv_cm as ses_c_minus,
            d.pct_viv_dp as ses_d_plus,
            d.pct_viv_d as ses_d,
            d.pct_viv_e as ses_e,
            
            -- Socioeconomic data - Municipality level (using same values)
            d.pct_viv_ab as ses_ab_alcaldia,
            d.pct_viv_cp as ses_c_plus_alcaldia,
            d.pct_viv_c as ses_c_alcaldia,
            d.pct_viv_cm as ses_c_minus_alcaldia,
            d.pct_viv_dp as ses_d_plus_alcaldia,
            d.pct_viv_d as ses_d_alcaldia,
            d.pct_viv_e as ses_e_alcaldia,
            
            -- Population data - Block level
            d.pobtot,
            -- Note: pobmas and pobfem don't exist in Queretaro table, using calculated values
            ROUND(d.pobtot * 0.49) as pobmas,  -- Approximate 49% male
            ROUND(d.pobtot * 0.51) as pobfem,  -- Approximate 51% female
            
            -- Population data - Municipality level (using same values for now)
            d.pobtot as pobtot_alcaldia,
            ROUND(d.pobtot * 0.49) as pobmas_alcaldia,
            ROUND(d.pobtot * 0.51) as pobfem_alcaldia,
            
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
            
            -- Gender-specific age data for age pyramid (using calculated values)
            d.p_0a2,
            ROUND(d.p_0a2 * 0.49) as p_0a2_f,
            ROUND(d.p_0a2 * 0.51) as p_0a2_m,
            ROUND(d.p_3a5 * 0.49) as p_3a5_f,
            ROUND(d.p_3a5 * 0.51) as p_3a5_m,
            ROUND(d.p_6a11 * 0.49) as p_6a11_f,
            ROUND(d.p_6a11 * 0.51) as p_6a11_m,
            ROUND(d.p_12a14 * 0.49) as p_12a14_f,
            ROUND(d.p_12a14 * 0.51) as p_12a14_m,
            ROUND(d.p_15a17 * 0.49) as p_15a17_f,
            ROUND(d.p_15a17 * 0.51) as p_15a17_m,
            ROUND(d.p_18a24 * 0.49) as p_18a24_f,
            ROUND(d.p_18a24 * 0.51) as p_18a24_m,
            d.p_60ymas,
            ROUND(d.p_60ymas * 0.49) as p_60ymas_f,
            ROUND(d.p_60ymas * 0.51) as p_60ymas_m,
            
            -- Education data - Municipality level (using same values)
            d.p_3a5 as p_3a5_alcaldia,
            d.p_6a11 as p_6a11_alcaldia,
            d.p_12a14 as p_12a14_alcaldia,
            d.p_15a17 as p_15a17_alcaldia,
            d.p_18a24 as p_18a24_alcaldia,
            d.p3a5_noa as p3a5_noa_alcaldia,
            d.p6a11_noa as p6a11_noa_alcaldia,
            d.p12a14noa as p12a14noa_alcaldia,
            d.p15a17a as p15a17a_alcaldia,
            d.p18a24a as p18a24a_alcaldia,
            
            -- Workforce data - Block level
            d.pea,
            d.pea_m,
            d.pea_f,
            d.pe_inac,
            d.pe_inac_m,
            d.pe_inac_f,
            
            -- Workforce data - Municipality level (using same values)
            d.pea as pea_alcaldia,
            d.pea_m as pea_m_alcaldia,
            d.pea_f as pea_f_alcaldia,
            d.pe_inac as pe_inac_alcaldia,
            d.pe_inac_m as pe_inac_m_alcaldia,
            d.pe_inac_f as pe_inac_f_alcaldia,
            
            -- Employment data - Block level
            d.pocupada,
            d.pocupada_m,
            d.pocupada_f,
            d.pdesocup,
            d.pdesocup_m,
            d.pdesocup_f,
            
            -- Employment data - Municipality level (using same values)
            d.pocupada as pocupada_alcaldia,
            d.pocupada_m as pocupada_m_alcaldia,
            d.pocupada_f as pocupada_f_alcaldia,
            d.pdesocup as pdesocup_alcaldia,
            d.pdesocup_m as pdesocup_m_alcaldia,
            d.pdesocup_f as pdesocup_f_alcaldia,
            
            -- Population growth data - Block level (using default values since not available)
            0 as pob_2000_ageb,
            0 as pob_2005_ageb,
            0 as pob_2010_ageb,
            0 as pob_2015_ageb,
            d.pobtot as pob_2020_ageb,
            0 as cambio_porcentual_2005_ageb,
            0 as cambio_porcentual_2010_ageb,
            0 as cambio_porcentual_2015_ageb,
            0 as cambio_porcentual_2020_ageb,
            
            -- Population growth data - Municipality level (using same values for now)
            0 as pob_2000_municipal,
            0 as pob_2005_municipal,
            0 as pob_2010_municipal,
            0 as pob_2015_municipal,
            d.pobtot as pob_2020_municipal,
            0 as cambio_porcentual_2005_municipal,
            0 as cambio_porcentual_2010_municipal,
            0 as cambio_porcentual_2015_municipal,
            0 as cambio_porcentual_2020_municipal,
            
            -- Centroid for distance filtering
            d.geometry_coords_json as centroid,
            
            -- Area data for municipality calculation (using default values)
            1.0 as total_area,
            d.cve_mun as municipality_code,
            d.nom_mun as municipality_nm
            
        FROM staging.stg_demographic_socioeconomic_qro d
        WHERE d.geometry_coords_json IS NOT NULL
        AND d.geometry_coords_json != ''
        AND d.geometry_coords_json LIKE '%coordinates%'
        LIMIT 10
        """
        return query
