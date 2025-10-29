import logging

logger = logging.getLogger(__name__)


class AreaAnalysisQuery:
    """Query builder class for area analysis operations."""

    def __init__(self):
        pass

    def build_traffic_by_day_query(self, lat, lng, radius, user_type=None, config_city='queretaro'):
        """Build SQL query for traffic data by day of the week."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"

        if config_city == 'mexico':
            # Mexico City - aggregate hourly data by day of week using year/month/day columns
            # Map user_type to appropriate column
            if user_type == 'peaton':
                column_name = 'a.pedestrian'
            elif user_type == 'vehiculo':
                column_name = 'a.motor_vehicle'
            elif user_type == 'estacionario':
                column_name = 'a.at_rest'
            else:
                # Default to total for all types
                column_name = 'a.total'

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
              SELECT H3_Polyfill(ST_Transform(geom, 4326), 11) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT o AS h3_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT  SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 1 THEN {column_name} ELSE 0 END) AS monday,
                    SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 2 THEN {column_name} ELSE 0 END) AS tuesday,
                    SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 3 THEN {column_name} ELSE 0 END) AS wednesday,
                    SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 4 THEN {column_name} ELSE 0 END) AS thursday,
                    SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 5 THEN {column_name} ELSE 0 END) AS friday,
                    SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 6 THEN {column_name} ELSE 0 END) AS saturday,
                    SUM(CASE WHEN EXTRACT(DOW FROM DATE(a.year || '-' || LPAD(a.month::text, 2, '0') || '-' || LPAD(a.day::text, 2, '0'))) = 0 THEN {column_name} ELSE 0 END) AS sunday
            FROM blackprint_db_prd.presentation.dataset_mobility_data_v2 a
            INNER JOIN h3_index b ON a.h3_index = b.h3_value
            """
        else:
            # Queretaro uses stg_data_movilidad_por_dia_qro with dia_de_la_semana column
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

    def build_traffic_by_hour_query(self, lat, lng, radius, user_type=None, config_city='queretaro'):
        """Build SQL query for traffic data by hour of the day."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"

        if config_city == 'mexico':
            # Mexico City uses dataset_mobility_data_v2 with pedestrian/motor_vehicle/at_rest/total columns
            # Map user_type to appropriate column
            if user_type == 'peaton':
                column_name = 'a.pedestrian'
            elif user_type == 'vehiculo':
                column_name = 'a.motor_vehicle'
            elif user_type == 'estacionario':
                column_name = 'a.at_rest'
            else:
                # Default to total for all types
                column_name = 'a.total'

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
              SELECT H3_Polyfill(ST_Transform(geom, 4326), 11) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT o AS h3_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT  SUM(CASE WHEN a.hour = 0 THEN {column_name} ELSE 0 END) AS hour_0,
                    SUM(CASE WHEN a.hour = 1 THEN {column_name} ELSE 0 END) AS hour_1,
                    SUM(CASE WHEN a.hour = 2 THEN {column_name} ELSE 0 END) AS hour_2,
                    SUM(CASE WHEN a.hour = 3 THEN {column_name} ELSE 0 END) AS hour_3,
                    SUM(CASE WHEN a.hour = 4 THEN {column_name} ELSE 0 END) AS hour_4,
                    SUM(CASE WHEN a.hour = 5 THEN {column_name} ELSE 0 END) AS hour_5,
                    SUM(CASE WHEN a.hour = 6 THEN {column_name} ELSE 0 END) AS hour_6,
                    SUM(CASE WHEN a.hour = 7 THEN {column_name} ELSE 0 END) AS hour_7,
                    SUM(CASE WHEN a.hour = 8 THEN {column_name} ELSE 0 END) AS hour_8,
                    SUM(CASE WHEN a.hour = 9 THEN {column_name} ELSE 0 END) AS hour_9,
                    SUM(CASE WHEN a.hour = 10 THEN {column_name} ELSE 0 END) AS hour_10,
                    SUM(CASE WHEN a.hour = 11 THEN {column_name} ELSE 0 END) AS hour_11,
                    SUM(CASE WHEN a.hour = 12 THEN {column_name} ELSE 0 END) AS hour_12,
                    SUM(CASE WHEN a.hour = 13 THEN {column_name} ELSE 0 END) AS hour_13,
                    SUM(CASE WHEN a.hour = 14 THEN {column_name} ELSE 0 END) AS hour_14,
                    SUM(CASE WHEN a.hour = 15 THEN {column_name} ELSE 0 END) AS hour_15,
                    SUM(CASE WHEN a.hour = 16 THEN {column_name} ELSE 0 END) AS hour_16,
                    SUM(CASE WHEN a.hour = 17 THEN {column_name} ELSE 0 END) AS hour_17,
                    SUM(CASE WHEN a.hour = 18 THEN {column_name} ELSE 0 END) AS hour_18,
                    SUM(CASE WHEN a.hour = 19 THEN {column_name} ELSE 0 END) AS hour_19,
                    SUM(CASE WHEN a.hour = 20 THEN {column_name} ELSE 0 END) AS hour_20,
                    SUM(CASE WHEN a.hour = 21 THEN {column_name} ELSE 0 END) AS hour_21,
                    SUM(CASE WHEN a.hour = 22 THEN {column_name} ELSE 0 END) AS hour_22,
                    SUM(CASE WHEN a.hour = 23 THEN {column_name} ELSE 0 END) AS hour_23
            FROM blackprint_db_prd.presentation.dataset_mobility_data_v2 a
            INNER JOIN h3_index b ON a.h3_index = b.h3_value
            """
        else:
            # Queretaro uses stg_data_movilidad_por_hora_qro with total_usuarios_unicos column
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

    def build_traffic_summary_query(self, lat, lng, radius, user_type=None, config_city='queretaro'):
        """Build SQL query for total traffic summary."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"

        if config_city == 'mexico':
            # Mexico City uses dataset_mobility_data_v2 with pedestrian/motor_vehicle/at_rest/total columns
            # Map user_type to appropriate column
            if user_type == 'peaton':
                column_name = 'a.pedestrian'
            elif user_type == 'vehiculo':
                column_name = 'a.motor_vehicle'
            elif user_type == 'estacionario':
                column_name = 'a.at_rest'
            else:
                # Default to total for all types
                column_name = 'a.total'

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
              SELECT H3_Polyfill(ST_Transform(geom, 4326), 11) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT o AS h3_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT  SUM({column_name}) as total_users
            FROM blackprint_db_prd.presentation.dataset_mobility_data_v2 a
            INNER JOIN h3_index b ON a.h3_index = b.h3_value
            """
        else:
            # Queretaro uses stg_data_movilidad_por_hora_qro with total_usuarios_unicos column
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
            )
            SELECT  SUM(a.total_usuarios_unicos) as total_users
            FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
            INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
            {user_type_condition}
            """
        return query

    def build_h3_traffic_summary_query(self, lat, lng, radius, config_city='queretaro'):
        """Build SQL query for H3 traffic summary with aggregated data."""
        if config_city == 'mexico':
            # Mexico City uses dataset_mobility_data_v2 with pedestrian/motor_vehicle/at_rest/total columns
            # Use total column for H3 summary (aggregated across all mobility types)
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
              SELECT H3_Polyfill(ST_Transform(geom, 4326), 11) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT o AS h3_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT 
                COUNT(DISTINCT a.h3_index) as unique_h3_count,
                SUM(a.total) as total_unique_users,
                ROUND(CAST(SUM(a.total) AS DECIMAL) / CAST(COUNT(DISTINCT a.h3_index) AS DECIMAL), 2) as avg_users_per_h3
            FROM blackprint_db_prd.presentation.dataset_mobility_data_v2 a
            INNER JOIN h3_index b ON a.h3_index = b.h3_value
            """
        else:
            # Queretaro uses stg_data_movilidad_por_dia_qro with total_usuarios_unicos column
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
            )
            SELECT 
                COUNT(DISTINCT a.h3_index) as unique_h3_count,
                SUM(a.total_usuarios_unicos) as total_unique_users,
                ROUND(CAST(SUM(a.total_usuarios_unicos) AS DECIMAL) / CAST(COUNT(DISTINCT a.h3_index) AS DECIMAL), 2) as avg_users_per_h3
            FROM blackprint_db_prd.staging.stg_data_movilidad_por_dia_qro a
            INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
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
            # For Mexico City (CDMX) - use v_parcel_v3 table with correct column names
            query = f"""
            SELECT 
                SUM(d.pobtot) as total_population,
                MAX(d.pobtot_alcaldia) as municipality_population,
                MAX(d.municipality_code) as municipality_code,
                MAX(d.municipality_nm) as municipality_name
            FROM blackprint_db_prd.data_product.v_parcel_v3 d
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
                d.municipality_nm as municipality
            FROM blackprint_db_prd.data_product.v_parcel_v3 d
            WHERE d.centroid IS NOT NULL
            AND d.centroid != ''
            AND d.centroid LIKE '%coordinates%'
            LIMIT 1000
            """
        return query

    def build_mexico_demographics_query(self, lat, lng, radius):
        """
        Build optimized Mexico City demographics query with all aggregations included.
        This query performs all the aggregations at the database level, providing better 
        control and performance by doing calculations in SQL rather than in the controller.
        Matches the structure of the Queretaro demographics query.
        """
        query = f"""
        WITH demographic_data AS (
        SELECT 
              -- ===== AGE PYRAMID DATA (Block-level totals) =====
              SUM(d.p_0a2) as p_0a2,                -- Population aged 0-2 years (block level)
              SUM(d.p_0a2_f) as p_0a2_f,            -- Female population aged 0-2 years
              SUM(d.p_0a2_m) as p_0a2_m,            -- Male population aged 0-2 years
              SUM(d.p_3a5) as p_3a5,                -- Population aged 3-5 years (block level)
              SUM(d.p_3a5_f) as p_3a5_f,            -- Female population aged 3-5 years
              SUM(d.p_3a5_m) as p_3a5_m,            -- Male population aged 3-5 years
              SUM(d.p_6a11) as p_6a11,              -- Population aged 6-11 years (block level)
              SUM(d.p_6a11_f) as p_6a11_f,          -- Female population aged 6-11 years
              SUM(d.p_6a11_m) as p_6a11_m,          -- Male population aged 6-11 years
              SUM(d.p_12a14) as p_12a14,            -- Population aged 12-14 years (block level)
              SUM(d.p_12a14_f) as p_12a14_f,        -- Female population aged 12-14 years
              SUM(d.p_12a14_m) as p_12a14_m,        -- Male population aged 12-14 years
              SUM(d.p_15a17) as p_15a17,            -- Population aged 15-17 years (block level)
              SUM(d.p_15a17_f) as p_15a17_f,        -- Female population aged 15-17 years
              SUM(d.p_15a17_m) as p_15a17_m,        -- Male population aged 15-17 years
              SUM(d.p_18a24) as p_18a24,            -- Population aged 18-24 years (block level)
              SUM(d.p_18a24_f) as p_18a24_f,        -- Female population aged 18-24 years
              SUM(d.p_18a24_m) as p_18a24_m,        -- Male population aged 18-24 years
              SUM(d.p_60ymas) as p_60ymas,          -- Population aged 60+ years (block level)
              SUM(d.p_60ymas_f) as p_60ymas_f,      -- Female population aged 60+ years
              SUM(d.p_60ymas_m) as p_60ymas_m,      -- Male population aged 60+ years
              
              -- Municipality-level gender ratios for proportional scaling (if block-level gender not available)
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
              SUM(d.pob_2015_ageb) as pob_2015_ageb, -- Population in 2015 (block level)
              SUM(d.pob_2020_ageb) as pob_2020_ageb, -- Population in 2020 (block level)
              
              -- ===== CURRENT POPULATION AND HOUSEHOLDS (Direct sum from block level) =====
              SUM(d.vivtot) as vivtot,                    -- Total households in selected area
              SUM(COALESCE(d.pobmas, 0) + COALESCE(d.pobfem, 0)) as pobtot,   -- Total population
              SUM(COALESCE(d.pobmas, 0)) as pobmas,       -- Total male population
              SUM(COALESCE(d.pobfem, 0)) as pobfem,       -- Total female population
              
              -- ===== SOCIOECONOMIC LEVELS (AVERAGE across all records) =====
              AVG(d.ses_ab) as ses_ab,              -- Percentage of households in socioeconomic level AB (highest)
              AVG(d.ses_c_plus) as ses_c_plus,      -- Percentage of households in socioeconomic level C+
              AVG(d.ses_c) as ses_c,                -- Percentage of households in socioeconomic level C
              AVG(d.ses_c_minus) as ses_c_minus,    -- Percentage of households in socioeconomic level C-
              AVG(d.ses_d_plus) as ses_d_plus,      -- Percentage of households in socioeconomic level D+
              AVG(d.ses_d) as ses_d,                -- Percentage of households in socioeconomic level D
              AVG(d.ses_e) as ses_e,                -- Percentage of households in socioeconomic level E (lowest)
              
              -- ===== POPULATION GROWTH RATES (AVERAGE across all records) =====
              AVG(d.cambio_porcentual_2005_ageb) as cambio_porcentual_2005_ageb, -- Population growth rate 2000-2005
              AVG(d.cambio_porcentual_2010_ageb) as cambio_porcentual_2010_ageb, -- Population growth rate 2005-2010
              AVG(d.cambio_porcentual_2015_ageb) as cambio_porcentual_2015_ageb, -- Population growth rate 2010-2015
              AVG(d.cambio_porcentual_2020_ageb) as cambio_porcentual_2020_ageb, -- Population growth rate 2015-2020
              
              -- ===== HOUSEHOLD OCCUPANCY (AVERAGE across all records) =====
              AVG(d.prom_ocup) as prom_ocup,        -- Average household occupancy
              AVG(d.pro_ocup_c) as pro_ocup_c,      -- Average number of rooms per household
              
              -- ===== LOCATION IDENTIFIERS (from first record) =====
              MAX(d.ageb_code) as ageb_code,         -- Block code (AGEB)
              MAX(d.municipality_nm) as nom_mun,            -- Municipality name
              MAX(d.municipality_code) as municipality_code,  -- Municipality code
              MAX(d.municipality_nm) as municipality_nm,    -- Municipality name (duplicate)
              MAX(d.centroid) as centroid,          -- Geographic centroid coordinates
              
              -- ===== MUNICIPALITY LEVEL DATA (from first record - same for all records in same municipality) =====
              MAX(d.pobtot_alcaldia) as pobtot_alcaldia,     -- Total population in municipality
              MAX(d.pobmas_alcaldia) as pobmas_alcaldia,     -- Total male population in municipality
              MAX(d.pobfem_alcaldia) as pobfem_alcaldia,     -- Total female population in municipality
              MAX(d.vivtot_alcaldia) as vivtot_alcaldia,     -- Total households in municipality
              
              -- ===== MUNICIPALITY LEVEL HISTORICAL DATA =====
              MAX(d.pob_2000_municipal) as pob_2000_municipal, -- Population in 2000 (municipality level)
              MAX(d.pob_2005_municipal) as pob_2005_municipal, -- Population in 2005 (municipality level)
              MAX(d.pob_2010_municipal) as pob_2010_municipal, -- Population in 2010 (municipality level)
              MAX(d.pob_2015_municipal) as pob_2015_municipal, -- Population in 2015 (municipality level)
              MAX(d.pob_2020_municipal) as pob_2020_municipal, -- Population in 2020 (municipality level)
              MAX(d.prom_ocup_alcaldia) as prom_ocup_alcaldia, -- Average household occupancy in municipality
              MAX(d.pro_ocup_c_alcaldia) as pro_ocup_c_alcaldia, -- Average number of rooms per household in municipality
              
              -- ===== MUNICIPALITY LEVEL SOCIOECONOMIC DATA =====
              MAX(d.ses_ab_alcaldia) as ses_ab_alcaldia,     -- Percentage of households in SES AB in municipality
              MAX(d.ses_c_plus_alcaldia) as ses_c_plus_alcaldia, -- Percentage of households in SES C+ in municipality
              MAX(d.ses_c_alcaldia) as ses_c_alcaldia,       -- Percentage of households in SES C in municipality
              MAX(d.ses_c_minus_alcaldia) as ses_c_minus_alcaldia, -- Percentage of households in SES C- in municipality
              MAX(d.ses_d_plus_alcaldia) as ses_d_plus_alcaldia, -- Percentage of households in SES D+ in municipality
              MAX(d.ses_d_alcaldia) as ses_d_alcaldia,       -- Percentage of households in SES D in municipality
              MAX(d.ses_e_alcaldia) as ses_e_alcaldia,       -- Percentage of households in SES E in municipality
              MAX(d.cambio_porcentual_2005_municipal) as cambio_porcentual_2005_municipal, -- Population growth rate 2000-2005 (municipality)
              MAX(d.cambio_porcentual_2010_municipal) as cambio_porcentual_2010_municipal, -- Population growth rate 2005-2010 (municipality)
              MAX(d.cambio_porcentual_2015_municipal) as cambio_porcentual_2015_municipal, -- Population growth rate 2010-2015 (municipality)
              MAX(d.cambio_porcentual_2020_municipal) as cambio_porcentual_2020_municipal, -- Population growth rate 2015-2020 (municipality)
              
              -- ===== COLONIA (NEIGHBORHOOD) LEVEL DATA =====
              SUM(d.vivtot_colonia) as vivtot_colonia,           -- Total households in colonia (summed)
              SUM(d.pobtot_colonia) as pobtot_colonia,           -- Total population in colonia (summed)
              SUM(d.pobmas_colonia) as pobmas_colonia,           -- Total male population in colonia (summed)
              SUM(d.pobfem_colonia) as pobfem_colonia,           -- Total female population in colonia (summed)
              
              -- ===== COLONIA LEVEL SOCIOECONOMIC DATA (AVERAGED) =====
              AVG(d.ses_ab_colonia) as ses_ab_colonia,       -- Percentage of households in SES AB in colonia
              AVG(d.ses_c_plus_colonia) as ses_c_plus_colonia, -- Percentage of households in SES C+ in colonia
              AVG(d.ses_c_colonia) as ses_c_colonia,         -- Percentage of households in SES C in colonia
              AVG(d.ses_c_minus_colonia) as ses_c_minus_colonia, -- Percentage of households in SES C- in colonia
              AVG(d.ses_d_plus_colonia) as ses_d_plus_colonia, -- Percentage of households in SES D+ in colonia
              AVG(d.ses_d_colonia) as ses_d_colonia,         -- Percentage of households in SES D in colonia
              AVG(d.ses_e_colonia) as ses_e_colonia,         -- Percentage of households in SES E in colonia
              AVG(d.prom_ocup_colonia) as prom_ocup_colonia,     -- Average household occupancy in colonia
              AVG(d.pro_ocup_c_colonia) as pro_ocup_c_colonia,   -- Average number of rooms per household in colonia
              
              -- ===== STATE LEVEL HISTORICAL DATA =====
              MAX(d.pob_2000_entidad) as pob_2000_entidad,       -- Population in 2000 (state level)
              MAX(d.pob_2005_entidad) as pob_2005_entidad,       -- Population in 2005 (state level)
              MAX(d.pob_2010_entidad) as pob_2010_entidad,       -- Population in 2010 (state level)
              MAX(d.pob_2015_entidad) as pob_2015_entidad,       -- Population in 2015 (state level)
              MAX(d.pob_2020_entidad) as pob_2020_entidad,       -- Population in 2020 (state level)
              MAX(d.cambio_porcentual_2005_entidad) as cambio_porcentual_2005_entidad, -- Population growth rate 2000-2005 (state)
              MAX(d.cambio_porcentual_2010_entidad) as cambio_porcentual_2010_entidad, -- Population growth rate 2005-2010 (state)
              MAX(d.cambio_porcentual_2015_entidad) as cambio_porcentual_2015_entidad, -- Population growth rate 2010-2015 (state)
              MAX(d.cambio_porcentual_2020_entidad) as cambio_porcentual_2020_entidad  -- Population growth rate 2015-2020 (state)
            
        FROM blackprint_db_prd.data_product.v_parcel_v3 d
        WHERE d.centroid IS NOT NULL
        AND d.centroid != ''
        AND d.centroid LIKE '%coordinates%'
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
            ROUND(PI() * POWER({radius} / 1000.0, 2), 2) as area_km2,  -- Area in km² using provided radius
            CASE 
                WHEN pobtot > 0 AND PI() * POWER({radius} / 1000.0, 2) > 0 
                THEN ROUND(CAST(pobtot AS DECIMAL(15,2)) / CAST(PI() * POWER({radius} / 1000.0, 2) AS DECIMAL(10,2)), 2) 
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

    def build_queretaro_demographics_query(self, lat, lng, radius):
        """
        Build Queretaro demographics query with all aggregations included.
        This query performs all the aggregations that were previously done in the controller,
        providing better control and performance by doing calculations at the database level.
        """
        query = f"""
        WITH demographic_data AS (
        SELECT 
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
              SUM(COALESCE(d.pobtot, 0)) as pobtot,   -- Total population from all intersecting colonias (block level)
              
              -- ===== COLONIA LEVEL GENDER DATA (for ratio calculation) =====
              SUM(COALESCE(d.pobmas_colonia, 0)) as total_pobmas_colonia,   -- Total male population from all intersecting colonias
              SUM(COALESCE(d.pobfem_colonia, 0)) as total_pobfem_colonia,   -- Total female population from all intersecting colonias
              SUM(COALESCE(d.pobtot_colonia, 0)) as total_pobtot_colonia,   -- Total population from all intersecting colonias (colonia level)
              
              -- ===== CALCULATED GENDER DISTRIBUTION (using colonia ratios applied to block-level total) =====
              -- Calculate colonia-level gender ratios
              CASE 
                  WHEN SUM(COALESCE(d.pobtot_colonia, 0)) > 0 THEN 
                      ROUND(CAST(SUM(COALESCE(d.pobmas_colonia, 0)) AS DECIMAL(15,2)) / CAST(SUM(COALESCE(d.pobtot_colonia, 0)) AS DECIMAL(15,2)), 4)
                  ELSE 0 
              END as colonia_male_ratio,  -- Male ratio from colonia data
              
              CASE 
                  WHEN SUM(COALESCE(d.pobtot_colonia, 0)) > 0 THEN 
                      ROUND(CAST(SUM(COALESCE(d.pobfem_colonia, 0)) AS DECIMAL(15,2)) / CAST(SUM(COALESCE(d.pobtot_colonia, 0)) AS DECIMAL(15,2)), 4)
                  ELSE 0 
              END as colonia_female_ratio,  -- Female ratio from colonia data
              
              -- Apply colonia gender ratios to block-level total population for consistency
              CASE 
                  WHEN SUM(COALESCE(d.pobtot, 0)) > 0 AND SUM(COALESCE(d.pobtot_colonia, 0)) > 0 THEN
                      ROUND(CAST(SUM(COALESCE(d.pobtot, 0)) AS DECIMAL(15,2)) * 
                            (CAST(SUM(COALESCE(d.pobmas_colonia, 0)) AS DECIMAL(15,2)) / CAST(SUM(COALESCE(d.pobtot_colonia, 0)) AS DECIMAL(15,2))), 0)
                  ELSE 0 
              END as pobmas,  -- Estimated male population using colonia ratio
              
              CASE 
                  WHEN SUM(COALESCE(d.pobtot, 0)) > 0 AND SUM(COALESCE(d.pobtot_colonia, 0)) > 0 THEN
                      ROUND(CAST(SUM(COALESCE(d.pobtot, 0)) AS DECIMAL(15,2)) * 
                            (CAST(SUM(COALESCE(d.pobfem_colonia, 0)) AS DECIMAL(15,2)) / CAST(SUM(COALESCE(d.pobtot_colonia, 0)) AS DECIMAL(15,2))), 0)
                  ELSE 0 
              END as pobfem,  -- Estimated female population using colonia ratio
              
              -- ===== SOCIOECONOMIC LEVELS (CORRECTED: Weighted average based on household counts) =====
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_ab * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_ab,          -- Percentage of households in socioeconomic level AB (highest)
              
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_cp * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_c_plus,      -- Percentage of households in socioeconomic level C+
              
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_c * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_c,            -- Percentage of households in socioeconomic level C
              
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_cm * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_c_minus,     -- Percentage of households in socioeconomic level C-
              
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_dp * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_d_plus,      -- Percentage of households in socioeconomic level D+
              
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_d * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_d,            -- Percentage of households in socioeconomic level D
              
              CASE 
                  WHEN SUM(d.tot_vivien) > 0 THEN 
                      ROUND(SUM((d.pct_viv_e * d.tot_vivien) / 100.0) / SUM(d.tot_vivien) * 100, 1)
                  ELSE 0 
              END as ses_e,            -- Percentage of households in socioeconomic level E (lowest)
              
              -- ===== POPULATION GROWTH RATES (AVERAGE across all records) =====
              AVG(d.cambio_porcentual_2005_ageb) as cambio_porcentual_2005_ageb, -- Population growth rate 2000-2005
              AVG(d.cambio_porcentual_2010_ageb) as cambio_porcentual_2010_ageb, -- Population growth rate 2005-2010
              AVG(d.cambio_porcentual_2020_ageb) as cambio_porcentual_2020_ageb, -- Population growth rate 2010-2020
              
              -- ===== LOCATION IDENTIFIERS (from first record) =====
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
              
              -- ===== MUNICIPALITY LEVEL AGE PYRAMID DATA =====
              -- (Moved to main age pyramid section above for proportional scaling)
              
              -- ===== MUNICIPALITY LEVEL HISTORICAL DATA =====
              MAX(d.pob_2000_municipal) as pob_2000_municipal, -- Population in 2000 (municipality level)
              MAX(d.pob_2005_municipal) as pob_2005_municipal, -- Population in 2005 (municipality level)
              MAX(d.pob_2010_municipal) as pob_2010_municipal, -- Population in 2010 (municipality level)
              MAX(d.pob_2020_municipal) as pob_2020_municipal, -- Population in 2020 (municipality level)
              MAX(d.prom_ocup_alcaldia) as prom_ocup_alcaldia, -- Average household occupancy in municipality
              MAX(d.pro_ocup_c_alcaldia) as pro_ocup_c_alcaldia, -- Average number of rooms per household in municipality
              
              -- ===== MUNICIPALITY LEVEL SOCIOECONOMIC DATA (CORRECTED: Use AVG instead of MAX) =====
              ROUND(AVG(d.pct_ses_ab_alcaldia), 1) as ses_ab_alcaldia,     -- Percentage of households in SES AB in municipality
              ROUND(AVG(d.pct_ses_c_plus_alcaldia), 1) as ses_c_plus_alcaldia, -- Percentage of households in SES C+ in municipality
              ROUND(AVG(d.pct_ses_c_alcaldia), 1) as ses_c_alcaldia,       -- Percentage of households in SES C in municipality
              ROUND(AVG(d.pct_ses_c_minus_alcaldia), 1) as ses_c_minus_alcaldia, -- Percentage of households in SES C- in municipality
              ROUND(AVG(d.pct_ses_d_plus_alcaldia), 1) as ses_d_plus_alcaldia, -- Percentage of households in SES D+ in municipality
              ROUND(AVG(d.pct_ses_d_alcaldia), 1) as ses_d_alcaldia,       -- Percentage of households in SES D in municipality
              ROUND(AVG(d.pct_ses_e_alcaldia), 1) as ses_e_alcaldia,       -- Percentage of households in SES E in municipality
              MAX(d.cambio_porcentual_2005_municipal) as cambio_porcentual_2005_municipal, -- Population growth rate 2000-2005 (municipality)
              MAX(d.cambio_porcentual_2010_municipal) as cambio_porcentual_2010_municipal, -- Population growth rate 2005-2010 (municipality)
              MAX(d.cambio_porcentual_2020_municipal) as cambio_porcentual_2020_municipal, -- Population growth rate 2010-2020 (municipality)
              
              -- ===== COLONIA (NEIGHBORHOOD) LEVEL DATA =====
              MAX(d.pobtot_colonia) as pobtot_colonia,           -- Total population in colonia
              MAX(d.pobmas_colonia) as pobmas_colonia,           -- Total male population in colonia
              MAX(d.pobfem_colonia) as pobfem_colonia,           -- Total female population in colonia
              MAX(d.vivtot_colonia) as vivtot_colonia,           -- Total households in colonia
              
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
            ROUND(PI() * POWER({radius} / 1000.0, 2), 2) as area_km2,  -- Area in km² using provided radius
            CASE 
                WHEN pobtot > 0 AND PI() * POWER({radius} / 1000.0, 2) > 0 
                THEN ROUND(CAST(pobtot AS DECIMAL(15,2)) / CAST(PI() * POWER({radius} / 1000.0, 2) AS DECIMAL(10,2)), 2) 
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
        # Convert radius from meters to degrees
        radius_degrees = radius / 111320.0
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
            ) <= {radius_degrees}
        """
        return query

    def build_h3_distribution_query(self, lat, lng, radius):
        """Build SQL query for H3 distribution based on all user types traffic data."""
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        buffered AS (
            SELECT geometry
            FROM blackprint_db_prd.integration.int_state_municipality_shapefile
            WHERE ST_Within((SELECT geom FROM point_geom), geometry)
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geometry, 4326), 10) AS h3_indexes
          FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        ),
        traffic_data AS (
            SELECT a.h3_index,
                   SUM(a.total_usuarios_unicos) AS total_users
            FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
            INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
            GROUP BY a.h3_index
        )
        SELECT 
            t.h3_index,
            t.total_users
        FROM traffic_data t
        ORDER BY t.total_users DESC
        """
        return query

    # to get the total population of the selected area for both the city

    def _get_total_population_query(self, lat, lng, radius, city='queretaro'):
        """Get population data within specified radius from coordinates using spatial calculations."""
        if city == 'queretaro':
            query = f"""
                SELECT 
                    SUM(d.pobtot) as total_population
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
        else:  # mexico
            query = f"""
                SELECT 
                    SUM(d.pobtot) as total_population
                FROM blackprint_db_prd.data_product.v_parcel_v3 d
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
        return query

    # query for the poi hierarchy with details
    def get_pois_hierarchy_with_brands_query(self, lat, lng, radius, config_city='queretaro'):
        """Generate query to get POI category hierarchy with brand counts within specified area."""
        if config_city == "queretaro" or config_city == "el_marques":
            places_table = 'blackprint_db_prd.presentation.dim_pois_qro'
        else:
            places_table = 'blackprint_db_prd.presentation.dim_pois_cdmx'

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
            ),
            area_pois AS (
                SELECT 
                    main_category,
                    sub_category,
                    sub_sub_category,
                    chain_id as brand,
                    name as business_name,
                    business_category,
                    COALESCE(address, '') || CASE 
                        WHEN address IS NOT NULL AND address2 IS NOT NULL THEN ', ' 
                        ELSE '' 
                    END || COALESCE(address2, '') as full_address,
                    opened_on,
                    average_stars,
                    COUNT(*) as brand_count
                FROM {places_table} a
                INNER JOIN h3_index b ON a.h3_value = b.h3_value
                WHERE main_category IS NOT NULL 
                AND main_category != ''
                AND chain_id IS NOT NULL
                AND chain_id != 'None'
                GROUP BY main_category, sub_category, sub_sub_category, chain_id, name, business_category, address, address2, opened_on, average_stars
            ),
            unique_brands_per_category AS (
                SELECT
                    main_category,
                    sub_category,
                    sub_sub_category,
                    COUNT(DISTINCT brand) as unique_brands
                FROM area_pois
                GROUP BY main_category, sub_category, sub_sub_category
            ),
            category_stats AS (
                SELECT
                    main_category as category_1,
                    sub_category as category_2,
                    sub_sub_category as category_3,
                    SUM(brand_count) as total_pois,
                    LISTAGG(
                        CASE
                            WHEN brand IS NOT NULL AND brand != ''
                            THEN '{{"name":"' || COALESCE(brand, '') || '","address":"' || COALESCE(full_address, '') || '","business_category":"' || COALESCE(business_category, '') || '","rating":"' || COALESCE(CAST(average_stars AS VARCHAR), '0') || '","date_opened":"' || COALESCE(CAST(opened_on AS VARCHAR), '') || '"}}'
                            ELSE NULL
                        END,
                        ','
                    ) WITHIN GROUP (ORDER BY brand) as brand_list
                FROM area_pois
                GROUP BY main_category, sub_category, sub_sub_category
            )
            SELECT 
                cs.category_1,
                cs.category_2,
                cs.category_3,
                ub.unique_brands,
                cs.total_pois,
                cs.brand_list
            FROM category_stats cs
            LEFT JOIN unique_brands_per_category ub 
                ON cs.category_1 = ub.main_category 
                AND cs.category_2 = ub.sub_category 
                AND cs.category_3 = ub.sub_sub_category
            ORDER BY cs.category_1, cs.category_2, cs.category_3
        """
        return query
    
    def _get_municipality_info_query(self, lat, lng, city='queretaro'):
        """Get municipality information from lat/lng coordinates."""
        print("getting municipality info", lat, lng, city)
        if city == 'queretaro':
            query = f"""
                SELECT 
                    h3_indexes,
                    cve_mun as municipality_code,
                    nom_mun as municipality_name,
                    pobtot_alcaldia as municipality_population
                FROM blackprint_db_prd.data_product.v_qro 
                WHERE ST_Contains(
                    ST_GeomFromGeoJSON(geometry_geojson),
                    ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326)
                )
                LIMIT 1
            """
        else:  # mexico
            query = f"""
                SELECT 
                    h3_indexes,
                    municipality_code,
                    municipality_nm as municipality_name,
                    pobtot as municipality_population
                FROM blackprint_db_prd.data_product.v_parcel_v3 
                WHERE ST_Contains(
                    ST_GeomFromGeoJSON(geometry_geojson),
                    ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326)
                )
                LIMIT 1
            """
        return query
    
    
    def _get_municipality_pois_query_direct(self, lat, lng, city='queretaro'):
        """Get POI data for municipality using direct H3_Polyfill without Python conversion."""
        if city == 'queretaro':
            query = f"""
                WITH point_geom AS (
                    SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
                ),
                buffered AS (
                    SELECT geometry
                    FROM blackprint_db_prd.integration.int_state_municipality_shapefile
                    WHERE ST_Within((SELECT geom FROM point_geom), geometry)
                ),
                h3_values AS (
                    SELECT H3_Polyfill(ST_Transform(geometry, 4326), 10) AS h3_indexes
                    FROM buffered
                ),
                h3_index AS (
                    SELECT o AS h3_value
                    FROM h3_values i, i.h3_indexes o
                )
                SELECT 
                    case when p.chain_id != 'None' then p.chain_id else null end as brand, 
                    p."name" as names_pri, 
                    p.geometry_wkt, 
                    p.main_category,
                    p.sub_category, 
                    p.sub_sub_category, 
                    p.business_category, 
                    p.open_closed_status, 
                    p.popularity_score, 
                    p.average_stars, 
                    p.number_of_reviews, 
                    p.sentiment_score
                FROM blackprint_db_prd.presentation.dim_pois_qro p
                INNER JOIN h3_index h ON p.h3_value::VARCHAR = h.h3_value::VARCHAR
                ORDER BY p.main_category, p.sub_category
            """
        else:  # mexico
            query = f"""
                WITH point_geom AS (
                    SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
                ),
                buffered AS (
                    SELECT geometry
                    FROM blackprint_db_prd.integration.int_state_municipality_shapefile_mexico
                    WHERE ST_Within((SELECT geom FROM point_geom), geometry)
                ),
                h3_values AS (
                    SELECT H3_Polyfill(ST_Transform(geometry, 4326), 10) AS h3_indexes
                    FROM buffered
                ),
                h3_index AS (
                    SELECT o AS h3_value
                    FROM h3_values i, i.h3_indexes o
                )
                SELECT 
                    case when p.chain_id != 'None' then p.chain_id else null end as brand, 
                    p."name" as names_pri, 
                    p.geometry_wkt, 
                    p.main_category,
                    p.sub_category, 
                    p.sub_sub_category, 
                    p.business_category, 
                    p.open_closed_status, 
                    p.popularity_score, 
                    p.average_stars, 
                    p.number_of_reviews, 
                    p.sentiment_score
                FROM blackprint_db_prd.presentation.dim_pois_mexico p
                INNER JOIN h3_index h ON p.h3_value::VARCHAR = h.h3_value::VARCHAR
                ORDER BY p.main_category, p.sub_category
            """
        return query

    def build_socioeconomic_income_analysis_query(self, lat, lng, radius, entity_code=22):
        """Build optimized query for comprehensive socioeconomic income analysis."""
        # Convert radius from meters to degrees (approximate: 1 degree ≈ 111,320 meters at equator)
        radius_degrees = radius / 111320.0
        query = f"""
        SELECT
          ROUND(SUM(total_housing)::NUMERIC, 0) AS total_households,
          ROUND(SUM(ab_2024 + cplus_2024 + c_2024 + cminus_2024 + dplus_2024 + d_2024 + e_2024)::NUMERIC, 0) AS total_household_income,
          ROUND(SUM(ab_2024 + cplus_2024 + c_2024 + cminus_2024 + dplus_2024 + d_2024 + e_2024)::NUMERIC / NULLIF(SUM(total_housing), 0), 2) AS avg_household_income
        FROM presentation.dim_socioeconomic_level_ageb
          WHERE ST_DWithin(
                ST_SetSRID(geometry_coords, 4326),
                ST_SetSRID(ST_Point({lng}, {lat}), 4326),
                  {radius_degrees}
                )
            AND entity_code = {entity_code}
        """
        return query

    def build_socioeconomic_municipality_analysis_query(self, lat, lng, radius, entity_code=22):
        """Build optimized query for municipality-level socioeconomic analysis."""
        query = f"""
        WITH municipality_bounds AS (
            SELECT geometry
            FROM blackprint_db_prd.integration.int_state_municipality_shapefile
            WHERE ST_Within(ST_SetSRID(ST_Point({lng}, {lat}), 4326), geometry)
            LIMIT 1
        )
        SELECT
            ROUND(SUM(total_housing)::NUMERIC, 0) AS total_households,
            ROUND(SUM(ab*ab_2024 + cplus*cplus_2024 + c*c_2024 + cminus*cminus_2024 + dplus*dplus_2024 + d*d_2024 + e*e_2024)::NUMERIC, 0) AS total_household_income,
            ROUND(SUM(ab*ab_2024 + cplus*cplus_2024 + c*c_2024 + cminus*cminus_2024 + dplus*dplus_2024 + d*d_2024 + e*e_2024)::NUMERIC / NULLIF(SUM(total_housing), 0), 2) AS avg_household_income
        FROM presentation.dim_socioeconomic_level_ageb a
        CROSS JOIN municipality_bounds m
        WHERE ST_Intersects(ST_SetSRID(a.geometry_coords, 4326), m.geometry)
          AND entity_code = {entity_code}
        """
        return query

    def build_socioeconomic_municipality_breakdown_query(self, lat, lng, radius, entity_code=22):
        """Build optimized query for municipality-level income breakdown."""
        query = f"""
        WITH municipality_bounds AS (
            SELECT geometry
            FROM blackprint_db_prd.integration.int_state_municipality_shapefile
            WHERE ST_Within(ST_SetSRID(ST_Point({lng}, {lat}), 4326), geometry)
            LIMIT 1
        ),
        municipality_data AS (
          SELECT 
                ab, cplus, c, cminus, dplus, d, e,
                ab_2024, cplus_2024, c_2024, cminus_2024, dplus_2024, d_2024, e_2024
            FROM presentation.dim_socioeconomic_level_ageb a
            CROSS JOIN municipality_bounds m
            WHERE ST_Intersects(ST_SetSRID(a.geometry_coords, 4326), m.geometry)
              AND entity_code = {entity_code}
        ),
        municipality_totals AS (
            SELECT 
                SUM(ab + cplus + c + cminus + dplus + d + e) AS total_households,
                SUM(ab_2024 + cplus_2024 + c_2024 + cminus_2024 + dplus_2024 + d_2024 + e_2024) AS total_income
            FROM municipality_data
        )
        SELECT 
            breakdown.level,
            ROUND(breakdown.households::NUMERIC, 0) AS households,
            ROUND((breakdown.households::NUMERIC / NULLIF(mt.total_households::NUMERIC, 0)) * 100, 2) AS pct_households,
            ROUND(breakdown.total_income_level::NUMERIC, 0) AS total_income_level,
            ROUND((breakdown.total_income_level::NUMERIC / NULLIF(mt.total_income::NUMERIC, 0)) * 100, 2) AS pct_income
        FROM (
            SELECT 'AB' AS level, SUM(ab) AS households, SUM(ab_2024) AS total_income_level FROM municipality_data
            UNION ALL
            SELECT 'C+' AS level, SUM(cplus), SUM(cplus_2024) FROM municipality_data
            UNION ALL
            SELECT 'C' AS level, SUM(c), SUM(c_2024) FROM municipality_data
            UNION ALL
            SELECT 'C-' AS level, SUM(cminus), SUM(cminus_2024) FROM municipality_data
            UNION ALL
            SELECT 'D+' AS level, SUM(dplus), SUM(dplus_2024) FROM municipality_data
            UNION ALL
            SELECT 'D' AS level, SUM(d), SUM(d_2024) FROM municipality_data
            UNION ALL
            SELECT 'E' AS level, SUM(e), SUM(e_2024) FROM municipality_data
        ) AS breakdown
        CROSS JOIN municipality_totals mt
        """
        return query

    def build_socioeconomic_municipality_growth_trends_query(self, lat, lng, radius, entity_code=22):
        """Build optimized query for municipality-level historical growth trends analysis."""
        query = f"""
        WITH municipality_bounds AS (
            SELECT geometry
            FROM blackprint_db_prd.integration.int_state_municipality_shapefile
            WHERE ST_Within(ST_SetSRID(ST_Point({lng}, {lat}), 4326), geometry)
            LIMIT 1
        ),
        municipality_data AS (
            SELECT 
                ab_2016, cplus_2016, c_2016, cminus_2016, dplus_2016, d_2016, e_2016,
                ab_2018, cplus_2018, c_2018, cminus_2018, dplus_2018, d_2018, e_2018,
                ab_2020, cplus_2020, c_2020, cminus_2020, dplus_2020, d_2020, e_2020,
                ab_2022, cplus_2022, c_2022, cminus_2022, dplus_2022, d_2022, e_2022,
                ab_2024, cplus_2024, c_2024, cminus_2024, dplus_2024, d_2024, e_2024
            FROM presentation.dim_socioeconomic_level_ageb a
            CROSS JOIN municipality_bounds m
            WHERE ST_Intersects(ST_SetSRID(a.geometry_coords, 4326), m.geometry)
              AND entity_code = {entity_code}
        ),
        municipality_totals AS (
            SELECT 
                SUM(ab_2016) AS ab_2016, SUM(cplus_2016) AS cplus_2016, SUM(c_2016) AS c_2016,
                SUM(cminus_2016) AS cminus_2016, SUM(dplus_2016) AS dplus_2016, SUM(d_2016) AS d_2016, SUM(e_2016) AS e_2016,
                SUM(ab_2018) AS ab_2018, SUM(cplus_2018) AS cplus_2018, SUM(c_2018) AS c_2018,
                SUM(cminus_2018) AS cminus_2018, SUM(dplus_2018) AS dplus_2018, SUM(d_2018) AS d_2018, SUM(e_2018) AS e_2018,
                SUM(ab_2020) AS ab_2020, SUM(cplus_2020) AS cplus_2020, SUM(c_2020) AS c_2020,
                SUM(cminus_2020) AS cminus_2020, SUM(dplus_2020) AS dplus_2020, SUM(d_2020) AS d_2020, SUM(e_2020) AS e_2020,
                SUM(ab_2022) AS ab_2022, SUM(cplus_2022) AS cplus_2022, SUM(c_2022) AS c_2022,
                SUM(cminus_2022) AS cminus_2022, SUM(dplus_2022) AS dplus_2022, SUM(d_2022) AS d_2022, SUM(e_2022) AS e_2022,
                SUM(ab_2024) AS ab_2024, SUM(cplus_2024) AS cplus_2024, SUM(c_2024) AS c_2024,
                SUM(cminus_2024) AS cminus_2024, SUM(dplus_2024) AS dplus_2024, SUM(d_2024) AS d_2024, SUM(e_2024) AS e_2024
            FROM municipality_data
        )
        SELECT
            'AB' AS level,
            ROUND(ab_2018 - ab_2016, 0) AS growth_2016_2018,
            ROUND(ab_2020 - ab_2018, 0) AS growth_2018_2020,
            ROUND(ab_2022 - ab_2020, 0) AS growth_2020_2022,
            ROUND(ab_2024 - ab_2022, 0) AS growth_2022_2024,
            ROUND(((ab_2018 - ab_2016)::NUMERIC / NULLIF(ab_2016,0)) * 100, 2) AS growth_pct_2016_2018,
            ROUND(((ab_2020 - ab_2018)::NUMERIC / NULLIF(ab_2018,0)) * 100, 2) AS growth_pct_2018_2020,
            ROUND(((ab_2022 - ab_2020)::NUMERIC / NULLIF(ab_2020,0)) * 100, 2) AS growth_pct_2020_2022,
            ROUND(((ab_2024 - ab_2022)::NUMERIC / NULLIF(ab_2022,0)) * 100, 2) AS growth_pct_2022_2024
        FROM municipality_totals
        UNION ALL
        SELECT
            'C+',
            ROUND(cplus_2018 - cplus_2016, 0),
            ROUND(cplus_2020 - cplus_2018, 0),
            ROUND(cplus_2022 - cplus_2020, 0),
            ROUND(cplus_2024 - cplus_2022, 0),
            ROUND(((cplus_2018 - cplus_2016)::NUMERIC / NULLIF(cplus_2016,0)) * 100, 2),
            ROUND(((cplus_2020 - cplus_2018)::NUMERIC / NULLIF(cplus_2018,0)) * 100, 2),
            ROUND(((cplus_2022 - cplus_2020)::NUMERIC / NULLIF(cplus_2020,0)) * 100, 2),
            ROUND(((cplus_2024 - cplus_2022)::NUMERIC / NULLIF(cplus_2022,0)) * 100, 2)
        FROM municipality_totals
        UNION ALL
        SELECT
            'C',
            ROUND(c_2018 - c_2016, 0),
            ROUND(c_2020 - c_2018, 0),
            ROUND(c_2022 - c_2020, 0),
            ROUND(c_2024 - c_2022, 0),
            ROUND(((c_2018 - c_2016)::NUMERIC / NULLIF(c_2016,0)) * 100, 2),
            ROUND(((c_2020 - c_2018)::NUMERIC / NULLIF(c_2018,0)) * 100, 2),
            ROUND(((c_2022 - c_2020)::NUMERIC / NULLIF(c_2020,0)) * 100, 2),
            ROUND(((c_2024 - c_2022)::NUMERIC / NULLIF(c_2022,0)) * 100, 2)
        FROM municipality_totals
        UNION ALL
        SELECT
            'C-',
            ROUND(cminus_2018 - cminus_2016, 0),
            ROUND(cminus_2020 - cminus_2018, 0),
            ROUND(cminus_2022 - cminus_2020, 0),
            ROUND(cminus_2024 - cminus_2022, 0),
            ROUND(((cminus_2018 - cminus_2016)::NUMERIC / NULLIF(cminus_2016,0)) * 100, 2),
            ROUND(((cminus_2020 - cminus_2018)::NUMERIC / NULLIF(cminus_2018,0)) * 100, 2),
            ROUND(((cminus_2022 - cminus_2020)::NUMERIC / NULLIF(cminus_2020,0)) * 100, 2),
            ROUND(((cminus_2024 - cminus_2022)::NUMERIC / NULLIF(cminus_2022,0)) * 100, 2)
        FROM municipality_totals
        UNION ALL
        SELECT
            'D+',
            ROUND(dplus_2018 - dplus_2016, 0),
            ROUND(dplus_2020 - dplus_2018, 0),
            ROUND(dplus_2022 - dplus_2020, 0),
            ROUND(dplus_2024 - dplus_2022, 0),
            ROUND(((dplus_2018 - dplus_2016)::NUMERIC / NULLIF(dplus_2016,0)) * 100, 2),
            ROUND(((dplus_2020 - dplus_2018)::NUMERIC / NULLIF(dplus_2018,0)) * 100, 2),
            ROUND(((dplus_2022 - dplus_2020)::NUMERIC / NULLIF(dplus_2020,0)) * 100, 2),
            ROUND(((dplus_2024 - dplus_2022)::NUMERIC / NULLIF(dplus_2022,0)) * 100, 2)
        FROM municipality_totals
        UNION ALL
        SELECT
            'D',
            ROUND(d_2018 - d_2016, 0),
            ROUND(d_2020 - d_2018, 0),
            ROUND(d_2022 - d_2020, 0),
            ROUND(d_2024 - d_2022, 0),
            ROUND(((d_2018 - d_2016)::NUMERIC / NULLIF(d_2016,0)) * 100, 2),
            ROUND(((d_2020 - d_2018)::NUMERIC / NULLIF(d_2018,0)) * 100, 2),
            ROUND(((d_2022 - d_2020)::NUMERIC / NULLIF(d_2020,0)) * 100, 2),
            ROUND(((d_2024 - d_2022)::NUMERIC / NULLIF(d_2022,0)) * 100, 2)
        FROM municipality_totals
        UNION ALL
        SELECT
            'E',
            ROUND(e_2018 - e_2016, 0),
            ROUND(e_2020 - e_2018, 0),
            ROUND(e_2022 - e_2020, 0),
            ROUND(e_2024 - e_2022, 0),
            ROUND(((e_2018 - e_2016)::NUMERIC / NULLIF(e_2016,0)) * 100, 2),
            ROUND(((e_2020 - e_2018)::NUMERIC / NULLIF(e_2018,0)) * 100, 2),
            ROUND(((e_2022 - e_2020)::NUMERIC / NULLIF(e_2020,0)) * 100, 2),
            ROUND(((e_2024 - e_2022)::NUMERIC / NULLIF(e_2022,0)) * 100, 2)
        FROM municipality_totals
        """
        return query


    def build_socioeconomic_breakdown_query(self, lat, lng, radius, entity_code=22):
        """Build optimized query for income level breakdown with percentages."""
        # Convert radius from meters to degrees
        radius_degrees = radius / 111320.0
        query = f"""
        WITH area_data AS (
          SELECT 
            ab, cplus, c, cminus, dplus, d, e,
            ab_2024, cplus_2024, c_2024, cminus_2024, dplus_2024, d_2024, e_2024
          FROM presentation.dim_socioeconomic_level_ageb
          WHERE ST_DWithin(
                  ST_SetSRID(geometry_coords, 4326),
                  ST_SetSRID(ST_Point({lng}, {lat}), 4326),
                  {radius_degrees}
                )
            AND entity_code = {entity_code}
        ),
        totals AS (
          SELECT 
            SUM(ab + cplus + c + cminus + dplus + d + e) AS total_households,
            SUM(ab_2024 + cplus_2024 + c_2024 + cminus_2024 + dplus_2024 + d_2024 + e_2024) AS total_income
          FROM area_data
        )
        SELECT 
          breakdown.level,
          ROUND(breakdown.households::NUMERIC, 0) AS households,
          ROUND((breakdown.households::NUMERIC / NULLIF(totals.total_households::NUMERIC, 0)) * 100, 2) AS pct_households,
          ROUND(breakdown.total_income_level::NUMERIC, 0) AS total_income_level,
          ROUND((breakdown.total_income_level::NUMERIC / NULLIF(totals.total_income::NUMERIC, 0)) * 100, 2) AS pct_income
        FROM (
          SELECT 'AB' AS level, SUM(ab) AS households, SUM(ab_2024) AS total_income_level FROM area_data
          UNION ALL
          SELECT 'C+' AS level, SUM(cplus), SUM(cplus_2024) FROM area_data
          UNION ALL
          SELECT 'C' AS level, SUM(c), SUM(c_2024) FROM area_data
          UNION ALL
          SELECT 'C-' AS level, SUM(cminus), SUM(cminus_2024) FROM area_data
          UNION ALL
          SELECT 'D+' AS level, SUM(dplus), SUM(dplus_2024) FROM area_data
          UNION ALL
          SELECT 'D' AS level, SUM(d), SUM(d_2024) FROM area_data
          UNION ALL
          SELECT 'E' AS level, SUM(e), SUM(e_2024) FROM area_data
        ) AS breakdown
        CROSS JOIN totals
        """
        return query

    def build_socioeconomic_growth_trends_query(self, lat, lng, radius, entity_code=22):
        """Build optimized query for historical growth trends analysis."""
        # Convert radius from meters to degrees
        radius_degrees = radius / 111320.0
        query = f"""
        WITH area_data AS (
          SELECT 
            ab_2016, cplus_2016, c_2016, cminus_2016, dplus_2016, d_2016, e_2016,
            ab_2018, cplus_2018, c_2018, cminus_2018, dplus_2018, d_2018, e_2018,
            ab_2020, cplus_2020, c_2020, cminus_2020, dplus_2020, d_2020, e_2020,
            ab_2022, cplus_2022, c_2022, cminus_2022, dplus_2022, d_2022, e_2022,
            ab_2024, cplus_2024, c_2024, cminus_2024, dplus_2024, d_2024, e_2024
          FROM presentation.dim_socioeconomic_level_ageb
          WHERE ST_DWithin(
                  ST_SetSRID(geometry_coords, 4326),
                  ST_SetSRID(ST_Point({lng}, {lat}), 4326),
                  {radius_degrees}
                )
            AND entity_code = {entity_code}
        ),
        totals AS (
          SELECT 
            SUM(ab_2016) AS ab_2016, SUM(cplus_2016) AS cplus_2016, SUM(c_2016) AS c_2016,
            SUM(cminus_2016) AS cminus_2016, SUM(dplus_2016) AS dplus_2016, SUM(d_2016) AS d_2016, SUM(e_2016) AS e_2016,
            SUM(ab_2018) AS ab_2018, SUM(cplus_2018) AS cplus_2018, SUM(c_2018) AS c_2018,
            SUM(cminus_2018) AS cminus_2018, SUM(dplus_2018) AS dplus_2018, SUM(d_2018) AS d_2018, SUM(e_2018) AS e_2018,
            SUM(ab_2020) AS ab_2020, SUM(cplus_2020) AS cplus_2020, SUM(c_2020) AS c_2020,
            SUM(cminus_2020) AS cminus_2020, SUM(dplus_2020) AS dplus_2020, SUM(d_2020) AS d_2020, SUM(e_2020) AS e_2020,
            SUM(ab_2022) AS ab_2022, SUM(cplus_2022) AS cplus_2022, SUM(c_2022) AS c_2022,
            SUM(cminus_2022) AS cminus_2022, SUM(dplus_2022) AS dplus_2022, SUM(d_2022) AS d_2022, SUM(e_2022) AS e_2022,
            SUM(ab_2024) AS ab_2024, SUM(cplus_2024) AS cplus_2024, SUM(c_2024) AS c_2024,
            SUM(cminus_2024) AS cminus_2024, SUM(dplus_2024) AS dplus_2024, SUM(d_2024) AS d_2024, SUM(e_2024) AS e_2024
          FROM area_data
        )
        SELECT
          'AB' AS level,
          ab_2018 - ab_2016 AS growth_2016_2018,
          ab_2020 - ab_2018 AS growth_2018_2020,
          ab_2022 - ab_2020 AS growth_2020_2022,
          ab_2024 - ab_2022 AS growth_2022_2024,
          ROUND(((ab_2018 - ab_2016)::NUMERIC / NULLIF(ab_2016,0)) * 100, 2) AS growth_pct_2016_2018,
          ROUND(((ab_2020 - ab_2018)::NUMERIC / NULLIF(ab_2018,0)) * 100, 2) AS growth_pct_2018_2020,
          ROUND(((ab_2022 - ab_2020)::NUMERIC / NULLIF(ab_2020,0)) * 100, 2) AS growth_pct_2020_2022,
          ROUND(((ab_2024 - ab_2022)::NUMERIC / NULLIF(ab_2022,0)) * 100, 2) AS growth_pct_2022_2024
        FROM totals
        UNION ALL
        SELECT
          'C+',
          cplus_2018 - cplus_2016,
          cplus_2020 - cplus_2018,
          cplus_2022 - cplus_2020,
          cplus_2024 - cplus_2022,
          ROUND(((cplus_2018 - cplus_2016)::NUMERIC / NULLIF(cplus_2016,0)) * 100, 2),
          ROUND(((cplus_2020 - cplus_2018)::NUMERIC / NULLIF(cplus_2018,0)) * 100, 2),
          ROUND(((cplus_2022 - cplus_2020)::NUMERIC / NULLIF(cplus_2020,0)) * 100, 2),
          ROUND(((cplus_2024 - cplus_2022)::NUMERIC / NULLIF(cplus_2022,0)) * 100, 2)
        FROM totals
        UNION ALL
        SELECT
          'C',
          c_2018 - c_2016,
          c_2020 - c_2018,
          c_2022 - c_2020,
          c_2024 - c_2022,
          ROUND(((c_2018 - c_2016)::NUMERIC / NULLIF(c_2016,0)) * 100, 2),
          ROUND(((c_2020 - c_2018)::NUMERIC / NULLIF(c_2018,0)) * 100, 2),
          ROUND(((c_2022 - c_2020)::NUMERIC / NULLIF(c_2020,0)) * 100, 2),
          ROUND(((c_2024 - c_2022)::NUMERIC / NULLIF(c_2022,0)) * 100, 2)
        FROM totals
        UNION ALL
        SELECT
          'C-',
          cminus_2018 - cminus_2016,
          cminus_2020 - cminus_2018,
          cminus_2022 - cminus_2020,
          cminus_2024 - cminus_2022,
          ROUND(((cminus_2018 - cminus_2016)::NUMERIC / NULLIF(cminus_2016,0)) * 100, 2),
          ROUND(((cminus_2020 - cminus_2018)::NUMERIC / NULLIF(cminus_2018,0)) * 100, 2),
          ROUND(((cminus_2022 - cminus_2020)::NUMERIC / NULLIF(cminus_2020,0)) * 100, 2),
          ROUND(((cminus_2024 - cminus_2022)::NUMERIC / NULLIF(cminus_2022,0)) * 100, 2)
        FROM totals
        UNION ALL
        SELECT
          'D+',
          dplus_2018 - dplus_2016,
          dplus_2020 - dplus_2018,
          dplus_2022 - dplus_2020,
          dplus_2024 - dplus_2022,
          ROUND(((dplus_2018 - dplus_2016)::NUMERIC / NULLIF(dplus_2016,0)) * 100, 2),
          ROUND(((dplus_2020 - dplus_2018)::NUMERIC / NULLIF(dplus_2018,0)) * 100, 2),
          ROUND(((dplus_2022 - dplus_2020)::NUMERIC / NULLIF(dplus_2020,0)) * 100, 2),
          ROUND(((dplus_2024 - dplus_2022)::NUMERIC / NULLIF(dplus_2022,0)) * 100, 2)
        FROM totals
        UNION ALL
        SELECT
          'D',
          d_2018 - d_2016,
          d_2020 - d_2018,
          d_2022 - d_2020,
          d_2024 - d_2022,
          ROUND(((d_2018 - d_2016)::NUMERIC / NULLIF(d_2016,0)) * 100, 2),
          ROUND(((d_2020 - d_2018)::NUMERIC / NULLIF(d_2018,0)) * 100, 2),
          ROUND(((d_2022 - d_2020)::NUMERIC / NULLIF(d_2020,0)) * 100, 2),
          ROUND(((d_2024 - d_2022)::NUMERIC / NULLIF(d_2022,0)) * 100, 2)
        FROM totals
        UNION ALL
        SELECT
          'E',
          e_2018 - e_2016,
          e_2020 - e_2018,
          e_2022 - e_2020,
          e_2024 - e_2022,
          ROUND(((e_2018 - e_2016)::NUMERIC / NULLIF(e_2016,0)) * 100, 2),
          ROUND(((e_2020 - e_2018)::NUMERIC / NULLIF(e_2018,0)) * 100, 2),
          ROUND(((e_2022 - e_2020)::NUMERIC / NULLIF(e_2020,0)) * 100, 2),
          ROUND(((e_2024 - e_2022)::NUMERIC / NULLIF(e_2022,0)) * 100, 2)
        FROM totals
        """
        return query