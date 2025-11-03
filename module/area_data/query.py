"""Query building functions for area data."""

import logging

logger = logging.getLogger(__name__)


def build_point_geometry(lng, lat, srid=4326):
    """Build a point geometry from longitude and latitude."""
    return f"ST_SetSRID(ST_MakePoint({lng}, {lat}), {srid})"


def build_buffer_from_point(lng, lat, radius):
    """Build a buffer geometry from a point with specified radius in degrees."""
    return f"""ST_setSRID(ST_Buffer(
        ST_MakePoint({lng}, {lat}),
        {radius}
    ), 4326)"""


def build_geometry_condition_catchment(geometry_column, lng, lat, radius):
    """Build ST_Intersects condition for catchment area (point + radius)."""
    buffer_geom = build_buffer_from_point(lng, lat, radius)
    return f"""ST_Intersects(
        ST_Transform(
            ST_SetSRID(
                {geometry_column}, 
                32614
            ),
            4326
        ),
        {buffer_geom}
    )"""


def build_demographics_query(boundary, geometry_column='geometry_coords', table='block'):
    """Build query to get demographic data for the selected catchment area (table: 'block' or 'locality')."""

    if table == 'locality':
        table_name = 'presentation.dim_demographic_by_localidad'
    else:
        table_name = 'presentation.dim_demographic_by_block_locality'

    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    area_calc AS (
        SELECT ST_Area(geom::geography) / 1000000.0 AS area_km2
        FROM geom_input
    ),
    demographic_data AS (
        SELECT 
            SUM(COALESCE(pobtot, 0)) AS total_population,
            SUM(COALESCE(pobmas, 0)) AS male_population,
            SUM(COALESCE(pobfem, 0)) AS female_population,
            SUM(COALESCE(tothog, vivtot, 0)) AS total_households,
            SUM(COALESCE(p_0a2, 0)) AS age_0_2,
            SUM(COALESCE(p_3a5, 0)) AS age_3_5,
            SUM(COALESCE(p_6a11, 0)) AS age_6_11,
            SUM(COALESCE(p_12a14, 0)) AS age_12_14,
            SUM(COALESCE(p_15a17, 0)) AS age_15_17,
            SUM(COALESCE(p_18a24, 0)) AS age_18_24,
            SUM(COALESCE(pob65_mas, 0)) AS age_65_plus,
            SUM(COALESCE(p_0a2_f, 0)) AS age_0_2_f,
            SUM(COALESCE(p_0a2_m, 0)) AS age_0_2_m,
            SUM(COALESCE(p_3a5_f, 0)) AS age_3_5_f,
            SUM(COALESCE(p_3a5_m, 0)) AS age_3_5_m,
            SUM(COALESCE(p_6a11_f, 0)) AS age_6_11_f,
            SUM(COALESCE(p_6a11_m, 0)) AS age_6_11_m,
            SUM(COALESCE(p_12a14_f, 0)) AS age_12_14_f,
            SUM(COALESCE(p_12a14_m, 0)) AS age_12_14_m,
            SUM(COALESCE(p_15a17_f, 0)) AS age_15_17_f,
            SUM(COALESCE(p_15a17_m, 0)) AS age_15_17_m,
            SUM(COALESCE(p_18a24_f, 0)) AS age_18_24_f,
            SUM(COALESCE(p_18a24_m, 0)) AS age_18_24_m
        FROM {table_name} d
        WHERE ST_Intersects(
            d.{geometry_column},
            (SELECT geom FROM geom_input)
        )
    ),
    pop_growth_data AS (
        SELECT 
            COALESCE(SUM(total_pop_2000), 0) AS population_2000,
            COALESCE(SUM(total_pop_2005), 0) AS population_2005,
            COALESCE(SUM(total_pop_2010), 0) AS population_2010,
            COALESCE(SUM(total_pop_2020), 0) AS population_2020,
            -- Simple average of growth rates (not weighted)
            COALESCE(AVG(pop_growth_rate_2000_2005), 0) AS pop_growth_rate_2000_2005,
            COALESCE(AVG(pop_growth_rate_2005_2010), 0) AS pop_growth_rate_2005_2010,
            COALESCE(AVG(pop_growth_rate_2010_2020), 0) AS pop_growth_rate_2010_2020
        FROM blackprint_db_prd.presentation.dim_pop_growth_per_ageb_locality pg
        WHERE ST_Intersects(
            pg.geometry_coords,
            (SELECT geom FROM geom_input)
        )
    )
    SELECT 
        dd.total_population,
        dd.male_population,
        dd.female_population,
        dd.total_households,
        CASE 
            WHEN dd.total_households > 0 
            THEN ROUND(CAST(dd.total_population AS DECIMAL(15,2)) / CAST(dd.total_households AS DECIMAL(15,2)), 2)
            ELSE 0 
        END AS average_people_per_household,
        CASE 
            WHEN (SELECT area_km2 FROM area_calc) > 0 
            THEN ROUND(CAST(dd.total_population AS DECIMAL(15,2)) / CAST((SELECT area_km2 FROM area_calc) AS DECIMAL(10,2)), 2)
            ELSE 0 
        END AS population_density,
        dd.age_0_2,
        dd.age_3_5,
        dd.age_6_11,
        dd.age_12_14,
        dd.age_15_17,
        dd.age_18_24,
        dd.age_65_plus,
        dd.age_0_2_f,
        dd.age_0_2_m,
        dd.age_3_5_f,
        dd.age_3_5_m,
        dd.age_6_11_f,
        dd.age_6_11_m,
        dd.age_12_14_f,
        dd.age_12_14_m,
        dd.age_15_17_f,
        dd.age_15_17_m,
        dd.age_18_24_f,
        dd.age_18_24_m,
        -- Population totals from dim_pop_growth_per_ageb_locality
        pg.population_2000,
        pg.population_2005,
        pg.population_2010,
        pg.population_2020,
        -- Population growth rates
        pg.pop_growth_rate_2000_2005,
        pg.pop_growth_rate_2005_2010,
        pg.pop_growth_rate_2010_2020,
        ROUND((SELECT area_km2 FROM area_calc), 2) AS area_km2
    FROM demographic_data dd
    CROSS JOIN pop_growth_data pg
    """

    return query


def build_traffic_summary_query(boundary, user_type=None):
    """Build query to get traffic summary data."""
    user_type_condition = ""
    if user_type:
        user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    h3_values AS (
        SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
    ),
    h3_index AS (
        SELECT o AS h3_value
        FROM h3_values i, i.h3_indexes o
    )
    SELECT  SUM(a.total_usuarios_unicos) AS total_users
    FROM blackprint_db_prd.presentation.dim_mobility_data_by_hour a
    INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
    {user_type_condition}
    """
    return query

def build_h3_traffic_summary_query(boundary):
    """Build query to get H3 traffic summary data."""
    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    h3_values AS (
        SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
    ),
    h3_index AS (
        SELECT o AS h3_value
        FROM h3_values i, i.h3_indexes o
    )
    SELECT 
        COUNT(DISTINCT a.h3_index) AS unique_h3_count,
        SUM(a.total_usuarios_unicos) AS total_unique_users,
        ROUND(CAST(SUM(a.total_usuarios_unicos) AS DECIMAL) / CAST(COUNT(DISTINCT a.h3_index) AS DECIMAL), 2) AS avg_users_per_h3
    FROM blackprint_db_prd.presentation.dim_mobility_data_by_day a
    INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
    """


def build_socioeconomic_query(boundary):
    """Build query to get socioeconomic data for the selected catchment area."""
    
    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    socioeconomic_data AS (
        SELECT 
            -- Current SES levels (latest available, assuming 2024)
            SUM(COALESCE(ab, 0)) AS ses_ab,
            SUM(COALESCE(cplus, 0)) AS ses_c_plus,
            SUM(COALESCE(c, 0)) AS ses_c,
            SUM(COALESCE(cminus, 0)) AS ses_c_minus,
            SUM(COALESCE(dplus, 0)) AS ses_d_plus,
            SUM(COALESCE(d, 0)) AS ses_d,
            SUM(COALESCE(e, 0)) AS ses_e,
            -- Total housing
            SUM(COALESCE(total_housing, 0)) AS total_housing,
            -- Predominant level (most common)
            MODE() WITHIN GROUP (ORDER BY predominant_level) AS predominant_level,
            -- Historical data for 2024
            SUM(COALESCE(ab_2024, 0)) AS ses_ab_2024,
            SUM(COALESCE(c_2024, 0)) AS ses_c_2024,
            SUM(COALESCE(cplus_2024, 0)) AS ses_c_plus_2024,
            SUM(COALESCE(cminus_2024, 0)) AS ses_c_minus_2024,
            SUM(COALESCE(d_2024, 0)) AS ses_d_2024,
            SUM(COALESCE(dplus_2024, 0)) AS ses_d_plus_2024,
            SUM(COALESCE(e_2024, 0)) AS ses_e_2024,
            -- Historical data for 2022
            SUM(COALESCE(ab_2022, 0)) AS ses_ab_2022,
            SUM(COALESCE(c_2022, 0)) AS ses_c_2022,
            SUM(COALESCE(cplus_2022, 0)) AS ses_c_plus_2022,
            SUM(COALESCE(cminus_2022, 0)) AS ses_c_minus_2022,
            SUM(COALESCE(d_2022, 0)) AS ses_d_2022,
            SUM(COALESCE(dplus_2022, 0)) AS ses_d_plus_2022,
            SUM(COALESCE(e_2022, 0)) AS ses_e_2022,
            -- Historical data for 2020
            SUM(COALESCE(ab_2020, 0)) AS ses_ab_2020,
            SUM(COALESCE(c_2020, 0)) AS ses_c_2020,
            SUM(COALESCE(cplus_2020, 0)) AS ses_c_plus_2020,
            SUM(COALESCE(cminus_2020, 0)) AS ses_c_minus_2020,
            SUM(COALESCE(d_2020, 0)) AS ses_d_2020,
            SUM(COALESCE(dplus_2020, 0)) AS ses_d_plus_2020,
            SUM(COALESCE(e_2020, 0)) AS ses_e_2020,
            -- Historical data for 2018
            SUM(COALESCE(ab_2018, 0)) AS ses_ab_2018,
            SUM(COALESCE(c_2018, 0)) AS ses_c_2018,
            SUM(COALESCE(cplus_2018, 0)) AS ses_c_plus_2018,
            SUM(COALESCE(cminus_2018, 0)) AS ses_c_minus_2018,
            SUM(COALESCE(d_2018, 0)) AS ses_d_2018,
            SUM(COALESCE(dplus_2018, 0)) AS ses_d_plus_2018,
            SUM(COALESCE(e_2018, 0)) AS ses_e_2018,
            -- Historical data for 2016
            SUM(COALESCE(ab_2016, 0)) AS ses_ab_2016,
            SUM(COALESCE(c_2016, 0)) AS ses_c_2016,
            SUM(COALESCE(cplus_2016, 0)) AS ses_c_plus_2016,
            SUM(COALESCE(cminus_2016, 0)) AS ses_c_minus_2016,
            SUM(COALESCE(d_2016, 0)) AS ses_d_2016,
            SUM(COALESCE(dplus_2016, 0)) AS ses_d_plus_2016,
            SUM(COALESCE(e_2016, 0)) AS ses_e_2016
        FROM blackprint_db_prd.presentation.dim_socioeconomic_level_ageb_locality se
        WHERE ST_Intersects(
            se.geometry_coords,
            (SELECT geom FROM geom_input)
        )
    )
    SELECT 
        sd.ses_ab,
        sd.ses_c_plus,
        sd.ses_c,
        sd.ses_c_minus,
        sd.ses_d_plus,
        sd.ses_d,
        sd.ses_e,
        sd.total_housing,
        sd.predominant_level,
        -- Historical data grouped by year
        sd.ses_ab_2024,
        sd.ses_c_2024,
        sd.ses_c_plus_2024,
        sd.ses_c_minus_2024,
        sd.ses_d_2024,
        sd.ses_d_plus_2024,
        sd.ses_e_2024,
        sd.ses_ab_2022,
        sd.ses_c_2022,
        sd.ses_c_plus_2022,
        sd.ses_c_minus_2022,
        sd.ses_d_2022,
        sd.ses_d_plus_2022,
        sd.ses_e_2022,
        sd.ses_ab_2020,
        sd.ses_c_2020,
        sd.ses_c_plus_2020,
        sd.ses_c_minus_2020,
        sd.ses_d_2020,
        sd.ses_d_plus_2020,
        sd.ses_e_2020,
        sd.ses_ab_2018,
        sd.ses_c_2018,
        sd.ses_c_plus_2018,
        sd.ses_c_minus_2018,
        sd.ses_d_2018,
        sd.ses_d_plus_2018,
        sd.ses_e_2018,
        sd.ses_ab_2016,
        sd.ses_c_2016,
        sd.ses_c_plus_2016,
        sd.ses_c_minus_2016,
        sd.ses_d_2016,
        sd.ses_d_plus_2016,
        sd.ses_e_2016
    FROM socioeconomic_data sd
    """
    
    return query


def build_pois_query(catchment):
    """Build query to get POIs data for the selected catchment."""

    query = f"""
        WITH geom_input AS (
            SELECT ST_GeomFromText('{catchment}', 4326) AS geom
        ),
        h3_values AS (
            SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT CASE WHEN chain_id != 'None' THEN chain_id ELSE NULL END AS brand,
               name AS names_pri,
               geometry_wkt,
               main_category,
               sub_category,
               sub_sub_category,
               business_category,
               open_closed_status,
               popularity_score,
               average_stars,
               number_of_reviews,
               sentiment_score
        FROM blackprint_db_prd.presentation.dim_pois_qro a
        INNER JOIN h3_index b ON a.h3_value = b.h3_value
    """
    return query

def build_h3_traffic_summary_query(boundary):
    """Build query to get H3 traffic summary data."""
    query = f"""WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    h3_values AS (
        SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
    ),
    h3_index AS (
        SELECT o AS h3_value
        FROM h3_values i, i.h3_indexes o
    )
    SELECT 
        COUNT(DISTINCT a.h3_index) AS unique_h3_count,
        SUM(a.total_usuarios_unicos) AS total_unique_users,
        ROUND(CAST(SUM(a.total_usuarios_unicos) AS DECIMAL) / CAST(COUNT(DISTINCT a.h3_index) AS DECIMAL), 2) AS avg_users_per_h3
    FROM blackprint_db_prd.presentation.dim_mobility_data_by_day a
    INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
    """
    return query
def build_h3_distribution_query(boundary):
    """Build query to get H3 distribution data for all user types combined."""
    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    h3_values AS (
        SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
    ),
    h3_index AS (
        SELECT o AS h3_value
        FROM h3_values i, i.h3_indexes o
    ),
    traffic_data AS (
        SELECT 
            a.h3_index,
            SUM(a.total_usuarios_unicos) AS total_users
        FROM blackprint_db_prd.presentation.dim_mobility_data_by_hour a
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

def build_traffic_by_hour_query(boundary, user_type=None):
    """Build query to get traffic data aggregated by hour of the day."""
    user_type_condition = ""
    if user_type:
        user_type_condition = f"AND a.tipo_usuario = '{user_type}'"
        
    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    h3_values AS (
        SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
    ),
    h3_index AS (
        SELECT o AS h3_value
        FROM h3_values i, i.h3_indexes o
    )
    SELECT  
        SUM(CASE WHEN a.hour = 0 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_0,
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
    FROM blackprint_db_prd.presentation.dim_mobility_data_by_hour a
    INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
    WHERE 1=1 {user_type_condition}
    """
    return query

def build_traffic_by_day_query(boundary, user_type=None):
    """Build query to get traffic data aggregated by day of the week."""
    user_type_condition = ""
    if user_type:
        user_type_condition = f"AND a.tipo_usuario = '{user_type}'"
        
    query = f"""
    WITH geom_input AS (
        SELECT ST_GeomFromText('{boundary}', 4326) AS geom
    ),
    h3_values AS (
        SELECT H3_Polyfill(geom, 10) AS h3_indexes FROM geom_input
    ),
    h3_index AS (
        SELECT o AS h3_value
        FROM h3_values i, i.h3_indexes o
    )
    SELECT  
        SUM(CASE WHEN a.dia_de_la_semana = 'Monday' THEN a.total_usuarios_unicos ELSE 0 END) AS monday,
        SUM(CASE WHEN a.dia_de_la_semana = 'Tuesday' THEN a.total_usuarios_unicos ELSE 0 END) AS tuesday,
        SUM(CASE WHEN a.dia_de_la_semana = 'Wednesday' THEN a.total_usuarios_unicos ELSE 0 END) AS wednesday,
        SUM(CASE WHEN a.dia_de_la_semana = 'Thursday' THEN a.total_usuarios_unicos ELSE 0 END) AS thursday,
        SUM(CASE WHEN a.dia_de_la_semana = 'Friday' THEN a.total_usuarios_unicos ELSE 0 END) AS friday,
        SUM(CASE WHEN a.dia_de_la_semana = 'Saturday' THEN a.total_usuarios_unicos ELSE 0 END) AS saturday,
        SUM(CASE WHEN a.dia_de_la_semana = 'Sunday' THEN a.total_usuarios_unicos ELSE 0 END) AS sunday
    FROM blackprint_db_prd.presentation.dim_mobility_data_by_day a
    INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
    WHERE 1=1 {user_type_condition}
    """
    return query

def get_total_population_query(catchment):
    query = f"""
            select sum(pobtot) as total_population from presentation.dim_demographic_by_block
            where ST_Intersects(
                    ST_Transform(
                            ST_SetSRID(
                                geometry_coords, 
                                32614
                            ),
                            4326
                        ),  
                    ST_GeomFromText('{catchment}', 4326)
                )
        """
    return query
