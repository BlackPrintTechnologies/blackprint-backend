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
        table_name = 'presentation.dim_demographic_by_block'
    
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
            SUM(COALESCE(pob_2000_ageb, pob_2000, 0)) AS population_2000,
            SUM(COALESCE(pob_2005_ageb, pob_2005, 0)) AS population_2005,
            SUM(COALESCE(pob_2010_ageb, pob_2010, 0)) AS population_2010,
            SUM(COALESCE(pob_2020_ageb, pob_2020, 0)) AS population_2020
        FROM {table_name} d
        WHERE ST_Intersects(
            d.{geometry_column},
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
        dd.population_2000,
        dd.population_2005,
        dd.population_2010,
        dd.population_2020,
        ROUND((SELECT area_km2 FROM area_calc), 2) AS area_km2
    FROM demographic_data dd
    """
    
    return query


def build_socioeconomic_query(lng, lat, radius, geometry_column='geometry_coords', table='ageb'):
    """Build query to get socio-economic data for the selected area (table: 'ageb' or 'locality')."""
    geometry_condition = build_geometry_condition_catchment(geometry_column, lng, lat, radius)
    
    if table == 'locality':
        table_name = 'presentation.dim_socioeconomic_level_localidad'
    else:
        table_name = 'presentation.dim_socioeconomic_level_ageb'
    
    query = f"""
    SELECT *
    FROM {table_name}
    WHERE {geometry_condition}
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
