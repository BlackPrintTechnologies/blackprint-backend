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


def build_demographics_query(lng, lat, radius, geometry_column='geometry_coords', table='block'):
    """Build query to get demographic data for the selected area (table: 'block' or 'locality')."""
    geometry_condition = build_geometry_condition_catchment(geometry_column, lng, lat, radius)
    
    if table == 'locality':
        table_name = 'presentation.dim_demographic_by_localidad'
    else:
        table_name = 'presentation.dim_demographic_by_block'
    
    query = f"""
    SELECT *
    FROM {table_name}
    WHERE {geometry_condition}
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
