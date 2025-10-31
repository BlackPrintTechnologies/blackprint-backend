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
    
    # Convert radius from degrees to meters (approximate: 1 degree ≈ 111,320 meters at equator)
    # Then calculate area in km²
    radius_meters = radius * 111320.0
    area_km2 = f"PI() * POWER({radius_meters} / 1000.0, 2)"
    
    query = f"""
    SELECT 
        SUM(COALESCE(pobtot, 0)) AS total_population,
        SUM(COALESCE(pobmas, 0)) AS male_population,
        SUM(COALESCE(pobfem, 0)) AS female_population,
        SUM(COALESCE(tothog, vivtot, 0)) AS total_households,
        CASE 
            WHEN SUM(COALESCE(tothog, vivtot, 0)) > 0 
            THEN ROUND(CAST(SUM(COALESCE(pobtot, 0)) AS DECIMAL(15,2)) / CAST(SUM(COALESCE(tothog, vivtot, 0)) AS DECIMAL(15,2)), 2)
            ELSE 0 
        END AS average_people_per_household,
        CASE 
            WHEN {area_km2} > 0 
            THEN ROUND(CAST(SUM(COALESCE(pobtot, 0)) AS DECIMAL(15,2)) / CAST({area_km2} AS DECIMAL(10,2)), 2)
            ELSE 0 
        END AS population_density,
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
        SUM(COALESCE(pob_2020_ageb, pob_2020, 0)) AS population_2020,
        ROUND({area_km2}, 2) AS area_km2
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

