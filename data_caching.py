#caching the municpality data for the area analysis in postgres

from utils.dbUtils import RedshiftDatabase
from psycopg2.extras import RealDictCursor

# Initialize Redshift database connection
redshift_db = RedshiftDatabase()

# SQL query for Queretaro - using shapefile for accurate geometry/area, joined with v_qro for demographics
query_queretaro = """
WITH municipality_area AS (
    SELECT 
        cve_mun,
        nomgeo,
        ROUND(ST_Area(ST_Transform(geometry, 3857)) / 1000000, 2) AS area_km2
    FROM blackprint_db_prd.integration.int_state_municipality_shapefile
    WHERE cve_ent = '22'
        AND geometry IS NOT NULL
),
municipality_demographics AS (
    SELECT 
        cve_mun,
        MAX(COALESCE(pobtot_alcaldia, 0)) AS total_population,
        MAX(COALESCE(pobmas_alcaldia, 0)) AS male_population,
        MAX(COALESCE(pobfem_alcaldia, 0)) AS female_population,
        MAX(COALESCE(vivtot_alcaldia, 0)) AS total_households,
        MAX(COALESCE(pob_2000_municipal, 0)) AS population_2000,
        MAX(COALESCE(pob_2005_municipal, 0)) AS population_2005,
        MAX(COALESCE(pob_2010_municipal, 0)) AS population_2010,
        MAX(COALESCE(pob_2020_municipal, 0)) AS population_2020
    FROM blackprint_db_prd.data_product.v_qro
    WHERE cve_mun IS NOT NULL
    GROUP BY cve_mun
),
municipality_socioeconomic AS (
    SELECT 
        CAST(TRIM(municipality_code) AS INTEGER) AS municipality_code_int,
        SUM(COALESCE(ab, 0)) AS households_ab,
        SUM(COALESCE(cplus, 0)) AS households_cplus,
        SUM(COALESCE(c, 0)) AS households_c,
        SUM(COALESCE(cminus, 0)) AS households_cminus,
        SUM(COALESCE(dplus, 0)) AS households_dplus,
        SUM(COALESCE(d, 0)) AS households_d,
        SUM(COALESCE(e, 0)) AS households_e,
        -- Calculate total household income (using 2020 data - most recent available)
        SUM(
            (COALESCE(ab, 0) * COALESCE(ab_2020, 0)) +
            (COALESCE(cplus, 0) * COALESCE(cplus_2020, 0)) +
            (COALESCE(c, 0) * COALESCE(c_2020, 0)) +
            (COALESCE(cminus, 0) * COALESCE(cminus_2020, 0)) +
            (COALESCE(dplus, 0) * COALESCE(dplus_2020, 0)) +
            (COALESCE(d, 0) * COALESCE(d_2020, 0)) +
            (COALESCE(e, 0) * COALESCE(e_2020, 0))
        ) AS total_household_income,
        SUM(
            COALESCE(ab, 0) + COALESCE(cplus, 0) + COALESCE(c, 0) + 
            COALESCE(cminus, 0) + COALESCE(dplus, 0) + COALESCE(d, 0) + COALESCE(e, 0)
        ) AS total_se_households
    FROM blackprint_db_prd.presentation.dim_socioeconomic_level_ageb
    WHERE entity_code = '22'
        AND municipality_code IS NOT NULL
    GROUP BY CAST(TRIM(municipality_code) AS INTEGER)
),
municipality_predominant_level AS (
    SELECT 
        municipality_code_int,
        CASE 
            WHEN max_households = households_ab THEN 'A/B'
            WHEN max_households = households_cplus THEN 'C+'
            WHEN max_households = households_c THEN 'C'
            WHEN max_households = households_cminus THEN 'C-'
            WHEN max_households = households_dplus THEN 'D+'
            WHEN max_households = households_d THEN 'D'
            WHEN max_households = households_e THEN 'E'
            ELSE 'N/A'
        END AS predominant_level,
        max_households
    FROM (
        SELECT 
            CAST(TRIM(municipality_code) AS INTEGER) AS municipality_code_int,
            SUM(COALESCE(ab, 0)) AS households_ab,
            SUM(COALESCE(cplus, 0)) AS households_cplus,
            SUM(COALESCE(c, 0)) AS households_c,
            SUM(COALESCE(cminus, 0)) AS households_cminus,
            SUM(COALESCE(dplus, 0)) AS households_dplus,
            SUM(COALESCE(d, 0)) AS households_d,
            SUM(COALESCE(e, 0)) AS households_e,
            GREATEST(
                SUM(COALESCE(ab, 0)),
                SUM(COALESCE(cplus, 0)),
                SUM(COALESCE(c, 0)),
                SUM(COALESCE(cminus, 0)),
                SUM(COALESCE(dplus, 0)),
                SUM(COALESCE(d, 0)),
                SUM(COALESCE(e, 0))
            ) AS max_households
        FROM blackprint_db_prd.presentation.dim_socioeconomic_level_ageb
        WHERE entity_code = '22'
            AND municipality_code IS NOT NULL
        GROUP BY CAST(TRIM(municipality_code) AS INTEGER)
    ) sub
)
SELECT 
    ma.cve_mun AS municipality_id,
    ma.nomgeo AS municipality_name,
    ma.area_km2,
    COALESCE(md.total_population, 0) AS total_population,
    COALESCE(md.male_population, 0) AS male_population,
    COALESCE(md.female_population, 0) AS female_population,
    COALESCE(md.total_households, 0) AS total_households,
    ROUND(COALESCE(md.total_population, 0) / NULLIF(ma.area_km2, 0), 2) AS population_density_per_km2,
    ROUND(COALESCE(md.total_population, 0) / NULLIF(COALESCE(md.total_households, 0), 0), 2) AS average_people_per_household,
    COALESCE(md.population_2000, 0) AS population_2000,
    COALESCE(md.population_2005, 0) AS population_2005,
    COALESCE(md.population_2010, 0) AS population_2010,
    COALESCE(md.population_2020, 0) AS population_2020,
    0 AS growth_2000,
    ROUND(CASE 
        WHEN COALESCE(md.population_2000, 0) > 0 
        THEN ((COALESCE(md.population_2005, 0)::NUMERIC - COALESCE(md.population_2000, 0)::NUMERIC) / COALESCE(md.population_2000, 0)::NUMERIC) * 100
        ELSE 0 
    END, 2) AS growth_2005,
    ROUND(CASE 
        WHEN COALESCE(md.population_2005, 0) > 0 
        THEN ((COALESCE(md.population_2010, 0)::NUMERIC - COALESCE(md.population_2005, 0)::NUMERIC) / COALESCE(md.population_2005, 0)::NUMERIC) * 100
        ELSE 0 
    END, 2) AS growth_2010,
    ROUND(CASE 
        WHEN COALESCE(md.population_2010, 0) > 0 
        THEN ((COALESCE(md.population_2020, 0)::NUMERIC - COALESCE(md.population_2010, 0)::NUMERIC) / COALESCE(md.population_2010, 0)::NUMERIC) * 100
        ELSE 0 
    END, 2) AS growth_2020,
    -- Socioeconomic metrics
    COALESCE(se.total_household_income, 0) AS total_household_income,
    ROUND(COALESCE(se.total_household_income, 0) / NULLIF(COALESCE(se.total_se_households, 0), 0), 2) AS average_household_income,
    COALESCE(pl.predominant_level, 'N/A') AS predominant_socioeconomic_level,
    ROUND(COALESCE(pl.max_households, 0)::NUMERIC / NULLIF(COALESCE(se.total_se_households, 0), 0) * 100, 2) AS predominant_level_percentage,
    COALESCE(se.households_ab, 0) + COALESCE(se.households_cplus, 0) + COALESCE(se.households_c, 0) AS households_top_level,
    COALESCE(se.households_d, 0) + COALESCE(se.households_e, 0) AS households_bottom_level,
    ROUND(
        (COALESCE(se.households_ab, 0) + COALESCE(se.households_cplus, 0) + COALESCE(se.households_c, 0))::NUMERIC / 
        NULLIF(COALESCE(se.total_se_households, 0), 0) * 100, 
        2
    ) AS households_top_level_percentage,
    ROUND(
        (COALESCE(se.households_d, 0) + COALESCE(se.households_e, 0))::NUMERIC / 
        NULLIF(COALESCE(se.total_se_households, 0), 0) * 100, 
        2
    ) AS households_bottom_level_percentage
FROM municipality_area ma
LEFT JOIN municipality_demographics md ON CAST(TRIM(ma.cve_mun) AS INTEGER) = CAST(TRIM(md.cve_mun) AS INTEGER)
LEFT JOIN municipality_socioeconomic se ON CAST(TRIM(ma.cve_mun) AS INTEGER) = se.municipality_code_int
LEFT JOIN municipality_predominant_level pl ON CAST(TRIM(ma.cve_mun) AS INTEGER) = pl.municipality_code_int
ORDER BY ma.cve_mun;
"""

# SQL query for Mexico City - using shapefile for accurate geometry/area, joined with v_parcel_v3 for demographics
query_mexico = """
WITH municipality_area AS (
    SELECT 
        cve_mun,
        nomgeo,
        ROUND(ST_Area(ST_Transform(geometry, 3857)) / 1000000, 2) AS area_km2
    FROM blackprint_db_prd.integration.int_state_municipality_shapefile
    WHERE cve_ent = '09'
        AND geometry IS NOT NULL
),
municipality_demographics AS (
    SELECT 
        municipality_code,
        MAX(COALESCE(pobtot_alcaldia, 0)) AS total_population,
        MAX(COALESCE(pobmas_alcaldia, 0)) AS male_population,
        MAX(COALESCE(pobfem_alcaldia, 0)) AS female_population,
        MAX(COALESCE(vivtot_alcaldia, 0)) AS total_households,
        MAX(COALESCE(pob_2000_municipal, 0)) AS population_2000,
        MAX(COALESCE(pob_2005_municipal, 0)) AS population_2005,
        MAX(COALESCE(pob_2010_municipal, 0)) AS population_2010,
        MAX(COALESCE(pob_2020_municipal, 0)) AS population_2020
    FROM blackprint_db_prd.data_product.v_parcel_v3
    WHERE municipality_code IS NOT NULL
    GROUP BY municipality_code
),
municipality_socioeconomic AS (
    SELECT 
        CAST(TRIM(municipality_code) AS INTEGER) AS municipality_code_int,
        SUM(COALESCE(ab, 0)) AS households_ab,
        SUM(COALESCE(cplus, 0)) AS households_cplus,
        SUM(COALESCE(c, 0)) AS households_c,
        SUM(COALESCE(cminus, 0)) AS households_cminus,
        SUM(COALESCE(dplus, 0)) AS households_dplus,
        SUM(COALESCE(d, 0)) AS households_d,
        SUM(COALESCE(e, 0)) AS households_e,
        -- Calculate total household income (using 2020 data - most recent available)
        SUM(
            (COALESCE(ab, 0) * COALESCE(ab_2020, 0)) +
            (COALESCE(cplus, 0) * COALESCE(cplus_2020, 0)) +
            (COALESCE(c, 0) * COALESCE(c_2020, 0)) +
            (COALESCE(cminus, 0) * COALESCE(cminus_2020, 0)) +
            (COALESCE(dplus, 0) * COALESCE(dplus_2020, 0)) +
            (COALESCE(d, 0) * COALESCE(d_2020, 0)) +
            (COALESCE(e, 0) * COALESCE(e_2020, 0))
        ) AS total_household_income,
        SUM(
            COALESCE(ab, 0) + COALESCE(cplus, 0) + COALESCE(c, 0) + 
            COALESCE(cminus, 0) + COALESCE(dplus, 0) + COALESCE(d, 0) + COALESCE(e, 0)
        ) AS total_se_households
    FROM blackprint_db_prd.presentation.dim_socioeconomic_level_ageb
    WHERE entity_code = '09'
        AND municipality_code IS NOT NULL
    GROUP BY CAST(TRIM(municipality_code) AS INTEGER)
),
municipality_predominant_level AS (
    SELECT 
        municipality_code_int,
        CASE 
            WHEN max_households = households_ab THEN 'A/B'
            WHEN max_households = households_cplus THEN 'C+'
            WHEN max_households = households_c THEN 'C'
            WHEN max_households = households_cminus THEN 'C-'
            WHEN max_households = households_dplus THEN 'D+'
            WHEN max_households = households_d THEN 'D'
            WHEN max_households = households_e THEN 'E'
            ELSE 'N/A'
        END AS predominant_level,
        max_households
    FROM (
        SELECT 
            CAST(TRIM(municipality_code) AS INTEGER) AS municipality_code_int,
            SUM(COALESCE(ab, 0)) AS households_ab,
            SUM(COALESCE(cplus, 0)) AS households_cplus,
            SUM(COALESCE(c, 0)) AS households_c,
            SUM(COALESCE(cminus, 0)) AS households_cminus,
            SUM(COALESCE(dplus, 0)) AS households_dplus,
            SUM(COALESCE(d, 0)) AS households_d,
            SUM(COALESCE(e, 0)) AS households_e,
            GREATEST(
                SUM(COALESCE(ab, 0)),
                SUM(COALESCE(cplus, 0)),
                SUM(COALESCE(c, 0)),
                SUM(COALESCE(cminus, 0)),
                SUM(COALESCE(dplus, 0)),
                SUM(COALESCE(d, 0)),
                SUM(COALESCE(e, 0))
            ) AS max_households
        FROM blackprint_db_prd.presentation.dim_socioeconomic_level_ageb
        WHERE entity_code = '09'
            AND municipality_code IS NOT NULL
        GROUP BY CAST(TRIM(municipality_code) AS INTEGER)
    ) sub
)
SELECT 
    ma.cve_mun AS municipality_id,
    ma.nomgeo AS municipality_name,
    ma.area_km2,
    COALESCE(md.total_population, 0) AS total_population,
    COALESCE(md.male_population, 0) AS male_population,
    COALESCE(md.female_population, 0) AS female_population,
    COALESCE(md.total_households, 0) AS total_households,
    ROUND(COALESCE(md.total_population, 0) / NULLIF(ma.area_km2, 0), 2) AS population_density_per_km2,
    ROUND(COALESCE(md.total_population, 0) / NULLIF(COALESCE(md.total_households, 0), 0), 2) AS average_people_per_household,
    COALESCE(md.population_2000, 0) AS population_2000,
    COALESCE(md.population_2005, 0) AS population_2005,
    COALESCE(md.population_2010, 0) AS population_2010,
    COALESCE(md.population_2020, 0) AS population_2020,
    0 AS growth_2000,
    ROUND(CASE 
        WHEN COALESCE(md.population_2000, 0) > 0 
        THEN ((COALESCE(md.population_2005, 0)::NUMERIC - COALESCE(md.population_2000, 0)::NUMERIC) / COALESCE(md.population_2000, 0)::NUMERIC) * 100
        ELSE 0 
    END, 2) AS growth_2005,
    ROUND(CASE 
        WHEN COALESCE(md.population_2005, 0) > 0 
        THEN ((COALESCE(md.population_2010, 0)::NUMERIC - COALESCE(md.population_2005, 0)::NUMERIC) / COALESCE(md.population_2005, 0)::NUMERIC) * 100
        ELSE 0 
    END, 2) AS growth_2010,
    ROUND(CASE
        WHEN COALESCE(md.population_2010, 0) > 0 
        THEN ((COALESCE(md.population_2020, 0)::NUMERIC - COALESCE(md.population_2010, 0)::NUMERIC) / COALESCE(md.population_2010, 0)::NUMERIC) * 100
        ELSE 0 
    END, 2) AS growth_2020,
    -- Socioeconomic metrics
    COALESCE(se.total_household_income, 0) AS total_household_income,
    ROUND(COALESCE(se.total_household_income, 0) / NULLIF(COALESCE(se.total_se_households, 0), 0), 2) AS average_household_income,
    COALESCE(pl.predominant_level, 'N/A') AS predominant_socioeconomic_level,
    ROUND(COALESCE(pl.max_households, 0)::NUMERIC / NULLIF(COALESCE(se.total_se_households, 0), 0) * 100, 2) AS predominant_level_percentage,
    COALESCE(se.households_ab, 0) + COALESCE(se.households_cplus, 0) + COALESCE(se.households_c, 0) AS households_top_level,
    COALESCE(se.households_d, 0) + COALESCE(se.households_e, 0) AS households_bottom_level,
    ROUND(
        (COALESCE(se.households_ab, 0) + COALESCE(se.households_cplus, 0) + COALESCE(se.households_c, 0))::NUMERIC / 
        NULLIF(COALESCE(se.total_se_households, 0), 0) * 100, 
        2
    ) AS households_top_level_percentage,
    ROUND(
        (COALESCE(se.households_d, 0) + COALESCE(se.households_e, 0))::NUMERIC / 
        NULLIF(COALESCE(se.total_se_households, 0), 0) * 100, 
        2
    ) AS households_bottom_level_percentage
FROM municipality_area ma
LEFT JOIN municipality_demographics md ON 
    TRIM(ma.cve_mun) = TRIM(CAST(md.municipality_code AS VARCHAR))
    OR TRIM(ma.cve_mun) = RIGHT(TRIM(CAST(md.municipality_code AS VARCHAR)), LENGTH(TRIM(ma.cve_mun)))
    OR CAST(TRIM(ma.cve_mun) AS INTEGER) = CAST(TRIM(md.municipality_code) AS INTEGER)
LEFT JOIN municipality_socioeconomic se ON 
    CAST(TRIM(ma.cve_mun) AS INTEGER) = se.municipality_code_int
    OR TRIM(ma.cve_mun) = RIGHT(CAST(se.municipality_code_int AS VARCHAR), LENGTH(TRIM(ma.cve_mun)))
LEFT JOIN municipality_predominant_level pl ON 
    CAST(TRIM(ma.cve_mun) AS INTEGER) = pl.municipality_code_int
    OR TRIM(ma.cve_mun) = RIGHT(CAST(pl.municipality_code_int AS VARCHAR), LENGTH(TRIM(ma.cve_mun)))
ORDER BY ma.cve_mun;
"""

# Execute query for Queretaro and fetch results
connection = redshift_db.connect()
cursor = connection.cursor(cursor_factory=RealDictCursor)
cursor.execute(query_queretaro)
municipalities_queretaro = cursor.fetchall()

# Execute query for Mexico City and fetch results
cursor.execute(query_mexico)
municipalities_mexico = cursor.fetchall()

cursor.close()
redshift_db.disconnect(connection)

# Print the data
print("=== QUERETARO MUNICIPALITIES ===")
for municipality in municipalities_queretaro:
    print(municipality)

print("\n=== MEXICO CITY MUNICIPALITIES ===")
for municipality in municipalities_mexico:
    print(municipality)
