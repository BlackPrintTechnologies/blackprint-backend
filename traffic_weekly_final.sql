-- Weekly Traffic Distribution with Municipality Analysis
-- Parameters: lat=20.6303604599386, lng=-100.415857940964, radius=2000

WITH point_geom AS (
  -- Create point geometry from input coordinates
  SELECT ST_SetSRID(ST_MakePoint(-100.415857940964, 20.6303604599386), 4326) AS geom
),
point_projected AS (
  -- Transform to projected coordinate system for buffer calculation
  SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
),
buffered AS (
  -- Create buffer around the point with specified radius
  SELECT ST_Buffer(geom, 2000) AS geom FROM point_projected
),
h3_values AS (
  -- Get H3 hexagons that intersect with the buffer
  SELECT H3_Polyfill(ST_Transform(geom, 4326), 10) AS h3_indexes FROM buffered
),
h3_index AS (
  -- Flatten H3 indexes for joining
  SELECT o AS h3_value
  FROM h3_values i, i.h3_indexes o
),
-- Get municipality info for the user's location
user_municipality AS (
  SELECT 
    d.cve_mun as municipality_code,
    d.nom_mun as municipality_name,
    d.pobtot_alcaldia as municipality_population,
    d.vivtot_alcaldia as municipality_households
  FROM data_product.v_qro d
  WHERE d.centroid IS NOT NULL
  AND ST_Intersects(
    ST_SetSRID(ST_MakePoint(-100.415857940964, 20.6303604599386), 4326),
    ST_Transform(ST_Buffer(ST_Transform(ST_SetSRID(ST_MakePoint(-100.415857940964, 20.6303604599386), 4326), 3857), 100), 4326)
  )
  LIMIT 1
),
-- Traffic data for selected area (within radius)
selected_area_traffic AS (
  SELECT 
    -- Weekly traffic distribution for selected area
    SUM(CASE WHEN a.dia_de_la_semana = 'Monday' THEN a.total_usuarios_unicos ELSE 0 END) AS monday,
    SUM(CASE WHEN a.dia_de_la_semana = 'Tuesday' THEN a.total_usuarios_unicos ELSE 0 END) AS tuesday,
    SUM(CASE WHEN a.dia_de_la_semana = 'Wednesday' THEN a.total_usuarios_unicos ELSE 0 END) AS wednesday,
    SUM(CASE WHEN a.dia_de_la_semana = 'Thursday' THEN a.total_usuarios_unicos ELSE 0 END) AS thursday,
    SUM(CASE WHEN a.dia_de_la_semana = 'Friday' THEN a.total_usuarios_unicos ELSE 0 END) AS friday,
    SUM(CASE WHEN a.dia_de_la_semana = 'Saturday' THEN a.total_usuarios_unicos ELSE 0 END) AS saturday,
    SUM(CASE WHEN a.dia_de_la_semana = 'Sunday' THEN a.total_usuarios_unicos ELSE 0 END) AS sunday,
    
    -- Total traffic for selected area
    SUM(a.total_usuarios_unicos) AS total_traffic_selected,
    
    -- Traffic by user type for selected area
    SUM(CASE WHEN a.tipo_usuario = 'vehiculo' THEN a.total_usuarios_unicos ELSE 0 END) AS vehiculo_selected,
    SUM(CASE WHEN a.tipo_usuario = 'peaton' THEN a.total_usuarios_unicos ELSE 0 END) AS peaton_selected,
    SUM(CASE WHEN a.tipo_usuario = 'estacionario' THEN a.total_usuarios_unicos ELSE 0 END) AS estacionario_selected,
    
    -- Count of H3 hexagons in selected area
    COUNT(DISTINCT a.h3_index) AS hexagons_count_selected
    
  FROM blackprint_db_prd.staging.stg_data_movilidad_por_dia_qro a
  INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
),
-- Traffic data for entire municipality (using spatial join with municipality boundaries)
municipality_traffic AS (
  SELECT 
    -- Weekly traffic distribution for municipality
    SUM(CASE WHEN a.dia_de_la_semana = 'Monday' THEN a.total_usuarios_unicos ELSE 0 END) AS monday_municipality,
    SUM(CASE WHEN a.dia_de_la_semana = 'Tuesday' THEN a.total_usuarios_unicos ELSE 0 END) AS tuesday_municipality,
    SUM(CASE WHEN a.dia_de_la_semana = 'Wednesday' THEN a.total_usuarios_unicos ELSE 0 END) AS wednesday_municipality,
    SUM(CASE WHEN a.dia_de_la_semana = 'Thursday' THEN a.total_usuarios_unicos ELSE 0 END) AS thursday_municipality,
    SUM(CASE WHEN a.dia_de_la_semana = 'Friday' THEN a.total_usuarios_unicos ELSE 0 END) AS friday_municipality,
    SUM(CASE WHEN a.dia_de_la_semana = 'Saturday' THEN a.total_usuarios_unicos ELSE 0 END) AS saturday_municipality,
    SUM(CASE WHEN a.dia_de_la_semana = 'Sunday' THEN a.total_usuarios_unicos ELSE 0 END) AS sunday_municipality,
    
    -- Total traffic for municipality
    SUM(a.total_usuarios_unicos) AS total_traffic_municipality,
    
    -- Traffic by user type for municipality
    SUM(CASE WHEN a.tipo_usuario = 'vehiculo' THEN a.total_usuarios_unicos ELSE 0 END) AS vehiculo_municipality,
    SUM(CASE WHEN a.tipo_usuario = 'peaton' THEN a.total_usuarios_unicos ELSE 0 END) AS peaton_municipality,
    SUM(CASE WHEN a.tipo_usuario = 'estacionario' THEN a.total_usuarios_unicos ELSE 0 END) AS estacionario_municipality,
    
    -- Count of H3 hexagons in municipality
    COUNT(DISTINCT a.h3_index) AS hexagons_count_municipality
    
  FROM blackprint_db_prd.staging.stg_data_movilidad_por_dia_qro a
  CROSS JOIN user_municipality um
  WHERE ST_Intersects(
    ST_GeomFromGeoJSON(d.geometry_geojson),
    H3_To_Geometry(a.h3_index)
  )
  AND d.cve_mun = um.municipality_code
)
SELECT 
  -- ===== SELECTED AREA TRAFFIC DATA =====
  sat.monday,
  sat.tuesday,
  sat.wednesday,
  sat.thursday,
  sat.friday,
  sat.saturday,
  sat.sunday,
  sat.total_traffic_selected,
  sat.vehiculo_selected,
  sat.peaton_selected,
  sat.estacionario_selected,
  sat.hexagons_count_selected,
  
  -- ===== MUNICIPALITY TRAFFIC DATA =====
  mt.monday_municipality,
  mt.tuesday_municipality,
  mt.wednesday_municipality,
  mt.thursday_municipality,
  mt.friday_municipality,
  mt.saturday_municipality,
  mt.sunday_municipality,
  mt.total_traffic_municipality,
  mt.vehiculo_municipality,
  mt.peaton_municipality,
  mt.estacionario_municipality,
  mt.hexagons_count_municipality,
  
  -- ===== MUNICIPALITY INFO =====
  um.municipality_code,
  um.municipality_name,
  um.municipality_population,
  um.municipality_households,
  
  -- ===== CALCULATED METRICS =====
  -- Traffic density (traffic per hexagon)
  CASE 
    WHEN sat.hexagons_count_selected > 0 
    THEN ROUND(CAST(sat.total_traffic_selected AS DECIMAL(15,2)) / CAST(sat.hexagons_count_selected AS DECIMAL(15,2)), 2)
    ELSE 0 
  END AS traffic_density_selected,
  
  CASE 
    WHEN mt.hexagons_count_municipality > 0 
    THEN ROUND(CAST(mt.total_traffic_municipality AS DECIMAL(15,2)) / CAST(mt.hexagons_count_municipality AS DECIMAL(15,2)), 2)
    ELSE 0 
  END AS traffic_density_municipality,
  
  -- Percentage of municipality traffic in selected area
  CASE 
    WHEN mt.total_traffic_municipality > 0 
    THEN ROUND(CAST(sat.total_traffic_selected AS DECIMAL(15,2)) / CAST(mt.total_traffic_municipality AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS selected_area_percentage_of_municipality,
  
  -- User type percentages for selected area
  CASE 
    WHEN sat.total_traffic_selected > 0 
    THEN ROUND(CAST(sat.vehiculo_selected AS DECIMAL(15,2)) / CAST(sat.total_traffic_selected AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS vehiculo_percentage_selected,
  
  CASE 
    WHEN sat.total_traffic_selected > 0 
    THEN ROUND(CAST(sat.peaton_selected AS DECIMAL(15,2)) / CAST(sat.total_traffic_selected AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS peaton_percentage_selected,
  
  CASE 
    WHEN sat.total_traffic_selected > 0 
    THEN ROUND(CAST(sat.estacionario_selected AS DECIMAL(15,2)) / CAST(sat.total_traffic_selected AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS estacionario_percentage_selected,
  
  -- User type percentages for municipality
  CASE 
    WHEN mt.total_traffic_municipality > 0 
    THEN ROUND(CAST(mt.vehiculo_municipality AS DECIMAL(15,2)) / CAST(mt.total_traffic_municipality AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS vehiculo_percentage_municipality,
  
  CASE 
    WHEN mt.total_traffic_municipality > 0 
    THEN ROUND(CAST(mt.peaton_municipality AS DECIMAL(15,2)) / CAST(mt.total_traffic_municipality AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS peaton_percentage_municipality,
  
  CASE 
    WHEN mt.total_traffic_municipality > 0 
    THEN ROUND(CAST(mt.estacionario_municipality AS DECIMAL(15,2)) / CAST(mt.total_traffic_municipality AS DECIMAL(15,2)) * 100, 2)
    ELSE 0 
  END AS estacionario_percentage_municipality,
  
  -- ===== INPUT PARAMETERS =====
  CAST(20.6303604599386 AS DECIMAL(10,7)) as center_lat,
  CAST(-100.415857940964 AS DECIMAL(10,7)) as center_lng,
  2000 as radius_meters

FROM selected_area_traffic sat
CROSS JOIN municipality_traffic mt
CROSS JOIN user_municipality um;
