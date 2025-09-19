class POIsQueryController:
    def __init__(self):
        pass

    def get_pois_hierarchy_query(self, config_city=None):
        """Generate query to get POI category hierarchy."""
        if config_city == "queretaro" or config_city == "el_marques":
            places_table = 'blackprint_db_prd.presentation.dim_pois_qro'
        else:
            places_table = 'blackprint_db_prd.presentation.dim_pois_cdmx'
        
        query = f'''
            SELECT DISTINCT 
                main_category as category_1, 
                sub_category as category_2, 
                sub_sub_category as category_3
            FROM {places_table}
            WHERE main_category IS NOT NULL 
            AND main_category != ''
            ORDER BY main_category, sub_category, sub_sub_category
        '''
        return query

    @staticmethod
    def get_brand_query(catchment, lat, lng, category_1=None, subcategories=None, subsubcategories=None, brand_names=None, city="mexico"):
        """Generate brand query based on catchment radius and city."""
        if city == "mexico":
            dim_places_table = 'blackprint_db_prd.presentation.dim_pois_cdmx'
        elif city == "queretaro" or city == "el_marques":
            dim_places_table = 'blackprint_db_prd.presentation.dim_pois_qro'

        query = f'''WITH point_geom AS (
            SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
            ),
            point_projected AS (
            SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
            ),
            buffered AS (
            -- radius n meters
            SELECT ST_Buffer(geom, {catchment}) AS geom FROM point_projected
            ),
            h3_values AS (
            SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
            ),
            h3_index AS (
                SELECT o AS h3_value
                FROM h3_values i, i.h3_indexes o
            )
            SELECT case when chain_id != 'None' then chain_id else null end as brand, name as names_pri, geometry_wkt, main_category as category_1 
            FROM {dim_places_table} a
            INNER JOIN h3_index b ON a.h3_value = b.h3_value
            ;'''

        if not catchment:
            query = f'''SELECT case when chain_id != 'None' then chain_id else null end as brand, name as names_pri, geometry_wkt, main_category as category_1  FROM {dim_places_table}
                        WHERE 1 = 1 '''
            if category_1:
                category_1_list = "', '".join([name.strip() for name in category_1.split(',')])
                query += f" AND main_category in ('{category_1_list}')"
            if subcategories:
                subcategories_list = "', '".join([name.strip() for name in subcategories.split(',')])
                query += f" AND sub_category in ('{subcategories_list}')"
            if subsubcategories:
                subsubcategories_list = "', '".join([name.strip() for name in subsubcategories.split(',')])
                query += f" AND sub_sub_category in ('{subsubcategories_list}')"
            if brand_names:  
                brand_list = "', '".join([name.strip() for name in brand_names.split(',')])
                query += f" AND chain_id in ('{brand_list}') "
        return query    

    @staticmethod
    def get_brand_search_query(brand_name, city="mexico"):
        """Generate query to search brands by name pattern."""
        if city == "queretaro" or city == "el_marques":
            places_table = 'blackprint_db_prd.presentation.dim_pois_qro'
        else:
            places_table = 'blackprint_db_prd.presentation.dim_pois_cdmx'
        
        query = f'''SELECT DISTINCT chain_id as brand 
                    FROM {places_table} 
                    WHERE chain_id ILIKE '{brand_name}%' 
                    LIMIT 50'''
        return query

    def get_pois_by_coordinates_query(self, lat, lng, radius, city="mexico"):
        """Generate query to get POIs within radius from coordinates."""
        # This can be implemented later for direct coordinate-based POI search
        # For now, return a placeholder query
        query = f"""
            SELECT * FROM (
                SELECT 
                    brand, 
                    names_pri, 
                    geometry_wkt, 
                    category_1,
                    ST_Distance(
                        ST_GeomFromText(geometry_wkt, 4326),
                        ST_GeomFromText('POINT({lng} {lat})', 4326)
                    ) as distance
                FROM blackprint_db_prd.presentation.dim_places
                WHERE geometry_wkt IS NOT NULL
            ) subquery
            WHERE distance <= {radius}
            ORDER BY distance
            LIMIT 100
        """
        return query