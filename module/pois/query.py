class POIsQueryController:
    def __init__(self):
        pass

    def get_pois_hierarchy_query(self, config_city=None):
        """Generate query to get POI category hierarchy."""
        if config_city == "queretaro" or config_city == "el_marques":
            places_table = 'blackprint_db_prd.presentation.dim_places_qro'
        else:
            places_table = 'blackprint_db_prd.presentation.dim_places'
        
        query = f'''
            SELECT DISTINCT 
                category_1, 
                category_2, 
                category_3
            FROM {places_table}
            WHERE category_1 IS NOT NULL 
            AND category_1 != ''
            ORDER BY category_1, category_2, category_3
        '''
        return query

    @staticmethod
    def get_brand_query(catchment, fid, category_1=None, subcategories=None, subsubcategories=None, brand_names=None, city="mexico"):
        """Generate brand query based on catchment radius and city."""
        if city == "mexico":
            id_column = "fid"
            parcel_table = 'blackprint_db_prd.data_product.v_parcel_v3'
            dim_places_table = 'blackprint_db_prd.presentation.dim_places'
        elif city == "queretaro" or city == "el_marques":
            id_column = "id_stg_demographic_socioeconomic_qro"
            parcel_table = 'blackprint_db_prd.data_product.v_qro'
            dim_places_table = 'blackprint_db_prd.presentation.dim_places_qro'

        if catchment == '500':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_500m FROM {parcel_table} WHERE {id_column} = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_500m FROM {parcel_table} WHERE {id_column} = {fid}), ',')
                        )
                        SELECT brand, names_pri, geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE id_place IN (SELECT value FROM split_values) ;'''

        elif catchment == '1000':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_1km FROM {parcel_table} WHERE {id_column} = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_1km FROM {parcel_table} WHERE {id_column} = {fid}), ',')
                        )
                        SELECT brand, names_pri, geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE id_place IN (SELECT value FROM split_values) ;'''

        elif catchment == '50':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_front FROM {parcel_table} WHERE {id_column} = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_front FROM {parcel_table} WHERE {id_column} = {fid}), ',')
                        )
                        SELECT brand, names_pri, geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE id_place IN (SELECT value FROM split_values) ;'''
        else:
            query = f'''SELECT brand, names_pri, geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE 1 = 1 '''
            if category_1:
                category_1_list = "', '".join([name.strip() for name in category_1.split(',')])
                query += f" AND category_1 in ('{category_1_list}')"
            if subcategories:
                subcategories_list = "', '".join([name.strip() for name in subcategories.split(',')])
                query += f" AND category_2 in ('{subcategories_list}')"
            if subsubcategories:
                subsubcategories_list = "', '".join([name.strip() for name in subsubcategories.split(',')])
                query += f" AND category_3 in ('{subsubcategories_list}')"
            if brand_names:  
                brand_list = "', '".join([name.strip() for name in brand_names.split(',')])
                query += f" AND names_pri in ('{brand_list}') "
        return query

    @staticmethod
    def get_brand_search_query(brand_name, city="mexico"):
        """Generate query to search brands by name pattern."""
        if city == "queretaro" or city == "el_marques":
            places_table = 'blackprint_db_prd.presentation.dim_places_qro'
        else:
            places_table = 'blackprint_db_prd.presentation.dim_places'
        
        query = f'''SELECT DISTINCT brand 
                    FROM {places_table} 
                    WHERE brand ILIKE '{brand_name}%' 
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