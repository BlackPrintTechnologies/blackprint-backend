class MarketsQueryController:
    """
    Query controller for markets module
    Handles all market-related database queries
    """
    
    def __init__(self):
        pass
    
    @staticmethod
    def get_all_distinct_property_types_query(city):
        """
        Get query for all distinct property types from market data tables
        Uses combined table approach for better performance
        """
        query = """
            SELECT DISTINCT property_type 
            FROM presentation.dim_market_data_combined
            where  property_type IS NOT NULL 
            ORDER BY property_type
        """

        
        return query
    
    @staticmethod
    def get_property_market_info_combined_query(spot2_id, inmuebles24_id, propiedades_id, city='mexico'):
        """
        Get market information using the combined table approach
        This provides better performance and consistent data structure
        """
        if city == 'queretaro' or city == 'el_marques':
            # QRO combined query
            if inmuebles24_id:
                query = f"""
                    SELECT
                        id_market_data,
                        title,
                        rent_price_clean,
                        rent_price_per_m2,
                        buy_price_clean,
                        buy_price_per_m2,
                        publication_date,
                        pictures,
                        property_type,
                        operation_type,
                        property_dimension_clean as total_area_clean,
                        city,
                        url,
                        'inmuebles24' as source
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE source = 'inmuebles24' AND id_market_data = {inmuebles24_id}
                      AND city IN ('Querétaro', 'El Marqués', 'Corregidora')
                """
            elif spot2_id:
                query = f"""
                    SELECT
                       id_market_data,
                        title,
                        rent_price_clean,
                        rent_price_per_m2,
                        buy_price_clean,
                        buy_price_per_m2,
                        publication_date,
                        pictures,
                        property_type,
                        operation_type,
                        property_dimension_clean as total_area_clean,
                        city,
                        url,
                        'spot2' as source
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE source = 'spot2' AND id_market_data = {spot2_id}
                      AND city IN ('Querétaro', 'El Marqués','Corregidora')
                """
            else:
                # No valid ID for QRO
                query = """
                    SELECT NULL WHERE 1=0
                """
        else:
            # Mexico combined query
            if inmuebles24_id:
                query = f"""
                    SELECT
                        id_market_data,
                        title,
                        rent_price_clean,
                        rent_price_per_m2,
                        buy_price_clean,
                        buy_price_per_m2,
                        publication_date,
                        pictures,
                        property_type,
                        operation_type,
                        property_dimension_clean as total_area_clean,
                        city,
                        url,
                        'inmuebles24' as source
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE source = 'inmuebles24' AND id_market_data = {inmuebles24_id}
                      AND city = 'mexico'
                """
            elif spot2_id:
                query = f"""
                    SELECT
                        id_market_data,
                        title,
                        city,
                        operation_type,
                        rent_price,
                        rent_price_clean,
                        rent_price_per_m2,
                        buy_price_clean,
                        buy_price_per_m2,
                        property_type,
                        property_dimension_clean as total_area_clean,
                        amenities,
                        pictures,
                        url,
                        publication_date,
                        'spot2' as source
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE source = 'spot2' AND id_market_data = {spot2_id}
                      AND city = 'mexico'
                """
            elif propiedades_id:
                query = f"""
                    SELECT
                        id_market_data,
                        url,
                        property_type,
                        buy_price_clean,
                        buy_price_per_m2,
                        rent_price_clean,
                        rent_price_per_m2,
                        property_dimension_clean as total_area_clean,
                        'propiedades' as source
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE source = 'propiedades' AND id_market_data = {propiedades_id}
                      AND city = 'mexico'
                """
            else:
                # No valid ID
                query = "SELECT NULL WHERE 1=0"
        
        return query
    
    @staticmethod
    def get_property_market_info_legacy_query(spot2_id, inmuebles24_id, propiedades_id, city='mexico'):
        """
        Legacy query method for backward compatibility
        Uses individual tables when combined table is not available
        """
        if inmuebles24_id:
            if city == 'queretaro' or city == 'el_marques':
                # QRO inmuebles24 query
                query = f"""
                    SELECT
                        "id_market_data_inmuebles24" AS "id_market_data_inmuebles24",
                        "title" AS "title",
                        "description" AS "description",
                        "rent_price" AS "rent_price",
                        "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price",
                        "publication_date" AS "publication_date",
                        "parking_lot" AS "parking_lot",
                        "bathrooms" AS "bathrooms",
                        "bedrooms" AS "bedrooms",
                        "age" AS "age",
                        "pictures" AS "pictures",
                        "property_type" AS "property_type",
                        "operation_type" AS "operation_type",
                        "property_dimension" AS "total_area",
                        "property_dimension_clean" AS "total_area_clean",
                        "zone" AS "zone",
                        "city" AS "city",
                        "address" AS "address",
                        "url" AS "url",
                        "amenities" AS "amenities"
                    FROM blackprint_db_prd.presentation.dim_market_data_inmuebles24_qro
                    WHERE id_market_data_inmuebles24 = {inmuebles24_id}
                """
            else:
                # Mexico inmuebles24 query
                query = f"""
                    SELECT
                        "id_market_data_inmuebles24" AS "id_market_data_inmuebles24",
                        "title" AS "title",
                        "description" AS "description",
                        "rent_price" AS "rent_price",
                        "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price",
                        "publication_date" AS "publication_date",
                        "parking_lot" AS "parking_lot",
                        "bathrooms" AS "bathrooms",
                        "bedrooms" AS "bedrooms",
                        "age" AS "age",
                        "pictures" AS "pictures",
                        "property_type" AS "property_type",
                        "operation_type" AS "operation_type",
                        "property_dimension" AS "total_area",
                        "property_dimension_clean" AS "total_area_clean",
                        "zone" AS "zone",
                        "city" AS "city",
                        "address" AS "address",
                        "url" AS "url",
                        "amenities" AS "amenities"
                    FROM blackprint_db_prd.presentation.dim_market_data_inmuebles24
                    WHERE id_market_data_inmuebles24 = {inmuebles24_id}
                """
        elif spot2_id:
            if city == 'queretaro' or city == 'el_marques':
                # QRO spot2 query
                query = f"""
                    SELECT
                        "id_market_data_spot2" AS "id_market_data_spot2",
                        "title" AS "title",
                        "address" AS "address",
                        "street_address" AS "street_address",
                        "city" AS "city",
                        "zip_code" AS "zip_code",
                        "description" AS "description",
                        "operation_type" AS "operation_type",
                        "rent_price" AS "rent_price",
                        "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price",
                        "property_type" AS "property_type",
                        "total_area" AS "total_area",
                        "total_area_clean" AS "total_area_clean",
                        "amenities" AS "amenities",
                        "pictures" AS "pictures",
                        "url" AS "url",
                        "parking_spaces" AS "parking_spaces",
                        "condition" AS "condition",
                        "date_published" AS "publication_date"
                    FROM blackprint_db_prd.presentation.dim_market_data_spot2_qro
                    WHERE id_market_data_spot2 = {spot2_id}
                """
            else:
                # Mexico spot2 query
                query = f"""
                    SELECT
                        "id_market_data_spot2" AS "id_market_data_spot2",
                        "title" AS "title",
                        "address" AS "address",
                        "street_address" AS "street_address",
                        "city" AS "city",
                        "zip_code" AS "zip_code",
                        "description" AS "description",
                        "operation_type" AS "operation_type",
                        "rent_price" AS "rent_price",
                        "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "buy_price" AS "buy_price",
                        "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "maintenance_price" AS "maintenance_price",
                        "property_type" AS "property_type",
                        "total_area" AS "total_area",
                        "total_area_clean" AS "total_area_clean",
                        "amenities" AS "amenities",
                        "pictures" AS "pictures",
                        "url" AS "url",
                        "parking_spaces" AS "parking_spaces",
                        "condition" AS "condition",
                        "date_published" AS "publication_date"
                    FROM blackprint_db_prd.presentation.dim_market_data_spot2
                    WHERE id_market_data_spot2 = {spot2_id}
                """
        elif propiedades_id:
            if city == 'queretaro' or city == 'el_marques':
                # QRO propiedades - table doesn't exist, return empty result
                query = """
                    SELECT
                        NULL AS "id_market_data_propiedades",
                        NULL AS "url",
                        NULL AS "property_type",
                        NULL AS "description",
                        NULL AS "buy_price",
                        NULL AS "buy_price_usd",
                        NULL AS "buy_price_clean",
                        NULL AS "buy_price_per_m2",
                        NULL AS "rent_price",
                        NULL AS "rent_price_usd",
                        NULL AS "rent_price_clean",
                        NULL AS "rent_price_per_m2",
                        NULL AS "size",
                        NULL AS "total_area_clean",
                        NULL AS "postal_code",
                        NULL AS "street_address",
                        NULL AS "bedrooms",
                        NULL AS "bathrooms",
                        NULL AS "geometry_coords"
                    WHERE 1=0
                """
            else:
                # Mexico propiedades query
                query = f"""
                    SELECT
                        "id_market_data_propiedades" AS "id_market_data_propiedades",
                        "url" AS "url",
                        "property_type" AS "property_type",
                        "description" AS "description",
                        "buy_price" AS "buy_price",
                        "buy_price_usd" AS "buy_price_usd",
                        "buy_price_clean" AS "buy_price_clean",
                        "buy_price_per_m2" AS "buy_price_per_m2",
                        "rent_price" AS "rent_price",
                        "rent_price_usd" AS "rent_price_usd",
                        "rent_price_clean" AS "rent_price_clean",
                        "rent_price_per_m2" AS "rent_price_per_m2",
                        "size" AS "size",
                        "total_area_clean" AS "total_area_clean",
                        "postal_code" AS "postal_code",
                        "street_address" AS "street_address",
                        "bedrooms" AS "bedrooms",
                        "bathrooms" AS "bathrooms",
                        "geometry_coords" AS "geometry_coords"
                    FROM blackprint_db_prd.presentation.dim_market_data_propiedades
                    WHERE id_market_data_propiedades = {propiedades_id}
                """
        else:
            # No valid ID provided
            query = "SELECT NULL WHERE 1=0"
        
        return query
    
    @staticmethod
    def get_market_summary_stats_query(city='mexico'):
        """
        Get market summary statistics for a city
        Uses combined table for better performance
        """
        if city == 'queretaro' or city == 'el_marques':
            query = """
                SELECT
                    COUNT(*) as total_properties,
                    COUNT(DISTINCT property_type) as unique_property_types,
                    COUNT(DISTINCT operation_type) as unique_operation_types,
                    AVG(rent_price_clean) as avg_rent_price,
                    AVG(buy_price_clean) as avg_buy_price,
                    AVG(rent_price_per_m2) as avg_rent_per_m2,
                    AVG(buy_price_per_m2) as avg_buy_per_m2
                FROM blackprint_db_prd.presentation.dim_market_data_combined
                WHERE city IN ('queretaro', 'el_marques')
                  AND (rent_price_clean IS NOT NULL OR buy_price_clean IS NOT NULL)
            """
        else:
            query = """
                SELECT
                    COUNT(*) as total_properties,
                    COUNT(DISTINCT property_type) as unique_property_types,
                    COUNT(DISTINCT operation_type) as unique_operation_types,
                    AVG(rent_price_clean) as avg_rent_price,
                    AVG(buy_price_clean) as avg_buy_price,
                    AVG(rent_price_per_m2) as avg_rent_per_m2,
                    AVG(buy_price_per_m2) as avg_buy_per_m2
                FROM blackprint_db_prd.presentation.dim_market_data_combined
                WHERE city = 'mexico'
                  AND (rent_price_clean IS NOT NULL OR buy_price_clean IS NOT NULL)
            """
        
        return query
    
    @staticmethod
    def get_property_type_distribution_query(city='mexico'):
        """
        Get distribution of properties by property type
        Uses combined table for better performance
        """
        if city == 'queretaro' or city == 'el_marques':
            query = """
                SELECT
                    property_type,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM blackprint_db_prd.presentation.dim_market_data_combined
                WHERE city IN ('queretaro', 'el_marques')
                  AND property_type IS NOT NULL
                  AND property_type != ''
                  AND property_type != 'NULL'
                GROUP BY property_type
                ORDER BY count DESC
            """
        else:
            query = """
                SELECT
                    property_type,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM blackprint_db_prd.presentation.dim_market_data_combined
                WHERE city = 'mexico'
                  AND property_type IS NOT NULL
                  AND property_type != ''
                  AND property_type != 'NULL'
                GROUP BY property_type
                ORDER BY count DESC
            """
        
        return query
    
    @staticmethod
    def get_price_range_distribution_query(city='mexico', price_type='buy'):
        """
        Get distribution of properties by price range
        Uses combined table for better performance
        """
        if city == 'queretaro' or city == 'el_marques':
            if price_type == 'buy':
                query = """
                    SELECT
                        CASE 
                            WHEN buy_price_clean < 1000000 THEN 'Under 1M'
                            WHEN buy_price_clean < 2000000 THEN '1M - 2M'
                            WHEN buy_price_clean < 5000000 THEN '2M - 5M'
                            WHEN buy_price_clean < 10000000 THEN '5M - 10M'
                            ELSE 'Over 10M'
                        END as price_range,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE city IN ('queretaro', 'el_marques')
                      AND buy_price_clean IS NOT NULL
                      AND buy_price_clean > 0
                    GROUP BY 
                        CASE 
                            WHEN buy_price_clean < 1000000 THEN 'Under 1M'
                            WHEN buy_price_clean < 2000000 THEN '1M - 2M'
                            WHEN buy_price_clean < 5000000 THEN '2M - 5M'
                            WHEN buy_price_clean < 10000000 THEN '5M - 10M'
                            ELSE 'Over 10M'
                        END
                    ORDER BY 
                        CASE price_range
                            WHEN 'Under 1M' THEN 1
                            WHEN '1M - 2M' THEN 2
                            WHEN '2M - 5M' THEN 3
                            WHEN '5M - 10M' THEN 4
                            ELSE 5
                        END
                """
            else:  # rent
                query = """
                    SELECT
                        CASE 
                            WHEN rent_price_clean < 5000 THEN 'Under 5K'
                            WHEN rent_price_clean < 10000 THEN '5K - 10K'
                            WHEN rent_price_clean < 20000 THEN '10K - 20K'
                            WHEN rent_price_clean < 50000 THEN '20K - 50K'
                            ELSE 'Over 50K'
                        END as price_range,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE city IN ('queretaro', 'el_marques')
                      AND rent_price_clean IS NOT NULL
                      AND rent_price_clean > 0
                    GROUP BY 
                        CASE 
                            WHEN rent_price_clean < 5000 THEN 'Under 5K'
                            WHEN rent_price_clean < 10000 THEN '5K - 10K'
                            WHEN rent_price_clean < 20000 THEN '10K - 20K'
                            WHEN rent_price_clean < 50000 THEN '20K - 50K'
                            ELSE 'Over 50K'
                        END
                    ORDER BY 
                        CASE price_range
                            WHEN 'Under 5K' THEN 1
                            WHEN '5K - 10K' THEN 2
                            WHEN '10K - 20K' THEN 3
                            WHEN '20K - 50K' THEN 4
                            ELSE 5
                        END
                """
        else:
            if price_type == 'buy':
                query = """
                    SELECT
                        CASE 
                            WHEN buy_price_clean < 1000000 THEN 'Under 1M'
                            WHEN buy_price_clean < 2000000 THEN '1M - 2M'
                            WHEN buy_price_clean < 5000000 THEN '2M - 5M'
                            WHEN buy_price_clean < 10000000 THEN '5M - 10M'
                            ELSE 'Over 10M'
                        END as price_range,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE city = 'mexico'
                      AND buy_price_clean IS NOT NULL
                      AND buy_price_clean > 0
                    GROUP BY 
                        CASE 
                            WHEN buy_price_clean < 1000000 THEN 'Under 1M'
                            WHEN buy_price_clean < 2000000 THEN '1M - 2M'
                            WHEN buy_price_clean < 5000000 THEN '2M - 5M'
                            WHEN buy_price_clean < 10000000 THEN '5M - 10M'
                            ELSE 'Over 10M'
                        END
                    ORDER BY 
                        CASE price_range
                            WHEN 'Under 1M' THEN 1
                            WHEN '1M - 2M' THEN 2
                            WHEN '2M - 5M' THEN 3
                            WHEN '5M - 10M' THEN 4
                            ELSE 5
                        END
                """
            else:  # rent
                query = """
                    SELECT
                        CASE 
                            WHEN rent_price_clean < 5000 THEN 'Under 5K'
                            WHEN rent_price_clean < 10000 THEN '5K - 10K'
                            WHEN rent_price_clean < 20000 THEN '10K - 20K'
                            WHEN rent_price_clean < 50000 THEN '20K - 50K'
                            ELSE 'Over 50K'
                        END as price_range,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM blackprint_db_prd.presentation.dim_market_data_combined
                    WHERE city = 'mexico'
                      AND rent_price_clean IS NOT NULL
                      AND rent_price_clean > 0
                    GROUP BY 
                        CASE 
                            WHEN rent_price_clean < 5000 THEN 'Under 5K'
                            WHEN rent_price_clean < 10000 THEN '5K - 10K'
                            WHEN rent_price_clean < 20000 THEN '10K - 20K'
                            WHEN rent_price_clean < 50000 THEN '20K - 50K'
                            ELSE 'Over 50K'
                        END
                    ORDER BY 
                        CASE price_range
                            WHEN 'Under 5K' THEN 1
                            WHEN '5K - 10K' THEN 2
                            WHEN '10K - 20K' THEN 3
                            WHEN '20K - 50K' THEN 4
                            ELSE 5
                        END
                """
        
        return query 