from utils.dbUtils import RedshiftDatabase, Database
from utils.responseUtils import Response
from psycopg2.extras import RealDictCursor
from utils.iconUtils import IconMapper
from utils.cacheUtlis import cache_response
import time

class PropertyLayerController:
    def __init__(self) :
        self.db = RedshiftDatabase()
        self.rdsDb = Database()
    
    @staticmethod
    def get_property_query(city='mexico'):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Generating property query for city: {city}")
        
        if city == 'queretaro' or city == 'el_marques':
            # QRO table with geometry from market data table - using the exact working query from DBeaver
            logger.info("Creating QRO query with geometry conversion")
            query = '''
                SELECT 
                    v.id_stg_demographic_socioeconomic_qro as fid,
                    -- Use converted market geometry for centroid when available, otherwise use original centroid
                    CASE 
                        WHEN mdc.geometry_coords IS NOT NULL THEN 
                            '{"type":"Point","coordinates":[' || 
                            CAST(ST_X(ST_Transform(mdc.geometry_coords, 4326)) AS VARCHAR) || ',' ||
                            CAST(ST_Y(ST_Transform(mdc.geometry_coords, 4326)) AS VARCHAR) || 
                            ']}'
                        ELSE v.centroid 
                    END as centroid,
                    v.is_on_market,
                    v.ids_market_data_spot2,
                    v.ids_market_data_inmuebles24,
                    v.geometry_type,
                    v.bbox,
                    v.h3_indexes,
                    -- Use market geometry if available, otherwise use centroid
                    -- Convert UTM geometry to GeoJSON format with longitude/latitude
                    CASE 
                        WHEN mdc.geometry_coords IS NOT NULL THEN 
                            '{"type":"Point","coordinates":[' || 
                            CAST(ST_X(ST_Transform(mdc.geometry_coords, 4326)) AS VARCHAR) || ',' ||
                            CAST(ST_Y(ST_Transform(mdc.geometry_coords, 4326)) AS VARCHAR) || 
                            ']}'
                         
                    END as geometry,
                    -- Market data columns
                    mdc.latitude,
                    mdc.longitude,
                    mdc.property_type,
                    mdc.operation_type,
                    mdc.rent_price_clean,
                    mdc.buy_price_clean,
                    mdc.property_dimension_clean,
                    mdc.title,
                    mdc.url,
                    -- QRO-specific columns
                    v.niv_predom as predominant_level,
                    v.tot_vivien as total_houses,
                    v.cve_mun as id_municipality,
                    -- QRO doesn't have these Mexico columns, so we'll use NULL or similar values
                    NULL as street_address,
                    COALESCE(mdc.property_dimension_clean, NULL) as total_surface_area,
                    NULL as total_construction_area,
                    COALESCE(mdc.property_type, NULL) as property_type_inmuebles24,
                    NULL as year_built,
                    NULL as special_facilities,
                    NULL as unit_land_value,
                    NULL as land_value,
                    NULL as key_vus,
                    v.niv_predom as predominant_level,
                    v.tot_vivien as total_houses,
                    NULL as locality_size,
                    NULL as floor_levels,
                    NULL as open_space,
                    NULL as id_land_use,
                    v.cve_mun as id_municipality,
                    NULL as id_city_blocks,
                    NULL as height,
                    NULL as cos,
                    NULL as cus,
                    NULL as min_housing
                FROM blackprint_db_prd.data_product.v_qro v
                LEFT JOIN blackprint_db_prd.presentation.dim_market_data_combined mdc 
                ON (
                    (mdc.source = 'spot2'
                     AND v.ids_market_data_spot2 IS NOT NULL AND v.ids_market_data_spot2 <> ''
                     AND (',' || REPLACE(CAST(v.ids_market_data_spot2 AS VARCHAR), ' ', '') || ',') LIKE '%,' || CAST(mdc.id_market_data AS VARCHAR) || ',%')
                 OR (mdc.source = 'inmuebles24'
                     AND v.ids_market_data_inmuebles24 IS NOT NULL AND v.ids_market_data_inmuebles24 <> ''
                     AND (',' || REPLACE(CAST(v.ids_market_data_inmuebles24 AS VARCHAR), ' ', '') || ',') LIKE '%,' || CAST(mdc.id_market_data AS VARCHAR) || ',%')
                  )
                WHERE 
                (v.is_on_market = 'On Market')
                AND (v.ids_market_data_spot2 IS NOT NULL OR v.ids_market_data_inmuebles24 IS NOT NULL)
                AND v.nom_mun IN ('El Marqués', 'Querétaro', 'Corregidora')
                '''
            logger.info(f"Generated QRO query with geometry conversion")
        else:
            # Mexico (existing query)
            query = f'''
                select   
                fid,
                centroid,
                street_address,
                is_on_market,
                total_surface_area,
                total_construction_area,
                property_type_inmuebles24,
                year_built,
                special_facilities,
                unit_land_value,
                land_value,
                key_vus,
                predominant_level,
                total_houses,
                locality_size,
                floor_levels,
                open_space,
                id_land_use,
                id_municipality,
                id_city_blocks,
                height,
                cos,
                cus,
                min_housing,
                ids_market_data_inmuebles24
                from blackprint_db_prd.data_product.v_parcel_v3
                WHERE 
                (is_on_market = 'On Market')
                AND (
                property_type_spot2 IN ('Local Comercial')
                OR property_type_inmuebles24 IN (
                    'Local comercial',
                    'Local en centro comercial',
                    'Terreno comercial'
                )
        )
                '''
            logger.info(f"Generated Mexico query")
        
        logger.info(f"Final query length: {len(query)} characters")
        return query
    
    # Remove @cache_response decorator for now - will implement city-aware caching manually
    def get_properties_layer_data(self, city='mexico'):
        import logging
        logger = logging.getLogger(__name__)
        
        connection = None
        resp = None
        try :
            logger.info(f"Starting get_properties_layer_data for city: {city}")
            
            # Log the query being executed
            query = self.get_property_query(city=city)
            logger.info(f"Generated query for city {city}: {query[:500]}...")  # Log first 500 chars
            
            connection = self.db.connect()
            logger.info("Database connection established successfully")
            
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            logger.info("Cursor created successfully")
            
            logger.info("Executing query...")
            cursor.execute(query)
            logger.info("Query executed successfully")
            
            connection.commit()
            logger.info("Transaction committed")
            
            res = cursor.fetchall()
            logger.info(f"Query returned {len(res)} rows")
            
            # Log sample data for debugging
            if res and len(res) > 0:
                sample_row = res[0]
                logger.info(f"Sample row keys: {list(sample_row.keys())}")
                logger.info(f"Sample row data: {dict(sample_row)}")
                
                # Check for geometry/centroid issues
                if 'geometry' in sample_row:
                    logger.info(f"Geometry field type: {type(sample_row['geometry'])}, value: {sample_row['geometry']}")
                if 'centroid' in sample_row:
                    logger.info(f"Centroid field type: {type(sample_row['centroid'])}, value: {sample_row['centroid']}")
                
                # Check for any null or problematic values
                for key, value in sample_row.items():
                    if value is None:
                        logger.warning(f"Null value found in field: {key}")
                    elif isinstance(value, str) and len(value) > 1000:
                        logger.info(f"Long string value in {key}: {value[:100]}...")
            else:
                logger.warning("No results returned from query")
            
            resp = Response.success(data={"response": res})
            logger.info("Response created successfully")
            
        except Exception as e :
            logger.error(f"Error in get_properties_layer_data for city {city}: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            if connection:
                connection.rollback()
                logger.info("Transaction rolled back")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.info("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.info("Database connection closed")
            logger.info(f"get_properties_layer_data completed for city: {city}")
            return resp

class BrandController: 
    def __init__(self) :
        self.db = RedshiftDatabase()
    
    @staticmethod
    def get_brand_query(catchment, fid, category_1=None, city="mexico" ):
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
                        SELECT brand, names_pri,  geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE id_place IN (SELECT value FROM split_values) ;'''

        elif catchment == '1000':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_1km FROM {parcel_table} WHERE {id_column} = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_1km FROM {parcel_table} WHERE {id_column} = {fid}), ',')
                        )
                        SELECT brand, names_pri,  geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE id_place IN (SELECT value FROM split_values) ;'''

        elif catchment == '50':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_front FROM {parcel_table} WHERE {id_column} = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_front FROM {parcel_table} WHERE {id_column} = {fid}), ',')
                        )
                        SELECT brand, names_pri,  geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE id_place IN (SELECT value FROM split_values) ;'''
        else :
            query = f'''SELECT brand, names_pri,  geometry_wkt, category_1 FROM {dim_places_table}
                        WHERE  category_1 = '{category_1}' ;'''

        return query
    
    # @cache_response(prefix='brands',expiration=3600)
    def get_brands(self, radius, fid, category=None, city="mexico"): 
        connection = None
        cursor = None
        resp = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.get_brand_query(radius, fid, category_1=category, city=city)
            print("query=====>", query)
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            # print("res=====>", res)
            #new chnage 
            # Add icon URLs to the results
            
            enhanced_results = []
            for result in res:
                result['icon_url'] = IconMapper.get_icon_url(result['category_1'])
                enhanced_results.append(result)
            #Filter by category
            if category:
                enhanced_results = [result for result in enhanced_results if result['category_1'] == category]
            print("enhanced_results=====>", enhanced_results)
            resp =  Response.success(data={"response": enhanced_results}) #
        except Exception as e :
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp
    
    def search_brands(self, brand_name):
        connection = None
        cursor = None
        resp = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = f'''SELECT distinct brand FROM blackprint_db_prd.presentation.dim_places where brand ilike '{brand_name}%'LIMIT 50'''
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            print("res=====>", res)
            resp =  Response.success(data=res)
        except Exception as e :
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp

class TrafficController:
    def __init__(self) :
        self.db = RedshiftDatabase()

    @staticmethod
    def get_traffic_query(catchment, fid, config_city=None):
        """Generates SQL query based on catchment radius and fid."""
        if config_city == "queretaro" or config_city == "el_marques":
            table_name = 'blackprint_db_prd.presentation.dataset_mobility_data_h3_qro'
            fid_column = 'id_stg_demographic_socioeconomic_qro'
        else:
            table_name = 'blackprint_db_prd.presentation.dataset_mobility_data_h3'
            fid_column = 'fid'
        query_map = {
            '500': f'''SELECT *
                        FROM {table_name} where {fid_column}={fid} and type='CIRCLE_500_METERS' ''',
            '1000': f'''SELECT *
                        FROM {table_name} where {fid_column}={fid} and type='CIRCLE_1000_METERS' ''',
            '5': f'''SELECT *
                        FROM {table_name} where {fid_column}={fid} and type='FRONT_OF_STORE' '''
        }
        return query_map.get(catchment)

    def get_mobility_data_within_buffer(self, fid, radius, config_city=None):
        query  = self.get_traffic_query(radius, fid, config_city=config_city)
        # Execute the query using your database connection
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            print("query=====>", query)
            cursor.execute(query)
            result = cursor.fetchall()
            resp =  Response.success(data={"response": result})
        except Exception as e:
            print(f"Error: {e}")
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)
            return resp
