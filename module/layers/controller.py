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
        if city == 'queretaro' or city == 'el_marques':
            # QRO layer with market-data geometry (via LEFT JOIN) and safe CSV id matching
            query = f'''
                select   
                v.id_stg_demographic_socioeconomic_qro as fid,
                ST_AsGeoJSON(
                  CASE
                    WHEN mdc.geometry_coords IS NULL THEN NULL
                    WHEN ST_SRID(mdc.geometry_coords) = 4326 THEN mdc.geometry_coords
                    WHEN ST_SRID(mdc.geometry_coords) = 0 OR ST_SRID(mdc.geometry_coords) IS NULL THEN ST_Transform(ST_SetSRID(mdc.geometry_coords, 32614), 4326)
                    ELSE ST_Transform(mdc.geometry_coords, 4326)
                  END
                ) AS centroid,
                v.is_on_market,
                v.ids_market_data_spot2,
                v.ids_market_data_inmuebles24,
                v.geometry_type,
                v.bbox,
                v.h3_indexes,
                -- QRO doesn't have these Mexico columns, so we'll use NULL or similar values
                NULL as street_address,
                NULL as total_surface_area,
                NULL as total_construction_area,
                mdc.property_type as property_type_inmuebles24,
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
                from blackprint_db_prd.data_product.v_qro v
                left join blackprint_db_prd.presentation.dim_market_data_combined mdc
                  on (
                       (mdc.source = 'spot2'
                        and v.ids_market_data_spot2 is not null and v.ids_market_data_spot2 <> ''
                        and (',' || replace(v.ids_market_data_spot2, ' ', '') || ',') like '%,' || cast(mdc.id_market_data as varchar) || ',%')
                    or (mdc.source = 'inmuebles24'
                        and v.ids_market_data_inmuebles24 is not null and v.ids_market_data_inmuebles24 <> ''
                        and (',' || replace(v.ids_market_data_inmuebles24, ' ', '') || ',') like '%,' || cast(mdc.id_market_data as varchar) || ',%')
                  )
                WHERE 
                (v.is_on_market = 'On Market')
                -- Filter for specific municipalities: El Marqués, Querétaro, and Corregidora
                AND v.nom_mun IN ('El Marqués', 'Querétaro', 'Corregidora')
                -- Also filter by city in dim_market_data_combined table
                AND mdc.city IN ('El Marqués', 'Querétaro', 'Corregidora')
                AND mdc.geometry_coords IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY v.id_stg_demographic_socioeconomic_qro
                    ORDER BY CASE WHEN mdc.source = 'inmuebles24' THEN 1 WHEN mdc.source = 'spot2' THEN 2 ELSE 3 END
                ) = 1
                '''
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
        return query
    
    # Remove @cache_response decorator for now - will implement city-aware caching manually
    def get_properties_layer_data(self, city='mexico'):
        connection = None
        resp = None
        try :
            print(f"get_properties_layer_data for city: {city}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.get_property_query(city=city)
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            resp =  Response.success(data={"response": res})
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
    
    def get_property_layer(self, city='mexico'):
        """Get property layer data for the specified city."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Starting get_property_layer for city: {city}")
        connection = None
        cursor = None
        resp = None
        try:
            logger.info("Connecting to database...")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            logger.info("Generating property query...")
            query = self.get_property_query(city=city)
            logger.info(f"Generated query length: {len(query)}")
            logger.info(f"Query preview (first 500 chars): {query[:500]}...")
            
            logger.info("Executing query...")
            cursor.execute(query)
            logger.info("Query executed successfully")
            
            logger.info("Committing transaction...")
            connection.commit()
            logger.info("Transaction committed")
            
            logger.info("Fetching results...")
            res = cursor.fetchall()
            logger.info(f"Property Layer Results Count: {len(res)}")
            
            if len(res) > 0:
                logger.info(f"First result keys: {list(res[0].keys())}")
                logger.info(f"Sample result: {dict(res[0])}")
            
            resp = Response.success(data={"response": res})
            logger.info("Successfully created response")
            
        except Exception as e:
            logger.error(f"Property Layer Error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            if connection:
                logger.info("Rolling back transaction...")
                connection.rollback()
            
            resp = Response.internal_server_error(message=str(e))
            logger.error(f"Created error response: {resp}")
            
        finally:
            if cursor:
                logger.info("Closing cursor...")
                cursor.close()
            if connection:
                logger.info("Disconnecting from database...")
                self.db.disconnect(connection)
            
            logger.info(f"Returning response with status: {resp[1] if isinstance(resp, tuple) else 'Unknown'}")
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
