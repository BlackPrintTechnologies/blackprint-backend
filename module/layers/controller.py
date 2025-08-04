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
            # QRO table with available columns from v_qro_column.txt
            query = f'''
                select   
                id_stg_demographic_socioeconomic_qro as fid,
                centroid,
                is_on_market,
                ids_market_data_spot2,
                ids_market_data_inmuebles24,
                geometry_type,
                bbox,
                h3_indexes,
                -- QRO doesn't have these Mexico columns, so we'll use NULL or similar values
                NULL as street_address,
                NULL as total_surface_area,
                NULL as total_construction_area,
                NULL as property_type_inmuebles24,
                NULL as year_built,
                NULL as special_facilities,
                NULL as unit_land_value,
                NULL as land_value,
                NULL as key_vus,
                niv_predom as predominant_level,
                tot_vivien as total_houses,
                NULL as locality_size,
                NULL as floor_levels,
                NULL as open_space,
                NULL as id_land_use,
                cve_mun as id_municipality,
                NULL as id_city_blocks,
                NULL as height,
                NULL as cos,
                NULL as cus,
                NULL as min_housing
                from blackprint_db_prd.data_product.v_qro
                WHERE 
                (is_on_market = 'On Market')
                -- For QRO, we'll filter based on available market data
                AND (ids_market_data_spot2 IS NOT NULL OR ids_market_data_inmuebles24 IS NOT NULL)
                -- Filter for specific municipalities: El Marqués, Querétaro, and Corregidora
                AND nom_mun IN ('El Marqués', 'Querétaro', 'Corregidora')
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
