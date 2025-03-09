from utils.dbUtils import RedshiftDatabase
from utils.responseUtils import Response
from psycopg2.extras import RealDictCursor
import time

class BrandController: 
    def __init__(self) :
        self.db = RedshiftDatabase()
    
    @staticmethod
    def get_brand_query(catchment, fid):
        if catchment == '500':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_500m FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_500m FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',')
                        )
                        SELECT brand, geometry_wkt, category_1 FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''

        if catchment == '1000':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_1km FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_1km FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',')
                        )
                        SELECT brand, geometry_wkt, category_1 FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''

        if catchment == '5':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_front FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_front FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',')
                        )
                        SELECT brand, geometry_wkt, category_1 FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''
        return query

    def get_brands(self, radius, fid): 
        connection = None
        cursor = None
        resp = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.get_brand_query(radius, fid)
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            print("res=====>", res)
            resp =  Response.success(data={"response": res})
        except Exception as e :
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp
    
    def search_brands(self, brand_name):
        connection = None
        cursor = None
        resp = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = f'''SELECT distinct brand FROM blackprint_db_prd.presentation.dim_places_v2 where brand ilike '{brand_name}%' '''
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
                self.db.disconnect()
            return resp

class TrafficController:
    def __init__(self) :
        self.db = RedshiftDatabase()

    @staticmethod
    def get_traffic_query(catchment, fid):
        """Generates SQL query based on catchment radius and fid."""
        query_map = {
            '500': f'''SELECT *
                        FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid={fid} and type='CIRCLE_500_METERS' ''',
            '1000': f'''SELECT *
                        FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid={fid} and type='CIRCLE_1000_METERS' ''',
            '5': f'''SELECT *
                        FROM blackprint_db_prd.presentation.dataset_mobility_data_h3 where fid={fid} and type='FRONT_OF_STORE' '''
        }
        return query_map.get(catchment)

    def get_mobility_data_within_buffer(self, fid, radius):
        query  = self.get_traffic_query(radius, fid)
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
                self.db.disconnect()
            return resp