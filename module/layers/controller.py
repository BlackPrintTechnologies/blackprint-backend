from utils.dbUtils import RedshiftDatabase
from utils.responseUtils import Response
from psycopg2.extras import RealDictCursor
import json
import h3

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
                        SELECT brand, geometry_wkt FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''

        if catchment == '1000':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_1km FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_1km FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',')
                        )
                        SELECT brand, geometry_wkt FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''

        if catchment == '5':
            query = f'''WITH split_values AS (
                        SELECT SPLIT_PART((SELECT ids_pois_front FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',', n)::INTEGER as value
                        FROM numbers
                        WHERE n <= f_count_elements((SELECT ids_pois_front FROM blackprint_db_prd.data_product.v_parcel_v3 WHERE fid = {fid}), ',')
                        )
                        SELECT brand, geometry_wkt FROM blackprint_db_prd.presentation.dim_places_v2
                        WHERE id_place IN (SELECT value FROM split_values) AND brand IS NOT NULL;'''
        return query

    def get_brands(self, radius, fid): 
        connection = None
        cursor = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = self.get_brand_query(radius, fid)
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            print("res=====>", res)
            return Response.success(data={"response": res})
        except Exception as e :
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()

class TrafficController:
    def __init__(self) :
        self.db = RedshiftDatabase()

    def get_buffer_coordinates_from_db(self,lng, lat, radius):
        query = f'''
        SELECT ST_AsGeoJSON(
            ST_Buffer(
                ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326),
                {radius} / (111.32 * 1000)
            )
        ) AS buffer_geojson;
        '''
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchone()
            buffer_geojson = result['buffer_geojson']
            return buffer_geojson
        except Exception as e:
            print(f"Error: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()

    def get_h3_indices_within_buffer(self, buffer_geojson, resolution=11):
        buffer_coords = json.loads(buffer_geojson)['coordinates'][0]
        h3_indices = set()
        for coord in buffer_coords:
            h3_index = h3.latlng_to_cell(coord[1], coord[0], resolution)
            int_h3_index_from_hex = h3.str_to_int(h3_index)
            h3_indices.add(int_h3_index_from_hex)
        return h3_indices

    def get_mobility_data_within_buffer(self, lng, lat, radius):
        buffer_geojson = self.get_buffer_coordinates_from_db(lng, lat, radius)
        print(buffer_geojson,"buffer_geojson")
        if not buffer_geojson:
            return None

        h3_indices = self.get_h3_indices_within_buffer(buffer_geojson)
        h3_indices_str = ', '.join([str(index) for index in h3_indices])
        query = f'''
                SELECT * FROM blackprint_db_prd.presentation.dataset_mobility_data_v2 
                WHERE h3_index IN ({h3_indices_str});
                '''
        # Execute the query using your database connection
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            print("query=====>", query)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Exception as e:
            print(f"Error: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()