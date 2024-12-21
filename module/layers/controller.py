from utils.dbUtils import RedshiftDatabase
from utils.responseUtils import Response
from psycopg2.extras import RealDictCursor


class BrandController: 
    def __init__(self) :
        self.db = RedshiftDatabase()

    def get_brands(self, requested_polygon) :
        connection = None
        cursor = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = f'''SELECT brand, ST_AsText(geometry_coords) AS geometry_wkt
                                        FROM blackprint_db_prd.presentation.dim_places
                                        WHERE brand IS NOT NULL
                                        AND ST_Intersects(
                                            geometry_coords,
                                            ST_GeomFromText('POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))', 4326)
                                        )'''
            print("query=====>", query)

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