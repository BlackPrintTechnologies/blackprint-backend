from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response


class PropertyController :
    def __init__(self):
        self.db = Database()
    
    def get_properties(self):
        pass

    def get_property_market_info(self, fid):
        connection = None
        cursor = None
        try :
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            query = f'select '

        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def get_property_pois(self):
        pass

    def get_property_traffic(self):
        pass

    def get_property_demographic(self):
        pass