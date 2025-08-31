import json
import logging
import traceback
from psycopg2.extras import RealDictCursor
from module.properties.query import QueryController
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase

logger = logging.getLogger(__name__)

class POIsController:
    def __init__(self):
        self.qc = QueryController()
        self.db = Database()
        self.redshift_connection = RedshiftDatabase()
        
        pass

    def get_pois(self, lat, lng, radius, config_city=None):
        pass

    def get_pois_hierarchy(self, config_city=None):
        try :
            pass
            # query = self.qc.get_pois_hierarchy_query(config_city)
            # cursor.execute(query)
            # result = cursor.fetchall()
            # return Response.success(data=result, message="Success")
        except Exception as e:
            logger.error(f"Error in get_pois_hierarchy: {e}")
            return Response.internal_server_error(message=str(e))
        