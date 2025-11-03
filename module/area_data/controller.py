"""Base classes for area data operations."""

import logging
from . import query
from psycopg2.extras import RealDictCursor
from utils.dbUtils import Database, RedshiftDatabase
from abc import ABC
logger = logging.getLogger(__name__)


class AbstractAreaData(ABC):
    """Base class for area data operations."""
    
    def __init__(self):
        """Initialize the AbstractAreaData class."""
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.redshift_connection = self.redshift_db.connect()
        self.cursor = self.redshift_connection.cursor(cursor_factory=RealDictCursor)
        pass
    
    def get_data(self, boundary):
        """Get data for the selected area."""
        raise NotImplementedError("Subclasses must implement get_data")


class TotalPopulation(AbstractAreaData):
    def __init__(self):
        super().__init__()
    
    def get_data(self, boundary):
        q = query.get_total_population_query(boundary)
        self.cursor.execute(q)
        data = self.cursor.fetchall()
        return data[0]['total_population'] 


class DemographicsAreaData(AbstractAreaData) :
    """Class for demographic data operations."""
    
    def __init__(self):
        """Initialize the Demographics class."""
        super().__init__()
    
    def get_data(self, boundary):
        """Get demographic data for the selected area (table: 'block' or 'locality')."""
        q = query.build_demographics_query(boundary)
        redshift_connection = self.redshift_db.connect()
        cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(q)
        data = cursor.fetchall()
        cursor.close()
        redshift_connection.close()
        return data 


class SocioeconomicAreaData(AbstractAreaData) :
    """Class for socio-economic data operations."""
    
    def __init__(self):
        """Initialize the Socioeconomic class."""
        super().__init__()
    
    def get_data(self, boundary):
        """Get socio-economic data for the selected area."""
        q = query.build_socioeconomic_query(boundary)
        print("socioeconomic query",q)
        redshift_connection = self.redshift_db.connect()
        cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(q)
        data = cursor.fetchall()
        print("socioeconomic data",data)
        cursor.close()
        redshift_connection.close()
        return data 
    
class TrafficAreaData(AbstractAreaData) :
    """Class for traffic data operations."""
    def __init__(self):
        super().__init__()
    
    def get_data(self, boundary):
        """Get traffic data for the selected area."""
        user_types = ['vehiculo', 'peaton', 'estacionario']
        total_user = 0
        traffic_data = {}
        for user_type in user_types:
            q = query.build_traffic_summary_query(boundary,user_type)
            self.cursor.execute(q)
            data = self.cursor.fetchall()
            print("data",data)
            traffic_data[user_type] = data[0]['total_users']
            total_user += data[0]['total_users']
        traffic_data['total_users'] = total_user
        #to get h3 traffic level data 
        h3_traffic_query = query.build_h3_traffic_summary_query(boundary)
        self.cursor.execute(h3_traffic_query)
        h3_data = self.cursor.fetchall()
        print("h3_data",h3_data)
        print("traffic_data",traffic_data)
        
        return traffic_data , h3_data
    
    
class TrafficByHourAreaData(AbstractAreaData) :
    """Class for traffic by hour data operations."""
    def __init__(self):
        super().__init__()
    
    def get_data(self, boundary,user_type=None):
        """Get traffic by hour data for the selected area."""
        q = query.build_traffic_by_hour_query(boundary,user_type)
        self.cursor.execute(q)
        data = self.cursor.fetchall()
        print("user_type",user_type,"data",data)
        return data[0]
    
class TrafficByDayAreaData(AbstractAreaData) :
    """Class for traffic by day data operations."""
    def __init__(self):
        super().__init__()
    
    def get_data(self, boundary,user_type=None):
        """Get traffic by day data for the selected area."""
        q = query.build_traffic_by_day_query(boundary,user_type)
        self.cursor.execute(q)
        data = self.cursor.fetchall()
        print("user_type",user_type,"daydata",data)
        return data[0]



class PoisAreaData(AbstractAreaData) :
    """Class for POIs data operations."""
    
    def __init__(self):
        """Initialize the Pois class."""
        super().__init__()
    
    def get_data(self, boundary):
        """Get POIs data for the selected area."""
        q = query.build_pois_query(boundary)
        self.cursor.execute(q)
        data = self.cursor.fetchall()
        return data 
