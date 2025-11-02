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
        """Get socio-economic data for the selected area (table: 'ageb' or 'locality')."""
        q = query.build_socioeconomic_query_controller(boundary)
        redshift_connection = self.redshift_db.connect()
        cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(q)
        data = cursor.fetchall()
        cursor.close()
        redshift_connection.close()
        return data 


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
