"""Base classes for area data operations."""

import logging
from . import query

logger = logging.getLogger(__name__)


class Demographics:
    """Class for demographic data operations."""
    
    def __init__(self):
        """Initialize the Demographics class."""
        pass
    
    def get_data(self, lng, lat, radius, geometry_column='geometry_coords', table='block'):
        """Get demographic data for the selected area (table: 'block' or 'locality')."""
        return query.build_demographics_query(lng, lat, radius, geometry_column, table)


class Socioeconomic:
    """Class for socio-economic data operations."""
    
    def __init__(self):
        """Initialize the Socioeconomic class."""
        pass
    
    def get_data(self, lng, lat, radius, geometry_column='geometry_coords', table='ageb'):
        """Get socio-economic data for the selected area (table: 'ageb' or 'locality')."""
        return query.build_socioeconomic_query(lng, lat, radius, geometry_column, table)

