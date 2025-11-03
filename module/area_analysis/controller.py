import json
import logging
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase
from utils.app_cache import set_in_cache, get_from_cache  # Uses LRUCache for fast in-memory caching
from module.area_analysis.query import AreaAnalysisQuery

logger = logging.getLogger(__name__)


#New area analysis for the area search

import copy
from decimal import Decimal
from module.area_data.controller import DemographicsAreaData , TrafficAreaData, TrafficByHourAreaData, TrafficByDayAreaData
from module.area_data.controller import TotalPopulation, SocioeconomicAreaData
from module.area_analysis.constants import DEMOGRAPHICS_DATA_FORMAT, TRAFFIC_DATA_FORMAT ,TRAFFIC_PATTERNS_FORMAT


class AbstractAreaAnalysisController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.redshift_connection = self.redshift_db.connect()
        self.cursor = self.redshift_connection.cursor(cursor_factory=RealDictCursor)
        self.qc = AreaAnalysisQuery()
    

    def get_boundary_from_coordinates(self, lat, lng, radius, city='queretaro'):
        """Get boundary from coordinates as WKT polygon (radius in meters)."""
        # Build buffer around point in meters using Web Mercator, then return as WKT in 4326
        query = self.qc.get_boundary_from_coordinates_query(lat, lng, radius, city)
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        return res.get('wkt') if res else None
    
    def get_municipality_info(self, lat, lng, city='queretaro'):
        """Get municipality info (code, name, population, boundary WKT) from coordinates."""
        query = self.qc.get_municipality_info_query(lat, lng, city)
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        return res.get('wkt') if res else None
    


class DemographicsAreaAnalysisController(AbstractAreaAnalysisController):
    """Controller for demographics area analysis."""
    DATA_FORMAT = DEMOGRAPHICS_DATA_FORMAT
    def __init__(self):
        super().__init__()
        
    def _to_float(self, value):
        """Convert Decimal/None to float."""
        if value is None:
            return 0.0
        return float(value) if not isinstance(value, float) else value
    
    def _to_dict(self, row):
        """Convert RealDictRow to dict with float conversion."""
        if not row:
            return {}
        return {k: self._to_float(v) for k, v in dict(row).items()}
    
    def _to_dict_socioeconomic(self, row):
        """Convert RealDictRow to dict with float conversion, preserving string fields."""
        if not row:
            return {}
        result = {}
        for k, v in dict(row).items():
            # Keep predominant_level as string
            if k == 'predominant_level' or isinstance(v, str):
                result[k] = v
            else:
                result[k] = self._to_float(v)
        return result

    def _pct(self, part, total):
        """Calculate percentage."""
        return round((part / total * 100), 1) if total > 0 else 0.0
    
    def _get_population_growth(self, pop_dict, years=[2000, 2005, 2010, 2020]):
        """Get population growth data from query results (using pre-calculated growth rates)."""
        pops = [pop_dict.get(f'population_{y}', 0) for y in years]
        
        # Get pre-calculated growth rates from query
        growth_rates = {
            2000: 0,  # No growth rate for first year
            2005: pop_dict.get('pop_growth_rate_2000_2005', 0) * 100,  # Convert to percentage
            2010: pop_dict.get('pop_growth_rate_2005_2010', 0) * 100,
            2020: pop_dict.get('pop_growth_rate_2010_2020', 0) * 100
        }
        
        growth = [
            {'year': y, 'growth_percentage': round(growth_rates.get(y, 0), 1)}
            for y in years
        ]
        
        growth_dict = {
            str(y): [
                int(pops[i]) if pops[i] else None, 
                growth[i]['growth_percentage']
            ] 
            for i, y in enumerate(years)
        }
        growth_dict['2015'] = [None, None]  # No 2015 data
        
        return growth, growth_dict
    
    def _get_age_pyramid(self, area_dict, total_pop):
        """Create age pyramid data."""
        age_ranges = [
            ('0-2', 'age_0_2_m', 'age_0_2_f'),
            ('3-5', 'age_3_5_m', 'age_3_5_f'),
            ('6-11', 'age_6_11_m', 'age_6_11_f'),
            ('12-14', 'age_12_14_m', 'age_12_14_f'),
            ('15-17', 'age_15_17_m', 'age_15_17_f'),
            ('18-24', 'age_18_24_m', 'age_18_24_f'),
        ]
        
        groups = [
            {
                'range': r,
                'male': int(area_dict.get(m, 0)),
                'female': int(area_dict.get(f, 0)),
                'male_percentage': self._pct(area_dict.get(m, 0), total_pop),
                'female_percentage': self._pct(area_dict.get(f, 0), total_pop)
            }
            for r, m, f in age_ranges
        ]
        
        # Add 65+ (approximate split)
        age_65 = area_dict.get('age_65_plus', 0)
        groups.append({
            'range': '65+',
            'male': int(age_65 / 2),
            'female': int(age_65 / 2),
            'male_percentage': self._pct(age_65 / 2, total_pop),
            'female_percentage': self._pct(age_65 / 2, total_pop)
        })
        
        # Calculate 25-65 as remainder
        total_accounted = sum(g.get('male', 0) + g.get('female', 0) for g in groups)
        male_accounted = sum(g.get('male', 0) for g in groups)
        female_accounted = sum(g.get('female', 0) for g in groups)
        area_male = area_dict.get('male_population', 0)
        area_female = area_dict.get('female_population', 0)
        
        groups.append({
            'range': '25-65',
            'male': int(area_male - male_accounted),
            'female': int(area_female - female_accounted),
            'male_percentage': self._pct(area_male - male_accounted, total_pop),
            'female_percentage': self._pct(area_female - female_accounted, total_pop)
        })
        
        return {'age_groups': groups, 'total_population': float(total_pop)}
    
    def _populate_demographics(self, area_data, municipality_data, lat, lng, radius):
        """Populate demographics format with actual data."""
        data = copy.deepcopy(DEMOGRAPHICS_DATA_FORMAT)
        area = self._to_dict(area_data[0] if area_data else {})
        mun = self._to_dict(municipality_data[0] if municipality_data else {})
        
        # Extract key values
        area_pop = area.get('total_population', 0)
        mun_pop = mun.get('total_population', 0)
        area_male, area_female = area.get('male_population', 0), area.get('female_population', 0)
        mun_male, mun_female = mun.get('male_population', 0), mun.get('female_population', 0)
        
        # Summary
        data['summary'].update({
            'area_km2': area.get('area_km2', 0),
            'population': area_pop,
            'population_density': area.get('population_density', 0),
            'population_density_formatted': f"{area.get('population_density', 0)} persons / km²",
            'center_point': {'lat': float(lat), 'lng': float(lng)},
            'radius_meters': int(radius)
        })
        
        # Demographics
        data['demographics'].update({
            'total_population': area_pop,
            'male_population': area_male,
            'female_population': area_female,
            'male_percentage': self._pct(area_male, area_pop),
            'female_percentage': self._pct(area_female, area_pop),
            'total_households': area.get('total_households', 0),
            'average_household_size': area.get('average_people_per_household', 0)
        })
        
        # Detailed data - general (using same structure for block/colonia/alcaldia)
        levels = ['block', 'colonia', 'alcaldia']
        for level in levels:
            data['detailed_data']['general'][level].update({
                'total_household': area.get('total_households', 0) if level != 'alcaldia' else mun.get('total_households', 0),
                'average_household_size': area.get('average_people_per_household', 0) if level != 'alcaldia' else mun.get('average_people_per_household', 0)
            })
            data['detailed_data']['population'][level].update({
                'total_population': area_pop if level != 'alcaldia' else mun_pop,
                'male_population': area_male if level != 'alcaldia' else mun_male,
                'female_population': area_female if level != 'alcaldia' else mun_female
            })
        
        # Population growth
        area_growth_list, area_growth_dict = self._get_population_growth(area)
        mun_growth_list, mun_growth_dict = self._get_population_growth(mun)
        
        data['population_growth_2024'] = {
            'area': area_growth_list,
            'municipality': mun_growth_list,
            'years': [2000, 2005, 2010, 2020]
        }
        
        for level in ['block', 'colonia']:
            data['detailed_data']['population_growth'][level] = area_growth_dict
        data['detailed_data']['population_growth']['alcaldia'] = mun_growth_dict
        
        # Age pyramid
        data['age_pyramid_2024'] = self._get_age_pyramid(area, area_pop)
        
        # Socioeconomic data
        catchment = self.get_boundary_from_coordinates(lat, lng, radius)
        municipality_wkt = self.get_municipality_info(lat, lng)
        
        if catchment:
            area_socio_data = SocioeconomicAreaData().get_data(catchment)
            area_socio = self._to_dict_socioeconomic(area_socio_data[0] if area_socio_data else {})
        else:
            area_socio = {}
        
        if municipality_wkt:
            mun_socio_data = SocioeconomicAreaData().get_data(municipality_wkt)
            mun_socio = self._to_dict_socioeconomic(mun_socio_data[0] if mun_socio_data else {})
        else:
            mun_socio = {}
        
        # Calculate total SES for percentage calculations
        area_total_ses = (
            area_socio.get('ses_ab', 0) + 
            area_socio.get('ses_c_plus', 0) + 
            area_socio.get('ses_c', 0) + 
            area_socio.get('ses_c_minus', 0) + 
            area_socio.get('ses_d_plus', 0) + 
            area_socio.get('ses_d', 0) + 
            area_socio.get('ses_e', 0)
        )
        
        mun_total_ses = (
            mun_socio.get('ses_ab', 0) + 
            mun_socio.get('ses_c_plus', 0) + 
            mun_socio.get('ses_c', 0) + 
            mun_socio.get('ses_c_minus', 0) + 
            mun_socio.get('ses_d_plus', 0) + 
            mun_socio.get('ses_d', 0) + 
            mun_socio.get('ses_e', 0)
        )
        
        # Populate socioeconomic data for each level
        for level in levels:
            if level != 'alcaldia':
                # Use area data for block and colonia
                data['detailed_data']['socio_economic_level'][level].update({
                    'ses_ab': self._to_float(area_socio.get('ses_ab', 0)),
                    'ses_c_plus': self._to_float(area_socio.get('ses_c_plus', 0)),
                    'ses_c': self._to_float(area_socio.get('ses_c', 0)),
                    'ses_c_minus': self._to_float(area_socio.get('ses_c_minus', 0)),
                    'ses_d_plus': self._to_float(area_socio.get('ses_d_plus', 0)),
                    'ses_d': self._to_float(area_socio.get('ses_d', 0)),
                    'ses_e': self._to_float(area_socio.get('ses_e', 0))
                })
            else:
                # Use municipality data for alcaldia
                data['detailed_data']['socio_economic_level'][level].update({
                    'ses_ab': self._to_float(mun_socio.get('ses_ab', 0)),
                    'ses_c_plus': self._to_float(mun_socio.get('ses_c_plus', 0)),
                    'ses_c': self._to_float(mun_socio.get('ses_c', 0)),
                    'ses_c_minus': self._to_float(mun_socio.get('ses_c_minus', 0)),
                    'ses_d_plus': self._to_float(mun_socio.get('ses_d_plus', 0)),
                    'ses_d': self._to_float(mun_socio.get('ses_d', 0)),
                    'ses_e': self._to_float(mun_socio.get('ses_e', 0))
                })
        
        # Populate socioeconomic_analysis section
        area_predominant_level = area_socio.get('predominant_level', '')
        mun_predominant_level = mun_socio.get('predominant_level', '')
        
        # Calculate percentage for predominant level (area)
        area_total = area_total_ses if area_total_ses > 0 else 1
        area_predominant_count = 0
        if area_predominant_level:
            # Map predominant level to SES count
            level_mapping = {
                'A/B': area_socio.get('ses_ab', 0),
                'C+': area_socio.get('ses_c_plus', 0),
                'C': area_socio.get('ses_c', 0),
                'C-': area_socio.get('ses_c_minus', 0),
                'D+': area_socio.get('ses_d_plus', 0),
                'D': area_socio.get('ses_d', 0),
                'E': area_socio.get('ses_e', 0)
            }
            area_predominant_count = level_mapping.get(area_predominant_level, 0)
        
        area_predominant_pct = self._pct(area_predominant_count, area_total)
        
        # Calculate percentage for predominant level (municipality)
        mun_total = mun_total_ses if mun_total_ses > 0 else 1
        mun_predominant_count = 0
        if mun_predominant_level:
            level_mapping_mun = {
                'A/B': mun_socio.get('ses_ab', 0),
                'C+': mun_socio.get('ses_c_plus', 0),
                'C': mun_socio.get('ses_c', 0),
                'C-': mun_socio.get('ses_c_minus', 0),
                'D+': mun_socio.get('ses_d_plus', 0),
                'D': mun_socio.get('ses_d', 0),
                'E': mun_socio.get('ses_e', 0)
            }
            mun_predominant_count = level_mapping_mun.get(mun_predominant_level, 0)
        
        mun_predominant_pct = self._pct(mun_predominant_count, mun_total)
        
        # Update predominant socioeconomic level
        data['socioeconomic_analysis']['predominant_socioeconomic_level'].update({
            'selected_area': {
                'level': area_predominant_level or '',
                'percentage': area_predominant_pct
            },
            'municipality': {
                'level': mun_predominant_level or '',
                'percentage': mun_predominant_pct
            }
        })
        
        # Populate households_per_level for selected area
        households_per_level = [
            {'level': 'A/B', 'households': int(area_socio.get('ses_ab', 0)), 'percentage': self._pct(area_socio.get('ses_ab', 0), area_total)},
            {'level': 'C+', 'households': int(area_socio.get('ses_c_plus', 0)), 'percentage': self._pct(area_socio.get('ses_c_plus', 0), area_total)},
            {'level': 'C', 'households': int(area_socio.get('ses_c', 0)), 'percentage': self._pct(area_socio.get('ses_c', 0), area_total)},
            {'level': 'C-', 'households': int(area_socio.get('ses_c_minus', 0)), 'percentage': self._pct(area_socio.get('ses_c_minus', 0), area_total)},
            {'level': 'D+', 'households': int(area_socio.get('ses_d_plus', 0)), 'percentage': self._pct(area_socio.get('ses_d_plus', 0), area_total)},
            {'level': 'D', 'households': int(area_socio.get('ses_d', 0)), 'percentage': self._pct(area_socio.get('ses_d', 0), area_total)},
            {'level': 'E', 'households': int(area_socio.get('ses_e', 0)), 'percentage': self._pct(area_socio.get('ses_e', 0), area_total)}
        ]
        
        data['socioeconomic_analysis']['households_per_level'] = households_per_level
        
        # Populate socioeconomic_income_analysis from SES data
        # Note: We don't have actual income amounts, but we can derive insights from SES levels
        area_total_households = area_socio.get('total_housing', 0) or area_total_ses
        mun_total_households = mun_socio.get('total_housing', 0) or mun_total_ses
        
        # Calculate total household income from 2024 SES income data
        area_total_income_2024 = (
            area_socio.get('ses_ab_2024', 0) +
            area_socio.get('ses_c_plus_2024', 0) +
            area_socio.get('ses_c_2024', 0) +
            area_socio.get('ses_c_minus_2024', 0) +
            area_socio.get('ses_d_plus_2024', 0) +
            area_socio.get('ses_d_2024', 0) +
            area_socio.get('ses_e_2024', 0)
        )
        
        mun_total_income_2024 = (
            mun_socio.get('ses_ab_2024', 0) +
            mun_socio.get('ses_c_plus_2024', 0) +
            mun_socio.get('ses_c_2024', 0) +
            mun_socio.get('ses_c_minus_2024', 0) +
            mun_socio.get('ses_d_plus_2024', 0) +
            mun_socio.get('ses_d_2024', 0) +
            mun_socio.get('ses_e_2024', 0)
        )
        
        # Calculate average household income
        area_avg_income = round(area_total_income_2024 / area_total_households, 2) if area_total_households > 0 else 0.0
        mun_avg_income = round(mun_total_income_2024 / mun_total_households, 2) if mun_total_households > 0 else 0.0
        
        # Income summary for selected area
        data['socioeconomic_income_analysis']['income_summary'].update({
            'total_households': int(area_total_households),
            'total_household_income': int(area_total_income_2024),
            'average_household_income': area_avg_income
        })
        
        # Income distribution by level (using current year 2024 data)
        income_distribution_levels = [
            {
                'level': 'A/B',
                'households': int(area_socio.get('ses_ab', 0)),
                'household_percentage': self._pct(area_socio.get('ses_ab', 0), area_total_households),
                'total_income': int(area_socio.get('ses_ab_2024', 0)),  # Historical income proxy
                'income_percentage': self._pct(area_socio.get('ses_ab_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'C+',
                'households': int(area_socio.get('ses_c_plus', 0)),
                'household_percentage': self._pct(area_socio.get('ses_c_plus', 0), area_total_households),
                'total_income': int(area_socio.get('ses_c_plus_2024', 0)),
                'income_percentage': self._pct(area_socio.get('ses_c_plus_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'C',
                'households': int(area_socio.get('ses_c', 0)),
                'household_percentage': self._pct(area_socio.get('ses_c', 0), area_total_households),
                'total_income': int(area_socio.get('ses_c_2024', 0)),
                'income_percentage': self._pct(area_socio.get('ses_c_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'C-',
                'households': int(area_socio.get('ses_c_minus', 0)),
                'household_percentage': self._pct(area_socio.get('ses_c_minus', 0), area_total_households),
                'total_income': int(area_socio.get('ses_c_minus_2024', 0)),
                'income_percentage': self._pct(area_socio.get('ses_c_minus_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'D+',
                'households': int(area_socio.get('ses_d_plus', 0)),
                'household_percentage': self._pct(area_socio.get('ses_d_plus', 0), area_total_households),
                'total_income': int(area_socio.get('ses_d_plus_2024', 0)),
                'income_percentage': self._pct(area_socio.get('ses_d_plus_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'D',
                'households': int(area_socio.get('ses_d', 0)),
                'household_percentage': self._pct(area_socio.get('ses_d', 0), area_total_households),
                'total_income': int(area_socio.get('ses_d_2024', 0)),
                'income_percentage': self._pct(area_socio.get('ses_d_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'E',
                'households': int(area_socio.get('ses_e', 0)),
                'household_percentage': self._pct(area_socio.get('ses_e', 0), area_total_households),
                'total_income': int(area_socio.get('ses_e_2024', 0)),
                'income_percentage': self._pct(area_socio.get('ses_e_2024', 0), sum([
                    area_socio.get('ses_ab_2024', 0),
                    area_socio.get('ses_c_plus_2024', 0),
                    area_socio.get('ses_c_2024', 0),
                    area_socio.get('ses_c_minus_2024', 0),
                    area_socio.get('ses_d_plus_2024', 0),
                    area_socio.get('ses_d_2024', 0),
                    area_socio.get('ses_e_2024', 0)
                ]))
            }
        ]
        
        data['socioeconomic_income_analysis']['income_distribution']['levels'] = income_distribution_levels
        
        # Historical trends - calculate growth between periods
        def calculate_growth(current, previous):
            """Calculate growth percentage between two values."""
            if previous and previous > 0:
                return round(((current - previous) / previous) * 100, 2)
            return 0.0
        
        historical_trends = []
        ses_levels = ['ab', 'c_plus', 'c', 'c_minus', 'd_plus', 'd', 'e']
        level_names = ['A/B', 'C+', 'C', 'C-', 'D+', 'D', 'E']
        
        for ses_key, level_name in zip(ses_levels, level_names):
            trends = {
                'level': level_name,
                'growth_2016_2018': int(area_socio.get(f'ses_{ses_key}_2018', 0) - area_socio.get(f'ses_{ses_key}_2016', 0)),
                'growth_2018_2020': int(area_socio.get(f'ses_{ses_key}_2020', 0) - area_socio.get(f'ses_{ses_key}_2018', 0)),
                'growth_2020_2022': int(area_socio.get(f'ses_{ses_key}_2022', 0) - area_socio.get(f'ses_{ses_key}_2020', 0)),
                'growth_2022_2024': int(area_socio.get(f'ses_{ses_key}_2024', 0) - area_socio.get(f'ses_{ses_key}_2022', 0)),
                'growth_pct_2016_2018': calculate_growth(area_socio.get(f'ses_{ses_key}_2018', 0), area_socio.get(f'ses_{ses_key}_2016', 0)),
                'growth_pct_2018_2020': calculate_growth(area_socio.get(f'ses_{ses_key}_2020', 0), area_socio.get(f'ses_{ses_key}_2018', 0)),
                'growth_pct_2020_2022': calculate_growth(area_socio.get(f'ses_{ses_key}_2022', 0), area_socio.get(f'ses_{ses_key}_2020', 0)),
                'growth_pct_2022_2024': calculate_growth(area_socio.get(f'ses_{ses_key}_2024', 0), area_socio.get(f'ses_{ses_key}_2022', 0))
            }
            historical_trends.append(trends)
        
        data['socioeconomic_income_analysis']['historical_trends']['growth_by_level'] = historical_trends
        
        # Municipality analysis
        data['socioeconomic_income_analysis']['municipality_analysis']['income_summary'].update({
            'total_households': int(mun_total_households),
            'total_household_income': int(mun_total_income_2024),
            'average_household_income': mun_avg_income
        })
        
        # Municipality income distribution
        mun_income_distribution_levels = [
            {
                'level': 'A/B',
                'households': int(mun_socio.get('ses_ab', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_ab', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_ab_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_ab_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'C+',
                'households': int(mun_socio.get('ses_c_plus', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_c_plus', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_c_plus_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_c_plus_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'C',
                'households': int(mun_socio.get('ses_c', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_c', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_c_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_c_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'C-',
                'households': int(mun_socio.get('ses_c_minus', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_c_minus', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_c_minus_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_c_minus_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'D+',
                'households': int(mun_socio.get('ses_d_plus', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_d_plus', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_d_plus_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_d_plus_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'D',
                'households': int(mun_socio.get('ses_d', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_d', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_d_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_d_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            },
            {
                'level': 'E',
                'households': int(mun_socio.get('ses_e', 0)),
                'household_percentage': self._pct(mun_socio.get('ses_e', 0), mun_total_households),
                'total_income': int(mun_socio.get('ses_e_2024', 0)),
                'income_percentage': self._pct(mun_socio.get('ses_e_2024', 0), sum([
                    mun_socio.get('ses_ab_2024', 0),
                    mun_socio.get('ses_c_plus_2024', 0),
                    mun_socio.get('ses_c_2024', 0),
                    mun_socio.get('ses_c_minus_2024', 0),
                    mun_socio.get('ses_d_plus_2024', 0),
                    mun_socio.get('ses_d_2024', 0),
                    mun_socio.get('ses_e_2024', 0)
                ]))
            }
        ]
        
        data['socioeconomic_income_analysis']['municipality_analysis']['income_distribution']['levels'] = mun_income_distribution_levels
        
        # Municipality historical trends
        mun_historical_trends = []
        for ses_key, level_name in zip(ses_levels, level_names):
            trends = {
                'level': level_name,
                'growth_2016_2018': int(mun_socio.get(f'ses_{ses_key}_2018', 0) - mun_socio.get(f'ses_{ses_key}_2016', 0)),
                'growth_2018_2020': int(mun_socio.get(f'ses_{ses_key}_2020', 0) - mun_socio.get(f'ses_{ses_key}_2018', 0)),
                'growth_2020_2022': int(mun_socio.get(f'ses_{ses_key}_2022', 0) - mun_socio.get(f'ses_{ses_key}_2020', 0)),
                'growth_2022_2024': int(mun_socio.get(f'ses_{ses_key}_2024', 0) - mun_socio.get(f'ses_{ses_key}_2022', 0)),
                'growth_pct_2016_2018': calculate_growth(mun_socio.get(f'ses_{ses_key}_2018', 0), mun_socio.get(f'ses_{ses_key}_2016', 0)),
                'growth_pct_2018_2020': calculate_growth(mun_socio.get(f'ses_{ses_key}_2020', 0), mun_socio.get(f'ses_{ses_key}_2018', 0)),
                'growth_pct_2020_2022': calculate_growth(mun_socio.get(f'ses_{ses_key}_2022', 0), mun_socio.get(f'ses_{ses_key}_2020', 0)),
                'growth_pct_2022_2024': calculate_growth(mun_socio.get(f'ses_{ses_key}_2024', 0), mun_socio.get(f'ses_{ses_key}_2022', 0))
            }
            mun_historical_trends.append(trends)
        
        data['socioeconomic_income_analysis']['municipality_analysis']['historical_trends']['growth_by_level'] = mun_historical_trends
        
        # Comparison
        area_density = area.get('population_density', 0)
        mun_density = mun.get('population_density', 0)
        area_male_pct = self._pct(area_male, area_pop)
        mun_male_pct = self._pct(mun_male, mun_pop)
        
        data['comparison'] = {
            'selected_area': {
                'population_density': area_density,
                'population_density_trend': 'up' if area_density > mun_density else 'down',
                'male_population': area_male,
                'male_percentage': f"{area_male_pct}%",
                'male_trend': 'down' if area_male_pct < mun_male_pct else 'up',
                'female_population': area_female,
                'female_percentage': f"{self._pct(area_female, area_pop)}%",
                'female_trend': 'up' if self._pct(area_female, area_pop) > self._pct(mun_female, mun_pop) else 'down',
                'total_households': area.get('total_households', 0),
                'households_trend': 'up'
            },
            'municipality': {
                'population_density': mun_density,
                'male_population': mun_male,
                'male_percentage': f"{mun_male_pct}%",
                'female_population': mun_female,
                'female_percentage': f"{self._pct(mun_female, mun_pop)}%",
                'total_households': mun.get('total_households', 0)
            }
        }
        
        return data
    
    
    
    def get_data(self, lat, lng, radius, city='queretaro'):
        """Get demographics area analysis data."""
        # Create cache key based on parameters
        cache_key = f"demographics_analysis_{city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached demographics analysis data for {city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
        catchment = self.get_boundary_from_coordinates(lat, lng, radius, city)
        municipality_wkt = self.get_municipality_info(lat, lng, city)
        # Fetch POIs within catchment and municipality
        area_data = DemographicsAreaData().get_data(catchment) if catchment else []
        municipality_area_data = DemographicsAreaData().get_data(municipality_wkt) if municipality_wkt else []
        print("area data",area_data)
        print("municipality area data",municipality_area_data)
        # return area_data, municipality_area_data
        if not area_data or not municipality_area_data:
            logger.warning("No data found for the demographics area analysis")
            return Response.error("No data found for the demographics area analysis")
        
        demographics_data = self._populate_demographics(area_data, municipality_area_data, lat, lng, radius)
        resp = Response.success(data=demographics_data)
        
        # Cache the successful response
        set_in_cache('demographic', cache_key, resp)
        logger.info(f"Cached demographics analysis data for {city} at ({lat}, {lng}) with radius {radius}")
        
        return resp
    



class TrafficAreaAnalysisController(AbstractAreaAnalysisController):
    """Controller for traffic area analysis."""
    DATA_FORMAT = TRAFFIC_DATA_FORMAT
    
    def __init__(self):
        super().__init__()
    
    def _to_float(self, value):
        """Convert Decimal/None to float."""
        if value is None:
            return 0.0
        return float(value) if not isinstance(value, float) else value
    
    def _to_dict(self, row):
        """Convert RealDictRow to dict with float conversion."""
        if not row:
            return {}
        return {k: self._to_float(v) for k, v in dict(row).items()}
    
    def _pct(self, part, total):
        """Calculate percentage."""
        return round((part / total * 100), 1) if total > 0 else 0.0
    
    def create_h3_buckets(self, h3_distribution_data, bucket_size=500):
        """Create buckets from H3 distribution data."""
        if not h3_distribution_data:
            return {"data_range": {"min_value": 0, "max_value": 0}, "buckets": [], "distribution_stats": {"total_points": 0, "bucket_count": 0, "mean_count": 0.0}}
        
        total_values = [item.get('total_users', 0) for item in h3_distribution_data if 'total_users' in item]
        if not total_values:
            return {"data_range": {"min_value": 0, "max_value": 0}, "buckets": [], "distribution_stats": {"total_points": 0, "bucket_count": 0, "mean_count": 0.0}}
        
        data_min, data_max = min(total_values), min(max(total_values), 6000)
        num_buckets = int((data_max - 0) / bucket_size) + 1
        buckets = []
        
        for i in range(num_buckets):
            bucket_min = 0 + (i * bucket_size)
            bucket_max = 0 + ((i + 1) * bucket_size)
            if i == num_buckets - 1:
                h3_count = sum(1 for item in h3_distribution_data 
                              if bucket_min <= item.get('total_users', 0) <= bucket_max)
            else:
                h3_count = sum(1 for item in h3_distribution_data 
                              if bucket_min <= item.get('total_users', 0) < bucket_max)
            
            if h3_count >= 10:  # Only include buckets with sufficient data
                buckets.append({
                    "bucket_number": i + 1,
                    "bucket_range": {"min_value": round(bucket_min, 2), "max_value": round(bucket_max, 2)},
                    "h3_count": h3_count
                })
        
        total_points = sum(b.get('h3_count', 0) for b in buckets)
        return {
            "data_range": {"min_value": 0, "max_value": round(data_max, 2)},
            "buckets": buckets,
            "distribution_stats": {
                "total_points": total_points,
                "bucket_count": len(buckets),
                "mean_count": round(total_points / len(buckets), 2) if buckets else 0
            }
        }
    
    def calculate_percentile_rank_for_value(self, target_value, buckets):
        """Calculate percentile rank for a specific value against bucket distribution."""
        if not buckets:
            return 0.0
        
        total_points = sum(b.get('h3_count', 0) for b in buckets)
        if total_points == 0:
            return 0.0
        
        cumulative_count = 0
        for bucket in buckets:
            max_val = bucket.get('bucket_range', {}).get('max_value', 0)
            if target_value <= max_val:
                break
            cumulative_count += bucket.get('h3_count', 0)
        
        return round((cumulative_count / total_points * 100), 1) if total_points > 0 else 0.0
    
    def get_data(self, lat, lng, radius, city='queretaro'):
        """Get traffic area analysis data."""
        # Create cache key based on parameters
        cache_key = f"traffic_summary_{city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached traffic summary data for {city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
                
        catchment = self.get_boundary_from_coordinates(lat, lng, radius, city)
        municipality_wkt = self.get_municipality_info(lat, lng, city)
        
        if not catchment:
            return Response.error("Invalid catchment area")
        
        # TrafficAreaData.get_data() returns tuple: (traffic_data_dict, h3_data_list)
        area_tuple = TrafficAreaData().get_data(catchment)
        mun_tuple = TrafficAreaData().get_data(municipality_wkt) if municipality_wkt else ({}, [])
        
        if not area_tuple or not isinstance(area_tuple, tuple) or len(area_tuple) != 2:
            return Response.error("No data found for the traffic area analysis")
        
        area_traffic_dict, area_h3_list = area_tuple
        mun_traffic_dict, mun_h3_list = mun_tuple if isinstance(mun_tuple, tuple) and len(mun_tuple) == 2 else ({}, [])
        
        # Extract traffic counts
        area_vehiculo = self._to_float(area_traffic_dict.get('vehiculo', 0))
        area_peaton = self._to_float(area_traffic_dict.get('peaton', 0))
        area_estacionario = self._to_float(area_traffic_dict.get('estacionario', 0))
        area_total_users = self._to_float(area_traffic_dict.get('total_users', 0))
        
        mun_vehiculo = self._to_float(mun_traffic_dict.get('vehiculo', 0))
        mun_peaton = self._to_float(mun_traffic_dict.get('peaton', 0))
        mun_estacionario = self._to_float(mun_traffic_dict.get('estacionario', 0))
        mun_total_users = self._to_float(mun_traffic_dict.get('total_users', 0))
        
        # Extract H3 data
        area_h3_dict = self._to_dict(area_h3_list[0]) if area_h3_list else {}
        area_unique_h3 = int(area_h3_dict.get('unique_h3_count', 0))
        area_total_unique_users = self._to_float(area_h3_dict.get('total_unique_users', 0))
        area_avg_users_per_h3 = self._to_float(area_h3_dict.get('avg_users_per_h3', 0))
        
        # Get population data
        print("getting total population")
        total_population_obj = TotalPopulation()
        print("total popualtion fetched")
        total_population = self._to_float(total_population_obj.get_data(catchment) if catchment else 0)
        municipality_population = self._to_float(total_population_obj.get_data(municipality_wkt) if municipality_wkt else 0)
        
        # Calculate metrics
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        vehicle_pct = self._pct(area_vehiculo, area_total_users)
        pedestrian_pct = self._pct(area_peaton, area_total_users)
        stationary_pct = self._pct(area_estacionario, area_total_users)
        mun_vehicle_pct = self._pct(mun_vehiculo, mun_total_users) if mun_total_users > 0 else None
        mun_pedestrian_pct = self._pct(mun_peaton, mun_total_users) if mun_total_users > 0 else None
        mun_stationary_pct = self._pct(mun_estacionario, mun_total_users) if mun_total_users > 0 else None
        devices_per_person = round(area_total_users / total_population, 2) if total_population > 0 else None
        municipality_average = round(mun_total_users / municipality_population, 2) if municipality_population > 0 and mun_total_users > 0 else None
        
        # Get H3 distribution for percentile calculation
        h3_distribution_data = []
        percentile_rank = 0.0
        #TODO skip bucket calculation for now we will do later
        h3_dist_query = self.qc.build_h3_distribution_query(lat, lng, radius)
        self.cursor.execute(h3_dist_query)
        h3_distribution_raw = self.cursor.fetchall()
        
        h3_distribution_data = [
            {'h3_index': str(row.get('h3_index', '')), 'total_users': self._to_float(row.get('total_users', 0))}
            for row in h3_distribution_raw
        ]
        
        # Calculate percentile if we have data
        if h3_distribution_data and area_avg_users_per_h3 > 0:
            bucket_data = self.create_h3_buckets(h3_distribution_data, bucket_size=500)
            buckets = bucket_data.get('buckets', [])
            if buckets:
                percentile_rank = self.calculate_percentile_rank_for_value(area_avg_users_per_h3, buckets)
        
        # Use deepcopy to avoid mutating the constant (has nested dicts)
        data = copy.deepcopy(TRAFFIC_DATA_FORMAT)
        
        # Populate data structure
        data['summary'].update({
            'num_parcels': None,
            'population': total_population,
            'area_km2': area_km2,
            'center_point': {'lat': float(lat), 'lng': float(lng)},
            'radius_meters': int(radius)
        })
        
        data['socioeconomic'].update({
            'total_unique_devices': int(area_total_users),
            'devices_per_person': devices_per_person,
            'municipality_average': municipality_average
        })
        
        data['traffic'].update({
            'vehicles': {'count': int(area_vehiculo), 'percentage': vehicle_pct, 'municipality_percentage': mun_vehicle_pct, 'trend': None},
            'pedestrians': {'count': int(area_peaton), 'percentage': pedestrian_pct, 'municipality_percentage': mun_pedestrian_pct, 'trend': None},
            'stationary_devices': {'count': int(area_estacionario), 'percentage': stationary_pct, 'municipality_percentage': mun_stationary_pct, 'trend': None}
        })
        
        data['h3_traffic_summary'].update({
            'unique_h3_count': area_unique_h3,
            'total_unique_users': int(area_total_unique_users),
            'avg_users_per_h3': area_avg_users_per_h3,
            'percentile_rank': percentile_rank
        })
        
        if h3_distribution_data:
            bucket_data = self.create_h3_buckets(h3_distribution_data, bucket_size=500)
            data['h3_distribution'].update({
                'total_h3_indexes': len(h3_distribution_data),
                'bucket_distribution': bucket_data
            })
        
        resp = Response.success(data=data)
        
        # Cache the successful response
        set_in_cache('demographic', cache_key, resp)
        logger.info(f"Cached traffic summary data for {city} at ({lat}, {lng}) with radius {radius}")
        
        return resp
    

class TrafficPatternsAreaAnalysisController(AbstractAreaAnalysisController):
    """Controller for traffic patterns area analysis (hourly and daily)."""
    DATA_FORMAT = TRAFFIC_PATTERNS_FORMAT
    
    def __init__(self):
        super().__init__()
    
    def _to_float(self, value):
        """Convert Decimal/None to float."""
        if value is None:
            return 0.0
        return float(value) if not isinstance(value, float) else value
    
    def _to_dict(self, row):
        """Convert RealDictRow to dict with float conversion."""
        if not row:
            return {}
        return {k: self._to_float(v) for k, v in dict(row).items()}
    
    def _pct(self, part, total):
        """Calculate percentage."""
        return round((part / total * 100), 1) if total > 0 else 0.0
    
    def _process_hourly_data(self, hourly_raw_data):
        """Process hourly traffic data into array format with calculations."""
        hourly_array = []
        max_value = 0
        
        for hour in range(24):
            hour_key = f"hour_{hour}"
            value = self._to_float(hourly_raw_data.get(hour_key, 0))
            hourly_array.append(value)
            max_value = max(max_value, value)
        
        total_visits = sum(hourly_array)
        avg_visits_per_hour = round(total_visits / 24) if total_visits > 0 else 0
        
        hourly_percentages = []
        for value in hourly_array:
            percentage = (value / total_visits * 100) if total_visits > 0 else 0
            hourly_percentages.append(round(percentage, 1))
        
        time_labels = [f"{hour:02d}:00" for hour in range(24)]
        
        return {
            "raw_values": hourly_array,
            "percentages": hourly_percentages,
            "max_value": max_value,
            "avg_visits_per_hour": avg_visits_per_hour,
            "total_visits": total_visits,
            "time_labels": time_labels,
            "x_axis_labels": list(range(24))
        }
    
    def _process_daily_data(self, daily_raw_data):
        """Process daily traffic data into array format with calculations."""
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        daily_array = []
        max_value = 0
        
        for day in days:
            value = self._to_float(daily_raw_data.get(day, 0))
            daily_array.append(value)
            max_value = max(max_value, value)
        
        total_visits = sum(daily_array)
        avg_visits_per_day = round(total_visits / 7) if total_visits > 0 else 0
        
        daily_percentages = []
        for value in daily_array:
            percentage = (value / total_visits * 100) if total_visits > 0 else 0
            daily_percentages.append(round(percentage, 1))
        
        day_labels = ['M', 'T', 'W', 'T', 'F', 'S', 'S']
        day_full_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        return {
            "raw_values": daily_array,
            "percentages": daily_percentages,
            "max_value": max_value,
            "avg_visits_per_day": avg_visits_per_day,
            "total_visits": total_visits,
            "days": days,
            "day_labels": day_labels,
            "day_full_names": day_full_names
        }
    
    def _populate_traffic_patterns(self, catchment, lat, lng, radius):
        """Populate traffic patterns format with actual data from new dimension tables."""
        data = copy.deepcopy(TRAFFIC_PATTERNS_FORMAT)
        
        # Initialize data classes
        hourly_data_class = TrafficByHourAreaData()
        daily_data_class = TrafficByDayAreaData()
        
        # Define user types mapping
        user_types = {
            'vehiculo': 'vehicles',
            'peaton': 'pedestrians',
            'estacionario': 'stationary_devices'
        }
        
        # Process hourly data for each user type
        for user_type, user_type_english in user_types.items():
            hourly_raw = hourly_data_class.get_data(catchment, user_type)
            data['hourly_traffic'][user_type_english] = self._process_hourly_data(hourly_raw)
        
        # Process daily data for each user type
        for user_type, user_type_english in user_types.items():
            daily_raw = daily_data_class.get_data(catchment, user_type)
            data['daily_traffic'][user_type_english] = self._process_daily_data(daily_raw)
        
        # Add metadata
        data['center_point'] = {"lat": float(lat), "lng": float(lng)}
        data['radius_meters'] = int(radius)
        data['pattern_type'] = "both_with_user_types"
        
        return data
    
    def get_data(self, lat, lng, radius, city='queretaro'):
        """Get traffic patterns area analysis data."""
        # Create cache key based on parameters
        cache_key = f"traffic_patterns_{city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached traffic patterns data for {city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
        catchment = self.get_boundary_from_coordinates(lat, lng, radius, city)
        
        if not catchment:
            logger.warning("No catchment area found for traffic patterns")
            return Response.error("Invalid catchment area")
        
        patterns_data = self._populate_traffic_patterns(catchment, lat, lng, radius)
        resp = Response.success(data=patterns_data)
        
        # Cache the successful response
        set_in_cache('demographic', cache_key, resp)
        logger.info(f"Cached traffic patterns data for {city} at ({lat}, {lng}) with radius {radius}")
        
        return resp
    

        