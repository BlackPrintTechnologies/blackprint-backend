import json
import logging
import traceback
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase
from module.area_analysis.query import AreaAnalysisQuery

logger = logging.getLogger(__name__)

class AreaAnalysisController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.query_builder = AreaAnalysisQuery()

    def get_traffic_by_day(self, lat, lng, radius=2000, user_type=None):
        """
        Get traffic data aggregated by day of the week within a specified radius.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            user_type (str): User type filter ('estacionario', 'vehiculo', 'peaton') or None for all
            
        Returns:
            dict: Response with traffic data by day
        """
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_traffic_by_day_query(lat, lng, radius, user_type)
            logger.info(f"Traffic by day query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Convert aggregated_result to a more readable format
            if res and len(res) > 0:
                aggregated_result = res[0]
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": sum([
                            aggregated_result.get('monday', 0),
                            aggregated_result.get('tuesday', 0),
                            aggregated_result.get('wednesday', 0),
                            aggregated_result.get('thursday', 0),
                            aggregated_result.get('friday', 0),
                            aggregated_result.get('saturday', 0),
                            aggregated_result.get('sunday', 0)
                        ])
                    },
                    "traffic_by_day": {
                        "monday": aggregated_result.get('monday', 0),
                        "tuesday": aggregated_result.get('tuesday', 0),
                        "wednesday": aggregated_result.get('wednesday', 0),
                        "thursday": aggregated_result.get('thursday', 0),
                        "friday": aggregated_result.get('friday', 0),
                        "saturday": aggregated_result.get('saturday', 0),
                        "sunday": aggregated_result.get('sunday', 0)
                    }
                }
            else:
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": 0
                    },
                    "traffic_by_day": {
                        "monday": 0, "tuesday": 0, "wednesday": 0, "thursday": 0,
                        "friday": 0, "saturday": 0, "sunday": 0
                    }
                }
            
            logger.info(f"Traffic by day aggregated_results: {traffic_data['summary']['total_unique_users']} total users")
            resp = Response.success(data=traffic_data)
            
        except Exception as e:
            logger.error(f"Error in get_traffic_by_day: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_traffic_by_hour(self, lat, lng, radius=2000, user_type=None):
        """
        Get traffic data aggregated by hour of the day within a specified radius.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            user_type (str): User type filter ('estacionario', 'vehiculo', 'peaton') or None for all
            
        Returns:
            dict: Response with traffic data by hour
        """
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_traffic_by_hour_query(lat, lng, radius, user_type)
            logger.info(f"Traffic by hour query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Convert aggregated_result to a more readable format
            if res and len(res) > 0:
                aggregated_result = res[0]
                hourly_data = {}
                total_users = 0
                
                for hour in range(24):
                    hour_key = f"hour_{hour}"
                    value = aggregated_result.get(hour_key, 0)
                    hourly_data[f"hour_{hour:02d}"] = value
                    total_users += value
                
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": total_users
                    },
                    "traffic_by_hour": hourly_data
                }
            else:
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": 0
                    },
                    "traffic_by_hour": {f"hour_{hour:02d}": 0 for hour in range(24)}
                }
            
            logger.info(f"Traffic by hour aggregated_results: {traffic_data['summary']['total_unique_users']} total users")
            resp = Response.success(data=traffic_data)
            
        except Exception as e:
            logger.error(f"Error in get_traffic_by_hour: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_traffic_summary(self, lat, lng, radius=2000, user_type=None):
        """
        Get total traffic summary within a specified radius.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            user_type (str): User type filter ('estacionario', 'vehiculo', 'peaton') or None for all
            
        Returns:
            dict: Response with total traffic summary
        """
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_traffic_summary_query(lat, lng, radius, user_type)
            logger.info(f"Traffic summary query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Convert aggregated_result to a more readable format
            if res and len(res) > 0:
                aggregated_result = res[0]
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": aggregated_result.get('total_users', 0)
                    }
                }
            else:
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": 0
                    }
                }
            
            logger.info(f"Traffic summary aggregated_results: {traffic_data['summary']['total_unique_users']} total users")
            resp = Response.success(data=traffic_data)
            
        except Exception as e:
            logger.error(f"Error in get_traffic_summary: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_area_summary(self, lat, lng, radius=2000, config_city='queretaro'):
        """
        Get area analysis summary matching the UI mockup exactly.
        Returns all the summary data needed for the main UI panel.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            config_city (str): City configuration - 'queretaro' or 'mexico'
            
        Returns:
            dict: Response with area summary data matching UI structure
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Get population data first
            population_query = self.query_builder.build_population_query(lat, lng, radius, config_city)
            logger.info(f"Executing population query: {population_query}")
            
            cursor.execute(population_query)
            connection.commit()
            population_res = cursor.fetchall()
            
            total_population = 0
            municipality_population = 0
            municipality_code = None
            
            if population_res and len(population_res) > 0:
                pop_data = population_res[0]
                total_population = pop_data.get('total_population', 0) or 0
                municipality_population = pop_data.get('municipality_population', 0) or 0
                municipality_code = pop_data.get('municipality_code')
                logger.info(f"Found population: {total_population} in area, {municipality_population} in municipality")
            
            user_types = ['vehiculo', 'peaton', 'estacionario']
            traffic_data = {}
            total_all_users = 0
            
            # Get traffic data for each user type directly from database
            for user_type in user_types:
                query = self.query_builder.build_traffic_summary_query(lat, lng, radius, user_type)
                logger.info(f"Executing query for {user_type}: {query}")
                
                cursor.execute(query)
                connection.commit()
                res = cursor.fetchall()
                
                if res and len(res) > 0:
                    user_total = res[0].get('total_users', 0) or 0
                    traffic_data[user_type] = user_total
                    total_all_users += user_total
                    logger.info(f"Found {user_total} users for {user_type}")
                else:
                    traffic_data[user_type] = 0
                    logger.info(f"No data found for {user_type}")
            
            # Get municipality-level traffic data if municipality_code is available
            municipality_traffic_data = {}
            municipality_total_users = 0
            
            if municipality_code:
                logger.info(f"Getting municipality traffic data for municipality_code: {municipality_code}")
                for user_type in user_types:
                    query = self.query_builder.build_municipality_traffic_query(municipality_code, user_type)
                    logger.info(f"Executing municipality query for {user_type}: {query}")
                    cursor.execute(query)
                    connection.commit()
                    res = cursor.fetchall()
                    
                    if res and len(res) > 0:
                        municipality_user_total = res[0].get('total_users', 0) or 0
                        municipality_traffic_data[user_type] = municipality_user_total
                        municipality_total_users += municipality_user_total
                        logger.info(f"Found {municipality_user_total} municipality users for {user_type}")
                    else:
                        municipality_traffic_data[user_type] = 0
                        logger.info(f"No municipality data found for {user_type}")
            else:
                logger.warning("No municipality_code found, skipping municipality traffic calculations")
            
            # Calculate area in km²
            area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
            
            # Calculate percentages
            vehicle_pct = round((traffic_data.get('vehiculo', 0) / total_all_users * 100), 0) if total_all_users > 0 else 0
            pedestrian_pct = round((traffic_data.get('peaton', 0) / total_all_users * 100), 0) if total_all_users > 0 else 0
            stationary_pct = round((traffic_data.get('estacionario', 0) / total_all_users * 100), 0) if total_all_users > 0 else 0
            
            # Calculate municipality percentages
            municipality_vehicle_pct = None
            municipality_pedestrian_pct = None
            municipality_stationary_pct = None
            
            if municipality_total_users > 0:
                municipality_vehicle_pct = round((municipality_traffic_data.get('vehiculo', 0) / municipality_total_users * 100), 0)
                municipality_pedestrian_pct = round((municipality_traffic_data.get('peaton', 0) / municipality_total_users * 100), 0)
                municipality_stationary_pct = round((municipality_traffic_data.get('estacionario', 0) / municipality_total_users * 100), 0)
            
            # Calculate devices per person
            devices_per_person = None
            if total_population > 0:
                devices_per_person = round(total_all_users / total_population, 2)
            
            # Calculate municipality average devices per person
            municipality_average_devices_per_person = None
            if municipality_population > 0 and municipality_total_users > 0:
                municipality_average_devices_per_person = round(municipality_total_users / municipality_population, 2)
            

            # Get H3 distribution data for all user types combined
            h3_distribution_data = {}
            
            try:
                h3_query = self.query_builder.build_h3_distribution_query(lat, lng, radius)
                logger.info(f"Executing H3 distribution query for all user types: {h3_query}")
                
                cursor.execute(h3_query)
                connection.commit()
                h3_res = cursor.fetchall()
                
                if h3_res and len(h3_res) > 0:
                    h3_distribution = []
                    for row in h3_res:
                        h3_distribution.append({
                            "h3_index": str(row.get('h3_index', '')),
                            "total_users": float(row.get('total_users', 0))
                        })
                    
                    # Create buckets from H3 distribution data
                    bucket_data = self.create_h3_buckets(h3_distribution, bucket_size=100)
                    
                    h3_distribution_data = {
                        "total_h3_indexes": len(h3_distribution),
                        "bucket_distribution": bucket_data
                    }
                    logger.info(f"H3 distribution data collected for all user types: {len(h3_distribution)} H3 indexes with {len(bucket_data['buckets'])} buckets")
                else:
                    h3_distribution_data = {
                        "total_h3_indexes": 0,
                        "bucket_distribution": {
                            "buckets": []
                        }
                    }
                    logger.info("No H3 distribution data found for all user types")
                
            except Exception as e:
                logger.error(f"Error getting H3 distribution data: {str(e)}")
                h3_distribution_data = {
                    "total_h3_indexes": 0,
                    "bucket_distribution": {
                        "buckets": []
                    }
                }

            # Structure response with calculated data
            summary_data = {
                "summary": {
                    "num_parcels": None,  # Not available - would need parcels/cadastral data
                    "population": total_population,   # Now calculated from population data
                    "area_km2": area_km2,
                    "center_point": {"lat": lat, "lng": lng},
                    "radius_meters": radius
                },
                "socioeconomic": {
                    "total_unique_devices": total_all_users,
                    "devices_per_person": devices_per_person,  # Now calculated
                    "municipality_average": municipality_average_devices_per_person  # Now calculated
                },
                "traffic": {
                    "vehicles": {
                        "count": traffic_data.get('vehiculo', 0),
                        "percentage": int(vehicle_pct),
                        "municipality_percentage": municipality_vehicle_pct,  # Now calculated
                        "trend": None  # Not available - would need historical data
                    },
                    "pedestrians": {
                        "count": traffic_data.get('peaton', 0), 
                        "percentage": int(pedestrian_pct),
                        "municipality_percentage": municipality_pedestrian_pct,  # Now calculated
                        "trend": None  # Not available - would need historical data
                    },
                    "stationary_devices": {
                        "count": traffic_data.get('estacionario', 0),
                        "percentage": int(stationary_pct), 
                        "municipality_percentage": municipality_stationary_pct,  # Now calculated
                        "trend": None  # Not available - would need historical data
                    }
                },
                "h3_distribution": h3_distribution_data
            }
            
            logger.info(f"Area summary completed for {total_all_users} total users")
            resp = Response.success(data=summary_data)
            
        except Exception as e:
            logger.error(f"Error in get_area_summary: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_traffic_patterns(self, lat, lng, radius=2000, config_city='queretaro'):
        """
        Get detailed traffic patterns for charts (both hourly and daily).
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            config_city (str): City configuration - 'queretaro' or 'mexico'
            
        Returns:
            dict: Response with traffic pattern data for charts
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            patterns_data = {
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius,
                "pattern_type": "both_with_user_types"
            }
            
            # Define user types and their English names
            user_types = {
                'vehiculo': 'vehicles',
                'peaton': 'pedestrians', 
                'estacionario': 'stationary_devices'
            }
            
            # Get hourly data for each user type
            hourly_traffic = {}
            for user_type, user_type_english in user_types.items():
                query = self.query_builder.build_traffic_by_hour_query(lat, lng, radius, user_type)
                logger.info(f"Executing hourly query for {user_type}: {query}")
                
                cursor.execute(query)
                connection.commit()
                res = cursor.fetchall()
                
                if res and len(res) > 0:
                    hourly_data = res[0]
                    
                    # Convert to array format for charts [0-23]
                    hourly_array = []
                    max_value = 0
                    for hour in range(24):
                        hour_key = f"hour_{hour}"
                        value = hourly_data.get(hour_key, 0)
                        hourly_array.append(value)
                        max_value = max(max_value, value)
                    
                    # Calculate average visits per hour
                    total_hourly_visits = sum(hourly_array)
                    avg_visits_per_hour = round(total_hourly_visits / 24) if total_hourly_visits > 0 else 0
                    
                    # Convert to percentages based on sum of all values (0-100%)
                    hourly_percentages = []
                    for value in hourly_array:
                        percentage = (value / total_hourly_visits * 100) if total_hourly_visits > 0 else 0
                        hourly_percentages.append(round(percentage, 1))
                    
                    # Create time labels for x-axis (0-23 hours)
                    time_labels = [f"{hour:02d}:00" for hour in range(24)]
                    
                    hourly_traffic[user_type_english] = {
                        "raw_values": hourly_array,
                        "percentages": hourly_percentages,
                        "max_value": max_value,
                        "avg_visits_per_hour": avg_visits_per_hour,
                        "total_visits": total_hourly_visits,
                        "time_labels": time_labels,
                        "x_axis_labels": list(range(24))  # For chart x-axis
                    }
                else:
                    # No data for this user type
                    hourly_traffic[user_type_english] = {
                        "raw_values": [0] * 24,
                        "percentages": [0] * 24,
                        "max_value": 0,
                        "avg_visits_per_hour": 0,
                        "total_visits": 0,
                        "time_labels": [f"{hour:02d}:00" for hour in range(24)],
                        "x_axis_labels": list(range(24))
                    }
            
            patterns_data["hourly_traffic"] = hourly_traffic
            
            # Get daily data for each user type
            daily_traffic = {}
            for user_type, user_type_english in user_types.items():
                query = self.query_builder.build_traffic_by_day_query(lat, lng, radius, user_type)
                logger.info(f"Executing daily query for {user_type}: {query}")
                
                cursor.execute(query)
                connection.commit()
                res = cursor.fetchall()
                
                if res and len(res) > 0:
                    daily_data = res[0]
                    
                    # Convert to array format for charts
                    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                    daily_array = []
                    max_value = 0
                    for day in days:
                        value = daily_data.get(day, 0)
                        daily_array.append(value)
                        max_value = max(max_value, value)
                    
                    # Calculate average visits per day
                    total_daily_visits = sum(daily_array)
                    avg_visits_per_day = round(total_daily_visits / 7) if total_daily_visits > 0 else 0
                    
                    # Convert to percentages based on sum of all values (0-100%)
                    daily_percentages = []
                    for value in daily_array:
                        percentage = (value / total_daily_visits * 100) if total_daily_visits > 0 else 0
                        daily_percentages.append(round(percentage, 1))
                    
                    # Create day labels for x-axis
                    day_labels = ['M', 'T', 'W', 'T', 'F', 'S', 'S']  # Short labels as shown in UI
                    day_full_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    
                    daily_traffic[user_type_english] = {
                        "raw_values": daily_array,
                        "percentages": daily_percentages,
                        "max_value": max_value,
                        "avg_visits_per_day": avg_visits_per_day,
                        "total_visits": total_daily_visits,
                        "days": days,
                        "day_labels": day_labels,  # Short labels for chart
                        "day_full_names": day_full_names  # Full names for tooltips
                    }
                else:
                    # No data for this user type
                    daily_traffic[user_type_english] = {
                        "raw_values": [0] * 7,
                        "percentages": [0] * 7,
                        "max_value": 0,
                        "avg_visits_per_day": 0,
                        "total_visits": 0,
                        "days": ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'],
                        "day_labels": ['M', 'T', 'W', 'T', 'F', 'S', 'S'],
                        "day_full_names": ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    }
            
            patterns_data["daily_traffic"] = daily_traffic
            
            logger.info("Traffic patterns completed with all user types")
            resp = Response.success(data=patterns_data)
            
        except Exception as e:
            logger.error(f"Error in get_traffic_patterns: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_area_demographics(self, lat, lng, radius=2000, config_city='mexico'):
        """
        Get demographic and socioeconomic analysis for a specific area.
        Returns population demographics, income levels, and socioeconomic distribution.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            config_city (str): City configuration - 'mexico' or 'queretaro'
            
        Returns:
            dict: Response with demographic analysis data matching UI structure
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Build query to get demographic data within the specified area
            if config_city == 'mexico':
                query = self.query_builder.build_mexico_demographics_query(lat, lng, radius)
            elif config_city == 'queretaro':
                query = self.query_builder.build_queretaro_demographics_query(lat, lng, radius)
            else:
                logger.error(f"Unsupported city configuration: {config_city}. Supported cities: mexico, queretaro")
                return Response.error("Unsupported city configuration. Supported cities: mexico, queretaro")
            logger.info(f"Executing demographics query for city={config_city}: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            if res and len(res) > 0:
                # Process demographic data
                if config_city == 'mexico':
                    demographics_data = self._process_mexico_demographics_data(res, lat, lng, radius, connection)
                elif config_city == 'queretaro':
                    demographics_data = self._process_queretaro_demographics_data(res, lat, lng, radius, connection)
                else:
                    logger.error(f"Unsupported city configuration for processing: {config_city}")
                    return Response.error("Unsupported city configuration. Supported cities: mexico, queretaro")
                if config_city == 'queretaro':
                    logger.info(f"Demographics analysis completed for pre-aggregated query result in {config_city}")
                else:
                    logger.info(f"Demographics analysis completed for {len(res)} records in {config_city}")
                resp = Response.success(data=demographics_data)
            else:
                # Return empty demographics structure
                demographics_data = self._get_empty_demographics_structure(lat, lng, radius)
                logger.info(f"No demographic data found for {config_city}, returning empty structure")
                resp = Response.success(data=demographics_data)
            
        except Exception as e:
            logger.error(f"Error in get_area_demographics for {config_city}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_area_socioeconomic(self, lat, lng, radius=2000, config_city='mexico'):
        """
        Get socioeconomic analysis for a specific area matching the UI mockup.
        Returns income levels, predominant socioeconomic level, and household distribution.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            config_city (str): City configuration - 'mexico' or 'queretaro'
            
        Returns:
            dict: Response with socioeconomic analysis data matching UI structure
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Build query to get socioeconomic data within the specified area
            query = self.query_builder.build_socioeconomic_query(lat, lng, radius, config_city)
            logger.info(f"Executing socioeconomic query for city={config_city}: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            if res and len(res) > 0:
                # Process socioeconomic data
                socioeconomic_data = self._process_socioeconomic_data(res, lat, lng, radius, config_city)
                logger.info(f"Socioeconomic analysis completed for {len(res)} records in {config_city}")
                resp = Response.success(data=socioeconomic_data)
            else:
                # Return empty socioeconomic structure
                socioeconomic_data = self._get_empty_socioeconomic_structure(lat, lng, radius)
                logger.info(f"No socioeconomic data found for {config_city}, returning empty structure")
                resp = Response.success(data=socioeconomic_data)
            
        except Exception as e:
            logger.error(f"Error in get_area_socioeconomic for {config_city}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def _aggregate_mexico_demographic_data(self, demographic_data):
        """
        Aggregate Mexico City demographic data from multiple records within the radius.
        This properly sums up all demographic data instead of using just the first record.
        """
        if not demographic_data:
            return {}
        
        if len(demographic_data) == 1:
            return demographic_data[0]
        
        # Initialize aggregated data
        aggregated = {}
        
        # Sum numeric fields for selected area (block level) - these should be summed
        selected_area_fields = [
            'pobtot', 'pobmas', 'pobfem', 'vivtot', 'p_0a2', 'p_3a5', 'p_6a11', 'p_12a14', 
            'p_15a17', 'p_18a24', 'p_60ymas', 'p_0a2_f', 'p_0a2_m', 'p_3a5_f', 'p_3a5_m',
            'p_6a11_f', 'p_6a11_m', 'p_12a14_f', 'p_12a14_m', 'p_15a17_f', 'p_15a17_m',
            'p_18a24_f', 'p_18a24_m', 'p_60ymas_f', 'p_60ymas_m', 'pea', 'pea_m', 'pea_f',
            'pe_inac', 'pe_inac_m', 'pe_inac_f', 'pocupada', 'pocupada_m', 'pocupada_f',
            'pdesocup', 'pdesocup_m', 'pdesocup_f', 'p3a5_noa', 'p6a11_noa', 'p12a14noa',
            'p15a17a', 'p18a24a', 'ses_ab', 'ses_c_plus', 'ses_c', 'ses_c_minus', 
            'ses_d_plus', 'ses_d', 'ses_e', 'pob_2000_ageb', 'pob_2005_ageb', 'pob_2010_ageb',
            'pob_2015_ageb', 'pob_2020_ageb', 'pob_2000_entidad', 'pob_2005_entidad', 
            'pob_2010_entidad', 'pob_2015_entidad', 'pob_2020_entidad', 'pob_2000_municipal',
            'pob_2005_municipal', 'pob_2010_municipal', 'pob_2015_municipal', 'pob_2020_municipal',
            # Colonia level fields (summed)
            'vivtot_colonia', 'pobtot_colonia', 'pobmas_colonia', 'pobfem_colonia', 'ses_ab_colonia', 'ses_c_plus_colonia',
            'ses_c_colonia', 'ses_c_minus_colonia', 'ses_d_colonia', 'ses_d_plus_colonia', 'ses_e_colonia',
            'p_3a5_colonia', 'p_6a11_colonia', 'p_12a14_colonia', 'p_15a17_colonia', 'p_18a24_colonia', 
            'p3a5_noa_colonia', 'p6a11_noa_colonia', 'p12a14noa_colonia', 'p15a17a_colonia', 'p18a24a_colonia', 
            'pea_colonia', 'pea_m_colonia', 'pea_f_colonia', 'pe_inac_colonia', 'pe_inac_m_colonia', 'pe_inac_f_colonia', 
            'pocupada_colonia', 'pocupada_m_colonia', 'pocupada_f_colonia', 'pdesocup_colonia', 'pdesocup_m_colonia',
            'pdesocup_f_colonia'
        ]
        
        # Municipality level fields (should NOT be summed - use single record)
        municipality_fields = [
            'vivtot_alcaldia', 'pobtot_alcaldia', 'pobmas_alcaldia', 'pobfem_alcaldia', 
            'ses_ab_alcaldia', 'ses_c_plus_alcaldia', 'ses_c_alcaldia', 'ses_c_minus_alcaldia',
            'ses_d_alcaldia', 'ses_d_plus_alcaldia', 'ses_e_alcaldia', 'p_3a5_alcaldia', 'p_6a11_alcaldia',
            'p_12a14_alcaldia', 'p_15a17_alcaldia', 'p_18a24_alcaldia', 'p3a5_noa_alcaldia', 'p6a11_noa_alcaldia',
            'p12a14noa_alcaldia', 'p15a17a_alcaldia', 'p18a24a_alcaldia', 'pea_alcaldia', 'pea_m_alcaldia', 
            'pea_f_alcaldia', 'pe_inac_alcaldia', 'pe_inac_m_alcaldia', 'pe_inac_f_alcaldia', 
            'pocupada_alcaldia', 'pocupada_m_alcaldia', 'pocupada_f_alcaldia', 'pdesocup_alcaldia', 
            'pdesocup_m_alcaldia', 'pdesocup_f_alcaldia'
        ]
        
        # Sum selected area fields (block and colonia level)
        for field in selected_area_fields:
            aggregated[field] = sum(record.get(field, 0) or 0 for record in demographic_data)
        
        # Get municipality data from first record (not summed)
        first_record = demographic_data[0]
        for field in municipality_fields:
            aggregated[field] = first_record.get(field, 0) or 0
        
        # Calculate weighted averages for percentage fields
        # For socioeconomic levels, we need to calculate weighted averages based on household counts
        total_households = aggregated['vivtot']
        if total_households > 0:
            # Calculate weighted averages for socioeconomic percentages
            for field in ['ses_ab', 'ses_c_plus', 'ses_c', 'ses_c_minus', 'ses_d_plus', 'ses_d', 'ses_e']:
                weighted_sum = sum(
                    (record.get(field, 0) or 0) * (record.get('vivtot', 0) or 0) 
                    for record in demographic_data
                )
                aggregated[field] = round(weighted_sum / total_households, 2) if total_households > 0 else 0
        
        # Calculate weighted averages for other percentage fields
        percentage_fields = [
            'prom_ocup', 'pro_ocup_c', 'cambio_porcentual_2005_ageb', 'cambio_porcentual_2010_ageb',
            'cambio_porcentual_2015_ageb', 'cambio_porcentual_2020_ageb', 'cambio_porcentual_2005_entidad',
            'cambio_porcentual_2010_entidad', 'cambio_porcentual_2015_entidad', 'cambio_porcentual_2020_entidad',
            'cambio_porcentual_2005_municipal', 'cambio_porcentual_2010_municipal', 'cambio_porcentual_2015_municipal',
            'cambio_porcentual_2020_municipal',
            # Add missing colonia and alcaldia level percentage fields
            'prom_ocup_colonia', 'pro_ocup_c_colonia', 'prom_ocup_alcaldia', 'pro_ocup_c_alcaldia'
        ]
        
        for field in percentage_fields:
            if field in ['prom_ocup', 'pro_ocup_c', 'prom_ocup_colonia', 'pro_ocup_c_colonia', 'prom_ocup_alcaldia', 'pro_ocup_c_alcaldia']:
                # For household size and rooms, calculate weighted average by households
                weighted_sum = sum(
                    (record.get(field, 0) or 0) * (record.get('vivtot', 0) or 0) 
                    for record in demographic_data
                )
                aggregated[field] = round(weighted_sum / total_households, 2) if total_households > 0 else 0
            else:
                # For growth percentages, calculate simple average
                values = [record.get(field, 0) or 0 for record in demographic_data if record.get(field, 0) is not None]
                aggregated[field] = round(sum(values) / len(values), 2) if values else 0
        
        # Take first values for non-numeric fields (neighborhood, municipality, etc.)
        first_record = demographic_data[0]
        non_numeric_fields = [
            'neighborhood', 'predominant_level', 'ageb_code', 'nom_mun', 'municipality_code', 
            'municipality_nm', 'centroid', 'total_area'
        ]
        
        for field in non_numeric_fields:
            aggregated[field] = first_record.get(field)
        
        # Calculate colonia level data (copy from selected area fields)
        # Note: Municipality-level data should come from the database query, not copied from block level
        # This ensures we get the actual municipality data instead of fallback values
        for field in selected_area_fields:
            if field not in ['pob_2000_municipal', 'pob_2005_municipal', 'pob_2010_municipal', 
                           'pob_2015_municipal', 'pob_2020_municipal']:
                # Only copy to colonia level, not alcaldia level
                aggregated[f"{field}_colonia"] = aggregated[field]
                # Municipality data should come from the actual database columns
                # aggregated[f"{field}_alcaldia"] = aggregated[field]  # REMOVED FALLBACK
        
        # For municipality level, we might want to keep the original values or calculate differently
        # This depends on your business logic requirements
        
        logger.info(f"Aggregated {len(demographic_data)} records into single demographic profile")
        return aggregated

    def _process_mexico_demographics_data(self, demographic_data, lat, lng, radius, connection=None):
        """
        Process Mexico City demographic data using the optimized structure from properties controller.
        This matches the demographic structure you provided for Mexico City.
        """
        # Filter records by distance for Mexico (since we can't do it in SQL)
        filtered_data = []
        radius_degrees = radius / 111000.0  # Convert meters to degrees
        
        for record in demographic_data:
            try:
                # Parse centroid JSON string to extract coordinates
                centroid_str = record.get('centroid', '')
                if centroid_str and 'coordinates' in centroid_str:
                    # Simple string parsing since JSON type is not available
                    import re
                    coords_match = re.search(r'\[([^,]+),\s*([^\]]+)\]', centroid_str)
                    if coords_match:
                        record_lng = float(coords_match.group(1))
                        record_lat = float(coords_match.group(2))
                        
                        # Check if within radius (simple bounding box)
                        if (abs(record_lat - lat) <= radius_degrees and 
                            abs(record_lng - lng) <= radius_degrees):
                            filtered_data.append(record)
            except (ValueError, AttributeError):
                continue
        
        demographic_data = filtered_data
        logger.info(f"Filtered to {len(demographic_data)} records within radius for Mexico")
        
        if not demographic_data:
            return self._get_empty_demographics_structure(lat, lng, radius)
        
        # AGGREGATE all records within the radius instead of using just the first one
        aggregated_result = self._aggregate_mexico_demographic_data(demographic_data)
        
        # Calculate area
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        
        # Build the demographic structure using aggregated data
        demographic = {
            "general": {
                "block": {
                    "neighborhood": aggregated_result["neighborhood"],
                    "predominant_level": aggregated_result["predominant_level"],
                    "ageb_code": aggregated_result["ageb_code"],
                    "total_household": aggregated_result["vivtot"],
                    "average_household_size": aggregated_result["prom_ocup"],
                    "average_number_of_rooms": aggregated_result["pro_ocup_c"]
                },
                "colonia": {
                    "neighborhood": aggregated_result["neighborhood"],
                    "predominant_level": aggregated_result["predominant_level"],
                    "ageb_code": aggregated_result["ageb_code"],
                    "total_household": aggregated_result.get("vivtot_colonia", 0),
                    "average_household_size": aggregated_result.get("prom_ocup_colonia", 0),
                    "average_number_of_rooms": aggregated_result.get("pro_ocup_c_colonia", 0)
                },
                "alcaldia": {
                    "neighborhood": aggregated_result['nom_mun'],
                    "predominant_level": aggregated_result["predominant_level"],
                    "ageb_code": aggregated_result["ageb_code"],
                    "total_household": aggregated_result.get("vivtot_alcaldia", 0),
                    "average_household_size": aggregated_result.get("prom_ocup_alcaldia", 0),
                    "average_number_of_rooms": aggregated_result.get("pro_ocup_c_alcaldia", 0)
                }
            },
            "socio_economic_level": {
                "block": {
                    "ses_ab": aggregated_result["ses_ab"],
                    "ses_c_plus": aggregated_result["ses_c_plus"],
                    "ses_c": aggregated_result["ses_c"],
                    "ses_c_minus": aggregated_result["ses_c_minus"],
                    "ses_d": aggregated_result["ses_d"],
                    "ses_d_plus": aggregated_result["ses_d_plus"],
                    "ses_e": aggregated_result["ses_e"]
                },
                "colonia": {
                    "ses_ab": aggregated_result["ses_ab_colonia"],
                    "ses_c_plus": aggregated_result["ses_c_plus_colonia"],
                    "ses_c": aggregated_result["ses_c_colonia"],
                    "ses_c_minus": aggregated_result["ses_c_minus_colonia"],
                    "ses_d": aggregated_result["ses_d_colonia"],
                    "ses_d_plus": aggregated_result["ses_d_plus_colonia"],
                    "ses_e": aggregated_result["ses_e_colonia"]
                },
                "alcaldia": {
                    "ses_ab": aggregated_result["ses_ab_alcaldia"],
                    "ses_c_plus": aggregated_result["ses_c_plus_alcaldia"],
                    "ses_c": aggregated_result["ses_c_alcaldia"],
                    "ses_c_minus": aggregated_result["ses_c_minus_alcaldia"],
                    "ses_d": aggregated_result["ses_d_alcaldia"],
                    "ses_d_plus": aggregated_result["ses_d_plus_alcaldia"],
                    "ses_e": aggregated_result["ses_e_alcaldia"]
                }
            },
            "population": {
                "block": {
                    "total_population": aggregated_result["pobtot"],
                    "male_population": aggregated_result["pobmas"],
                    "female_population": aggregated_result["pobfem"],
                },
                "colonia": {
                    "total_population": aggregated_result["pobtot_colonia"],
                    "male_population": aggregated_result["pobmas_colonia"], 
                    "female_population": aggregated_result["pobfem_colonia"],
                },
                "alcaldia": {
                    "total_population": aggregated_result["pobtot_alcaldia"],
                    "male_population": aggregated_result.get("pobmas_alcaldia", 0) or 0,
                    "female_population": aggregated_result.get("pobfem_alcaldia", 0) or 0,
                }
            },
            "education": {
                "block": {
                    "education_3_5": aggregated_result["p_3a5"],
                    "education_6_11": aggregated_result["p_6a11"],
                    "education_12_14": aggregated_result["p_12a14"],
                    "education_15_17": aggregated_result["p_15a17"],
                    "education_18_24": aggregated_result["p_18a24"],
                    "education_3_5_attending_school": aggregated_result["p3a5_noa"],
                    "education_6_11_attending_school": aggregated_result["p6a11_noa"],
                    "education_12_14_attending_school": aggregated_result["p12a14noa"],
                    "education_15_17_attending_school": aggregated_result["p15a17a"],
                    "education_18_24_attending_school": aggregated_result["p18a24a"]
                },
                "colonia": {
                    "education_3_5": aggregated_result["p_3a5_colonia"],
                    "education_6_11": aggregated_result["p_6a11_colonia"],
                    "education_12_14": aggregated_result["p_12a14_colonia"],
                    "education_15_17": aggregated_result["p_15a17_colonia"],
                    "education_18_24": aggregated_result["p_18a24_colonia"],
                    "education_3_5_attending_school": aggregated_result["p3a5_noa_colonia"],
                    "education_6_11_attending_school": aggregated_result["p6a11_noa_colonia"],
                    "education_12_14_attending_school": aggregated_result["p12a14noa_colonia"],
                    "education_15_17_attending_school": aggregated_result["p15a17a_colonia"],
                    "education_18_24_attending_school": aggregated_result["p18a24a_colonia"]
                },
                "alcaldia": {
                    "education_3_5": aggregated_result["p_3a5_alcaldia"],
                    "education_6_11": aggregated_result["p_6a11_alcaldia"],
                    "education_12_14": aggregated_result["p_12a14_alcaldia"],
                    "education_15_17": aggregated_result["p_15a17_alcaldia"],
                    "education_18_24": aggregated_result["p_18a24_alcaldia"],
                    "education_3_5_attending_school": aggregated_result["p3a5_noa_alcaldia"],
                    "education_6_11_attending_school": aggregated_result["p6a11_noa_alcaldia"],
                    "education_12_14_attending_school": aggregated_result["p12a14noa_alcaldia"],
                    "education_15_17_attending_school": aggregated_result["p15a17a_alcaldia"],
                    "education_18_24_attending_school": aggregated_result["p18a24a_alcaldia"]
                }
            },
            "workforce": {
                "block": {
                    "total_workforce": aggregated_result["pea"],
                    "total_male_workforce": aggregated_result["pea_m"],
                    "total_female_workforce": aggregated_result["pea_f"],
                    "total_inactive_population": aggregated_result["pe_inac"],
                    "total_inactive_male_population": aggregated_result["pe_inac_m"],
                    "total_inactive_female_population": aggregated_result["pe_inac_f"]
                },
                "colonia": {
                    "total_workforce": aggregated_result["pea_colonia"],
                    "total_male_workforce": aggregated_result["pea_m_colonia"],
                    "total_female_workforce": aggregated_result["pea_f_colonia"],
                    "total_inactive_population": aggregated_result["pe_inac_colonia"],
                    "total_inactive_male_population": aggregated_result["pe_inac_m_colonia"],
                    "total_inactive_female_population": aggregated_result["pe_inac_f_colonia"]
                },
                "alcaldia": {
                    "total_workforce": aggregated_result["pea_alcaldia"],
                    "total_male_workforce": aggregated_result["pea_m_alcaldia"],
                    "total_female_workforce": aggregated_result["pea_f_alcaldia"],
                    "total_inactive_population": aggregated_result["pe_inac_alcaldia"],
                    "total_inactive_male_population": aggregated_result["pe_inac_m_alcaldia"],
                    "total_inactive_female_population": aggregated_result["pe_inac_f_alcaldia"]
                }
            },
            "employment": {
                "block": {
                    "total_employed_population": aggregated_result["pocupada"],
                    "total_male_employed_population": aggregated_result["pocupada_m"],
                    "total_female_emloyed_population": aggregated_result["pocupada_f"],
                    "total_unemployed_population": aggregated_result["pdesocup"],
                    "total_unemployed_male_population": aggregated_result["pdesocup_m"],
                    "total_unemployed_female_population": aggregated_result["pdesocup_f"]
                },
                "colonia": {
                    "total_employed_population": aggregated_result["pocupada_colonia"],
                    "total_male_employed_population": aggregated_result["pocupada_m_colonia"],
                    "total_female_emloyed_population": aggregated_result["pocupada_f_colonia"],
                    "total_unemployed_population": aggregated_result["pdesocup_colonia"],
                    "total_unemployed_male_population": aggregated_result["pdesocup_m_colonia"],
                    "total_unemployed_female_population": aggregated_result["pdesocup_f_colonia"] 
                },
                "alcaldia": {
                    "total_employed_population": aggregated_result["pocupada_alcaldia"],
                    "total_male_employed_population": aggregated_result["pocupada_m_alcaldia"],
                    "total_female_emloyed_population": aggregated_result["pocupada_f_alcaldia"],
                    "total_unemployed_population": aggregated_result["pdesocup_alcaldia"],
                    "total_unemployed_male_population": aggregated_result["pdesocup_m_alcaldia"],
                    "total_unemployed_female_population": aggregated_result["pdesocup_f_alcaldia"],
                }   
            },    
            "population_growth": {
                "block": { 
                    "2000": [aggregated_result.get('pob_2000_ageb', 0), 0],
                    "2005": [aggregated_result.get('pob_2005_ageb', 0), float(aggregated_result.get('cambio_porcentual_2005_ageb', 0) or 0)],
                    "2010": [aggregated_result.get('pob_2010_ageb', 0), float(aggregated_result.get('cambio_porcentual_2010_ageb', 0) or 0)],
                    "2015": [aggregated_result.get('pob_2015_ageb', 0), float(aggregated_result.get('cambio_porcentual_2015_ageb', 0) or 0)],
                    "2020": [aggregated_result.get('pob_2020_ageb', 0), float(aggregated_result.get('cambio_porcentual_2020_ageb', 0) or 0)]
                },
                "colonia": {
                    "2000": [aggregated_result.get('pob_2000_entidad', 0), 0],
                    "2005": [aggregated_result.get('pob_2005_entidad', 0), float(aggregated_result.get('cambio_porcentual_2005_entidad', 0) or 0)],
                    "2010": [aggregated_result.get('pob_2010_entidad', 0), float(aggregated_result.get('cambio_porcentual_2010_entidad', 0) or 0)],
                    "2015": [aggregated_result.get('pob_2015_entidad', 0), float(aggregated_result.get('cambio_porcentual_2015_entidad', 0) or 0)],
                    "2020": [aggregated_result.get('pob_2020_entidad', 0), float(aggregated_result.get('cambio_porcentual_2020_entidad', 0) or 0)]
                },
                "alcaldia": {
                    "2000": [aggregated_result.get('pob_2000_municipal', 0), 0],
                    "2005": [aggregated_result.get('pob_2005_municipal', 0), float(aggregated_result.get('cambio_porcentual_2005_municipal', 0) or 0)],
                    "2010": [aggregated_result.get('pob_2010_municipal', 0), float(aggregated_result.get('cambio_porcentual_2010_municipal', 0) or 0)],
                    "2015": [aggregated_result.get('pob_2015_municipal', 0), float(aggregated_result.get('cambio_porcentual_2015_municipal', 0) or 0)],
                    "2020": [aggregated_result.get('pob_2020_municipal', 0), float(aggregated_result.get('cambio_porcentual_2020_municipal', 0) or 0)]
                }
            }
        }
        
        # Calculate population density for selected area
        area_population_density = round(aggregated_result["pobtot"] / area_km2, 2) if area_km2 > 0 else 0
        
        # Calculate municipality population density dynamically from database
        municipality_code = aggregated_result.get("municipality_code")
        if municipality_code and connection:
            municipality_area_km2 = self._get_municipality_area_km2(municipality_code, connection)
        else:
            # Fallback: calculate area from available data or use a reasonable estimate
            municipality_area_km2 = self._calculate_fallback_municipality_area(aggregated_result)
        municipality_population_density = round(aggregated_result["pobtot_alcaldia"] / municipality_area_km2, 2) if municipality_area_km2 > 0 else 0
        
        # Calculate male/female percentages
        male_percentage = round((aggregated_result["pobmas"] / aggregated_result["pobtot"] * 100), 1) if aggregated_result["pobtot"] > 0 else 0
        female_percentage = round((aggregated_result["pobfem"] / aggregated_result["pobtot"] * 100), 1) if aggregated_result["pobtot"] > 0 else 0
        
        # Calculate municipality male/female percentages
        municipality_male_percentage = round((aggregated_result["pobmas_alcaldia"] / aggregated_result["pobtot_alcaldia"] * 100), 1) if aggregated_result["pobtot_alcaldia"] > 0 else 0
        municipality_female_percentage = round((aggregated_result["pobfem_alcaldia"] / aggregated_result["pobtot_alcaldia"] * 100), 1) if aggregated_result["pobtot_alcaldia"] > 0 else 0
        
        # Create age pyramid data structure for 2024
        age_pyramid_data = self._create_age_pyramid_data(aggregated_result)
        
        # Create population growth data structure for 2000-2020
        population_growth_data = self._create_population_growth_data(aggregated_result)
        
        # Structure response to match UI mockup
        demographics_data = {
            "summary": {
                "area_km2": area_km2,
                "population": aggregated_result["pobtot"],
                "population_density": area_population_density,
                "population_density_formatted": f"{area_population_density} persons / km²",
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius
            },
            "demographics": {
                "total_population": aggregated_result["pobtot"],
                "male_population": aggregated_result["pobmas"],
                "female_population": aggregated_result["pobfem"],
                "male_percentage": male_percentage,
                "female_percentage": female_percentage,
                "total_households": aggregated_result["vivtot"],
                "average_household_size": aggregated_result["prom_ocup"]
            },
            "detailed_data": demographic,  # Include the full detailed structure
            "age_pyramid_2024": age_pyramid_data,
            "population_growth_2024": population_growth_data,
            "comparison": {
                "selected_area": {
                    "population_density": area_population_density,
                    "population_density_trend": "down" if area_population_density < municipality_population_density else "up",
                    "male_population": aggregated_result["pobmas"],
                    "male_percentage": f"{male_percentage}%",
                    "male_trend": "down" if male_percentage < municipality_male_percentage else "up",
                    "female_population": aggregated_result["pobfem"],
                    "female_percentage": f"{female_percentage}%",
                    "female_trend": "up" if female_percentage > municipality_female_percentage else "down",
                    "total_households": aggregated_result["vivtot"],
                    "households_trend": "up"  # Default trend
                },
                "municipality": {
                    "population_density": municipality_population_density,
                    "male_population": aggregated_result.get("pobmas_alcaldia", 0) or 0,
                    "male_percentage": f"{municipality_male_percentage}%",
                    "female_population": aggregated_result.get("pobfem_alcaldia", 0) or 0,
                    "female_percentage": f"{municipality_female_percentage}%",
                    "total_households": aggregated_result.get("vivtot_alcaldia", 0) or 0
                }
            }
        }
        
        return demographics_data

    def _create_age_pyramid_data(self, aggregated_result):
        """Create age pyramid data structure for 2024 visualization using proportional scaling."""
        # Debug: Log available columns
        logger.info(f"Available columns in aggregated_result: {list(aggregated_result.keys())}")
        
        # Get block-level age totals and municipality-level gender ratios
        block_age_totals = {
            "0a2": aggregated_result.get("p_0a2", 0) or 0,
            "3a5": aggregated_result.get("p_3a5", 0) or 0,
            "6a11": aggregated_result.get("p_6a11", 0) or 0,
            "12a14": aggregated_result.get("p_12a14", 0) or 0,
            "15a17": aggregated_result.get("p_15a17", 0) or 0,
            "18a24": aggregated_result.get("p_18a24", 0) or 0,
            "60ymas": aggregated_result.get("p_60ymas", 0) or 0
        }
        
        # Get municipality-level gender data for ratios
        municipality_gender_data = {
            "0a2": {
                "male": aggregated_result.get("p_0a2_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_0a2_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_0a2_alcaldia", 1) or 1
            },
            "3a5": {
                "male": aggregated_result.get("p_3a5_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_3a5_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_3a5_alcaldia", 1) or 1
            },
            "6a11": {
                "male": aggregated_result.get("p_6a11_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_6a11_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_6a11_alcaldia", 1) or 1
            },
            "12a14": {
                "male": aggregated_result.get("p_12a14_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_12a14_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_12a14_alcaldia", 1) or 1
            },
            "15a17": {
                "male": aggregated_result.get("p_15a17_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_15a17_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_15a17_alcaldia", 1) or 1
            },
            "18a24": {
                "male": aggregated_result.get("p_18a24_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_18a24_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_18a24_alcaldia", 1) or 1
            },
            "60ymas": {
                "male": aggregated_result.get("p_60ymas_m_alcaldia", 0) or 0,
                "female": aggregated_result.get("p_60ymas_f_alcaldia", 0) or 0,
                "total": aggregated_result.get("p_60ymas_alcaldia", 1) or 1
            }
        }
        
        # Age groups for the pyramid with proportional scaling
        age_groups = []
        for age_key, age_range in [("0a2", "0-2"), ("3a5", "3-5"), ("6a11", "6-11"), ("12a14", "12-14"), 
                                   ("15a17", "15-17"), ("18a24", "18-24"), ("60ymas", "65+")]:
            
            block_total = block_age_totals[age_key]
            muni_data = municipality_gender_data[age_key]
            
            # Calculate gender ratios from municipality data
            if muni_data["total"] > 0:
                male_ratio = muni_data["male"] / muni_data["total"]
                female_ratio = muni_data["female"] / muni_data["total"]
            else:
                male_ratio = 0.5  # Default 50/50 split if no data
                female_ratio = 0.5
            
            # Apply ratios to block-level totals
            male_count = round(block_total * male_ratio)
            female_count = round(block_total * female_ratio)
            
            age_groups.append({
                "range": age_range,
                "male": male_count,
                "female": female_count
            })
        
        # Add missing age groups (not available in data)
        age_groups.extend([
            {"range": "25-34", "male": 0, "female": 0},
            {"range": "35-44", "male": 0, "female": 0},
            {"range": "45-54", "male": 0, "female": 0},
            {"range": "55-64", "male": 0, "female": 0}
        ])
        
        # Debug: Log the calculated values
        logger.info(f"Age pyramid - Block totals: {block_age_totals}")
        logger.info(f"Age pyramid - Calculated groups: {[(g['range'], g['male'], g['female']) for g in age_groups[:7]]}")
        
        # Calculate percentages for each age group
        total_population = aggregated_result.get("pobtot", 1) or 1
        for group in age_groups:
            group["male_percentage"] = round((group["male"] / total_population * 100), 2) if total_population > 0 else 0
            group["female_percentage"] = round((group["female"] / total_population * 100), 2) if total_population > 0 else 0
        
        return {
            "age_groups": age_groups,
            "total_population": total_population
        }

    def _create_population_growth_data(self, aggregated_result):
        """Create population growth data structure using only years available from database."""
        # Only use years that actually exist in the database
        # Based on the query: 2000, 2005, 2010, 2020 (no 2015, no interpolated 2007)
        block_growth = {
            "2000": [aggregated_result.get('pob_2000_ageb', 0), 0],  # Base year, 0% growth
            "2005": [aggregated_result.get('pob_2005_ageb', 0), float(aggregated_result.get('cambio_porcentual_2005_ageb', 0) or 0)],
            "2010": [aggregated_result.get('pob_2010_ageb', 0), float(aggregated_result.get('cambio_porcentual_2010_ageb', 0) or 0)],
            "2020": [aggregated_result.get('pob_2020_ageb', 0), float(aggregated_result.get('cambio_porcentual_2020_ageb', 0) or 0)]
        }
        
        alcaldia_growth = {
            "2000": [aggregated_result.get('pob_2000_municipal', 0), 0],  # Base year, 0% growth
            "2005": [aggregated_result.get('pob_2005_municipal', 0), float(aggregated_result.get('cambio_porcentual_2005_municipal', 0) or 0)],
            "2010": [aggregated_result.get('pob_2010_municipal', 0), float(aggregated_result.get('cambio_porcentual_2010_municipal', 0) or 0)],
            "2020": [aggregated_result.get('pob_2020_municipal', 0), float(aggregated_result.get('cambio_porcentual_2020_municipal', 0) or 0)]
        }
        
        # Only use years that exist in the database (no hardcoded 2007 or missing 2015)
        years = [2000, 2005, 2010, 2020]
        area_data = []
        municipality_data = []
        
        for year in years:
            year_str = str(year)
            if year_str in block_growth:
                area_growth = block_growth[year_str][1] if len(block_growth[year_str]) > 1 else 0
            else:
                area_growth = 0
                
            if year_str in alcaldia_growth:
                municipality_growth = alcaldia_growth[year_str][1] if len(alcaldia_growth[year_str]) > 1 else 0
            else:
                municipality_growth = 0
            
            area_data.append({
                "year": year,
                "growth_percentage": round(area_growth, 1)
            })
            municipality_data.append({
                "year": year,
                "growth_percentage": round(municipality_growth, 1)
            })
        
        return {
            "area": area_data,
            "municipality": municipality_data,
            "years": years
        }

    def _calculate_fallback_municipality_area(self, aggregated_result):
        """Calculate fallback municipality area when database query fails."""
        # Try to estimate based on population density patterns
        population = aggregated_result.get("pobtot_alcaldia", 0)
        if population > 0:
            # Estimate area based on typical population density patterns
            # Mexico City average density is around 6,000 people/km²
            estimated_area = population / 6000
            return max(estimated_area, 10.0)  # Minimum 10 km²
        else:
            return self._get_default_municipality_area()

    def _estimate_municipality_area_from_population(self, population):
        """Estimate municipality area based on population size."""
        if population > 0:
            # Estimate area based on typical population density patterns
            # Mexico City average density is around 6,000 people/km²
            estimated_area = population / 6000
            return max(estimated_area, 10.0)  # Minimum 10 km²
        else:
            return self._get_default_municipality_area()

    def _interpolate_growth_for_year(self, growth_data, target_year):
        """Interpolate growth percentage for a specific year based on available data."""
        # Get available years and their growth rates
        available_years = []
        growth_rates = []
        
        for year_str, data in growth_data.items():
            if len(data) > 1 and data[1] is not None:
                available_years.append(int(year_str))
                growth_rates.append(data[1])
        
        if len(available_years) < 2:
            return 0.0  # Not enough data for interpolation
        
        # Sort by year
        sorted_data = sorted(zip(available_years, growth_rates))
        years, rates = zip(*sorted_data)
        
        # Find the two closest years for interpolation
        if target_year <= years[0]:
            return rates[0]
        elif target_year >= years[-1]:
            return rates[-1]
        else:
            # Linear interpolation between two points
            for i in range(len(years) - 1):
                if years[i] <= target_year <= years[i + 1]:
                    # Linear interpolation
                    x1, y1 = years[i], rates[i]
                    x2, y2 = years[i + 1], rates[i + 1]
                    interpolated = y1 + (y2 - y1) * (target_year - x1) / (x2 - x1)
                    return round(interpolated, 1)
        
        return 0.0

    def _get_default_municipality_area(self):
        """Get default municipality area based on typical Mexico City patterns."""
        # Use a reasonable default based on typical Mexico City municipality sizes
        # Most municipalities range from 20-50 km², with 32.44 being a reasonable median
        return 32.44

    def _get_municipality_area_km2(self, municipality_code, connection):
        """Calculate municipality area in km² from database data."""
        cursor = None
        try:
            cursor = connection.cursor()
            
            # Query to get total area of all parcels in the municipality
            query = """
            SELECT SUM(total_area) as total_area_m2
            FROM blackprint_db_prd.data_product.v_parcel_v3 
            WHERE municipality_code = %s
            AND total_area IS NOT NULL
            AND total_area > 0
            """
            
            cursor.execute(query, (municipality_code,))
            aggregated_result = cursor.fetchone()
            
            if aggregated_result and aggregated_result[0]:
                # Convert from square meters to square kilometers
                total_area_m2 = float(aggregated_result[0])
                total_area_km2 = total_area_m2 / 1_000_000  # Convert m² to km²
                return round(total_area_km2, 2)
            else:
                # Fallback: estimate based on population density patterns
                return self._estimate_municipality_area_from_population(aggregated_result.get("pobtot_alcaldia", 0))
                
        except Exception as e:
            logger.error(f"Error calculating municipality area: {str(e)}")
            return self._estimate_municipality_area_from_population(0)
        finally:
            if cursor:
                cursor.close()

    def _process_queretaro_demographics_data(self, demographic_data, lat, lng, radius, connection=None):
        """
        Process Queretaro demographic data from the new aggregated query.
        The query now returns a single pre-aggregated record with all calculations already done.
        """
        # Spatial filtering and aggregation are now handled in the SQL query
        if not demographic_data:
            logger.warning("No Queretaro demographic data found")
            return self._get_empty_demographics_structure(lat, lng, radius)
        
        # Get the single aggregated result (no need for aggregation since query does it)
        aggregated_result = demographic_data[0]  # Query returns single pre-aggregated record
        
        # All calculations are now done in the SQL query, just extract the values
        area_km2 = aggregated_result.get("area_km2", 0)
        area_population_density = aggregated_result.get("population_density", 0)
        male_percentage = aggregated_result.get("male_percentage")
        female_percentage = aggregated_result.get("female_percentage")
        municipality_male_percentage = aggregated_result.get("municipality_male_percentage")
        municipality_female_percentage = aggregated_result.get("municipality_female_percentage")
        avg_household_size = aggregated_result.get("average_household_size", 0)
        
        # Calculate municipality population density if needed (fallback for older data)
        municipality_code = aggregated_result.get("municipality_code")
        if municipality_code and connection:
            municipality_area_km2 = self._get_municipality_area_km2(municipality_code, connection)
            municipality_population_density = round(aggregated_result["pobtot_alcaldia"] / municipality_area_km2, 2) if municipality_area_km2 > 0 and aggregated_result.get("pobtot_alcaldia") else 0
        else:
            # Use a reasonable estimate for municipality density
            municipality_population_density = area_population_density * 0.8  # Approximate fallback
        
        # Create age pyramid data
        age_pyramid_data = self._create_age_pyramid_data(aggregated_result)
        
        # Create population growth data
        population_growth_data = self._create_population_growth_data(aggregated_result)
        
        # Structure the response to match the UI design
        demographics_data = {
            "summary": {
                "area_km2": area_km2,
                "population": aggregated_result["pobtot"],
                "population_density": area_population_density,
                "population_density_formatted": f"{area_population_density} persons / km²",
                "center_point": {
                    "lat": lat,
                    "lng": lng
                },
                "radius_meters": radius
            },
            "demographics": {
                "total_population": aggregated_result["pobtot"],
                "male_population": aggregated_result["pobmas"],
                "female_population": aggregated_result["pobfem"],
                "male_percentage": male_percentage,
                "female_percentage": female_percentage,
                "total_households": aggregated_result["vivtot"],
                "average_household_size": avg_household_size
            },
            "detailed_data": {
                "general": {
                    "block": {
                        "neighborhood": aggregated_result["neighborhood"],
                        "predominant_level": aggregated_result["predominant_level"],
                        "ageb_code": aggregated_result["ageb_code"],
                        "total_household": aggregated_result["vivtot"],
                        "average_household_size": avg_household_size,
                        "average_number_of_rooms": aggregated_result.get("pro_ocup_c")  # Use actual data if available
                    },
                    "colonia": {
                        "neighborhood": aggregated_result["neighborhood"],
                        "predominant_level": aggregated_result["predominant_level"],
                        "ageb_code": aggregated_result["ageb_code"],
                        "total_household": aggregated_result["vivtot"],
                        "average_household_size": avg_household_size,
                        "average_number_of_rooms": aggregated_result.get("pro_ocup_c")  # Use actual data if available
                    },
                    "alcaldia": {
                        "neighborhood": aggregated_result["nom_mun"],
                        "predominant_level": aggregated_result["predominant_level"],
                        "ageb_code": aggregated_result["ageb_code"],
                        "total_household": aggregated_result["vivtot"],  # Use same as selected area for Queretaro
                        "average_household_size": avg_household_size,  # Use same as selected area
                        "average_number_of_rooms": aggregated_result.get("pro_ocup_c")  # Use actual data if available
                    }
                },
                "socio_economic_level": {
                    "block": {
                        "ses_ab": aggregated_result["ses_ab"] or 0,
                        "ses_c_plus": aggregated_result["ses_c_plus"] or 0,
                        "ses_c": aggregated_result["ses_c"] or 0,
                        "ses_c_minus": aggregated_result["ses_c_minus"] or 0,
                        "ses_d": aggregated_result["ses_d"] or 0,
                        "ses_d_plus": aggregated_result["ses_d_plus"] or 0,
                        "ses_e": aggregated_result["ses_e"] or 0
                    },
                    "colonia": {
                        "ses_ab": aggregated_result["ses_ab"] or 0,
                        "ses_c_plus": aggregated_result["ses_c_plus"] or 0,
                        "ses_c": aggregated_result["ses_c"] or 0,
                        "ses_c_minus": aggregated_result["ses_c_minus"] or 0,
                        "ses_d": aggregated_result["ses_d"] or 0,
                        "ses_d_plus": aggregated_result["ses_d_plus"] or 0,
                        "ses_e": aggregated_result["ses_e"] or 0
                    },
                    "alcaldia": {
                        "ses_ab": aggregated_result["ses_ab"] or 0,  # Use same as selected area for Queretaro
                        "ses_c_plus": aggregated_result["ses_c_plus"] or 0,
                        "ses_c": aggregated_result["ses_c"] or 0,
                        "ses_c_minus": aggregated_result["ses_c_minus"] or 0,
                        "ses_d": aggregated_result["ses_d"] or 0,
                        "ses_d_plus": aggregated_result["ses_d_plus"] or 0,
                        "ses_e": aggregated_result["ses_e"] or 0
                    }
                },
                "population": {
                    "block": {
                        "total_population": aggregated_result["pobtot"],
                        "male_population": aggregated_result["pobmas"],
                        "female_population": aggregated_result["pobfem"]
                    },
                    "colonia": {
                        "total_population": aggregated_result["pobtot"],
                        "male_population": aggregated_result["pobmas"],
                        "female_population": aggregated_result["pobfem"]
                    },
                    "alcaldia": {
                        "total_population": aggregated_result["pobtot"],  # Use same as selected area for Queretaro
                        "male_population": aggregated_result["pobmas"],
                        "female_population": aggregated_result["pobfem"]
                    }
                },
                "education": {
                    "block": {
                        "education_3_5": aggregated_result["p_3a5"] or 0,
                        "education_6_11": aggregated_result["p_6a11"] or 0,
                        "education_12_14": aggregated_result["p_12a14"] or 0,
                        "education_15_17": aggregated_result["p_15a17"] or 0,
                        "education_18_24": aggregated_result["p_18a24"] or 0,
                        "education_3_5_attending_school": aggregated_result["p3a5_noa"] or 0,
                        "education_6_11_attending_school": aggregated_result["p6a11_noa"] or 0,
                        "education_12_14_attending_school": aggregated_result["p12a14noa"] or 0,
                        "education_15_17_attending_school": aggregated_result["p15a17a"] or 0,
                        "education_18_24_attending_school": aggregated_result["p18a24a"] or 0
                    },
                    "colonia": {
                        "education_3_5": aggregated_result["p_3a5"] or 0,
                        "education_6_11": aggregated_result["p_6a11"] or 0,
                        "education_12_14": aggregated_result["p_12a14"] or 0,
                        "education_15_17": aggregated_result["p_15a17"] or 0,
                        "education_18_24": aggregated_result["p_18a24"] or 0,
                        "education_3_5_attending_school": aggregated_result["p3a5_noa"] or 0,
                        "education_6_11_attending_school": aggregated_result["p6a11_noa"] or 0,
                        "education_12_14_attending_school": aggregated_result["p12a14noa"] or 0,
                        "education_15_17_attending_school": aggregated_result["p15a17a"] or 0,
                        "education_18_24_attending_school": aggregated_result["p18a24a"] or 0
                    },
                    "alcaldia": {
                        "education_3_5": aggregated_result.get("p_3a5"),
                        "education_6_11": aggregated_result.get("p_6a11"),
                        "education_12_14": aggregated_result.get("p_12a14"),
                        "education_15_17": aggregated_result.get("p_15a17"),
                        "education_18_24": aggregated_result.get("p_18a24"),
                        "education_3_5_attending_school": aggregated_result.get("p3a5_noa"),
                        "education_6_11_attending_school": aggregated_result.get("p6a11_noa"),
                        "education_12_14_attending_school": aggregated_result.get("p12a14noa"),
                        "education_15_17_attending_school": aggregated_result.get("p15a17a"),
                        "education_18_24_attending_school": aggregated_result.get("p18a24a")
                    }
                },
                "workforce": {
                    "block": {
                        "total_workforce": aggregated_result["pea"] or 0,
                        "total_male_workforce": aggregated_result["pea_m"] or 0,
                        "total_female_workforce": aggregated_result["pea_f"] or 0,
                        "total_inactive_population": aggregated_result["pe_inac"] or 0,
                        "total_inactive_male_population": aggregated_result["pe_inac_m"] or 0,
                        "total_inactive_female_population": aggregated_result["pe_inac_f"] or 0
                    },
                    "colonia": {
                        "total_workforce": aggregated_result["pea"] or 0,
                        "total_male_workforce": aggregated_result["pea_m"] or 0,
                        "total_female_workforce": aggregated_result["pea_f"] or 0,
                        "total_inactive_population": aggregated_result["pe_inac"] or 0,
                        "total_inactive_male_population": aggregated_result["pe_inac_m"] or 0,
                        "total_inactive_female_population": aggregated_result["pe_inac_f"] or 0
                    },
                    "alcaldia": {
                        "total_workforce": aggregated_result.get("pea"),
                        "total_male_workforce": aggregated_result.get("pea_m"),
                        "total_female_workforce": aggregated_result.get("pea_f"),
                        "total_inactive_population": aggregated_result.get("pe_inac"),
                        "total_inactive_male_population": aggregated_result.get("pe_inac_m"),
                        "total_inactive_female_population": aggregated_result.get("pe_inac_f")
                    }
                },
                "employment": {
                    "block": {
                        "total_employed_population": aggregated_result["pocupada"] or 0,
                        "total_male_employed_population": aggregated_result["pocupada_m"] or 0,
                        "total_female_emloyed_population": aggregated_result["pocupada_f"] or 0,
                        "total_unemployed_population": aggregated_result["pdesocup"] or 0,
                        "total_unemployed_male_population": aggregated_result["pdesocup_m"] or 0,
                        "total_unemployed_female_population": aggregated_result["pdesocup_f"] or 0
                    },
                    "colonia": {
                        "total_employed_population": aggregated_result["pocupada"] or 0,
                        "total_male_employed_population": aggregated_result["pocupada_m"] or 0,
                        "total_female_emloyed_population": aggregated_result["pocupada_f"] or 0,
                        "total_unemployed_population": aggregated_result["pdesocup"] or 0,
                        "total_unemployed_male_population": aggregated_result["pdesocup_m"] or 0,
                        "total_unemployed_female_population": aggregated_result["pdesocup_f"] or 0
                    },
                    "alcaldia": {
                        "total_employed_population": aggregated_result.get("pocupada"),
                        "total_male_employed_population": aggregated_result.get("pocupada_m"),
                        "total_female_emloyed_population": aggregated_result.get("pocupada_f"),
                        "total_unemployed_population": aggregated_result.get("pdesocup"),
                        "total_unemployed_male_population": aggregated_result.get("pdesocup_m"),
                        "total_unemployed_female_population": aggregated_result.get("pdesocup_f")
                    }
                },
                "population_growth": {
                    "block": {
                        "2000": [aggregated_result.get("pob_2000_ageb"), 0],
                        "2005": [aggregated_result.get("pob_2005_ageb"), aggregated_result.get("cambio_porcentual_2005_ageb")],
                        "2010": [aggregated_result.get("pob_2010_ageb"), aggregated_result.get("cambio_porcentual_2010_ageb")],
                        "2015": [aggregated_result.get("pob_2015_ageb"), aggregated_result.get("cambio_porcentual_2015_ageb")],
                        "2020": [aggregated_result.get("pob_2020_ageb"), aggregated_result.get("cambio_porcentual_2020_ageb")]
                    },
                    "colonia": {
                        "2000": [aggregated_result.get("pob_2000_ageb"), 0],
                        "2005": [aggregated_result.get("pob_2005_ageb"), aggregated_result.get("cambio_porcentual_2005_ageb")],
                        "2010": [aggregated_result.get("pob_2010_ageb"), aggregated_result.get("cambio_porcentual_2010_ageb")],
                        "2015": [aggregated_result.get("pob_2015_ageb"), aggregated_result.get("cambio_porcentual_2015_ageb")],
                        "2020": [aggregated_result.get("pob_2020_ageb"), aggregated_result.get("cambio_porcentual_2020_ageb")]
                    },
                    "alcaldia": {
                        "2000": [aggregated_result.get("pob_2000_municipal"), 0],
                        "2005": [aggregated_result.get("pob_2005_municipal"), aggregated_result.get("cambio_porcentual_2005_municipal")],
                        "2010": [aggregated_result.get("pob_2010_municipal"), aggregated_result.get("cambio_porcentual_2010_municipal")],
                        "2015": [aggregated_result.get("pob_2015_municipal"), aggregated_result.get("cambio_porcentual_2015_municipal")],
                        "2020": [aggregated_result.get("pob_2020_municipal"), aggregated_result.get("cambio_porcentual_2020_municipal")]
                    }
                }
            },
            "age_pyramid_2024": age_pyramid_data,
            "population_growth_2024": population_growth_data,
            "comparison": {
                "selected_area": {
                    "population_density": area_population_density,
                    "population_density_trend": "down" if area_population_density < municipality_population_density else "up",
                    "male_population": aggregated_result["pobmas"],
                    "male_percentage": f"{male_percentage}%",
                    "male_trend": "down" if male_percentage < municipality_male_percentage else "up",
                    "female_population": aggregated_result["pobfem"],
                    "female_percentage": f"{female_percentage}%",
                    "female_trend": "up" if female_percentage > municipality_female_percentage else "down",
                    "total_households": aggregated_result["vivtot"],
                    "households_trend": "up"  # Default trend
                },
                "municipality": {
                    "population_density": municipality_population_density,
                    "male_population": aggregated_result["pobmas_alcaldia"],
                    "male_percentage": f"{municipality_male_percentage}%",
                    "female_population": aggregated_result["pobfem_alcaldia"],
                    "female_percentage": f"{municipality_female_percentage}%",
                    "total_households": aggregated_result["vivtot_alcaldia"]
                }
            },
            "socioeconomic_analysis": self._create_queretaro_socioeconomic_analysis(aggregated_result)
        }
        
        return demographics_data

    def _aggregate_queretaro_demographic_data(self, demographic_data):
        """Aggregate Queretaro demographic data from multiple records using data_product.v_qro table."""
        if not demographic_data:
            return {}
        
        if len(demographic_data) == 1:
            return demographic_data[0]
        
        # Aggregate multiple records
        aggregated = {}
        
        # Fields that should be summed (counts) - Block level only
        count_fields = [
            'pea', 'pea_m', 'pea_f', 
            'pe_inac', 'pe_inac_m', 'pe_inac_f', 'pocupada', 'pocupada_m', 
            'pocupada_f', 'pdesocup', 'pdesocup_m', 'pdesocup_f',
            'p_3a5', 'p_6a11', 'p_12a14', 'p_15a17', 'p_18a24', 'p_60ymas',
            'p3a5_noa', 'p6a11_noa', 'p12a14noa', 'p15a17a', 'p18a24a',
            'p_0a2', 'p_0a2_m', 'p_0a2_f', 'p_3a5_m', 'p_3a5_f', 'p_6a11_m', 
            'p_6a11_f', 'p_12a14_m', 'p_12a14_f', 'p_15a17_m', 'p_15a17_f', 
            'p_18a24_m', 'p_18a24_f', 'p_60ymas_m', 'p_60ymas_f',
            'pob_2000_ageb', 'pob_2005_ageb', 'pob_2010_ageb', 'pob_2015_ageb', 'pob_2020_ageb'
        ]
        
        # Fields that should be averaged (percentages) - Block level
        ses_fields = [
            'ses_ab', 'ses_c_plus', 'ses_c', 'ses_c_minus', 'ses_d_plus', 'ses_d', 'ses_e',
            'cambio_porcentual_2005_ageb', 'cambio_porcentual_2010_ageb', 'cambio_porcentual_2015_ageb', 'cambio_porcentual_2020_ageb'
        ]
        
        # Sum count fields
        for field in count_fields:
            aggregated[field] = sum(record.get(field, 0) or 0 for record in demographic_data)
        
        # Calculate male/female population and households for selected area (sum across records)
        # The query maps pobmas_alcaldia as pobmas and pobfem_alcaldia as pobfem
        aggregated['pobmas'] = sum(record.get('pobmas', 0) or 0 for record in demographic_data)
        aggregated['pobfem'] = sum(record.get('pobfem', 0) or 0 for record in demographic_data)
        # Calculate total population as sum of male + female
        aggregated['pobtot'] = aggregated['pobmas'] + aggregated['pobfem']
        # The query maps tot_vivien as vivtot
        aggregated['vivtot'] = sum(record.get('vivtot', 0) or 0 for record in demographic_data)
        
        # Average SES fields
        for field in ses_fields:
            values = [record.get(field, 0) or 0 for record in demographic_data if record.get(field) is not None]
            aggregated[field] = sum(values) / len(values) if values else 0
        
        # Non-numeric fields (take from first record)
        non_numeric_fields = [
            'neighborhood', 'predominant_level', 'ageb_code', 'nom_mun', 
            'municipality_code', 'municipality_nm', 'centroid', 'total_area'
        ]
        
        first_record = demographic_data[0]
        for field in non_numeric_fields:
            aggregated[field] = first_record.get(field)
        
        # Municipality level data - use actual data from v_qro table
        municipality_fields = [
            'pobtot_alcaldia', 'pobmas_alcaldia', 'pobfem_alcaldia', 'vivtot_alcaldia',
            'pea_alcaldia', 'pea_m_alcaldia', 'pea_f_alcaldia', 'pe_inac_alcaldia', 
            'pe_inac_m_alcaldia', 'pe_inac_f_alcaldia', 'pocupada_alcaldia', 'pocupada_m_alcaldia', 
            'pocupada_f_alcaldia', 'pdesocup_alcaldia', 'pdesocup_m_alcaldia', 'pdesocup_f_alcaldia',
            'p_3a5_alcaldia', 'p_6a11_alcaldia', 'p_12a14_alcaldia', 'p_15a17_alcaldia', 'p_18a24_alcaldia',
            'p3a5_noa_alcaldia', 'p6a11_noa_alcaldia', 'p12a14noa_alcaldia', 'p15a17a_alcaldia', 'p18a24a_alcaldia',
            'p_0a2_alcaldia', 'p_0a2_m_alcaldia', 'p_0a2_f_alcaldia', 'p_3a5_m_alcaldia', 'p_3a5_f_alcaldia',
            'p_6a11_m_alcaldia', 'p_6a11_f_alcaldia', 'p_12a14_m_alcaldia', 'p_12a14_f_alcaldia',
            'p_15a17_m_alcaldia', 'p_15a17_f_alcaldia', 'p_18a24_m_alcaldia', 'p_18a24_f_alcaldia',
            'p_60ymas_alcaldia', 'p_60ymas_m_alcaldia', 'p_60ymas_f_alcaldia',
            'pob_2000_municipal', 'pob_2005_municipal', 'pob_2010_municipal', 'pob_2015_municipal', 'pob_2020_municipal',
            'prom_ocup_alcaldia', 'pro_ocup_c_alcaldia'
        ]
        
        for field in municipality_fields:
            aggregated[field] = first_record.get(field)
        
        # Municipality level SES data (percentages)
        municipality_ses_fields = [
            'ses_ab_alcaldia', 'ses_c_plus_alcaldia', 'ses_c_alcaldia', 'ses_c_minus_alcaldia',
            'ses_d_plus_alcaldia', 'ses_d_alcaldia', 'ses_e_alcaldia',
            'cambio_porcentual_2005_municipal', 'cambio_porcentual_2010_municipal', 
            'cambio_porcentual_2015_municipal', 'cambio_porcentual_2020_municipal'
        ]
        
        for field in municipality_ses_fields:
            aggregated[field] = first_record.get(field)
        
        # Colonia level data - use actual data from v_qro table
        colonia_fields = [
            'pobtot_colonia', 'pobmas_colonia', 'pobfem_colonia', 'vivtot_colonia',
            'pea_colonia', 'pea_m_colonia', 'pea_f_colonia', 'pe_inac_colonia', 
            'pe_inac_m_colonia', 'pe_inac_f_colonia', 'pocupada_colonia', 'pocupada_m_colonia', 
            'pocupada_f_colonia', 'pdesocup_colonia', 'pdesocup_m_colonia', 'pdesocup_f_colonia',
            'p_3a5_colonia', 'p_6a11_colonia', 'p_12a14_colonia', 'p_15a17_colonia', 'p_18a24_colonia',
            'p3a5_noa_colonia', 'p6a11_noa_colonia', 'p12a14noa_colonia', 'p15a17a_colonia', 'p18a24a_colonia',
            'p_0a2_colonia', 'p_0a2_m_colonia', 'p_0a2_f_colonia', 'p_3a5_m_colonia', 'p_3a5_f_colonia',
            'p_6a11_m_colonia', 'p_6a11_f_colonia', 'p_12a14_m_colonia', 'p_12a14_f_colonia',
            'p_15a17_m_colonia', 'p_15a17_f_colonia', 'p_18a24_m_colonia', 'p_18a24_f_colonia',
            'p_60ymas_colonia', 'p_60ymas_m_colonia', 'p_60ymas_f_colonia',
            'prom_ocup_colonia', 'pro_ocup_c_colonia'
        ]
        
        for field in colonia_fields:
            aggregated[field] = first_record.get(field)
        
        # Colonia level SES data (percentages)
        colonia_ses_fields = [
            'ses_ab_colonia', 'ses_c_plus_colonia', 'ses_c_colonia', 'ses_c_minus_colonia',
            'ses_d_plus_colonia', 'ses_d_colonia', 'ses_e_colonia'
        ]
        
        for field in colonia_ses_fields:
            aggregated[field] = first_record.get(field)
        
        # State level population growth data
        state_fields = [
            'pob_2000_entidad', 'pob_2005_entidad', 'pob_2010_entidad', 'pob_2015_entidad', 'pob_2020_entidad',
            'cambio_porcentual_2005_entidad', 'cambio_porcentual_2010_entidad', 
            'cambio_porcentual_2015_entidad', 'cambio_porcentual_2020_entidad'
        ]
        
        for field in state_fields:
            aggregated[field] = first_record.get(field)

        return aggregated

    def _create_queretaro_socioeconomic_analysis(self, aggregated_result):
        """Create socioeconomic analysis for Queretaro demographics API."""
        # Calculate total households for selected area
        total_households = aggregated_result.get('vivtot', 0) or 0
        
        # SES levels mapping
        ses_levels = [
            {'key': 'ses_ab', 'level': 'AB'},
            {'key': 'ses_c_plus', 'level': 'C+'},
            {'key': 'ses_c', 'level': 'C'},
            {'key': 'ses_c_minus', 'level': 'C-'},
            {'key': 'ses_d_plus', 'level': 'D+'},
            {'key': 'ses_d', 'level': 'D'},
            {'key': 'ses_e', 'level': 'E'}
        ]
        
        # Calculate household distribution for selected area
        household_distribution = []
        predominant_level = None
        max_percentage = 0
        
        for ses in ses_levels:
            percentage = aggregated_result.get(ses['key'], 0) or 0
            households = round((percentage * total_households) / 100) if total_households > 0 else 0
            
            household_distribution.append({
                'level': ses['level'],
                'households': households,
                'percentage': round(percentage, 1)
            })
            
            if percentage > max_percentage:
                max_percentage = percentage
                predominant_level = ses['level']
        
        # Municipality level data
        municipality_total_households = aggregated_result.get('vivtot_alcaldia', 0) or 0
        municipality_predominant_level = None
        municipality_max_percentage = 0
        
        # Find municipality predominant level
        for ses in ses_levels:
            municipality_percentage = aggregated_result.get(f"{ses['key']}_alcaldia", 0) or 0
            if municipality_percentage > municipality_max_percentage:
                municipality_max_percentage = municipality_percentage
                municipality_predominant_level = ses['level']
        
        return {
            'predominant_socioeconomic_level': {
                'selected_area': {
                    'level': predominant_level or 'N/A',
                    'percentage': round(max_percentage, 1)
                },
                'municipality': {
                    'level': municipality_predominant_level or 'N/A',
                    'percentage': round(municipality_max_percentage, 1)
                }
            },
            'households_per_level': household_distribution
        }


    def _process_socioeconomic_data(self, socioeconomic_data, lat, lng, radius, config_city='mexico'):
        
        # Filter records by distance for Mexico (since we can't do it in SQL)
        if config_city != 'queretaro':
            filtered_data = []
            radius_degrees = radius / 111000.0
            
            for record in socioeconomic_data:
                try:
                    centroid_str = record.get('centroid', '')
                    if centroid_str and 'coordinates' in centroid_str:
                        import re
                        coords_match = re.search(r'\[([^,]+),\s*([^\]]+)\]', centroid_str)
                        if coords_match:
                            record_lng = float(coords_match.group(1))
                            record_lat = float(coords_match.group(2))
                            
                            if (abs(record_lat - lat) <= radius_degrees and 
                                abs(record_lng - lng) <= radius_degrees):
                                filtered_data.append(record)
                except (ValueError, AttributeError):
                    continue
            
            socioeconomic_data = filtered_data
            logger.info(f"Filtered to {len(socioeconomic_data)} records within radius for Mexico")
        
        # Initialize totals
        total_population = 0
        total_households = 0
        
        # Socioeconomic level totals
        ses_totals = {
            "AB": 0, "C+": 0, "C": 0, "C-": 0, "D+": 0, "D": 0, "E": 0
        }
        
        # Process each record
        for record in socioeconomic_data:
            pop = record.get('total_population', 0) or 0
            households = record.get('total_households', 0) or 0
            
            total_population += pop
            total_households += households
            
            if households > 0:
                # Calculate actual household counts from percentages
                ses_totals["AB"] += (record.get('pct_viv_ab', 0) or 0) * households / 100
                ses_totals["C+"] += (record.get('pct_viv_cp', 0) or 0) * households / 100
                ses_totals["C"] += (record.get('pct_viv_c', 0) or 0) * households / 100
                ses_totals["C-"] += (record.get('pct_viv_cm', 0) or 0) * households / 100
                ses_totals["D+"] += (record.get('pct_viv_dp', 0) or 0) * households / 100
                ses_totals["D"] += (record.get('pct_viv_d', 0) or 0) * households / 100
                ses_totals["E"] += (record.get('pct_viv_e', 0) or 0) * households / 100
        
        # Calculate area
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        
        # Find predominant level
        predominant_level_name = max(ses_totals, key=ses_totals.get) if ses_totals else "N/A"
        predominant_households = int(ses_totals.get(predominant_level_name, 0))
        predominant_percentage = round((predominant_households / total_households * 100), 1) if total_households > 0 else 0
        
        # Create household distribution for chart
        household_distribution = []
        for level in ["AB", "C+", "C", "C-", "D+", "D", "E"]:
            count = int(ses_totals[level])
            percentage = round((count / total_households * 100), 1) if total_households > 0 else 0
            
            household_distribution.append({
                "level": level,
                "households": count,
                "percentage": percentage
            })
        
        # Calculate income estimates (based on typical SES income ranges in Mexico)
        income_estimates = {
            "AB": 50000,  # High income
            "C+": 25000,  # Upper middle
            "C": 15000,   # Middle
            "C-": 10000,  # Lower middle
            "D+": 7000,   # Lower
            "D": 5000,    # Low
            "E": 3000     # Very low
        }
        
        # Calculate weighted average income
        total_income_weighted = 0
        for level, households in ses_totals.items():
            total_income_weighted += households * income_estimates[level]
        
        average_income = round(total_income_weighted / total_households) if total_households > 0 else 0
        total_income = round(total_income_weighted)
        
        # Structure response to match UI mockup exactly
        socioeconomic_response = {
            "summary": {
                "area_km2": area_km2,
                "population": total_population,
                "total_households": total_households,
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius
            },
            "income": {
                "average_income": {
                    "amount": average_income,
                    "currency": "MXN",
                    "formatted": f"${average_income:,} MXN",
                    "trend": None  # Not available - would need historical data
                },
                "total_income": {
                    "amount": total_income,
                    "currency": "MXN",
                    "formatted": f"${total_income/1000000:,.1f}M MXN" if total_income > 0 else "$0 MXN",
                    "trend": None  # Not available - would need historical data
                }
            },
            "predominant_socioeconomic_level": {
                "selected_area": {
                    "level": predominant_level_name,
                    "percentage": predominant_percentage,
                    "households": predominant_households
                },
                "municipality_average": {
                    "level": "B",  # Static - would need municipality-wide data
                    "percentage": None  # Not available
                }
            },
            "household_distribution": {
                "levels": household_distribution,
                "chart_data": {
                    "labels": ["AB", "C+", "C", "C-", "D+", "D", "E"],
                    "values": [ses_totals[level] for level in ["AB", "C+", "C", "C-", "D+", "D", "E"]],
                    "percentages": [round((ses_totals[level] / total_households * 100), 1) if total_households > 0 else 0 
                                  for level in ["AB", "C+", "C", "C-", "D+", "D", "E"]],
                    "colors": ["#22c55e", "#84cc16", "#eab308", "#f59e0b", "#ef4444", "#dc2626", "#991b1b"]  # Green to red gradient
                }
            }
        }
        
        return socioeconomic_response

    def _get_empty_socioeconomic_structure(self, lat, lng, radius):
        """Return empty socioeconomic structure when no data is found."""
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        
        return {
            "summary": {
                "area_km2": area_km2,
                "population": 0,
                "total_households": 0,
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius
            },
            "income": {
                "average_income": {"amount": 0, "currency": "MXN", "formatted": "$0 MXN", "trend": None},
                "total_income": {"amount": 0, "currency": "MXN", "formatted": "$0 MXN", "trend": None}
            },
            "predominant_socioeconomic_level": {
                "selected_area": {"level": "N/A", "percentage": 0, "households": 0},
                "municipality_average": {"level": "N/A", "percentage": None}
            },
            "household_distribution": {
                "levels": [],
                "chart_data": {"labels": [], "values": [], "percentages": [], "colors": []}
            }
        }


    # def _process_demographics_data(self, demographic_data, lat, lng, radius, config_city='mexico'):
        """Process demographic data and create response structure matching UI mockup."""
        # Initialize totals
        total_population = 0
        total_male = 0
        total_female = 0
        
        # Age group totals
        age_totals = {
            '0-14': {'male': 0, 'female': 0},
            '15-24': {'male': 0, 'female': 0},
            '25-59': {'male': 0, 'female': 0},
            '60+': {'male': 0, 'female': 0}
        }
        
        # Income and education totals
        total_income = 0
        total_education_level = 0
        total_households = 0
        
        # Filter records by distance for Mexico (since we can't do it in SQL)
        if config_city != 'queretaro':
            filtered_data = []
            radius_degrees = radius / 111000.0  # Convert meters to degrees
            
            for record in demographic_data:
                try:
                    # Parse centroid JSON string to extract coordinates
                    centroid_str = record.get('centroid', '')
                    if centroid_str and 'coordinates' in centroid_str:
                        # Simple string parsing since JSON type is not available
                        import re
                        coords_match = re.search(r'\[([^,]+),\s*([^\]]+)\]', centroid_str)
                        if coords_match:
                            record_lng = float(coords_match.group(1))
                            record_lat = float(coords_match.group(2))
                            
                            # Check if within radius (simple bounding box)
                            if (abs(record_lat - lat) <= radius_degrees and 
                                abs(record_lng - lng) <= radius_degrees):
                                filtered_data.append(record)
                except (ValueError, AttributeError):
                    continue
            
            demographic_data = filtered_data
            logger.info(f"Filtered to {len(demographic_data)} records within radius for Mexico")
        
        # Process each record
        for record in demographic_data:
            # Population totals
            pop = record.get('total_population', 0) or 0
            total_population += pop
            
            if config_city == 'queretaro':
                # QRO data structure - use available age group columns
                child_pop = record.get('child_population', 0) or 0  # 0-14 years
                working_pop = record.get('working_age_population', 0) or 0  # 15-64 years
                elderly_pop = record.get('elderly_population', 0) or 0  # 65+ years
                
                # Use existing age group data from the table
                # 0-14 age group (both male and female combined)
                age_totals['0-14']['male'] += child_pop // 2  # Approximate split
                age_totals['0-14']['female'] += child_pop - (child_pop // 2)
                
                # For specific age ranges, use the available columns
                p_15a17 = record.get('p_15a17', 0) or 0
                p_18a24 = record.get('p_18a24', 0) or 0
                p_60ymas = record.get('p_60ymas', 0) or 0
                
                # 15-24 age group
                age_15_24 = p_15a17 + p_18a24
                age_totals['15-24']['male'] += age_15_24 // 2  # Approximate split
                age_totals['15-24']['female'] += age_15_24 - (age_15_24 // 2)
                
                # 60+ age group  
                age_totals['60+']['male'] += p_60ymas // 2  # Approximate split
                age_totals['60+']['female'] += p_60ymas - (p_60ymas // 2)
                
                # 25-59 age group (derived from working age minus 15-24)
                age_25_59 = working_pop - age_15_24
                age_totals['25-59']['male'] += max(0, age_25_59 // 2)
                age_totals['25-59']['female'] += max(0, age_25_59 - (age_25_59 // 2))
                
                # Calculate male/female totals (approximate split for QRO)
                total_male += pop // 2
                total_female += pop - (pop // 2)
                
                # Household data
                if record.get('tot_vivien'):
                    total_households += record.get('tot_vivien', 0)
                    
            else:
                # Mexico data structure - has actual male/female population columns
                male_pop = record.get('male_population', 0) or 0
                female_pop = record.get('female_population', 0) or 0
                
                total_male += male_pop
                total_female += female_pop
                
                # For Mexico, calculate age groups from specific columns
                p_0a2 = record.get('p_0a2', 0) or 0
                p_3a5 = record.get('p_3a5', 0) or 0
                p_6a11 = record.get('p_6a11', 0) or 0
                p_12a14 = record.get('p_12a14', 0) or 0
                p_15a17 = record.get('p_15a17', 0) or 0
                p_18a24 = record.get('p_18a24', 0) or 0
                p_60ymas = record.get('p_60ymas', 0) or 0
                
                # 0-14 age group
                age_0_14 = p_0a2 + p_3a5 + p_6a11 + p_12a14
                age_totals['0-14']['male'] += age_0_14 // 2
                age_totals['0-14']['female'] += age_0_14 - (age_0_14 // 2)
                
                # 15-24 age group
                age_15_24 = p_15a17 + p_18a24
                age_totals['15-24']['male'] += age_15_24 // 2
                age_totals['15-24']['female'] += age_15_24 - (age_15_24 // 2)
                
                # 60+ age group
                age_totals['60+']['male'] += p_60ymas // 2
                age_totals['60+']['female'] += p_60ymas - (p_60ymas // 2)
                
                # 25-59 age group (derived)
                age_25_59 = pop - age_0_14 - age_15_24 - p_60ymas
                age_totals['25-59']['male'] += max(0, age_25_59 // 2)
                age_totals['25-59']['female'] += max(0, age_25_59 - (age_25_59 // 2))
                
                # Household data
                if record.get('vivtot'):
                    total_households += record.get('vivtot', 0)
            
            # Education data (common for both)
            if record.get('graproes'):
                total_education_level += record.get('graproes', 0) * pop
        
        # Calculate percentages and averages
        male_percentage = (total_male / total_population * 100) if total_population > 0 else 0
        female_percentage = (total_female / total_population * 100) if total_population > 0 else 0
        
        # Average income per person
        avg_income = (total_income / total_population) if total_population > 0 else 0
        
        # Calculate area
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        
        # Create age distribution for population pyramid (consistent with PropertyDemographic)
        age_distribution = []
        for age_group in ['0-14', '15-24', '25-59', '60+']:
            male_count = age_totals[age_group]['male']
            female_count = age_totals[age_group]['female']
            total_age_group = male_count + female_count
            
            age_distribution.append({
                "age_group": age_group,
                "male": male_count,
                "female": female_count,
                "total": total_age_group,
                "percentage": round((total_age_group / total_population * 100), 1) if total_population > 0 else 0
            })
        
        # Process socioeconomic levels from actual housing data (consistent with PropertyDemographic)
        # Collect housing percentages from all records using same column names
        housing_totals = {
            "ses_ab": 0, "ses_c_plus": 0, "ses_c": 0, "ses_c_minus": 0, 
            "ses_d_plus": 0, "ses_d": 0, "ses_e": 0
        }
        
        total_households_processed = 0
        
        for record in demographic_data:
            if config_city == 'queretaro':
                # QRO uses tot_vivien and pct_viv_* columns
                households = record.get('tot_vivien', 0) or 0
                total_households_processed += households
                if households > 0:
                    housing_totals["ses_ab"] += (record.get('pct_viv_ab', 0) or 0) * households / 100
                    housing_totals["ses_c_plus"] += (record.get('pct_viv_cp', 0) or 0) * households / 100
                    housing_totals["ses_c"] += (record.get('pct_viv_c', 0) or 0) * households / 100
                    housing_totals["ses_c_minus"] += (record.get('pct_viv_cm', 0) or 0) * households / 100
                    housing_totals["ses_d_plus"] += (record.get('pct_viv_dp', 0) or 0) * households / 100
                    housing_totals["ses_d"] += (record.get('pct_viv_d', 0) or 0) * households / 100
                    housing_totals["ses_e"] += (record.get('pct_viv_e', 0) or 0) * households / 100
            else:
                # Mexico uses vivtot and direct ses_* percentages
                households = record.get('vivtot', 0) or 0
                total_households_processed += households
                if households > 0:
                    # For Mexico, the columns already contain percentages, so convert them
                    housing_totals["ses_ab"] += (record.get('pct_viv_ab', 0) or 0) * households / 100
                    housing_totals["ses_c_plus"] += (record.get('pct_viv_cp', 0) or 0) * households / 100
                    housing_totals["ses_c"] += (record.get('pct_viv_c', 0) or 0) * households / 100
                    housing_totals["ses_c_minus"] += (record.get('pct_viv_cm', 0) or 0) * households / 100
                    housing_totals["ses_d_plus"] += (record.get('pct_viv_dp', 0) or 0) * households / 100
                    housing_totals["ses_d"] += (record.get('pct_viv_d', 0) or 0) * households / 100
                    housing_totals["ses_e"] += (record.get('pct_viv_e', 0) or 0) * households / 100
        
        # Calculate total households and percentages
        total_housing = sum(housing_totals.values())
        socioeconomic_levels = []
        
        # Structure consistent with PropertyDemographic naming
        level_mapping = {
            "ses_ab": "AB", "ses_c_plus": "C+", "ses_c": "C", "ses_c_minus": "C-",
            "ses_d_plus": "D+", "ses_d": "D", "ses_e": "E"
        }
        
        for ses_key, level_name in level_mapping.items():
            count = housing_totals[ses_key]
            percentage = (count / total_housing * 100) if total_housing > 0 else 0
            socioeconomic_levels.append({
                "level": level_name,
                "households": int(count),
                "percentage": round(percentage, 1),
                "ses_key": ses_key  # For consistency with PropertyDemographic
            })
        
        # Find predominant level (highest percentage)
        predominant_level = max(socioeconomic_levels, key=lambda x: x['percentage']) if socioeconomic_levels else None
        
        # Calculate population density (persons per km²)
        population_density = round(total_population / area_km2, 2) if area_km2 > 0 else 0
        
        # Structure response to match your UI exactly
        demographics_data = {
            "summary": {
                "area_km2": area_km2,
                "population": total_population,
                "population_density": population_density,
                "population_density_formatted": f"{population_density} persons / km²",
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius
            },
            "demographics": {
                "total_population": total_population,
                "male_population": total_male,
                "female_population": total_female,
                "male_percentage": round(male_percentage, 1),
                "female_percentage": round(female_percentage, 1),
                "age_distribution": age_distribution,
                "age_pyramid_data": {
                    "age_groups": [group["age_group"] for group in age_distribution],
                    "male_values": [group["male"] for group in age_distribution],
                    "female_values": [group["female"] for group in age_distribution],
                    "male_negative": [-group["male"] for group in age_distribution],  # For left side of pyramid
                    "labels": [group["age_group"] for group in age_distribution]  # Dynamic labels from actual data
                }
            },
            "socioeconomic": {
                "predominant_level": {
                    "level": predominant_level["level"] if socioeconomic_levels else "N/A",
                    "percentage": predominant_level["percentage"] if socioeconomic_levels else 0,
                    "households": predominant_level["households"] if socioeconomic_levels else 0
                },
                "municipality_average": None,  # Not available
                "levels_distribution": socioeconomic_levels,
                "average_income": {
                    "amount": round(avg_income, 2),
                    "currency": "MXN",
                    "formatted": f"${avg_income:,.0f} MXN" if avg_income > 0 else "$0 MXN",
                    "trend": None  # Not available
                },
                "total_income": {
                    "amount": round(total_income, 2),
                    "currency": "MXN", 
                    "formatted": f"${total_income/1000000:,.0f} million MXN" if total_income > 0 else "$0 MXN",
                    "trend": None  # Not available
                }
            }
        }
        
        return demographics_data

    def _get_empty_demographics_structure(self, lat, lng, radius):
        """Return empty demographics structure when no data is found."""
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        
        return {
            "summary": {
                "area_km2": area_km2,
                "population": 0,
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius
            },
            "demographics": {
                "total_population": 0,
                "male_population": 0,
                "female_population": 0,
                "male_percentage": 0,
                "female_percentage": 0,
                "age_distribution": []
            },
            "socioeconomic": {
                "predominant_level": {"level": "N/A", "percentage": 0, "households": 0},
                "municipality_average": "N/A",
                "levels_distribution": [],
                "average_income": {"amount": 0, "currency": "MXN", "formatted": "$0 MXN", "trend": "stable"},
                "total_income": {"amount": 0, "currency": "MXN", "formatted": "$0 MXN", "trend": "stable"}
            }
        }

    def get_weekly_traffic_by_municipality(self, lat, lng, radius=2000):
        """
        Get weekly traffic distribution by municipality for a given location.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            
        Returns:
            dict: Response with weekly traffic data by municipality
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_weekly_traffic_by_municipality_query(lat, lng, radius)
            logger.info(f"Weekly traffic by municipality query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Process the results
            if res and len(res) > 0:
                # Group data by municipality
                municipalities = {}
                for row in res:
                    cve_mun = row.get('cve_mun')
                    nom_mun = row.get('nom_mun')
                    dia_de_la_semana = row.get('dia_de_la_semana')
                    tipo_usuario = row.get('tipo_usuario')
                    total_users = row.get('total_users', 0)
                    
                    if cve_mun not in municipalities:
                        municipalities[cve_mun] = {
                            "municipality_code": cve_mun,
                            "municipality_name": nom_mun,
                            "weekly_traffic": {}
                        }
                    
                    if dia_de_la_semana not in municipalities[cve_mun]["weekly_traffic"]:
                        municipalities[cve_mun]["weekly_traffic"][dia_de_la_semana] = {}
                    
                    municipalities[cve_mun]["weekly_traffic"][dia_de_la_semana][tipo_usuario] = total_users
                
                # Convert to list format
                municipalities_list = list(municipalities.values())
                
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "total_municipalities": len(municipalities_list)
                    },
                    "municipalities": municipalities_list
                }
            else:
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "total_municipalities": 0
                    },
                    "municipalities": []
                }
            
            logger.info(f"Weekly traffic by municipality: {len(municipalities_list) if res else 0} municipalities found")
            resp = Response.success(data=traffic_data)
            
        except Exception as e:
            logger.error(f"Error in get_weekly_traffic_by_municipality: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def get_h3_distribution(self, lat, lng, radius=2000):
        """
        Get H3 distribution based on pedestrian traffic data with frequency percentages.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            
        Returns:
            dict: Response with H3 distribution data including frequency percentages
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_h3_distribution_query(lat, lng, radius)
            logger.info(f"H3 distribution query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Process the results
            h3_distribution = []
            if res and len(res) > 0:
                for row in res:
                    h3_distribution.append({
                        "h3_index": str(row.get('h3_index', '')),
                        "total_users": float(row.get('total_users', 0))
                    })
                
                logger.info(f"H3 distribution: {len(h3_distribution)} H3 indexes found")
            else:
                logger.info("No H3 distribution data found")
            
            # Structure response
            distribution_data = {
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius,
                "total_h3_indexes": len(h3_distribution),
                "h3_distribution": h3_distribution
            }
            
            resp = Response.success(data=distribution_data)
            
        except Exception as e:
            logger.error(f"Error in get_h3_distribution: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.redshift_db.disconnect(connection)
            return resp

    def create_h3_buckets(self, h3_distribution_data, min_value=None, max_value=None, bucket_size=10):
        """
        Create buckets from H3 distribution data based on min/max values and bucket size.
        
        Args:
            h3_distribution_data (list): List of H3 distribution data with avg_pedestrian values
            min_value (float): Minimum value for bucket range (if None, uses min from data)
            max_value (float): Maximum value for bucket range (if None, uses max from data)
            bucket_size (int): Number of buckets to create (default: 10)
            
        Returns:
            dict: Bucket distribution with counts and ranges
        """
        if not h3_distribution_data or len(h3_distribution_data) == 0:
            return {
                "buckets": []
            }
        print("bucket_size=====>", bucket_size)
        print("h3_distribution_data=====>", h3_distribution_data[:20])
        # Extract total_users values
        total_values = [item.get('total_users', 0) for item in h3_distribution_data if 'total_users' in item]
        
        if not total_values:
            return {    
                "buckets": []
            }
        
        # Determine min and max values
        data_min = min(total_values)
        data_max = max(total_values)
        
        # Always start from 0 unless explicitly overridden
        actual_min = min_value if min_value is not None else 0
        actual_max = max_value if max_value is not None else data_max
        
        # Calculate bucket range
        bucket_range = (actual_max - actual_min) / bucket_size
        
        # Create buckets
        buckets = []
        for i in range(bucket_size):
            bucket_min = actual_min + (i * bucket_range)
            bucket_max = actual_min + ((i + 1) * bucket_range)
            
            # Count H3 indexes in this bucket
            h3_count = 0
            
            for item in h3_distribution_data:
                total_users = item.get('total_users', 0)
                # Check if value falls in this bucket (inclusive of min, exclusive of max for all but last bucket)
                if i == bucket_size - 1:  # Last bucket includes max value
                    if bucket_min <= total_users <= bucket_max:
                        h3_count += 1
                else:
                    if bucket_min <= total_users < bucket_max:
                        h3_count += 1
            
            buckets.append({
                "bucket_number": i + 1,
                "bucket_range": {
                    "min_value": round(bucket_min, 2),
                    "max_value": round(bucket_max, 2)
                },
                "h3_count": h3_count
            })
        
        # Reorder buckets to create a proper curve with highest values in center
        # Sort buckets by h3_count in descending order
        sorted_buckets = sorted(buckets, key=lambda x: x['h3_count'], reverse=True)
        
        # Create a curve pattern: center has highest values, edges have lowest
        reordered_buckets = []
        center_index = bucket_size // 2
        
        # Place buckets in curve pattern
        for i in range(bucket_size):
            if i == 0 or i == bucket_size - 1:
                # First and last positions get the lowest h3_count
                reordered_buckets.append(sorted_buckets[-1])
            elif i == 1 or i == bucket_size - 2:
                # Second and second-to-last get second lowest
                reordered_buckets.append(sorted_buckets[-2])
            elif i == center_index:
                # Center gets the highest h3_count
                reordered_buckets.append(sorted_buckets[0])
            elif i == center_index - 1 or i == center_index + 1:
                # Adjacent to center get second highest
                reordered_buckets.append(sorted_buckets[1])
            else:
                # Fill remaining positions with middle values
                remaining_buckets = sorted_buckets[2:-2]  # Exclude highest, second highest, and two lowest
                if remaining_buckets:
                    # Distribute remaining buckets based on distance from center
                    distance_from_center = abs(i - center_index)
                    if distance_from_center < len(remaining_buckets):
                        reordered_buckets.append(remaining_buckets[distance_from_center])
                    else:
                        reordered_buckets.append(remaining_buckets[-1])
                else:
                    # Fallback to second lowest if no remaining buckets
                    reordered_buckets.append(sorted_buckets[-2])
        
        # Update bucket numbers to maintain sequential numbering
        for i, bucket in enumerate(reordered_buckets):
            bucket["bucket_number"] = i + 1
        
        return {
            "data_range": {
                "min_value": round(actual_min, 2),
                "max_value": round(actual_max, 2)
            },
            "buckets": reordered_buckets
        }


