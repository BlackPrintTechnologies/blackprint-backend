import json
import logging
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase
from utils.app_cache import set_in_cache, get_from_cache
from module.area_analysis.query import AreaAnalysisQuery

logger = logging.getLogger(__name__)

class AreaAnalysisController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.query_builder = AreaAnalysisQuery()

    def get_traffic_by_day(self, lat, lng, radius=2000, user_type=None, config_city='queretaro'):
        """Get traffic data aggregated by day of the week."""
        # Create cache key based on parameters
        cache_key = f"traffic_by_day_{config_city}_{lat}_{lng}_{radius}_{user_type or 'all'}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached traffic by day data for {config_city} at ({lat}, {lng}) with radius {radius}, user_type {user_type}")
            return cached_response
        
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_traffic_by_day_query(lat, lng, radius, user_type, config_city)
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached traffic by day data for {config_city} at ({lat}, {lng}) with radius {radius}, user_type {user_type}")
            
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

    def get_traffic_by_hour(self, lat, lng, radius=2000, user_type=None, config_city='queretaro'):
        """Get traffic data aggregated by hour of the day."""
        # Create cache key based on parameters
        cache_key = f"traffic_by_hour_{config_city}_{lat}_{lng}_{radius}_{user_type or 'all'}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached traffic by hour data for {config_city} at ({lat}, {lng}) with radius {radius}, user_type {user_type}")
            return cached_response
        
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_traffic_by_hour_query(lat, lng, radius, user_type, config_city)
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached traffic by hour data for {config_city} at ({lat}, {lng}) with radius {radius}, user_type {user_type}")
            
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

    def get_traffic_summary(self, lat, lng, radius=2000, user_type=None, config_city='queretaro'):
        """Get total traffic summary within a specified radius."""
        # Create cache key based on parameters
        cache_key = f"traffic_summary_{config_city}_{lat}_{lng}_{radius}_{user_type or 'all'}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached traffic summary data for {config_city} at ({lat}, {lng}) with radius {radius}, user_type {user_type}")
            return cached_response
        
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = self.query_builder.build_traffic_summary_query(lat, lng, radius, user_type, config_city)
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached traffic summary data for {config_city} at ({lat}, {lng}) with radius {radius}, user_type {user_type}")
            
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
        """Get area analysis summary matching the UI mockup."""
        # Create cache key based on parameters
        cache_key = f"area_summary_{config_city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached area summary data for {config_city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
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
                query = self.query_builder.build_traffic_summary_query(lat, lng, radius, user_type, config_city)
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
            

            # Get H3 traffic summary data with aggregated data
            h3_traffic_summary = {
                "unique_h3_count": 0,
                "total_unique_users": 0,
                "avg_users_per_h3": 0,
                "percentile_rank": 0
            }
            
            try:
                h3_traffic_query = self.query_builder.build_h3_traffic_summary_query(lat, lng, radius, config_city)
                logger.info(f"Executing H3 traffic summary query: {h3_traffic_query}")
                
                cursor.execute(h3_traffic_query)
                connection.commit()
                h3_traffic_res = cursor.fetchall()
                
                if h3_traffic_res and len(h3_traffic_res) > 0:
                    row = h3_traffic_res[0]
                    unique_h3_count = row.get('unique_h3_count', 0) or 0
                    total_unique_users = row.get('total_unique_users', 0) or 0
                    avg_users_per_h3 = row.get('avg_users_per_h3', 0) or 0
                    
                    h3_traffic_summary = {
                        "unique_h3_count": unique_h3_count,
                        "total_unique_users": total_unique_users,
                        "avg_users_per_h3": float(avg_users_per_h3),
                        "percentile_rank": 0
                    }
                    
                    logger.info(f"H3 traffic summary data collected: {unique_h3_count} unique H3, {total_unique_users} total users, {avg_users_per_h3} avg per H3")
                else:
                    logger.info("No H3 traffic summary data found")
                
            except Exception as e:
                logger.error(f"Error getting H3 traffic summary data: {str(e)}")

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
                    bucket_data = self.create_h3_buckets(h3_distribution, bucket_size=1000)
                    
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

            # Calculate percentile rank for avg_users_per_h3 against bucket distribution
            if h3_traffic_summary.get('avg_users_per_h3', 0) > 0 and h3_distribution_data.get('bucket_distribution', {}).get('buckets'):
                try:
                    avg_value = h3_traffic_summary['avg_users_per_h3']
                    buckets = h3_distribution_data['bucket_distribution']['buckets']
                    
                    # Calculate percentile rank for the avg_users_per_h3 value
                    percentile_rank = self.calculate_percentile_rank_for_value(avg_value, buckets)
                    h3_traffic_summary['percentile_rank'] = percentile_rank
                    
                    logger.info(f"Calculated percentile rank for avg_users_per_h3 {avg_value}: {percentile_rank}")
                except Exception as e:
                    logger.error(f"Error calculating percentile rank: {str(e)}")
                    h3_traffic_summary['percentile_rank'] = 0


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
                "h3_traffic_summary": h3_traffic_summary,
                "h3_distribution": h3_distribution_data
            }
            
            logger.info(f"Area summary completed for {total_all_users} total users")
            resp = Response.success(data=summary_data)
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached area summary data for {config_city} at ({lat}, {lng}) with radius {radius}")
            
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
        """Get detailed traffic patterns for charts (both hourly and daily)."""
        # Create cache key based on parameters
        cache_key = f"traffic_patterns_{config_city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached traffic patterns data for {config_city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
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
                query = self.query_builder.build_traffic_by_hour_query(lat, lng, radius, user_type, config_city)
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
                        # Handle None values from database
                        if value is None:
                            value = 0
                        hourly_array.append(value)
                        # Additional safety check for max_value
                        if max_value is None:
                            max_value = 0
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
                query = self.query_builder.build_traffic_by_day_query(lat, lng, radius, user_type, config_city)
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
                        # Handle None values from database
                        if value is None:
                            value = 0
                        daily_array.append(value)
                        # Additional safety check for max_value
                        if max_value is None:
                            max_value = 0
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached traffic patterns data for {config_city} at ({lat}, {lng}) with radius {radius}")
            
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
        """Get demographic and socioeconomic analysis for a specific area."""
        # Create cache key based on parameters
        cache_key = f"demographics_{config_city}_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached demographics data for {config_city} at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
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
                # Process demographic data - both Mexico and Queretaro now use pre-aggregated queries
                if config_city == 'mexico':
                    demographics_data = self._process_queretaro_demographics_data(res, lat, lng, radius, connection)
                elif config_city == 'queretaro':
                    demographics_data = self._process_queretaro_demographics_data(res, lat, lng, radius, connection)
                else:
                    logger.error(f"Unsupported city configuration for processing: {config_city}")
                    return Response.error("Unsupported city configuration. Supported cities: mexico, queretaro")
                
                # ADD SOCIOECONOMIC INCOME ANALYSIS
                if config_city == 'queretaro':
                    # Get entity code for Queretaro
                    entity_code = 22  # Queretaro entity code
                    
                    # Get income analysis data
                    income_analysis = self._get_socioeconomic_income_analysis(connection, lat, lng, radius, entity_code)
                    demographics_data['socioeconomic_income_analysis'] = income_analysis
                
                logger.info(f"Demographics analysis completed for pre-aggregated query result in {config_city}")
                resp = Response.success(data=demographics_data)
                
                # Cache the successful response
                set_in_cache('demographic', cache_key, resp)
                logger.info(f"Cached demographics data for {config_city} at ({lat}, {lng}) with radius {radius}")
            else:
                # No data found - return error instead of empty structure
                logger.warning(f"No demographic data found for {config_city} at ({lat}, {lng}) with radius {radius}")
                resp = Response.error(f"No demographic data found for the specified location")
            
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


    def _create_age_pyramid_data(self, aggregated_result):
        """Create age pyramid data structure for visualization."""
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
        
        # Calculate missing 25-64 age range population
        total_population = aggregated_result.get("pobtot", 0) or 0
        existing_population = sum(block_age_totals.values())
        missing_population = max(0, total_population - existing_population)
        
        # Get municipality-level gender ratio for 25-64 age range (use 18-24 as proxy)
        muni_18_24_data = municipality_gender_data["18a24"]
        if muni_18_24_data["total"] > 0:
            male_ratio_25_64 = muni_18_24_data["male"] / muni_18_24_data["total"]
            female_ratio_25_64 = muni_18_24_data["female"] / muni_18_24_data["total"]
        else:
            male_ratio_25_64 = 0.5  # Default 50/50 split if no data
            female_ratio_25_64 = 0.5
        
        # Calculate male and female counts for 25-64 age range
        male_25_64 = round(missing_population * male_ratio_25_64)
        female_25_64 = round(missing_population * female_ratio_25_64)
        
        # Add single 25-64 age group with calculated population
        age_groups.append({
            "range": "25-65",
            "male": male_25_64,
            "female": female_25_64
        })
        
        # Debug: Log the calculated values
        logger.info(f"Age pyramid - Total population: {total_population}")
        logger.info(f"Age pyramid - Existing population: {existing_population}")
        logger.info(f"Age pyramid - Missing population (25-64): {missing_population}")
        logger.info(f"Age pyramid - Block totals: {block_age_totals}")
        logger.info(f"Age pyramid - Calculated groups: {[(g['range'], g['male'], g['female']) for g in age_groups[:7]]}")
        
        # Calculate percentages for each age group
        for group in age_groups:
            group["male_percentage"] = round((group["male"] / total_population * 100), 2) if total_population > 0 else 0
            group["female_percentage"] = round((group["female"] / total_population * 100), 2) if total_population > 0 else 0
        
        return {
            "age_groups": age_groups,
            "total_population": total_population
        }

    def _create_population_growth_data(self, aggregated_result):
        """Create population growth data structure."""
        # Only use years that actually exist in the database
        # Based on the query: 2000, 2005, 2010, 2020 (no 2015, no interpolated 2007)
        
        # Get population values with None handling
        pop_2000 = aggregated_result.get('pob_2000_ageb', 0) or 0
        pop_2005 = aggregated_result.get('pob_2005_ageb', 0) or 0
        pop_2010 = aggregated_result.get('pob_2010_ageb', 0) or 0
        pop_2020 = aggregated_result.get('pob_2020_ageb', 0) or 0
        
        # Get pre-calculated growth rates from database (preferred method)
        growth_2005 = aggregated_result.get('cambio_porcentual_2005_ageb', 0) or 0
        growth_2010 = aggregated_result.get('cambio_porcentual_2010_ageb', 0) or 0
        growth_2020 = aggregated_result.get('cambio_porcentual_2020_ageb', 0) or 0
        
        # Calculate period-to-period growth rates using pre-calculated values
        block_growth = {
            "2000": [pop_2000, 0],  # Base year, 0% growth
            "2005": [
                pop_2005,
                round(growth_2005, 4)  # Use pre-calculated growth rate from database
            ],
            "2010": [
                pop_2010,
                round(growth_2010, 4)  # Use pre-calculated growth rate from database
            ],
            "2015": [None, None],  # No data for 2015
            "2020": [
                pop_2020,
                round(growth_2020, 4)  # Use pre-calculated growth rate from database
            ]
        }
        
        # Get municipal population values with None handling
        mun_2000 = aggregated_result.get('pob_2000_municipal', 0) or 0
        mun_2005 = aggregated_result.get('pob_2005_municipal', 0) or 0
        mun_2010 = aggregated_result.get('pob_2010_municipal', 0) or 0
        mun_2020 = aggregated_result.get('pob_2020_municipal', 0) or 0
        
        # Get pre-calculated municipal growth rates from database
        mun_growth_2005 = aggregated_result.get('cambio_porcentual_2005_municipal', 0) or 0
        mun_growth_2010 = aggregated_result.get('cambio_porcentual_2010_municipal', 0) or 0
        mun_growth_2020 = aggregated_result.get('cambio_porcentual_2020_municipal', 0) or 0
        
        # Calculate period-to-period growth rates for municipality using pre-calculated values
        alcaldia_growth = {
            "2000": [mun_2000, 0],  # Base year, 0% growth
            "2005": [
                mun_2005,
                round(mun_growth_2005, 4)  # Use pre-calculated growth rate from database
            ],
            "2010": [
                mun_2010,
                round(mun_growth_2010, 4)  # Use pre-calculated growth rate from database
            ],
            "2015": [None, None],  # No data for 2015
            "2020": [
                mun_2020,
                round(mun_growth_2020, 4)  # Use pre-calculated growth rate from database
            ]
        }
        
        # OLD CALCULATION METHOD (COMMENTED OUT - KEPT FOR REFERENCE)
        # # Calculate period-to-period growth rates manually (fallback method)
        # block_growth_old = {
        #     "2000": [pop_2000, 0],  # Base year, 0% growth
        #     "2005": [
        #         pop_2005,
        #         round(((pop_2005 - pop_2000) / pop_2000 * 100), 4) if pop_2000 > 0 else 0  # 2005 vs 2000
        #     ],
        #     "2010": [
        #         pop_2010,
        #         round(((pop_2010 - pop_2005) / pop_2005 * 100), 4) if pop_2005 > 0 else 0  # 2010 vs 2005
        #     ],
        #     "2015": [None, None],  # No data for 2015
        #     "2020": [
        #         pop_2020,
        #         round(((pop_2020 - pop_2010) / pop_2010 * 100), 4) if pop_2010 > 0 else 0  # 2020 vs 2010
        #     ]
        # }
        # 
        # # Calculate period-to-period growth rates for municipality manually (fallback method)
        # alcaldia_growth_old = {
        #     "2000": [mun_2000, 0],  # Base year, 0% growth
        #     "2005": [
        #         mun_2005,
        #         round(((mun_2005 - mun_2000) / mun_2000 * 100), 4) if mun_2000 > 0 else 0  # 2005 vs 2000
        #     ],
        #     "2010": [
        #         mun_2010,
        #         round(((mun_2010 - mun_2005) / mun_2005 * 100), 4) if mun_2005 > 0 else 0  # 2010 vs 2005
        #     ],
        #     "2015": [None, None],  # No data for 2015
        #     "2020": [
        #         mun_2020,
        #         round(((mun_2020 - mun_2010) / mun_2010 * 100), 4) if mun_2010 > 0 else 0  # 2020 vs 2010
        #     ]
        # }
        
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

    def _interpolate_growth_for_year(self, growth_data, target_year):
        """Interpolate growth percentage for a specific year."""
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

    def _interpolate_growth_for_year(self, growth_data, target_year):
        """Interpolate growth percentage for a specific year."""
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

    def _get_municipality_area_km2(self, municipality_code, connection):
        """Calculate municipality area in km²."""
        cursor = None
        try:
            cursor = connection.cursor()
            
            # Query to get total area using ST_Area calculation from v_qro table
            query = """
            SELECT SUM(ST_Area(ST_GeomFromGeoJSON(bbox))) AS total_area_deg2
            FROM data_product.v_qro
            WHERE cve_mun = %s
            """
            
            logger.info(f"Calculating municipality area for municipality_code: {municipality_code}")
            cursor.execute(query, (municipality_code,))
            aggregated_result = cursor.fetchone()
            
            if aggregated_result and aggregated_result[0]:
                # Convert from square degrees to square kilometers
                # Approximate conversion: 1 degree ≈ 111 km at equator
                # For more accurate conversion, we'd need to consider latitude
                total_area_deg2 = float(aggregated_result[0])
                # Rough conversion: 1 deg² ≈ 12,364 km² at the equator
                # For Queretaro (around 20°N), multiply by cos(20°) ≈ 0.94
                total_area_km2 = total_area_deg2 * 12364 * 0.94
                
                logger.info(f"Municipality area calculation - municipality_code: {municipality_code}, "
                           f"total_area_deg2: {total_area_deg2}, "
                           f"total_area_km2: {round(total_area_km2, 2)}")
                
                return round(total_area_km2, 2)
            else:
                logger.error(f"No area data found for municipality_code: {municipality_code}")
                raise ValueError(f"No area data found for municipality_code: {municipality_code}")
                
        except ValueError:
            # Re-raise ValueError as-is
            raise
        except Exception as e:
            logger.error(f"Error calculating municipality area for municipality_code {municipality_code}: {str(e)}")
            raise ValueError(f"Failed to calculate municipality area: {str(e)}")
        finally:
            if cursor:
                cursor.close()

    def _process_queretaro_demographics_data(self, demographic_data, lat, lng, radius, connection=None):
        """Process Queretaro demographic data from aggregated query."""
        # Spatial filtering and aggregation are now handled in the SQL query
        if not demographic_data:
            logger.error("No demographic data found")
            raise ValueError("No demographic data found for the specified location")
        
        # Get the single aggregated result (no need for aggregation since query does it)
        aggregated_result = demographic_data[0]  # Query returns single pre-aggregated record
        
        # All calculations are now done in the SQL query, just extract the values
        area_km2 = aggregated_result.get("area_km2", 0)
        area_population_density = aggregated_result.get("population_density", 0)
        
        logger.info(f"Area analysis - lat: {lat}, lng: {lng}, radius: {radius}, "
                   f"area_km2: {area_km2}, area_population_density: {area_population_density}")
        male_percentage = aggregated_result.get("male_percentage")
        female_percentage = aggregated_result.get("female_percentage")
        municipality_male_percentage = aggregated_result.get("municipality_male_percentage")
        municipality_female_percentage = aggregated_result.get("municipality_female_percentage")
        avg_household_size = aggregated_result.get("average_household_size", 0)
        
        # Calculate municipality population density
        municipality_code = aggregated_result.get("municipality_code")
        if not municipality_code or not connection:
            raise ValueError("Municipality code or database connection not available for municipality population density calculation")
        
        municipality_area_km2 = self._get_municipality_area_km2(municipality_code, connection)
        pobtot_alcaldia = aggregated_result.get("pobtot_alcaldia", 0)
        
        if municipality_area_km2 <= 0:
            raise ValueError(f"Invalid municipality area calculated: {municipality_area_km2} km²")
        
        municipality_population_density = round(pobtot_alcaldia / municipality_area_km2, 2) if pobtot_alcaldia else 0
        
        logger.info(f"Municipality population density calculation - "
                   f"municipality_code: {municipality_code}, "
                   f"pobtot_alcaldia: {pobtot_alcaldia}, "
                   f"municipality_area_km2: {municipality_area_km2}, "
                   f"municipality_population_density: {municipality_population_density}")
        
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
                        "ageb_code": aggregated_result["ageb_code"],
                        "total_household": aggregated_result["vivtot"],
                        "average_household_size": avg_household_size,
                        "average_number_of_rooms": aggregated_result.get("pro_ocup_c")  # Use actual data if available
                    },
                    "colonia": {
                        "ageb_code": aggregated_result["ageb_code"],
                        "total_household": aggregated_result["vivtot"],
                        "average_household_size": avg_household_size,
                        "average_number_of_rooms": aggregated_result.get("pro_ocup_c")  # Use actual data if available
                    },
                    "alcaldia": {
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
                    "population_density_trend": "down" if float(area_population_density) < municipality_population_density else "up",
                    "male_population": aggregated_result["pobmas"],
                    "male_percentage": f"{male_percentage}%",
                    "male_trend": "down" if (male_percentage or 0) < (municipality_male_percentage or 0) else "up",
                    "female_population": aggregated_result["pobfem"],
                    "female_percentage": f"{female_percentage}%",
                    "female_trend": "up" if (female_percentage or 0) > (municipality_female_percentage or 0) else "down",
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

    def _create_queretaro_socioeconomic_analysis(self, aggregated_result):
        """Create socioeconomic analysis for Queretaro demographics."""
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
        
        def validate_percentage(value, name):
            """Validate and cap percentage values between 0-100."""
            if value is None:
                return 0.0
            if value > 100:
                logger.warning(f"{name} percentage {value} is > 100%, capping at 100%")
                return 100.0
            if value < 0:
                logger.warning(f"{name} percentage {value} is < 0%, setting to 0%")
                return 0.0
            return float(value)
        
        # Calculate household distribution for selected area
        household_distribution = []
        predominant_level = None
        max_percentage = 0
        
        for ses in ses_levels:
            # Get percentage from corrected query (already calculated as weighted average)
            percentage = validate_percentage(aggregated_result.get(ses['key'], 0), f"Selected area {ses['level']}")
            
            # Calculate actual household count from percentage
            households = round((percentage * total_households) / 100) if total_households > 0 else 0
            
            household_distribution.append({
                'level': ses['level'],
                'households': households,
                'percentage': round(percentage, 1)
            })
            
            if percentage > max_percentage:
                max_percentage = percentage
                predominant_level = ses['level']
        
        # Municipality level data (CORRECTED: Now using AVG instead of MAX)
        municipality_total_households = aggregated_result.get('vivtot_alcaldia', 0) or 0
        municipality_predominant_level = None
        municipality_max_percentage = 0
        
        # Find municipality predominant level with validation
        for ses in ses_levels:
            municipality_percentage = validate_percentage(
                aggregated_result.get(f"{ses['key']}_alcaldia", 0), 
                f"Municipality {ses['level']}"
            )
            
            if municipality_percentage > municipality_max_percentage:
                municipality_max_percentage = municipality_percentage
                municipality_predominant_level = ses['level']
        
        # Log corrected values for debugging
        logger.info(f"Socioeconomic analysis - Selected area predominant: {predominant_level} ({max_percentage}%)")
        logger.info(f"Socioeconomic analysis - Municipality predominant: {municipality_predominant_level} ({municipality_max_percentage}%)")
        
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

    def get_weekly_traffic_by_municipality(self, lat, lng, radius=2000):
        """Get weekly traffic distribution by municipality."""
        # Create cache key based on parameters
        cache_key = f"weekly_traffic_municipality_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached weekly traffic by municipality data at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached weekly traffic by municipality data at ({lat}, {lng}) with radius {radius}")
            
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
        """Get H3 distribution based on pedestrian traffic data."""
        # Create cache key based on parameters
        cache_key = f"h3_distribution_{lat}_{lng}_{radius}"
        
        # Check cache first
        cached_response = get_from_cache('demographic', cache_key)
        if cached_response:
            logger.info(f"Returning cached H3 distribution data at ({lat}, {lng}) with radius {radius}")
            return cached_response
        
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
            
            # Cache the successful response
            set_in_cache('demographic', cache_key, resp)
            logger.info(f"Cached H3 distribution data at ({lat}, {lng}) with radius {radius}")
            
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
        """Create buckets from H3 distribution data."""
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
        # Restrict max to 6000
        actual_max = min(max_value if max_value is not None else data_max, 6000)
        
        # Calculate bucket range - each bucket is 500
        bucket_range = 500
        # Calculate number of buckets needed
        bucket_size = int((actual_max - actual_min) / bucket_range) + 1
        
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
        
        # Filter out consecutive low h3_count points
        # Remove all consecutive buckets with h3_count < 10 from the beginning
        filtered_buckets = []
        consecutive_low_counts = 0
        max_consecutive_low_counts = 200
        min_h3_count_threshold = 10
        
        for bucket in buckets:
            if bucket["h3_count"] < min_h3_count_threshold:
                consecutive_low_counts += 1
                if consecutive_low_counts >= max_consecutive_low_counts:
                    # Stop adding buckets once we hit 200 consecutive low counts
                    break
                # Don't add low count buckets to filtered results
                continue
            else:
                # Reset consecutive low counts counter when we find a count >= 10
                consecutive_low_counts = 0
                filtered_buckets.append(bucket)
        
        # Calculate distribution statistics
        total_points = sum(bucket["h3_count"] for bucket in filtered_buckets)
        
        return {
            "data_range": {
                "min_value": round(actual_min, 2),
                "max_value": round(actual_max, 2)
            },
            "buckets": filtered_buckets,
            "distribution_stats": {
                "total_points": total_points,
                "bucket_count": len(filtered_buckets),
                "mean_count": round(total_points / len(filtered_buckets), 2) if filtered_buckets else 0
            }
        }

    def calculate_percentile_rank_for_value(self, target_value, buckets):
        """Calculate percentile rank for a specific value against bucket distribution."""
        if not buckets or len(buckets) == 0:
            return 0
        
        # Calculate total points across all buckets
        total_points = sum(bucket.get('h3_count', 0) for bucket in buckets)
        if total_points == 0:
            return 0
        
        # Calculate cumulative count up to the target value
        cumulative_count = 0
        for bucket in buckets:
            bucket_range = bucket.get('bucket_range', {})
            max_val = bucket_range.get('max_value', 0)
            
            if target_value <= max_val:
                break
            cumulative_count += bucket.get('h3_count', 0)
        
        # Calculate percentile for the target value
        percentile = (cumulative_count / total_points * 100) if total_points > 0 else 0
        
        return round(percentile, 1)

    def _get_socioeconomic_income_analysis(self, connection, lat, lng, radius, entity_code):
        """Get optimized socioeconomic income analysis data."""
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Execute queries sequentially to avoid connection pool exhaustion
            # Radius analysis
            radius_query = self.query_builder.build_socioeconomic_income_analysis_query(lat, lng, radius, entity_code)
            cursor.execute(radius_query)
            radius_data = cursor.fetchall()
            for row in radius_data:
                row['analysis_type'] = 'radius'
            
            # Breakdown data
            breakdown_query = self.query_builder.build_socioeconomic_breakdown_query(lat, lng, radius, entity_code)
            cursor.execute(breakdown_query)
            breakdown_data = cursor.fetchall()
            
            # Trends data
            trends_query = self.query_builder.build_socioeconomic_growth_trends_query(lat, lng, radius, entity_code)
            cursor.execute(trends_query)
            trends_data = cursor.fetchall()
            
            # Municipality analysis
            municipality_query = self.query_builder.build_socioeconomic_municipality_analysis_query(lat, lng, radius, entity_code)
            cursor.execute(municipality_query)
            municipality_data = cursor.fetchall()
            for row in municipality_data:
                row['analysis_type'] = 'municipality'
            
            # Municipality breakdown
            municipality_breakdown_query = self.query_builder.build_socioeconomic_municipality_breakdown_query(lat, lng, radius, entity_code)
            cursor.execute(municipality_breakdown_query)
            municipality_breakdown_data = cursor.fetchall()
            
            # Municipality trends
            municipality_trends_query = self.query_builder.build_socioeconomic_municipality_growth_trends_query(lat, lng, radius, entity_code)
            cursor.execute(municipality_trends_query)
            municipality_trends_data = cursor.fetchall()
            
            cursor.close()
            
            # Combine radius and municipality data for processing
            combined_data = radius_data + municipality_data
            
            # Process the combined data
            return self._process_combined_socioeconomic_data(
                combined_data, breakdown_data, trends_data, 
                municipality_breakdown_data, municipality_trends_data,
                lat, lng, radius
            )
            
        except Exception as e:
            logger.error(f"Error in _get_socioeconomic_income_analysis: {str(e)}")
            raise ValueError(f"Failed to retrieve socioeconomic income analysis: {str(e)}")

    def _process_combined_socioeconomic_data(self, combined_data, breakdown_data, trends_data, municipality_breakdown_data, municipality_trends_data, lat, lng, radius):
        """Process combined socioeconomic data."""
        if not combined_data:
            raise ValueError("No socioeconomic income data found for the specified location")
        
        
        # Separate radius and municipality data
        radius_data = next((row for row in combined_data if row.get('analysis_type') == 'radius'), {})
        municipality_data = next((row for row in combined_data if row.get('analysis_type') == 'municipality'), {})
        
        # Process breakdown data
        income_levels = []
        for level_data in breakdown_data:
            income_levels.append({
                "level": level_data.get("level"),
                "households": int(level_data.get("households", 0)),
                "household_percentage": float(level_data.get("pct_households", 0)),
                "total_income": int(level_data.get("total_income_level", 0)),
                "income_percentage": float(level_data.get("pct_income", 0))
            })
        
        # Process trends data
        growth_trends = []
        for trend_data in trends_data:
            growth_trends.append({
                "level": trend_data.get("level"),
                "growth_2016_2018": int(trend_data.get("growth_2016_2018", 0)),
                "growth_2018_2020": int(trend_data.get("growth_2018_2020", 0)),
                "growth_2020_2022": int(trend_data.get("growth_2020_2022", 0)),
                "growth_2022_2024": int(trend_data.get("growth_2022_2024", 0)),
                "growth_pct_2016_2018": float(trend_data.get("growth_pct_2016_2018", 0)),
                "growth_pct_2018_2020": float(trend_data.get("growth_pct_2018_2020", 0)),
                "growth_pct_2020_2022": float(trend_data.get("growth_pct_2020_2022", 0)),
                "growth_pct_2022_2024": float(trend_data.get("growth_pct_2022_2024", 0))
            })
        
        # Process municipality breakdown data
        municipality_income_levels = []
        for level_data in municipality_breakdown_data:
            municipality_income_levels.append({
                "level": level_data.get("level"),
                "households": int(level_data.get("households", 0)),
                "household_percentage": float(level_data.get("pct_households", 0)),
                "total_income": int(level_data.get("total_income_level", 0)),
                "income_percentage": float(level_data.get("pct_income", 0))
            })
        
        # Process municipality trends data
        municipality_growth_trends = []
        for trend_data in municipality_trends_data:
            municipality_growth_trends.append({
                "level": trend_data.get("level"),
                "growth_2016_2018": int(trend_data.get("growth_2016_2018", 0)),
                "growth_2018_2020": int(trend_data.get("growth_2018_2020", 0)),
                "growth_2020_2022": int(trend_data.get("growth_2020_2022", 0)),
                "growth_2022_2024": int(trend_data.get("growth_2022_2024", 0)),
                "growth_pct_2016_2018": float(trend_data.get("growth_pct_2016_2018", 0)),
                "growth_pct_2018_2020": float(trend_data.get("growth_pct_2018_2020", 0)),
                "growth_pct_2020_2022": float(trend_data.get("growth_pct_2020_2022", 0)),
                "growth_pct_2022_2024": float(trend_data.get("growth_pct_2022_2024", 0))
            })
        
        return {
            "income_summary": {
                "total_households": int(radius_data.get("total_households", 0)),
                "total_household_income": int(radius_data.get("total_household_income", 0)),
                "average_household_income": float(radius_data.get("avg_household_income", 0))
            },
            "income_distribution": {
                "levels": income_levels
            },
            "historical_trends": {
                "growth_by_level": growth_trends
            },
            "municipality_analysis": {
                "income_summary": {
                    "total_households": int(municipality_data.get("total_households", 0)),
                    "total_household_income": int(municipality_data.get("total_household_income", 0)),
                    "average_household_income": float(municipality_data.get("avg_household_income", 0))
                },
                "income_distribution": {
                    "levels": municipality_income_levels
                },
                "historical_trends": {
                    "growth_by_level": municipality_growth_trends
                }
            }
        }

    def _process_socioeconomic_income_data(self, income_data, breakdown_data, trends_data, municipality_income_data, municipality_breakdown_data, municipality_trends_data, lat, lng, radius):
        """Process socioeconomic income analysis data."""
        if not income_data:
            raise ValueError("No socioeconomic income data found for the specified location")
        
        
        income_summary = income_data[0] if income_data else {}
        breakdown = breakdown_data if breakdown_data else []
        trends = trends_data if trends_data else []
        
        # Process breakdown data
        income_levels = []
        for level_data in breakdown:
            income_levels.append({
                "level": level_data.get("level"),
                "households": int(level_data.get("households", 0)),
                "household_percentage": float(level_data.get("pct_households", 0)),
                "total_income": int(level_data.get("total_income_level", 0)),
                "income_percentage": float(level_data.get("pct_income", 0))
            })
        
        # Process trends data
        growth_trends = []
        for trend_data in trends:
            growth_trends.append({
                "level": trend_data.get("level"),
                "growth_2016_2018": int(trend_data.get("growth_2016_2018", 0)),
                "growth_2018_2020": int(trend_data.get("growth_2018_2020", 0)),
                "growth_2020_2022": int(trend_data.get("growth_2020_2022", 0)),
                "growth_2022_2024": int(trend_data.get("growth_2022_2024", 0)),
                "growth_pct_2016_2018": float(trend_data.get("growth_pct_2016_2018", 0)),
                "growth_pct_2018_2020": float(trend_data.get("growth_pct_2018_2020", 0)),
                "growth_pct_2020_2022": float(trend_data.get("growth_pct_2020_2022", 0)),
                "growth_pct_2022_2024": float(trend_data.get("growth_pct_2022_2024", 0))
            })
        
        # Process municipality-level data
        municipality_summary = municipality_income_data[0] if municipality_income_data else {}
        municipality_breakdown = municipality_breakdown_data if municipality_breakdown_data else []
        municipality_trends = municipality_trends_data if municipality_trends_data else []
        
        # Process municipality breakdown data
        municipality_income_levels = []
        for level_data in municipality_breakdown:
            municipality_income_levels.append({
                "level": level_data.get("level"),
                "households": int(level_data.get("households", 0)),
                "household_percentage": float(level_data.get("pct_households", 0)),
                "total_income": int(level_data.get("total_income_level", 0)),
                "income_percentage": float(level_data.get("pct_income", 0))
            })
        
        # Process municipality trends data
        municipality_growth_trends = []
        for trend_data in municipality_trends:
            municipality_growth_trends.append({
                "level": trend_data.get("level"),
                "growth_2016_2018": int(trend_data.get("growth_2016_2018", 0)),
                "growth_2018_2020": int(trend_data.get("growth_2018_2020", 0)),
                "growth_2020_2022": int(trend_data.get("growth_2020_2022", 0)),
                "growth_2022_2024": int(trend_data.get("growth_2022_2024", 0)),
                "growth_pct_2016_2018": float(trend_data.get("growth_pct_2016_2018", 0)),
                "growth_pct_2018_2020": float(trend_data.get("growth_pct_2018_2020", 0)),
                "growth_pct_2020_2022": float(trend_data.get("growth_pct_2020_2022", 0)),
                "growth_pct_2022_2024": float(trend_data.get("growth_pct_2022_2024", 0))
            })
        
        return {
            "income_summary": {
                "total_households": int(income_summary.get("total_households", 0)),
                "total_household_income": int(income_summary.get("total_household_income", 0)),
                "average_household_income": float(income_summary.get("avg_household_income", 0))
            },
            "income_distribution": {
                "levels": income_levels
            },
            "historical_trends": {
                "growth_by_level": growth_trends
            },
            "municipality_analysis": {
                "income_summary": {
                    "total_households": int(municipality_summary.get("total_households", 0)),
                    "total_household_income": int(municipality_summary.get("total_household_income", 0)),
                    "average_household_income": float(municipality_summary.get("avg_household_income", 0))
                },
                "income_distribution": {
                    "levels": municipality_income_levels
                },
                "historical_trends": {
                    "growth_by_level": municipality_growth_trends
                }
            }
        }


#New area analysis for the area search

from module.area_data.controller import DemographicsAreaData

class DemographicsAreaAnalysisController:
    """Controller for demographics area analysis."""
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()
        self.qc = ""
    
    def get_boundary_from_coordinates(self,lat,long,radius,city=None):
        """Get boundary from coordinates as WKT polygon (radius in meters)."""
        query = self.qc.get_boundary_from_coordinates(lat,long,radius,city)
        redshift_connection = self.redshift_db.connect()
        cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        res = cursor.fetchone()
        cursor.close()
        redshift_connection.close()
        return res
    
    def get_municipality_info(self,lat,lng,city=None):
        """Get municipality info (code, name, population, boundary WKT) from coordinates."""
        query = self.qc.get_municipality_info(lat,lng,city)
        redshift_connection = self.redshift_db.connect()
        cursor = redshift_connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        res = cursor.fetchone()
        cursor.close()
        redshift_connection.close()
        return res
    

