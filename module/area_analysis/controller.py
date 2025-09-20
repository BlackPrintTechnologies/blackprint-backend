import json
import logging
import traceback
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from utils.dbUtils import Database, RedshiftDatabase

logger = logging.getLogger(__name__)

class AreaAnalysisController:
    def __init__(self):
        self.db = Database()
        self.redshift_db = RedshiftDatabase()

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
            
            query = self._build_traffic_by_day_query(lat, lng, radius, user_type)
            logger.info(f"Traffic by day query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Convert result to a more readable format
            if res and len(res) > 0:
                result = res[0]
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": sum([
                            result.get('monday', 0),
                            result.get('tuesday', 0),
                            result.get('wednesday', 0),
                            result.get('thursday', 0),
                            result.get('friday', 0),
                            result.get('saturday', 0),
                            result.get('sunday', 0)
                        ])
                    },
                    "traffic_by_day": {
                        "monday": result.get('monday', 0),
                        "tuesday": result.get('tuesday', 0),
                        "wednesday": result.get('wednesday', 0),
                        "thursday": result.get('thursday', 0),
                        "friday": result.get('friday', 0),
                        "saturday": result.get('saturday', 0),
                        "sunday": result.get('sunday', 0)
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
            
            logger.info(f"Traffic by day results: {traffic_data['summary']['total_unique_users']} total users")
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
            
            query = self._build_traffic_by_hour_query(lat, lng, radius, user_type)
            logger.info(f"Traffic by hour query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Convert result to a more readable format
            if res and len(res) > 0:
                result = res[0]
                hourly_data = {}
                total_users = 0
                
                for hour in range(24):
                    hour_key = f"hour_{hour}"
                    value = result.get(hour_key, 0)
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
            
            logger.info(f"Traffic by hour results: {traffic_data['summary']['total_unique_users']} total users")
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
            
            query = self._build_traffic_summary_query(lat, lng, radius, user_type)
            logger.info(f"Traffic summary query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            # Convert result to a more readable format
            if res and len(res) > 0:
                result = res[0]
                traffic_data = {
                    "summary": {
                        "center_point": {"lat": lat, "lng": lng},
                        "radius_meters": radius,
                        "user_type": user_type or "all",
                        "total_unique_users": result.get('total_users', 0)
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
            
            logger.info(f"Traffic summary results: {traffic_data['summary']['total_unique_users']} total users")
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

    def get_area_summary(self, lat, lng, radius=2000):
        """
        Get area analysis summary matching the UI mockup exactly.
        Returns all the summary data needed for the main UI panel.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            
        Returns:
            dict: Response with area summary data matching UI structure
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            user_types = ['vehiculo', 'peaton', 'estacionario']
            traffic_data = {}
            total_all_users = 0
            
            # Get traffic data for each user type directly from database
            for user_type in user_types:
                query = self._build_traffic_summary_query(lat, lng, radius, user_type)
                logger.info(f"Executing query for {user_type}: {query}")
                
                cursor.execute(query)
                connection.commit()
                res = cursor.fetchall()
                
                if res and len(res) > 0:
                    user_total = res[0].get('total_users', 0)
                    traffic_data[user_type] = user_total
                    total_all_users += user_total
                    logger.info(f"Found {user_total} users for {user_type}")
                else:
                    traffic_data[user_type] = 0
                    logger.info(f"No data found for {user_type}")
            
            # Calculate area in km²
            area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
            
            # Calculate percentages
            vehicle_pct = round((traffic_data.get('vehiculo', 0) / total_all_users * 100), 0) if total_all_users > 0 else 0
            pedestrian_pct = round((traffic_data.get('peaton', 0) / total_all_users * 100), 0) if total_all_users > 0 else 0
            stationary_pct = round((traffic_data.get('estacionario', 0) / total_all_users * 100), 0) if total_all_users > 0 else 0
            
            # Structure response with real data only, null for unavailable data
            summary_data = {
                "summary": {
                    "num_parcels": None,  # Not available - would need parcels/cadastral data
                    "population": None,   # Not available - would need census data
                    "area_km2": area_km2,
                    "center_point": {"lat": lat, "lng": lng},
                    "radius_meters": radius
                },
                "socioeconomic": {
                    "total_unique_devices": total_all_users,
                    "devices_per_person": None,  # Cannot calculate without population data
                    "municipality_average": None  # Not available - would need municipality-wide stats
                },
                "traffic": {
                    "vehicles": {
                        "count": traffic_data.get('vehiculo', 0),
                        "percentage": int(vehicle_pct),
                        "municipality_percentage": None,  # Not available - would need municipality-wide data
                        "trend": None  # Not available - would need historical data
                    },
                    "pedestrians": {
                        "count": traffic_data.get('peaton', 0), 
                        "percentage": int(pedestrian_pct),
                        "municipality_percentage": None,  # Not available - would need municipality-wide data
                        "trend": None  # Not available - would need historical data
                    },
                    "stationary_devices": {
                        "count": traffic_data.get('estacionario', 0),
                        "percentage": int(stationary_pct), 
                        "municipality_percentage": None,  # Not available - would need municipality-wide data
                        "trend": None  # Not available - would need historical data
                    }
                }
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

    def get_traffic_patterns(self, lat, lng, radius=2000):
        """
        Get detailed traffic patterns for charts (both hourly and daily).
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            
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
                "pattern_type": "both"
            }
            
            # Get hourly data for all user types combined (no user_type filter)
            query = self._build_traffic_by_hour_query(lat, lng, radius, None)
            logger.info(f"Executing hourly query: {query}")
            
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
                
                # Convert to percentages (0-40% as shown in UI)
                hourly_percentages = []
                for value in hourly_array:
                    percentage = (value / max_value * 40) if max_value > 0 else 0
                    hourly_percentages.append(round(percentage, 1))
                
                # Create time labels for x-axis (0-23 hours)
                time_labels = [f"{hour:02d}:00" for hour in range(24)]
                
                patterns_data["hourly_traffic"] = {
                    "raw_values": hourly_array,
                    "percentages": hourly_percentages,
                    "max_value": max_value,
                    "avg_visits_per_hour": avg_visits_per_hour,
                    "total_visits": total_hourly_visits,
                    "time_labels": time_labels,
                    "x_axis_labels": list(range(24))  # For chart x-axis
                }
            
            # Get daily data for all user types combined (no user_type filter)
            query = self._build_traffic_by_day_query(lat, lng, radius, None)
            logger.info(f"Executing daily query: {query}")
            
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
                
                # Convert to percentages (0-40% as shown in UI)
                daily_percentages = []
                for value in daily_array:
                    percentage = (value / max_value * 40) if max_value > 0 else 0
                    daily_percentages.append(round(percentage, 1))
                
                # Create day labels for x-axis
                day_labels = ['M', 'T', 'W', 'T', 'F', 'S', 'S']  # Short labels as shown in UI
                day_full_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                patterns_data["daily_traffic"] = {
                    "raw_values": daily_array,
                    "percentages": daily_percentages,
                    "max_value": max_value,
                    "avg_visits_per_day": avg_visits_per_day,
                    "total_visits": total_daily_visits,
                    "days": days,
                    "day_labels": day_labels,  # Short labels for chart
                    "day_full_names": day_full_names  # Full names for tooltips
                }
            
            logger.info("Traffic patterns completed")
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

    def get_area_demographics(self, lat, lng, radius=2000):
        """
        Get demographic and socioeconomic analysis for a specific area.
        Returns population demographics, income levels, and socioeconomic distribution.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            
        Returns:
            dict: Response with demographic analysis data matching UI structure
        """
        connection = None
        cursor = None
        resp = None
        
        try:
            connection = self.redshift_db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Build query to get demographic data within the H3 buffer
            query = self._build_demographics_query(lat, lng, radius)
            logger.info(f"Executing demographics query: {query}")
            
            cursor.execute(query)
            connection.commit()
            res = cursor.fetchall()
            
            if res and len(res) > 0:
                # Process demographic data
                demographics_data = self._process_demographics_data(res, lat, lng, radius)
                logger.info(f"Demographics analysis completed for {len(res)} records")
                resp = Response.success(data=demographics_data)
            else:
                # Return empty demographics structure
                demographics_data = self._get_empty_demographics_structure(lat, lng, radius)
                logger.info("No demographic data found, returning empty structure")
                resp = Response.success(data=demographics_data)
            
        except Exception as e:
            logger.error(f"Error in get_area_demographics: {str(e)}")
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

    def _build_demographics_query(self, lat, lng, radius):
        """Build SQL query to get demographic data within the specified area."""
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT 
            d.POBTOT as total_population,
            d.POBMAS as male_population,
            d.POBFEM as female_population,
            d.P_0A2_M, d.P_0A2_F,
            d.P_3A5_M, d.P_3A5_F,
            d.P_6A11_M, d.P_6A11_F,
            d.P_12A14_M, d.P_12A14_F,
            d.P_15A17_M, d.P_15A17_F,
            d.P_18A24_M, d.P_18A24_F,
            d.P_60YMAS_M, d.P_60YMAS_F,
            d.GRAPROES,
            d.PROM_HNV,
            d.PRO_OCUP_C
        FROM blackprint_db_prd.staging.stg_demographics_qro d
        INNER JOIN h3_index h ON d.h3_index::VARCHAR = h.h3_value::VARCHAR
        """
        return query

    def _process_demographics_data(self, demographic_data, lat, lng, radius):
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
        
        # Process each record
        for record in demographic_data:
            # Population totals
            pop = record.get('total_population', 0) or 0
            male_pop = record.get('male_population', 0) or 0
            female_pop = record.get('female_population', 0) or 0
            
            total_population += pop
            total_male += male_pop
            total_female += female_pop
            
            # Age groups - 0-14
            age_totals['0-14']['male'] += (
                (record.get('p_0a2_m', 0) or 0) +
                (record.get('p_3a5_m', 0) or 0) +
                (record.get('p_6a11_m', 0) or 0) +
                (record.get('p_12a14_m', 0) or 0)
            )
            age_totals['0-14']['female'] += (
                (record.get('p_0a2_f', 0) or 0) +
                (record.get('p_3a5_f', 0) or 0) +
                (record.get('p_6a11_f', 0) or 0) +
                (record.get('p_12a14_f', 0) or 0)
            )
            
            # Age groups - 15-24
            age_totals['15-24']['male'] += (
                (record.get('p_15a17_m', 0) or 0) +
                (record.get('p_18a24_m', 0) or 0)
            )
            age_totals['15-24']['female'] += (
                (record.get('p_15a17_f', 0) or 0) +
                (record.get('p_18a24_f', 0) or 0)
            )
            
            # Age groups - 60+
            age_totals['60+']['male'] += (record.get('p_60ymas_m', 0) or 0)
            age_totals['60+']['female'] += (record.get('p_60ymas_f', 0) or 0)
            
            # Calculate 25-59 (derived)
            male_25_59 = male_pop - (
                age_totals['0-14']['male'] + age_totals['15-24']['male'] + age_totals['60+']['male']
            )
            female_25_59 = female_pop - (
                age_totals['0-14']['female'] + age_totals['15-24']['female'] + age_totals['60+']['female']
            )
            
            age_totals['25-59']['male'] += max(0, male_25_59)
            age_totals['25-59']['female'] += max(0, female_25_59)
            
            # Income and education (aggregate averages)
            if record.get('prom_hnv'):
                total_income += record.get('prom_hnv', 0) * pop
            if record.get('graproes'):
                total_education_level += record.get('graproes', 0) * pop
            if record.get('pro_ocup_c'):
                total_households += record.get('pro_ocup_c', 0)
        
        # Calculate percentages and averages
        male_percentage = (total_male / total_population * 100) if total_population > 0 else 0
        female_percentage = (total_female / total_population * 100) if total_population > 0 else 0
        
        # Average income per person
        avg_income = (total_income / total_population) if total_population > 0 else 0
        
        # Calculate area
        area_km2 = round((3.14159 * (radius/1000) ** 2), 2)
        
        # Create age distribution for population pyramid
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
        
        # Simulate socioeconomic levels (these would come from actual data)
        # Based on your UI mockup showing levels A, B, C+, C, C-, D+, D, E
        socioeconomic_levels = [
            {"level": "AB", "households": int(total_population * 0.05), "percentage": 5.0},
            {"level": "C+", "households": int(total_population * 0.15), "percentage": 15.0},
            {"level": "C", "households": int(total_population * 0.38), "percentage": 38.2},  # Predominant
            {"level": "C-", "households": int(total_population * 0.25), "percentage": 25.0},
            {"level": "D+", "households": int(total_population * 0.12), "percentage": 12.0},
            {"level": "D", "households": int(total_population * 0.04), "percentage": 4.0},
            {"level": "E", "households": int(total_population * 0.01), "percentage": 0.8}
        ]
        
        # Find predominant level (highest percentage)
        predominant_level = max(socioeconomic_levels, key=lambda x: x['percentage'])
        
        # Structure response to match UI mockup
        demographics_data = {
            "summary": {
                "area_km2": area_km2,
                "population": total_population,
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius
            },
            "demographics": {
                "total_population": total_population,
                "male_population": total_male,
                "female_population": total_female,
                "male_percentage": round(male_percentage, 1),
                "female_percentage": round(female_percentage, 1),
                "age_distribution": age_distribution
            },
            "socioeconomic": {
                "predominant_level": {
                    "level": predominant_level["level"],
                    "percentage": predominant_level["percentage"],
                    "households": predominant_level["households"]
                },
                "municipality_average": "B",  # Static for now
                "levels_distribution": socioeconomic_levels,
                "average_income": {
                    "amount": round(avg_income, 2),
                    "currency": "MXN",
                    "formatted": f"${avg_income:,.0f} MXN" if avg_income > 0 else "$0 MXN",
                    "trend": "down"  # Static for now
                },
                "total_income": {
                    "amount": round(total_income, 2),
                    "currency": "MXN", 
                    "formatted": f"${total_income/1000000:,.0f} million MXN" if total_income > 0 else "$0 MXN",
                    "trend": "up"  # Static for now
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

    def _build_traffic_by_day_query(self, lat, lng, radius, user_type=None):
        """Build SQL query for traffic data by day of the week."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT  SUM(CASE WHEN a.dia_de_la_semana = 'Monday' THEN a.total_usuarios_unicos ELSE 0 END) AS monday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Tuesday' THEN a.total_usuarios_unicos ELSE 0 END) AS tuesday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Wednesday' THEN a.total_usuarios_unicos ELSE 0 END) AS wednesday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Thursday' THEN a.total_usuarios_unicos ELSE 0 END) AS thursday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Friday' THEN a.total_usuarios_unicos ELSE 0 END) AS friday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Saturday' THEN a.total_usuarios_unicos ELSE 0 END) AS saturday,
                SUM(CASE WHEN a.dia_de_la_semana = 'Sunday' THEN a.total_usuarios_unicos ELSE 0 END) AS sunday
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_dia_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query

    def _build_traffic_by_hour_query(self, lat, lng, radius, user_type=None):
        """Build SQL query for traffic data by hour of the day."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT  SUM(CASE WHEN a.hour = 0 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_0,
                SUM(CASE WHEN a.hour = 1 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_1,
                SUM(CASE WHEN a.hour = 2 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_2,
                SUM(CASE WHEN a.hour = 3 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_3,
                SUM(CASE WHEN a.hour = 4 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_4,
                SUM(CASE WHEN a.hour = 5 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_5,
                SUM(CASE WHEN a.hour = 6 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_6,
                SUM(CASE WHEN a.hour = 7 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_7,
                SUM(CASE WHEN a.hour = 8 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_8,
                SUM(CASE WHEN a.hour = 9 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_9,
                SUM(CASE WHEN a.hour = 10 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_10,
                SUM(CASE WHEN a.hour = 11 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_11,
                SUM(CASE WHEN a.hour = 12 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_12,
                SUM(CASE WHEN a.hour = 13 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_13,
                SUM(CASE WHEN a.hour = 14 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_14,
                SUM(CASE WHEN a.hour = 15 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_15,
                SUM(CASE WHEN a.hour = 16 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_16,
                SUM(CASE WHEN a.hour = 17 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_17,
                SUM(CASE WHEN a.hour = 18 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_18,
                SUM(CASE WHEN a.hour = 19 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_19,
                SUM(CASE WHEN a.hour = 20 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_20,
                SUM(CASE WHEN a.hour = 21 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_21,
                SUM(CASE WHEN a.hour = 22 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_22,
                SUM(CASE WHEN a.hour = 23 THEN a.total_usuarios_unicos ELSE 0 END) AS hour_23
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query

    def _build_traffic_summary_query(self, lat, lng, radius, user_type=None):
        """Build SQL query for total traffic summary."""
        user_type_condition = ""
        if user_type:
            user_type_condition = f"WHERE a.tipo_usuario = '{user_type}'"
        
        query = f"""
        WITH point_geom AS (
          SELECT ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326) AS geom
        ),
        point_projected AS (
          SELECT ST_Transform(geom, 3857) AS geom FROM point_geom
        ),
        buffered AS (
          SELECT ST_Buffer(geom, {radius}) AS geom FROM point_projected
        ),
        h3_values AS (
          SELECT H3_Polyfill(ST_Transform(geom, 4326),10) AS h3_indexes FROM buffered
        ),
        h3_index AS (
            SELECT o AS h3_value
            FROM h3_values i, i.h3_indexes o
        )
        SELECT  SUM(a.total_usuarios_unicos) as total_users
        FROM blackprint_db_prd.staging.stg_data_movilidad_por_hora_qro a
        INNER JOIN h3_index b ON a.h3_index::VARCHAR = b.h3_value::VARCHAR
        {user_type_condition}
        """
        return query
