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
            
            # Structure response to match UI mockup exactly
            summary_data = {
                "summary": {
                    "num_parcels": 435,  # Static for now - could be calculated from H3 cells
                    "population": 6489,  # Static for now - could be from census data
                    "area_km2": area_km2,
                    "center_point": {"lat": lat, "lng": lng},
                    "radius_meters": radius
                },
                "socioeconomic": {
                    "total_unique_devices": total_all_users,
                    "devices_per_person": round(total_all_users / 6489, 2) if total_all_users > 0 else 0,
                    "municipality_average": 1.84  # Static for now
                },
                "traffic": {
                    "vehicles": {
                        "count": traffic_data.get('vehiculo', 0),
                        "percentage": int(vehicle_pct),
                        "municipality_percentage": 50,  # Static for now
                        "trend": "down"  # Static for now - could be calculated from historical data
                    },
                    "pedestrians": {
                        "count": traffic_data.get('peaton', 0), 
                        "percentage": int(pedestrian_pct),
                        "municipality_percentage": 36,  # Static for now
                        "trend": "down"  # Static for now
                    },
                    "stationary_devices": {
                        "count": traffic_data.get('estacionario', 0),
                        "percentage": int(stationary_pct), 
                        "municipality_percentage": 14,  # Static for now
                        "trend": "up"  # Static for now
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

    def get_traffic_patterns(self, lat, lng, radius=2000, pattern_type='both'):
        """
        Get detailed traffic patterns for charts (hourly and/or daily).
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            pattern_type (str): 'hourly', 'daily', or 'both'
            
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
                "pattern_type": pattern_type
            }
            
            # Get combined traffic data for all user types
            if pattern_type in ['hourly', 'both']:
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
                    
                    # Convert to percentages (0-40% as shown in UI)
                    hourly_percentages = []
                    for value in hourly_array:
                        percentage = (value / max_value * 40) if max_value > 0 else 0
                        hourly_percentages.append(round(percentage, 1))
                    
                    patterns_data["hourly_traffic"] = {
                        "raw_values": hourly_array,
                        "percentages": hourly_percentages,
                        "max_value": max_value
                    }
            
            if pattern_type in ['daily', 'both']:
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
                    
                    # Convert to percentages (0-40% as shown in UI)
                    daily_percentages = []
                    for value in daily_array:
                        percentage = (value / max_value * 40) if max_value > 0 else 0
                        daily_percentages.append(round(percentage, 1))
                    
                    patterns_data["daily_traffic"] = {
                        "raw_values": daily_array,
                        "percentages": daily_percentages,
                        "max_value": max_value,
                        "days": days
                    }
            
            logger.info(f"Traffic patterns ({pattern_type}) completed")
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
