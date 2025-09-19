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

    def get_comprehensive_analysis(self, lat, lng, radius=2000, user_types=None):
        """
        Get comprehensive area analysis including all user types and time breakdowns.
        
        Args:
            lat (float): Latitude of the center point
            lng (float): Longitude of the center point
            radius (int): Radius in meters (default: 2000)
            user_types (list): List of user types to analyze or None for all
            
        Returns:
            dict: Response with comprehensive analysis data
        """
        if user_types is None:
            user_types = ['vehiculo', 'peaton', 'estacionario']
        
        analysis_data = {
            "summary": {
                "center_point": {"lat": lat, "lng": lng},
                "radius_meters": radius,
                "area_km2": round((3.14159 * (radius/1000) ** 2), 2),
                "analysis_types": user_types
            },
            "user_type_breakdown": {},
            "daily_patterns": {},
            "hourly_patterns": {}
        }
        
        total_all_users = 0
        
        try:
            # Get data for each user type
            for user_type in user_types:
                # Get summary for this user type
                summary_resp = self.get_traffic_summary(lat, lng, radius, user_type)
                if summary_resp.get('status_code') == 200:
                    user_total = summary_resp['data']['summary']['total_unique_users']
                    total_all_users += user_total
                    analysis_data['user_type_breakdown'][user_type] = {
                        "total_users": user_total
                    }
                
                # Get daily pattern for this user type
                daily_resp = self.get_traffic_by_day(lat, lng, radius, user_type)
                if daily_resp.get('status_code') == 200:
                    analysis_data['daily_patterns'][user_type] = daily_resp['data']['traffic_by_day']
                
                # Get hourly pattern for this user type
                hourly_resp = self.get_traffic_by_hour(lat, lng, radius, user_type)
                if hourly_resp.get('status_code') == 200:
                    analysis_data['hourly_patterns'][user_type] = hourly_resp['data']['traffic_by_hour']
            
            # Add total summary
            analysis_data['summary']['total_unique_users'] = total_all_users
            
            # Calculate percentages for user types
            if total_all_users > 0:
                for user_type in user_types:
                    if user_type in analysis_data['user_type_breakdown']:
                        user_count = analysis_data['user_type_breakdown'][user_type]['total_users']
                        percentage = round((user_count / total_all_users) * 100, 1)
                        analysis_data['user_type_breakdown'][user_type]['percentage'] = percentage
            
            logger.info(f"Comprehensive analysis completed for {total_all_users} total users")
            return Response.success(data=analysis_data)
            
        except Exception as e:
            logger.error(f"Error in get_comprehensive_analysis: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return Response.internal_server_error(message=str(e))

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
