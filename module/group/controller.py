from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from datetime import datetime
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class GroupsController:
    def __init__(self):
        logger.debug("Initializing GroupsController")
        self.db = Database()

    def get_groups(self, grp_id=None, user_id=None):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Getting groups - grp_id: {grp_id}, user_id: {user_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM groups WHERE 1=1'
            if grp_id:
                query += f' AND grp_id = {grp_id}'
            if user_id:
                query += f' AND user_id = {user_id}'

            logger.debug(f"Executing query: {query}")
            cursor.execute(query)
            result = cursor.fetchall()
            
            logger.debug(f"Found {len(result)} groups")
            processed_result = []
            for group in result:
                group_dict = dict(group)
                group_dict['created_at'] = group_dict['created_at'].isoformat()
                group_dict['updated_at'] = group_dict['updated_at'].isoformat()
                processed_result.append(group_dict)

            resp = Response.success(data=processed_result)
            logger.info("Successfully retrieved groups")
            
        except Exception as e:
            logger.error(f"Error getting groups: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp

    def create_group(self, user_id, grp_name, property_ids):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Creating group - user_id: {user_id}, grp_name: {grp_name}")
            logger.debug(f"Property IDs: {property_ids}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = '''
                INSERT INTO groups (user_id, grp_name, property_ids)
                VALUES (%s, %s, %s)
                RETURNING grp_id
            '''
            logger.debug(f"Executing query: {query} with params: ({user_id}, {grp_name}, {property_ids})")

            cursor.execute(query, (user_id, grp_name, property_ids))
            connection.commit()
            grp_id = cursor.fetchone()['grp_id']
            logger.info(f"Group created successfully with ID: {grp_id}")
            resp = Response.created(data={"grp_id": grp_id})
            
        except Exception as e:
            logger.error(f"Error creating group: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp

    def update_group(self, grp_id, grp_name=None, property_ids=None, gpr_status=None):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Updating group - grp_id: {grp_id}")
            logger.debug(f"Update params - grp_name: {grp_name}, property_ids: {property_ids}, gpr_status: {gpr_status}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE groups SET '
            updates = []
            params = []

            if grp_name:
                updates.append('grp_name = %s')
                params.append(grp_name)
            if property_ids:
                updates.append('property_ids = %s')
                params.append(property_ids)
            if gpr_status is not None:
                updates.append('gpr_status = %s')
                params.append(gpr_status)

            if not updates:
                logger.warning("No fields provided for update")
                return Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE grp_id = %s'
            params.append(grp_id)

            logger.debug(f"Executing update query: {query} with params: {params}")
            cursor.execute(query, tuple(params))
            connection.commit()
            logger.info(f"Group {grp_id} updated successfully")
            resp = Response.success(message="Group updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating group {grp_id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp

    def delete_group(self, grp_id):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Deleting group - grp_id: {grp_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM groups WHERE grp_id = %s'
            logger.debug(f"Executing delete query: {query} with param: {grp_id}")

            cursor.execute(query, (grp_id,))
            connection.commit()
            logger.info(f"Group {grp_id} deleted successfully")
            resp = Response.success(message="Group deleted successfully")
            
        except Exception as e:
            logger.error(f"Error deleting group {grp_id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp

    def add_property_to_group(self, grp_id, property_id):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Adding property to group - grp_id: {grp_id}, property_id: {property_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE groups SET property_ids = array_append(property_ids, %s) WHERE grp_id = %s'
            logger.debug(f"Executing query: {query} with params: ({property_id}, {grp_id})")

            cursor.execute(query, (property_id, grp_id))
            connection.commit()
            logger.info(f"Property {property_id} added to group {grp_id} successfully")
            resp = Response.success(message="Property added to group successfully")
            
        except Exception as e:
            logger.error(f"Error adding property {property_id} to group {grp_id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp

    def update_property_for_group(self, grp_id, property_id):
        connection = None
        cursor = None
        resp = None
        try:
            logger.info(f"Updating property for group - grp_id: {grp_id}")
            logger.debug(f"New property_ids: {property_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE groups SET property_ids = %s::integer[] WHERE grp_id = %s RETURNING property_ids'
            logger.debug(f"Executing query: {query} with params: ({property_id}, {grp_id})")

            cursor.execute(query, (property_id, grp_id))
            connection.commit()
            
            updated_property_ids = cursor.fetchone()['property_ids']
            logger.info(f"Group {grp_id} properties updated successfully")
            logger.debug(f"Updated property_ids: {updated_property_ids}")
            
            resp = Response.success(data={"property_ids": updated_property_ids}, message="Property updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating properties for group {grp_id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp

    def remove_property_from_group(self, grp_id, property_id):
        connection = None
        cursor = None
        try:
            logger.info(f"Removing property from group - grp_id: {grp_id}, property_id: {property_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE groups SET property_ids = array_remove(property_ids, %s) WHERE grp_id = %s'
            logger.debug(f"Executing query: {query} with params: ({property_id}, {grp_id})")

            cursor.execute(query, (property_id, grp_id))
            connection.commit()
            logger.info(f"Property {property_id} removed from group {grp_id} successfully")
            resp = Response.success(message="Property removed from group successfully")
            
        except Exception as e:
            logger.error(f"Error removing property {property_id} from group {grp_id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect(connection)
                logger.debug("Database connection closed")
            return resp