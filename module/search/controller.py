from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from datetime import datetime
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

class SavedSearchesController:
    def __init__(self):
        logger.debug("Initializing SavedSearchesController")
        self.db = Database()

    def get_saved_searches(self, id=None, user_id=None):
        connection = None
        cursor = None
        try:
            logger.info(f"Getting saved searches - id: {id}, user_id: {user_id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM bp_saved_searches WHERE 1=1'
            if id:
                query += f' AND id = {id}'
            if user_id:
                query += f' AND user_id = {user_id}'

            logger.debug(f"Executing query: {query}")
            print(query, "<==============")
            cursor.execute(query)
            result = cursor.fetchall()
            
            logger.debug(f"Found {len(result)} saved searches")
            processed_result = []
            for search in result:
                search_dict = dict(search)
                search_dict['created_at'] = search_dict['created_at'].isoformat()
                search_dict['updated_at'] = search_dict['updated_at'].isoformat()
                processed_result.append(search_dict)

            logger.info("Successfully retrieved saved searches")
            return Response.success(data=processed_result)
        except Exception as e:
            logger.error(f"Error getting saved searches: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect()
                logger.debug("Database connection closed")

    def create_saved_search(self, user_id, search_name, search_query, search_value, search_response):
        connection = None
        cursor = None
        try:
            logger.info(f"Creating saved search - user_id: {user_id}, search_name: {search_name}")
            logger.debug(f"Search params - query: {search_query}, value: {search_value}, response: {search_response}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = f'''
                INSERT INTO bp_saved_searches (user_id, search_name, search_query, search_value, search_response)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            '''
            logger.debug(f"Executing query: {query} with params: ({user_id}, {search_name}, {search_query}, {search_value}, {search_response})")
            print("query=====>", query)

            cursor.execute(query, (user_id, search_name, search_query, search_value, search_response))
            connection.commit()
            search_id = cursor.fetchone()['id']
            logger.info(f"Saved search created successfully with ID: {search_id}")
            return Response.created(data={"id": search_id})
        except Exception as e:
            logger.error(f"Error creating saved search: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect()
                logger.debug("Database connection closed")

    def update_saved_search(self, id, search_name=None, search_query=None, search_value=None, search_response=None, search_status=None):
        connection = None
        cursor = None
        try:
            logger.info(f"Updating saved search - id: {id}")
            logger.debug(f"Update params - name: {search_name}, query: {search_query}, value: {search_value}, response: {search_response}, status: {search_status}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE bp_saved_searches SET '
            updates = []
            params = []

            if search_name:
                updates.append('search_name = %s')
                params.append(search_name)
            if search_query:
                updates.append('search_query = %s')
                params.append(search_query)
            if search_value:
                updates.append('search_value = %s')
                params.append(search_value)
            if search_response:
                updates.append('search_response = %s')
                params.append(search_response)
            if search_status is not None:
                updates.append('search_status = %s')
                params.append(search_status)

            if not updates:
                logger.warning("No fields provided for update")
                return Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE id = %s'
            params.append(id)

            logger.debug(f"Executing update query: {query} with params: {params}")
            cursor.execute(query, tuple(params))
            connection.commit()
            logger.info(f"Saved search {id} updated successfully")
            return Response.success(message="Saved search updated successfully")
        except Exception as e:
            logger.error(f"Error updating saved search {id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect()
                logger.debug("Database connection closed")

    def delete_saved_search(self, id):
        connection = None
        cursor = None
        try:
            logger.info(f"Deleting saved search - id: {id}")
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM bp_saved_searches WHERE id = %s'
            logger.debug(f"Executing delete query: {query} with param: {id}")

            cursor.execute(query, (id,))
            connection.commit()
            logger.info(f"Saved search {id} deleted successfully")
            return Response.success(message="Saved search deleted successfully")
        except Exception as e:
            logger.error(f"Error deleting saved search {id}: {str(e)}", exc_info=True)
            if connection:
                connection.rollback()
                logger.debug("Transaction rolled back due to error")
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
                logger.debug("Cursor closed")
            if connection:
                self.db.disconnect()
                logger.debug("Database connection closed")