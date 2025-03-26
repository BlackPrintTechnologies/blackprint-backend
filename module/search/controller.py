from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from datetime import datetime
from decimal import Decimal

class SavedSearchesController:
    def __init__(self):
        self.db = Database()

    def get_saved_searches(self, id=None, user_id=None):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM bp_saved_searches WHERE 1=1'
            if id:
                query += f' AND id = {id}'
            if user_id:
                query += f' AND user_id = {user_id}'

            print(query, "<==============")
            cursor.execute(query)
            result = cursor.fetchall()
            processed_result = []
            for search in result:
                search_dict = dict(search)
                search_dict['created_at'] = search_dict['created_at'].isoformat()
                search_dict['updated_at'] = search_dict['updated_at'].isoformat()
                processed_result.append(search_dict)

            return Response.success(data=processed_result)
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)

    def create_saved_search(self, user_id, search_name, search_query, search_value, search_response):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = f'''
                INSERT INTO bp_saved_searches (user_id, search_name, search_query, search_value, search_response)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            '''
            print("query=====>", query)

            cursor.execute(query, (user_id, search_name, search_query, search_value, search_response))
            connection.commit()
            search_id = cursor.fetchone()['id']
            return Response.created(data={"id": search_id})
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)

    def update_saved_search(self, id, search_name=None, search_query=None, search_value=None, search_response=None, search_status=None):
        connection = None
        cursor = None
        try:
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
                return Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE id = %s'
            params.append(id)

            cursor.execute(query, tuple(params))
            connection.commit()
            return Response.success(message="Saved search updated successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)

    def delete_saved_search(self, id):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM bp_saved_searches WHERE id = %s'
            cursor.execute(query, (id,))
            connection.commit()
            return Response.success(message="Saved search deleted successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect(connection)