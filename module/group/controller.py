from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from datetime import datetime
from decimal import Decimal

class GroupsController:
    def __init__(self):
        self.db = Database()

    def get_groups(self, grp_id=None, user_id=None):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM groups WHERE 1=1'
            if grp_id:
                query += f' AND grp_id = {grp_id}'
            if user_id:
                query += f' AND user_id = {user_id}'

            print(query, "<==============")
            cursor.execute(query)
            result = cursor.fetchall()
            processed_result = []
            for group in result:
                group_dict = dict(group)
                group_dict['created_at'] = group_dict['created_at'].isoformat()
                group_dict['updated_at'] = group_dict['updated_at'].isoformat()
                processed_result.append(group_dict)

            resp =  Response.success(data=processed_result)
        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def create_group(self, user_id, grp_name, property_ids):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = f'''
                INSERT INTO groups (user_id, grp_name, property_ids)
                VALUES (%s, %s, %s)
                RETURNING grp_id
            '''
            print("query=====>", query)

            cursor.execute(query, (user_id, grp_name, property_ids))
            connection.commit()
            grp_id = cursor.fetchone()['grp_id']
            resp =  Response.created(data={"grp_id": grp_id})
        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def update_group(self, grp_id, grp_name=None, property_ids=None, gpr_status=None):
        connection = None
        cursor = None
        resp = None
        try:
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
                return Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE grp_id = %s'
            params.append(grp_id)

            cursor.execute(query, tuple(params))
            connection.commit()
            resp =  Response.success(message="Group updated successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def delete_group(self, grp_id):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM groups WHERE grp_id = %s'
            cursor.execute(query, (grp_id,))
            connection.commit()
            resp =  Response.success(message="Group deleted successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            resp = Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def add_property_to_group(self, grp_id, property_id):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE groups SET property_ids = array_append(property_ids, %s) WHERE grp_id = %s'
            cursor.execute(query, (property_id, grp_id))
            connection.commit()
            resp =  Response.success(message="Property added to group successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def update_property_for_group(self, grp_id, property_id):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            print(grp_id, property_id, "<=====property_id")
            # Update the property_ids for the group
            query = 'UPDATE groups SET property_ids = %s::integer[] WHERE grp_id = %s RETURNING property_ids'
            cursor.execute(query, (property_id, grp_id))
            connection.commit()
            
            # Fetch the updated property_ids
            updated_property_ids = cursor.fetchone()['property_ids']
            
            resp =  Response.success(data={"property_ids": updated_property_ids}, message="Property updated successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp

    def remove_property_from_group(self, grp_id, property_id):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE groups SET property_ids = array_remove(property_ids, %s) WHERE grp_id = %s'
            cursor.execute(query, (property_id, grp_id))
            connection.commit()
            resp =  Response.success(message="Property removed from group successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            resp =  Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()
            return resp