from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from decimal import Decimal

class UsersController:
    def __init__(self):
        self.db = Database()

    def get_users(self, id=None, email=None):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM bp_users where bp_status = 1 '
            if id:
                query += f' and bp_user_id = {id}'
            if email:
                query += f" and bp_email = '{email}'"

            print(query, "<==============")
            cursor.execute(query)
            result = cursor.fetchall()
            processed_result = []
            for user in result:
                user_dict = dict(user)
                user_dict['bp_created_on'] = user_dict['bp_created_on'].isoformat()
                user_dict['bp_status'] = float(user_dict['bp_status']) if isinstance(user_dict['bp_status'], Decimal) else user_dict['bp_status']
                # user_dict.pop('bp_password', None)  # Remove password if it exists
                processed_result.append(user_dict)

            return Response.success(data=processed_result)
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()

    def create_user(self, bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = f'''
                INSERT INTO bp_users (bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING bp_user_id
            '''
            print("query=====>", query)

            cursor.execute(query, (bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status))
            connection.commit()
            user_id = cursor.fetchone()['bp_user_id']
            return Response.created(data={"bp_user_id": user_id})
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()

    def update_user(self, id, bp_name=None, bp_company=None, bp_industry=None, bp_email=None, bp_password=None, bp_status=None):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'UPDATE bp_users SET '
            updates = []
            params = []

            if bp_name:
                updates.append('bp_name = %s')
                params.append(bp_name)
            if bp_company:
                updates.append('bp_company = %s')
                params.append(bp_company)
            if bp_industry:
                updates.append('bp_industry = %s')
                params.append(bp_industry)
            if bp_email:
                updates.append('bp_email = %s')
                params.append(bp_email)
            if bp_password:
                updates.append('bp_password = %s')
                params.append(bp_password)
            if bp_status:
                updates.append('bp_status = %s')
                params.append(bp_status)

            if not updates:
                return Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE bp_user_id = %s'
            params.append(id)

            cursor.execute(query, tuple(params))
            connection.commit()
            return Response.success(message="User updated successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()

    def delete_user(self, id):
        connection = None
        cursor = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM bp_users WHERE bp_user_id = %s'
            cursor.execute(query, (id,))
            connection.commit()
            return Response.success(message="User deleted successfully")
        except Exception as e:
            if connection:
                connection.rollback()
            return Response.internal_server_error(message=str(e))
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.db.disconnect()