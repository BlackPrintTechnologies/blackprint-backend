from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response



class UsersController:
    def __init__(self):
        self.db = Database()
        self.db_connection = self.db.connect()
        self.cursor = self.db_connection.cursor(cursor_factory=RealDictCursor)

    def get_users(self, id=None, email=None):
        try:
            query = 'SELECT * FROM bp_users where bp_status = 1 '
            if id:
                query += f' and bp_user_id = {id}'
            if email:
                query += f' and bp_email = {email}'
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return Response.success(data=result)
        except Exception as e:
            return Response.internal_server_error(message=str(e))

    def create_user(self, bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status):
        try:
            query = '''
                INSERT INTO bp_users (bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING bp_user_id
            '''
            self.cursor.execute(query, (bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status))
            self.db_connection.commit()
            user_id = self.cursor.fetchone()['bp_user_id']
            return Response.created(data={"bp_user_id": user_id})
        except Exception as e:
            self.db_connection.rollback()
            return Response.internal_server_error(message=str(e))

    def update_user(self, id, bp_name=None, bp_company=None, bp_industry=None, bp_email=None, bp_password=None, bp_status=None):
        try:
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

            self.cursor.execute(query, tuple(params))
            self.db_connection.commit()
            return Response.success(message="User updated successfully")
        except Exception as e:
            self.db_connection.rollback()
            return Response.internal_server_error(message=str(e))

    def delete_user(self, id):
        try:
            query = 'DELETE FROM bp_users WHERE bp_user_id = %s'
            self.cursor.execute(query, (id,))
            self.db_connection.commit()
            return Response.success(message="User deleted successfully")
        except Exception as e:
            self.db_connection.rollback()
            return Response.internal_server_error(message=str(e))