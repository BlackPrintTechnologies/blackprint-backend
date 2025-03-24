from utils.dbUtils import Database
from psycopg2.extras import RealDictCursor
from utils.responseUtils import Response
from decimal import Decimal
from utils.commonUtil import  get_token, get_user_id_from_token, PLATFORM_URL
from utils.emailUtils import send_email

class UsersController:
    def __init__(self):
        self.db = Database()

    def get_users(self, id=None, email=None):
        connection = None
        cursor = None
        resp = None
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
                user_dict['bp_is_onboarded'] = float(user_dict['bp_is_onboarded']) if isinstance(user_dict['bp_is_onboarded'], Decimal) else user_dict['bp_is_onboarded']
                # user_dict.pop('bp_password', None)  # Remove password if it exists
                processed_result.append(user_dict)

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

    def create_user(self, bp_name, bp_company, bp_industry, bp_email, bp_password, bp_status):
        connection = None
        cursor = None
        resp = None
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
            self.send_user_verification_email(bp_email)
            resp = Response.created(data={"bp_user_id": user_id})
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

    def update_user(self, id, bp_name=None, bp_company=None, bp_industry=None, bp_email=None, bp_password=None, bp_status=None, bp_is_onboarded=None, bp_user_verified=None):
        connection = None
        cursor = None
        resp = None
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
            if bp_status is not None:
                updates.append('bp_status = %s')
                params.append(bp_status)
            if bp_is_onboarded is not None:
                updates.append('bp_is_onboarded = %s')
                params.append(bp_is_onboarded)
            if bp_user_verified is not None :
                updates.append('bp_user_verified = %s')
                params.append(bp_user_verified)
            if not updates:
                resp =  Response.bad_request(message="No fields to update")

            query += ', '.join(updates)
            query += ' WHERE bp_user_id = %s'
            params.append(id)
            print(query, params)
            cursor.execute(query, tuple(params))
            connection.commit()
            resp =  Response.success(message="User updated successfully")
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

    def delete_user(self, id):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'DELETE FROM bp_users WHERE bp_user_id = %s'
            cursor.execute(query, (id,))
            connection.commit()
            resp =  Response.success(message="User deleted successfully")
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

    def verify_user(self, bp_email, token):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            print(bp_email,token)
            query = 'SELECT bp_email FROM bp_users WHERE bp_email = %s'
            cursor.execute(query, (bp_email,))
            result = cursor.fetchone()
            print(result)
            token_email_id = get_user_id_from_token(token)
            print(token_email_id)
            if result['bp_email'] == token_email_id:
                query = f"update bp_users set bp_user_verified=1 where bp_email = '{bp_email}'"
                cursor.execute(query)
                connection.commit()
                resp =  Response.success(message="User Verified")
            else:
                resp =  Response.not_found(message="User Verification failed")

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

    def send_user_verification_email(self, bp_email):
        # Here you would normally send an email with a verification link or token
        token = get_token(bp_email)
        url = f'{PLATFORM_URL}/verify?email=' + bp_email + '&token=' + token
        print("token=====>", url)
        message = send_email(bp_email, 'Verification Email','static/verification_email_tempate.html', {'token': url})
        return Response.success(message=message)

class UserQuestionareController: 
    def __init__(self):
        self.db = Database()

    def create_questionare(self, bp_user_id, bp_brand_name,bp_user_type, bp_category, bp_product, bp_market_segment, bp_target_audience, bp_competitor_brands, bp_complementary_brands):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Insert into bp_users_questionare
            query = '''
                INSERT INTO bp_users_questionare (bp_user_id, bp_brand_name, bp_user_type, bp_category, bp_product, bp_market_segment, bp_target_audience, bp_competitor_brands, bp_complementary_brands)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING bp_user_questionare_id
            '''
            print("query=====>", query)
            cursor.execute(query, (bp_user_id, bp_brand_name, bp_user_type, bp_category, bp_product, bp_market_segment, bp_target_audience, bp_competitor_brands, bp_complementary_brands))
            connection.commit()
            questionare_id = cursor.fetchone()['bp_user_questionare_id']
            user_controller = UsersController()
            user_controller.update_user(id=bp_user_id, bp_is_onboarded=1)
            resp = Response.created(data={"bp_user_questionare_id": questionare_id})
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

    def update_questionare(self, id=None, bp_user_id=None, bp_brand_name=None, bp_user_type=None, bp_category=None, bp_product=None, bp_market_segment=None, bp_target_audience=None, bp_competitor_brands=None, bp_complementary_brands=None):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            # Build the update query dynamically based on provided parameters
            query = 'UPDATE bp_users_questionare SET '
            updates = []
            params = []

            if bp_user_id is not None:
                updates.append('bp_user_id = %s')
                params.append(bp_user_id)
            if bp_user_type is not None:
                updates.append('bp_user_type = %s')
                params.append(bp_user_type)
            if bp_brand_name is not None:
                updates.append('bp_brand_name = %s')
                params.append(bp_brand_name)
            if bp_category is not None:
                updates.append('bp_category = %s')
                params.append(bp_category)
            if bp_product is not None:
                updates.append('bp_product = %s')
                params.append(bp_product)
            if bp_market_segment is not None:
                updates.append('bp_market_segment = %s')
                params.append(bp_market_segment)
            if bp_target_audience is not None:
                updates.append('bp_target_audience = %s')
                params.append(bp_target_audience)
            if bp_competitor_brands is not None:
                updates.append('bp_competitor_brands = %s')
                params.append(bp_competitor_brands)
            if bp_complementary_brands is not None:
                updates.append('bp_complementary_brands = %s')
                params.append(bp_complementary_brands)

            if not updates:
                resp = Response.bad_request(message="No fields to update")
            else  :
                query += ', '.join(updates)
                query += ' WHERE bp_user_id= %s'
                params.append(bp_user_id)
                cursor.execute(query, tuple(params))
                connection.commit()
                resp =  Response.success(message="Questionare updated successfully")
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

    def get_questionare(self, id=None, bp_user_id=None):
        connection = None
        cursor = None
        resp = None
        try:
            connection = self.db.connect()
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = 'SELECT * FROM bp_users_questionare WHERE 1=1'
            params = []

            if id is not None:
                query += ' AND bp_user_questionare_id = %s'
                params.append(id)
            if bp_user_id is not None:
                query += ' AND bp_user_id = %s'
                params.append(bp_user_id)

            cursor.execute(query, tuple(params))
            result = cursor.fetchall()
            
            if not result:
                resp =  Response.not_found(message="Questionare not found")
            
            resp =  Response.success(data=result)
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