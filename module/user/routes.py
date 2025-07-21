from flask_restful import Resource, reqparse
from flask import request, jsonify, make_response
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import datetime
import os
import json
from utils.responseUtils import Response  # Assuming ResponseUtil is in response_util.py
from module.user.controller import UsersController, UserQuestionareController  # Assuming UsersController is in users_controller.py
from decimal import Decimal
from utils.commonUtil import authenticate, get_token, get_user_id_from_token
from logsmanager.logging_config import setup_logging
import logging

# Initialize logging
setup_logging()

# Retrieve the logger
logger = logging.getLogger(__name__)
# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']

# Initialize UsersController
users_controller = UsersController()
user_questionare_controller = UserQuestionareController()

class User:
    @staticmethod
    def create_user(email, password, name):
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
        response = users_controller.create_user(
            bp_name=name,
            bp_company='',
            bp_industry='',
            bp_email=email,
            bp_password=hashed_password,
            bp_status=1
        )
        return response

    @staticmethod
    def validate_user(email, password):
        response = users_controller.get_users(email=email)
        print(response,"=======================>")
        if response[1] != 200:
            return None
        users = response[0]['data']
        for user in users:
            if user['bp_email'] == email and check_password_hash(user['bp_password'], password):
                return user
        return None

    @staticmethod
    def user_exists(email):
        response = users_controller.get_users()
        if response[1] != 200:
            return False
        users = response[0]['data']
        for user in users:
            if user['bp_email'] == email:
                return True
        return False

class Signup(Resource):
    signup_parser = reqparse.RequestParser()
    signup_parser.add_argument('email', type=str, required=True, help='Email is required')
    signup_parser.add_argument('password', type=str, required=True, help='Password is required')
    signup_parser.add_argument('name', type=str, required=True, help='Name is required')

    def post(self):
        logger.info("Received signup request")
        data = self.signup_parser.parse_args()
        email = data.get('email')
        password = data.get('password')
        name = data.get('name')
        logger.debug(f"Signup details: email={email}, name={name}")
        
        if User.user_exists(email):
            logger.warning(f"Signup failed - User with email {email} already exists")
            return Response.bad_request(message='User already exists')
        
        response = User.create_user(email, password, name)
        logger.info(f"User {email} created successfully")
        return response

class Signin(Resource):
    signin_parser = reqparse.RequestParser()
    signin_parser.add_argument('email', type=str, required=True, help='Email is required')
    signin_parser.add_argument('password', type=str, required=True, help='Password is required')

    def post(self):
        logger.info("Received sign-in request")
        data = self.signin_parser.parse_args()
        email = data.get('email')
        password = data.get('password')
        logger.debug(f"Sign-in attempt for email: {email}")

        user = User.validate_user(email, password)
        
        if not user:
            logger.warning(f"Sign-in failed - Invalid credentials for email: {email}")
            return Response.unauthorized(message='Invalid credentials')
        
        token = get_token(user['bp_user_id'])
        logger.info(f"User {email} signed in successfully, token generated")


        response = make_response(Response.success(data={'token': token, 'user': user}))
        response.set_cookie('authToken', token, httponly=True, secure=True, samesite='Lax')
        return response

class ForgotPassword(Resource):
    signin_parser = reqparse.RequestParser()
    signin_parser.add_argument('email', type=str, required=True, help='Email is required')

    def post(self):
        logger.info("Received password reset request")
        data = self.signin_parser.parse_args()
        email = data.get('email')
        logger.debug(f"Password reset attempt for email: {email}")
        if not User.user_exists(email):
            logger.warning(f"Password reset failed - User not found: {email}")
            return Response.not_found(message='User not found')
        
        # Here you would normally send an email with a reset link or token
        logger.info(f"Password reset link sent to email: {email}")
        return Response.success(message='Password reset link sent')


class GetUser(Resource):
    
    @authenticate
    def get(self, current_user):
        logger.info(f"user {current_user}")
        response = users_controller.get_users(
            id=current_user
        )
        return response

class UpdateUser(Resource):
    update_parser = reqparse.RequestParser()
    update_parser.add_argument('name', type=str, required=False)
    update_parser.add_argument('company', type=str, required=False)
    update_parser.add_argument('industry', type=str, required=False)
    update_parser.add_argument('email', type=str, required=False)
    update_parser.add_argument('password', type=str, required=False)
    update_parser.add_argument('status', type=int, required=False)

    @authenticate
    def put(self, user_id):
        logger.info(f"Received update request for user_id: {user_id}")
        data = self.update_parser.parse_args()
        name = data.get('name')
        company = data.get('company')
        industry = data.get('industry')
        email = data.get('email')
        password = data.get('password')
        status = data.get('status')
        logger.debug(f"Update data for user_id {user_id}: {data}")
        if password:
            logger.info(f"Updating password for user_id: {user_id}")
            password = generate_password_hash(password, method='sha256')

        response = users_controller.update_user(
            id=user_id,
            bp_name=name,
            bp_company=company,
            bp_industry=industry,
            bp_email=email,
            bp_password=password,
            bp_status=status
        )
        return response

class VerifyUser(Resource):
    verify_parser = reqparse.RequestParser()
    verify_parser.add_argument('email', type=str, required=True, help='Email is required')
    verify_parser.add_argument('token', type=str, required=True, help='Token is required')

    def post(self):
        logger.info("Received user verification request")
        data = self.verify_parser.parse_args()
        email = data.get('email')
        token = data.get('token')
        logger.debug(f"Verifying user with email: {email} and token: {token}")
        return users_controller.verify_user(bp_email=email, token=token) 

class ResendVerification(Resource):
    resend_parser = reqparse.RequestParser()
    resend_parser.add_argument('email', type=str, required=True, help='Email is required')

    def post(self):
        logger.info("Received request to resend verification email")
        data = self.resend_parser.parse_args()
        email = data.get('email')
        logger.debug(f"Attempting to resend verification email to: {email}")

        return users_controller.send_user_verification_email(bp_email=email)


class UserQuestionare(Resource):
    user_questionare_parser = reqparse.RequestParser()
    user_questionare_parser.add_argument('bp_brand_name', type=str, required=True, help='Brand name is required')
    user_questionare_parser.add_argument('bp_user_type', type=str, required=True, help='bp_user_type is required')
    user_questionare_parser.add_argument('bp_category', type=str, required=True, help='Category is required')
    user_questionare_parser.add_argument('bp_product', type=str, required=True, help='Product is required')
    user_questionare_parser.add_argument('bp_market_segment', type=str, required=True, help='Market segment is required')
    user_questionare_parser.add_argument('bp_target_audience', type=str, required=True, help='Target audience is required')
    user_questionare_parser.add_argument('bp_competitor_brands', type=list, location='json', required=True, help='Competitor brands are required')
    user_questionare_parser.add_argument('bp_complementary_brands', type=list, location='json', required=True, help='Complementary brands are required')
    user_questionare_parser.add_argument('bp_company_role', type=str, required=True, help='Company role is required')
    user_questionare_parser.add_argument('bp_full_name', type=str, required=True, help='Full name is required')
    user_questionare_parser.add_argument('bp_phone_number', type=str, required=False, help='Phone number is optional')

    update_parser = reqparse.RequestParser()
    update_parser.add_argument('bp_user_questionare_id', type=int, required=False)
    update_parser.add_argument('bp_brand_name', type=str, required=False)
    update_parser.add_argument('bp_user_type', type=str, required=False)
    update_parser.add_argument('bp_category', type=str, required=False)
    update_parser.add_argument('bp_product', type=str, required=False)
    update_parser.add_argument('bp_market_segment', type=str, required=False)
    update_parser.add_argument('bp_target_audience', type=str, required=False)
    update_parser.add_argument('bp_competitor_brands', type=list, location='json', required=False)
    update_parser.add_argument('bp_complementary_brands', type=list, location='json', required=False)
    update_parser.add_argument('bp_company_role', type=str, required=True, help='Company role is required')
    update_parser.add_argument('bp_full_name', type=str, required=True, help='Full name is required')
    update_parser.add_argument('bp_phone_number', type=str, required=False, help='Phone number is optional')

    @authenticate
    def post(self, current_user):
        logger.info(f"Received request to create questionnaire for user {current_user}")
        data = self.user_questionare_parser.parse_args()
        logger.debug(f"Parsed request data: {data}")

        response = user_questionare_controller.create_questionare(
            bp_user_id=current_user,
            bp_brand_name=data['bp_brand_name'],
            bp_user_type=data['bp_user_type'],
            bp_category=data['bp_category'],
            bp_product=data['bp_product'],
            bp_market_segment=data['bp_market_segment'],
            bp_target_audience=data['bp_target_audience'],
            bp_competitor_brands=data['bp_competitor_brands'],
            bp_complementary_brands=data['bp_complementary_brands'],
            bp_full_name= data['bp_full_name'],
            bp_company_role=data['bp_company_role'],
            bp_phone_number=data.get('bp_phone_number', None)  # Optional field
        )
        return response

    @authenticate
    def put(self, current_user):
        logger.info(f"Received request to update questionnaire for user {current_user}")
        data = self.update_parser.parse_args()
        logger.debug(f"Parsed update data: {data}")

        response = user_questionare_controller.update_questionare(
            bp_user_id=current_user,
            bp_brand_name=data.get('bp_brand_name'),
            bp_user_type=data.get('bp_user_type'),
            bp_category=data.get('bp_category'),
            bp_product=data.get('bp_product'),
            bp_market_segment=data.get('bp_market_segment'),
            bp_target_audience=data.get('bp_target_audience'),
            bp_competitor_brands=data.get('bp_competitor_brands'),
            bp_complementary_brands=data.get('bp_complementary_brands'),
            bp_full_name=data.get('bp_full_name'),
            bp_company_role=data.get('bp_company_role'),
            bp_phone_number=data.get('bp_phone_number', None)  # Optional field
        )
        return response

    @authenticate
    def get(self, current_user, id=None):
        logger.info(f"Recevied GET request for user {current_user}")
        response = user_questionare_controller.get_questionare(id=id, bp_user_id=current_user)
        return response


class UpdateQuestionare(Resource):
    update_parser = reqparse.RequestParser()
    update_parser.add_argument('bp_brand_name', type=str, required=False)
    update_parser.add_argument('bp_category', type=str, required=False)
    update_parser.add_argument('bp_product', type=str, required=False)
    update_parser.add_argument('bp_market_segment', type=str, required=False)
    update_parser.add_argument('bp_target_audience', type=str, required=False)
    update_parser.add_argument('bp_competitor_brands', type=list, location='json', required=False)
    update_parser.add_argument('bp_complementary_brands', type=list, location='json', required=False)

    @authenticate
    def post(self, current_user):
        logger.info(f"Received request to update questionnaire for user {current_user}")
        data = self.update_parser.parse_args()
        logger.debug(f"Parsed update data: {data}")
        response = user_questionare_controller.update_questionare(
            bp_user_id=current_user,
            bp_brand_name=data['bp_brand_name'],
            bp_category=data['bp_category'],
            bp_product=data['bp_product'],
            bp_market_segment=data['bp_market_segment'],
            bp_target_audience=data['bp_target_audience'],
            bp_competitor_brands=data['bp_competitor_brands'],
            bp_complementary_brands=data['bp_complementary_brands']
        )
        return response