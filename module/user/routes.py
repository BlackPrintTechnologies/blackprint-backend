from flask_restful import Resource,reqparse
from flask import request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import datetime
import os
import json


# Mock database
users = {}

config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']


class Signup(Resource):
    signup_parser = reqparse.RequestParser()
    signup_parser.add_argument('username', type=str, required=True, help='Username is required')
    signup_parser.add_argument('password', type=str, required=True, help='Password is required')

    def post(self):
        data = self.signup_parser.parse_args()
        username = data.get('username')
        password = data.get('password')
        
        if username in users:
            return {'message': 'User already exists'}, 400
        
        hashed_password = generate_password_hash(password, method='sha256')
        users[username] = hashed_password
        
        return {'message': 'User created successfully'}, 201

class Signin(Resource):
    signin_parser = reqparse.RequestParser()
    signin_parser.add_argument('username', type=str, required=True, help='Username is required')
    signin_parser.add_argument('password', type=str, required=True, help='Password is required')
    def post(self):
        data = self.signin_parser.parse_args()
        username = data.get('username')
        password = data.get('password')
        
        if username not in users or not check_password_hash(users[username], password):
            return {'message': 'Invalid credentials'}, 401
        
        token = jwt.encode({
            'username': username,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }, SECRET_KEY, algorithm='HS256')
        
        return {'token': token}, 200

class ForgotPassword(Resource):
    signin_parser = reqparse.RequestParser()
    signin_parser.add_argument('username', type=str, required=True, help='Username is required')

    def post(self):

        data = self.signin_parser.parse_args()
        username = data.get('username')
        
        if username not in users:
            return {'message': 'User not found'}, 404
        
        # Here you would normally send an email with a reset link or token
        return {'message': 'Password reset link sent'}, 200
