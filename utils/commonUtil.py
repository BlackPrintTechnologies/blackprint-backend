from functools import wraps
from flask import request, jsonify
import jwt
import json
from utils.responseUtils import Response


# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']

def authenticate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]

        if not token:
            return Response.unauthorized(message="Token is missing!")

        try:
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            current_user = data['id']
        except jwt.ExpiredSignatureError:
            return Response.unauthorized(message="Token has expired!")
        except jwt.InvalidTokenError:
            return Response.unauthorized(message="Invalid token!")

        kwargs['current_user'] = current_user
        print(args, kwargs)
        return f(*args, **kwargs)
    return decorated_function