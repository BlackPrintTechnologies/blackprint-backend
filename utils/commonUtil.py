from functools import wraps
from flask import request, jsonify, make_response
import jwt
import json
import datetime
from utils.responseUtils import Response


# Load configuration
config_path = 'app.json'
with open(config_path, 'r') as config_file:
    config = json.load(config_file)

# Secret key for JWT
SECRET_KEY = config['SECRET_KEY']
PLATFORM_URL = config['PLATFORM_URL']

def get_token(id):
    token = jwt.encode({
            'id': id,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }, SECRET_KEY, algorithm='HS256')
    return token

def get_user_id_from_token(token):
    data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    current_user = data['id']
    return current_user

def get_google_street(lat, lng, address=None):
    import googlemaps
    gmaps = googlemaps.Client(key=config['GOOGLE_MAP_API_KEY'])
    
    if address:
        geocode_result = gmaps.geocode(address)
        if not geocode_result:
            return None
        location = geocode_result[0]['geometry']['location']
        lat, lng = location['lat'], location['lng']
    
    reverse_geocode_result = gmaps.reverse_geocode((lat, lng))
    formatted_address = reverse_geocode_result[0]['formatted_address']
    
    street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=400x200&location={lat},{lng}&fov=80&heading=70&pitch=0&key={config['GOOGLE_MAP_API_KEY']}"
    
    return {
        'address': formatted_address,
        'street_view_url': street_view_url
    }

def authenticate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]

        if not token:
            return Response.unauthorized(message="Token is missing!")

        try:
            current_user = get_user_id_from_token(token)
        except jwt.ExpiredSignatureError:
            response = make_response(Response.unauthorized(message="Token has expired!"))
            response.delete_cookie('authToken')
            return response
        except jwt.InvalidTokenError:
            response = make_response(Response.unauthorized(message="Invalid token!"))
            response.delete_cookie('authToken')
            return response

        kwargs['current_user'] = current_user
        print(args, kwargs)
        return f(*args, **kwargs)
    return decorated_function

