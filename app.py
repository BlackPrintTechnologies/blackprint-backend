from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS
import logging
from logsmanager.logging_config import setup_logging
from flask import Flask, g, request 
from flask_compress import Compress
from decimal import Decimal
import uuid
import json
app = Flask(__name__)
api = Api(app)

# Allow CORS for specific origins (localhost:3000 in this case)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)
    

CORS(app)
Compress(app)  # Enable compression
app.json_encoder = DecimalEncoder  # Set custom encoder
app.config['COMPRESS_ALGORITHM'] = 'gzip'
app.config['COMPRESS_LEVEL'] = 1  # 1 (fastest) to 9 (best compression)
app.config['COMPRESS_MIMETYPES'] = ['text/html', 'application/json']

@app.before_request
def before_request():
    """Generate or get request ID for each incoming request"""
    request.request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    g.request_id = request.request_id  # Make available in Flask context
    logger.info(f"Starting request {request.request_id}")
# Initialize logging

setup_logging()

# Retrieve the logger
logger = logging.getLogger(__name__)


# Log application startup
logger.info("Starting the Flask application...")

# Import your routes
from module.user.routes import Signup, Signin, ForgotPassword, UpdateUser, GetUser, UserQuestionare, VerifyUser, ResendVerification, UpdateQuestionare
from module.search.routes import SavedSearches
from module.group.routes import Group, GroupProperty
from module.layers.routes import Brands, Traffic, SearchBrands, PropertyLayer
from module.properties.routes import Property, PropertyDemographic, StreetViewImage, UpdateRequestInfo, RequestedProperties, UserProperty, PropertyTraffic, PropertyMarketInfo

# Define API routes
api.add_resource(Signup, '/user/signup')
api.add_resource(Signin, '/user/signin')
api.add_resource(ForgotPassword, '/user/forgot-password')
api.add_resource(UpdateUser, '/user/updateuser')
api.add_resource(GetUser, '/user/getuser')
api.add_resource(SavedSearches, '/savesearch', '/savesearch/<int:search_id>')
api.add_resource(Group, '/group', '/group/<int:grp_id>')
api.add_resource(GroupProperty, '/groupproperty')
api.add_resource(UserQuestionare, '/user/questionare/', '/user/questionare/<int:id>')
api.add_resource(UpdateQuestionare, '/user/updatequestionare/')
api.add_resource(VerifyUser, '/user/verify')  # Missing '/' added
api.add_resource(ResendVerification, '/user/resend-verification')
api.add_resource(Brands, '/brands')
api.add_resource(SearchBrands, '/searchbrands/')
api.add_resource(Traffic, '/traffic')
# property related routes
api.add_resource(Property, '/property')
api.add_resource(UserProperty, '/property/userproperty')
api.add_resource(PropertyLayer, '/property/layer')
api.add_resource(PropertyDemographic, '/property/demographic')
api.add_resource(UpdateRequestInfo, '/property/requestinfo')
api.add_resource(StreetViewImage, '/properties/street_view_image') #act as a proxy url to serve the image
api.add_resource(RequestedProperties, '/property/requested')  
api.add_resource(PropertyTraffic, '/property/traffic')
api.add_resource(PropertyMarketInfo, '/property/marketinfo')  # Catchment and fid as parameters

@app.after_request
def after_request(response):
    """Add request ID to response headers"""
    response.headers['X-Request-ID'] = getattr(request, 'request_id', 'none')
    logger.info(f"Completed request {getattr(request, 'request_id', 'none')} with status {response.status_code}")
    return response

# Log routes being added
logger.debug("API routes have been configured.")

if __name__ == '__main__':
    logger.info("Starting the Flask development server.....")
    logger.error("Logs Check for production")
    app.run(debug=True,port=5002)