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
import asyncio
import threading
from utils.async_utils import setup_async_environment
from utils.app_warmup import perform_startup_warmup, get_app_warmup
# Pre-initialize thread pool as early as possible
from utils.pre_init_thread_pool import pre_init_thread_pool

app = Flask(__name__)
api = Api(app)

# Setup async environment
setup_async_environment()

# Allow CORS for specific origins (localhost:3000 in this case)


CORS(app)
Compress(app)  # Enable compression
# Set custom encoder
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
from module.properties.routes import Property, PropertyDemographic, StreetViewImage, UpdateRequestInfo, RequestedProperties, UserProperty, PropertyTraffic, PropertyMarketInfo, PropertyCommercialGrowth, PropertyFilter, AdvancedMunicipalitySearch

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
api.add_resource(PropertyCommercialGrowth, '/property/commercial-growth')
api.add_resource(PropertyFilter, '/property/filter')
api.add_resource(AdvancedMunicipalitySearch, '/properties/municipality_search/')

@app.after_request
def after_request(response):
    """Add request ID to response headers"""
    response.headers['X-Request-ID'] = getattr(request, 'request_id', 'none')
    logger.info(f"Completed request {getattr(request, 'request_id', 'none')} with status {response.status_code}")
    return response

# Health check endpoint to verify warmup status
class HealthCheck(Resource):
    def get(self):
        warmup = get_app_warmup()
        status = warmup.get_warmup_status()
        
        if status['is_warmed_up']:
            return {
                'status': 'healthy',
                'message': 'Application is warmed up and ready',
                'warmup_details': status
            }, 200
        else:
            return {
                'status': 'warming_up',
                'message': 'Application is still warming up',
                'warmup_details': status
            }, 202

api.add_resource(HealthCheck, '/health')

# Cache monitoring endpoint
class CacheStats(Resource):
    def get(self):
        try:
            from utils.redis_client import get_redis_client
            redis_client = get_redis_client()
            
            if redis_client.is_connected():
                stats = redis_client.get_stats()
                health = redis_client.health_check()
                
                return {
                    'status': 'connected',
                    'redis_stats': stats,
                    'redis_health': health,
                    'cache_namespaces': [
                        'property', 'demographic', 'user', 'search', 
                        'layer', 'market_info', 'brands', 'traffic'
                    ]
                }, 200
            else:
                return {
                    'status': 'disconnected',
                    'message': 'Redis is not connected'
                }, 503
                
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Cache monitoring failed: {str(e)}'
            }, 500

api.add_resource(CacheStats, '/cache/stats')

# Startup warmup function
def startup_warmup():
    """Run warmup in background thread during startup"""
    def run_warmup():
        try:
            logger.info("🚀 Starting background warmup process...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            success = loop.run_until_complete(perform_startup_warmup())
            if success:
                logger.info("🎉 Background warmup completed successfully")
            else:
                logger.warning("⚠️ Background warmup completed with some issues")
            loop.close()
        except Exception as e:
            logger.error(f"❌ Background warmup failed: {e}")
    
    # Start warmup in background thread
    warmup_thread = threading.Thread(target=run_warmup, daemon=True)
    warmup_thread.start()
    logger.info("🔄 Warmup process started in background")

# Start warmup process during application initialization
startup_warmup()

# Log routes being added
logger.debug("API routes have been configured.")

if __name__ == '__main__':
    logger.info("Starting the Flask development server.....")
    logger.error("Logs Check for production")
    app.run(debug=True,port=5002)