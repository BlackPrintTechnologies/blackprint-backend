from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS
import logging
from logsmanager.logging_config import setup_logging

# Initialize logging
setup_logging()

# Retrieve the logger
logger = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(app)

# Allow CORS for specific origins (localhost:3000 in this case)
CORS(app)

# Log application startup
logger.info("Starting the Flask application...")

# Import your routes
from module.user.routes import Signup, Signin, ForgotPassword, UpdateUser, GetUser, UserQuestionare, VerifyUser, ResendVerification, UpdateQuestionare
from module.search.routes import SavedSearches
from module.group.routes import Group, GroupProperty
from module.layers.routes import Brands, Traffic, SearchBrands
from module.properties.routes import Property, PropertyDemographic, StreetViewImage

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
api.add_resource(Property, '/property')
api.add_resource(PropertyDemographic, '/property/demographic')
api.add_resource(StreetViewImage, '/properties/street_view_image') #act as a proxy url to serve the image

# Log routes being added
logger.debug("API routes have been configured.")

if __name__ == '__main__':
    logger.info("Starting the Flask development server.....")
    logger.error("This is an  test error message. for slack integration")
    app.run(debug=True)