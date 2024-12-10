from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS
app = Flask(__name__)
api = Api(app)
CORS(app)


CORS(app, resources={r"/*": {"origins": "*"}})


from module.user.routes import Signup, Signin, ForgotPassword, UpdateUser, GetUser, UserQuestionare
from module.search.routes import SavedSearches
from module.group.routes import Group, GroupProperty
# users 
# api.add_resource(Items, '/')
# api.add_resource(Item, '/<int:pk>')
api.add_resource(Signup, '/user/signup')
api.add_resource(Signin, '/user/signin')
api.add_resource(ForgotPassword, '/user/forgot-password')
api.add_resource(UpdateUser, '/user/updateuser')
api.add_resource(GetUser, '/user/getuser')
api.add_resource(SavedSearches, '/savesearch', '/savesearch/<int:search_id>')
api.add_resource(Group, '/group', '/group/<int:grp_id>')
api.add_resource(GroupProperty, '/groupproperty')
api.add_resource(UserQuestionare, '/user/questionare/', '/user/questionare/<int:id>')


if __name__ == '__main__':
    app.run(debug=True)
