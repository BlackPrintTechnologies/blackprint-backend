from flask import Flask
from flask_restful import Resource, Api
from flask_cors import CORS
app = Flask(__name__)
api = Api(app)
CORS(app)


from module.user.routes import Signup, Signin, ForgotPassword, UpdateUser, GetUser
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
api.add_resource(SavedSearches, '/savesearch')
api.add_resource(Group, '/group')
api.add_resource(GroupProperty, '/groupproperty')


if __name__ == '__main__':
    app.run(debug=True)

 