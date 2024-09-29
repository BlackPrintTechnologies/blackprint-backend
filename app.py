from flask import Flask
from flask_restful import Resource, Api
app = Flask(__name__)
api = Api(app)
from flask_cors import CORS
CORS(app)


from module.user.routes import Signup, Signin, ForgotPassword, UpdateUser
# users 
# api.add_resource(Items, '/')
# api.add_resource(Item, '/<int:pk>')
api.add_resource(Signup, '/user/signup')
api.add_resource(Signin, '/user/signin')
api.add_resource(ForgotPassword, '/user/forgot-password')
api.add_resource(UpdateUser, '/user/updateuser')


if __name__ == '__main__':
    app.run(debug=True)

 