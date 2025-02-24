from flask_restful import Resource, reqparse
from flask import request, jsonify
from utils.responseUtils import Response
from module.properties.controller import PropertyController  # Assuming SavedSearchesController is in search_controller.py
from utils.commonUtil import authenticate



class Property(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('fid', type=str, required=True, help='fid is required', location='args')

    @authenticate
    def get(self, current_user):
        data = self.create_parser.parse_args()
        fid = data.get('fid')
        pc = PropertyController()
        print("fid=====>", fid)
        response = pc.get_properties(fid)
        return response


    