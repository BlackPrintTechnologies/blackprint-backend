from flask_restful import Resource, reqparse
from flask import request
from utils.responseUtils import Response
from module.group.controller import GroupsController  # Assuming GroupsController is in group/controller.py
from utils.commonUtil import authenticate

# Initialize GroupsController
groups_controller = GroupsController()

class Group(Resource):
    create_parser = reqparse.RequestParser()
    create_parser.add_argument('grp_name', type=str, required=True, help='Group name is required')
    create_parser.add_argument('property_ids', type=list, location='json', required=True, help='Property IDs are required')

    get_parser = reqparse.RequestParser()
    get_parser.add_argument('grp_id', type=int, required=False)

    update_parser = reqparse.RequestParser()
    update_parser.add_argument('grp_id', type=int, required=True, help='Group ID is required')
    update_parser.add_argument('grp_name', type=str, required=False)
    update_parser.add_argument('property_ids', type=list, location='json', required=False)
    update_parser.add_argument('grp_status', type=int, required=False)

    @authenticate
    def get(self, current_user, grp_id=None):
        # data = self.get_parser.parse_args()
        # grp_id = data.get('grp_id')
        response = groups_controller.get_groups(grp_id=grp_id, user_id=current_user)
        return response

    @authenticate
    def post(self, current_user):
        data = self.create_parser.parse_args()
        user_id = current_user
        grp_name = data.get('grp_name')
        property_ids = data.get('property_ids')

        response = groups_controller.create_group(
            user_id=user_id,
            grp_name=grp_name,
            property_ids=property_ids
        )
        return response

    @authenticate
    def put(self, current_user):
        data = self.update_parser.parse_args()
        grp_id = data.get('grp_id')
        grp_name = data.get('grp_name')
        property_ids = data.get('property_ids')

        response = groups_controller.update_group(
            grp_id=grp_id,
            grp_name=grp_name,
            property_ids=property_ids
        )
        return response

    @authenticate
    def delete(self, current_user):
        data = self.update_parser.parse_args()
        grp_id = data.get('grp_id')
        response = groups_controller.delete_group(grp_id=grp_id)
        return response

class GroupProperty(Resource):
    property_parser = reqparse.RequestParser()
    property_parser.add_argument('property_ids', type=list, location='json', required=True, help='Property IDs are required')
    property_parser.add_argument('grp_id', type=int, required=True, help='Group ID is required')

    @authenticate
    def post(self, current_user):
        data = self.property_parser.parse_args()
        property_id = data.get('property_ids')
        grp_id = data.get('grp_id')
        print(property_id)
        response = groups_controller.update_property_for_group(grp_id=grp_id, property_id=property_id)
        return response
    

    @authenticate
    def delete(self, current_user, grp_id):
        data = self.property_parser.parse_args()
        property_id = data.get('property_ids')

        response = groups_controller.remove_property_from_group(grp_id=grp_id, property_id=property_id)
        return response