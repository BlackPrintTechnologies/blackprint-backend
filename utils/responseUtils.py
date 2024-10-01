class Response:
    @staticmethod
    def success(data=None, message="Success"):
        return {
            "message": message,
            "data": data
        }, 200

    @staticmethod
    def created(data=None, message="Created"):
        return {
            "message": message,
            "data": data
        }, 201

    @staticmethod
    def bad_request(data=None, message="Bad Request"):
        return {
            "message": message,
            "data": data
        }, 400

    @staticmethod
    def unauthorized(data=None, message="Unauthorized"):
        return {
            "message": message,
            "data": data
        }, 401

    @staticmethod
    def forbidden(data=None, message="Forbidden"):
        return {
            "message": message,
            "data": data
        }, 403

    @staticmethod
    def not_found(data=None, message="Not Found"):
        return {
            "message": message,
            "data": data
        }, 404

    @staticmethod
    def internal_server_error(data=None, message="Internal Server Error"):
        print(data,message)
        return {
            "message": "Internal Server Error",
            "data": data
        }, 500

# Usage example
# response, status_code = Response.success(data={"key": "value"})
# print(response, status_code)