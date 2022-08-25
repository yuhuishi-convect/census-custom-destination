from flask import Flask, request, g

from jsonrpc.backend.flask import api
from methods import *


app = Flask(__name__)
app.register_blueprint(api.as_blueprint())

# middleware to get api keys, airtable base id and table id from request urls
@app.before_request
def get_auth_info():
    g.api_key = request.args.get("api_key")
    g.base_id = request.args.get("base_id")
    g.table_id = request.args.get("table_id")

    if not g.api_key:
        return "No api_key provided", 401
    if not g.base_id:
        return "No base_id provided", 401
    if not g.table_id:
        return "No table_id provided", 401


# methods
@api.dispatcher.add_method
def echo(message):
    return {"message": message}


api.dispatcher.add_method(test_connections)
api.dispatcher.add_method(list_objects)
api.dispatcher.add_method(list_fields)
api.dispatcher.add_method(get_sync_speed)
api.dispatcher.add_method(supported_operations)
api.dispatcher.add_method(sync_batch)


if __name__ == "__main__":
    app.run(debug=True)
