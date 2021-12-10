import utils
import api_utils
import json
import flask

from flask import request, jsonify
from flask_cors import CORS, cross_origin

# Flask application configuration
app = flask.Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# TSDB Connection
connection = utils.init_tsdb_connection()

# Node id - node name mapping
ID_NODE_MAPPING = api_utils.get_id_node_mapping(connection)

# All metric-fqdd mapping
METRIC_FQDD_MAPPING = api_utils.get_metric_fqdd_mapping(connection)


@app.route('/', methods=['GET'])
@cross_origin()
def index():
    print(request.headers, request.get_json(silent=True))
    return 'Success'


@app.route('/search', methods=['POST'])
@cross_origin()
def search():
    metric_fqdd_tree = api_utils.get_metric_fqdd_tree(METRIC_FQDD_MAPPING)
    return jsonify(metric_fqdd_tree)


@app.route('/query', methods=['POST'])
@cross_origin()
def query():
    results = api_utils.query_tsdb(request, ID_NODE_MAPPING, connection)
    return jsonify(results)


if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5001, threaded=True, debug=True)