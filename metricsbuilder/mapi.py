"""
MIT License

Copyright (c) 2022 Texas Tech University

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

"""
This file is part of MonSter.

Author:
    Jie Li, jie.li@ttu.edu
"""

import sys
sys.path.insert(0, '../monster')

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
    results = api_utils.query_tsdb_parallel(request, ID_NODE_MAPPING, connection)
    return jsonify(results)


if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5001, threaded=True, debug=False)