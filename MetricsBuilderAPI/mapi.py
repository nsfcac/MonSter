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
from datetime import datetime, timedelta

# Flask application configuration
app = flask.Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

# TSDB Connection
connection = utils.init_tsdb_connection()

# Node id - node name mapping
ID_NODE_MAPPING = api_utils.get_id_node_mapping(connection)

# All metric-fqdd mapping
# METRIC_FQDD_MAPPING = api_utils.get_metric_fqdd_mapping(connection)


# @app.route('/metrics_builder', methods=['POST'])
# @cross_origin()
def metrics_builder():
    # Range
    now = datetime.now() - timedelta(minutes=1)
    # prev = now - timedelta(hours=3)
    prev = now - timedelta(minutes=20)
    start = prev.strftime(DATETIME_FORMAT)
    end = now.strftime(DATETIME_FORMAT)

    # Interval
    interval = "5m"

    # Self-defined Targets object
    targets = [
        {
            "metric": "idrac | rpmreading | FAN_1",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | rpmreading | FAN_2",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | rpmreading | FAN_3",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | rpmreading | FAN_4",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | systempowerconsumption | System Power Control",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | temperaturereading | CPU1 Temp",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | temperaturereading | CPU2 Temp",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "idrac | temperaturereading | Inlet Temp",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "slurm | memoryusage | memoryusage",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "slurm | memory_used | memory_used",
            "type": "metrics",
            "nodes": None,
        },
        {
            "metric": "slurm | cpu_load | cpu_load",
            "type": "metrics",
            "nodes": None,
        },
        {
            "type": "jobs",
        },
        {
            "type": "node_core",
        },
    ]

    request = {
        "range": {
            "from": start,
            "to": end
        },
        "interval": interval,
        "targets": targets,
    }

    results = api_utils.query_tsdb_parallel(request, ID_NODE_MAPPING, connection)
    
    # ret = jsonify(results)
    print(results)
    # return jsonify(results)


if __name__ == '__main__':
    # app.run(host= '0.0.0.0', port=5000, threaded=True, debug=False)
    metrics_builder()