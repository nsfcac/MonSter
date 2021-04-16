# -*- coding: utf-8 -*-
"""
This module start a queue status service
Jie Li (jie.li@ttu.edu)
"""
import os
import sys
import time
import json
import logging
import flask
from flask_cors import CORS, cross_origin

sys.path.append('../')

logging_path = './queue_status_service.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

from sharings.utils import parse_config
app = flask.Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/queue_status')
@cross_origin()
def get_queue_status():
    queue_status = []
    with open('../slurmapi/data/queue_status.json', 'r') as f:
        queue_status = json.load(f)
    return queue_status

# @app.route('/queue_status')
# def stream_queue_status():
#     # return "this is the data"
#     def eventStream():
#         while True:
#             yield f'data: {fetch_slurm_queue()}\n\n'
#     return flask.Response(eventStream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # To test: hugo.hpcc.ttu.edu:5000/queue_status
    app.run(host= '0.0.0.0', port=5000, threaded=True, debug=True)