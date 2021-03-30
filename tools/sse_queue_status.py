# -*- coding: utf-8 -*-
"""
This module create a SSE to push queue status of redraider cluster
Jie Li (jie.li@ttu.edu)
"""
import os
import sys
import time
import json
import logging
import flask

sys.path.append('../')

logging_path = './sse_queue_status.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

from slurmapi.fetch_slurm_queue import fetch_slurm_queue
from sharings.utils import parse_config
from slurmapi.fetch_slurm import get_slurm_token, fetch_slurm
app = flask.Flask(__name__)


@app.route('/sse_queue_status')
def stream_queue_status():
    # return "this is the data"
    def eventStream():
        while True:
            yield f'data: {fetch_slurm_queue()}\n\n'
    return flask.Response(eventStream(), mimetype="text/event-stream")

if __name__ == '__main__':
    # To test: hugo.hpcc.ttu.edu:5000/sse_queue_status
    app.run(host= '0.0.0.0', port=5000, threaded=True, debug=True)