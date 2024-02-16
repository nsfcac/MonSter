#!/usr/bin/env python3
import os
import sys
import connexion

from flask_cors import CORS

cur_dir = os.path.dirname(__file__)
openapi_server_dir = os.path.join(cur_dir, 'mbuilder_server')
specification_dir  = os.path.join(openapi_server_dir, 'openapi_server', 'openapi')
sys.path.append(openapi_server_dir)

from openapi_server import encoder

app = connexion.App(__name__, specification_dir=specification_dir)
app.app.json_encoder = encoder.JSONEncoder
app.add_api('openapi.yaml',
            arguments={'title': 'MetricsBuilder API'},
            pythonic_params=True)

CORS(app.app)
# crt = os.environ['FLASKCRT']
# key = os.environ['FLASKKEY']

# if __name__ == '__main__':
#   app.run(port=5000, ssl_context=(crt, key))
