import json
from glancesapi.fetch_glances import *

glances_config = {
    'api': '/api/3/all',
    'port': 61208,
    'nodes': '10.10.1/4'
}


datapoints = fetch_glances(glances_config)
print(json.dumps(datapoints, indent=4))