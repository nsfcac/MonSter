import json
import sys
sys.path.append('../../')

from glancesapi.fetch_glances import fetch_glances

glances_config = {
    'api': '/api/3/all',
    'port': 61208,
    'nodelist': ['10.10.1/4']
}


datapoints = fetch_glances(glances_config)
print(json.dumps(datapoints, indent=4))