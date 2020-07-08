import json
import sys
sys.path.append('../')

from classes.AsyncioRequests import AsyncioRequests
from glancesapi.ProcessGlances import ProcessGlances
from monster.helper import parse_config, parse_hostlist


def fetch_glances() -> object:
    """
    fetch OS metrics from glances API. 
    Examples of using glances API:
    curl http://10.10.1.4:61208/api/3/pluginslist | python -m json.tool
    curl http://10.10.1.4:61208/api/3/percpu | python -m json.tool
    """
    config = parse_config('../config.yml')
    try:
        api = config["glances"]["api"]
        port = config["glances"]["port"]
        hosts = parse_hostlist(config["glances"]["hosts"])

        urls = ["http://" + host + ":" + str(port) + api for host in hosts]

        # Asynchronously fetch glances metrics from all nodes
        glances = AsyncioRequests()
        metrics = glances.bulk_fetch(urls)

        # Process metrics and generate data points
        process = ProcessGlances(metrics[0], hosts[0])
        datapoints = process.get_datapoints()
        print(json.dumps(datapoints, indent = 4))

    except Exception as e:
        print(e)


fetch_glances()
