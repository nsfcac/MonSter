import json
import multiprocessing
import sys
sys.path.append('../')

from classes.AsyncioRequests import AsyncioRequests
from glancesapi.ProcessGlances import ProcessGlances
from monster.helper import parse_config, parse_nodelist


def fetch_glances(glances_config: dict) -> object:
    """
    fetch OS metrics from glances API. 
    Examples of using glances API:
    curl http://10.10.1.4:61208/api/3/pluginslist | python -m json.tool
    curl http://10.10.1.4:61208/api/3/percpu | python -m json.tool
    """
    try:
        api = glances_config["api"]
        port = glances_config["port"]
        nodes = parse_nodelist(glances_config["nodes"])

        # Generate glances API urls for the specified nodes
        urls = ["http://" + node + ":" + str(port) + api for node in nodes]

        # Asynchronously fetch glances metrics from all nodes
        glances = AsyncioRequests()
        node_metrics = glances.bulk_fetch(urls, nodes)

        # Process metrics and generate data points using multiprocessing
        with multiprocessing.Pool() as pool:
            datapoints = pool.map(process_metric, node_metrics)

        # Flatten the datapoints
        flat_datapoints = [item for sublist in datapoints for item in sublist]

        return flat_datapoints

    except Exception as e:
        print(e)


def process_metric(node_metric: dict) -> list:
    process = ProcessGlances(node_metric)
    datapoints = process.get_datapoints()
    return datapoints


glances_config = {
    'api': '/api/3/all',
    'port': 61208,
    'nodes': '10.10.1/4'
}
datapoints = fetch_glances(glances_config)
print(json.dumps(datapoints, indent=4))