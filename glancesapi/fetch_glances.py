import json
import multiprocessing
import sys
sys.path.append('../')

from classes.AsyncioRequests import AsyncioRequests
from glancesapi.ProcessGlances import ProcessGlances
from monster.helper import parse_config, parse_nodelist


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
        nodes = parse_nodelist(config["glances"]["nodes"])

        urls = ["http://" + node + ":" + str(port) + api for node in nodes]

        # Asynchronously fetch glances metrics from all nodes
        glances = AsyncioRequests()
        node_metrics = glances.bulk_fetch(urls, nodes)

        # Process metrics and generate data points using multiprocessing
        with multiprocessing.Pool() as pool:
            datapoints = pool.map(process_metric, node_metrics)

        # Flatten the datapoints of each node
        flat_datapoints = [item for sublist in datapoints for item in sublist]

        print(json.dumps(flat_datapoints, indent = 4))

    except Exception as e:
        print(e)


def process_metric(node_metric: dict) -> list:
    process = ProcessGlances(node_metric)
    datapoints = process.get_datapoints()
    return datapoints


fetch_glances()
