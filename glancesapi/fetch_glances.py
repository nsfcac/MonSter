import json
import multiprocessing
import sys
sys.path.append('../')

from sharings.AsyncioRequests import AsyncioRequests
from glancesapi.ProcessGlances import ProcessGlances
from monster.utils import parse_nodelist


def fetch_glances(glances_config: dict) -> list:
    """
    fetch OS metrics from glances API. 
    Examples of using glances API:
    curl http://10.10.1.4:61208/api/3/pluginslist | python -m json.tool
    curl http://10.10.1.4:61208/api/3/percpu | python -m json.tool
    """
    all_datapoints = []
    try:
        api = glances_config["api"]
        port = glances_config["port"]
        nodes = parse_nodelist(glances_config["nodelist"])

        # Generate glances API urls for the specified nodes
        urls = ["http://" + node + ":" + str(port) + api for node in nodes]

        # Asynchronously fetch glances metrics from all nodes
        glances = AsyncioRequests()
        node_metrics = glances.bulk_fetch(urls, nodes)

        # Process metrics and generate data points using multiprocessing
        with multiprocessing.Pool() as pool:
            datapoints = pool.map(process_metric, node_metrics)

        # Flatten the datapoints
        all_datapoints = [item for sublist in datapoints for item in sublist]

        return all_datapoints

    except Exception as e:
        print(e)


def process_metric(node_metric: dict) -> list:
    process = ProcessGlances(node_metric)
    datapoints = process.get_datapoints()
    return datapoints