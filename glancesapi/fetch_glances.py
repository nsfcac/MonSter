import json
import sys
sys.path.append('../')

from classes.AsyncioRequests import AsyncioRequests
from glancesapi.ProcessGlances import ProcessGlances
from monster.helper import parse_config, parse_hostlist


def fetch_glances() -> object:
    config = parse_config('../config.yml')
    try:
        api = config["glances"]["api"]
        port = config["glances"]["port"]
        hosts = parse_hostlist(config["glances"]["hosts"])

        urls = ["http://" + host + ":" + str(port) + api for host in hosts]

        glances = AsyncioRequests()
        metrics = glances.bulk_fetch(urls)

        print(metrics[0])
        process = ProcessGlances(metrics[0], hosts[0])
        datapoints = process.get_datapoints()
        print(datapoints)

    except Exception as e:
        print(e)


fetch_glances()
