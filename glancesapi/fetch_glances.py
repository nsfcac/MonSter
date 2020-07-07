import json

from classes.AsyncRequests import AsyncRequests
from monster.helper import parse_config, parse_hostlist


def fetch_glances(hostlist: list) -> object:
    config = parse_config('../config.yml')
    try:
        api = config["glances"]["api"]
        port = config["glances"]["port"]
        hosts = parse_hostlist(config["glances"]["hosts"])

        urls = ["http://" + host + ":" + str(port) + api for host in hosts]

        glances = AsyncRequests()
        result = glances.requests(urls)
    except Exception as e:
        print(e)


# fetch_glances()
