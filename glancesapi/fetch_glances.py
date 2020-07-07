import json
import sys
sys.path.append('../')

from classes.AsyncioRequests import AsyncioRequests
from monster.helper import parse_config, parse_hostlist


def fetch_glances() -> object:
    config = parse_config('../config.yml')
    try:
        api = config["glances"]["api"]
        port = config["glances"]["port"]
        hosts = parse_hostlist(config["glances"]["hosts"])

        urls = ["http://" + host + ":" + str(port) + api for host in hosts]

        glances = AsyncioRequests()
        result = glances.requests(urls)
        print(result)
    except Exception as e:
        print(e)


fetch_glances()
