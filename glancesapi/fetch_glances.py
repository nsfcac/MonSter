import json
import asyncio
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

        loop = asyncio.get_event_loop()
        glances = AsyncioRequests()
        result = loop.run_until_complete(glances.request(urls))
        
        print(result)
    except Exception as e:
        print(e)


fetch_glances()
