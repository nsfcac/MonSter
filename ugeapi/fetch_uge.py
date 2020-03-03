import requests
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

from ugeapi.convert import get_hostip

config = {
    "host": "129.118.104.35",
    "port": "8182",
    "user": "username",
    "password": "password",
    "timeout": [2, 6],
    "max_retries": 3,
    "ssl_verify": False
}

ugeapi_adapter = HTTPAdapter(config["max_retries"])
session = requests.Session()

def fetch_uge(config: object, session: object) -> object:
    """
    Fetch metrics from UGE api
    """
    # Get executing hosts
    exechosts_url = "http://" + config["host"] + ":" + config["port"] + "/exechosts" 
    session.mount(exechosts_url, ugeapi_adapter)
    try:
        exechosts_response = session.get(
            exechosts_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"][0], config["timeout"][1])
        )
        exehosts = [get_hostip(h) for h in exechosts_response.json() if '-' in h]
        print(exehosts)
    except ConnectionError as err:
        print(err)

fetch_uge(config, session)