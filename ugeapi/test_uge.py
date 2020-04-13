import json
import requests
from requests.adapters import HTTPAdapter


config = {
    "host": "129.118.104.35",
    "port": "8182",
    "user": "username",
    "password": "password",
    "timeout": {
        "connect": 2,
        "read": 6
    },
    "max_retries": 3,
    "ssl_verify": False
}

uge_url = "http://" + config["host"] + ":" + config["port"]
ugeapi_adapter = HTTPAdapter(config["max_retries"])

def get_exechosts(config: dict, uge_url: str, session: object, ugeapi_adapter: object) -> list:
    """
    Get executing hosts
    """
    exechosts = []
    exechosts_url = uge_url + "/exechosts" 
    session.mount(exechosts_url, ugeapi_adapter)
    try:
        exechosts_response = session.get(
            exechosts_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        exechosts = [host for host in exechosts_response.json()]
    except ConnectionError as err:
        print("get_exechosts ERROR: ", end = " ")
        print(err)
        # pass
    return exechosts

with requests.Session() as session:
    exechosts = get_exechosts(config, uge_url, session, ugeapi_adapter)
    print(json.dumps(exechosts, indent=4))