import json
import requests
from requests.adapters import HTTPAdapter

from fetch_uge import get_exechosts, get_host_detail

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


with requests.Session() as session:
    host_detail = get_host_detail(config, uge_url, session, ugeapi_adapter, 500)
    print(len(host_detail))
    hostname = [item["hostname"] for item in host_detail]
    print(json.dumps(hostname, indent=4))