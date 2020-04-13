import json
import time
import requests
from requests.adapters import HTTPAdapter

from fetch_uge import get_host_detail
from process_uge import process_host

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
    "ssl_verify": False,
    "computing_hosts": 467
}

uge_url = "http://" + config["host"] + ":" + config["port"]
ugeapi_adapter = HTTPAdapter(config["max_retries"])


with requests.Session() as session:
    epoch_time = int(round(time.time() * 1000000000))
    host_detail = get_host_detail(config, uge_url, session, ugeapi_adapter)
    exechosts = [item["hostname"] for item in host_detail]
    print(json.dumps(exechosts, indent=4))
    # processed_host_detail = process_host(host_detail[0], epoch_time)
    # print(json.dumps(processed_host_detail, indent=4))