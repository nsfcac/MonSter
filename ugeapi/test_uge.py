import json
import time

import requests
import multiprocessing
from itertools import repeat
from requests.exceptions import Timeout
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

all_host_points = []
node_jobs = {}

cpu_count = multiprocessing.cpu_count()

with requests.Session() as session:
    epoch_time = int(round(time.time() * 1000000000))
    host_detail = get_host_detail(config, uge_url, session, ugeapi_adapter)
    
    # Process host info
    process_host_args = zip(host_detail, repeat(epoch_time))
    with multiprocessing.Pool(processes=cpu_count) as pool:
        processed_host_detail = pool.starmap(process_host, process_host_args)

    exechosts = [item["hostname"] for item in host_detail]
    for index, host in enumerate(exechosts):
        try:
            all_host_points.extend(processed_host_detail[index]["dpoints"])
            node_jobs[host] = processed_host_detail[index]["joblist"]
        except Exception as err:
            print(err)
    # processed_host_detail = process_host(host_detail[0], epoch_time)
    print(json.dumps(node_jobs, indent=4))