import json
import time

import requests
import multiprocessing
from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

from convert import get_hostip
from fetch_uge import get_host_detail
from process_uge import process_host, aggregate_node_jobs

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
    
    # current_jobs = get_current_jobs(config, uge_url, session, ugeapi_adapter)
    # Process host info
    process_host_args = zip(host_detail, repeat(epoch_time))
    with multiprocessing.Pool(processes=cpu_count) as pool:
        processed_host_detail = pool.starmap(process_host, process_host_args)

    # exechosts = [item["hostname"] for item in host_detail]

    # # print(json.dumps(exechosts, indent=4))

    # for index, host in enumerate(exechosts):
    #     host_ip = get_hostip(host)
    #     if processed_host_detail[index]["data_points"]:
    #         all_host_points.extend(processed_host_detail[index]["data_points"])
    #     if processed_host_detail[index]["jobs_detail"]:
    #         node_jobs[host_ip] = processed_host_detail[index]["jobs_detail"]

    # all_job_points = aggregate_node_jobs(node_jobs)
    # processed_host_detail = process_host(host_detail[0], epoch_time)
    # for k, v in node_jobs.items():
    #     if not v:
    #         print("Empty Job List")
    # print("End")
    print(json.dumps(processed_host_detail, indent=4))
    # print(json.dumps(node_jobs, indent=4))
    # print(json.dumps(all_job_points, indent=4))