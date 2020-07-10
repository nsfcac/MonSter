import json
import time

import requests
import multiprocessing
import logging 

from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

from ugeapi.get_hostip import get_hostip
from ugeapi.process_uge import process_host, aggregate_node_jobs


def fetch_uge(config: object) -> object:
    """
    Fetch metrics from UGE api, average query and process time is: 1.4088s
    """
    uge_info = {}
    # Get cpu counts
    try:
        cpu_count = multiprocessing.cpu_count()

        uge_url = "http://" + config["host"] + ":" + config["port"]
        ugeapi_adapter = HTTPAdapter(max_retries=config["max_retries"])

        node_jobs = {}
        all_host_points = []
        all_job_points = []

        with requests.Session() as session:

            epoch_time = int(round(time.time() * 1000000000))

            # Get hosts detail
            host_detail = get_host_detail(config, uge_url, session, ugeapi_adapter)

            # Process hosts info
            process_host_args = zip(host_detail, repeat(epoch_time))
            with multiprocessing.Pool(processes=cpu_count) as pool:
                processed_host_detail = pool.starmap(process_host, process_host_args)

            exechosts = [item["hostname"] for item in host_detail]
            for index, host in enumerate(exechosts):
                host_ip = get_hostip(host)
                if processed_host_detail[index]["data_points"]:
                    all_host_points.extend(processed_host_detail[index]["data_points"])
                if processed_host_detail[index]["jobs_detail"]:
                    node_jobs[host_ip] = processed_host_detail[index]["jobs_detail"]

            # Process jobs info
            all_job_points = aggregate_node_jobs(node_jobs)

        uge_info = {
            "all_job_points": all_job_points,
            "all_host_points": all_host_points,
            "epoch_time": epoch_time
        }

    except:
        logging.error("Cannot get UGE data points")

    return uge_info


def get_host_detail(config: dict, uge_url: str, session: object, ugeapi_adapter: object) -> list:
    """
    Get host details
    """
    host = None
    host_url = uge_url + "/hostsummary/" + "compute/" + str(config["computing_hosts"])
    session.mount(host_url, ugeapi_adapter)
    try:
        host_response = session.get(
            host_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        host = host_response.json()
    except:
        logging.error("Cannot get host details from UGE")

    return host