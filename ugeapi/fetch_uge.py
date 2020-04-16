import json
import time

import requests
import multiprocessing
from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

# from ugeapi.convert import get_hostip
# from ugeapi.process_uge import process_host, process_job, process_node_jobs, aggregate_node_jobs

# For test single function
from convert import get_hostip
from process_uge import process_host, aggregate_node_jobs

# config = {
#     "host": "129.118.104.35",
#     "port": "8182",
#     "user": "username",
#     "password": "password",
#     "timeout": {
#         "connect": 2,
#         "read": 6
#     },
#     "max_retries": 3,
#     "ssl_verify": False
# }

def fetch_uge(config: object) -> object:
    """
    Fetch metrics from UGE api, average query and process time is: 7.79s
    """
    uge_info = {}
    # Get cpu counts
    try:
        cpu_count = multiprocessing.cpu_count()

        uge_url = "http://" + config["host"] + ":" + config["port"]
        ugeapi_adapter = HTTPAdapter(config["max_retries"])

        jobs_info = {}
        node_jobs = {}
        job_detail = {}

        all_host_points = []
        all_job_points = []

        # start = time.time()

        with requests.Session() as session:

            epoch_time = int(round(time.time() * 1000000000))

#--------------------------------- Host info -----------------------------------
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
            
            # elapsed = float("{0:.4f}".format(time.time() - start))
            # print("Query and process time: ")
            # print(elapsed)
#---------------------------- End Job Points -----------------------------------
        uge_info = {
            "all_job_points": all_job_points,
            "all_host_points": all_host_points
        }

    except Exception as err:
        print("fetch_uge ERROR: ", end = " ")
        print(err)
        # pass
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
    except ConnectionError as err:
        print("get_host_detail ERROR: ", end = " ")
        print(err)
        # pass
    return host


# def get_job_detail(config: dict, uge_url: str, session: object, ugeapi_adapter: object, job_id: str) -> object:
#     """
#     Get job details
#     """
#     job = {}
#     job_url = uge_url + "/jobs" + "/" + job_id
#     session.mount(job_url, ugeapi_adapter)
#     try:
#         job_response = session.get(
#             job_url, verify = config["ssl_verify"], 
#             timeout = (config["timeout"]["connect"], config["timeout"]["read"])
#         )
#         job = job_response.json()
#     except ConnectionError as err:
#         print("get_job_detail ERROR: ", end = " ")
#         print(job_id, end = " ")
#         print(err)
#         pass
#     return job


# def get_exechosts(config: dict, uge_url: str, session: object, ugeapi_adapter: object) -> list:
#     """
#     Get executing hosts
#     """
#     exechosts = []
#     exechosts_url = uge_url + "/exechosts" 
#     session.mount(exechosts_url, ugeapi_adapter)
#     try:
#         exechosts_response = session.get(
#             exechosts_url, verify = config["ssl_verify"], 
#             timeout = (config["timeout"]["connect"], config["timeout"]["read"])
#         )
#         exechosts = [host for host in exechosts_response.json()]
#     except ConnectionError as err:
#         print("get_exechosts ERROR: ", end = " ")
#         print(err)
#         # pass
#     return exechosts


# def get_current_jobs(config: dict, uge_url: str, session: object, ugeapi_adapter: object) -> list:
#     """
#     Get executing jobs
#     """
#     exechosts = []
#     exechosts_url = uge_url + "/jobs" 
#     session.mount(exechosts_url, ugeapi_adapter)
#     try:
#         exechosts_response = session.get(
#             exechosts_url, verify = config["ssl_verify"], 
#             timeout = (config["timeout"]["connect"], config["timeout"]["read"])
#         )
#         exechosts = [host for host in exechosts_response.json()]
#     except ConnectionError as err:
#         print("get_current_jobs ERROR: ", end = " ")
#         print(err)
#         # pass
#     return exechosts

# fetch_uge(config)
