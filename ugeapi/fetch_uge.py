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
from process_uge import process_host, process_job, process_node_jobs, aggregate_node_jobs

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

            # Process host info
            process_host_args = zip(host_detail, repeat(epoch_time))
            with multiprocessing.Pool(processes=cpu_count) as pool:
                processed_host_detail = pool.starmap(process_host, process_host_args)

            exechosts = [item["hostname"] for item in host_detail]
            for index, host in enumerate(exechosts):
                try:
                    # Add processed data points to the list
                    all_host_points.extend(processed_host_detail[index]["dpoints"])
                    node_jobs[host] = processed_host_detail[index]["joblist"]
                except Exception as err:
                    print(err)
#----------------------------- End Host Points ---------------------------------

#-------------------------------- Job Points -----------------------------------
            process_node_jobs_args = zip(exechosts, repeat(node_jobs))
            with multiprocessing.Pool(processes=cpu_count) as pool:
                processed_node_jobs = pool.starmap(process_node_jobs, process_node_jobs_args)
            
            aggregated_node_jobs = aggregate_node_jobs(processed_node_jobs)

            jobs = list(aggregated_node_jobs.keys())

            # Get jobs detail in parallel
            pool_job_args = zip(repeat(config), repeat(uge_url), repeat(session), repeat(ugeapi_adapter), jobs)
            with multiprocessing.Pool(processes=cpu_count) as pool:
                job_data = pool.starmap(get_job_detail, pool_job_args)
            
            for index, job in enumerate(jobs):
                jobs_info[job] = job_data[index]

            # Process job info (job_id:str, jobs_info: object, time: int)
            process_job_args = zip(jobs, repeat(jobs_info), repeat(epoch_time))
            with multiprocessing.Pool(processes=cpu_count) as pool:
                processed_job_info = pool.starmap(process_job, process_job_args)

            for index, job in enumerate(jobs):
                job_detail[job] = processed_job_info[index]

            for job in jobs:
                if job_detail[job]:
                    job_detail[job]["fields"].update({
                        "totalnodes": aggregated_node_jobs[job]["totalnodes"],
                        "nodelist": str(aggregated_node_jobs[job]["nodelist"]),
                        "cpucores": aggregated_node_jobs[job]["cpucores"]
                    })
                    all_job_points.append(job_detail[job])
            
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


def get_current_jobs(config: dict, uge_url: str, session: object, ugeapi_adapter: object) -> list:
    """
    Get executing jobs
    """
    exechosts = []
    exechosts_url = uge_url + "/jobs" 
    session.mount(exechosts_url, ugeapi_adapter)
    try:
        exechosts_response = session.get(
            exechosts_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        exechosts = [host for host in exechosts_response.json()]
    except ConnectionError as err:
        print("get_current_jobs ERROR: ", end = " ")
        print(err)
        # pass
    return exechosts



def get_host_detail(config: dict, uge_url: str, session: object, ugeapi_adapter: object) -> list:
    """
    Get host details
    """
    host = None
    # host_url = uge_url + "/hostsummary" + "/" + host_id
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


def get_job_detail(config: dict, uge_url: str, session: object, ugeapi_adapter: object, job_id: str) -> object:
    """
    Get job details
    """
    job = {}
    job_url = uge_url + "/jobs" + "/" + job_id
    session.mount(job_url, ugeapi_adapter)
    try:
        job_response = session.get(
            job_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        job = job_response.json()
    except ConnectionError as err:
        print("get_job_detail ERROR: ", end = " ")
        print(job_id, end = " ")
        print(err)
        pass
    return job

# fetch_uge(config)
