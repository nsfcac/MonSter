import json
import time

import requests
import multiprocessing
from itertools import repeat
from requests.exceptions import Timeout
from requests.adapters import HTTPAdapter

from convert import get_hostip
from process_uge import process_host, process_job, process_node_jobs, aggregate_node_jobs

# config = {
#     "host": "129.118.104.35",
#     "port": "8182",
#     "user": "username",
#     "password": "password",
#     "timeout": [2, 6],
#     "max_retries": 3,
#     "ssl_verify": False,
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

        host_info = {}
        jobs_info = {}
        node_jobs = {}
        job_detail = {}

        all_host_points = []
        all_job_points = []

        start = time.time()

        with requests.Session() as session:

            # Get executing hosts running on the cluster
            exechosts = get_exechosts(config, uge_url, session, ugeapi_adapter)
            exechosts = [host for host in exechosts if '-' in host]

            epoch_time = int(round(time.time() * 1000000000))

#--------------------------------- Host info -----------------------------------
            # Get hosts detail in parallel 
            pool_host_args = zip(repeat(config), repeat(uge_url), repeat(session), 
                                 repeat(ugeapi_adapter), exechosts)
            with multiprocessing.Pool(processes=cpu_count) as pool:
                host_data = pool.starmap(get_host_detail, pool_host_args)
            
            for index, host in enumerate(exechosts):
                host_info[host] = host_data[index]

            # Process host info
            process_host_args = zip(exechosts, repeat(host_info), repeat(epoch_time))
            with multiprocessing.Pool(processes=cpu_count) as pool:
                processed_host_info = pool.starmap(process_host, process_host_args)

            for index, host in enumerate(exechosts):
                try:
                    if processed_host_info[index]:
                        all_host_points.extend(processed_host_info[index]["dpoints"])
                        node_jobs[host] = processed_host_info[index]["joblist"]
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
                        "nodelist": aggregated_node_jobs[job]["nodelist"],
                        "cpucores": aggregated_node_jobs[job]["cpucores"]
                    })
                    all_job_points.append(job_detail[job])
            
            elapsed = float("{0:.4f}".format(time.time() - start))
            print("Query and process time: ")
            print(elapsed)
#---------------------------- End Job Points -----------------------------------
        uge_info = {
            "all_job_points": all_job_points,
            "all_host_points": all_host_points
        }

        # print(json.dumps(uge_info, indent=4))
    except Exception as err:
        print(err)
    return uge_info


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
        # exechosts = [get_hostip(h) for h in exechosts_response.json() if '-' in h]
        exechosts = [host for host in exechosts_response.json()]
    except ConnectionError as err:
        print(err)
    return exechosts


def get_host_detail(config: dict, uge_url: str, session: object, ugeapi_adapter: object, host_id: str) -> object:
    """
    Get host details
    """
    host = {}
    host_url = uge_url + "/hostsummary" + "/" + host_id
    session.mount(host_url, ugeapi_adapter)
    try:
        host_response = session.get(
            host_url, verify = config["ssl_verify"], 
            timeout = (config["timeout"]["connect"], config["timeout"]["read"])
        )
        host = host_response.json()
    except ConnectionError as err:
        print(err)
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
        print(err)
    return job

# fetch_uge(config)
