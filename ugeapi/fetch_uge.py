import json
import time
import logging
import requests
import multiprocessing

from itertools import repeat
from dateutil.parser import parse

from requests.adapters import HTTPAdapter
from ugeapi.ProcessUge import ProcessUge


def fetch_uge(uge_config: dict) -> list:
    """
    fetch UGE metrics from UGE API. 
    Examples of using UGE API:
    curl http://129.118.104.35:8182/jobs | python -m json.tool
    curl http://129.118.104.35:8182/hostsummary/compute/467 | python -m json.tool
    """
    uge_metrics = {}
    try:
        api = uge_config["api"]
        # job_list_url = f"http://{api['hostname']}:{api['port']}{api['job_list']}"
        host_summary_url = f"http://{api['hostname']}:{api['port']}{api['host_summary']}"

        timestamp = int(time.time() * 1000000000)
        # Fetch UGE metrics from urls
        # job_list_url = fetch(uge_config, job_list_url)
        # print(f"Job list length: {len(job_list_url)}")

        host_summary = fetch(uge_config, host_summary_url)
        # print(f"Host summary length: {len(host_summary)}")

        # Parallel process metrics
        all_data = parallel_process(host_summary, timestamp)

        # Aggregate processed metrics
        uge_metrics = aggregate(all_data, timestamp)

        return uge_metrics
    except Exception as err:
        logging.error(f"Fetch UGE metrics error: {err}")
    

def fetch(uge_config:dict, url: str) -> list:
    """
    Use requests to query the url
    """
    metrics = []
    adapter = HTTPAdapter(max_retries=uge_config["max_retries"])
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(
                url, verify = uge_config["ssl_verify"],
                timeout = (uge_config["timeout"]["connect"], uge_config["timeout"]["read"])
            )
            metrics = response.json()
        except Exception as err:
            logging.error(f"fetch_uge : fetch error : {err}")
    return metrics


def fetch_jobs(uge_config: dict) -> list:
    """
    fetch UGE metrics from UGE API. 
    Examples of using UGE API:
    curl http://129.118.104.35:8182/jobs | python -m json.tool
    """
    job_list = []
    try:
        api = uge_config["api"]
        job_list_url = f"http://{api['hostname']}:{api['port']}{api['job_list']}"

        job_list = fetch(uge_config, job_list_url)

    except Exception as err:
        logging.error(f"fetch_jobs : fetch error : {err}")
    
    return job_list


def parallel_process(metrics: list, timestamp: int) -> list:
    """
    Parallel process metrics
    """
    all_data = []
    try:
        process_args = zip(metrics, repeat(timestamp))
        with multiprocessing.Pool() as pool:
            all_data = pool.starmap(process, process_args)
    except Exception as err:
        logging.error(f"fetch_uge : parallel_process error : {err}")
    return all_data


def process(metrics: dict, timestamp: int) -> list:
    """
    Process UGE metrics
    """
    try:
        datapoints = []
        process = ProcessUge(metrics, timestamp)
        datapoints = process.get_datapoints()
    except Exception as err:
        logging.error(f"fetch_uge : parallel_process : process error : {err}")
    return datapoints


def aggregate(all_data: dict, timestamp: int) -> list:
    """
    Aggregate datapoints, total nodes and cpu cores for each job
    """
    uge_metrics = {}
    all_datapoints = []
    all_jobspoints = []
    # all_job_list = []
    all_jobs_info = {}
    try:
        for data in all_data:
            datapoints = data["datapoints"]
            all_datapoints.extend(datapoints)
            jobs_info = data["jobs_info"]
            job_list = list(jobs_info.keys())
            # all_job_list.extend(job_list)
            for job in job_list:
                if job not in all_jobs_info:
                    all_jobs_info.update({
                        job: jobs_info[job]
                    })
                else:
                    pre_cores = all_jobs_info[job]["fields"]["CPUCores"]
                    cur_cores = jobs_info[job]["fields"]["CPUCores"]

                    pre_nodes = all_jobs_info[job]["fields"]["TotalNodes"]

                    pre_node_list = all_jobs_info[job]["fields"]["NodeList"]
                    cur_node_list = jobs_info[job]["fields"]["NodeList"]

                    all_jobs_info[job]["fields"].update({
                        "TotalNodes": pre_nodes + 1,
                        "CPUCores": pre_cores + cur_cores,
                        "NodeList": pre_node_list + cur_node_list
                    })

        # Stringify NodeList in job info
        for jobs_info in all_jobs_info.values():
            node_list = jobs_info["fields"]["NodeList"]
            jobs_info["fields"].update({
                "NodeList": str(node_list)
            })

        # all_jobspoints = list(all_jobs_info.values())

        uge_metrics.update({
            "timestamp": timestamp,
            "jobs_info": all_jobs_info,
            "datapoints": all_datapoints
        })
    except Exception as err:
        logging.error(f"fetch_uge : aggregate error : {err}")

    return uge_metrics
    