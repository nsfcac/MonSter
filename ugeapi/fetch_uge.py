import json
import time
import requests
import multiprocessing
# import sys
# sys.path.append('../')

from itertools import repeat

from requests.adapters import HTTPAdapter
from ugeapi.ProcessUge import ProcessUge


def fetch_uge(uge_config: dict) -> list:
    """
    fetch UGE metrics from UGE API. 
    Examples of using UGE API:
    curl http://129.118.104.35:8182/jobs | python -m json.tool
    curl http://129.118.104.35:8182/hostsummary/compute/467 | python -m json.tool
    """
    all_datapoints = []
    try:
        api = uge_config["api"]
        job_list_url = f"http://{api['hostname']}:{api['port']}{api['job_list']}"
        host_summary_url = f"http://{api['hostname']}:{api['port']}{api['host_summary']}"

        timestamp = int(time.time()) * 1000000
        # Fetch UGE metrics from urls
        host_summary = fetch(uge_config, host_summary_url)
        # print(f"Host summary length: {len(host_summary)}")

        # Parallel process metrics
        all_data = parallel_process(host_summary, timestamp)

        # Aggregate processed metrics
        all_datapoints = aggregate(all_data)

        return all_datapoints
    except Exception as e:
        print(e)
    

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
        except Exception as e:
            print(e)
    return metrics


def parallel_process(metrics: list, timestamp: int) -> list:
    """
    Parallel process metrics
    """
    all_data = []
    process_args = zip(metrics, repeat(timestamp))
    with multiprocessing.Pool() as pool:
        all_data = pool.starmap(process, process_args)
    return all_data


def process(metrics: dict, timestamp: int) -> list:
    """
    Process UGE metrics
    """
    datapoints = []
    process = ProcessUge(metrics, timestamp)
    datapoints = process.get_datapoints()

    return datapoints


def aggregate(all_data: dict) -> list:
    """
    Aggregate datapoints, total nodes and cpu cores for each job
    """
    all_datapoints = []
    all_jobpoints = []
    all_job_info = {}
    for data in all_data:
        datapoints = data["datapoints"]
        all_datapoints.extend(datapoints)
        job_info = data["job_info"]
        job_list = list(job_info.keys())
        for job in job_list:
            if job not in all_job_info:
                all_job_info.update({
                    job: job_info[job]
                })
            else:
                pre_cores = all_job_info[job]["fields"]["CPUCores"]
                cur_cores = job_info[job]["fields"]["CPUCores"]
                pre_nodes = all_job_info[job]["fields"]["TotalNodes"]
                all_job_info[job]["fields"].update({
                    "TotalNodes": pre_nodes + 1,
                    "CPUCores": pre_cores + cur_cores
                })
    all_jobpoints = list(all_job_info.values())
    all_datapoints.extend(all_jobpoints)

    return all_datapoints
    