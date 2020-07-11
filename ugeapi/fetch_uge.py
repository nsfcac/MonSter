import json
import time
import requests
import multiprocessing
import sys
sys.path.append('../')

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
        all_datapoints = parallel_process(host_summary, timestamp)

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
    flat_datapoints = []
    process_args = zip(metrics, repeat(timestamp))
    with multiprocessing.Pool() as pool:
        datapoints = pool.starmap(process, process_args)
    # flat_datapoints = [item for sublist in datapoints for item in sublist]
    flat_datapoints = datapoints
    return flat_datapoints


def process(metrics: dict, timestamp: int) -> list:
    """
    Process UGE metrics
    """
    datapoints = []
    process = ProcessUge(metrics, timestamp)
    datapoints = process.get_datapoints()

    return datapoints

    