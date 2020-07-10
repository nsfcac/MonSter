import json
import time
import multiprocessing
import sys
sys.path.append('../')

import requests
from requests.adapters import HTTPAdapter


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

        # Fetch UGE metrics from urls
        host_summary = fetch(uge_config, host_summary_url)

        all_datapoints = host_summary
        
        return all_datapoints
    except Exception as e:
        print(e)
    

def fetch(uge_config:dict, url: str) -> list:
    """
    Use requests to query url
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
    