# -*- coding: utf-8 -*-
"""
This module calls Slurm REST API to obtain detailed accouting information about
individual jobs or job steps and nodes information.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import time
import logging
import requests
import subprocess

from requests.adapters import HTTPAdapter
from multiprocessing import Process, Queue

sys.path.append('../')

from sharings.utils import parse_config

logging_path = './fetch_slurm.log'

logging.basicConfig(
    level=logging.ERROR,
    filename= logging_path,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)

def get_slurm_token(config_slurm: dict) -> list:
    """
    Get JWT token from Slurm. This requires the public key on this node to be 
    added to the target cluster headnode.
    """
    try:
        # Setting command parameters
        slurm_rest_api_ip = config_slurm['ip']
        slurm_rest_api_port = config_slurm['port']
        slurm_rest_api_user = config_slurm['user']
        slurm_headnode = config_slurm['headnode']
        
        print("Get a new token...")
        # The command used in cli
        command = [f"ssh {slurm_headnode} 'scontrol token lifespan=3600'"]
        # Get the string from command line
        rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
        # Get token
        token = rtn_str.splitlines()[0].split('=')[1]
        timestamp = int(time.time())

        token_record = {
            'time': timestamp,
            'token': token
        }

        with open('./token.json', 'w') as f:
            json.dump(token_record, f, indent = 4)
        
        return token
    except Exception as err:
        print("Get Slurm token error!")


def fetch_slurm(config_slurm: dict, token: str, url: str) -> dict:
    """
    Fetch slurm info via Slurm REST API
    """
    metrics = []
    headers = {"X-SLURM-USER-NAME": config_slurm['user'], "X-SLURM-USER-TOKEN": token}
    adapter = HTTPAdapter(max_retries=3)
    with requests.Session() as session:
        session.mount(url, adapter)
        try:
            response = session.get(url, headers=headers)
            metrics = response.json()
        except Exception as err:
            logging.error(f"Fetch slurm metrics error: {err}")
    return metrics


def convert_str_json(fields: list, job_str: str, queue: object) -> dict:
    """
    Convert the job data in string to job data in json.
    """
    job_dict = {}
    job_data = {}
    job_str_arr = job_str.split("|")
    
    for i in range(len(fields)):
        job_data.update({
            fields[i]: job_str_arr[i]
        })
    
    job_dict = {
        job_data["JobID"]: job_data
    }

    queue.put(job_dict)


def generate_job_dict(fields: list, rtn_str_arr: list) -> dict:
    """
    Generate the job dict from string using multiprocesses.
    """
    job_dict_all = {}
    queue = Queue()
    procs = []
    for rtn_str in rtn_str_arr:
        p = Process(target=convert_str_json, args=(fields, rtn_str, queue))
        procs.append(p)
        p.start()
    
    for _ in procs:
        job_dict = queue.get()
        job_dict_all.update(job_dict)

    for p in procs:
        p.join()

    return job_dict_all


def unfold_metrics(metric_str: str, in_out: str) -> dict:
    """
    Unfold the metrics under the same metric name(such as tresusageintot, tresusageouttot)
    """
    metric_dict = {}
    for item in metric_str.split(","):
        item_pair = item.split("=")

        if item_pair[0] == "fs/disk" or item_pair[0] == "energy":
            key_name = item_pair[0] + "_" + in_out
        else:
            key_name = item_pair[0]

        metric_dict.update({
            key_name: item_pair[1]
        })

    return metric_dict


def merge_metrics(job_metircs: dict, batch_step_metrics: dict) -> dict:
    """
    Merge metrics under JobID with metrics under batch and jobstep, update the job name
    """
    merged_metrics = {}
    for key, value in batch_step_metrics.items():
        if value == "" or key == "JobName":
            merged_metrics.update({
                key: job_metircs[key]
            })
        else:
            merged_metrics.update({
                key: value
            })
    return merged_metrics


def merge_job_dict(job_dict_all: dict, job_id_raw: str, queue: object) -> dict:
    """
    Aggregate jobid with jobid.batch and jobid.step# , and unfold several metrics under the same 
    attribute, such as "tresusageintot", "tresusageouttot".
    """
    merged_data = {}
    # only keep resource statistics under batch and jobstep, discard extern
    if ".batch" in job_id_raw or "." in job_id_raw and ".extern" not in job_id_raw:
        # merge metrics
        job_id = job_id_raw.split('.')[0]
        merged_data = merge_metrics(job_dict_all[job_id], job_dict_all[job_id_raw])
        
        # Unfold metrics in treusageintot and tresusageoutot
        folded_metrics = merged_data.get("TresUsageInTot", None)
        if folded_metrics:
            unfolded_metrics = unfold_metrics(folded_metrics, "in")
            merged_data.update(unfolded_metrics)
            merged_data.pop("TresUsageInTot")
        
        folded_metrics = merged_data.get("TresUsageOutTot", None)
        if folded_metrics:
            unfolded_metrics = unfold_metrics(folded_metrics, "out")
            merged_data.update(unfolded_metrics)
            merged_data.pop("TresUsageOutTot")

        if ".batch" in job_id_raw:
            # Update the job id if it contains batch
            merged_data.update({
                "JobID": job_id
            })
        
        # Add unique ids, which is used as unique ids for the record
        merged_data.update({
            "_id": merged_data["JobID"]
        })

    queue.put(merged_data)


def aggregate_job_data(job_dict_all: dict) -> dict:
    """
    Aggregate job dict using multiprocesses.
    """
    aggregated_job_data = []
    job_id_raw_list = job_dict_all.keys()
    queue = Queue()
    procs = []
    for job_id_raw in job_id_raw_list:
        p = Process(target=merge_job_dict, args=(job_dict_all, job_id_raw, queue))
        procs.append(p)
        p.start()
    
    for _ in procs:
        job_data = queue.get()
        if job_data:
            aggregated_job_data.append(job_data)

    for p in procs:
        p.join()

    return aggregated_job_data


def get_response_properties() -> None:
    job_properties = {}
    node_properties = {}
    with open('./data/openapi.json', 'r') as f:
        openapi = json.load(f)
        job_properties = openapi['components']['schemas']['v0.0.36_job_response_properties']['properties']
        node_properties = openapi['components']['schemas']['v0.0.36_node']['properties'] 
    with open('./data/job_properties.json', 'w') as f:
        json.dump(job_properties, f, indent = 4)
    with open('./data/node_properties.json', 'w') as f:
        json.dump(node_properties, f, indent = 4)


if __name__ == '__main__':
    # Read configuration file
    config_path = '../config.yml'
    config_slurm = parse_config(config_path)['slurm_rest_api']

    # Read token file, if it is out of date, get a new token
    try:
        with open('./token.json', 'r') as f:
            token_record = json.load(f)
            time_interval = int(time.time()) - token_record['time']
            if time_interval >= 3600:
                token = get_slurm_token(config_slurm)
            else:
                token = token_record['token']
    except:
        token = get_slurm_token(config_slurm)

    jobs_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_jobs']}"
    nodes_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['slurm_nodes']}"
    # openapi_url = f"http://{config_slurm['ip']}:{config_slurm['port']}{config_slurm['openapi']}"

    jobs_metrics = fetch_slurm(config_slurm, token, jobs_url)

    # queue_status = aggregate_queue_status(jobs_metrics)

    # for status, job_list in queue_status.items():
    #     print(f"{status} : {len(job_list)}")
    # with open('./data/job_status.json', 'w') as f:
    #     json.dump(job_status, f, indent=4)
