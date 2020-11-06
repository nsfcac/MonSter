# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting infieldsion about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Get all jobs that have been terminated.
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State -p > sacct_raw_parse.txt
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State > sacct_raw.txt

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import subprocess
from multiprocessing import Process, Queue

sys.path.append('../')

from sharings.utils import parse_config

def fetch_slurm_job() -> list:
    # Read configuration file
    config_path = './config.yml'
    config = parse_config(config_path)

    # Setting command parameters
    accounting_fields = config["slurm"]["accounting_fields"]
    job_states = config["slurm"]["job_states"]
    start_time = config["slurm"]["start_time"]
    end_time = config["slurm"]["end_time"]
    
    # The command used in cli
    command = ["ssh monster@login.hpcc.ttu.edu " + "'sacct  --allusers --starttime " + start_time + " --endtime " + end_time + " --state " + ",".join(job_states) + " --fields=" + ",".join(accounting_fields) + " -p'"]

    # Get strings from command line
    rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
    
    # Split strings by line, and discard the first line that indicates the metrics name
    rtn_str_arr = rtn_str.splitlines()[1:]

    # Get all job data dict
    job_dict_all = generate_job_dict(accounting_fields, rtn_str_arr)

    # Aggregate job data
    aggregated_job_data = aggregate_job_data(job_dict_all)
    
    return aggregated_job_data


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


if __name__ == '__main__':
    records = fetch_slurm_job()
    print(json.dumps(records, indent=4))