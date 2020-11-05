# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting infieldsion about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Job State:
https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES

Get all jobs that have been terminated.
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem -p > sacct_raw_parse.txt
sacct --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=partition,nodelist,group,user,jobname,jobid,submit,start,end,exitcode,cputimeraw,tresusageintot,tresusageouttot,maxvmsize,alloccpus,ntasks,cluster,timelimitraw,reqmem,State

Jie Li (jie.li@ttu.edu)
"""
import sys
import json
import subprocess
from multiprocessing import Process, Queue

sys.path.append('../')

from sharings.utils import parse_config

def main():
    # Read configuration file
    config_path = './config.yml'
    config = parse_config(config_path)

    # Job data fields, should be configurable
    accounting_fields = config["slurm"]["accounting_fields"]
    
    # The command used in command line
    command = ["sacct  --allusers --starttime midnight --endtime now --state BOOT_FAIL,CANCELLED,COMPLETED,DEADLINE,FAILED,NODE_FAIL,OUT_OF_MEMORY,PREEMPTED,TIMEOUT --fields=" + ",".join(accounting_fields) + " -p"]

    # Get strings from command line
    rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
    
    # Split strings by line, and discard the first line that indicates the metrics name
    rtn_str_arr = rtn_str.splitlines()[1:]

    # # Get all job data dict
    job_dict_all = process_job_dict(accounting_fields, rtn_str_arr)

    # Aggregate job data
    aggregated_job_dict = aggregate_job_dict(job_dict_all)
    print(json.dumps(aggregated_job_dict, indent=4))
    
    return


def str_2_json(fields: list, job_str: str, queue: object) -> dict:
    """
    Process the job string, and generate job data corresponding to job id in json format.
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
    # return job_dict


def process_job_dict(fields: list, rtn_str_arr: list) -> dict:
    """
    Process the job string using multiprocesses.
    """
    job_dict_all = {}
    queue = Queue()
    procs = []
    for rtn_str in rtn_str_arr:
        p = Process(target=str_2_json, args=(fields, rtn_str, queue))
        procs.append(p)
        p.start()
    
    for _ in procs:
        job_dict = queue.get()
        job_dict_all.update(job_dict)

    for p in procs:
        p.join()

    return job_dict_all


def unfold(metric_str: str, type: str) -> dict:
    """
    Unfold the metrics under the same metric name(such as tresusageintot, tresusageouttot)
    """
    metric_dict = {}
    for item in metric_str.split(","):
        item_pair = item.split("=")

        if item_pair[0] == "fs/disk" or item_pair[0] == "energy":
            key_name = item_pair[0] + "_" + type
        else:
            key_name = item_pair[0]

        metric_dict.update({
            key_name: item_pair[1]
        })

    return metric_dict


def merge_metrics(job_metircs: dict, batch_step_metrics: dict) -> dict:
    """
    Merge metrics under JobID with metrics under batch and jobstep.
    """
    merged_metrics = {}
    for key, value in batch_step_metrics.items():
        if value == "":
            merged_metrics.update({
                key: job_metircs[key]
            })
        else:
            merged_metrics.update({
                key: value
            })
    return merged_metrics


def aggregate_job_dict(job_dict_all: dict) -> dict:
    """
    Aggregate jobid with jobid.batch and jobid.step# , and unfold several metrics under the same 
    attribute, such as "tresusageintot", "tresusageouttot".
    """
    aggregated_job_dict = {}
    job_id_list = job_dict_all.keys()
    for job_id_raw in job_id_list:
        # only keep resource statistics under batch and jobstep
        if ".batch" in job_id_raw or "." in job_id_raw and ".extern" not in job_id_raw:
            # merge metrics
            job_id = job_id_raw.split('.')[0]
            merged_data = merge_metrics(job_dict_all[job_id], job_dict_all[job_id_raw])
            
            # Unfold metrics in treusageintot and tresusageoutot
            folded_metrics = merged_data.get("tresusageintot", None)
            if folded_metrics:
                unfolded_metrics = unfold(folded_metrics, "in")
                merged_data.update(unfolded_metrics)
                merged_data.pop("tresusageintot")
            
            folded_metrics = merged_data.get("tresusageouttot", None)
            if folded_metrics:
                unfolded_metrics = unfold(folded_metrics, "out")
                merged_data.update(unfolded_metrics)
                merged_data.pop("tresusageouttot")
            
            aggregated_job_dict.update({
                job_id: merged_data
            })
    return aggregated_job_dict


if __name__ == '__main__':
    main()