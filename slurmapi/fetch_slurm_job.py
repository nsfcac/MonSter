# -*- coding: utf-8 -*-
"""
This module calls sacct in Slurm to obtain detailed accouting information about
individual jobs or job steps. This module can only be used in Python 3.5 or above.

Jie Li (jie.li@ttu.edu)
"""
import json
import subprocess
from multiprocessing import Process, Queue


def main():
    # Job data format, should be configurable
    format = ["partition", "nodelist", "group", "user", "jobname", "jobid", \
              "submit","start","end","exitcode","cputimeraw","tresusageintot", \
              "tresusageouttot","maxvmsize","alloccpus","ntasks","cluster",\
              "timelimitraw","reqmem"]
    
    # The command used in command line
    command = ["sacct --format=" + ",".join(format) + " -p"]

    # Get strings from command line
    rtn_str = subprocess.run(command, shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
    
    # Split strings by line, and discard the first line that indicates the metrics name
    rtn_str_arr = rtn_str.splitlines()[1:]

    # Get all job data dict
    job_dict_all = process_job_dict(format, rtn_str_arr)

    # Aggregate job data
    aggregated_job_dict = aggregate_job_dict(job_dict_all)
    print(json.dumps(aggregated_job_dict, indent=4))
    # for i in rtn_str_arr:
    #     processed = str_2_json(format, i)
    #     print(json.dumps(processed, indent=4))
    return


def str_2_json(format: list, job_str: str, queue: object) -> dict:
    """
    Process the job string, and generate the json format job data corresponding to job id.
    """
    job_dict = {}
    job_data = {}
    job_str_arr = job_str.split("|")
    
    for i in range(len(format)):
        job_data.update({
            format[i]: job_str_arr[i]
        })
    
    job_dict = {
        job_data["jobid"]: job_data
    }

    queue.put(job_dict)
    # return job_dict


def unfold(metric_str: str) -> dict:
    """
    Unfold the metrics under the same metric name(such as tresusageintot, tresusageouttot)
    """
    metric_dict = {}
    for item in metric_str.split(","):
        item_pair = item.split("=")
        metric_dict.update({
            item_pair[0]: item_pair[1]
        })

    return metric_dict


def process_job_dict(format: list, rtn_str_arr: list) -> dict:
    """
    Process the job string using multiprocesses.
    """
    job_dict_all = {}
    queue = Queue()
    procs = []
    for rtn_str in rtn_str_arr:
        p = Process(target=str_2_json, args=(format, rtn_str, queue))
        procs.append(p)
        p.start()
    
    for _ in procs:
        job_dict = queue.get()
        job_dict_all.update(job_dict)

    for p in procs:
        p.join()

    return job_dict_all


def aggregate_job_dict(job_dict_all: dict) -> dict:
    """
    Aggregate jobid.batch with jobid, and unfold several metrics under the same 
    attribute, such as "tresusageintot", "tresusageouttot".
    """
    aggregated_job_dict = {}
    job_id_list = job_dict_all.keys()
    for job_id in job_id_list:
        if ".batch" not in job_id:
            job_id_batch = job_id + ".batch"
            job_data = merge_metrics(job_dict_all[job_id], job_dict_all[job_id_batch])
            
            # Unfold metrics in treusageintot and tresusageoutot
            folded_metrics = job_data.get("tresusageintot", None)
            if folded_metrics:
                unfolded_metrics = unfold(folded_metrics)
                job_data.update(unfolded_metrics)
                job_data.pop("tresusageintot")
            
            folded_metrics = job_data.get("tresusageouttot", None)
            if folded_metrics:
                unfolded_metrics = unfold(folded_metrics)
                job_data.update(unfolded_metrics)
                job_data.pop("tresusageouttot")
            
            aggregated_job_dict.update({
                job_id: job_data
            })
    return aggregated_job_dict
    

def merge_metrics(job_metircs: dict, batch_metrics: dict) -> dict:
    """
    Merge metrics under jobid.batch to metrics under jobid.
    """
    merged_metrics = {}
    for key, value in job_metircs.items():
        if value == None:
            merged_metrics.update({
                key: batch_metrics[key]
            })
        else:
            merged_metrics.update({
                key: value
            })
    return merged_metrics



if __name__ == '__main__':
    main()